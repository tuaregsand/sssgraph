package stream

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/websocket/v2"
	"github.com/laserstream/api/cache"
	"github.com/laserstream/api/observability"
)

type Options struct {
	ChannelPrefix       string
	ClientBuffer        int
	PingPeriod          time.Duration
	PongWait            time.Duration
	WriteTimeout        time.Duration
	MaxConnections      int
	MaxConnectionsPerIP int
}

type Manager struct {
	redis *cache.RedisClient
	opts  Options

	mu               sync.Mutex
	hubs             map[string]*channelHub
	totalConnections int
	connectionsPerIP map[string]int
}

type channelHub struct {
	channel string
	cancel  context.CancelFunc
	clients map[*client]struct{}
}

type client struct {
	conn    *websocket.Conn
	send    chan []byte
	channel string

	closeOnce sync.Once
}

func NewManager(redisClient *cache.RedisClient, opts Options) *Manager {
	if opts.ChannelPrefix == "" {
		opts.ChannelPrefix = "events"
	}
	if opts.ClientBuffer <= 0 {
		opts.ClientBuffer = 256
	}
	if opts.PingPeriod <= 0 {
		opts.PingPeriod = 30 * time.Second
	}
	if opts.PongWait <= 0 {
		opts.PongWait = 60 * time.Second
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 10 * time.Second
	}
	if opts.MaxConnections <= 0 {
		opts.MaxConnections = 20000
	}
	if opts.MaxConnectionsPerIP <= 0 {
		opts.MaxConnectionsPerIP = 200
	}

	return &Manager{
		redis:            redisClient,
		opts:             opts,
		hubs:             make(map[string]*channelHub),
		connectionsPerIP: make(map[string]int),
	}
}

func (m *Manager) Serve(conn *websocket.Conn, programID, eventType string) {
	programID = strings.TrimSpace(programID)
	eventType = strings.TrimSpace(eventType)
	clientIP := remoteIP(conn.RemoteAddr().String())

	if !m.acquireConnectionSlot(clientIP) {
		observability.WSConnectionRejected()
		m.closeConn(conn, websocket.CloseTryAgainLater, "websocket capacity reached")
		return
	}
	observability.WSConnectionOpened()
	defer func() {
		m.releaseConnectionSlot(clientIP)
		observability.WSConnectionClosed()
	}()

	if programID == "" {
		m.closeConn(conn, websocket.CloseUnsupportedData, "program_id is required")
		return
	}
	if eventType == "" {
		eventType = "all"
	}

	channel := m.channelName(programID, eventType)
	c := &client{
		conn:    conn,
		send:    make(chan []byte, m.opts.ClientBuffer),
		channel: channel,
	}

	conn.SetReadLimit(1024)
	_ = conn.SetReadDeadline(time.Now().Add(m.opts.PongWait))
	conn.SetPongHandler(func(_ string) error {
		return conn.SetReadDeadline(time.Now().Add(m.opts.PongWait))
	})

	if err := m.register(c); err != nil {
		m.closeConn(conn, websocket.CloseInternalServerErr, "unable to subscribe to stream")
		return
	}
	defer m.unregister(c)

	go m.writePump(c)

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (m *Manager) Close() {
	m.mu.Lock()
	hubs := make([]*channelHub, 0, len(m.hubs))
	for _, hub := range m.hubs {
		hubs = append(hubs, hub)
	}
	m.hubs = make(map[string]*channelHub)
	m.totalConnections = 0
	m.connectionsPerIP = make(map[string]int)
	m.mu.Unlock()

	for _, hub := range hubs {
		hub.cancel()
		for c := range hub.clients {
			c.closeSend()
			_ = c.conn.Close()
		}
	}
}

func (m *Manager) register(c *client) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	hub, exists := m.hubs[c.channel]
	if !exists {
		ctx, cancel := context.WithCancel(context.Background())
		hub = &channelHub{
			channel: c.channel,
			cancel:  cancel,
			clients: make(map[*client]struct{}),
		}
		m.hubs[c.channel] = hub
		go m.forwardMessages(ctx, hub.channel)
	}

	hub.clients[c] = struct{}{}
	return nil
}

func (m *Manager) unregister(c *client) {
	var shutdownHub *channelHub

	m.mu.Lock()
	hub, exists := m.hubs[c.channel]
	if exists {
		if _, ok := hub.clients[c]; ok {
			delete(hub.clients, c)
		}
		if len(hub.clients) == 0 {
			delete(m.hubs, c.channel)
			shutdownHub = hub
		}
	}
	c.closeSend()
	m.mu.Unlock()

	if shutdownHub != nil {
		shutdownHub.cancel()
	}
}

func (m *Manager) forwardMessages(ctx context.Context, channel string) {
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if m.redis == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = nextBackoff(backoff)
				continue
			}
		}

		subscription, err := m.redis.Subscribe(ctx, channel)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = nextBackoff(backoff)
				continue
			}
		}

		backoff = time.Second
		if !m.consumeSubscription(ctx, channel, subscription) {
			_ = subscription.Close()
			return
		}
		_ = subscription.Close()
	}
}

func (m *Manager) consumeSubscription(ctx context.Context, channel string, subscription *cache.Subscription) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case payload, ok := <-subscription.Messages():
			if !ok {
				return true
			}
			m.broadcast(channel, []byte(payload))
		}
	}
}

func (m *Manager) broadcast(channel string, payload []byte) {
	var slowClients []*client
	var shutdownHub *channelHub

	m.mu.Lock()
	hub, exists := m.hubs[channel]
	if !exists {
		m.mu.Unlock()
		return
	}

	for c := range hub.clients {
		select {
		case c.send <- payload:
		default:
			delete(hub.clients, c)
			c.closeSend()
			slowClients = append(slowClients, c)
		}
	}

	if len(hub.clients) == 0 {
		delete(m.hubs, channel)
		shutdownHub = hub
	}
	m.mu.Unlock()

	if shutdownHub != nil {
		shutdownHub.cancel()
	}

	for _, c := range slowClients {
		observability.WSSlowClientDropped()
		m.closeConn(c.conn, websocket.ClosePolicyViolation, "client too slow; disconnected")
	}
}

func (m *Manager) writePump(c *client) {
	ticker := time.NewTicker(m.opts.PingPeriod)
	defer ticker.Stop()
	defer c.conn.Close()

	for {
		select {
		case payload, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(m.opts.WriteTimeout))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "closed"))
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
			observability.WSMessageOut()
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(m.opts.WriteTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (m *Manager) closeConn(conn *websocket.Conn, code int, reason string) {
	deadline := time.Now().Add(m.opts.WriteTimeout)
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline)
	_ = conn.Close()
}

func (m *Manager) channelName(programID, eventType string) string {
	return fmt.Sprintf("%s:%s:%s", m.opts.ChannelPrefix, programID, eventType)
}

func (c *client) closeSend() {
	c.closeOnce.Do(func() {
		close(c.send)
	})
}

func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > 30*time.Second {
		return 30 * time.Second
	}
	return next
}

func (m *Manager) acquireConnectionSlot(ip string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.totalConnections >= m.opts.MaxConnections {
		return false
	}

	if ip != "" && m.connectionsPerIP[ip] >= m.opts.MaxConnectionsPerIP {
		return false
	}

	m.totalConnections++
	if ip != "" {
		m.connectionsPerIP[ip]++
	}
	return true
}

func (m *Manager) releaseConnectionSlot(ip string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.totalConnections > 0 {
		m.totalConnections--
	}
	if ip == "" {
		return
	}

	count := m.connectionsPerIP[ip]
	if count <= 1 {
		delete(m.connectionsPerIP, ip)
		return
	}
	m.connectionsPerIP[ip] = count - 1
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err == nil {
		return host
	}
	return strings.TrimSpace(remoteAddr)
}
