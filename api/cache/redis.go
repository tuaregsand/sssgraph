package cache

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultDialTimeout = 5 * time.Second
	defaultWriteWait   = 5 * time.Second
	defaultReadWait    = 60 * time.Second
)

var Client *RedisClient

type Config struct {
	Addr                  string
	Password              string
	DB                    int
	UseTLS                bool
	TLSServerName         string
	TLSInsecureSkipVerify bool
}

type RedisClient struct {
	cfg Config
}

type Subscription struct {
	messages chan string
	closeFn  func() error
}

func Connect(cfg Config) error {
	client := &RedisClient{cfg: cfg}
	if err := client.Ping(context.Background()); err != nil {
		return err
	}

	Client = client
	return nil
}

func Ping(ctx context.Context) error {
	if Client == nil {
		return errors.New("redis client is not initialized")
	}
	return Client.Ping(ctx)
}

func Close() error {
	Client = nil
	return nil
}

func (c *RedisClient) Ping(ctx context.Context) error {
	conn, rw, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := writeCommand(conn, rw, "PING"); err != nil {
		return err
	}

	reply, err := readRESP(conn, rw.Reader)
	if err != nil {
		return err
	}

	switch value := reply.(type) {
	case string:
		if strings.ToUpper(value) == "PONG" {
			return nil
		}
		return fmt.Errorf("unexpected ping response: %s", value)
	default:
		return fmt.Errorf("unexpected ping response type: %T", value)
	}
}

func (c *RedisClient) Subscribe(ctx context.Context, channel string) (*Subscription, error) {
	if strings.TrimSpace(channel) == "" {
		return nil, errors.New("channel is required")
	}

	conn, rw, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}

	if err := writeCommand(conn, rw, "SUBSCRIBE", channel); err != nil {
		_ = conn.Close()
		return nil, err
	}

	reply, err := readRESP(conn, rw.Reader)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	ack, ok := reply.([]any)
	if !ok || len(ack) < 3 || strings.ToLower(toString(ack[0])) != "subscribe" {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected subscribe ack: %v", reply)
	}

	subCtx, cancel := context.WithCancel(ctx)
	messages := make(chan string, 1024)
	var closeOnce sync.Once

	closeSubscription := func() error {
		var closeErr error
		closeOnce.Do(func() {
			cancel()
			closeErr = conn.Close()
		})
		return closeErr
	}

	go func() {
		defer close(messages)
		defer cancel()
		defer conn.Close()

		for {
			select {
			case <-subCtx.Done():
				return
			default:
			}

			payload, err := readRESP(conn, rw.Reader)
			if err != nil {
				if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
					return
				}
				return
			}

			arr, ok := payload.([]any)
			if !ok || len(arr) < 3 {
				continue
			}

			kind := strings.ToLower(toString(arr[0]))
			if kind != "message" {
				continue
			}

			message := toString(arr[2])
			select {
			case messages <- message:
			case <-subCtx.Done():
				return
			}
		}
	}()

	return &Subscription{
		messages: messages,
		closeFn:  closeSubscription,
	}, nil
}

func (s *Subscription) Messages() <-chan string {
	return s.messages
}

func (s *Subscription) Close() error {
	if s == nil || s.closeFn == nil {
		return nil
	}
	return s.closeFn()
}

func (c *RedisClient) dial(ctx context.Context) (net.Conn, *bufio.ReadWriter, error) {
	address := strings.TrimSpace(c.cfg.Addr)
	if address == "" {
		return nil, nil, errors.New("redis address is required")
	}

	dialer := &net.Dialer{Timeout: defaultDialTimeout}
	var conn net.Conn
	var err error
	if c.cfg.UseTLS {
		serverName := strings.TrimSpace(c.cfg.TLSServerName)
		if serverName == "" {
			host, _, splitErr := net.SplitHostPort(address)
			if splitErr == nil {
				serverName = host
			}
		}

		tlsDialer := &tls.Dialer{
			NetDialer: dialer,
			Config: &tls.Config{
				ServerName:         serverName,
				InsecureSkipVerify: c.cfg.TLSInsecureSkipVerify,
			},
		}
		conn, err = tlsDialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, nil, err
		}
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, nil, err
		}
	}

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	if c.cfg.Password != "" {
		if err := writeCommand(conn, rw, "AUTH", c.cfg.Password); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}

		reply, err := readRESP(conn, rw.Reader)
		if err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		if !strings.EqualFold(toString(reply), "OK") {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("redis auth failed: %v", reply)
		}
	}

	if c.cfg.DB > 0 {
		if err := writeCommand(conn, rw, "SELECT", strconv.Itoa(c.cfg.DB)); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}

		reply, err := readRESP(conn, rw.Reader)
		if err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		if !strings.EqualFold(toString(reply), "OK") {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("redis select failed: %v", reply)
		}
	}

	return conn, rw, nil
}

func writeCommand(conn net.Conn, rw *bufio.ReadWriter, args ...string) error {
	if len(args) == 0 {
		return errors.New("command is required")
	}

	if err := conn.SetWriteDeadline(time.Now().Add(defaultWriteWait)); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(rw, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(rw, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}

	return rw.Flush()
}

func readRESP(conn net.Conn, reader *bufio.Reader) (any, error) {
	if err := conn.SetReadDeadline(time.Now().Add(defaultReadWait)); err != nil {
		return nil, err
	}

	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		return line, nil
	case '-':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(line)
	case ':':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, err
		}
		return value, nil
	case '$':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if length < 0 {
			return nil, nil
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*':
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, nil
		}
		array := make([]any, 0, count)
		for i := 0; i < count; i++ {
			value, err := readRESP(conn, reader)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix: %q", prefix)
	}
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

func toString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}
