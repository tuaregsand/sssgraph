package ratelimit

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
)

type FixedWindowLimiter struct {
	limit  int
	window time.Duration

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	windowStart time.Time
	count       int
}

func NewFixedWindowLimiter(limit int, window time.Duration) *FixedWindowLimiter {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}
	return &FixedWindowLimiter{
		limit:   limit,
		window:  window,
		buckets: make(map[string]*bucket),
	}
}

func (l *FixedWindowLimiter) Middleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		key := clientIP(c)
		allowed, remaining := l.Allow(key)
		c.Set("X-RateLimit-Limit", intToString(l.limit))
		c.Set("X-RateLimit-Remaining", intToString(remaining))
		if !allowed {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": "rate limit exceeded",
			})
		}
		return c.Next()
	}
}

func (l *FixedWindowLimiter) Allow(key string) (bool, int) {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	entry, exists := l.buckets[key]
	if !exists || now.Sub(entry.windowStart) >= l.window {
		entry = &bucket{
			windowStart: now,
			count:       0,
		}
		l.buckets[key] = entry
	}

	if entry.count >= l.limit {
		return false, 0
	}

	entry.count++
	remaining := l.limit - entry.count

	// Opportunistic cleanup to prevent unbounded map growth.
	if len(l.buckets) > 20000 {
		cutoff := now.Add(-2 * l.window)
		for k, v := range l.buckets {
			if v.windowStart.Before(cutoff) {
				delete(l.buckets, k)
			}
		}
	}

	return true, remaining
}

func clientIP(c *fiber.Ctx) string {
	ip := strings.TrimSpace(c.IP())
	if ip != "" {
		return ip
	}

	host, _, err := net.SplitHostPort(strings.TrimSpace(c.Context().RemoteAddr().String()))
	if err == nil && host != "" {
		return host
	}

	return "unknown"
}

func intToString(v int) string {
	if v < 0 {
		v = 0
	}
	return strconv.Itoa(v)
}
