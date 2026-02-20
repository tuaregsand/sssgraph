package ratelimit

import (
	"testing"
	"time"
)

func TestAllowHonorsLimitWithinWindow(t *testing.T) {
	limiter := NewFixedWindowLimiter(2, time.Minute)

	if ok, _ := limiter.Allow("ip-1"); !ok {
		t.Fatal("expected first request to be allowed")
	}
	if ok, _ := limiter.Allow("ip-1"); !ok {
		t.Fatal("expected second request to be allowed")
	}
	if ok, _ := limiter.Allow("ip-1"); ok {
		t.Fatal("expected third request to be blocked")
	}
}

func TestAllowResetsAfterWindow(t *testing.T) {
	limiter := NewFixedWindowLimiter(1, 20*time.Millisecond)

	if ok, _ := limiter.Allow("ip-1"); !ok {
		t.Fatal("expected first request to be allowed")
	}
	if ok, _ := limiter.Allow("ip-1"); ok {
		t.Fatal("expected second request in same window to be blocked")
	}

	time.Sleep(25 * time.Millisecond)
	if ok, _ := limiter.Allow("ip-1"); !ok {
		t.Fatal("expected request after window to be allowed")
	}
}
