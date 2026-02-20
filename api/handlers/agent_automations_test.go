package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laserstream/api/models"
)

func TestNormalizeDurationMinutes(t *testing.T) {
	if got := normalizeDurationMinutes(0, 60); got != 60 {
		t.Fatalf("expected fallback=60, got %d", got)
	}
	if got := normalizeDurationMinutes(15, 60); got != 15 {
		t.Fatalf("expected explicit value=15, got %d", got)
	}
}

func TestCooldownElapsed(t *testing.T) {
	now := time.Now().UTC()
	recent := now.Add(-5 * time.Minute)
	old := now.Add(-40 * time.Minute)

	if cooldownElapsed(&recent, 30, now) {
		t.Fatalf("expected cooldown to block recent trigger")
	}
	if !cooldownElapsed(&old, 30, now) {
		t.Fatalf("expected cooldown to pass for old trigger")
	}
	if !cooldownElapsed(nil, 30, now) {
		t.Fatalf("expected cooldown to pass when no prior trigger exists")
	}
}

func TestIsAutomationDue(t *testing.T) {
	now := time.Now().UTC()
	recent := now.Add(-10 * time.Minute)
	old := now.Add(-2 * time.Hour)

	inactive := &models.AgentAutomation{
		IsActive: true,
	}
	if !isAutomationDue(inactive, now) {
		t.Fatalf("expected never-evaluated automation to be due")
	}

	notDue := &models.AgentAutomation{
		IsActive:        true,
		ScheduleMinutes: 60,
		LastEvaluatedAt: &recent,
	}
	if isAutomationDue(notDue, now) {
		t.Fatalf("expected recently evaluated automation to be not due")
	}

	due := &models.AgentAutomation{
		IsActive:        true,
		ScheduleMinutes: 60,
		LastEvaluatedAt: &old,
	}
	if !isAutomationDue(due, now) {
		t.Fatalf("expected old evaluation to be due")
	}
}

func TestValidateAutomationQueryTemplate(t *testing.T) {
	sql, err := validateAutomationQueryTemplate(
		"SELECT count(*) AS value FROM solana_events WHERE program_id='{{program_id}}' AND timestamp >= parseDateTimeBestEffort('{{from}}') AND timestamp < parseDateTimeBestEffort('{{to}}')",
		"11111111111111111111111111111111",
		60,
	)
	if err != nil {
		t.Fatalf("expected valid template, got error: %v", err)
	}
	if sql == "" {
		t.Fatalf("expected rendered SQL to be returned")
	}
}

func TestPersistAutomationEvaluation(t *testing.T) {
	automation := &models.AgentAutomation{}
	evaluatedAt := time.Now().UTC()
	result := automationEvaluation{
		Value:       42,
		AlertSent:   true,
		EvaluatedAt: evaluatedAt,
	}

	persistAutomationEvaluation(automation, result, "")

	if automation.LastEvaluatedAt == nil || !automation.LastEvaluatedAt.Equal(evaluatedAt) {
		t.Fatalf("expected LastEvaluatedAt to be persisted")
	}
	if automation.LastValue == nil || *automation.LastValue != 42 {
		t.Fatalf("expected LastValue to be persisted")
	}
	if automation.LastTriggeredAt == nil || !automation.LastTriggeredAt.Equal(evaluatedAt) {
		t.Fatalf("expected LastTriggeredAt to be persisted when alert is sent")
	}
}

func TestDedupeElapsed(t *testing.T) {
	now := time.Now().UTC()
	last := now.Add(-5 * time.Minute)

	if dedupeElapsed(&last, "a", "b", 15, now) != true {
		t.Fatalf("expected different dedupe keys to allow delivery")
	}
	if dedupeElapsed(&last, "a", "a", 15, now) {
		t.Fatalf("expected matching dedupe keys inside window to be blocked")
	}
	if !dedupeElapsed(&last, "a", "a", 1, now) {
		t.Fatalf("expected matching dedupe keys outside dedupe window to pass")
	}
}

func TestAutomationDedupeKeyStable(t *testing.T) {
	automation := &models.AgentAutomation{
		ID:            10,
		ProgramID:     "Program1",
		Comparator:    "gt",
		QueryTemplate: "SELECT count(*) FROM solana_events",
		Threshold:     100,
	}

	first := automationDedupeKey(automation, 150.25)
	second := automationDedupeKey(automation, 150.25)
	third := automationDedupeKey(automation, 120.00)

	if first == "" || second == "" {
		t.Fatalf("expected non-empty dedupe key")
	}
	if first != second {
		t.Fatalf("expected stable dedupe key for same inputs")
	}
	if first == third {
		t.Fatalf("expected different dedupe key for different value")
	}
}

func TestSendAutomationWebhookWithRetry(t *testing.T) {
	t.Setenv("WEBHOOK_SIGNING_ENABLED", "true")
	t.Setenv("WEBHOOK_SIGNING_SECRET", "test-webhook-secret")
	t.Setenv("WEBHOOK_SIGNING_KEY_ID", "test-key")

	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := strings.TrimSpace(r.Header.Get(webhookSignatureVersionHeader)); got == "" {
			t.Fatalf("expected webhook signature header to be set")
		}
		if got := strings.TrimSpace(r.Header.Get(webhookTimestampHeader)); got == "" {
			t.Fatalf("expected webhook timestamp header to be set")
		}
		if got := strings.TrimSpace(r.Header.Get(webhookNonceHeader)); got == "" {
			t.Fatalf("expected webhook nonce header to be set")
		}
		if got := strings.TrimSpace(r.Header.Get(webhookKeyIDHeader)); got != "test-key" {
			t.Fatalf("expected key id header test-key, got %q", got)
		}

		current := atomic.AddInt32(&attempts, 1)
		if current < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	evaluation := automationEvaluation{
		AutomationID:  1,
		Name:          "retry-test",
		ProgramID:     "Program1",
		Value:         42,
		Threshold:     10,
		Comparator:    "gt",
		Triggered:     true,
		AlertSent:     false,
		DeliveryState: "sent",
		Query:         "SELECT 42",
		WindowFrom:    time.Now().UTC().Add(-time.Minute).Format(time.RFC3339),
		WindowTo:      time.Now().UTC().Format(time.RFC3339),
		EvaluatedAt:   time.Now().UTC(),
	}

	usedAttempts, err := sendAutomationWebhookWithRetry(
		server.URL,
		evaluation,
		2*time.Second,
		3,
		5*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("expected retry delivery to succeed: %v", err)
	}
	if usedAttempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", usedAttempts)
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("expected server to receive 3 attempts, got %d", attempts)
	}
}

func TestFormatIntervalLiteral(t *testing.T) {
	if got := formatIntervalLiteral(0); got != "" {
		t.Fatalf("expected empty interval for zero duration, got %q", got)
	}
	if got := formatIntervalLiteral(90 * time.Second); got != "90 seconds" {
		t.Fatalf("expected 90 second interval literal, got %q", got)
	}
}
