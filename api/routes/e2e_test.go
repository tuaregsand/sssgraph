package routes

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/database"
	"github.com/laserstream/api/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestAPIEndToEndFlow(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("WEBHOOK_SIGNING_ENABLED", "true")
	t.Setenv("WEBHOOK_SIGNING_SECRET", "e2e-webhook-secret")
	t.Setenv("WEBHOOK_SIGNING_KEY_ID", "e2e-key")

	db := setupE2EDatabase(t)
	database.DB = db
	t.Cleanup(func() {
		database.DB = nil
	})

	var deliveredWebhooks int32
	automationWebhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&deliveredWebhooks, 1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(automationWebhookServer.Close)

	clickhouseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := strings.TrimSpace(string(body))

		switch {
		case strings.Contains(query, "SELECT 1 FORMAT TabSeparated"):
			_, _ = w.Write([]byte("1\n"))
		case strings.Contains(query, "baseline_events"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"baseline_events":100,"baseline_unique_signatures":50,"baseline_transactions":40,"evaluation_events":500,"evaluation_unique_signatures":220,"evaluation_transactions":180}` + "\n"))
		case strings.Contains(query, "metric_for_automation"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metric_for_automation":150}` + "\n"))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"event_count":1234,"program_id":"11111111111111111111111111111111","signature":"5igE2ETestSig","slot":90210}` + "\n"))
		}
	}))
	t.Cleanup(func() {
		clickhouseServer.Close()
		analytics.Client = nil
	})

	if err := analytics.Connect(analytics.Config{
		URL:     clickhouseServer.URL,
		Table:   "solana_events",
		Timeout: 2 * time.Second,
	}); err != nil {
		t.Fatalf("failed to connect test clickhouse client: %v", err)
	}

	app := setupTestApp()

	createdWebhookID := createAgentWebhookForE2E(t, app)
	if createdWebhookID == 0 {
		t.Fatalf("expected webhook to be created")
	}
	assertInboundWebhookReplayProtectionForE2E(t, app)

	assertAgentQueryEvidenceForE2E(t, app)

	automationID := createAutomationForE2E(t, app, automationWebhookServer.URL)
	evaluateAutomationForE2E(t, app, automationID)
	evaluateAutomationForE2E(t, app, automationID)

	if got := atomic.LoadInt32(&deliveredWebhooks); got != 1 {
		t.Fatalf("expected one webhook delivery due to dedupe, got %d", got)
	}

	assertAutomationAuditLogsForE2E(t, app, automationID)
	assertBacktestEndpointsForE2E(t, app)
}

func setupE2EDatabase(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}

	if err := db.AutoMigrate(
		&models.IDL{},
		&models.AgentWebhook{},
		&models.AgentAutomation{},
		&models.AgentAutomationEvaluation{},
		&models.WebhookReplayNonce{},
		&models.AgentInboundWebhook{},
	); err != nil {
		t.Fatalf("failed to migrate sqlite schema: %v", err)
	}

	return db
}

func createAgentWebhookForE2E(t *testing.T, app *fiber.App) float64 {
	t.Helper()

	body := []byte(`{"url":"https://agent.example/webhook","program_id":"11111111111111111111111111111111","event_type":"transaction"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/webhooks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "admin")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating webhook, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode webhook create response: %v", err)
	}
	id, ok := payload["id"].(float64)
	if !ok || id <= 0 {
		t.Fatalf("expected created webhook id, got %v", payload["id"])
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/agent/webhooks", nil)
	listReq.Header.Set("X-API-Key", "admin")
	listResp, err := app.Test(listReq)
	if err != nil {
		t.Fatalf("unexpected list error: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 listing webhooks, got %d", listResp.StatusCode)
	}

	var listPayload []map[string]any
	if err := json.NewDecoder(listResp.Body).Decode(&listPayload); err != nil {
		t.Fatalf("failed to decode webhook list response: %v", err)
	}
	if len(listPayload) == 0 {
		t.Fatalf("expected webhook list to contain created record")
	}

	return id
}

func assertAgentQueryEvidenceForE2E(t *testing.T, app *fiber.App) {
	t.Helper()

	body := []byte(`{"prompt":"count events in the last day","mode":"verified","execute":true,"limit":10}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected query test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on query endpoint, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode query response: %v", err)
	}

	evidence, ok := payload["evidence_bundle"].(map[string]any)
	if !ok {
		t.Fatalf("expected evidence_bundle object")
	}
	if _, ok := evidence["sql_hash_sha256"].(string); !ok {
		t.Fatalf("expected sql hash in evidence bundle")
	}
	if signatures, ok := evidence["signatures"].([]any); !ok || len(signatures) == 0 {
		t.Fatalf("expected signature list in evidence bundle")
	}
	if slots, ok := evidence["slots"].([]any); !ok || len(slots) == 0 {
		t.Fatalf("expected slots list in evidence bundle")
	}
}

func createAutomationForE2E(t *testing.T, app *fiber.App, webhookURL string) float64 {
	t.Helper()

	body := []byte(`{
		"name":"Growth Monitor",
		"description":"E2E monitor",
		"program_id":"11111111111111111111111111111111",
		"query_template":"SELECT 150 AS metric_for_automation FROM solana_events WHERE timestamp >= parseDateTimeBestEffort('{{from}}') AND timestamp < parseDateTimeBestEffort('{{to}}')",
		"comparator":"gt",
		"threshold":100,
		"schedule_minutes":1,
		"window_minutes":60,
		"cooldown_minutes":0,
		"dedupe_minutes":60,
		"retry_max_attempts":2,
		"retry_initial_backoff_ms":5,
		"webhook_url":"` + webhookURL + `",
		"is_active":true
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/automations", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "admin")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected automation create error: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating automation, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode automation create response: %v", err)
	}
	id, ok := payload["id"].(float64)
	if !ok || id <= 0 {
		t.Fatalf("expected automation id, got %v", payload["id"])
	}
	return id
}

func evaluateAutomationForE2E(t *testing.T, app *fiber.App, id float64) {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/api/agent/automations/"+formatID(id)+"/evaluate", nil)
	req.Header.Set("X-API-Key", "admin")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected automation evaluate error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 evaluating automation, got %d", resp.StatusCode)
	}
}

func assertAutomationAuditLogsForE2E(t *testing.T, app *fiber.App, id float64) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/api/agent/automations/"+formatID(id)+"/audit", nil)
	req.Header.Set("X-API-Key", "admin")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected audit list error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 listing automation audit logs, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode audit logs response: %v", err)
	}
	items, ok := payload["items"].([]any)
	if !ok || len(items) < 2 {
		t.Fatalf("expected at least two audit log entries, got %v", payload["items"])
	}
}

func assertBacktestEndpointsForE2E(t *testing.T, app *fiber.App) {
	t.Helper()

	backtestBody := []byte(`{
		"launches": [{"name":"Launch A","program_id":"11111111111111111111111111111111","launch_at":"2025-01-05T12:00:00Z"}],
		"baseline_hours": 24,
		"evaluation_hours": 6
	}`)
	backtestReq := httptest.NewRequest(http.MethodPost, "/api/agent/backtest/launches", bytes.NewReader(backtestBody))
	backtestReq.Header.Set("Content-Type", "application/json")
	backtestReq.Header.Set("X-API-Key", "reader")

	backtestResp, err := app.Test(backtestReq)
	if err != nil {
		t.Fatalf("unexpected backtest error: %v", err)
	}
	if backtestResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from backtest endpoint, got %d", backtestResp.StatusCode)
	}

	scoreBody := []byte(`{
		"history_launches": [
			{"name":"Launch A","program_id":"11111111111111111111111111111111","launch_at":"2025-01-05T12:00:00Z"},
			{"name":"Launch B","program_id":"11111111111111111111111111111111","launch_at":"2025-01-06T12:00:00Z"},
			{"name":"Launch C","program_id":"11111111111111111111111111111111","launch_at":"2025-01-07T12:00:00Z"}
		],
		"candidate":{"name":"Launch X","program_id":"11111111111111111111111111111111","launch_at":"2025-01-08T12:00:00Z"},
		"baseline_hours": 24,
		"evaluation_hours": 6
	}`)
	scoreReq := httptest.NewRequest(http.MethodPost, "/api/agent/backtest/launches/score", bytes.NewReader(scoreBody))
	scoreReq.Header.Set("Content-Type", "application/json")
	scoreReq.Header.Set("X-API-Key", "reader")

	scoreResp, err := app.Test(scoreReq)
	if err != nil {
		t.Fatalf("unexpected score error: %v", err)
	}
	if scoreResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from score endpoint, got %d", scoreResp.StatusCode)
	}
}

func assertInboundWebhookReplayProtectionForE2E(t *testing.T, app *fiber.App) {
	t.Helper()

	body := []byte(`{"source":"agent","event":"hello"}`)
	timestamp := time.Now().UTC().Format(time.RFC3339)
	nonce := "e2e-nonce-1"
	signature := webhookSignatureForE2E("e2e-webhook-secret", timestamp, nonce, body)

	req := httptest.NewRequest(http.MethodPost, "/api/agent/webhooks/inbound", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Laser-Timestamp", timestamp)
	req.Header.Set("X-Laser-Nonce", nonce)
	req.Header.Set("X-Laser-Signature", "v1="+signature)
	req.Header.Set("X-Laser-Key-Id", "e2e-key")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected inbound webhook error: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 for signed inbound webhook, got %d", resp.StatusCode)
	}

	replayReq := httptest.NewRequest(http.MethodPost, "/api/agent/webhooks/inbound", bytes.NewReader(body))
	replayReq.Header.Set("Content-Type", "application/json")
	replayReq.Header.Set("X-Laser-Timestamp", timestamp)
	replayReq.Header.Set("X-Laser-Nonce", nonce)
	replayReq.Header.Set("X-Laser-Signature", "v1="+signature)
	replayReq.Header.Set("X-Laser-Key-Id", "e2e-key")

	replayResp, err := app.Test(replayReq)
	if err != nil {
		t.Fatalf("unexpected replay request error: %v", err)
	}
	if replayResp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for replayed nonce, got %d", replayResp.StatusCode)
	}
}

func webhookSignatureForE2E(secret, timestamp, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("\n"))
	mac.Write([]byte(nonce))
	mac.Write([]byte("\n"))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func formatID(id float64) string {
	return strconv.FormatInt(int64(id), 10)
}
