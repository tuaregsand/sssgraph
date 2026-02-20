package routes

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/auth"
	"github.com/laserstream/api/config"
	"github.com/laserstream/api/stream"
)

func TestRoutesRequireAuthentication(t *testing.T) {
	app := setupTestApp()

	unauthReq := httptest.NewRequest(http.MethodGet, "/api/events?program_id=11111111111111111111111111111111", nil)
	unauthResp, err := app.Test(unauthReq)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated request, got %d", unauthResp.StatusCode)
	}

	readReq := httptest.NewRequest(http.MethodPost, "/api/idls", nil)
	readReq.Header.Set("X-API-Key", "reader")
	readResp, err := app.Test(readReq)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if readResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for read-only key on admin route, got %d", readResp.StatusCode)
	}
}

func TestAgentQueryReportReturnsStrictSchema(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	configureTestClickHouse(t, "solana_events")
	app := setupTestApp()

	reqBody := []byte(`{"prompt":"How many events happened in the last day?","limit":50}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/query/report", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}

	if got := payload["mode"]; got != "analyst_report" {
		t.Fatalf("expected mode=analyst_report, got %v", got)
	}

	verification, ok := payload["verification"].(map[string]any)
	if !ok {
		t.Fatalf("expected verification object in response")
	}
	strict, ok := verification["strict_verified"].(bool)
	if !ok || !strict {
		t.Fatalf("expected strict_verified=true in verification, got %v", verification["strict_verified"])
	}

	report, ok := payload["report"].(map[string]any)
	if !ok {
		t.Fatalf("expected report object in response")
	}
	if report["report_version"] != "1.0" {
		t.Fatalf("expected report_version=1.0, got %v", report["report_version"])
	}
	if _, ok := report["verdict"].(string); !ok {
		t.Fatalf("expected report verdict string, got %T", report["verdict"])
	}
	if _, ok := report["confidence"].(float64); !ok {
		t.Fatalf("expected report confidence number, got %T", report["confidence"])
	}
	if _, ok := report["executive_summary"].(string); !ok {
		t.Fatalf("expected report executive_summary string, got %T", report["executive_summary"])
	}

	items, ok := payload["items"].([]any)
	if !ok || len(items) == 0 {
		t.Fatalf("expected non-empty items array in response")
	}
}

func TestAgentQueryReportAliasForcesStrictMode(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	configureTestClickHouse(t, "solana_events")
	app := setupTestApp()

	reqBody := []byte(`{"prompt":"show activity","mode":"sql","execute":false,"strict_verified":false}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/query/report", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}

	if payload["mode"] != "analyst_report" {
		t.Fatalf("expected mode to be forced to analyst_report, got %v", payload["mode"])
	}
	if _, ok := payload["report"].(map[string]any); !ok {
		t.Fatalf("expected report object from strict alias endpoint")
	}
	verification := payload["verification"].(map[string]any)
	if strict, ok := verification["strict_verified"].(bool); !ok || !strict {
		t.Fatalf("expected strict_verified=true, got %v", verification["strict_verified"])
	}
}

func TestAgentQueryVerifiedModeReturnsExecutionEvidence(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	configureTestClickHouse(t, "solana_events")
	app := setupTestApp()

	reqBody := []byte(`{"prompt":"count events in the last day","mode":"verified","execute":false,"limit":10}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/query", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}

	if payload["mode"] != "verified" {
		t.Fatalf("expected mode=verified, got %v", payload["mode"])
	}
	verification := payload["verification"].(map[string]any)
	if strict, ok := verification["strict_verified"].(bool); !ok || strict {
		t.Fatalf("expected strict_verified=false, got %v", verification["strict_verified"])
	}
	if _, ok := payload["report"]; ok {
		t.Fatalf("did not expect report payload for verified mode")
	}
	if _, ok := payload["items"].([]any); !ok {
		t.Fatalf("expected items array in verified mode response")
	}
	evidence, ok := payload["evidence_bundle"].(map[string]any)
	if !ok {
		t.Fatalf("expected evidence_bundle object")
	}
	if _, ok := evidence["sql_hash_sha256"].(string); !ok {
		t.Fatalf("expected sql_hash_sha256 in evidence bundle")
	}
	if _, ok := evidence["program_ids"].([]any); !ok {
		t.Fatalf("expected program_ids list in evidence bundle")
	}
}

func TestAgentBacktestLaunchesSuccess(t *testing.T) {
	configureTestClickHouse(t, "solana_events")
	app := setupTestApp()

	reqBody := []byte(`{
		"baseline_hours": 24,
		"evaluation_hours": 6,
		"min_evaluation_events": 100,
		"thresholds": {
			"event_growth_ratio": 2.0,
			"unique_signer_growth_ratio": 1.7,
			"transaction_growth_ratio": 2.0
		},
		"launches": [{
			"name": "Launch A",
			"program_id": "11111111111111111111111111111111",
			"launch_at": "2025-01-05T12:00:00Z"
		}]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/backtest/launches", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}

	if payload["status"] != "success" {
		t.Fatalf("expected status=success, got %v", payload["status"])
	}
	summary, ok := payload["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary object")
	}
	if summary["worked"].(float64) < 1 {
		t.Fatalf("expected at least one worked launch, got %v", summary["worked"])
	}
	launches, ok := payload["launches"].([]any)
	if !ok || len(launches) != 1 {
		t.Fatalf("expected one launch result, got %v", payload["launches"])
	}
}

func TestAgentBacktestLaunchesRejectsInvalidLaunchTime(t *testing.T) {
	configureTestClickHouse(t, "solana_events")
	app := setupTestApp()

	reqBody := []byte(`{
		"launches": [{
			"name": "Launch A",
			"program_id": "11111111111111111111111111111111",
			"launch_at": "not-a-time"
		}]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/backtest/launches", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestAgentBacktestScoreLaunchSuccess(t *testing.T) {
	configureTestClickHouse(t, "solana_events")
	app := setupTestApp()

	reqBody := []byte(`{
		"history_launches": [
			{"name":"Launch A","program_id":"11111111111111111111111111111111","launch_at":"2025-01-05T12:00:00Z"},
			{"name":"Launch B","program_id":"11111111111111111111111111111111","launch_at":"2025-01-06T12:00:00Z"},
			{"name":"Launch C","program_id":"11111111111111111111111111111111","launch_at":"2025-01-07T12:00:00Z"}
		],
		"candidate": {"name":"Candidate X","program_id":"11111111111111111111111111111111","launch_at":"2025-01-10T12:00:00Z"},
		"baseline_hours": 24,
		"evaluation_hours": 6
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/backtest/launches/score", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}

	if payload["status"] != "success" {
		t.Fatalf("expected status=success, got %v", payload["status"])
	}
	if _, ok := payload["probability"].(float64); !ok {
		t.Fatalf("expected probability number, got %T", payload["probability"])
	}
	if _, ok := payload["confidence"].(float64); !ok {
		t.Fatalf("expected confidence number, got %T", payload["confidence"])
	}
	if _, ok := payload["probability_band"].(string); !ok {
		t.Fatalf("expected probability_band string, got %T", payload["probability_band"])
	}
	if _, ok := payload["top_contributing_signals"].([]any); !ok {
		t.Fatalf("expected top_contributing_signals list")
	}
}

func setupTestApp() *fiber.App {
	app := fiber.New()
	manager := stream.NewManager(nil, stream.Options{})
	SetupRoutes(app, manager, Options{
		AuthService:       newTestAuthService(),
		AllowPublicHealth: false,
		MetricsEnabled:    false,
	})
	return app
}

func configureTestClickHouse(t *testing.T, table string) {
	t.Helper()

	clickhouseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := strings.TrimSpace(string(body))

		switch {
		case strings.Contains(query, "SELECT 1 FORMAT TabSeparated"):
			_, _ = w.Write([]byte("1\n"))
		case strings.Contains(query, "baseline_events"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"baseline_events":100,"baseline_unique_signatures":50,"baseline_transactions":40,"evaluation_events":500,"evaluation_unique_signatures":200,"evaluation_transactions":160}` + "\n"))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"event_count":1234}` + "\n"))
		}
	}))
	t.Cleanup(func() {
		clickhouseServer.Close()
		analytics.Client = nil
	})

	if err := analytics.Connect(analytics.Config{
		URL:     clickhouseServer.URL,
		Table:   table,
		Timeout: 2 * time.Second,
	}); err != nil {
		t.Fatalf("failed to connect test clickhouse client: %v", err)
	}
}

func newTestAuthService() *auth.Service {
	return auth.NewService(config.AuthConfig{
		Enabled: true,
		APIKeys: []config.APIKeyConfig{
			{
				Key:             "reader",
				Subject:         "reader-subject",
				Role:            "read",
				AllowedPrograms: []string{"*"},
			},
			{
				Key:             "admin",
				Subject:         "admin-subject",
				Role:            "admin",
				AllowedPrograms: []string{"*"},
			},
		},
	})
}
