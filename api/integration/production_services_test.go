package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/auth"
	"github.com/laserstream/api/cache"
	"github.com/laserstream/api/config"
	"github.com/laserstream/api/database"
	"github.com/laserstream/api/routes"
	"github.com/laserstream/api/stream"
)

func TestProductionLikeServices(t *testing.T) {
	if strings.TrimSpace(os.Getenv("INTEGRATION_TEST_SERVICES")) != "1" {
		t.Skip("INTEGRATION_TEST_SERVICES is not enabled")
	}

	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("WEBHOOK_SIGNING_ENABLED", "false")

	dbCfg := config.DatabaseConfig{
		Host:          envOrDefault("TEST_POSTGRES_HOST", "127.0.0.1"),
		User:          envOrDefault("TEST_POSTGRES_USER", "postgres"),
		Password:      envOrDefault("TEST_POSTGRES_PASSWORD", "postgres"),
		Name:          envOrDefault("TEST_POSTGRES_DB", "laserstream"),
		Port:          envOrDefault("TEST_POSTGRES_PORT", "5432"),
		SSLMode:       envOrDefault("TEST_POSTGRES_SSLMODE", "disable"),
		TimeZone:      "UTC",
		RunMigrations: true,
		RequireSchema: true,
	}
	if err := database.ConnectDB(dbCfg); err != nil {
		t.Fatalf("failed to connect postgres: %v", err)
	}
	t.Cleanup(func() {
		_ = database.Close()
	})

	if err := cache.Connect(cache.Config{
		Addr:     envOrDefault("TEST_REDIS_ADDR", "127.0.0.1:6379"),
		Password: envOrDefault("TEST_REDIS_PASSWORD", ""),
		DB:       0,
		UseTLS:   false,
	}); err != nil {
		t.Fatalf("failed to connect redis: %v", err)
	}
	t.Cleanup(func() {
		_ = cache.Close()
	})

	clickhouseURL := envOrDefault("TEST_CLICKHOUSE_HTTP_URL", "http://127.0.0.1:8123")
	if err := analytics.Connect(analytics.Config{
		URL:      clickhouseURL,
		Database: envOrDefault("TEST_CLICKHOUSE_DATABASE", "default"),
		Table:    "solana_events",
		Timeout:  5 * time.Second,
	}); err != nil {
		t.Fatalf("failed to connect clickhouse: %v", err)
	}
	if err := ensureClickHouseEventsTable(clickhouseURL); err != nil {
		t.Fatalf("failed to prepare clickhouse table: %v", err)
	}

	app := fiber.New()
	wsManager := stream.NewManager(cache.Client, stream.Options{})
	authService := auth.NewService(config.AuthConfig{
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

	routes.SetupRoutes(app, wsManager, routes.Options{
		AuthService:       authService,
		AllowPublicHealth: false,
		MetricsEnabled:    true,
	})

	assertReadyEndpoint(t, app)
	assertWebhookInsert(t, app)
	assertVerifiedQuery(t, app)
	assertMetricsContainsSLOCounters(t, app)
}

func assertReadyEndpoint(t *testing.T, app *fiber.App) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/api/ready", nil)
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("ready endpoint request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected ready status 200, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func assertWebhookInsert(t *testing.T, app *fiber.App) {
	t.Helper()

	body := []byte(`{"url":"https://agent.example/webhook","program_id":"11111111111111111111111111111111","event_type":"transaction"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/webhooks", bytes.NewReader(body))
	req.Header.Set("X-API-Key", "admin")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("webhook insert request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected webhook create status 201, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func assertVerifiedQuery(t *testing.T, app *fiber.App) {
	t.Helper()

	body := []byte(`{"prompt":"count events in the last day","mode":"verified","execute":true,"limit":10}`)
	req := httptest.NewRequest(http.MethodPost, "/api/agent/query", bytes.NewReader(body))
	req.Header.Set("X-API-Key", "reader")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("agent query request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected agent query status 200, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}

	var parsed map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode query response: %v", err)
	}
	if _, ok := parsed["sql_hash_sha256"].(string); !ok {
		t.Fatalf("expected sql_hash_sha256 field in query response")
	}
}

func assertMetricsContainsSLOCounters(t *testing.T, app *fiber.App) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("X-API-Key", "reader")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("metrics endpoint request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d", resp.StatusCode)
	}

	payload, _ := io.ReadAll(resp.Body)
	text := string(payload)
	required := []string{
		"api_agent_query_executions_total",
		"api_agent_query_errors_total",
		"api_agent_automation_evaluations_total",
		"api_agent_automation_failures_total",
	}
	for _, key := range required {
		if !strings.Contains(text, key) {
			t.Fatalf("expected metrics output to contain %s", key)
		}
	}
}

func ensureClickHouseEventsTable(clickhouseURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	createTable := `
CREATE TABLE IF NOT EXISTS solana_events (
    timestamp DateTime,
    event_id String,
    event_type String,
    program_id String,
    slot UInt64,
    signature String,
    payload String
) ENGINE = MergeTree()
ORDER BY (timestamp, event_id)
`
	if err := postClickHouseQuery(ctx, clickhouseURL, createTable); err != nil {
		return err
	}

	insertRow := `
INSERT INTO solana_events (timestamp, event_id, event_type, program_id, slot, signature, payload)
VALUES (now(), 'evt-1', 'transaction', '11111111111111111111111111111111', 1, 'sig-1', '{}')
`
	return postClickHouseQuery(ctx, clickhouseURL, insertRow)
}

func postClickHouseQuery(ctx context.Context, clickhouseURL string, query string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, clickhouseURL, strings.NewReader(strings.TrimSpace(query)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse query failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
