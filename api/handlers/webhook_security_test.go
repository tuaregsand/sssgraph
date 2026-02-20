package handlers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/database"
	"github.com/laserstream/api/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestApplyWebhookSignatureHeaders(t *testing.T) {
	t.Setenv("WEBHOOK_SIGNING_ENABLED", "true")
	t.Setenv("WEBHOOK_SIGNING_SECRET", "unit-test-secret")
	t.Setenv("WEBHOOK_SIGNING_KEY_ID", "unit-key")

	body := []byte(`{"hello":"world"}`)
	req, err := http.NewRequest(http.MethodPost, "https://example.com", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("unexpected request creation error: %v", err)
	}

	if err := applyWebhookSignatureHeaders(req, body); err != nil {
		t.Fatalf("expected headers to be applied: %v", err)
	}

	timestamp := req.Header.Get(webhookTimestampHeader)
	nonce := req.Header.Get(webhookNonceHeader)
	signature := req.Header.Get(webhookSignatureVersionHeader)

	if timestamp == "" || nonce == "" || signature == "" {
		t.Fatalf("expected timestamp/nonce/signature headers to be set")
	}
	if req.Header.Get(webhookKeyIDHeader) != "unit-key" {
		t.Fatalf("expected webhook key id header")
	}

	expected := webhookSignaturePrefix + computeWebhookSignature("unit-test-secret", timestamp, nonce, body)
	if signature != expected {
		t.Fatalf("unexpected signature value")
	}
}

func TestVerifyInboundWebhook(t *testing.T) {
	t.Setenv("WEBHOOK_SIGNING_ENABLED", "true")
	t.Setenv("WEBHOOK_SIGNING_SECRET", "unit-test-secret")
	t.Setenv("WEBHOOK_SIGNATURE_TOLERANCE", "5m")

	body := []byte(`{"event":"ok"}`)
	timestamp := time.Now().UTC().Format(time.RFC3339)
	nonce := "nonce-123"
	signature := webhookSignaturePrefix + computeWebhookSignature("unit-test-secret", timestamp, nonce, body)

	app := fiber.New()
	app.Post("/verify", func(c *fiber.Ctx) error {
		_, statusCode, err := verifyInboundWebhook(c, c.Body())
		if err != nil {
			return c.Status(statusCode).JSON(fiber.Map{"error": err.Error()})
		}
		return c.SendStatus(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodPost, "/verify", bytes.NewReader(body))
	req.Header.Set(webhookTimestampHeader, timestamp)
	req.Header.Set(webhookNonceHeader, nonce)
	req.Header.Set(webhookSignatureVersionHeader, signature)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected app test error: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 from valid signed webhook, got %d", resp.StatusCode)
	}
}

func TestRegisterWebhookNonceBlocksReplay(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	if err := db.AutoMigrate(&models.WebhookReplayNonce{}); err != nil {
		t.Fatalf("failed to migrate replay table: %v", err)
	}

	database.DB = db
	t.Cleanup(func() {
		database.DB = nil
	})

	now := time.Now().UTC()
	inserted, err := registerWebhookNonce("nonce-a", now)
	if err != nil {
		t.Fatalf("expected first nonce registration to pass: %v", err)
	}
	if !inserted {
		t.Fatalf("expected first nonce registration to be inserted")
	}

	inserted, err = registerWebhookNonce("nonce-a", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("expected replay registration check to not error: %v", err)
	}
	if inserted {
		t.Fatalf("expected replay nonce registration to be blocked")
	}
}
