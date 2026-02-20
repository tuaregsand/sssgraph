package handlers

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/database"
	"github.com/laserstream/api/models"
	"gorm.io/gorm/clause"
)

const (
	webhookSignatureVersionHeader = "X-Laser-Signature"
	webhookTimestampHeader        = "X-Laser-Timestamp"
	webhookNonceHeader            = "X-Laser-Nonce"
	webhookKeyIDHeader            = "X-Laser-Key-Id"
	webhookSignaturePrefix        = "v1="
)

type inboundWebhookAuth struct {
	Nonce     string
	Signature string
	KeyID     string
	Timestamp time.Time
}

func AgentInboundWebhook(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	if !webhookSigningEnabled() {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "inbound webhook signing is disabled",
		})
	}

	body := c.Body()
	if len(strings.TrimSpace(string(body))) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "payload is required"})
	}
	if !json.Valid(body) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "payload must be valid JSON"})
	}

	auth, statusCode, err := verifyInboundWebhook(c, body)
	if err != nil {
		return c.Status(statusCode).JSON(fiber.Map{"error": err.Error()})
	}

	ok, nonceErr := registerWebhookNonce(auth.Nonce, auth.Timestamp)
	if nonceErr != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to persist webhook nonce"})
	}
	if !ok {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "replay detected: nonce already used"})
	}

	record := models.AgentInboundWebhook{
		Nonce:      auth.Nonce,
		Signature:  auth.Signature,
		KeyID:      auth.KeyID,
		SourceIP:   strings.TrimSpace(c.IP()),
		Payload:    json.RawMessage(body),
		ReceivedAt: auth.Timestamp.UTC(),
	}
	if err := database.DB.Create(&record).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to persist inbound webhook"})
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"status":      "accepted",
		"id":          record.ID,
		"received_at": record.ReceivedAt.UTC().Format(time.RFC3339),
	})
}

func GetAgentInboundWebhooks(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	limit := c.QueryInt("limit", 100)
	offset := c.QueryInt("offset", 0)
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	var items []models.AgentInboundWebhook
	if err := database.DB.Order("received_at desc").Limit(limit).Offset(offset).Find(&items).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch inbound webhooks"})
	}

	return c.JSON(fiber.Map{
		"items":  items,
		"limit":  limit,
		"offset": offset,
		"count":  len(items),
	})
}

func applyWebhookSignatureHeaders(req *http.Request, body []byte) error {
	if req == nil {
		return fmt.Errorf("request cannot be nil")
	}
	if !webhookSigningEnabled() {
		return nil
	}

	secret := webhookSigningSecret()
	if secret == "" {
		return fmt.Errorf("webhook signing secret is empty")
	}

	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	nonce, err := generateWebhookNonce()
	if err != nil {
		return err
	}
	signature := computeWebhookSignature(secret, timestamp, nonce, body)

	req.Header.Set(webhookTimestampHeader, timestamp)
	req.Header.Set(webhookNonceHeader, nonce)
	req.Header.Set(webhookSignatureVersionHeader, webhookSignaturePrefix+signature)
	if keyID := webhookSigningKeyID(); keyID != "" {
		req.Header.Set(webhookKeyIDHeader, keyID)
	}

	return nil
}

func verifyInboundWebhook(c *fiber.Ctx, body []byte) (inboundWebhookAuth, int, error) {
	secret := webhookSigningSecret()
	if secret == "" {
		return inboundWebhookAuth{}, fiber.StatusInternalServerError, fmt.Errorf("webhook signing secret is not configured")
	}

	timestampHeader := strings.TrimSpace(c.Get(webhookTimestampHeader))
	if timestampHeader == "" {
		return inboundWebhookAuth{}, fiber.StatusUnauthorized, fmt.Errorf("%s header is required", webhookTimestampHeader)
	}

	timestamp, parseErr := parseWebhookTimestamp(timestampHeader)
	if parseErr != nil {
		return inboundWebhookAuth{}, fiber.StatusUnauthorized, fmt.Errorf("%s must be unix seconds or RFC3339", webhookTimestampHeader)
	}

	tolerance := webhookSignatureTolerance()
	if tolerance <= 0 {
		tolerance = 5 * time.Minute
	}
	drift := time.Since(timestamp.UTC())
	if drift < 0 {
		drift = -drift
	}
	if drift > tolerance {
		return inboundWebhookAuth{}, fiber.StatusUnauthorized, fmt.Errorf("webhook timestamp outside allowed tolerance")
	}

	nonce := strings.TrimSpace(c.Get(webhookNonceHeader))
	if nonce == "" {
		return inboundWebhookAuth{}, fiber.StatusUnauthorized, fmt.Errorf("%s header is required", webhookNonceHeader)
	}
	if len(nonce) > 128 {
		return inboundWebhookAuth{}, fiber.StatusUnauthorized, fmt.Errorf("%s exceeds maximum length", webhookNonceHeader)
	}

	signature := strings.TrimSpace(c.Get(webhookSignatureVersionHeader))
	signature = strings.TrimSpace(strings.TrimPrefix(signature, webhookSignaturePrefix))
	if signature == "" {
		return inboundWebhookAuth{}, fiber.StatusUnauthorized, fmt.Errorf("%s header is required", webhookSignatureVersionHeader)
	}

	expected := computeWebhookSignature(secret, timestampHeader, nonce, body)
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return inboundWebhookAuth{}, fiber.StatusUnauthorized, fmt.Errorf("invalid webhook signature")
	}

	keyID := strings.TrimSpace(c.Get(webhookKeyIDHeader))
	configuredKeyID := webhookSigningKeyID()
	if configuredKeyID != "" && keyID != "" && configuredKeyID != keyID {
		return inboundWebhookAuth{}, fiber.StatusUnauthorized, fmt.Errorf("webhook key id mismatch")
	}

	return inboundWebhookAuth{
		Nonce:     nonce,
		Signature: signature,
		KeyID:     keyID,
		Timestamp: timestamp.UTC(),
	}, fiber.StatusOK, nil
}

func registerWebhookNonce(nonce string, receivedAt time.Time) (bool, error) {
	if database.DB == nil {
		return false, fmt.Errorf("database unavailable")
	}

	expiresAt := receivedAt.UTC().Add(webhookNonceTTL())
	if expiresAt.Before(receivedAt.UTC()) {
		expiresAt = receivedAt.UTC().Add(10 * time.Minute)
	}

	_ = database.DB.Where("expires_at < ?", receivedAt.UTC()).Delete(&models.WebhookReplayNonce{}).Error

	record := models.WebhookReplayNonce{
		Nonce:      nonce,
		ReceivedAt: receivedAt.UTC(),
		ExpiresAt:  expiresAt.UTC(),
	}

	result := database.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&record)
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func computeWebhookSignature(secret, timestamp, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("\n"))
	mac.Write([]byte(nonce))
	mac.Write([]byte("\n"))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func parseWebhookTimestamp(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}

	if unixSeconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Unix(unixSeconds, 0).UTC(), nil
	}

	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func generateWebhookNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func webhookSigningEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("WEBHOOK_SIGNING_ENABLED"))
	if raw == "" {
		return true
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return true
	}
}

func webhookSigningSecret() string {
	return strings.TrimSpace(os.Getenv("WEBHOOK_SIGNING_SECRET"))
}

func webhookSigningKeyID() string {
	return strings.TrimSpace(os.Getenv("WEBHOOK_SIGNING_KEY_ID"))
}

func webhookSignatureTolerance() time.Duration {
	raw := strings.TrimSpace(os.Getenv("WEBHOOK_SIGNATURE_TOLERANCE"))
	if raw == "" {
		return 5 * time.Minute
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 5 * time.Minute
	}
	return value
}

func webhookNonceTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv("WEBHOOK_NONCE_TTL"))
	if raw == "" {
		return 10 * time.Minute
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 10 * time.Minute
	}
	return value
}
