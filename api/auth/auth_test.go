package auth

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/config"
)

func TestAPIKeyMiddlewareAllowsValidKey(t *testing.T) {
	service := NewService(config.AuthConfig{
		Enabled: true,
		APIKeys: []config.APIKeyConfig{
			{
				Key:             "test-key",
				Subject:         "svc",
				Role:            "read",
				AllowedPrograms: []string{"*"},
			},
		},
	})

	app := fiber.New()
	app.Get("/", service.Middleware(RoleRead), func(c *fiber.Ctx) error {
		principal, err := GetPrincipal(c)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		}
		return c.SendString(principal.Subject)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "test-key")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("expected request to succeed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestJWTMiddlewareRejectsExpiredToken(t *testing.T) {
	service := NewService(config.AuthConfig{
		Enabled:   true,
		JWTSecret: "secret",
	})

	app := fiber.New()
	app.Get("/", service.Middleware(RoleRead), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusNoContent)
	})

	token := mustBuildJWT(t, "secret", jwtClaims{
		Sub:      "user-1",
		Role:     "read",
		Programs: []string{"*"},
		Exp:      time.Now().Add(-time.Minute).Unix(),
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("expected request to complete: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected status 401 for expired token, got %d", resp.StatusCode)
	}
}

func TestEnsureProgramAccessForPrincipal(t *testing.T) {
	principal := &Principal{
		Subject: "reader",
		Role:    RoleRead,
		AllowedPrograms: map[string]struct{}{
			"ProgramA": {},
		},
	}

	if err := EnsureProgramAccessForPrincipal(principal, "ProgramA"); err != nil {
		t.Fatalf("expected access to ProgramA, got error: %v", err)
	}
	if err := EnsureProgramAccessForPrincipal(principal, "ProgramB"); err == nil {
		t.Fatal("expected access denial for ProgramB")
	}
}

func mustBuildJWT(t *testing.T, secret string, claims jwtClaims) string {
	t.Helper()

	header := map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	}

	headerBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("failed to marshal jwt header: %v", err)
	}
	payloadBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("failed to marshal jwt claims: %v", err)
	}

	encodedHeader := base64.RawURLEncoding.EncodeToString(headerBytes)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payloadBytes)
	unsigned := encodedHeader + "." + encodedPayload
	signature := signHS256(unsigned, []byte(secret))
	encodedSignature := base64.RawURLEncoding.EncodeToString(signature)

	return unsigned + "." + encodedSignature
}
