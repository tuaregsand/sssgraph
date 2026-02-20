package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/config"
)

const (
	ContextPrincipalKey = "principal"
)

type Role string

const (
	RoleRead  Role = "read"
	RoleAdmin Role = "admin"
)

type Principal struct {
	Subject           string
	Role              Role
	AllowedPrograms   map[string]struct{}
	AuthenticatedBy   string
	TokenExpiresAtUTC *time.Time
}

type Service struct {
	enabled bool
	keys    map[string]Principal
	jwtKey  []byte
}

type jwtClaims struct {
	Sub      string   `json:"sub"`
	Role     string   `json:"role"`
	Programs []string `json:"programs"`
	Exp      int64    `json:"exp"`
}

func NewService(cfg config.AuthConfig) *Service {
	keys := make(map[string]Principal, len(cfg.APIKeys))
	for _, entry := range cfg.APIKeys {
		role := normalizeRole(entry.Role)
		allowed := toProgramMap(entry.AllowedPrograms)
		subject := strings.TrimSpace(entry.Subject)
		if subject == "" {
			subject = "api-key"
		}

		keys[entry.Key] = Principal{
			Subject:         subject,
			Role:            role,
			AllowedPrograms: allowed,
			AuthenticatedBy: "api_key",
		}
	}

	return &Service{
		enabled: cfg.Enabled,
		keys:    keys,
		jwtKey:  []byte(cfg.JWTSecret),
	}
}

func (s *Service) Enabled() bool {
	return s != nil && s.enabled
}

func (s *Service) Middleware(minRole Role) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if !s.Enabled() {
			return c.Next()
		}

		principal, err := s.Authenticate(c)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "unauthorized",
			})
		}

		if !hasRequiredRole(principal.Role, minRole) {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "forbidden",
			})
		}

		c.Locals(ContextPrincipalKey, principal)
		return c.Next()
	}
}

func (s *Service) Authenticate(c *fiber.Ctx) (*Principal, error) {
	authHeader := strings.TrimSpace(c.Get(fiber.HeaderAuthorization))
	if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		token := strings.TrimSpace(authHeader[7:])
		if token == "" {
			return nil, errors.New("empty bearer token")
		}
		return s.authenticateJWT(token)
	}

	apiKey := strings.TrimSpace(c.Get("X-API-Key"))
	if apiKey == "" {
		return nil, errors.New("missing credentials")
	}

	principal, ok := s.keys[apiKey]
	if !ok {
		return nil, errors.New("invalid api key")
	}

	copyPrincipal := principal
	return &copyPrincipal, nil
}

func (s *Service) authenticateJWT(token string) (*Principal, error) {
	if len(s.jwtKey) == 0 {
		return nil, errors.New("jwt secret is not configured")
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid jwt format")
	}

	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}

	var header map[string]any
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, err
	}

	alg, _ := header["alg"].(string)
	if !strings.EqualFold(alg, "HS256") {
		return nil, errors.New("unsupported jwt alg")
	}

	signed := parts[0] + "." + parts[1]
	expectedSig := signHS256(signed, s.jwtKey)

	receivedSig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, err
	}
	if !hmac.Equal(receivedSig, expectedSig) {
		return nil, errors.New("invalid jwt signature")
	}

	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}

	var claims jwtClaims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, err
	}

	if claims.Role == "" {
		return nil, errors.New("missing role claim")
	}
	if claims.Exp <= 0 {
		return nil, errors.New("missing exp claim")
	}
	exp := time.Unix(claims.Exp, 0).UTC()
	if time.Now().UTC().After(exp) {
		return nil, errors.New("token expired")
	}

	subject := strings.TrimSpace(claims.Sub)
	if subject == "" {
		subject = "jwt-user"
	}

	principal := &Principal{
		Subject:           subject,
		Role:              normalizeRole(claims.Role),
		AllowedPrograms:   toProgramMap(claims.Programs),
		AuthenticatedBy:   "jwt",
		TokenExpiresAtUTC: &exp,
	}
	if len(principal.AllowedPrograms) == 0 {
		principal.AllowedPrograms = map[string]struct{}{"*": {}}
	}

	return principal, nil
}

func GetPrincipal(c *fiber.Ctx) (*Principal, error) {
	return ParsePrincipal(c.Locals(ContextPrincipalKey))
}

func EnsureProgramAccess(c *fiber.Ctx, programID string) error {
	principal, err := GetPrincipal(c)
	if err != nil {
		return err
	}
	return EnsureProgramAccessForPrincipal(principal, programID)
}

func ParsePrincipal(raw any) (*Principal, error) {
	if raw == nil {
		return nil, errors.New("principal not found")
	}

	principal, ok := raw.(*Principal)
	if !ok || principal == nil {
		return nil, errors.New("invalid principal")
	}
	return principal, nil
}

func EnsureProgramAccessForPrincipal(principal *Principal, programID string) error {
	programID = strings.TrimSpace(programID)
	if programID == "" {
		return nil
	}
	if principal == nil {
		return errors.New("principal is nil")
	}

	if principal.Role == RoleAdmin {
		return nil
	}

	if _, ok := principal.AllowedPrograms["*"]; ok {
		return nil
	}
	if _, ok := principal.AllowedPrograms[programID]; ok {
		return nil
	}

	return fmt.Errorf("program access denied")
}

func FilterPrograms(programIDs []string, principal *Principal) []string {
	if principal == nil || principal.Role == RoleAdmin {
		return programIDs
	}
	if _, ok := principal.AllowedPrograms["*"]; ok {
		return programIDs
	}

	out := make([]string, 0, len(programIDs))
	for _, id := range programIDs {
		if _, ok := principal.AllowedPrograms[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

func normalizeRole(raw string) Role {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(RoleAdmin):
		return RoleAdmin
	default:
		return RoleRead
	}
}

func hasRequiredRole(actual, minimum Role) bool {
	if actual == RoleAdmin {
		return true
	}
	if minimum == RoleRead && actual == RoleRead {
		return true
	}
	return false
}

func toProgramMap(programs []string) map[string]struct{} {
	if len(programs) == 0 {
		return map[string]struct{}{"*": {}}
	}

	out := make(map[string]struct{}, len(programs))
	for _, program := range programs {
		program = strings.TrimSpace(program)
		if program == "" {
			continue
		}
		out[program] = struct{}{}
	}
	if len(out) == 0 {
		out["*"] = struct{}{}
	}
	return out
}

func signHS256(message string, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return mac.Sum(nil)
}
