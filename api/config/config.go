package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port          string
	Database      DatabaseConfig
	ClickHouse    ClickHouseConfig
	Redis         RedisConfig
	WebSocket     WebSocketConfig
	Auth          AuthConfig
	Agent         AgentConfig
	RateLimit     RateLimitConfig
	Observability ObservabilityConfig
	Security      SecurityConfig
}

type DatabaseConfig struct {
	Host          string
	User          string
	Password      string
	Name          string
	Port          string
	SSLMode       string
	SSLRootCert   string
	SSLCert       string
	SSLKey        string
	TimeZone      string
	RunMigrations bool
	RequireSchema bool
}

func (c DatabaseConfig) DSN() string {
	parts := []string{
		fmt.Sprintf("host=%s", c.Host),
		fmt.Sprintf("user=%s", c.User),
		fmt.Sprintf("password=%s", c.Password),
		fmt.Sprintf("dbname=%s", c.Name),
		fmt.Sprintf("port=%s", c.Port),
		fmt.Sprintf("sslmode=%s", c.SSLMode),
		fmt.Sprintf("TimeZone=%s", c.TimeZone),
	}
	if strings.TrimSpace(c.SSLRootCert) != "" {
		parts = append(parts, fmt.Sprintf("sslrootcert=%s", c.SSLRootCert))
	}
	if strings.TrimSpace(c.SSLCert) != "" {
		parts = append(parts, fmt.Sprintf("sslcert=%s", c.SSLCert))
	}
	if strings.TrimSpace(c.SSLKey) != "" {
		parts = append(parts, fmt.Sprintf("sslkey=%s", c.SSLKey))
	}
	return strings.Join(parts, " ")
}

type RedisConfig struct {
	Addr                  string
	Password              string
	DB                    int
	ChannelPrefix         string
	UseTLS                bool
	TLSServerName         string
	TLSInsecureSkipVerify bool
}

type ClickHouseConfig struct {
	URL      string
	Database string
	Table    string
	User     string
	Password string
	Timeout  time.Duration
}

type WebSocketConfig struct {
	ClientBuffer        int
	PingPeriod          time.Duration
	PongWait            time.Duration
	WriteTimeout        time.Duration
	MaxConnections      int
	MaxConnectionsPerIP int
}

type AuthConfig struct {
	Enabled           bool
	AllowPublicHealth bool
	JWTSecret         string
	APIKeys           []APIKeyConfig
}

type AgentConfig struct {
	AutomationsEnabled       bool
	AutomationPollInterval   time.Duration
	AutomationEvalTimeout    time.Duration
	AutomationWebhookTimeout time.Duration
}

type APIKeyConfig struct {
	Key             string
	Subject         string
	Role            string
	AllowedPrograms []string
}

type RateLimitConfig struct {
	Enabled                  bool
	EventsPerMinutePerIP     int
	WSUpgradesPerMinutePerIP int
}

type ObservabilityConfig struct {
	EnableMetrics bool
}

type SecurityConfig struct {
	RequireSecureBackends bool
}

func Load() Config {
	cfg := Config{
		Port: getEnv("PORT", "3000"),
		Database: DatabaseConfig{
			Host:          getEnv("DB_HOST", "localhost"),
			User:          getEnv("DB_USER", "postgres"),
			Password:      getEnv("DB_PASSWORD", "postgres"),
			Name:          getEnv("DB_NAME", "laserstream"),
			Port:          getEnv("DB_PORT", "5432"),
			SSLMode:       getEnv("DB_SSLMODE", "require"),
			SSLRootCert:   getEnv("DB_SSLROOTCERT", ""),
			SSLCert:       getEnv("DB_SSLCERT", ""),
			SSLKey:        getEnv("DB_SSLKEY", ""),
			TimeZone:      getEnv("DB_TIMEZONE", "UTC"),
			RunMigrations: getBoolEnv("DB_RUN_MIGRATIONS", true),
			RequireSchema: getBoolEnv("DB_REQUIRE_SCHEMA", true),
		},
		ClickHouse: ClickHouseConfig{
			URL:      getEnv("CLICKHOUSE_HTTP_URL", ""),
			Database: getEnv("CLICKHOUSE_DATABASE", "default"),
			Table:    getEnv("CLICKHOUSE_TABLE", "solana_events"),
			User:     getEnv("CLICKHOUSE_USER", ""),
			Password: getEnv("CLICKHOUSE_PASSWORD", ""),
			Timeout:  getDurationEnv("CLICKHOUSE_TIMEOUT", 5*time.Second),
		},
		Redis: RedisConfig{
			Addr:                  getEnv("REDIS_ADDR", "127.0.0.1:6379"),
			Password:              getEnv("REDIS_PASSWORD", ""),
			DB:                    getIntEnv("REDIS_DB", 0),
			ChannelPrefix:         getEnv("REDIS_CHANNEL_PREFIX", "events"),
			UseTLS:                getBoolEnv("REDIS_TLS", true),
			TLSServerName:         getEnv("REDIS_TLS_SERVER_NAME", ""),
			TLSInsecureSkipVerify: getBoolEnv("REDIS_TLS_INSECURE_SKIP_VERIFY", false),
		},
		WebSocket: WebSocketConfig{
			ClientBuffer:        getIntEnv("WS_CLIENT_BUFFER", 256),
			PingPeriod:          getDurationEnv("WS_PING_PERIOD", 30*time.Second),
			PongWait:            getDurationEnv("WS_PONG_WAIT", 60*time.Second),
			WriteTimeout:        getDurationEnv("WS_WRITE_TIMEOUT", 10*time.Second),
			MaxConnections:      getIntEnv("WS_MAX_CONNECTIONS", 20000),
			MaxConnectionsPerIP: getIntEnv("WS_MAX_CONNECTIONS_PER_IP", 200),
		},
		Auth: AuthConfig{
			Enabled:           getBoolEnv("AUTH_ENABLED", true),
			AllowPublicHealth: getBoolEnv("AUTH_ALLOW_PUBLIC_HEALTH", false),
			JWTSecret:         getEnv("AUTH_JWT_HS256_SECRET", ""),
			APIKeys:           parseAPIKeys(getEnv("AUTH_API_KEYS", "")),
		},
		Agent: AgentConfig{
			AutomationsEnabled:       getBoolEnv("AGENT_AUTOMATIONS_ENABLED", true),
			AutomationPollInterval:   getDurationEnv("AGENT_AUTOMATIONS_POLL_INTERVAL", time.Minute),
			AutomationEvalTimeout:    getDurationEnv("AGENT_AUTOMATIONS_EVAL_TIMEOUT", 10*time.Second),
			AutomationWebhookTimeout: getDurationEnv("AGENT_AUTOMATIONS_WEBHOOK_TIMEOUT", 5*time.Second),
		},
		RateLimit: RateLimitConfig{
			Enabled:                  getBoolEnv("RATE_LIMIT_ENABLED", true),
			EventsPerMinutePerIP:     getIntEnv("RATE_LIMIT_EVENTS_PER_MINUTE_PER_IP", 120),
			WSUpgradesPerMinutePerIP: getIntEnv("RATE_LIMIT_WS_UPGRADES_PER_MINUTE_PER_IP", 60),
		},
		Observability: ObservabilityConfig{
			EnableMetrics: getBoolEnv("METRICS_ENABLED", true),
		},
		Security: SecurityConfig{
			RequireSecureBackends: getBoolEnv("REQUIRE_SECURE_BACKENDS", true),
		},
	}

	return cfg
}

func (c Config) Validate() error {
	if c.Auth.Enabled {
		if c.Auth.JWTSecret == "" && len(c.Auth.APIKeys) == 0 {
			return errors.New("auth is enabled but no credentials configured (set AUTH_JWT_HS256_SECRET and/or AUTH_API_KEYS)")
		}
	}

	if c.Security.RequireSecureBackends {
		if strings.EqualFold(c.Database.SSLMode, "disable") {
			return errors.New("REQUIRE_SECURE_BACKENDS is enabled but DB_SSLMODE=disable")
		}
		if c.ClickHouse.URL != "" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(c.ClickHouse.URL)), "http://") {
			return errors.New("REQUIRE_SECURE_BACKENDS is enabled but CLICKHOUSE_HTTP_URL is not https")
		}
		if c.Redis.Addr != "" && !c.Redis.UseTLS {
			return errors.New("REQUIRE_SECURE_BACKENDS is enabled but REDIS_TLS=false")
		}
	}

	if c.WebSocket.MaxConnections <= 0 {
		return errors.New("WS_MAX_CONNECTIONS must be > 0")
	}
	if c.WebSocket.MaxConnectionsPerIP <= 0 {
		return errors.New("WS_MAX_CONNECTIONS_PER_IP must be > 0")
	}

	if c.RateLimit.EventsPerMinutePerIP <= 0 || c.RateLimit.WSUpgradesPerMinutePerIP <= 0 {
		return errors.New("rate limits must be > 0")
	}

	if c.Agent.AutomationsEnabled {
		if c.Agent.AutomationPollInterval <= 0 {
			return errors.New("AGENT_AUTOMATIONS_POLL_INTERVAL must be > 0")
		}
		if c.Agent.AutomationEvalTimeout <= 0 {
			return errors.New("AGENT_AUTOMATIONS_EVAL_TIMEOUT must be > 0")
		}
		if c.Agent.AutomationWebhookTimeout <= 0 {
			return errors.New("AGENT_AUTOMATIONS_WEBHOOK_TIMEOUT must be > 0")
		}
	}

	return nil
}

func parseAPIKeys(raw string) []APIKeyConfig {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	records := strings.Split(raw, ";")
	keys := make([]APIKeyConfig, 0, len(records))
	for _, record := range records {
		record = strings.TrimSpace(record)
		if record == "" {
			continue
		}

		parts := strings.Split(record, "|")
		if len(parts) < 3 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		subject := strings.TrimSpace(parts[1])
		role := strings.TrimSpace(parts[2])
		if key == "" || role == "" {
			continue
		}

		allowedPrograms := []string{"*"}
		if len(parts) >= 4 {
			allowedPrograms = parseCSVString(parts[3])
			if len(allowedPrograms) == 0 {
				allowedPrograms = []string{"*"}
			}
		}

		keys = append(keys, APIKeyConfig{
			Key:             key,
			Subject:         subject,
			Role:            role,
			AllowedPrograms: allowedPrograms,
		})
	}

	return keys
}

func parseCSVString(raw string) []string {
	values := strings.Split(raw, ",")
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}
	return result
}

func getBoolEnv(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}

	switch strings.ToLower(raw) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getIntEnv(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}

	return value
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}

	return value
}
