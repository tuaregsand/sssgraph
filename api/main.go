package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/joho/godotenv"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/auth"
	"github.com/laserstream/api/cache"
	"github.com/laserstream/api/config"
	"github.com/laserstream/api/database"
	"github.com/laserstream/api/handlers"
	"github.com/laserstream/api/observability"
	"github.com/laserstream/api/ratelimit"
	"github.com/laserstream/api/routes"
	"github.com/laserstream/api/stream"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	if err := database.ConnectDB(cfg.Database); err != nil {
		log.Fatal(err)
	}

	if err := analytics.Connect(analytics.Config{
		URL:      cfg.ClickHouse.URL,
		Database: cfg.ClickHouse.Database,
		Table:    cfg.ClickHouse.Table,
		User:     cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
		Timeout:  cfg.ClickHouse.Timeout,
	}); err != nil {
		log.Fatal(err)
	}

	if err := cache.Connect(cache.Config{
		Addr:                  cfg.Redis.Addr,
		Password:              cfg.Redis.Password,
		DB:                    cfg.Redis.DB,
		UseTLS:                cfg.Redis.UseTLS,
		TLSServerName:         cfg.Redis.TLSServerName,
		TLSInsecureSkipVerify: cfg.Redis.TLSInsecureSkipVerify,
	}); err != nil {
		log.Fatal(err)
	}

	wsManager := stream.NewManager(cache.Client, stream.Options{
		ChannelPrefix:       cfg.Redis.ChannelPrefix,
		ClientBuffer:        cfg.WebSocket.ClientBuffer,
		PingPeriod:          cfg.WebSocket.PingPeriod,
		PongWait:            cfg.WebSocket.PongWait,
		WriteTimeout:        cfg.WebSocket.WriteTimeout,
		MaxConnections:      cfg.WebSocket.MaxConnections,
		MaxConnectionsPerIP: cfg.WebSocket.MaxConnectionsPerIP,
	})
	authService := auth.NewService(cfg.Auth)

	var eventsLimiter *ratelimit.FixedWindowLimiter
	var wsLimiter *ratelimit.FixedWindowLimiter
	if cfg.RateLimit.Enabled {
		eventsLimiter = ratelimit.NewFixedWindowLimiter(cfg.RateLimit.EventsPerMinutePerIP, time.Minute)
		wsLimiter = ratelimit.NewFixedWindowLimiter(cfg.RateLimit.WSUpgradesPerMinutePerIP, time.Minute)
	}

	var automationRunner *handlers.AgentAutomationRunner
	if cfg.Agent.AutomationsEnabled {
		automationRunner = handlers.NewAgentAutomationRunner(
			cfg.Agent.AutomationPollInterval,
			cfg.Agent.AutomationEvalTimeout,
			cfg.Agent.AutomationWebhookTimeout,
		)
		automationRunner.Start()
		log.Println("Agent automation runner started")
	}

	var automationRetentionRunner *handlers.AgentAutomationRetentionRunner
	if cfg.Agent.AuditRetentionEnabled {
		automationRetentionRunner = handlers.NewAgentAutomationRetentionRunner(
			cfg.Agent.AuditCleanupInterval,
			cfg.Agent.AuditRetention,
		)
		automationRetentionRunner.Start()
		log.Println("Agent automation retention runner started")
	}

	app := fiber.New()
	app.Use(requestid.New())
	app.Use(logger.New())
	app.Use(observability.Middleware())
	routes.SetupRoutes(app, wsManager, routes.Options{
		AuthService:          authService,
		AllowPublicHealth:    cfg.Auth.AllowPublicHealth,
		MetricsEnabled:       cfg.Observability.EnableMetrics,
		EventsRateLimiter:    eventsLimiter,
		WSUpgradeRateLimiter: wsLimiter,
	})

	go handleShutdown(app, wsManager, automationRunner, automationRetentionRunner)

	log.Printf("API listening on :%s", cfg.Port)
	log.Fatal(app.Listen(":" + cfg.Port))
}

func handleShutdown(
	app *fiber.App,
	wsManager *stream.Manager,
	automationRunner *handlers.AgentAutomationRunner,
	automationRetentionRunner *handlers.AgentAutomationRetentionRunner,
) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down API service...")
	if automationRunner != nil {
		automationRunner.Stop()
	}
	if automationRetentionRunner != nil {
		automationRetentionRunner.Stop()
	}
	wsManager.Close()

	if err := cache.Close(); err != nil {
		log.Printf("cache close error: %v", err)
	}

	if err := database.Close(); err != nil {
		log.Printf("database close error: %v", err)
	}

	if err := app.Shutdown(); err != nil {
		log.Printf("app shutdown error: %v", err)
	}
}
