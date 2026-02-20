package routes

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/laserstream/api/auth"
	"github.com/laserstream/api/handlers"
	"github.com/laserstream/api/observability"
	"github.com/laserstream/api/ratelimit"
	"github.com/laserstream/api/stream"
)

type Options struct {
	AuthService          *auth.Service
	AllowPublicHealth    bool
	MetricsEnabled       bool
	EventsRateLimiter    *ratelimit.FixedWindowLimiter
	WSUpgradeRateLimiter *ratelimit.FixedWindowLimiter
}

func SetupRoutes(app *fiber.App, manager *stream.Manager, opts Options) {
	readAuth := roleMiddleware(opts.AuthService, auth.RoleRead)
	adminAuth := roleMiddleware(opts.AuthService, auth.RoleAdmin)

	api := app.Group("/api")
	if opts.AllowPublicHealth {
		api.Get("/health", handlers.Health)
		api.Get("/ready", handlers.Ready)
	} else {
		api.Get("/health", readAuth, handlers.Health)
		api.Get("/ready", readAuth, handlers.Ready)
	}

	api.Post("/idls", adminAuth, handlers.CreateIDL)
	api.Get("/idls", readAuth, handlers.GetIDLs)
	api.Get("/idls/:programID", readAuth, handlers.GetIDL)
	api.Put("/idls/:programID", adminAuth, handlers.UpdateIDL)
	api.Delete("/idls/:programID", adminAuth, handlers.DeleteIDL)

	api.Post("/agent/webhooks", adminAuth, handlers.CreateAgentWebhook)
	api.Get("/agent/webhooks", adminAuth, handlers.GetAgentWebhooks)
	api.Post("/agent/query", readAuth, handlers.AgentQuery)
	api.Post("/agent/query/report", readAuth, handlers.AgentQueryReport)
	api.Post("/agent/backtest/launches", readAuth, handlers.AgentBacktestLaunches)
	api.Post("/agent/backtest/launches/score", readAuth, handlers.AgentBacktestScoreLaunch)
	api.Post("/agent/automations", adminAuth, handlers.CreateAgentAutomation)
	api.Get("/agent/automations", adminAuth, handlers.GetAgentAutomations)
	api.Get("/agent/automations/:id/audit", adminAuth, handlers.GetAgentAutomationAuditLogs)
	api.Patch("/agent/automations/:id", adminAuth, handlers.UpdateAgentAutomation)
	api.Post("/agent/automations/:id/evaluate", adminAuth, handlers.EvaluateAgentAutomation)

	if opts.EventsRateLimiter != nil {
		api.Get("/events", readAuth, opts.EventsRateLimiter.Middleware(), handlers.GetEvents)
	} else {
		api.Get("/events", readAuth, handlers.GetEvents)
	}

	if opts.MetricsEnabled {
		app.Get("/metrics", readAuth, observability.Handler)
	}

	app.Use("/ws", readAuth)
	if opts.WSUpgradeRateLimiter != nil {
		app.Use("/ws", opts.WSUpgradeRateLimiter.Middleware())
	}
	app.Use("/ws", websocketUpgradeMiddleware())

	wsHandler := websocket.New(func(c *websocket.Conn) {
		if manager == nil {
			_ = c.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "stream manager unavailable"),
			)
			_ = c.Close()
			return
		}

		programID := strings.TrimSpace(c.Query("program_id"))
		if programID == "" {
			programID = strings.TrimSpace(c.Params("programID"))
		}

		if opts.AuthService != nil && opts.AuthService.Enabled() {
			rawPrincipal := c.Locals(auth.ContextPrincipalKey)
			principal, err := auth.ParsePrincipal(rawPrincipal)
			if err != nil {
				_ = c.WriteMessage(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "unauthorized"),
				)
				_ = c.Close()
				return
			}

			if err := auth.EnsureProgramAccessForPrincipal(principal, programID); err != nil {
				_ = c.WriteMessage(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "forbidden program subscription"),
				)
				_ = c.Close()
				return
			}
		}

		eventType := strings.TrimSpace(c.Query("event_type"))
		manager.Serve(c, programID, eventType)
	})

	app.Get("/ws", wsHandler)
	app.Get("/ws/:programID", wsHandler)
}

func roleMiddleware(service *auth.Service, role auth.Role) fiber.Handler {
	if service == nil {
		return func(c *fiber.Ctx) error {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "auth service unavailable",
			})
		}
	}
	return service.Middleware(role)
}

func websocketUpgradeMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	}
}
