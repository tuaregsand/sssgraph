package handlers

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/cache"
	"github.com/laserstream/api/database"
)

func Health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok"})
}

func Ready(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	problems := make([]string, 0, 2)
	if err := database.Ping(ctx); err != nil {
		problems = append(problems, "database unavailable")
	}
	if err := cache.Ping(ctx); err != nil {
		problems = append(problems, "redis unavailable")
	}
	if analytics.Client != nil {
		if err := analytics.Ping(ctx); err != nil {
			problems = append(problems, "clickhouse unavailable")
		}
	}

	if len(problems) > 0 {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"status":   "degraded",
			"problems": problems,
		})
	}

	return c.JSON(fiber.Map{"status": "ready"})
}
