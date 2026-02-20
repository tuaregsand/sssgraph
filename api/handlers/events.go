package handlers

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/auth"
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)
	base58Pattern     = regexp.MustCompile(`^[1-9A-HJ-NP-Za-km-z]+$`)
)

func GetEvents(c *fiber.Ctx) error {
	table, err := analytics.EventsTable()
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "clickhouse is not configured",
		})
	}

	if !identifierPattern.MatchString(table) {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "invalid clickhouse table configuration",
		})
	}

	programID := strings.TrimSpace(c.Query("program_id"))
	if programID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "program_id is required",
		})
	}
	if !base58Pattern.MatchString(programID) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "program_id must be base58",
		})
	}
	if principal, err := auth.GetPrincipal(c); err == nil {
		if err := auth.EnsureProgramAccessForPrincipal(principal, programID); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "forbidden for this program_id",
			})
		}
	}

	eventType := strings.TrimSpace(c.Query("event_type"))
	if eventType != "" && !identifierPattern.MatchString(eventType) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "event_type contains unsupported characters",
		})
	}

	limit := c.QueryInt("limit", 100)
	offset := c.QueryInt("offset", 0)
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}

	conditions := []string{
		fmt.Sprintf("program_id = '%s'", escapeSQLLiteral(programID)),
	}
	if eventType != "" {
		conditions = append(conditions, fmt.Sprintf("event_type = '%s'", escapeSQLLiteral(eventType)))
	}

	from := strings.TrimSpace(c.Query("from"))
	if from != "" {
		if _, err := time.Parse(time.RFC3339, from); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "from must be RFC3339 timestamp",
			})
		}
		conditions = append(conditions, fmt.Sprintf("timestamp >= parseDateTimeBestEffort('%s')", escapeSQLLiteral(from)))
	}

	to := strings.TrimSpace(c.Query("to"))
	if to != "" {
		if _, err := time.Parse(time.RFC3339, to); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "to must be RFC3339 timestamp",
			})
		}
		conditions = append(conditions, fmt.Sprintf("timestamp <= parseDateTimeBestEffort('%s')", escapeSQLLiteral(to)))
	}

	query := fmt.Sprintf(
		"SELECT timestamp, program_id, event_type, signature, payload FROM %s WHERE %s ORDER BY timestamp DESC LIMIT %d OFFSET %d FORMAT JSONEachRow",
		table,
		strings.Join(conditions, " AND "),
		limit,
		offset,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := analytics.QueryRows(ctx, query)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to query historical events",
		})
	}

	return c.JSON(fiber.Map{
		"items":  rows,
		"limit":  limit,
		"offset": offset,
		"count":  len(rows),
	})
}

func escapeSQLLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
