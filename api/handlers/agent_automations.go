package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/auth"
	"github.com/laserstream/api/database"
	"github.com/laserstream/api/models"
	"github.com/laserstream/api/observability"
)

const (
	defaultAutomationScheduleMinutes = 60
	defaultAutomationWindowMinutes   = 60
	defaultAutomationCooldownMinutes = 30
	defaultAutomationDedupeMinutes   = 15
	defaultAutomationRetryAttempts   = 3
	defaultAutomationRetryBackoffMS  = 200
	maxAutomationWindowMinutes       = 60 * 24 * 14
	maxAutomationDedupeMinutes       = 60 * 24 * 14
	maxAutomationRetryAttempts       = 10
	maxAutomationRetryBackoffMS      = 30_000
)

type createAgentAutomationPayload struct {
	Name                  string  `json:"name"`
	Description           string  `json:"description"`
	ProgramID             string  `json:"program_id"`
	QueryTemplate         string  `json:"query_template"`
	Comparator            string  `json:"comparator"`
	Threshold             float64 `json:"threshold"`
	ScheduleMinutes       int     `json:"schedule_minutes"`
	WindowMinutes         int     `json:"window_minutes"`
	CooldownMinutes       int     `json:"cooldown_minutes"`
	DedupeMinutes         int     `json:"dedupe_minutes"`
	RetryMaxAttempts      int     `json:"retry_max_attempts"`
	RetryInitialBackoffMS int     `json:"retry_initial_backoff_ms"`
	WebhookURL            string  `json:"webhook_url"`
	IsActive              *bool   `json:"is_active"`
	DryRun                *bool   `json:"dry_run"`
}

type updateAgentAutomationPayload struct {
	Name                  *string  `json:"name"`
	Description           *string  `json:"description"`
	ProgramID             *string  `json:"program_id"`
	QueryTemplate         *string  `json:"query_template"`
	Comparator            *string  `json:"comparator"`
	Threshold             *float64 `json:"threshold"`
	ScheduleMinutes       *int     `json:"schedule_minutes"`
	WindowMinutes         *int     `json:"window_minutes"`
	CooldownMinutes       *int     `json:"cooldown_minutes"`
	DedupeMinutes         *int     `json:"dedupe_minutes"`
	RetryMaxAttempts      *int     `json:"retry_max_attempts"`
	RetryInitialBackoffMS *int     `json:"retry_initial_backoff_ms"`
	WebhookURL            *string  `json:"webhook_url"`
	IsActive              *bool    `json:"is_active"`
	DryRun                *bool    `json:"dry_run"`
}

type automationEvaluation struct {
	AutomationID  uint      `json:"automation_id"`
	Name          string    `json:"name"`
	ProgramID     string    `json:"program_id"`
	Value         float64   `json:"value"`
	Threshold     float64   `json:"threshold"`
	Comparator    string    `json:"comparator"`
	Triggered     bool      `json:"triggered"`
	AlertSent     bool      `json:"alert_sent"`
	DeliveryState string    `json:"delivery_state"`
	Query         string    `json:"query"`
	WindowFrom    string    `json:"window_from"`
	WindowTo      string    `json:"window_to"`
	Reason        string    `json:"reason"`
	DryRun        bool      `json:"dry_run"`
	DedupeKey     string    `json:"dedupe_key,omitempty"`
	RetryAttempts int       `json:"retry_attempts"`
	Error         string    `json:"error,omitempty"`
	EvaluatedAt   time.Time `json:"evaluated_at"`
}

func CreateAgentAutomation(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	var payload createAgentAutomationPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	automation, err := normalizeCreateAutomationPayload(c, payload)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	if err := database.DB.Create(&automation).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create automation"})
	}

	return c.Status(fiber.StatusCreated).JSON(automation)
}

func GetAgentAutomations(c *fiber.Ctx) error {
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

	var automations []models.AgentAutomation
	if err := database.DB.Order("updated_at desc").Limit(limit).Offset(offset).Find(&automations).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch automations"})
	}

	if principal, err := auth.GetPrincipal(c); err == nil {
		filtered := make([]models.AgentAutomation, 0, len(automations))
		for _, automation := range automations {
			if automation.ProgramID == "" || auth.EnsureProgramAccessForPrincipal(principal, automation.ProgramID) == nil {
				filtered = append(filtered, automation)
			}
		}
		automations = filtered
	}

	return c.JSON(fiber.Map{
		"items":  automations,
		"limit":  limit,
		"offset": offset,
		"count":  len(automations),
	})
}

func GetAgentAutomationAuditLogs(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	id, err := strconv.ParseUint(strings.TrimSpace(c.Params("id")), 10, 64)
	if err != nil || id == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid automation id"})
	}

	var automation models.AgentAutomation
	if err := database.DB.First(&automation, "id = ?", id).Error; err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "automation not found"})
	}

	if principal, err := auth.GetPrincipal(c); err == nil {
		if automation.ProgramID != "" && auth.EnsureProgramAccessForPrincipal(principal, automation.ProgramID) != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "forbidden for this program_id"})
		}
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

	var logs []models.AgentAutomationEvaluation
	if err := database.DB.
		Where("automation_id = ?", id).
		Order("evaluated_at desc").
		Limit(limit).
		Offset(offset).
		Find(&logs).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch automation audit logs"})
	}

	return c.JSON(fiber.Map{
		"automation_id": automation.ID,
		"items":         logs,
		"limit":         limit,
		"offset":        offset,
		"count":         len(logs),
	})
}

func UpdateAgentAutomation(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	id, err := strconv.ParseUint(strings.TrimSpace(c.Params("id")), 10, 64)
	if err != nil || id == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid automation id"})
	}

	var automation models.AgentAutomation
	if err := database.DB.First(&automation, "id = ?", id).Error; err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "automation not found"})
	}

	var payload updateAgentAutomationPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	if err := applyAutomationUpdatePayload(c, &automation, payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	if err := database.DB.Save(&automation).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update automation"})
	}

	return c.JSON(automation)
}

func EvaluateAgentAutomation(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	id, err := strconv.ParseUint(strings.TrimSpace(c.Params("id")), 10, 64)
	if err != nil || id == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid automation id"})
	}

	var automation models.AgentAutomation
	if err := database.DB.First(&automation, "id = ?", id).Error; err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "automation not found"})
	}

	if principal, err := auth.GetPrincipal(c); err == nil {
		if automation.ProgramID != "" && auth.EnsureProgramAccessForPrincipal(principal, automation.ProgramID) != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "forbidden for this program_id"})
		}
	}

	sendWebhook := true
	if raw := strings.TrimSpace(c.Query("send_webhook")); raw != "" {
		parsed, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "send_webhook must be a boolean"})
		}
		sendWebhook = parsed
	}

	dryRun := automation.DryRun
	if raw := strings.TrimSpace(c.Query("dry_run")); raw != "" {
		parsed, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "dry_run must be a boolean"})
		}
		dryRun = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	observability.AgentAutomationEvaluation()
	evaluation, evalErr := evaluateAutomation(ctx, &automation, now, sendWebhook, dryRun, 5*time.Second)
	if evalErr != nil {
		observability.AgentAutomationFailure("evaluation_error")
		automation.LastError = evalErr.Error()
		automation.LastEvaluatedAt = &now
		_ = database.DB.Save(&automation).Error
		_ = persistAutomationFailureAudit(&automation, now, evalErr)

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":      "automation evaluation failed",
			"automation": automation,
		})
	}
	if strings.TrimSpace(evaluation.Error) != "" {
		observability.AgentAutomationFailure("delivery_error")
	}

	persistAutomationEvaluation(&automation, evaluation, evaluation.Error)
	if err := database.DB.Save(&automation).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":      "automation evaluated but failed to persist state",
			"evaluation": evaluation,
		})
	}
	_ = persistAutomationAudit(&automation, evaluation)

	return c.JSON(fiber.Map{
		"automation": automation,
		"evaluation": evaluation,
	})
}

func normalizeCreateAutomationPayload(c *fiber.Ctx, payload createAgentAutomationPayload) (models.AgentAutomation, error) {
	payload.Name = strings.TrimSpace(payload.Name)
	payload.Description = strings.TrimSpace(payload.Description)
	payload.ProgramID = strings.TrimSpace(payload.ProgramID)
	payload.QueryTemplate = strings.TrimSpace(payload.QueryTemplate)
	payload.Comparator = strings.ToLower(strings.TrimSpace(payload.Comparator))
	payload.WebhookURL = strings.TrimSpace(payload.WebhookURL)

	if payload.Name == "" {
		return models.AgentAutomation{}, fmt.Errorf("name is required")
	}
	if payload.QueryTemplate == "" {
		return models.AgentAutomation{}, fmt.Errorf("query_template is required")
	}
	if payload.Comparator == "" {
		payload.Comparator = "lt"
	}
	if !isSupportedComparator(payload.Comparator) {
		return models.AgentAutomation{}, fmt.Errorf("comparator must be one of: gt, gte, lt, lte")
	}
	if payload.ProgramID != "" {
		if !base58Pattern.MatchString(payload.ProgramID) {
			return models.AgentAutomation{}, fmt.Errorf("program_id must be base58")
		}
		if principal, err := auth.GetPrincipal(c); err == nil {
			if err := auth.EnsureProgramAccessForPrincipal(principal, payload.ProgramID); err != nil {
				return models.AgentAutomation{}, fmt.Errorf("forbidden for this program_id")
			}
		}
	}
	if payload.WebhookURL == "" {
		return models.AgentAutomation{}, fmt.Errorf("webhook_url is required")
	}
	if err := validateWebhookURL(payload.WebhookURL); err != nil {
		return models.AgentAutomation{}, err
	}

	payload.ScheduleMinutes = normalizeDurationMinutes(payload.ScheduleMinutes, defaultAutomationScheduleMinutes)
	payload.WindowMinutes = normalizeDurationMinutes(payload.WindowMinutes, defaultAutomationWindowMinutes)
	payload.CooldownMinutes = normalizeDurationMinutes(payload.CooldownMinutes, defaultAutomationCooldownMinutes)
	payload.DedupeMinutes = normalizeDurationMinutes(payload.DedupeMinutes, defaultAutomationDedupeMinutes)
	payload.RetryMaxAttempts = normalizeDurationMinutes(payload.RetryMaxAttempts, defaultAutomationRetryAttempts)
	payload.RetryInitialBackoffMS = normalizeDurationMinutes(payload.RetryInitialBackoffMS, defaultAutomationRetryBackoffMS)
	if payload.WindowMinutes > maxAutomationWindowMinutes {
		return models.AgentAutomation{}, fmt.Errorf("window_minutes cannot exceed %d", maxAutomationWindowMinutes)
	}
	if payload.DedupeMinutes > maxAutomationDedupeMinutes {
		return models.AgentAutomation{}, fmt.Errorf("dedupe_minutes cannot exceed %d", maxAutomationDedupeMinutes)
	}
	if payload.RetryMaxAttempts > maxAutomationRetryAttempts {
		return models.AgentAutomation{}, fmt.Errorf("retry_max_attempts cannot exceed %d", maxAutomationRetryAttempts)
	}
	if payload.RetryInitialBackoffMS > maxAutomationRetryBackoffMS {
		return models.AgentAutomation{}, fmt.Errorf("retry_initial_backoff_ms cannot exceed %d", maxAutomationRetryBackoffMS)
	}

	if _, err := validateAutomationQueryTemplate(payload.QueryTemplate, payload.ProgramID, payload.WindowMinutes); err != nil {
		return models.AgentAutomation{}, err
	}

	isActive := true
	if payload.IsActive != nil {
		isActive = *payload.IsActive
	}
	dryRun := false
	if payload.DryRun != nil {
		dryRun = *payload.DryRun
	}

	return models.AgentAutomation{
		Name:                  payload.Name,
		Description:           payload.Description,
		ProgramID:             payload.ProgramID,
		QueryTemplate:         payload.QueryTemplate,
		Comparator:            payload.Comparator,
		Threshold:             payload.Threshold,
		ScheduleMinutes:       payload.ScheduleMinutes,
		WindowMinutes:         payload.WindowMinutes,
		CooldownMinutes:       payload.CooldownMinutes,
		DedupeMinutes:         payload.DedupeMinutes,
		RetryMaxAttempts:      payload.RetryMaxAttempts,
		RetryInitialBackoffMS: payload.RetryInitialBackoffMS,
		WebhookURL:            payload.WebhookURL,
		IsActive:              isActive,
		DryRun:                dryRun,
	}, nil
}

func applyAutomationUpdatePayload(c *fiber.Ctx, automation *models.AgentAutomation, payload updateAgentAutomationPayload) error {
	if automation == nil {
		return fmt.Errorf("automation payload cannot be nil")
	}

	if payload.Name != nil {
		name := strings.TrimSpace(*payload.Name)
		if name == "" {
			return fmt.Errorf("name cannot be empty")
		}
		automation.Name = name
	}

	if payload.Description != nil {
		automation.Description = strings.TrimSpace(*payload.Description)
	}

	if payload.ProgramID != nil {
		programID := strings.TrimSpace(*payload.ProgramID)
		if programID != "" && !base58Pattern.MatchString(programID) {
			return fmt.Errorf("program_id must be base58")
		}
		if principal, err := auth.GetPrincipal(c); err == nil {
			if programID != "" && auth.EnsureProgramAccessForPrincipal(principal, programID) != nil {
				return fmt.Errorf("forbidden for this program_id")
			}
		}
		automation.ProgramID = programID
	}

	if payload.Comparator != nil {
		comparator := strings.ToLower(strings.TrimSpace(*payload.Comparator))
		if !isSupportedComparator(comparator) {
			return fmt.Errorf("comparator must be one of: gt, gte, lt, lte")
		}
		automation.Comparator = comparator
	}

	if payload.Threshold != nil {
		automation.Threshold = *payload.Threshold
	}

	if payload.ScheduleMinutes != nil {
		automation.ScheduleMinutes = normalizeDurationMinutes(*payload.ScheduleMinutes, defaultAutomationScheduleMinutes)
	}

	if payload.WindowMinutes != nil {
		automation.WindowMinutes = normalizeDurationMinutes(*payload.WindowMinutes, defaultAutomationWindowMinutes)
	}

	if payload.CooldownMinutes != nil {
		automation.CooldownMinutes = normalizeDurationMinutes(*payload.CooldownMinutes, defaultAutomationCooldownMinutes)
	}
	if payload.DedupeMinutes != nil {
		automation.DedupeMinutes = normalizeDurationMinutes(*payload.DedupeMinutes, defaultAutomationDedupeMinutes)
	}
	if payload.RetryMaxAttempts != nil {
		automation.RetryMaxAttempts = normalizeDurationMinutes(*payload.RetryMaxAttempts, defaultAutomationRetryAttempts)
	}
	if payload.RetryInitialBackoffMS != nil {
		automation.RetryInitialBackoffMS = normalizeDurationMinutes(*payload.RetryInitialBackoffMS, defaultAutomationRetryBackoffMS)
	}

	if automation.WindowMinutes > maxAutomationWindowMinutes {
		return fmt.Errorf("window_minutes cannot exceed %d", maxAutomationWindowMinutes)
	}
	if automation.DedupeMinutes > maxAutomationDedupeMinutes {
		return fmt.Errorf("dedupe_minutes cannot exceed %d", maxAutomationDedupeMinutes)
	}
	if automation.RetryMaxAttempts > maxAutomationRetryAttempts {
		return fmt.Errorf("retry_max_attempts cannot exceed %d", maxAutomationRetryAttempts)
	}
	if automation.RetryInitialBackoffMS > maxAutomationRetryBackoffMS {
		return fmt.Errorf("retry_initial_backoff_ms cannot exceed %d", maxAutomationRetryBackoffMS)
	}

	if payload.WebhookURL != nil {
		url := strings.TrimSpace(*payload.WebhookURL)
		if url == "" {
			return fmt.Errorf("webhook_url cannot be empty")
		}
		if err := validateWebhookURL(url); err != nil {
			return err
		}
		automation.WebhookURL = url
	}

	if payload.QueryTemplate != nil {
		queryTemplate := strings.TrimSpace(*payload.QueryTemplate)
		if queryTemplate == "" {
			return fmt.Errorf("query_template cannot be empty")
		}
		automation.QueryTemplate = queryTemplate
	}

	if payload.IsActive != nil {
		automation.IsActive = *payload.IsActive
	}
	if payload.DryRun != nil {
		automation.DryRun = *payload.DryRun
	}

	if _, err := validateAutomationQueryTemplate(automation.QueryTemplate, automation.ProgramID, automation.WindowMinutes); err != nil {
		return err
	}

	return nil
}

func normalizeDurationMinutes(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func validateAutomationQueryTemplate(template, programID string, windowMinutes int) (string, error) {
	to := time.Now().UTC()
	from := to.Add(-time.Duration(windowMinutes) * time.Minute)
	rendered := renderAutomationQuery(template, programID, from, to)
	readOnlySQL, err := normalizeReadOnlySQL(rendered)
	if err != nil {
		return "", fmt.Errorf("query_template is invalid: %w", err)
	}
	return readOnlySQL, nil
}

func persistAutomationEvaluation(automation *models.AgentAutomation, evaluation automationEvaluation, lastError string) {
	now := evaluation.EvaluatedAt
	automation.LastEvaluatedAt = &now
	automation.LastValue = &evaluation.Value
	automation.LastError = strings.TrimSpace(lastError)
	if evaluation.AlertSent {
		automation.LastTriggeredAt = &now
		automation.LastTriggerFingerprint = evaluation.DedupeKey
	}
}

func evaluateAutomation(
	ctx context.Context,
	automation *models.AgentAutomation,
	now time.Time,
	sendWebhook bool,
	dryRun bool,
	webhookTimeout time.Duration,
) (automationEvaluation, error) {
	from := now.Add(-time.Duration(automation.WindowMinutes) * time.Minute)
	rendered := renderAutomationQuery(automation.QueryTemplate, automation.ProgramID, from, now)
	readOnlySQL, err := normalizeReadOnlySQL(rendered)
	if err != nil {
		return automationEvaluation{}, fmt.Errorf("query validation failed: %w", err)
	}

	rows, err := analytics.QueryRows(ctx, wrapQueryForJSONRows(readOnlySQL, 1))
	if err != nil {
		return automationEvaluation{}, fmt.Errorf("query execution failed: %w", err)
	}

	value, err := extractFirstNumeric(rows)
	if err != nil {
		return automationEvaluation{}, fmt.Errorf("failed to read metric value: %w", err)
	}

	triggered, err := compareAgainstThreshold(value, automation.Comparator, automation.Threshold)
	if err != nil {
		return automationEvaluation{}, err
	}

	result := automationEvaluation{
		AutomationID:  automation.ID,
		Name:          automation.Name,
		ProgramID:     automation.ProgramID,
		Value:         value,
		Threshold:     automation.Threshold,
		Comparator:    automation.Comparator,
		Triggered:     triggered,
		AlertSent:     false,
		DeliveryState: "not_triggered",
		Query:         readOnlySQL,
		WindowFrom:    from.UTC().Format(time.RFC3339),
		WindowTo:      now.UTC().Format(time.RFC3339),
		Reason:        "condition not met",
		DryRun:        dryRun,
		RetryAttempts: 0,
		EvaluatedAt:   now.UTC(),
	}

	if !triggered {
		return result, nil
	}

	result.DeliveryState = "triggered_no_delivery"
	result.DedupeKey = automationDedupeKey(automation, value)

	if dryRun {
		result.Reason = "triggered in dry-run mode (no webhook sent)"
		result.DeliveryState = "dry_run"
		return result, nil
	}

	if !sendWebhook {
		result.Reason = "triggered but webhook delivery disabled for this run"
		result.DeliveryState = "disabled"
		return result, nil
	}

	if !cooldownElapsed(automation.LastTriggeredAt, automation.CooldownMinutes, now) {
		result.Reason = "triggered but still in cooldown"
		result.DeliveryState = "cooldown"
		return result, nil
	}

	if !dedupeElapsed(
		automation.LastTriggeredAt,
		automation.LastTriggerFingerprint,
		result.DedupeKey,
		automation.DedupeMinutes,
		now,
	) {
		result.Reason = "triggered but dedupe window suppressed duplicate alert"
		result.DeliveryState = "deduped"
		return result, nil
	}

	retryBackoff := time.Duration(normalizeDurationMinutes(
		automation.RetryInitialBackoffMS,
		defaultAutomationRetryBackoffMS,
	)) * time.Millisecond
	attempts, deliveryErr := sendAutomationWebhookWithRetry(
		automation.WebhookURL,
		result,
		webhookTimeout,
		normalizeDurationMinutes(automation.RetryMaxAttempts, defaultAutomationRetryAttempts),
		retryBackoff,
	)
	result.RetryAttempts = attempts

	if deliveryErr != nil {
		result.Error = deliveryErr.Error()
		result.Reason = "triggered but webhook delivery failed after retries"
		result.DeliveryState = "failed"
		return result, nil
	}

	result.AlertSent = true
	result.Reason = "triggered and webhook sent"
	result.DeliveryState = "sent"
	return result, nil
}

func cooldownElapsed(lastTriggeredAt *time.Time, cooldownMinutes int, now time.Time) bool {
	if lastTriggeredAt == nil {
		return true
	}
	if cooldownMinutes <= 0 {
		return true
	}
	return now.Sub(lastTriggeredAt.UTC()) >= time.Duration(cooldownMinutes)*time.Minute
}

func dedupeElapsed(
	lastTriggeredAt *time.Time,
	lastFingerprint string,
	currentFingerprint string,
	dedupeMinutes int,
	now time.Time,
) bool {
	if dedupeMinutes <= 0 || lastTriggeredAt == nil {
		return true
	}
	if strings.TrimSpace(lastFingerprint) == "" || strings.TrimSpace(currentFingerprint) == "" {
		return true
	}
	if lastFingerprint != currentFingerprint {
		return true
	}
	return now.Sub(lastTriggeredAt.UTC()) >= time.Duration(dedupeMinutes)*time.Minute
}

func automationDedupeKey(automation *models.AgentAutomation, value float64) string {
	if automation == nil {
		return ""
	}
	raw := fmt.Sprintf(
		"%d|%s|%s|%s|%.6f|%.6f",
		automation.ID,
		automation.ProgramID,
		automation.Comparator,
		automation.QueryTemplate,
		automation.Threshold,
		value,
	)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func sendAutomationWebhook(webhookURL string, evaluation automationEvaluation, timeout time.Duration) error {
	payload := map[string]any{
		"event":          "agent_automation_triggered",
		"automation_id":  evaluation.AutomationID,
		"name":           evaluation.Name,
		"program_id":     evaluation.ProgramID,
		"value":          evaluation.Value,
		"threshold":      evaluation.Threshold,
		"comparator":     evaluation.Comparator,
		"triggered":      evaluation.Triggered,
		"query":          evaluation.Query,
		"window_from":    evaluation.WindowFrom,
		"window_to":      evaluation.WindowTo,
		"evaluated_at":   evaluation.EvaluatedAt.UTC().Format(time.RFC3339),
		"delivery_state": evaluation.DeliveryState,
		"dry_run":        evaluation.DryRun,
		"dedupe_key":     evaluation.DedupeKey,
		"retry_attempts": evaluation.RetryAttempts,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := applyWebhookSignatureHeaders(req, body); err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

func sendAutomationWebhookWithRetry(
	webhookURL string,
	evaluation automationEvaluation,
	timeout time.Duration,
	maxAttempts int,
	initialBackoff time.Duration,
) (int, error) {
	attempts := normalizeDurationMinutes(maxAttempts, defaultAutomationRetryAttempts)
	if attempts > maxAutomationRetryAttempts {
		attempts = maxAutomationRetryAttempts
	}

	backoff := initialBackoff
	if backoff <= 0 {
		backoff = time.Duration(defaultAutomationRetryBackoffMS) * time.Millisecond
	}
	if backoff > time.Duration(maxAutomationRetryBackoffMS)*time.Millisecond {
		backoff = time.Duration(maxAutomationRetryBackoffMS) * time.Millisecond
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := sendAutomationWebhook(webhookURL, evaluation, timeout); err == nil {
			return attempt, nil
		} else {
			lastErr = err
		}

		if attempt < attempts {
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}

	return attempts, lastErr
}

func persistAutomationAudit(automation *models.AgentAutomation, evaluation automationEvaluation) error {
	if database.DB == nil || automation == nil {
		return nil
	}

	windowFrom, _ := time.Parse(time.RFC3339, evaluation.WindowFrom)
	windowTo, _ := time.Parse(time.RFC3339, evaluation.WindowTo)

	record := models.AgentAutomationEvaluation{
		AutomationID:  automation.ID,
		Value:         evaluation.Value,
		Threshold:     evaluation.Threshold,
		Comparator:    evaluation.Comparator,
		Triggered:     evaluation.Triggered,
		AlertSent:     evaluation.AlertSent,
		DeliveryState: evaluation.DeliveryState,
		Reason:        evaluation.Reason,
		Query:         evaluation.Query,
		WindowFrom:    windowFrom.UTC(),
		WindowTo:      windowTo.UTC(),
		EvaluatedAt:   evaluation.EvaluatedAt.UTC(),
		DryRun:        evaluation.DryRun,
		DedupeKey:     evaluation.DedupeKey,
		RetryAttempts: evaluation.RetryAttempts,
		Error:         strings.TrimSpace(evaluation.Error),
	}

	return database.DB.Create(&record).Error
}

func persistAutomationFailureAudit(
	automation *models.AgentAutomation,
	evaluatedAt time.Time,
	evalErr error,
) error {
	if automation == nil {
		return nil
	}

	evaluation := automationEvaluation{
		AutomationID:  automation.ID,
		Name:          automation.Name,
		ProgramID:     automation.ProgramID,
		Threshold:     automation.Threshold,
		Comparator:    automation.Comparator,
		Triggered:     false,
		AlertSent:     false,
		DeliveryState: "evaluation_failed",
		Query:         automation.QueryTemplate,
		WindowFrom:    evaluatedAt.UTC().Format(time.RFC3339),
		WindowTo:      evaluatedAt.UTC().Format(time.RFC3339),
		Reason:        "automation evaluation failed before delivery",
		DryRun:        automation.DryRun,
		Error:         strings.TrimSpace(evalErr.Error()),
		EvaluatedAt:   evaluatedAt.UTC(),
	}

	return persistAutomationAudit(automation, evaluation)
}

type AgentAutomationRunner struct {
	pollInterval   time.Duration
	evalTimeout    time.Duration
	webhookTimeout time.Duration

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

type AgentAutomationRetentionRunner struct {
	cleanupInterval time.Duration
	retention       time.Duration

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewAgentAutomationRunner(
	pollInterval time.Duration,
	evalTimeout time.Duration,
	webhookTimeout time.Duration,
) *AgentAutomationRunner {
	return &AgentAutomationRunner{
		pollInterval:   pollInterval,
		evalTimeout:    evalTimeout,
		webhookTimeout: webhookTimeout,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
}

func NewAgentAutomationRetentionRunner(cleanupInterval time.Duration, retention time.Duration) *AgentAutomationRetentionRunner {
	return &AgentAutomationRetentionRunner{
		cleanupInterval: cleanupInterval,
		retention:       retention,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}
}

func (r *AgentAutomationRunner) Start() {
	go r.loop()
}

func (r *AgentAutomationRetentionRunner) Start() {
	go r.loop()
}

func (r *AgentAutomationRunner) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
		<-r.doneCh
	})
}

func (r *AgentAutomationRetentionRunner) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
		<-r.doneCh
	})
}

func (r *AgentAutomationRunner) loop() {
	defer close(r.doneCh)

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.runTick()
		}
	}
}

func (r *AgentAutomationRetentionRunner) loop() {
	defer close(r.doneCh)

	ticker := time.NewTicker(r.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.runTick()
		}
	}
}

func (r *AgentAutomationRunner) runTick() {
	if database.DB == nil {
		return
	}

	var automations []models.AgentAutomation
	if err := database.DB.Where("is_active = ?", true).Find(&automations).Error; err != nil {
		log.Printf("agent automation runner: failed to load automations: %v", err)
		return
	}

	now := time.Now().UTC()
	for _, automation := range automations {
		if !isAutomationDue(&automation, now) {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), r.evalTimeout)
		observability.AgentAutomationEvaluation()
		evaluation, err := evaluateAutomation(ctx, &automation, now, true, automation.DryRun, r.webhookTimeout)
		cancel()
		if err != nil {
			observability.AgentAutomationFailure("evaluation_error")
			automation.LastError = err.Error()
			automation.LastEvaluatedAt = &now
			if saveErr := database.DB.Save(&automation).Error; saveErr != nil {
				log.Printf("agent automation runner: failed to persist error for automation=%d: %v", automation.ID, saveErr)
			}
			if auditErr := persistAutomationFailureAudit(&automation, now, err); auditErr != nil {
				log.Printf("agent automation runner: failed to persist failure audit for automation=%d: %v", automation.ID, auditErr)
			}
			continue
		}
		if strings.TrimSpace(evaluation.Error) != "" {
			observability.AgentAutomationFailure("delivery_error")
		}

		persistAutomationEvaluation(&automation, evaluation, evaluation.Error)
		if saveErr := database.DB.Save(&automation).Error; saveErr != nil {
			log.Printf("agent automation runner: failed to persist automation=%d: %v", automation.ID, saveErr)
		}
		if auditErr := persistAutomationAudit(&automation, evaluation); auditErr != nil {
			log.Printf("agent automation runner: failed to persist audit for automation=%d: %v", automation.ID, auditErr)
		}
	}
}

func (r *AgentAutomationRetentionRunner) runTick() {
	if database.DB == nil {
		return
	}
	if database.DB.Dialector.Name() != "postgres" {
		return
	}

	retentionLiteral := formatIntervalLiteral(r.retention)
	if strings.TrimSpace(retentionLiteral) == "" {
		return
	}

	var deletedRows int64
	if err := database.DB.Raw(
		"SELECT purge_agent_automation_evaluations(?::interval)",
		retentionLiteral,
	).Scan(&deletedRows).Error; err != nil {
		log.Printf("agent automation retention runner: cleanup failed: %v", err)
		return
	}

	if deletedRows > 0 {
		log.Printf("agent automation retention runner: purged %d old audit rows", deletedRows)
	}
}

func formatIntervalLiteral(retention time.Duration) string {
	if retention <= 0 {
		return ""
	}
	seconds := int(retention.Seconds())
	if seconds <= 0 {
		seconds = 1
	}
	return fmt.Sprintf("%d seconds", seconds)
}

func isAutomationDue(automation *models.AgentAutomation, now time.Time) bool {
	if automation == nil || !automation.IsActive {
		return false
	}
	if automation.LastEvaluatedAt == nil {
		return true
	}
	wait := time.Duration(normalizeDurationMinutes(automation.ScheduleMinutes, defaultAutomationScheduleMinutes)) * time.Minute
	return now.Sub(automation.LastEvaluatedAt.UTC()) >= wait
}
