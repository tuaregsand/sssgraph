package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/auth"
	"github.com/laserstream/api/database"
	"github.com/laserstream/api/models"
)

const (
	defaultAgentQueryLimit = 100
	maxAgentQueryLimit     = 1000
	agentQueryModeSQL      = "sql"
	agentQueryModeVerified = "verified"
	agentQueryModeReport   = "analyst_report"
)

type webhookPayload struct {
	URL       string `json:"url"`
	ProgramID string `json:"program_id"`
	EventType string `json:"event_type"`
}

type queryPayload struct {
	Prompt         string `json:"prompt"`
	Execute        bool   `json:"execute"`
	Limit          int    `json:"limit"`
	Mode           string `json:"mode"`
	StrictVerified bool   `json:"strict_verified"`
}

type openAIChatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func CreateAgentWebhook(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	var payload webhookPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	payload.URL = strings.TrimSpace(payload.URL)
	payload.ProgramID = strings.TrimSpace(payload.ProgramID)
	payload.EventType = strings.TrimSpace(payload.EventType)

	if payload.URL == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "url is required"})
	}
	if err := validateWebhookURL(payload.URL); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	if payload.ProgramID != "" {
		if !base58Pattern.MatchString(payload.ProgramID) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "program_id must be base58"})
		}
		if principal, err := auth.GetPrincipal(c); err == nil {
			if err := auth.EnsureProgramAccessForPrincipal(principal, payload.ProgramID); err != nil {
				return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "forbidden for this program_id"})
			}
		}
	}
	if payload.EventType != "" && !identifierPattern.MatchString(payload.EventType) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "event_type contains unsupported characters"})
	}

	webhook := models.AgentWebhook{
		URL:       payload.URL,
		ProgramID: payload.ProgramID,
		EventType: payload.EventType,
		IsActive:  true,
	}

	if err := database.DB.Create(&webhook).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create webhook"})
	}

	return c.Status(fiber.StatusCreated).JSON(webhook)
}

func GetAgentWebhooks(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	var webhooks []models.AgentWebhook
	if err := database.DB.Order("created_at desc").Find(&webhooks).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch webhooks"})
	}

	if principal, err := auth.GetPrincipal(c); err == nil {
		filtered := make([]models.AgentWebhook, 0, len(webhooks))
		for _, webhook := range webhooks {
			if webhook.ProgramID == "" || auth.EnsureProgramAccessForPrincipal(principal, webhook.ProgramID) == nil {
				filtered = append(filtered, webhook)
			}
		}
		webhooks = filtered
	}

	return c.JSON(webhooks)
}

func AgentQuery(c *fiber.Ctx) error {
	var payload queryPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	return runAgentQuery(c, payload)
}

func AgentQueryReport(c *fiber.Ctx) error {
	var payload queryPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	// Dedicated strict-report alias for agent integrations.
	payload.Mode = agentQueryModeReport
	payload.StrictVerified = true
	payload.Execute = true

	return runAgentQuery(c, payload)
}

func runAgentQuery(c *fiber.Ctx, payload queryPayload) error {
	prompt := strings.TrimSpace(payload.Prompt)
	if prompt == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "prompt is required"})
	}

	queryMode, execute, err := resolveAgentQueryMode(payload.Mode, payload.Execute, payload.StrictVerified)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	eventsTable := resolveAgentQueryTable()
	sqlQuery, generationStatus, err := generateSQLFromPrompt(prompt, os.Getenv("OPENAI_API_KEY"), eventsTable)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	readOnlySQL, err := normalizeReadOnlySQL(sqlQuery)
	if err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"prompt":              prompt,
			"sql":                 sqlQuery,
			"status":              "rejected",
			"validation_error":    err.Error(),
			"generation_status":   generationStatus,
			"execution_supported": false,
		})
	}

	if queryMode == agentQueryModeReport && !queryReferencesTable(readOnlySQL, eventsTable) {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"prompt":            prompt,
			"sql":               readOnlySQL,
			"status":            "rejected",
			"generation_status": generationStatus,
			"validation_error":  fmt.Sprintf("strict mode requires querying table `%s`", eventsTable),
		})
	}

	resultLimit := payload.Limit
	if resultLimit <= 0 {
		resultLimit = defaultAgentQueryLimit
	}
	if resultLimit > maxAgentQueryLimit {
		resultLimit = maxAgentQueryLimit
	}

	response := fiber.Map{
		"prompt":              prompt,
		"sql":                 readOnlySQL,
		"sql_hash_sha256":     sqlHash(readOnlySQL),
		"status":              "success",
		"generation_status":   generationStatus,
		"execution_supported": true,
		"mode":                queryMode,
	}

	if execute {
		executionQuery := wrapQueryForJSONRows(readOnlySQL, resultLimit)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		executedAt := time.Now().UTC()

		rows, queryErr := analytics.QueryRows(ctx, executionQuery)
		if queryErr != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"prompt":            prompt,
				"sql":               readOnlySQL,
				"status":            "execution_error",
				"generation_status": generationStatus,
				"error":             "failed to execute query",
			})
		}

		response["items"] = rows
		response["count"] = len(rows)
		response["limit"] = resultLimit
		response["verified_answer"] = summarizeRows(rows)
		response["automation_suggestion"] = buildAutomationSuggestion(prompt, readOnlySQL, rows)
		evidenceBundle := buildQueryEvidenceBundle(readOnlySQL, eventsTable, resultLimit, rows, executedAt)
		response["evidence_bundle"] = evidenceBundle
		response["verification"] = fiber.Map{
			"guardrail":        "read-only sql policy",
			"executed_at_utc":  executedAt.Format(time.RFC3339),
			"row_count":        len(rows),
			"strict_verified":  queryMode == agentQueryModeReport,
			"table":            eventsTable,
			"sql_hash_sha256":  evidenceBundle["sql_hash_sha256"],
			"program_id_count": len(evidenceBundle["program_ids"].([]string)),
			"signature_count":  len(evidenceBundle["signatures"].([]string)),
			"slot_count":       len(evidenceBundle["slots"].([]uint64)),
		}

		if queryMode == agentQueryModeReport {
			report := buildOpsGrowthAnalystReport(prompt, readOnlySQL, rows, eventsTable, resultLimit, evidenceBundle)
			response["report"] = report
			response["verified_answer"] = report["executive_summary"]
		}
	}

	return c.JSON(response)
}

func resolveAgentQueryMode(mode string, execute bool, strictVerified bool) (string, bool, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		if execute {
			mode = agentQueryModeVerified
		} else {
			mode = agentQueryModeSQL
		}
	}

	if strictVerified {
		mode = agentQueryModeReport
		execute = true
	}

	switch mode {
	case agentQueryModeSQL:
		return mode, execute, nil
	case agentQueryModeVerified:
		return mode, true, nil
	case agentQueryModeReport:
		return mode, true, nil
	default:
		return "", false, fmt.Errorf("mode must be one of: %s, %s, %s", agentQueryModeSQL, agentQueryModeVerified, agentQueryModeReport)
	}
}

func resolveAgentQueryTable() string {
	table := "solana_events"
	if current, err := analytics.EventsTable(); err == nil && identifierPattern.MatchString(current) {
		table = current
	}
	return table
}

func generateSQLFromPrompt(prompt, apiKey, eventsTable string) (string, string, error) {
	apiKey = strings.TrimSpace(apiKey)
	eventsTable = strings.TrimSpace(eventsTable)
	if eventsTable == "" {
		eventsTable = "solana_events"
	}

	if apiKey == "" {
		return fmt.Sprintf("SELECT count(*) AS event_count FROM %s WHERE timestamp >= now() - INTERVAL 1 DAY", eventsTable), "mocked (set OPENAI_API_KEY for live generation)", nil
	}

	systemPrompt := `You translate natural language requests into a single read-only ClickHouse SQL query.
Use only SELECT or WITH queries.
Use table ` + eventsTable + ` with columns: timestamp, event_id, event_type, program_id, slot, signature, payload.
Default to recent windows when time range is missing.
Never return markdown, comments, explanations, or multiple statements.`

	resp, err := callChatCompletion(apiKey, map[string]any{
		"model": "gpt-4o-mini",
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.0,
	})
	if err != nil {
		return "", "", err
	}

	sqlQuery := strings.TrimSpace(resp)
	sqlQuery = stripMarkdownFences(sqlQuery)
	sqlQuery = strings.TrimSpace(sqlQuery)
	if sqlQuery == "" {
		return "", "", errors.New("LLM returned empty SQL")
	}

	return sqlQuery, "llm", nil
}

func callChatCompletion(apiKey string, body map[string]any) (string, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("failed to encode llm request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewBuffer(payload))
	if err != nil {
		return "", fmt.Errorf("failed to create llm request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to call llm api: %w", err)
	}
	defer resp.Body.Close()

	var llmResp openAIChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
		return "", fmt.Errorf("failed to decode llm response: %w", err)
	}
	if llmResp.Error != nil {
		return "", fmt.Errorf("llm api error: %s", strings.TrimSpace(llmResp.Error.Message))
	}
	if len(llmResp.Choices) == 0 {
		return "", errors.New("llm response missing choices")
	}

	content := strings.TrimSpace(llmResp.Choices[0].Message.Content)
	if content == "" {
		return "", errors.New("llm response content is empty")
	}

	return content, nil
}

func summarizeRows(rows []map[string]any) string {
	if len(rows) == 0 {
		return "No matching rows were found for this query."
	}

	if len(rows) == 1 {
		row := rows[0]
		keys := make([]string, 0, len(row))
		for key := range row {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if len(keys) == 1 {
			return fmt.Sprintf("Query returned one row: %s=%v.", keys[0], row[keys[0]])
		}
		return fmt.Sprintf("Query returned one row with fields: %s.", strings.Join(keys, ", "))
	}

	return fmt.Sprintf("Query returned %d rows.", len(rows))
}

func buildAutomationSuggestion(prompt, sql string, rows []map[string]any) fiber.Map {
	comparator := "lt"
	threshold := 0.0
	loweredPrompt := strings.ToLower(prompt)
	if strings.Contains(loweredPrompt, "increase") || strings.Contains(loweredPrompt, "spike") || strings.Contains(loweredPrompt, "up") {
		comparator = "gt"
	}

	if len(rows) > 0 {
		if value, err := extractFirstNumeric(rows); err == nil {
			if comparator == "lt" {
				threshold = value * 0.7
			} else {
				threshold = value * 1.3
			}
		}
	}

	return fiber.Map{
		"name":             "Auto monitor from query",
		"query_template":   sql,
		"comparator":       comparator,
		"threshold_hint":   threshold,
		"schedule_minutes": 60,
		"window_minutes":   60,
		"cooldown_minutes": 30,
	}
}

func buildOpsGrowthAnalystReport(
	prompt string,
	sql string,
	rows []map[string]any,
	eventsTable string,
	limit int,
	evidenceBundle fiber.Map,
) fiber.Map {
	kpis := extractKPIs(rows)
	verdict := deriveReportVerdict(prompt, kpis, rows)
	confidence := deriveReportConfidence(kpis, rows)

	findings := []string{
		fmt.Sprintf("Executed read-only query against `%s` with limit=%d.", eventsTable, limit),
		fmt.Sprintf("Returned %d row(s).", len(rows)),
	}
	if len(kpis) > 0 {
		findings = append(findings, fmt.Sprintf("Top KPI `%s` has value %.4f.", kpis[0]["metric"], kpis[0]["value"]))
	} else {
		findings = append(findings, "No numeric KPI fields were detected in the result set.")
	}

	executiveSummary := "Query executed successfully with verified evidence."
	switch verdict {
	case "insufficient_data":
		executiveSummary = "Query executed, but data is insufficient for a confident ops/growth conclusion."
	case "critical":
		executiveSummary = "Query executed and indicates a critical metric condition that should trigger immediate follow-up."
	case "watch":
		executiveSummary = "Query executed and indicates a watch-level condition; monitor closely."
	case "healthy":
		executiveSummary = "Query executed and metrics appear healthy for the requested scope."
	}

	return fiber.Map{
		"report_version":         "1.0",
		"audience":               "ops_growth_agents",
		"question":               prompt,
		"executive_summary":      executiveSummary,
		"verdict":                verdict,
		"confidence":             confidence,
		"kpis":                   kpis,
		"findings":               findings,
		"recommended_actions":    []string{"Review suggested automation and tune threshold before activating in production."},
		"recommended_automation": buildAutomationSuggestion(prompt, sql, rows),
		"evidence":               evidenceBundle,
		"verification": fiber.Map{
			"strict_verified": true,
			"checks": []string{
				"single read-only statement",
				"no SQL comments",
				"execution evidence attached",
				"table reference validated",
			},
		},
	}
}

func extractKPIs(rows []map[string]any) []fiber.Map {
	if len(rows) == 0 {
		return nil
	}

	firstRow := rows[0]
	keys := make([]string, 0, len(firstRow))
	for key := range firstRow {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	kpis := make([]fiber.Map, 0, len(keys))
	for _, key := range keys {
		value, ok := toFloat64(firstRow[key])
		if !ok {
			continue
		}
		kpis = append(kpis, fiber.Map{
			"metric": key,
			"value":  value,
			"unit":   "number",
		})
	}
	return kpis
}

func deriveReportVerdict(prompt string, kpis []fiber.Map, rows []map[string]any) string {
	if len(rows) == 0 {
		return "insufficient_data"
	}
	if len(kpis) == 0 {
		return "watch"
	}

	primaryValue, _ := kpis[0]["value"].(float64)
	loweredPrompt := strings.ToLower(prompt)
	if primaryValue <= 0 {
		return "critical"
	}
	if strings.Contains(loweredPrompt, "drop") || strings.Contains(loweredPrompt, "decrease") || strings.Contains(loweredPrompt, "decline") {
		if primaryValue < 1 {
			return "critical"
		}
		if primaryValue < 10 {
			return "watch"
		}
		return "healthy"
	}
	if primaryValue < 1 {
		return "watch"
	}
	return "healthy"
}

func deriveReportConfidence(kpis []fiber.Map, rows []map[string]any) float64 {
	confidence := 0.45
	if len(rows) > 0 {
		confidence += 0.2
	}
	if len(rows) >= 5 {
		confidence += 0.1
	}
	if len(kpis) > 0 {
		confidence += 0.2
	}
	if len(kpis) >= 3 {
		confidence += 0.05
	}
	if confidence > 0.95 {
		confidence = 0.95
	}
	return confidence
}

func sampleResultRows(rows []map[string]any, max int) []map[string]any {
	if len(rows) == 0 || max <= 0 {
		return []map[string]any{}
	}
	if len(rows) <= max {
		return rows
	}
	return rows[:max]
}

func buildQueryEvidenceBundle(
	sql string,
	eventsTable string,
	limit int,
	rows []map[string]any,
	executedAt time.Time,
) fiber.Map {
	return fiber.Map{
		"sql_hash_sha256": sqlHash(sql),
		"sql":             sql,
		"table":           eventsTable,
		"executed_at_utc": executedAt.UTC().Format(time.RFC3339),
		"row_count":       len(rows),
		"limit":           limit,
		"program_ids":     collectUniqueStringField(rows, "program_id", 100),
		"signatures":      collectUniqueStringField(rows, "signature", 100),
		"slots":           collectUniqueSlotField(rows, "slot", 100),
		"sample_rows":     sampleResultRows(rows, 20),
	}
}

func sqlHash(sql string) string {
	trimmed := strings.TrimSpace(sql)
	sum := sha256.Sum256([]byte(trimmed))
	return hex.EncodeToString(sum[:])
}

func collectUniqueStringField(rows []map[string]any, field string, limit int) []string {
	if limit <= 0 {
		limit = 100
	}

	seen := make(map[string]struct{}, limit)
	values := make([]string, 0, limit)
	for _, row := range rows {
		raw, ok := row[field]
		if !ok || raw == nil {
			continue
		}

		switch v := raw.(type) {
		case string:
			value := strings.TrimSpace(v)
			if value == "" {
				continue
			}
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			values = append(values, value)
		case []any:
			for _, element := range v {
				text, ok := element.(string)
				if !ok {
					continue
				}
				value := strings.TrimSpace(text)
				if value == "" {
					continue
				}
				if _, exists := seen[value]; exists {
					continue
				}
				seen[value] = struct{}{}
				values = append(values, value)
				if len(values) >= limit {
					return values
				}
			}
		}

		if len(values) >= limit {
			return values
		}
	}

	return values
}

func collectUniqueSlotField(rows []map[string]any, field string, limit int) []uint64 {
	if limit <= 0 {
		limit = 100
	}

	seen := make(map[uint64]struct{}, limit)
	values := make([]uint64, 0, limit)
	for _, row := range rows {
		raw, ok := row[field]
		if !ok || raw == nil {
			continue
		}

		number, ok := toFloat64(raw)
		if !ok {
			continue
		}
		if number < 0 {
			continue
		}

		slot := uint64(number)
		if _, exists := seen[slot]; exists {
			continue
		}
		seen[slot] = struct{}{}
		values = append(values, slot)
		if len(values) >= limit {
			return values
		}
	}

	return values
}
