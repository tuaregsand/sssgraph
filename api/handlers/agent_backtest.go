package handlers

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/analytics"
	"github.com/laserstream/api/auth"
)

const (
	launchSignalEventGrowth        = "event_growth"
	launchSignalUniqueSignerGrowth = "unique_signer_growth"
	launchSignalTransactionGrowth  = "transaction_growth"
)

var launchSignals = []string{
	launchSignalEventGrowth,
	launchSignalUniqueSignerGrowth,
	launchSignalTransactionGrowth,
}

type launchBacktestPayload struct {
	Launches            []launchInput           `json:"launches"`
	BaselineHours       int                     `json:"baseline_hours"`
	EvaluationHours     int                     `json:"evaluation_hours"`
	MinEvaluationEvents int                     `json:"min_evaluation_events"`
	Thresholds          launchBacktestThreshold `json:"thresholds"`
}

type launchScorePayload struct {
	HistoryLaunches     []launchInput           `json:"history_launches"`
	Launches            []launchInput           `json:"launches"`
	Candidate           launchInput             `json:"candidate"`
	BaselineHours       int                     `json:"baseline_hours"`
	EvaluationHours     int                     `json:"evaluation_hours"`
	MinEvaluationEvents int                     `json:"min_evaluation_events"`
	Thresholds          launchBacktestThreshold `json:"thresholds"`
}

type launchInput struct {
	Name      string `json:"name"`
	ProgramID string `json:"program_id"`
	LaunchAt  string `json:"launch_at"`
}

type launchBacktestThreshold struct {
	EventGrowthRatio        float64 `json:"event_growth_ratio"`
	UniqueSignerGrowthRatio float64 `json:"unique_signer_growth_ratio"`
	TransactionGrowthRatio  float64 `json:"transaction_growth_ratio"`
}

type launchBacktestSettings struct {
	BaselineHours       int
	EvaluationHours     int
	MinEvaluationEvents int
	Thresholds          launchBacktestThreshold
}

type launchBacktestResult struct {
	Name      string                 `json:"name"`
	ProgramID string                 `json:"program_id"`
	LaunchAt  string                 `json:"launch_at"`
	Outcome   string                 `json:"outcome"`
	Score     float64                `json:"score"`
	Signals   []map[string]any       `json:"signals"`
	Metrics   map[string]any         `json:"metrics"`
	Reason    string                 `json:"reason"`
	Query     string                 `json:"query"`
	Windows   map[string]interface{} `json:"windows"`
}

func AgentBacktestLaunches(c *fiber.Ctx) error {
	var payload launchBacktestPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	if len(payload.Launches) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "launches is required and cannot be empty"})
	}
	if len(payload.Launches) > 100 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "launches cannot exceed 100 entries"})
	}

	settings, err := normalizeLaunchBacktestSettings(payload.BaselineHours, payload.EvaluationHours, payload.MinEvaluationEvents, payload.Thresholds)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	table, err := resolveBacktestTable()
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}

	results := make([]launchBacktestResult, 0, len(payload.Launches))
	workedCount := 0
	signalWins := map[string]int{
		launchSignalEventGrowth:        0,
		launchSignalUniqueSignerGrowth: 0,
		launchSignalTransactionGrowth:  0,
	}

	for _, launch := range payload.Launches {
		sanitizedLaunch, statusCode, message := validateLaunchInput(c, launch)
		if statusCode != 0 {
			return c.Status(statusCode).JSON(fiber.Map{"error": message})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, evalErr := evaluateLaunchBacktest(ctx, sanitizedLaunch, table, settings)
		cancel()
		if evalErr != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error":      "failed to query launch backtest window",
				"program_id": sanitizedLaunch.ProgramID,
			})
		}

		if result.Outcome == "worked" {
			workedCount++
		}
		accumulateSignalWins(signalWins, result)
		results = append(results, result)
	}

	leaderboard := signalWinsLeaderboard(signalWins)

	return c.JSON(fiber.Map{
		"status": "success",
		"config": fiber.Map{
			"baseline_hours":         settings.BaselineHours,
			"evaluation_hours":       settings.EvaluationHours,
			"min_evaluation_events":  settings.MinEvaluationEvents,
			"thresholds":             settings.Thresholds,
			"classification_formula": "worked when score >= 2.0 and evaluation_events >= min_evaluation_events",
		},
		"summary": fiber.Map{
			"total_launches":         len(results),
			"worked":                 workedCount,
			"not_worked":             len(results) - workedCount,
			"top_predictive_signals": leaderboard,
		},
		"launches": results,
	})
}

func AgentBacktestScoreLaunch(c *fiber.Ctx) error {
	var payload launchScorePayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	history := payload.HistoryLaunches
	if len(history) == 0 {
		history = payload.Launches
	}
	if len(history) < 3 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "history_launches requires at least 3 launches"})
	}
	if len(history) > 200 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "history_launches cannot exceed 200 entries"})
	}

	candidate, candidateStatus, candidateMessage := validateLaunchInput(c, payload.Candidate)
	if candidateStatus != 0 {
		return c.Status(candidateStatus).JSON(fiber.Map{"error": candidateMessage})
	}

	settings, err := normalizeLaunchBacktestSettings(payload.BaselineHours, payload.EvaluationHours, payload.MinEvaluationEvents, payload.Thresholds)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	table, err := resolveBacktestTable()
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}

	historyResults := make([]launchBacktestResult, 0, len(history))
	workedCount := 0
	for _, launch := range history {
		sanitizedLaunch, statusCode, message := validateLaunchInput(c, launch)
		if statusCode != 0 {
			return c.Status(statusCode).JSON(fiber.Map{"error": message})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, evalErr := evaluateLaunchBacktest(ctx, sanitizedLaunch, table, settings)
		cancel()
		if evalErr != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error":      "failed to query history launch window",
				"program_id": sanitizedLaunch.ProgramID,
			})
		}

		if result.Outcome == "worked" {
			workedCount++
		}
		historyResults = append(historyResults, result)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	candidateResult, evalErr := evaluateLaunchBacktest(ctx, candidate, table, settings)
	cancel()
	if evalErr != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":      "failed to query candidate launch window",
			"program_id": candidate.ProgramID,
		})
	}

	prior := 0.0
	if len(historyResults) > 0 {
		prior = float64(workedCount) / float64(len(historyResults))
	}

	type signalContribution struct {
		Signal             string
		Contribution       float64
		CandidateTriggered bool
		Precision          float64
		Coverage           float64
		WorkedWithSignal   int
		TotalWithSignal    int
	}

	contributions := make([]signalContribution, 0, len(launchSignals))
	probability := prior
	signalPerformance := make([]map[string]any, 0, len(launchSignals))

	for _, signal := range launchSignals {
		totalWithSignal := 0
		workedWithSignal := 0
		for _, result := range historyResults {
			if !hasSignal(result, signal) {
				continue
			}
			totalWithSignal++
			if result.Outcome == "worked" {
				workedWithSignal++
			}
		}

		coverage := 0.0
		if len(historyResults) > 0 {
			coverage = float64(totalWithSignal) / float64(len(historyResults))
		}

		precision := prior
		if totalWithSignal > 0 {
			precision = float64(workedWithSignal) / float64(totalWithSignal)
		}

		candidateTriggered := hasSignal(candidateResult, signal)
		delta := precision - prior
		contribution := delta * (0.6 + 0.4*coverage)
		if !candidateTriggered {
			contribution = -delta * 0.25 * coverage
		}

		probability += contribution
		contributions = append(contributions, signalContribution{
			Signal:             signal,
			Contribution:       contribution,
			CandidateTriggered: candidateTriggered,
			Precision:          precision,
			Coverage:           coverage,
			WorkedWithSignal:   workedWithSignal,
			TotalWithSignal:    totalWithSignal,
		})

		signalPerformance = append(signalPerformance, map[string]any{
			"signal":             signal,
			"precision":          precision,
			"coverage":           coverage,
			"worked_with_signal": workedWithSignal,
			"total_with_signal":  totalWithSignal,
		})
	}

	candidateEvaluationEvents, _ := toFloat64(candidateResult.Metrics["evaluation_events"])
	if candidateEvaluationEvents < float64(settings.MinEvaluationEvents) {
		probability -= 0.1
	}
	if candidateResult.Score >= 2.0 {
		probability += 0.05
	}
	probability = clamp(probability, 0.01, 0.99)

	sort.Slice(contributions, func(i, j int) bool {
		left := math.Abs(contributions[i].Contribution)
		right := math.Abs(contributions[j].Contribution)
		if left == right {
			return contributions[i].Signal < contributions[j].Signal
		}
		return left > right
	})

	topSignals := make([]map[string]any, 0, len(contributions))
	for _, item := range contributions {
		topSignals = append(topSignals, map[string]any{
			"signal":              item.Signal,
			"contribution":        item.Contribution,
			"candidate_triggered": item.CandidateTriggered,
			"precision":           item.Precision,
			"coverage":            item.Coverage,
			"worked_with_signal":  item.WorkedWithSignal,
			"total_with_signal":   item.TotalWithSignal,
		})
	}

	avgCoverage := 0.0
	for _, item := range contributions {
		avgCoverage += item.Coverage
	}
	if len(contributions) > 0 {
		avgCoverage /= float64(len(contributions))
	}

	historyFactor := math.Min(1.0, float64(len(historyResults))/20.0)
	confidence := 0.35 + (historyFactor * 0.35) + (avgCoverage * 0.2)
	if candidateEvaluationEvents >= float64(settings.MinEvaluationEvents) {
		confidence += 0.1
	}
	if confidence > 0.95 {
		confidence = 0.95
	}
	if confidence < 0.05 {
		confidence = 0.05
	}

	band := probabilityBand(probability)

	return c.JSON(fiber.Map{
		"status":        "success",
		"model_version": "launch_signal_v1",
		"config": fiber.Map{
			"baseline_hours":        settings.BaselineHours,
			"evaluation_hours":      settings.EvaluationHours,
			"min_evaluation_events": settings.MinEvaluationEvents,
			"thresholds":            settings.Thresholds,
		},
		"history_summary": fiber.Map{
			"total_launches": len(historyResults),
			"worked":         workedCount,
			"not_worked":     len(historyResults) - workedCount,
		},
		"candidate":                candidateResult,
		"prior_probability":        prior,
		"probability":              probability,
		"probability_band":         band,
		"confidence":               confidence,
		"top_contributing_signals": topSignals,
		"signal_performance":       signalPerformance,
		"rationale":                "Probability blends historical signal precision, candidate-triggered signals, and sample confidence.",
	})
}

func normalizeLaunchBacktestSettings(
	baselineHours int,
	evaluationHours int,
	minEvents int,
	thresholds launchBacktestThreshold,
) (launchBacktestSettings, error) {
	if baselineHours <= 0 {
		baselineHours = 24
	}
	if baselineHours > 24*14 {
		return launchBacktestSettings{}, fmt.Errorf("baseline_hours cannot exceed 336")
	}

	if evaluationHours <= 0 {
		evaluationHours = 6
	}
	if evaluationHours > 24*14 {
		return launchBacktestSettings{}, fmt.Errorf("evaluation_hours cannot exceed 336")
	}

	if minEvents <= 0 {
		minEvents = 100
	}

	if thresholds.EventGrowthRatio <= 0 {
		thresholds.EventGrowthRatio = 2.0
	}
	if thresholds.UniqueSignerGrowthRatio <= 0 {
		thresholds.UniqueSignerGrowthRatio = 1.7
	}
	if thresholds.TransactionGrowthRatio <= 0 {
		thresholds.TransactionGrowthRatio = 2.0
	}

	return launchBacktestSettings{
		BaselineHours:       baselineHours,
		EvaluationHours:     evaluationHours,
		MinEvaluationEvents: minEvents,
		Thresholds:          thresholds,
	}, nil
}

func resolveBacktestTable() (string, error) {
	table, err := analytics.EventsTable()
	if err != nil {
		return "", fmt.Errorf("clickhouse is not configured")
	}
	if !identifierPattern.MatchString(table) {
		return "", fmt.Errorf("invalid clickhouse table configuration")
	}
	return table, nil
}

func validateLaunchInput(c *fiber.Ctx, launch launchInput) (launchInput, int, string) {
	name := strings.TrimSpace(launch.Name)
	programID := strings.TrimSpace(launch.ProgramID)
	if name == "" {
		name = programID
	}
	if programID == "" {
		return launchInput{}, fiber.StatusBadRequest, "each launch requires program_id"
	}
	if !base58Pattern.MatchString(programID) {
		return launchInput{}, fiber.StatusBadRequest, "program_id must be base58"
	}
	if principal, principalErr := auth.GetPrincipal(c); principalErr == nil {
		if accessErr := auth.EnsureProgramAccessForPrincipal(principal, programID); accessErr != nil {
			return launchInput{}, fiber.StatusForbidden, "forbidden for this program_id"
		}
	}

	launchAt := strings.TrimSpace(launch.LaunchAt)
	if _, parseErr := time.Parse(time.RFC3339, launchAt); parseErr != nil {
		return launchInput{}, fiber.StatusBadRequest, "launch_at must be RFC3339 timestamp"
	}

	return launchInput{
		Name:      name,
		ProgramID: programID,
		LaunchAt:  launchAt,
	}, 0, ""
}

func evaluateLaunchBacktest(
	ctx context.Context,
	launch launchInput,
	table string,
	settings launchBacktestSettings,
) (launchBacktestResult, error) {
	launchAt, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(launch.LaunchAt))
	if parseErr != nil {
		return launchBacktestResult{}, fmt.Errorf("launch_at must be RFC3339 timestamp")
	}

	baselineStart := launchAt.Add(-time.Duration(settings.BaselineHours) * time.Hour)
	evaluationEnd := launchAt.Add(time.Duration(settings.EvaluationHours) * time.Hour)

	query := fmt.Sprintf(
		`SELECT
countIf(timestamp >= parseDateTimeBestEffort('%s') AND timestamp < parseDateTimeBestEffort('%s')) AS baseline_events,
uniqIf(signature, timestamp >= parseDateTimeBestEffort('%s') AND timestamp < parseDateTimeBestEffort('%s')) AS baseline_unique_signatures,
countIf(event_type = 'transaction' AND timestamp >= parseDateTimeBestEffort('%s') AND timestamp < parseDateTimeBestEffort('%s')) AS baseline_transactions,
countIf(timestamp >= parseDateTimeBestEffort('%s') AND timestamp < parseDateTimeBestEffort('%s')) AS evaluation_events,
uniqIf(signature, timestamp >= parseDateTimeBestEffort('%s') AND timestamp < parseDateTimeBestEffort('%s')) AS evaluation_unique_signatures,
countIf(event_type = 'transaction' AND timestamp >= parseDateTimeBestEffort('%s') AND timestamp < parseDateTimeBestEffort('%s')) AS evaluation_transactions
FROM %s
WHERE program_id = '%s'
AND timestamp >= parseDateTimeBestEffort('%s')
AND timestamp < parseDateTimeBestEffort('%s')
FORMAT JSONEachRow`,
		escapeSQLLiteral(baselineStart.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(launchAt.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(baselineStart.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(launchAt.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(baselineStart.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(launchAt.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(launchAt.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(evaluationEnd.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(launchAt.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(evaluationEnd.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(launchAt.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(evaluationEnd.UTC().Format(time.RFC3339)),
		table,
		escapeSQLLiteral(launch.ProgramID),
		escapeSQLLiteral(baselineStart.UTC().Format(time.RFC3339)),
		escapeSQLLiteral(evaluationEnd.UTC().Format(time.RFC3339)),
	)

	rows, queryErr := analytics.QueryRows(ctx, query)
	if queryErr != nil {
		return launchBacktestResult{}, queryErr
	}
	if len(rows) == 0 {
		rows = append(rows, map[string]any{})
	}
	row := rows[0]

	baselineEvents, _ := toFloat64(row["baseline_events"])
	baselineUniqueSigners, _ := toFloat64(row["baseline_unique_signatures"])
	baselineTransactions, _ := toFloat64(row["baseline_transactions"])
	evaluationEvents, _ := toFloat64(row["evaluation_events"])
	evaluationUniqueSigners, _ := toFloat64(row["evaluation_unique_signatures"])
	evaluationTransactions, _ := toFloat64(row["evaluation_transactions"])

	eventGrowth := growthRatio(baselineEvents, evaluationEvents)
	uniqueSignerGrowth := growthRatio(baselineUniqueSigners, evaluationUniqueSigners)
	transactionGrowth := growthRatio(baselineTransactions, evaluationTransactions)

	signals := make([]map[string]any, 0, len(launchSignals))
	score := 0.0
	if eventGrowth >= settings.Thresholds.EventGrowthRatio {
		score += 1.0
		signals = append(signals, map[string]any{
			"signal":    launchSignalEventGrowth,
			"value":     eventGrowth,
			"threshold": settings.Thresholds.EventGrowthRatio,
			"impact":    "strong",
		})
	}
	if uniqueSignerGrowth >= settings.Thresholds.UniqueSignerGrowthRatio {
		score += 1.0
		signals = append(signals, map[string]any{
			"signal":    launchSignalUniqueSignerGrowth,
			"value":     uniqueSignerGrowth,
			"threshold": settings.Thresholds.UniqueSignerGrowthRatio,
			"impact":    "strong",
		})
	}
	if transactionGrowth >= settings.Thresholds.TransactionGrowthRatio {
		score += 1.0
		signals = append(signals, map[string]any{
			"signal":    launchSignalTransactionGrowth,
			"value":     transactionGrowth,
			"threshold": settings.Thresholds.TransactionGrowthRatio,
			"impact":    "strong",
		})
	}
	if evaluationEvents >= float64(settings.MinEvaluationEvents) {
		score += 0.5
	}

	outcome := "not_worked"
	reason := "launch did not meet required signal thresholds"
	if score >= 2.0 && evaluationEvents >= float64(settings.MinEvaluationEvents) {
		outcome = "worked"
		reason = "launch passed at least two growth signals with sufficient post-launch activity"
	}

	return launchBacktestResult{
		Name:      launch.Name,
		ProgramID: launch.ProgramID,
		LaunchAt:  launchAt.UTC().Format(time.RFC3339),
		Outcome:   outcome,
		Score:     score,
		Signals:   signals,
		Reason:    reason,
		Query:     query,
		Metrics: map[string]any{
			"baseline_events":              baselineEvents,
			"evaluation_events":            evaluationEvents,
			"baseline_unique_signatures":   baselineUniqueSigners,
			"evaluation_unique_signatures": evaluationUniqueSigners,
			"baseline_transactions":        baselineTransactions,
			"evaluation_transactions":      evaluationTransactions,
			"event_growth_ratio":           eventGrowth,
			"unique_signer_growth_ratio":   uniqueSignerGrowth,
			"transaction_growth_ratio":     transactionGrowth,
		},
		Windows: map[string]interface{}{
			"baseline_start": baselineStart.UTC().Format(time.RFC3339),
			"launch_at":      launchAt.UTC().Format(time.RFC3339),
			"evaluation_end": evaluationEnd.UTC().Format(time.RFC3339),
		},
	}, nil
}

func hasSignal(result launchBacktestResult, signal string) bool {
	for _, entry := range result.Signals {
		if entry["signal"] == signal {
			return true
		}
	}
	return false
}

func accumulateSignalWins(signalWins map[string]int, result launchBacktestResult) {
	for _, signal := range launchSignals {
		if hasSignal(result, signal) {
			signalWins[signal]++
		}
	}
}

func signalWinsLeaderboard(signalWins map[string]int) []map[string]any {
	leaderboard := make([]map[string]any, 0, len(signalWins))
	for signal, count := range signalWins {
		leaderboard = append(leaderboard, map[string]any{
			"signal":         signal,
			"wins_in_launch": count,
		})
	}

	sort.Slice(leaderboard, func(i, j int) bool {
		left := leaderboard[i]["wins_in_launch"].(int)
		right := leaderboard[j]["wins_in_launch"].(int)
		if left == right {
			return leaderboard[i]["signal"].(string) < leaderboard[j]["signal"].(string)
		}
		return left > right
	})

	return leaderboard
}

func probabilityBand(value float64) string {
	switch {
	case value >= 0.75:
		return "high"
	case value >= 0.45:
		return "medium"
	default:
		return "low"
	}
}

func clamp(value float64, min float64, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func growthRatio(baseline, evaluation float64) float64 {
	if baseline <= 0 {
		if evaluation <= 0 {
			return 1.0
		}
		return evaluation
	}
	return evaluation / baseline
}
