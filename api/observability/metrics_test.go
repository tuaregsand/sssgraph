package observability

import (
	"strings"
	"testing"
)

func TestRegistryPrometheusIncludesAgentSLOMetrics(t *testing.T) {
	previous := Metrics
	Metrics = NewRegistry()
	defer func() {
		Metrics = previous
	}()

	Metrics.recordRequest("GET", "/api/health", 200, 1200)
	AgentQueryExecution()
	AgentQueryError("sql_validation")
	AgentAutomationEvaluation()
	AgentAutomationFailure("delivery_error")

	output := Metrics.prometheus()
	required := []string{
		"api_agent_query_executions_total",
		`api_agent_query_errors_total{stage="sql_validation"}`,
		"api_agent_automation_evaluations_total",
		`api_agent_automation_failures_total{stage="delivery_error"}`,
	}
	for _, key := range required {
		if !strings.Contains(output, key) {
			t.Fatalf("expected prometheus output to contain %s", key)
		}
	}
}

func TestSanitizeStage(t *testing.T) {
	if got := sanitizeStage(" SQL.Validation "); got != "sql_validation" {
		t.Fatalf("unexpected sanitized stage: %s", got)
	}
	if got := sanitizeStage(""); got != "unknown" {
		t.Fatalf("expected unknown stage for empty value, got %s", got)
	}
	if got := sanitizeStage("!@#$"); !strings.Contains(got, "_") {
		t.Fatalf("expected sanitized special chars to underscores, got %s", got)
	}
}
