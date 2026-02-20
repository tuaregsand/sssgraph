package handlers

import (
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

func TestResolveAgentQueryMode(t *testing.T) {
	mode, execute, err := resolveAgentQueryMode("", false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != agentQueryModeSQL || execute {
		t.Fatalf("unexpected mode resolution: mode=%s execute=%v", mode, execute)
	}

	mode, execute, err = resolveAgentQueryMode("verified", false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != agentQueryModeVerified || !execute {
		t.Fatalf("expected verified mode with forced execute, got mode=%s execute=%v", mode, execute)
	}

	mode, execute, err = resolveAgentQueryMode("sql", false, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != agentQueryModeReport || !execute {
		t.Fatalf("expected strict to force analyst report mode, got mode=%s execute=%v", mode, execute)
	}

	if _, _, err := resolveAgentQueryMode("unknown", false, false); err == nil {
		t.Fatalf("expected invalid mode error")
	}
}

func TestBuildOpsGrowthAnalystReportIncludesSections(t *testing.T) {
	rows := []map[string]any{
		{
			"event_count": 1200,
			"volume":      99.5,
		},
	}

	report := buildOpsGrowthAnalystReport(
		"show me activity",
		"SELECT count(*) AS event_count FROM solana_events",
		rows,
		"solana_events",
		100,
		buildQueryEvidenceBundle(
			"SELECT count(*) AS event_count FROM solana_events",
			"solana_events",
			100,
			rows,
			time.Now().UTC(),
		),
	)

	if report["report_version"] != "1.0" {
		t.Fatalf("expected report_version=1.0, got %v", report["report_version"])
	}
	if _, ok := report["executive_summary"].(string); !ok {
		t.Fatalf("expected executive_summary string")
	}
	if _, ok := report["verdict"].(string); !ok {
		t.Fatalf("expected verdict string")
	}
	if _, ok := report["confidence"].(float64); !ok {
		t.Fatalf("expected confidence number")
	}
	if _, ok := report["kpis"].([]fiber.Map); !ok {
		t.Fatalf("expected kpis list")
	}
	if _, ok := report["evidence"].(fiber.Map); !ok {
		t.Fatalf("expected evidence object")
	}
}

func TestSampleResultRowsCapsOutput(t *testing.T) {
	rows := []map[string]any{
		{"value": 1},
		{"value": 2},
		{"value": 3},
	}
	sampled := sampleResultRows(rows, 2)
	if len(sampled) != 2 {
		t.Fatalf("expected 2 sampled rows, got %d", len(sampled))
	}
}

func TestBuildQueryEvidenceBundleIncludesHashesAndIdentifiers(t *testing.T) {
	rows := []map[string]any{
		{
			"program_id": "Program11111111111111111111111111111111",
			"signature":  "SigA",
			"slot":       123.0,
		},
		{
			"program_id": "Program11111111111111111111111111111111",
			"signature":  "SigB",
			"slot":       124.0,
		},
	}

	evidence := buildQueryEvidenceBundle(
		"SELECT * FROM solana_events",
		"solana_events",
		50,
		rows,
		time.Now().UTC(),
	)

	hash, ok := evidence["sql_hash_sha256"].(string)
	if !ok || hash == "" {
		t.Fatalf("expected sql_hash_sha256")
	}
	programIDs, ok := evidence["program_ids"].([]string)
	if !ok || len(programIDs) != 1 {
		t.Fatalf("expected one unique program_id, got %v", evidence["program_ids"])
	}
	signatures, ok := evidence["signatures"].([]string)
	if !ok || len(signatures) != 2 {
		t.Fatalf("expected two signatures, got %v", evidence["signatures"])
	}
	slots, ok := evidence["slots"].([]uint64)
	if !ok || len(slots) != 2 {
		t.Fatalf("expected two slots, got %v", evidence["slots"])
	}
}
