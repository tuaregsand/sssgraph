package handlers

import "testing"

func TestNormalizeReadOnlySQLSanitizesFencesAndFormat(t *testing.T) {
	sql, err := normalizeReadOnlySQL("```sql\nSELECT count(*) FROM solana_events FORMAT JSONEachRow\n```;")
	if err != nil {
		t.Fatalf("expected SQL to be accepted, got error: %v", err)
	}

	if sql != "SELECT count(*) FROM solana_events" {
		t.Fatalf("unexpected normalized SQL: %q", sql)
	}
}

func TestNormalizeReadOnlySQLRejectsUnsafeStatements(t *testing.T) {
	cases := []string{
		"DELETE FROM solana_events",
		"SELECT * FROM solana_events; SELECT 1",
		"SELECT * FROM solana_events -- comment",
		"/* x */ SELECT * FROM solana_events",
	}

	for _, raw := range cases {
		if _, err := normalizeReadOnlySQL(raw); err == nil {
			t.Fatalf("expected SQL to be rejected: %q", raw)
		}
	}
}

func TestExtractFirstNumericParsesStringValues(t *testing.T) {
	value, err := extractFirstNumeric([]map[string]any{{"metric": "42.5"}})
	if err != nil {
		t.Fatalf("expected numeric extraction to succeed: %v", err)
	}
	if value != 42.5 {
		t.Fatalf("expected value=42.5, got %v", value)
	}
}

func TestCompareAgainstThreshold(t *testing.T) {
	passed, err := compareAgainstThreshold(10, "gt", 5)
	if err != nil {
		t.Fatalf("expected comparator to be valid: %v", err)
	}
	if !passed {
		t.Fatalf("expected comparison to pass")
	}

	passed, err = compareAgainstThreshold(4, "lte", 4)
	if err != nil {
		t.Fatalf("expected comparator to be valid: %v", err)
	}
	if !passed {
		t.Fatalf("expected comparison to pass for lte")
	}

	if _, err := compareAgainstThreshold(1, "bad", 1); err == nil {
		t.Fatalf("expected invalid comparator error")
	}
}

func TestQueryReferencesTable(t *testing.T) {
	if !queryReferencesTable("SELECT * FROM solana_events WHERE program_id = 'x'", "solana_events") {
		t.Fatalf("expected table reference check to pass")
	}
	if queryReferencesTable("SELECT * FROM other_table", "solana_events") {
		t.Fatalf("expected table reference check to fail")
	}
}
