package handlers

import "testing"

func TestGrowthRatio(t *testing.T) {
	cases := []struct {
		name       string
		baseline   float64
		evaluation float64
		expected   float64
	}{
		{name: "normal ratio", baseline: 100, evaluation: 250, expected: 2.5},
		{name: "zero baseline zero eval", baseline: 0, evaluation: 0, expected: 1.0},
		{name: "zero baseline positive eval", baseline: 0, evaluation: 12, expected: 12},
		{name: "drop ratio", baseline: 50, evaluation: 10, expected: 0.2},
	}

	for _, tc := range cases {
		got := growthRatio(tc.baseline, tc.evaluation)
		if got != tc.expected {
			t.Fatalf("%s: expected %v, got %v", tc.name, tc.expected, got)
		}
	}
}

func TestProbabilityBand(t *testing.T) {
	cases := []struct {
		value    float64
		expected string
	}{
		{value: 0.80, expected: "high"},
		{value: 0.60, expected: "medium"},
		{value: 0.20, expected: "low"},
	}

	for _, tc := range cases {
		if got := probabilityBand(tc.value); got != tc.expected {
			t.Fatalf("expected %s for %v, got %s", tc.expected, tc.value, got)
		}
	}
}

func TestClamp(t *testing.T) {
	if got := clamp(-1, 0, 1); got != 0 {
		t.Fatalf("expected clamp lower bound, got %v", got)
	}
	if got := clamp(2, 0, 1); got != 1 {
		t.Fatalf("expected clamp upper bound, got %v", got)
	}
	if got := clamp(0.4, 0, 1); got != 0.4 {
		t.Fatalf("expected unclamped value, got %v", got)
	}
}
