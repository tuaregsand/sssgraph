package handlers

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	sqlDangerousPattern = regexp.MustCompile(`(?i)\b(insert|update|delete|drop|alter|truncate|optimize|system|create|attach|detach|grant|revoke|kill|rename)\b`)
	sqlFormatPattern    = regexp.MustCompile(`(?is)\s+format\s+[a-z_0-9]+\s*$`)
)

func normalizeReadOnlySQL(raw string) (string, error) {
	sql := strings.TrimSpace(raw)
	if sql == "" {
		return "", errors.New("sql query is empty")
	}

	sql = strings.TrimSuffix(sql, ";")
	sql = strings.TrimSpace(sql)
	sql = stripMarkdownFences(sql)
	sql = stripTrailingFormatClause(sql)
	if sql == "" {
		return "", errors.New("sql query is empty")
	}

	lowered := strings.ToLower(sql)
	if strings.Contains(lowered, "--") || strings.Contains(lowered, "/*") || strings.Contains(lowered, "*/") {
		return "", errors.New("sql comments are not allowed")
	}
	if strings.Count(sql, ";") > 0 {
		return "", errors.New("multiple statements are not allowed")
	}

	if !(strings.HasPrefix(lowered, "select") || strings.HasPrefix(lowered, "with")) {
		return "", errors.New("only read-only SELECT queries are allowed")
	}
	if sqlDangerousPattern.MatchString(sql) {
		return "", errors.New("query contains unsupported keywords")
	}

	return sql, nil
}

func wrapQueryForJSONRows(sql string, limit int) string {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	return fmt.Sprintf("SELECT * FROM (%s) LIMIT %d FORMAT JSONEachRow", sql, limit)
}

func renderAutomationQuery(template, programID string, from, to time.Time) string {
	sql := strings.TrimSpace(template)
	sql = strings.ReplaceAll(sql, "{{program_id}}", escapeSQLLiteral(programID))
	sql = strings.ReplaceAll(sql, "{{from}}", from.UTC().Format(time.RFC3339))
	sql = strings.ReplaceAll(sql, "{{to}}", to.UTC().Format(time.RFC3339))
	return sql
}

func extractFirstNumeric(rows []map[string]any) (float64, error) {
	if len(rows) == 0 {
		return 0, errors.New("query returned zero rows")
	}

	row := rows[0]
	for _, value := range row {
		number, ok := toFloat64(value)
		if ok {
			return number, nil
		}
	}

	return 0, errors.New("first row does not contain a numeric field")
}

func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func compareAgainstThreshold(value float64, comparator string, threshold float64) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(comparator)) {
	case "gt":
		return value > threshold, nil
	case "gte":
		return value >= threshold, nil
	case "lt":
		return value < threshold, nil
	case "lte":
		return value <= threshold, nil
	default:
		return false, fmt.Errorf("unsupported comparator: %s", comparator)
	}
}

func isSupportedComparator(comparator string) bool {
	_, err := compareAgainstThreshold(1, comparator, 1)
	return err == nil
}

func validateWebhookURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid webhook url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("webhook url must use http or https")
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return errors.New("webhook url host is required")
	}
	return nil
}

func stripMarkdownFences(sql string) string {
	sql = strings.TrimSpace(sql)
	if strings.HasPrefix(sql, "```sql") {
		sql = strings.TrimSpace(strings.TrimPrefix(sql, "```sql"))
	}
	if strings.HasPrefix(sql, "```") {
		sql = strings.TrimSpace(strings.TrimPrefix(sql, "```"))
	}
	sql = strings.TrimSuffix(sql, "```")
	return strings.TrimSpace(sql)
}

func stripTrailingFormatClause(sql string) string {
	return strings.TrimSpace(sqlFormatPattern.ReplaceAllString(sql, ""))
}

func queryReferencesTable(sql, table string) bool {
	table = strings.TrimSpace(table)
	if table == "" {
		return true
	}

	loweredSQL := strings.ToLower(sql)
	loweredTable := strings.ToLower(table)
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(loweredTable) + `\b`)
	return pattern.MatchString(loweredSQL)
}
