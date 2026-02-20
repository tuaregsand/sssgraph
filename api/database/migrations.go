package database

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type migrationScript struct {
	Version int64
	Name    string
	UpSQL   string
	DownSQL string
}

func RunMigrations(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("database handle is nil")
	}

	if err := ensureSchemaMigrationsTable(db); err != nil {
		return err
	}

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	applied, err := loadAppliedVersions(db)
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		if _, ok := applied[migration.Version]; ok {
			continue
		}

		tx := db.Begin()
		if tx.Error != nil {
			return tx.Error
		}

		if err := tx.Exec(migration.UpSQL).Error; err != nil {
			_ = tx.Rollback().Error
			return fmt.Errorf("failed to apply migration %04d_%s: %w", migration.Version, migration.Name, err)
		}

		if err := tx.Exec(
			"INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, NOW())",
			migration.Version,
			migration.Name,
		).Error; err != nil {
			_ = tx.Rollback().Error
			return fmt.Errorf("failed to record migration %04d_%s: %w", migration.Version, migration.Name, err)
		}

		if err := tx.Commit().Error; err != nil {
			return fmt.Errorf("failed to commit migration %04d_%s: %w", migration.Version, migration.Name, err)
		}
	}

	return nil
}

func RollbackLastMigration(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("database handle is nil")
	}

	if err := ensureSchemaMigrationsTable(db); err != nil {
		return err
	}

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	byVersion := make(map[int64]migrationScript, len(migrations))
	for _, migration := range migrations {
		byVersion[migration.Version] = migration
	}

	var version sql.NullInt64
	row := db.Raw("SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").Row()
	if err := row.Scan(&version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to inspect latest migration: %w", err)
	}
	if !version.Valid {
		return nil
	}

	migration, ok := byVersion[version.Int64]
	if !ok {
		return fmt.Errorf("missing migration script for applied version %d", version.Int64)
	}

	tx := db.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	if err := tx.Exec(migration.DownSQL).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("failed to rollback migration %04d_%s: %w", migration.Version, migration.Name, err)
	}

	if err := tx.Exec("DELETE FROM schema_migrations WHERE version = ?", migration.Version).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("failed to remove migration record %04d_%s: %w", migration.Version, migration.Name, err)
	}

	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("failed to commit rollback %04d_%s: %w", migration.Version, migration.Name, err)
	}

	return nil
}

func VerifySchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("database handle is nil")
	}

	requiredColumns := map[string][]string{
		"idls": {
			"program_id",
			"name",
			"content",
			"created_at",
			"updated_at",
		},
		"agent_webhooks": {
			"id",
			"url",
			"program_id",
			"event_type",
			"is_active",
			"created_at",
			"updated_at",
		},
		"agent_automations": {
			"id",
			"name",
			"program_id",
			"query_template",
			"comparator",
			"threshold",
			"schedule_minutes",
			"window_minutes",
			"cooldown_minutes",
			"dedupe_minutes",
			"retry_max_attempts",
			"retry_initial_backoff_ms",
			"dry_run",
			"webhook_url",
			"is_active",
			"last_evaluated_at",
			"last_triggered_at",
			"last_trigger_fingerprint",
			"last_error",
			"created_at",
			"updated_at",
		},
		"agent_automation_evaluations": {
			"id",
			"automation_id",
			"value",
			"threshold",
			"comparator",
			"triggered",
			"alert_sent",
			"delivery_state",
			"reason",
			"query",
			"window_from",
			"window_to",
			"evaluated_at",
			"dry_run",
			"dedupe_key",
			"retry_attempts",
			"error",
			"created_at",
		},
	}

	requiredIndexes := map[string][]string{
		"agent_webhooks": {
			"idx_agent_webhooks_program_id",
			"idx_agent_webhooks_event_type",
			"idx_agent_webhooks_is_active",
		},
		"agent_automations": {
			"idx_agent_automations_program_id",
			"idx_agent_automations_is_active",
			"idx_agent_automations_due",
		},
		"agent_automation_evaluations": {
			"idx_agent_automation_evaluations_automation_id",
			"idx_agent_automation_evaluations_evaluated_at",
			"idx_agent_automation_evaluations_automation_eval",
		},
	}

	migrator := db.Migrator()
	for table, columns := range requiredColumns {
		if !migrator.HasTable(table) {
			return fmt.Errorf("required table missing: %s", table)
		}
		for _, column := range columns {
			if !migrator.HasColumn(table, column) {
				return fmt.Errorf("required column missing: %s.%s", table, column)
			}
		}
	}

	for table, indexes := range requiredIndexes {
		for _, index := range indexes {
			if !migrator.HasIndex(table, index) {
				return fmt.Errorf("required index missing: %s.%s", table, index)
			}
		}
	}

	var purgeFunctionExists bool
	if err := db.Raw(
		`SELECT EXISTS (
			SELECT 1
			FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE p.proname = 'purge_agent_automation_evaluations'
			AND n.nspname = current_schema()
		)`,
	).Scan(&purgeFunctionExists).Error; err != nil {
		return fmt.Errorf("failed to validate purge function: %w", err)
	}
	if !purgeFunctionExists {
		return fmt.Errorf("required function missing: purge_agent_automation_evaluations(interval)")
	}

	return nil
}

func ensureSchemaMigrationsTable(db *gorm.DB) error {
	return db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`).Error
}

func loadAppliedVersions(db *gorm.DB) (map[int64]struct{}, error) {
	type migrationRecord struct {
		Version int64
	}

	var records []migrationRecord
	if err := db.Raw("SELECT version FROM schema_migrations").Scan(&records).Error; err != nil {
		return nil, fmt.Errorf("failed to read applied migrations: %w", err)
	}

	versions := make(map[int64]struct{}, len(records))
	for _, record := range records {
		versions[record.Version] = struct{}{}
	}

	return versions, nil
}

func loadMigrations() ([]migrationScript, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to read migration directory: %w", err)
	}

	type pair struct {
		name string
		up   string
		down string
	}
	pairs := map[int64]pair{}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		version, name, direction, ok := parseMigrationFilename(entry.Name())
		if !ok {
			continue
		}

		content, readErr := migrationFS.ReadFile("migrations/" + entry.Name())
		if readErr != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %w", entry.Name(), readErr)
		}

		current := pairs[version]
		if current.name != "" && current.name != name {
			return nil, fmt.Errorf("migration version %d has conflicting names: %s vs %s", version, current.name, name)
		}
		current.name = name

		switch direction {
		case "up":
			current.up = string(content)
		case "down":
			current.down = string(content)
		default:
			return nil, fmt.Errorf("unsupported migration direction: %s", direction)
		}

		pairs[version] = current
	}

	if len(pairs) == 0 {
		return nil, fmt.Errorf("no migrations discovered")
	}

	versions := make([]int64, 0, len(pairs))
	for version := range pairs {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[i] < versions[j]
	})

	migrations := make([]migrationScript, 0, len(versions))
	for _, version := range versions {
		pair := pairs[version]
		if strings.TrimSpace(pair.up) == "" || strings.TrimSpace(pair.down) == "" {
			return nil, fmt.Errorf("migration %04d_%s must include both up and down SQL", version, pair.name)
		}

		migrations = append(migrations, migrationScript{
			Version: version,
			Name:    pair.name,
			UpSQL:   pair.up,
			DownSQL: pair.down,
		})
	}

	return migrations, nil
}

func parseMigrationFilename(fileName string) (version int64, name string, direction string, ok bool) {
	trimmed := strings.TrimSpace(fileName)
	switch {
	case strings.HasSuffix(trimmed, ".up.sql"):
		direction = "up"
		trimmed = strings.TrimSuffix(trimmed, ".up.sql")
	case strings.HasSuffix(trimmed, ".down.sql"):
		direction = "down"
		trimmed = strings.TrimSuffix(trimmed, ".down.sql")
	default:
		return 0, "", "", false
	}

	separator := strings.Index(trimmed, "_")
	if separator <= 0 || separator == len(trimmed)-1 {
		return 0, "", "", false
	}

	versionValue, err := strconv.ParseInt(trimmed[:separator], 10, 64)
	if err != nil {
		return 0, "", "", false
	}

	name = trimmed[separator+1:]
	if name == "" {
		return 0, "", "", false
	}

	return versionValue, name, direction, true
}
