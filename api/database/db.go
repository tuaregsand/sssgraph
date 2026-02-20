package database

import (
	"context"
	"fmt"
	"time"

	"github.com/laserstream/api/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

func ConnectDB(cfg config.DatabaseConfig) error {
	dsn := cfg.DSN()
	if dsn == "" {
		return fmt.Errorf("database DSN is empty")
	}

	var err error
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect database: %w", err)
	}

	if cfg.RunMigrations {
		if err := RunMigrations(DB); err != nil {
			return fmt.Errorf("failed to run database migrations: %w", err)
		}
	}

	if cfg.RequireSchema {
		if err := VerifySchema(DB); err != nil {
			return fmt.Errorf("database schema validation failed: %w", err)
		}
	}

	return nil
}

func Ping(ctx context.Context) error {
	if DB == nil {
		return fmt.Errorf("database is not initialized")
	}

	sqlDB, err := DB.DB()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	return sqlDB.PingContext(ctx)
}

func Close() error {
	if DB == nil {
		return nil
	}

	sqlDB, err := DB.DB()
	if err != nil {
		return err
	}

	return sqlDB.Close()
}
