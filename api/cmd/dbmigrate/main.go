package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/laserstream/api/config"
	"github.com/laserstream/api/database"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	_ = godotenv.Load()

	command := "up"
	if len(os.Args) > 1 {
		command = strings.ToLower(strings.TrimSpace(os.Args[1]))
	}

	cfg := config.Load()
	if strings.TrimSpace(cfg.Database.DSN()) == "" {
		log.Fatal("database DSN is empty")
	}

	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	switch command {
	case "up":
		if err := database.RunMigrations(db); err != nil {
			log.Fatalf("migration failed: %v", err)
		}
		fmt.Println("migrations applied successfully")
	case "down":
		if err := database.RollbackLastMigration(db); err != nil {
			log.Fatalf("rollback failed: %v", err)
		}
		fmt.Println("rolled back latest migration successfully")
	case "verify":
		if err := database.VerifySchema(db); err != nil {
			log.Fatalf("schema verification failed: %v", err)
		}
		fmt.Println("schema verification passed")
	default:
		log.Fatalf("unsupported command: %s (use: up | down | verify)", command)
	}
}
