package models

import (
	"time"
)

// AgentWebhook represents an AI agent destination to stream parsed events.
type AgentWebhook struct {
	ID        uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	URL       string    `json:"url" gorm:"not null"`
	ProgramID string    `json:"program_id" gorm:"index"`
	EventType string    `json:"event_type" gorm:"index"`
	IsActive  bool      `json:"is_active" gorm:"default:true"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// AgentAutomation represents a persisted metric monitor that can notify an AI agent.
type AgentAutomation struct {
	ID                     uint       `json:"id" gorm:"primaryKey;autoIncrement"`
	Name                   string     `json:"name" gorm:"not null"`
	Description            string     `json:"description,omitempty" gorm:"type:text"`
	ProgramID              string     `json:"program_id" gorm:"index"`
	QueryTemplate          string     `json:"query_template" gorm:"type:text;not null"`
	Comparator             string     `json:"comparator" gorm:"size:8;not null"`
	Threshold              float64    `json:"threshold" gorm:"not null"`
	ScheduleMinutes        int        `json:"schedule_minutes" gorm:"not null;default:60"`
	WindowMinutes          int        `json:"window_minutes" gorm:"not null;default:60"`
	CooldownMinutes        int        `json:"cooldown_minutes" gorm:"not null;default:30"`
	DedupeMinutes          int        `json:"dedupe_minutes" gorm:"not null;default:15"`
	RetryMaxAttempts       int        `json:"retry_max_attempts" gorm:"not null;default:3"`
	RetryInitialBackoffMS  int        `json:"retry_initial_backoff_ms" gorm:"not null;default:200"`
	WebhookURL             string     `json:"webhook_url" gorm:"not null"`
	IsActive               bool       `json:"is_active" gorm:"not null;default:true;index"`
	DryRun                 bool       `json:"dry_run" gorm:"not null;default:false"`
	LastValue              *float64   `json:"last_value,omitempty"`
	LastEvaluatedAt        *time.Time `json:"last_evaluated_at,omitempty"`
	LastTriggeredAt        *time.Time `json:"last_triggered_at,omitempty"`
	LastTriggerFingerprint string     `json:"last_trigger_fingerprint,omitempty" gorm:"type:text"`
	LastError              string     `json:"last_error,omitempty" gorm:"type:text"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
}

// AgentAutomationEvaluation captures each automation run for auditability.
type AgentAutomationEvaluation struct {
	ID            uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	AutomationID  uint      `json:"automation_id" gorm:"not null;index"`
	Value         float64   `json:"value" gorm:"not null"`
	Threshold     float64   `json:"threshold" gorm:"not null"`
	Comparator    string    `json:"comparator" gorm:"size:8;not null"`
	Triggered     bool      `json:"triggered" gorm:"not null"`
	AlertSent     bool      `json:"alert_sent" gorm:"not null"`
	DeliveryState string    `json:"delivery_state" gorm:"type:text;not null"`
	Reason        string    `json:"reason" gorm:"type:text;not null"`
	Query         string    `json:"query" gorm:"type:text;not null"`
	WindowFrom    time.Time `json:"window_from" gorm:"not null"`
	WindowTo      time.Time `json:"window_to" gorm:"not null"`
	EvaluatedAt   time.Time `json:"evaluated_at" gorm:"not null;index"`
	DryRun        bool      `json:"dry_run" gorm:"not null;default:false"`
	DedupeKey     string    `json:"dedupe_key,omitempty" gorm:"type:text"`
	RetryAttempts int       `json:"retry_attempts" gorm:"not null;default:0"`
	Error         string    `json:"error,omitempty" gorm:"type:text"`
	CreatedAt     time.Time `json:"created_at"`
}
