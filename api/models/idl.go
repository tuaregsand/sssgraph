package models

import (
	"encoding/json"
	"time"
)

// IDL represents an Anchor IDL stored in the database
type IDL struct {
	ProgramID string          `json:"program_id" gorm:"primaryKey;size:64"`
	Name      string          `json:"name"`
	Content   json.RawMessage `json:"content" gorm:"type:jsonb;not null"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}
