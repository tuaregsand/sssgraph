package handlers

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/laserstream/api/auth"
	"github.com/laserstream/api/database"
	"github.com/laserstream/api/models"
	"gorm.io/gorm"
)

type idlPayload struct {
	ProgramID string          `json:"program_id"`
	Name      string          `json:"name"`
	Content   json.RawMessage `json:"content"`
}

func CreateIDL(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	var payload idlPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	payload.ProgramID = strings.TrimSpace(payload.ProgramID)
	payload.Name = strings.TrimSpace(payload.Name)

	if payload.ProgramID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "program_id is required"})
	}
	if err := ensureProgramAccessIfAuthenticated(c, payload.ProgramID); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "forbidden for this program_id"})
	}
	if len(payload.Content) == 0 || !json.Valid(payload.Content) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "content must be a valid JSON object"})
	}

	idl := models.IDL{
		ProgramID: payload.ProgramID,
		Name:      payload.Name,
		Content:   payload.Content,
	}

	if err := database.DB.Create(&idl).Error; err != nil {
		if isDuplicateError(err) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "program_id already exists"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create IDL"})
	}

	return c.Status(fiber.StatusCreated).JSON(idl)
}

func GetIDLs(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	limit := c.QueryInt("limit", 100)
	offset := c.QueryInt("offset", 0)

	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	var idls []models.IDL
	if err := database.DB.Order("updated_at desc").Limit(limit).Offset(offset).Find(&idls).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch IDLs"})
	}

	if _, err := auth.GetPrincipal(c); err == nil {
		filtered := make([]models.IDL, 0, len(idls))
		for _, idl := range idls {
			if accessErr := ensureProgramAccessIfAuthenticated(c, idl.ProgramID); accessErr == nil {
				filtered = append(filtered, idl)
			}
		}
		idls = filtered
	}

	return c.JSON(fiber.Map{
		"items":  idls,
		"limit":  limit,
		"offset": offset,
	})
}

func GetIDL(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	programID := strings.TrimSpace(c.Params("programID"))
	if programID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "programID is required"})
	}
	if err := ensureProgramAccessIfAuthenticated(c, programID); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "forbidden for this program_id"})
	}

	var idl models.IDL
	if err := database.DB.First(&idl, "program_id = ?", programID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "IDL not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch IDL"})
	}

	return c.JSON(idl)
}

func UpdateIDL(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	programID := strings.TrimSpace(c.Params("programID"))
	if programID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "programID is required"})
	}
	if err := ensureProgramAccessIfAuthenticated(c, programID); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "forbidden for this program_id"})
	}

	var payload idlPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
	}

	if payload.ProgramID != "" && strings.TrimSpace(payload.ProgramID) != programID {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "program_id does not match URL parameter"})
	}

	updates := map[string]any{}
	if strings.TrimSpace(payload.Name) != "" {
		updates["name"] = strings.TrimSpace(payload.Name)
	}
	if len(payload.Content) > 0 {
		if !json.Valid(payload.Content) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "content must be valid JSON"})
		}
		updates["content"] = payload.Content
	}

	if len(updates) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "at least one field must be provided"})
	}

	result := database.DB.Model(&models.IDL{}).Where("program_id = ?", programID).Updates(updates)
	if result.Error != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update IDL"})
	}
	if result.RowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "IDL not found"})
	}

	var idl models.IDL
	if err := database.DB.First(&idl, "program_id = ?", programID).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to load updated IDL"})
	}

	return c.JSON(idl)
}

func DeleteIDL(c *fiber.Ctx) error {
	if database.DB == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "database unavailable"})
	}

	programID := strings.TrimSpace(c.Params("programID"))
	if programID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "programID is required"})
	}
	if err := ensureProgramAccessIfAuthenticated(c, programID); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "forbidden for this program_id"})
	}

	result := database.DB.Delete(&models.IDL{}, "program_id = ?", programID)
	if result.Error != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to delete IDL"})
	}
	if result.RowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "IDL not found"})
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func isDuplicateError(err error) bool {
	if err == nil {
		return false
	}

	text := strings.ToLower(err.Error())
	return strings.Contains(text, "duplicate") || strings.Contains(text, "unique")
}

func ensureProgramAccessIfAuthenticated(c *fiber.Ctx, programID string) error {
	principal, err := auth.GetPrincipal(c)
	if err != nil {
		return nil
	}
	return auth.EnsureProgramAccessForPrincipal(principal, programID)
}
