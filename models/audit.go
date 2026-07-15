// Package models - system data models
package models

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	"github.com/go-playground/validator/v10"
	"gorm.io/datatypes"
)

// SystemEventTypeENUM system event type ENUM value type
type SystemEventTypeENUM string

// Various system event types which will be captured
const (
	// SystemEventTypeActivateTask activated a system task
	SystemEventTypeActivateTask SystemEventTypeENUM = "ACTIVATE_TASK"
	// SystemEventTypeCompleteTask complete a system task
	SystemEventTypeCompleteTask SystemEventTypeENUM = "COMPLETE_TASK"
	// SystemEventTypeFailedTask failed a system task
	SystemEventTypeFailedTask SystemEventTypeENUM = "FAILED_TASK"
	// SystemEventTypeCancelledTask cancelled a system task
	SystemEventTypeCancelledTask SystemEventTypeENUM = "CANCELLED_TASK"
	// SystemEventTypeTimedOutTask timed out a system task
	SystemEventTypeTimedOutTask SystemEventTypeENUM = "TIMED_OUT_TASK"
	// SystemEventTypeDeleteTask deleted a system task
	SystemEventTypeDeleteTask SystemEventTypeENUM = "DELETE_TASK"
)

// Values all valid SystemEventTypeENUM values
func (SystemEventTypeENUM) Values() []SystemEventTypeENUM {
	return []SystemEventTypeENUM{
		SystemEventTypeActivateTask,
		SystemEventTypeCompleteTask,
		SystemEventTypeFailedTask,
		SystemEventTypeCancelledTask,
		SystemEventTypeTimedOutTask,
		SystemEventTypeDeleteTask,
	}
}

// SystemEventAudit recording of events occurring at the system level
type SystemEventAudit struct {
	// ID audit entry ID
	ID string `json:"id" gorm:"column:id;primaryKey;unique" validate:"required"`
	// EventType system event type
	EventType SystemEventTypeENUM `json:"type" gorm:"column:type;not null" validate:"required,system_event"`
	// Metadata a metadata relating to the event
	Metadata datatypes.JSON `json:"metadata,omitempty" gorm:"column:metadata;default:null"`
	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// ParseMetadata parse the metadata based on the event type
func (a SystemEventAudit) ParseMetadata(validator *validator.Validate) (interface{}, error) {
	validate := func(parsed interface{}) error {
		if err := validator.Struct(parsed); err != nil {
			return goutils.NewValidationError(
				fmt.Sprintf("system event '%s' metadata validation failed", a.EventType), err, true,
			)
		}
		return nil
	}
	switch a.EventType {
	case SystemEventTypeActivateTask:
		fallthrough
	case SystemEventTypeCompleteTask:
		fallthrough
	case SystemEventTypeFailedTask:
		fallthrough
	case SystemEventTypeCancelledTask:
		fallthrough
	case SystemEventTypeTimedOutTask:
		fallthrough
	case SystemEventTypeDeleteTask:
		var parsed SystemEventTaskEvents
		if err := json.Unmarshal(a.Metadata, &parsed); err != nil {
			return nil, goutils.NewConsistencyError(
				fmt.Sprintf("system event '%s' metadata parse failed", a.EventType), err, true,
			)
		}
		return parsed, validate(&parsed)
	default:
		return nil, goutils.NewConsistencyError(
			fmt.Sprintf("unsupported system event type '%s'", a.EventType), nil, true,
		)
	}
}

// SystemEventTaskEvents system task related system event
type SystemEventTaskEvents struct {
	// TaskID task ID
	TaskID string `json:"task_id" validate:"required"`
}
