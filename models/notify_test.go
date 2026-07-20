package models_test

import (
	"encoding/json"
	"testing"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/models"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// notifyTestValidator build a validator wired with the model validation macros, matching how
// ParseMetadata is called in production.
func notifyTestValidator(t *testing.T) *validator.Validate {
	v := validator.New()
	require.NoError(t, models.RegisterWithValidator(v))
	return v
}

// TestNotificationEventParseMetadata verifies NotificationEvent.ParseMetadata mirrors
// SystemEventAudit.ParseMetadata: it unmarshals the raw metadata into the struct matching the
// event type, validates it, and reports the right error class for unsupported types.
func TestNotificationEventParseMetadata(t *testing.T) {
	validator := notifyTestValidator(t)

	t.Run("task-event metadata parses into SystemEventTaskEvents", func(t *testing.T) {
		assert := assert.New(t)

		meta, err := json.Marshal(models.SystemEventTaskEvents{
			TaskID: "task-id", Creator: "creator-id",
		})
		require.NoError(t, err)

		event := models.NotificationEvent{
			ID:        "event-id",
			EventType: models.SystemEventTypeCompleteTask,
			Metadata:  meta,
		}

		parsed, err := event.ParseMetadata(validator)
		require.NoError(t, err)
		taskEvent, ok := parsed.(models.SystemEventTaskEvents)
		require.True(t, ok)
		assert.Equal("task-id", taskEvent.TaskID)
		assert.Equal("creator-id", taskEvent.Creator)
	})

	t.Run("engine-failure metadata parses into SystemEventEngineFailedTask", func(t *testing.T) {
		assert := assert.New(t)

		meta, err := json.Marshal(models.SystemEventEngineFailedTask{
			TaskID: "task-id", InstanceID: "instance-id", Reason: "boom", Creator: "creator-id",
		})
		require.NoError(t, err)

		event := models.NotificationEvent{
			ID:        "event-id",
			EventType: models.SystemEventTypeEngineFailedTask,
			Metadata:  meta,
		}

		parsed, err := event.ParseMetadata(validator)
		require.NoError(t, err)
		engineFailed, ok := parsed.(models.SystemEventEngineFailedTask)
		require.True(t, ok)
		assert.Equal("task-id", engineFailed.TaskID)
		assert.Equal("creator-id", engineFailed.Creator)
	})

	t.Run("invalid-IPC metadata parses into SystemEventInvalidTaskIPCMessage", func(t *testing.T) {
		assert := assert.New(t)

		meta, err := json.Marshal(models.SystemEventInvalidTaskIPCMessage{
			Receiver: "scheduler", Reason: "bad message",
		})
		require.NoError(t, err)

		event := models.NotificationEvent{
			ID:        "event-id",
			EventType: models.SystemEventTypeInvalidTaskIPCMessage,
			Metadata:  meta,
		}

		parsed, err := event.ParseMetadata(validator)
		require.NoError(t, err)
		ipcInvalid, ok := parsed.(models.SystemEventInvalidTaskIPCMessage)
		require.True(t, ok)
		assert.Equal("scheduler", ipcInvalid.Receiver)
	})

	t.Run("unsupported event type is a ConsistencyError", func(t *testing.T) {
		assert := assert.New(t)

		event := models.NotificationEvent{
			ID:        "event-id",
			EventType: models.SystemEventTypeENUM("NOT_A_REAL_TYPE"),
			Metadata:  json.RawMessage(`{}`),
		}

		_, err := event.ParseMetadata(validator)
		require.Error(t, err)
		var consistencyErr goutils.ConsistencyError
		assert.ErrorAs(err, &consistencyErr)
	})
}
