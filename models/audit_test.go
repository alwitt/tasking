package models_test

import (
	"testing"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/models"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"gorm.io/datatypes"
)

// auditTestValidator build a validator wired with the model validation macros, matching how
// ParseMetadata is called in production.
func auditTestValidator(t *testing.T) *validator.Validate {
	v := validator.New()
	assert.Nil(t, models.RegisterWithValidator(v))
	return v
}

// TestSystemEventAuditParseMetadata verifies ParseMetadata unmarshals an audit entry's raw
// metadata into the struct matching its event type, and reports the right error class for
// unsupported types, unparsable JSON, and metadata that fails validation.
func TestSystemEventAuditParseMetadata(t *testing.T) {
	validator := auditTestValidator(t)

	// ------------------------------------------------------------------------------------
	// Task-event group: every one of these types parses into SystemEventTaskEvents.

	taskEventTypes := []models.SystemEventTypeENUM{
		models.SystemEventTypeActivateTask,
		models.SystemEventTypeCompleteTask,
		models.SystemEventTypeFailedTask,
		models.SystemEventTypeCancelledTask,
		models.SystemEventTypeTimedOutTask,
		models.SystemEventTypeDeleteTask,
	}
	for _, eventType := range taskEventTypes {
		t.Run(string(eventType)+" parses task-event metadata", func(t *testing.T) {
			assert := assert.New(t)

			entry := models.SystemEventAudit{
				EventType: eventType,
				Metadata: datatypes.JSON(
					`{"task_id": "unit-test-task-id", "creator": "unit-test-creator"}`,
				),
			}

			parsed, err := entry.ParseMetadata(validator)
			assert.Nil(err)
			taskEvent, ok := parsed.(models.SystemEventTaskEvents)
			assert.True(ok, "expected SystemEventTaskEvents, got %T", parsed)
			assert.Equal("unit-test-task-id", taskEvent.TaskID)
			assert.Equal("unit-test-creator", taskEvent.Creator)
		})
	}

	t.Run("task-event metadata missing task_id fails validation", func(t *testing.T) {
		assert := assert.New(t)

		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeFailedTask,
			Metadata:  datatypes.JSON(`{}`),
		}

		_, err := entry.ParseMetadata(validator)
		assert.NotNil(err)
		var validationErr goutils.ValidationError
		assert.ErrorAs(err, &validationErr)
	})

	// ------------------------------------------------------------------------------------
	// Invalid task IPC message

	t.Run("INVALID_TASK_IPC_MESSAGE parses its metadata", func(t *testing.T) {
		assert := assert.New(t)

		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeInvalidTaskIPCMessage,
			Metadata: datatypes.JSON(
				`{"receiver": "scheduler", "raw_message": "{bad}", "reason": "unparsable message"}`,
			),
		}

		parsed, err := entry.ParseMetadata(validator)
		assert.Nil(err)
		invalidMsg, ok := parsed.(models.SystemEventInvalidTaskIPCMessage)
		assert.True(ok, "expected SystemEventInvalidTaskIPCMessage, got %T", parsed)
		assert.Equal("scheduler", invalidMsg.Receiver)
		assert.Equal("{bad}", invalidMsg.RawMessage)
		assert.Equal("unparsable message", invalidMsg.Reason)
	})

	t.Run("INVALID_TASK_IPC_MESSAGE allows an empty raw_message", func(t *testing.T) {
		assert := assert.New(t)

		// RawMessage is not `required`: an unreadable payload legitimately has none.
		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeInvalidTaskIPCMessage,
			Metadata: datatypes.JSON(
				`{"receiver": "scheduler", "raw_message": "", "reason": "unreadable payload"}`,
			),
		}

		parsed, err := entry.ParseMetadata(validator)
		assert.Nil(err)
		invalidMsg, ok := parsed.(models.SystemEventInvalidTaskIPCMessage)
		assert.True(ok, "expected SystemEventInvalidTaskIPCMessage, got %T", parsed)
		assert.Equal("", invalidMsg.RawMessage)
	})

	t.Run("INVALID_TASK_IPC_MESSAGE missing receiver fails validation", func(t *testing.T) {
		assert := assert.New(t)

		// Receiver is `required`.
		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeInvalidTaskIPCMessage,
			Metadata:  datatypes.JSON(`{"reason": "unreadable payload"}`),
		}

		_, err := entry.ParseMetadata(validator)
		assert.NotNil(err)
		var validationErr goutils.ValidationError
		assert.ErrorAs(err, &validationErr)
	})

	// ------------------------------------------------------------------------------------
	// Engine-failed task

	t.Run("ENGINE_FAILED_TASK parses its metadata", func(t *testing.T) {
		assert := assert.New(t)

		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeEngineFailedTask,
			Metadata: datatypes.JSON(
				`{"task_id": "unit-test-task-id", "instance_id": "unit-test-instance-id", ` +
					`"reason": "could not claim instance", "creator": "unit-test-creator"}`,
			),
		}

		parsed, err := entry.ParseMetadata(validator)
		assert.Nil(err)
		engineFailed, ok := parsed.(models.SystemEventEngineFailedTask)
		assert.True(ok, "expected SystemEventEngineFailedTask, got %T", parsed)
		assert.Equal("unit-test-task-id", engineFailed.TaskID)
		assert.Equal("unit-test-instance-id", engineFailed.InstanceID)
		assert.Equal("could not claim instance", engineFailed.Reason)
		assert.Equal("unit-test-creator", engineFailed.Creator)
	})

	t.Run("ENGINE_FAILED_TASK missing instance_id fails validation", func(t *testing.T) {
		assert := assert.New(t)

		// Both task_id and instance_id are `required`.
		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeEngineFailedTask,
			Metadata:  datatypes.JSON(`{"task_id": "unit-test-task-id"}`),
		}

		_, err := entry.ParseMetadata(validator)
		assert.NotNil(err)
		var validationErr goutils.ValidationError
		assert.ErrorAs(err, &validationErr)
	})

	// ------------------------------------------------------------------------------------
	// Workflow-event group: every one of these types parses into SystemEventWorkflowEvents.

	workflowEventTypes := []models.SystemEventTypeENUM{
		models.SystemEventTypeDefineWorkflow,
		models.SystemEventTypeWorkflowRunning,
		models.SystemEventTypeWorkflowComplete,
		models.SystemEventTypeWorkflowFailed,
		models.SystemEventTypeWorkflowTimedOut,
		models.SystemEventTypeWorkflowCancelling,
		models.SystemEventTypeWorkflowCancelled,
		models.SystemEventTypeWorkflowDeadlineUpdate,
		models.SystemEventTypeDeleteWorkflow,
	}
	for _, eventType := range workflowEventTypes {
		t.Run(string(eventType)+" parses workflow-event metadata", func(t *testing.T) {
			assert := assert.New(t)

			entry := models.SystemEventAudit{
				EventType: eventType,
				Metadata: datatypes.JSON(
					`{"workflow_id": "unit-test-workflow-id", "creator": "unit-test-creator"}`,
				),
			}

			parsed, err := entry.ParseMetadata(validator)
			assert.Nil(err)
			wfEvent, ok := parsed.(models.SystemEventWorkflowEvents)
			assert.True(ok, "expected SystemEventWorkflowEvents, got %T", parsed)
			assert.Equal("unit-test-workflow-id", wfEvent.WorkflowID)
			assert.Equal("unit-test-creator", wfEvent.Creator)
		})
	}

	t.Run("workflow-event metadata missing workflow_id fails validation", func(t *testing.T) {
		assert := assert.New(t)

		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeWorkflowFailed,
			Metadata:  datatypes.JSON(`{"creator": "unit-test-creator"}`),
		}

		_, err := entry.ParseMetadata(validator)
		assert.NotNil(err)
		var validationErr goutils.ValidationError
		assert.ErrorAs(err, &validationErr)
	})

	// ------------------------------------------------------------------------------------
	// Workflow-step-event group: every type parses into SystemEventWorkflowStepEvents.

	workflowStepEventTypes := []models.SystemEventTypeENUM{
		models.SystemEventTypeWorkflowStepDefined,
		models.SystemEventTypeWorkflowStepPending,
		models.SystemEventTypeWorkflowStepRunning,
		models.SystemEventTypeWorkflowStepComplete,
		models.SystemEventTypeWorkflowStepFailed,
		models.SystemEventTypeWorkflowStepTimedOut,
		models.SystemEventTypeWorkflowStepCancelling,
		models.SystemEventTypeWorkflowStepCancelled,
	}
	for _, eventType := range workflowStepEventTypes {
		t.Run(string(eventType)+" parses workflow-step-event metadata", func(t *testing.T) {
			assert := assert.New(t)

			entry := models.SystemEventAudit{
				EventType: eventType,
				Metadata: datatypes.JSON(
					`{"step_id": "unit-test-step-id", "workflow_id": "unit-test-workflow-id", ` +
						`"creator": "unit-test-creator"}`,
				),
			}

			parsed, err := entry.ParseMetadata(validator)
			assert.Nil(err)
			stepEvent, ok := parsed.(models.SystemEventWorkflowStepEvents)
			assert.True(ok, "expected SystemEventWorkflowStepEvents, got %T", parsed)
			assert.Equal("unit-test-step-id", stepEvent.StepID)
			assert.Equal("unit-test-workflow-id", stepEvent.WorkflowID)
			assert.Equal("unit-test-creator", stepEvent.Creator)
		})
	}

	t.Run("workflow-step-event metadata missing step_id fails validation", func(t *testing.T) {
		assert := assert.New(t)

		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeWorkflowStepFailed,
			Metadata: datatypes.JSON(
				`{"workflow_id": "unit-test-workflow-id", "creator": "unit-test-creator"}`,
			),
		}

		_, err := entry.ParseMetadata(validator)
		assert.NotNil(err)
		var validationErr goutils.ValidationError
		assert.ErrorAs(err, &validationErr)
	})

	// ------------------------------------------------------------------------------------
	// Error paths shared across event types

	t.Run("unparsable metadata is a consistency error", func(t *testing.T) {
		assert := assert.New(t)

		// Well-known event type, but the metadata JSON is malformed.
		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeFailedTask,
			Metadata:  datatypes.JSON(`{not valid json`),
		}

		_, err := entry.ParseMetadata(validator)
		assert.NotNil(err)
		var consistencyErr goutils.ConsistencyError
		assert.ErrorAs(err, &consistencyErr)
	})

	t.Run("unsupported event type is a consistency error", func(t *testing.T) {
		assert := assert.New(t)

		entry := models.SystemEventAudit{
			EventType: models.SystemEventTypeENUM("NOT_A_REAL_EVENT_TYPE"),
			Metadata:  datatypes.JSON(`{"task_id": "unit-test-task-id"}`),
		}

		_, err := entry.ParseMetadata(validator)
		assert.NotNil(err)
		var consistencyErr goutils.ConsistencyError
		assert.ErrorAs(err, &consistencyErr)
	})
}
