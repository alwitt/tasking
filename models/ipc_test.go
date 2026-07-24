package models_test

import (
	"testing"
	"time"

	"github.com/alwitt/tasking/models"
	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newIPCValidator build a validator with the model macros installed (ParseIPCMessage needs the
// ipc_msg_type / workflow_step_state macros).
func newIPCValidator(t *testing.T) *validator.Validate {
	t.Helper()
	validate := validator.New()
	require.NoError(t, models.RegisterWithValidator(validate))
	return validate
}

// parseEnvelope marshal an envelope to its wire payload and parse it back through
// ParseIPCMessage.
func parseEnvelope(
	t *testing.T, validate *validator.Validate, env interface{ StringPayload() (string, error) },
) (interface{}, error) {
	t.Helper()
	payload, err := env.StringPayload()
	require.NoError(t, err)
	return models.ParseIPCMessage(validate, []byte(payload))
}

// TestParseWorkflowProcessAndCancel Process/Cancel share the IPCMessageWorkflow shape and are
// distinguished by Type.
func TestParseWorkflowProcessAndCancel(t *testing.T) {
	assert := assert.New(t)
	validate := newIPCValidator(t)
	ts := time.Now().UTC()

	t.Run("process", func(t *testing.T) {
		workflowID := ulid.Make().String()
		parsed, err := parseEnvelope(
			t, validate, models.PrepareIPCMsgWFProcessWorkflow("unit-test", workflowID, ts),
		)
		assert.NoError(err)
		typed, ok := parsed.(models.IPCMessageWorkflow)
		assert.True(ok, "expected IPCMessageWorkflow, got %T", parsed)
		assert.Equal(models.IPCMsgTypeWFProcessWorkflow, typed.Type)
		assert.Equal(workflowID, typed.WorkflowID)
		assert.Equal("unit-test", typed.Sender)
	})

	t.Run("cancel", func(t *testing.T) {
		workflowID := ulid.Make().String()
		parsed, err := parseEnvelope(
			t, validate, models.PrepareIPCMsgWFCancelWorkflow("unit-test", workflowID, ts),
		)
		assert.NoError(err)
		typed, ok := parsed.(models.IPCMessageWorkflow)
		assert.True(ok, "expected IPCMessageWorkflow, got %T", parsed)
		assert.Equal(models.IPCMsgTypeWFCancelWorkflow, typed.Type)
		assert.Equal(workflowID, typed.WorkflowID)
	})
}

// TestParseWorkflowScheduleStep round-trips a Schedule Workflow Step message.
func TestParseWorkflowScheduleStep(t *testing.T) {
	assert := assert.New(t)
	validate := newIPCValidator(t)
	stepID := ulid.Make().String()

	parsed, err := parseEnvelope(
		t, validate,
		models.PrepareIPCMsgWFScheduleStep("unit-test", stepID, time.Now().UTC()),
	)
	assert.NoError(err)
	typed, ok := parsed.(models.IPCMessageWorkflowStep)
	assert.True(ok, "expected IPCMessageWorkflowStep, got %T", parsed)
	assert.Equal(models.IPCMsgTypeWFScheduleStep, typed.Type)
	assert.Equal(stepID, typed.StepID)
}

// TestParseWorkflowStepExecUpdate round-trips an Execution Update carrying a resolved step state.
func TestParseWorkflowStepExecUpdate(t *testing.T) {
	assert := assert.New(t)
	validate := newIPCValidator(t)
	stepID := ulid.Make().String()

	parsed, err := parseEnvelope(
		t, validate,
		models.PrepareIPCMsgWFStepExecUpdate(
			"unit-test", stepID, models.WorkflowStepStateComplete, time.Now().UTC(),
		),
	)
	assert.NoError(err)
	typed, ok := parsed.(models.IPCMessageWorkflowStepExecUpdate)
	assert.True(ok, "expected IPCMessageWorkflowStepExecUpdate, got %T", parsed)
	assert.Equal(models.IPCMsgTypeWFStepExecUpdate, typed.Type)
	assert.Equal(stepID, typed.StepID)
	assert.Equal(models.WorkflowStepStateComplete, typed.NewStepState)
}

// TestParseWorkflowReviveWithAndWithoutDeadline the optional new_deadline round-trips as present
// or nil.
func TestParseWorkflowReviveWithAndWithoutDeadline(t *testing.T) {
	assert := assert.New(t)
	validate := newIPCValidator(t)

	t.Run("with deadline", func(t *testing.T) {
		workflowID := ulid.Make().String()
		deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
		parsed, err := parseEnvelope(
			t, validate,
			models.PrepareIPCMsgWFReviveWorkflow(
				"unit-test", workflowID, &deadline, time.Now().UTC(),
			),
		)
		assert.NoError(err)
		typed, ok := parsed.(models.IPCMessageWorkflowRevive)
		assert.True(ok, "expected IPCMessageWorkflowRevive, got %T", parsed)
		assert.Equal(models.IPCMsgTypeWFReviveWorkflow, typed.Type)
		assert.Equal(workflowID, typed.WorkflowID)
		if assert.NotNil(typed.NewDeadline) {
			assert.True(deadline.Equal(*typed.NewDeadline))
		}
	})

	t.Run("without deadline", func(t *testing.T) {
		workflowID := ulid.Make().String()
		parsed, err := parseEnvelope(
			t, validate,
			models.PrepareIPCMsgWFReviveWorkflow("unit-test", workflowID, nil, time.Now().UTC()),
		)
		assert.NoError(err)
		typed, ok := parsed.(models.IPCMessageWorkflowRevive)
		assert.True(ok, "expected IPCMessageWorkflowRevive, got %T", parsed)
		assert.Equal(workflowID, typed.WorkflowID)
		assert.Nil(typed.NewDeadline)
	})
}

// TestParseWorkflowMaintenance the payload-less maintenance message parses back as the base
// message.
func TestParseWorkflowMaintenance(t *testing.T) {
	assert := assert.New(t)
	validate := newIPCValidator(t)

	parsed, err := parseEnvelope(
		t, validate, models.PrepareIPCMsgWFMaintenance("unit-test", time.Now().UTC()),
	)
	assert.NoError(err)
	typed, ok := parsed.(models.BaseIPCMessage)
	assert.True(ok, "expected BaseIPCMessage, got %T", parsed)
	assert.Equal(models.IPCMsgTypeWFMaintenance, typed.Type)
	assert.Equal("unit-test", typed.Sender)
}

// TestParseWorkflowMessagesRejectInvalid missing required fields / bad enum values fail
// validation.
func TestParseWorkflowMessagesRejectInvalid(t *testing.T) {
	assert := assert.New(t)
	validate := newIPCValidator(t)
	ts := time.Now().UTC()

	t.Run("missing workflow_id", func(t *testing.T) {
		// Empty workflow ID -> the required tag fails.
		_, err := parseEnvelope(
			t, validate, models.PrepareIPCMsgWFProcessWorkflow("unit-test", "", ts),
		)
		assert.Error(err)
	})

	t.Run("missing step_id", func(t *testing.T) {
		_, err := parseEnvelope(
			t, validate, models.PrepareIPCMsgWFScheduleStep("unit-test", "", ts),
		)
		assert.Error(err)
	})

	t.Run("bad new_step_state", func(t *testing.T) {
		// An unresolvable step state fails the workflow_step_state macro.
		_, err := parseEnvelope(
			t, validate,
			models.PrepareIPCMsgWFStepExecUpdate(
				"unit-test", ulid.Make().String(), models.WorkflowStepStateENUM("NOT_A_STATE"), ts,
			),
		)
		assert.Error(err)
	})
}
