package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/alwitt/tasking/db"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mockmodels "github.com/alwitt/tasking/mocks/models"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/workflow"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gorm.io/datatypes"
)

// stepTaskParams build the task Parameters blob carrying a step ID.
func stepTaskParams(t *testing.T, stepID string) datatypes.JSON {
	t.Helper()
	raw, err := json.Marshal(models.TaskParameterExecuteWorkflowStep{StepID: stepID})
	assert.Nil(t, err)
	return raw
}

// stepRunnerTask build a Task carrying the given task Parameters.
func stepRunnerTask(taskID string, params datatypes.JSON) models.Task {
	return models.Task{
		ID:                taskID,
		TaskName:          models.WorkflowExecutionTaskName,
		Creator:           models.WorkflowExecutionTaskCreator,
		TaskScheduleClass: models.TaskScheduleClassImmediateOneShot,
		TaskState:         models.TaskStateActive,
		Parameters:        params,
		RetryParams:       models.DefaultTaskRetryParameters(),
	}
}

// wireUseDatabaseInTransaction wire the mock Client to run the callback against the mock Database.
func wireUseDatabaseInTransaction(mockClient *mockdb.Client, mockDatabase *mockdb.Database) {
	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, core func(context.Context, db.Database) error) error {
			return core(ctx, mockDatabase)
		})
}

// TestStepRunnerHappyPath the param parses, step + workflow load, and the handler registered for
// the step's Type is invoked with the loaded workflow and step.
func TestStepRunnerHappyPath(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	assert := assert.New(t)
	utCtx := context.Background()

	stepID := ulid.Make().String()
	workflowID := ulid.Make().String()
	taskID := ulid.Make().String()
	const stepType = "unit-test-step-type"

	step := models.WorkflowStep{ID: stepID, WorkflowID: workflowID, Type: stepType}
	wf := models.Workflow{ID: workflowID}

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	mockHandler := mockmodels.NewWorkflowStepProcessor(t)

	wireUseDatabaseInTransaction(mockClient, mockDatabase)
	mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, stepID).Return(step, nil)
	mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflowID).Return(wf, nil)
	mockHandler.EXPECT().
		ProcessWorkflowStep(mock.Anything, wf, step).
		Return(nil)

	runner, err := workflow.NewRunWorkflowStepTaskProcessor(
		mockClient, map[string]models.WorkflowStepProcessor{stepType: mockHandler},
	)
	assert.Nil(err)

	assert.Nil(runner.ProcessTaskExecution(
		utCtx, stepRunnerTask(taskID, stepTaskParams(t, stepID)), models.TaskExecution{},
	))
}

// TestStepRunnerNoHandler a step whose Type has no registered handler fails with a
// NonRecoverableError (non-retryable). The step and workflow still load; only the dispatch fails.
func TestStepRunnerNoHandler(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	assert := assert.New(t)
	utCtx := context.Background()

	stepID := ulid.Make().String()
	workflowID := ulid.Make().String()
	taskID := ulid.Make().String()

	step := models.WorkflowStep{ID: stepID, WorkflowID: workflowID, Type: "unregistered-type"}
	wf := models.Workflow{ID: workflowID}

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	mockHandler := mockmodels.NewWorkflowStepProcessor(t)

	wireUseDatabaseInTransaction(mockClient, mockDatabase)
	mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, stepID).Return(step, nil)
	mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflowID).Return(wf, nil)

	runner, err := workflow.NewRunWorkflowStepTaskProcessor(
		mockClient, map[string]models.WorkflowStepProcessor{"some-other-type": mockHandler},
	)
	assert.Nil(err)

	gotErr := runner.ProcessTaskExecution(
		utCtx, stepRunnerTask(taskID, stepTaskParams(t, stepID)), models.TaskExecution{},
	)
	assert.NotNil(gotErr)
	var nonRecoverable models.NonRecoverableError
	assert.True(
		errors.As(gotErr, &nonRecoverable), "expected NonRecoverableError, got %T: %v", gotErr, gotErr,
	)
}

// TestStepRunnerHandlerFails a handler failure is wrapped in a StepExecutionError which is
// retryable (NOT a NonRecoverableError) and still exposes the handler's underlying error.
func TestStepRunnerHandlerFails(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	assert := assert.New(t)
	utCtx := context.Background()

	stepID := ulid.Make().String()
	workflowID := ulid.Make().String()
	taskID := ulid.Make().String()
	const stepType = "unit-test-step-type"

	step := models.WorkflowStep{ID: stepID, WorkflowID: workflowID, Type: stepType}
	wf := models.Workflow{ID: workflowID}
	handlerErr := fmt.Errorf("simulated handler failure")

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	mockHandler := mockmodels.NewWorkflowStepProcessor(t)

	wireUseDatabaseInTransaction(mockClient, mockDatabase)
	mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, stepID).Return(step, nil)
	mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflowID).Return(wf, nil)
	mockHandler.EXPECT().ProcessWorkflowStep(mock.Anything, wf, step).Return(handlerErr)

	runner, err := workflow.NewRunWorkflowStepTaskProcessor(
		mockClient, map[string]models.WorkflowStepProcessor{stepType: mockHandler},
	)
	assert.Nil(err)

	gotErr := runner.ProcessTaskExecution(
		utCtx, stepRunnerTask(taskID, stepTaskParams(t, stepID)), models.TaskExecution{},
	)
	assert.NotNil(gotErr)

	var stepExecErr models.StepExecutionError
	assert.True(
		errors.As(gotErr, &stepExecErr), "expected StepExecutionError, got %T: %v", gotErr, gotErr,
	)
	// The handler's error is preserved through the wrapping.
	assert.ErrorIs(gotErr, handlerErr)
	// A handler failure is retryable - it must NOT be a NonRecoverableError.
	var nonRecoverable models.NonRecoverableError
	assert.False(errors.As(gotErr, &nonRecoverable))
}

// TestStepRunnerStepLoadFails a failed step load surfaces as a retryable StepPreprocessError, not
// a NonRecoverableError.
func TestStepRunnerStepLoadFails(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	assert := assert.New(t)
	utCtx := context.Background()

	stepID := ulid.Make().String()
	taskID := ulid.Make().String()
	dbErr := fmt.Errorf("simulated DB failure")

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)

	wireUseDatabaseInTransaction(mockClient, mockDatabase)
	mockDatabase.EXPECT().
		GetWorkflowStep(mock.Anything, stepID).
		Return(models.WorkflowStep{}, dbErr)

	runner, err := workflow.NewRunWorkflowStepTaskProcessor(
		mockClient, map[string]models.WorkflowStepProcessor{},
	)
	assert.Nil(err)

	gotErr := runner.ProcessTaskExecution(
		utCtx, stepRunnerTask(taskID, stepTaskParams(t, stepID)), models.TaskExecution{},
	)
	assert.NotNil(gotErr)

	var preErr models.StepPreprocessError
	assert.True(
		errors.As(gotErr, &preErr), "expected StepPreprocessError, got %T: %v", gotErr, gotErr,
	)
	assert.ErrorIs(gotErr, dbErr)
	var nonRecoverable models.NonRecoverableError
	assert.False(errors.As(gotErr, &nonRecoverable))
}

// TestStepRunnerWorkflowLoadFails a failed parent-workflow load surfaces as a retryable
// StepPreprocessError.
func TestStepRunnerWorkflowLoadFails(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	assert := assert.New(t)
	utCtx := context.Background()

	stepID := ulid.Make().String()
	workflowID := ulid.Make().String()
	taskID := ulid.Make().String()
	dbErr := fmt.Errorf("simulated DB failure")

	step := models.WorkflowStep{ID: stepID, WorkflowID: workflowID, Type: "any-type"}

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)

	wireUseDatabaseInTransaction(mockClient, mockDatabase)
	mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, stepID).Return(step, nil)
	mockDatabase.EXPECT().
		GetWorkflow(mock.Anything, workflowID).
		Return(models.Workflow{}, dbErr)

	runner, err := workflow.NewRunWorkflowStepTaskProcessor(
		mockClient, map[string]models.WorkflowStepProcessor{},
	)
	assert.Nil(err)

	gotErr := runner.ProcessTaskExecution(
		utCtx, stepRunnerTask(taskID, stepTaskParams(t, stepID)), models.TaskExecution{},
	)
	assert.NotNil(gotErr)

	var preErr models.StepPreprocessError
	assert.True(
		errors.As(gotErr, &preErr), "expected StepPreprocessError, got %T: %v", gotErr, gotErr,
	)
	assert.ErrorIs(gotErr, dbErr)
}

// TestStepRunnerMalformedParams a task Parameters blob that will not parse fails immediately as a
// NonRecoverableError (non-retryable) and never touches the DB.
func TestStepRunnerMalformedParams(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	assert := assert.New(t)
	utCtx := context.Background()

	taskID := ulid.Make().String()

	// mockClient with no expectations: the DB must never be touched.
	mockClient := mockdb.NewClient(t)

	runner, err := workflow.NewRunWorkflowStepTaskProcessor(
		mockClient, map[string]models.WorkflowStepProcessor{},
	)
	assert.Nil(err)

	cases := []datatypes.JSON{
		datatypes.JSON([]byte(`{"step_id":`)),    // invalid JSON
		datatypes.JSON([]byte(`{"step_id":""}`)), // missing required step_id -> validation fails
		datatypes.JSON([]byte(`{}`)),             // missing step_id entirely
	}
	for idx, params := range cases {
		gotErr := runner.ProcessTaskExecution(
			utCtx, stepRunnerTask(taskID, params), models.TaskExecution{},
		)
		assert.NotNil(gotErr, "case %d expected an error", idx)
		var nonRecoverable models.NonRecoverableError
		assert.True(
			errors.As(gotErr, &nonRecoverable),
			"case %d expected NonRecoverableError, got %T: %v", idx, gotErr, gotErr,
		)
	}
}

// TestStepRunnerRejectsNilHandler the constructor rejects a nil handler in the registry.
func TestStepRunnerRejectsNilHandler(t *testing.T) {
	assert := assert.New(t)

	mockClient := mockdb.NewClient(t)

	_, err := workflow.NewRunWorkflowStepTaskProcessor(
		mockClient, map[string]models.WorkflowStepProcessor{"bad-type": nil},
	)
	assert.NotNil(err)
}
