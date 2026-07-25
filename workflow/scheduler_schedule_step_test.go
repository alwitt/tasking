package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktask "github.com/alwitt/tasking/mocks/task"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// newScheduleStepTestScheduler build a white-box workflow schedulerImpl for driving
// scheduleWorkflowStep directly: the persistence client plus the task client used to define and
// submit the step's execution task.
func newScheduleStepTestScheduler(
	mockClient *mockdb.Client, taskClient *mocktask.Client,
) *schedulerImpl {
	return &schedulerImpl{
		Component:   goutils.Component{LogTags: log.Fields{"module": "workflow"}},
		wg:          &sync.WaitGroup{},
		persistence: mockClient,
		ipcName:     "workflow-scheduler",
		taskClient:  taskClient,
	}
}

// pendingStepFixture a PENDING workflow step belonging to the given workflow, carrying its own
// deadline and retry parameters (the ones scheduleWorkflowStep must hand to the task).
func pendingStepFixture(workflowID string) models.WorkflowStep {
	maxDelay := 30
	return models.WorkflowStep{
		ID:         ulid.Make().String(),
		Name:       "unit-test-step-" + ulid.Make().String(),
		WorkflowID: workflowID,
		Creator:    "unit-test-creator",
		Type:       "unit-test-step-type",
		State:      models.WorkflowStepStatePending,
		Deadline:   time.Now().UTC().Add(time.Hour),
		RetryParams: models.TaskRetryParameters{
			MaxRetries: 4, InitialDelaySec: 7, MaxDelaySec: &maxDelay, Factor: 1.5,
		},
	}
}

// TestScheduleWorkflowStep covers the Schedule Workflow Step handler: the hard-stop / not-PENDING /
// live-task NOOP guards, the happy define+link+RUNNING+submit dispatch, each persistence failure
// path, and the non-fatal submit-failure behavior (state-before-poke).
func TestScheduleWorkflowStep(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	// A hard-stop workflow state must NOOP: fetch step + workflow, then nothing else. Mock
	// strictness asserts no define / link / mark / submit occurs.
	for _, state := range []models.WorkflowStateENUM{
		models.WorkflowStateTimedOut,
		models.WorkflowStateCancelling,
		models.WorkflowStateCancelled,
		models.WorkflowStateComplete,
	} {
		t.Run(fmt.Sprintf("hard-stop workflow state %s is a no-op", state), func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			taskClient := mocktask.NewClient(t)
			s := newScheduleStepTestScheduler(mockClient, taskClient)

			workflow := workflowFixture(state)
			step := pendingStepFixture(workflow.ID)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
				Return(step, nil, nil)
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)

			assert.Nil(s.scheduleWorkflowStep(utCtx, step.ID))
		})
	}

	// A step that is not PENDING must NOOP (superseded/stale poke).
	for _, stepState := range []models.WorkflowStepStateENUM{
		models.WorkflowStepStateDefined,
		models.WorkflowStepStateRunning,
		models.WorkflowStepStateComplete,
	} {
		t.Run(fmt.Sprintf("step in state %s is a no-op", stepState), func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			taskClient := mocktask.NewClient(t)
			s := newScheduleStepTestScheduler(mockClient, taskClient)

			workflow := workflowFixture(models.WorkflowStateRunning)
			step := pendingStepFixture(workflow.ID)
			step.State = stepState

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
				Return(step, nil, nil)
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)

			assert.Nil(s.scheduleWorkflowStep(utCtx, step.ID))
		})
	}

	t.Run("a live linked task suppresses a duplicate dispatch", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := pendingStepFixture(workflow.ID)
		liveTasks := []models.Task{{ID: ulid.Make().String()}}

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
			Return(step, liveTasks, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		// No define / link / mark / submit: the strict mocks assert the idempotency guard tripped.

		assert.Nil(s.scheduleWorkflowStep(utCtx, step.ID))
	})

	t.Run("happy path defines, links, marks RUNNING, then submits", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := pendingStepFixture(workflow.ID)
		created := models.Task{ID: ulid.Make().String()}

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
			Return(step, nil, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)

		// The step task carries the fixed workflow name/creator, the step's own deadline + retry
		// (Factor included), and only the step ID as its Parameters.
		taskClient.EXPECT().
			DefineImmediateOneShotTask(
				mock.Anything,
				mock.MatchedBy(func(p task.DefineTaskParams) bool {
					param, ok := p.Parameters.(models.TaskParameterExecuteWorkflowStep)
					return p.Name == models.WorkflowExecutionTaskName &&
						p.Creator != nil && *p.Creator == models.WorkflowExecutionTaskCreator &&
						p.Deadline != nil && p.Deadline.Equal(step.Deadline) &&
						p.Retry != nil && p.Retry.MaxRetries == step.RetryParams.MaxRetries &&
						p.Retry.Factor == step.RetryParams.Factor &&
						ok && param.StepID == step.ID
				}),
				mock.Anything,
			).
			Return(created, nil)
		mockDatabase.EXPECT().
			LinkWorkflowStepWithExecutorTask(mock.Anything, step.ID, created.ID).
			Return(nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepRunning(
				mock.Anything, workflow.ID, mock.MatchedBy(func(ids []string) bool {
					return len(ids) == 1 && ids[0] == step.ID
				}), mock.Anything,
			).
			Return(nil)
		// Submit happens AFTER the transaction commits (state-before-poke).
		taskClient.EXPECT().SubmitTask(mock.Anything, created.ID).Return(nil)

		assert.Nil(s.scheduleWorkflowStep(utCtx, step.ID))
	})

	// Each inner persistence failure bubbles up as a fatal WorkflowSchedulerError.
	t.Run("step fetch failure is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		stepID := ulid.Make().String()
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, stepID, true).
			Return(models.WorkflowStep{}, nil, simErr)

		err := s.scheduleWorkflowStep(utCtx, stepID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("workflow fetch failure is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		step := pendingStepFixture(ulid.Make().String())
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
			Return(step, nil, nil)
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, step.WorkflowID).
			Return(models.Workflow{}, simErr)

		err := s.scheduleWorkflowStep(utCtx, step.ID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("task define failure is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := pendingStepFixture(workflow.ID)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
			Return(step, nil, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		taskClient.EXPECT().
			DefineImmediateOneShotTask(mock.Anything, mock.Anything, mock.Anything).
			Return(models.Task{}, simErr)

		err := s.scheduleWorkflowStep(utCtx, step.ID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("link failure is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := pendingStepFixture(workflow.ID)
		created := models.Task{ID: ulid.Make().String()}

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
			Return(step, nil, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		taskClient.EXPECT().
			DefineImmediateOneShotTask(mock.Anything, mock.Anything, mock.Anything).
			Return(created, nil)
		mockDatabase.EXPECT().
			LinkWorkflowStepWithExecutorTask(mock.Anything, step.ID, created.ID).
			Return(simErr)

		err := s.scheduleWorkflowStep(utCtx, step.ID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("mark-running failure is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := pendingStepFixture(workflow.ID)
		created := models.Task{ID: ulid.Make().String()}

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
			Return(step, nil, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		taskClient.EXPECT().
			DefineImmediateOneShotTask(mock.Anything, mock.Anything, mock.Anything).
			Return(created, nil)
		mockDatabase.EXPECT().
			LinkWorkflowStepWithExecutorTask(mock.Anything, step.ID, created.ID).
			Return(nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepRunning(mock.Anything, workflow.ID, mock.Anything, mock.Anything).
			Return(simErr)

		err := s.scheduleWorkflowStep(utCtx, step.ID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("submit failure is non-fatal (state committed, poke lost)", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := pendingStepFixture(workflow.ID)
		created := models.Task{ID: ulid.Make().String()}

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
			Return(step, nil, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		taskClient.EXPECT().
			DefineImmediateOneShotTask(mock.Anything, mock.Anything, mock.Anything).
			Return(created, nil)
		mockDatabase.EXPECT().
			LinkWorkflowStepWithExecutorTask(mock.Anything, step.ID, created.ID).
			Return(nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepRunning(mock.Anything, workflow.ID, mock.Anything, mock.Anything).
			Return(nil)
		// Submit fails, but the driving state is already committed, so the handler swallows it.
		taskClient.EXPECT().SubmitTask(mock.Anything, created.ID).Return(simErr)

		assert.Nil(s.scheduleWorkflowStep(utCtx, step.ID))
	})

	t.Run("past-deadline workflow times out instead of dispatching", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		taskClient := mocktask.NewClient(t)
		s := newScheduleStepTestScheduler(mockClient, taskClient)

		// A RUNNING workflow whose deadline has already passed.
		workflow := workflowFixture(models.WorkflowStateRunning)
		workflow.Deadline = time.Now().UTC().Add(-time.Hour)
		step := pendingStepFixture(workflow.ID)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, step.ID, true).
			Return(step, nil, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		// timeOutWorkflowSteps path: list steps, flip the PENDING step, flip the workflow. No live
		// task (the reported step is only PENDING), so nothing to cancel.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{step}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepTimedOut(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		mockDatabase.EXPECT().MarkWorkflowTimedOut(mock.Anything, workflow.ID, mock.Anything).Return(nil)
		// No DefineImmediateOneShotTask / LinkWorkflowStepWithExecutorTask / MarkWorkflowStepRunning /
		// SubmitTask: the strict mocks assert the dispatch path was skipped entirely.

		assert.Nil(s.scheduleWorkflowStep(utCtx, step.ID))
	})
}
