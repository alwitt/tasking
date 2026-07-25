package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/alwitt/goutils"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktask "github.com/alwitt/tasking/mocks/task"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// newStepExecUpdateTestScheduler build a white-box workflow schedulerImpl for driving
// applyStepExecutionUpdate directly: the persistence client, the queue sender (for the Process
// Workflow fan-out poke), and the task client (for timed-out task cancellation).
func newStepExecUpdateTestScheduler(
	mockClient *mockdb.Client, ipcSender *mockcommon.IPCMessageSend, taskClient *mocktask.Client,
) *schedulerImpl {
	return &schedulerImpl{
		Component:   goutils.Component{LogTags: log.Fields{"module": "workflow"}},
		wg:          &sync.WaitGroup{},
		persistence: mockClient,
		ipcName:     "workflow-scheduler",
		ipcSender:   ipcSender,
		taskClient:  taskClient,
	}
}

// stepInState a workflow step of the given state belonging to the given workflow.
func stepInState(workflowID string, state models.WorkflowStepStateENUM) models.WorkflowStep {
	step := stepFixture(workflowID)
	step.State = state
	return step
}

// taskInState a task in the given state (used for the live-task cancel path).
func taskInState(state models.TaskStateENUM) models.Task {
	return models.Task{ID: ulid.Make().String(), TaskState: state}
}

// TestApplyStepExecutionUpdate covers the Execution Update reducer: the terminal-only guard, the
// cancellation-wins guard, the idempotency guard, each terminal-outcome branch (COMPLETE settling or
// fanning out, FAILED, TIMED_OUT, CANCELLED), the persistence failure paths, and the non-fatal
// post-commit poke behavior (state-before-poke).
func TestApplyStepExecutionUpdate(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("non-terminal new step state is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		// No transaction is even opened for a non-terminal outcome: mock strictness asserts
		// UseDatabaseInTransaction is never called.
		err := s.applyStepExecutionUpdate(utCtx, ulid.Make().String(), models.WorkflowStepStateRunning)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("fetch step fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		stepID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.
			EXPECT().
			GetWorkflowStep(mock.Anything, stepID).
			Return(models.WorkflowStep{}, simErr)

		err := s.applyStepExecutionUpdate(utCtx, stepID, models.WorkflowStepStateComplete)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("fetch workflow fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(models.Workflow{}, simErr)

		err := s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	// --- Cancellation-wins guard -------------------------------------------------------------

	t.Run(
		"cancellation wins: COMPLETE outcome marks step CANCELLED, settles workflow",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			workflow := workflowFixture(models.WorkflowStateCancelling)
			step := stepInState(workflow.ID, models.WorkflowStepStateRunning)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
			// Cancellation wins: the step is marked CANCELLED, not COMPLETE.
			mockDatabase.EXPECT().
				MarkWorkflowStepCancelled(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
				Return(nil)
			// settleWorkflowIfDone re-lists: this was the last in-flight step, so the workflow settles.
			settledStep := step
			settledStep.State = models.WorkflowStepStateCancelled
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, workflow.ID).
				Return([]models.WorkflowStep{settledStep}, nil)
			mockDatabase.
				EXPECT().
				MarkWorkflowCancelled(mock.Anything, workflow.ID, mock.Anything).
				Return(nil)

			assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete))
		},
	)

	t.Run(
		"cancellation wins: still-in-flight sibling leaves workflow CANCELLING",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			workflow := workflowFixture(models.WorkflowStateCancelling)
			step := stepInState(workflow.ID, models.WorkflowStepStateRunning)
			sibling := stepInState(workflow.ID, models.WorkflowStepStateCancelling)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
			mockDatabase.EXPECT().
				MarkWorkflowStepCancelled(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
				Return(nil)
			settledStep := step
			settledStep.State = models.WorkflowStepStateCancelled
			// A still-CANCELLING sibling means the workflow is not settled: no MarkWorkflowCancelled.
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, workflow.ID).
				Return([]models.WorkflowStep{settledStep, sibling}, nil)

			assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete))
		},
	)

	t.Run("cancellation wins: already-terminal step is not re-marked", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateCancelling)
		// The step already reached a terminal state (e.g. FAILED before the cancel landed).
		step := stepInState(workflow.ID, models.WorkflowStepStateFailed)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		// No MarkWorkflowStepCancelled: the step is already terminal. Still settle the workflow.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{step}, nil)
		mockDatabase.
			EXPECT().
			MarkWorkflowCancelled(mock.Anything, workflow.ID, mock.Anything).
			Return(nil)

		assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete))
	})

	// --- Idempotency guard -------------------------------------------------------------------

	t.Run("idempotency: already-terminal step is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateComplete)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		// No Mark*, no ListWorkflowSteps, no emit: mock strictness asserts a pure no-op.

		assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete))
	})

	// --- COMPLETE ----------------------------------------------------------------------------

	t.Run("COMPLETE, not all done: mark step, emit Process Workflow", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)
		sibling := stepInState(workflow.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepComplete(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		completedStep := step
		completedStep.State = models.WorkflowStepStateComplete
		// Not every step is COMPLETE (the sibling still runs): no MarkWorkflowComplete.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{completedStep, sibling}, nil)
		// Fan out via Process Workflow after commit.
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil).Once()

		assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete))
	})

	t.Run("COMPLETE, all done: mark step + workflow COMPLETE, no emit", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepComplete(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		completedStep := step
		completedStep.State = models.WorkflowStepStateComplete
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{completedStep}, nil)
		mockDatabase.
			EXPECT().
			MarkWorkflowComplete(mock.Anything, workflow.ID, mock.Anything).
			Return(nil)
		// No EnqueueMessage: the workflow settled, so there is nothing to fan out.

		assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete))
	})

	t.Run("mark step complete fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepComplete(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(simErr)

		err := s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	// --- FAILED ------------------------------------------------------------------------------

	t.Run("FAILED: mark step + workflow FAILED", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepFailed(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		mockDatabase.EXPECT().MarkWorkflowFailed(mock.Anything, workflow.ID, mock.Anything).Return(nil)
		// No emit, no settle: FAILED is a soft stop, fan-out comes from the next Process Workflow.

		assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateFailed))
	})

	t.Run("FAILED: workflow already FAILED is not re-flipped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateFailed)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepFailed(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		// No MarkWorkflowFailed: the workflow is already FAILED.

		assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateFailed))
	})

	// --- TIMED_OUT ---------------------------------------------------------------------------

	t.Run(
		"TIMED_OUT: whole workflow flipped, running task cancelled post-commit",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			workflow := workflowFixture(models.WorkflowStateRunning)
			// The reported step (RUNNING) plus a DEFINED step that never ran; both get flipped.
			reported := stepInState(workflow.ID, models.WorkflowStepStateRunning)
			unstarted := stepInState(workflow.ID, models.WorkflowStepStateDefined)
			liveTask := taskInState(models.TaskStateActive)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, reported.ID).Return(reported, nil)
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
			// timeOutWorkflowSteps: list steps, look up the RUNNING step's live task, flip both steps,
			// flip the workflow.
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, workflow.ID).
				Return([]models.WorkflowStep{reported, unstarted}, nil)
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, reported.ID, true).
				Return(reported, []models.Task{liveTask}, nil)
			mockDatabase.EXPECT().
				MarkWorkflowStepTimedOut(
					mock.Anything, workflow.ID,
					mock.MatchedBy(func(ids []string) bool {
						return len(ids) == 2 &&
							((ids[0] == reported.ID && ids[1] == unstarted.ID) ||
								(ids[0] == unstarted.ID && ids[1] == reported.ID))
					}),
					mock.Anything,
				).
				Return(nil)
			mockDatabase.
				EXPECT().
				MarkWorkflowTimedOut(mock.Anything, workflow.ID, mock.Anything).
				Return(nil)
			// After commit: cancel the still-running step's task.
			mockTasks.EXPECT().CancelTask(mock.Anything, liveTask.ID, mock.Anything).Return(nil)

			assert.Nil(s.applyStepExecutionUpdate(utCtx, reported.ID, models.WorkflowStepStateTimedOut))
		},
	)

	// --- CANCELLED ---------------------------------------------------------------------------

	t.Run("CANCELLED outcome: mark step, settle if applicable", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		// A RUNNING workflow receiving a CANCELLED step outcome: the step is marked, but the
		// workflow does not settle (settleWorkflowIfDone only settles a CANCELLING workflow).
		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepCancelled(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		// settleWorkflowIfDone re-lists but a RUNNING workflow is not a settle-candidate for
		// completion (the just-cancelled step is not COMPLETE), so no workflow mark.
		cancelledStep := step
		cancelledStep.State = models.WorkflowStepStateCancelled
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{cancelledStep}, nil)

		assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateCancelled))
	})

	// --- post-commit poke failures are non-fatal (state-before-poke) -------------------------

	t.Run("emit failure is non-fatal (COMPLETE fan-out)", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)
		sibling := stepInState(workflow.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepComplete(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		completedStep := step
		completedStep.State = models.WorkflowStepStateComplete
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{completedStep, sibling}, nil)
		// The poke fails, but the work is committed: applyStepExecutionUpdate must still return nil.
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr).Once()

		assert.Nil(s.applyStepExecutionUpdate(utCtx, step.ID, models.WorkflowStepStateComplete))
	})

	t.Run("cancel failure is non-fatal (TIMED_OUT)", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		workflow := workflowFixture(models.WorkflowStateRunning)
		reported := stepInState(workflow.ID, models.WorkflowStepStateRunning)
		liveTask := taskInState(models.TaskStateActive)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, reported.ID).Return(reported, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{reported}, nil)
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, reported.ID, true).
			Return(reported, []models.Task{liveTask}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepTimedOut(mock.Anything, workflow.ID, []string{reported.ID}, mock.Anything).
			Return(nil)
		mockDatabase.
			EXPECT().
			MarkWorkflowTimedOut(mock.Anything, workflow.ID, mock.Anything).
			Return(nil)
		// The cancel poke fails, but the work is committed: still returns nil.
		mockTasks.EXPECT().CancelTask(mock.Anything, liveTask.ID, mock.Anything).Return(simErr)

		assert.Nil(s.applyStepExecutionUpdate(utCtx, reported.ID, models.WorkflowStepStateTimedOut))
	})
}
