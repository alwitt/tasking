package workflow

import (
	"context"
	"fmt"
	"testing"

	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktask "github.com/alwitt/tasking/mocks/task"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestCancelWorkflow covers the Cancel Workflow handler: the mixed-step cancel (RUNNING -> CANCELLING
// with a post-commit task cancel, others -> CANCELLED) that stays draining, the immediate settle to
// CANCELLED when nothing is in-flight, the idempotent NOOP on an already cancelling/terminal
// workflow, the persistence failure path, and the non-fatal post-commit task-cancel failure.
func TestCancelWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("mixed steps: running -> cancelling (task cancelled), others -> cancelled, still draining", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateRunning)
		runningStep := stepInState(wf.ID, models.WorkflowStepStateRunning)
		pendingStep := stepInState(wf.ID, models.WorkflowStepStatePending)
		failedStep := stepInState(wf.ID, models.WorkflowStepStateFailed)
		completeStep := stepInState(wf.ID, models.WorkflowStepStateComplete)
		liveTask := taskInState(models.TaskStateActive)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().MarkWorkflowCancelling(mock.Anything, wf.ID, mock.Anything).Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{runningStep, pendingStep, failedStep, completeStep}, nil).
			Once()
		// The running step's live task is looked up for cancellation.
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, runningStep.ID, true).
			Return(runningStep, []models.Task{liveTask}, nil)
		// RUNNING -> CANCELLING.
		mockDatabase.EXPECT().
			MarkWorkflowStepCancelling(mock.Anything, wf.ID, []string{runningStep.ID}, mock.Anything).
			Return(nil)
		// PENDING + FAILED -> CANCELLED; COMPLETE untouched.
		mockDatabase.EXPECT().
			MarkWorkflowStepCancelled(
				mock.Anything, wf.ID, []string{pendingStep.ID, failedStep.ID}, mock.Anything,
			).
			Return(nil)
		// Settle re-lists: a CANCELLING step remains, so the workflow is NOT flipped to CANCELLED.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{
				stepInState(wf.ID, models.WorkflowStepStateCancelling),
				stepInState(wf.ID, models.WorkflowStepStateCancelled),
				stepInState(wf.ID, models.WorkflowStepStateCancelled),
				completeStep,
			}, nil).
			Once()
		// Post-commit: the live task is cancelled.
		mockTasks.EXPECT().CancelTask(mock.Anything, liveTask.ID, mock.Anything).Return(nil)

		assert.NoError(s.cancelWorkflow(utCtx, wf.ID))
	})

	t.Run("no in-flight steps: settles straight to CANCELLED, no task cancel", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateFailed)
		definedStep := stepInState(wf.ID, models.WorkflowStepStateDefined)
		failedStep := stepInState(wf.ID, models.WorkflowStepStateFailed)
		completeStep := stepInState(wf.ID, models.WorkflowStepStateComplete)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().MarkWorkflowCancelling(mock.Anything, wf.ID, mock.Anything).Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{definedStep, failedStep, completeStep}, nil).
			Once()
		// No RUNNING step -> no GetWorkflowStepAndExecutorTask, no MarkWorkflowStepCancelling.
		mockDatabase.EXPECT().
			MarkWorkflowStepCancelled(
				mock.Anything, wf.ID, []string{definedStep.ID, failedStep.ID}, mock.Anything,
			).
			Return(nil)
		// Settle re-lists: nothing RUNNING / CANCELLING remains -> flip the workflow to CANCELLED.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{
				stepInState(wf.ID, models.WorkflowStepStateCancelled),
				stepInState(wf.ID, models.WorkflowStepStateCancelled),
				completeStep,
			}, nil).
			Once()
		mockDatabase.EXPECT().MarkWorkflowCancelled(mock.Anything, wf.ID, mock.Anything).Return(nil)

		assert.NoError(s.cancelWorkflow(utCtx, wf.ID))
	})

	// A stale / re-delivered cancel on an already cancelling or terminal workflow is a benign NOOP.
	noopStates := []models.WorkflowStateENUM{
		models.WorkflowStateCancelling,
		models.WorkflowStateCancelled,
		models.WorkflowStateComplete,
	}
	for _, state := range noopStates {
		t.Run("NOOP when workflow already "+string(state), func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			wf := workflowFixture(state)
			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
			// No marks, no list, no task cancel.

			assert.NoError(s.cancelWorkflow(utCtx, wf.ID))
		})
	}

	t.Run("DB error on a mark is fatal, no task cancel", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateRunning)
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().
			MarkWorkflowCancelling(mock.Anything, wf.ID, mock.Anything).
			Return(simErr)

		err := s.cancelWorkflow(utCtx, wf.ID)
		assert.Error(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("post-commit task cancel failure is non-fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateRunning)
		runningStep := stepInState(wf.ID, models.WorkflowStepStateRunning)
		liveTask := taskInState(models.TaskStateActive)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().MarkWorkflowCancelling(mock.Anything, wf.ID, mock.Anything).Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{runningStep}, nil).
			Once()
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, runningStep.ID, true).
			Return(runningStep, []models.Task{liveTask}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepCancelling(mock.Anything, wf.ID, []string{runningStep.ID}, mock.Anything).
			Return(nil)
		// Settle re-lists: the CANCELLING step keeps the workflow draining.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{stepInState(wf.ID, models.WorkflowStepStateCancelling)}, nil).
			Once()
		// The committed state stands; a failed cancel is not fatal.
		mockTasks.EXPECT().CancelTask(mock.Anything, liveTask.ID, mock.Anything).Return(simErr)

		assert.NoError(s.cancelWorkflow(utCtx, wf.ID))
	})
}
