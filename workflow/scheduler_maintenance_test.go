package workflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	goutilsRedis "github.com/alwitt/goutils/redis"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktask "github.com/alwitt/tasking/mocks/task"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// envType extract the IPC message type of an enqueued envelope for order/shape assertions.
func envType(env goutilsRedis.QueueMessageEnvelope) models.IPCMessageTypeEnum {
	switch typed := env.(type) {
	case models.IPCMessageWorkflow:
		return typed.Type
	case models.IPCMessageWorkflowStep:
		return typed.Type
	case models.IPCMessageWorkflowStepExecUpdate:
		return typed.Type
	default:
		return ""
	}
}

// TestMapTaskStateToStepState covers the task-state -> step-state map the sweep uses to reconcile a
// step against its linked task's persisted state: terminal task states map to their step outcome
// and report terminal; live states report non-terminal.
func TestMapTaskStateToStepState(t *testing.T) {
	assert := assert.New(t)

	terminal := map[models.TaskStateENUM]models.WorkflowStepStateENUM{
		models.TaskStateComplete:  models.WorkflowStepStateComplete,
		models.TaskStateFailed:    models.WorkflowStepStateFailed,
		models.TaskStateTimeout:   models.WorkflowStepStateTimedOut,
		models.TaskStateCancelled: models.WorkflowStepStateCancelled,
	}
	for taskState, want := range terminal {
		got, isTerminal := mapTaskStateToStepState(taskState)
		assert.True(isTerminal, "task state %s should be terminal", taskState)
		assert.Equal(want, got, "task state %s", taskState)
	}

	for _, live := range []models.TaskStateENUM{
		models.TaskStatePending, models.TaskStateActive, models.TaskStateCancelling,
	} {
		_, isTerminal := mapTaskStateToStepState(live)
		assert.False(isTerminal, "task state %s should be live", live)
	}
}

// TestReconcileWorkflow covers the per-workflow reconciliation: the past-deadline short-circuit,
// each per-step classifier row (DEFINED / PENDING / RUNNING live|terminal|zombie / CANCELLING
// terminal|live), the PENDING-workflow re-drive, the aggregate settle, the ordered post-commit poke
// emission, the persistence failure path, and the non-fatal poke behavior.
func TestReconcileWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run(
		"past deadline short-circuits to whole-workflow timeout, cancels running task",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			wf := workflowFixture(models.WorkflowStateRunning)
			wf.Deadline = time.Now().UTC().Add(-time.Hour) // in the past
			runningStep := stepInState(wf.ID, models.WorkflowStepStateRunning)
			liveTask := taskInState(models.TaskStateActive)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
			// timeOutWorkflowSteps lists steps, collects the RUNNING step's live task, times
			// everything out.
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return([]models.WorkflowStep{runningStep}, nil)
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, runningStep.ID, true).
				Return(runningStep, []models.Task{liveTask}, nil)
			mockDatabase.EXPECT().
				MarkWorkflowStepTimedOut(mock.Anything, wf.ID, []string{runningStep.ID}, mock.Anything).
				Return(nil)
			mockDatabase.EXPECT().MarkWorkflowTimedOut(mock.Anything, wf.ID, mock.Anything).Return(nil)
			// The still-RUNNING task is cancelled post-commit. No per-step reconciliation pokes.
			mockTasks.EXPECT().CancelTask(mock.Anything, liveTask.ID, mock.Anything).Return(nil)

			assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
		},
	)

	t.Run("PENDING workflow with a DEFINED step: one process-workflow poke", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStatePending)
		definedStep := stepInState(wf.ID, models.WorkflowStepStateDefined)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{definedStep}, nil)
		// PENDING workflow is not RUNNING/FAILED/CANCELLING, so settleWorkflowIfDone does NOT re-list.
		// Exactly one Process Workflow poke (PENDING workflow + DEFINED step collapse to one).
		var enqueued []models.IPCMessageTypeEnum
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Run(func(_ context.Context, env goutilsRedis.QueueMessageEnvelope) {
				enqueued = append(enqueued, envType(env))
			}).
			Return(nil).
			Once()

		assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
		assert.Equal([]models.IPCMessageTypeEnum{models.IPCMsgTypeWFProcessWorkflow}, enqueued)
	})

	t.Run("PENDING step: one schedule-step poke", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateRunning)
		pendingStep := stepInState(wf.ID, models.WorkflowStepStatePending)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{pendingStep}, nil).
			Once()
		// RUNNING workflow -> settleWorkflowIfDone re-lists; a PENDING step keeps it un-settled.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{pendingStep}, nil).
			Once()
		var enqueued []models.IPCMessageTypeEnum
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Run(func(_ context.Context, env goutilsRedis.QueueMessageEnvelope) {
				enqueued = append(enqueued, envType(env))
			}).
			Return(nil).
			Once()

		assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
		assert.Equal([]models.IPCMessageTypeEnum{models.IPCMsgTypeWFScheduleStep}, enqueued)
	})

	t.Run("RUNNING step with a live task: left alone, no poke", func(t *testing.T) {
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
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{runningStep}, nil).
			Once()
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, runningStep.ID, false).
			Return(runningStep, []models.Task{liveTask}, nil)
		// Settle re-list: the RUNNING step keeps the workflow un-settled.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{runningStep}, nil).
			Once()
		// No pokes: the live task's feedback will come.

		assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
	})

	t.Run(
		"RUNNING step with a terminal task: synthesized execution update from task state",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			wf := workflowFixture(models.WorkflowStateRunning)
			runningStep := stepInState(wf.ID, models.WorkflowStepStateRunning)
			doneTask := taskInState(models.TaskStateComplete)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return([]models.WorkflowStep{runningStep}, nil).
				Once()
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, runningStep.ID, false).
				Return(runningStep, []models.Task{doneTask}, nil)
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return([]models.WorkflowStep{runningStep}, nil).
				Once()
			// A synthesized COMPLETE execution update for the still-RUNNING step (lost feedback).
			var captured models.IPCMessageWorkflowStepExecUpdate
			mockSender.EXPECT().
				EnqueueMessage(mock.Anything, mock.Anything).
				Run(func(_ context.Context, env goutilsRedis.QueueMessageEnvelope) {
					captured = env.(models.IPCMessageWorkflowStepExecUpdate)
				}).
				Return(nil).
				Once()

			assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
			assert.Equal(runningStep.ID, captured.StepID)
			assert.Equal(models.WorkflowStepStateComplete, captured.NewStepState)
		},
	)

	t.Run(
		"RUNNING step re-run after revive: drives the most-recent (current) task's outcome",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			wf := workflowFixture(models.WorkflowStateRunning)
			runningStep := stepInState(wf.ID, models.WorkflowStepStateRunning)
			// A revived-and-re-run step keeps its prior attempt's task alongside the current one.
			// GetWorkflowStepAndExecutorTask orders most-recent-first, so the current attempt is
			// tasks[0].
			currentTask := taskInState(models.TaskStateComplete)
			staleTask := taskInState(models.TaskStateFailed)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return([]models.WorkflowStep{runningStep}, nil).
				Once()
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, runningStep.ID, false).
				Return(runningStep, []models.Task{currentTask, staleTask}, nil)
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return([]models.WorkflowStep{runningStep}, nil).
				Once()
			var captured models.IPCMessageWorkflowStepExecUpdate
			mockSender.EXPECT().
				EnqueueMessage(mock.Anything, mock.Anything).
				Run(func(_ context.Context, env goutilsRedis.QueueMessageEnvelope) {
					captured = env.(models.IPCMessageWorkflowStepExecUpdate)
				}).
				Return(nil).
				Once()

			assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
			assert.Equal(runningStep.ID, captured.StepID)
			// The current attempt COMPLETE wins over the stale FAILED attempt.
			assert.Equal(models.WorkflowStepStateComplete, captured.NewStepState)
		},
	)

	t.Run("RUNNING step with no task (zombie): synthesized FAILED", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateRunning)
		runningStep := stepInState(wf.ID, models.WorkflowStepStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{runningStep}, nil).
			Once()
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, runningStep.ID, false).
			Return(runningStep, []models.Task{}, nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{runningStep}, nil).
			Once()
		var captured models.IPCMessageWorkflowStepExecUpdate
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Run(func(_ context.Context, env goutilsRedis.QueueMessageEnvelope) {
				captured = env.(models.IPCMessageWorkflowStepExecUpdate)
			}).
			Return(nil).
			Once()

		assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
		assert.Equal(runningStep.ID, captured.StepID)
		assert.Equal(models.WorkflowStepStateFailed, captured.NewStepState)
	})

	t.Run("CANCELLING step with a terminal task: synthesized CANCELLED", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateCancelling)
		cancellingStep := stepInState(wf.ID, models.WorkflowStepStateCancelling)
		doneTask := taskInState(models.TaskStateCancelled)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{cancellingStep}, nil).
			Once()
		mockDatabase.EXPECT().
			GetWorkflowStepAndExecutorTask(mock.Anything, cancellingStep.ID, false).
			Return(cancellingStep, []models.Task{doneTask}, nil)
		// CANCELLING workflow -> settleWorkflowIfDone re-lists; the still-CANCELLING step keeps it
		// un-settled (the synthesized CANCELLED lands on a later tick).
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{cancellingStep}, nil).
			Once()
		var captured models.IPCMessageWorkflowStepExecUpdate
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Run(func(_ context.Context, env goutilsRedis.QueueMessageEnvelope) {
				captured = env.(models.IPCMessageWorkflowStepExecUpdate)
			}).
			Return(nil).
			Once()

		assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
		assert.Equal(cancellingStep.ID, captured.StepID)
		assert.Equal(models.WorkflowStepStateCancelled, captured.NewStepState)
	})

	t.Run(
		"CANCELLING step with a live task: re-issues cancel, no execution update", func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			wf := workflowFixture(models.WorkflowStateCancelling)
			cancellingStep := stepInState(wf.ID, models.WorkflowStepStateCancelling)
			liveTask := taskInState(models.TaskStateActive)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return([]models.WorkflowStep{cancellingStep}, nil).
				Once()
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, cancellingStep.ID, false).
				Return(cancellingStep, []models.Task{liveTask}, nil)
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return([]models.WorkflowStep{cancellingStep}, nil).
				Once()
			// The live task's cancel is re-issued post-commit; no execution update.
			mockTasks.EXPECT().CancelTask(mock.Anything, liveTask.ID, mock.Anything).Return(nil)

			assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
		},
	)

	t.Run("aggregate settle: all steps COMPLETE flips workflow to COMPLETE", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateRunning)
		completeStep := stepInState(wf.ID, models.WorkflowStepStateComplete)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{completeStep}, nil).
			Once()
		// Settle re-list: every step COMPLETE -> mark the workflow COMPLETE. No pokes.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{completeStep}, nil).
			Once()
		mockDatabase.EXPECT().MarkWorkflowComplete(mock.Anything, wf.ID, mock.Anything).Return(nil)

		assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
	})

	t.Run(
		"poke emission order: all exec updates, then schedule steps, then process workflow",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			mockTasks := mocktask.NewClient(t)
			s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

			wf := workflowFixture(models.WorkflowStateRunning)
			// -> process workflow
			definedStep := stepInState(wf.ID, models.WorkflowStepStateDefined)
			// -> schedule step
			pendingStep := stepInState(wf.ID, models.WorkflowStepStatePending)
			// -> exec update (lost feedback)
			runningStep := stepInState(wf.ID, models.WorkflowStepStateRunning)
			doneTask := taskInState(models.TaskStateComplete)
			steps := []models.WorkflowStep{definedStep, pendingStep, runningStep}

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return(steps, nil).
				Once()
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, runningStep.ID, false).
				Return(runningStep, []models.Task{doneTask}, nil)
			// Settle re-list: non-terminal steps remain, workflow not settled.
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return(steps, nil).
				Once()

			var order []models.IPCMessageTypeEnum
			mockSender.EXPECT().
				EnqueueMessage(mock.Anything, mock.Anything).
				Run(func(_ context.Context, env goutilsRedis.QueueMessageEnvelope) {
					order = append(order, envType(env))
				}).
				Return(nil).
				Times(3)

			assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
			assert.Equal([]models.IPCMessageTypeEnum{
				models.IPCMsgTypeWFStepExecUpdate,
				models.IPCMsgTypeWFScheduleStep,
				models.IPCMsgTypeWFProcessWorkflow,
			}, order)
		},
	)

	t.Run("DB error inside the transaction is fatal", func(t *testing.T) {
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
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(models.Workflow{}, simErr)

		err := s.reconcileWorkflow(utCtx, wf.ID)
		assert.Error(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("post-commit poke failure is non-fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		wf := workflowFixture(models.WorkflowStateRunning)
		pendingStep := stepInState(wf.ID, models.WorkflowStepStatePending)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{pendingStep}, nil).
			Once()
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{pendingStep}, nil).
			Once()
		// The committed state stands; a failed poke is logged, not returned.
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr).Once()

		assert.NoError(s.reconcileWorkflow(utCtx, wf.ID))
	})
}

// TestRunMaintenanceSweep covers the sweep driver: per-workflow failure isolation (one workflow's
// failure does not stop the others) and the fatal listing error.
func TestRunMaintenanceSweep(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("one workflow failing does not stop the others", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		badWf := workflowFixture(models.WorkflowStateRunning)
		goodWf := workflowFixture(models.WorkflowStateRunning)
		goodStep := stepInState(goodWf.ID, models.WorkflowStepStateComplete)

		// The listing transaction returns both workflows.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase)).
			Once()
		mockDatabase.EXPECT().
			ListWorkflows(mock.Anything, mock.Anything).
			Return([]models.Workflow{badWf, goodWf}, nil).
			Once()

		// Each workflow is reconciled in its own transaction, in listed order.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase)).
			Once()
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, badWf.ID).
			Return(models.Workflow{}, simErr).
			Once() // bad workflow: reconcile fails, logged and skipped

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase)).
			Once()
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, goodWf.ID).Return(goodWf, nil).Once()
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, goodWf.ID).
			Return([]models.WorkflowStep{goodStep}, nil).
			Once()
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, goodWf.ID).
			Return([]models.WorkflowStep{goodStep}, nil).
			Once()
		mockDatabase.EXPECT().
			MarkWorkflowComplete(mock.Anything, goodWf.ID, mock.Anything).
			Return(nil).
			Once()

		// The sweep swallows the per-workflow failure and returns nil.
		assert.NoError(s.runMaintenanceSweep(utCtx))
	})

	t.Run("listing failure is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			ListWorkflows(mock.Anything, mock.Anything).
			Return(nil, simErr)

		err := s.runMaintenanceSweep(utCtx)
		assert.Error(err)
		assertWorkflowSchedulerError(t, err)
	})
}
