package task

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alwitt/tasking/db"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// taskStatesMatch a mock matcher asserting a TaskQueryFilter carries exactly the given
// task-state set (order-insensitive).
func taskStatesMatch(states ...models.TaskStateENUM) interface{} {
	want := map[models.TaskStateENUM]bool{}
	for _, s := range states {
		want[s] = true
	}
	return mock.MatchedBy(func(f db.TaskQueryFilter) bool {
		if len(f.TaskStates) != len(want) {
			return false
		}
		for _, s := range f.TaskStates {
			if !want[s] {
				return false
			}
		}
		return true
	})
}

// execStatesMatch a mock matcher asserting a TaskExecutionQueryFilter carries exactly
// the given execution-state set (order-insensitive).
func execStatesMatch(states ...models.TaskExecutionStateENUM) interface{} {
	want := map[models.TaskExecutionStateENUM]bool{}
	for _, s := range states {
		want[s] = true
	}
	return mock.MatchedBy(func(f db.TaskExecutionQueryFilter) bool {
		if len(f.ExecStates) != len(want) {
			return false
		}
		for _, s := range f.ExecStates {
			if !want[s] {
				return false
			}
		}
		return true
	})
}

// TestPerformMaintenance verifies the maintenance loop issues the correct list queries
// (five phases, each with its own state filter) and dispatches each listed entry to the
// handler matching its state. Every handler is stubbed to fail at its first fetch: a
// handler error is logged-and-continued, so this isolates the loop's list + dispatch
// behavior from the handlers' internals.
func TestPerformMaintenance(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("issues every list query and dispatches by state", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		// One entry per state the loop routes on. IDs are distinct so each handler's
		// fetch (GetTask / GetTaskExecution) is attributable to a specific dispatch.
		pendingTask := taskInState("unit-test-task", models.TaskStatePending)
		cancellingTask := taskInState("unit-test-task", models.TaskStateCancelling)
		activeTask := taskInState("unit-test-task", models.TaskStateActive)
		processedExec := execInstanceFixture(pendingTask, models.TaskExecutionClassImmediate)
		processedExec.ExecutionState = models.TaskExecutionStateProcessed
		failedExec := execInstanceFixture(pendingTask, models.TaskExecutionClassImmediate)
		failedExec.ExecutionState = models.TaskExecutionStateFailed
		scheduledExec := execInstanceFixture(pendingTask, models.TaskExecutionClassScheduled)
		scheduledExec.ExecutionState = models.TaskExecutionStateScheduled
		liveExec := execInstanceFixture(pendingTask, models.TaskExecutionClassImmediate)
		liveExec.ExecutionState = models.TaskExecutionStateProcessing

		// Every transaction (the 5 list phases plus each handler's own) runs against the
		// same mock Database.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))

		// Phase 1: pending + cancelling tasks.
		mockDatabase.EXPECT().
			ListTasks(mock.Anything, taskStatesMatch(
				models.TaskStatePending, models.TaskStateCancelling,
			)).
			Return([]models.Task{pendingTask, cancellingTask}, nil).
			Once()
		// Phase 2: active tasks past their deadline (timed out).
		mockDatabase.EXPECT().
			ListTasks(mock.Anything, mock.MatchedBy(func(f db.TaskQueryFilter) bool {
				return len(f.TaskStates) == 1 &&
					f.TaskStates[0] == models.TaskStateActive &&
					f.TargetDeadline != nil
			})).
			Return([]models.Task{activeTask}, nil).
			Once()
		// Phase 3: processed + failed execution instances.
		mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, execStatesMatch(
				models.TaskExecutionStateProcessed, models.TaskExecutionStateFailed,
			)).
			Return([]models.TaskExecution{processedExec, failedExec}, nil).
			Once()
		// Phase 4: scheduled execution instances due to start.
		mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.MatchedBy(func(f db.TaskExecutionQueryFilter) bool {
				return len(f.ExecStates) == 1 &&
					f.ExecStates[0] == models.TaskExecutionStateScheduled &&
					f.TargetStart != nil
			})).
			Return([]models.TaskExecution{scheduledExec}, nil).
			Once()
		// Phase 5: live execution instances past their deadline (timed out).
		mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.MatchedBy(func(f db.TaskExecutionQueryFilter) bool {
				return len(f.ExecStates) == 5 && f.TargetDeadline != nil
			})).
			Return([]models.TaskExecution{liveExec}, nil).
			Once()

		// Each handler is stubbed to fail at its first fetch. The fetch key (task vs
		// execution ID) proves which handler ran for which listed entry.
		//   - PENDING task     -> processNewPendingTask -> GetTask(pendingTask.ID)
		//   - CANCELLING task   -> processCancelTask     -> GetTask(cancellingTask.ID)
		//   - ACTIVE task       -> processTaskTimeout    -> GetTask(activeTask.ID)
		mockDatabase.EXPECT().GetTask(mock.Anything, pendingTask.ID).Return(models.Task{}, simErr).Once()
		mockDatabase.EXPECT().GetTask(mock.Anything, cancellingTask.ID).Return(models.Task{}, simErr).Once()
		mockDatabase.EXPECT().GetTask(mock.Anything, activeTask.ID).Return(models.Task{}, simErr).Once()
		//   - PROCESSED exec -> processTaskExecutionComplete  -> GetTaskExecution(processedExec.ID)
		//   - FAILED exec    -> processTaskExecutionFailed    -> GetTaskExecution(failedExec.ID)
		//   - SCHEDULED exec -> processTaskExecutionStarting  -> GetTaskExecution(scheduledExec.ID)
		//   - live exec      -> processTaskExecutionTimedOut  -> GetTaskExecution(liveExec.ID)
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, processedExec.ID).
			Return(models.TaskExecution{}, simErr).
			Once()
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, failedExec.ID).
			Return(models.TaskExecution{}, simErr).
			Once()
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, scheduledExec.ID).
			Return(models.TaskExecution{}, simErr).
			Once()
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, liveExec.ID).
			Return(models.TaskExecution{}, simErr).
			Once()

		// Handler errors are logged-and-continued, so the loop still completes cleanly.
		assert.Nil(s.performMaintenance(utCtx))
	})

	t.Run("empty lists dispatch no handlers", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		// All five list phases run and return nothing: no GetTask / GetTaskExecution
		// expectations, so the strict mock fails if any handler is dispatched.
		mockDatabase.EXPECT().ListTasks(mock.Anything, mock.Anything).Return(nil, nil).Twice()
		mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.Anything).
			Return(nil, nil).
			Times(3)

		assert.Nil(s.performMaintenance(utCtx))
	})

	t.Run("a list query failure aborts with a maintenance error", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		// The very first list (pending + cancelling) fails: maintenance aborts before
		// any later phase or handler runs.
		mockDatabase.EXPECT().ListTasks(mock.Anything, mock.Anything).Return(nil, simErr).Once()

		err := s.performMaintenance(utCtx)
		assert.NotNil(err)
		var maintErr models.TaskMaintenanceError
		assert.True(
			errors.As(err, &maintErr), "expected TaskMaintenanceError, got %T: %v", err, err,
		)
	})
}
