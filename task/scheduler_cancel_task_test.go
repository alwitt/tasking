package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/tasking/db"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// liveExecStatesFilter the filter cancelOngoingExecInstancesOfTask passes to
// ListTaskExecutions: every live (not-yet-ended) execution state.
var liveExecStatesFilter = db.TaskExecutionQueryFilter{
	ExecStates: []models.TaskExecutionStateENUM{
		models.TaskExecutionStateDefined,
		models.TaskExecutionStateScheduled,
		models.TaskExecutionStateEnqueued,
		models.TaskExecutionStateAcquired,
		models.TaskExecutionStateProcessing,
	},
}

// taskInState a task of the given name fixed to a specific state.
func taskInState(taskName string, state models.TaskStateENUM) models.Task {
	task := pendingTaskFixture(taskName)
	task.TaskState = state
	return task
}

// TestProcessCancelTask covers the cancel handler: the fetch failure, the
// already-resting idempotency guard, the CANCELLING staging fork, each write
// failure, and the fan-out into cancelOngoingExecInstancesOfTask.
func TestProcessCancelTask(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("fetch task fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		taskID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, taskID).Return(models.Task{}, simErr)

		err := s.processCancelTask(utCtx, taskID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("task already resting is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		// A task already in a terminal/resting state has nothing left to cancel.
		task := taskInState("unit-test-task", models.TaskStateComplete)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		// No MarkTaskCancelling / MarkTaskCancelled / list: the guard returns first.

		err := s.processCancelTask(utCtx, task.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("mark cancelling fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := taskInState("unit-test-task", models.TaskStateActive)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskCancelling(mock.Anything, task.ID).Return(simErr)

		err := s.processCancelTask(utCtx, task.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("mark cancelled fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := taskInState("unit-test-task", models.TaskStateActive)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskCancelling(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskCancelled(mock.Anything, task.ID).Return(simErr)

		err := s.processCancelTask(utCtx, task.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("already cancelling skips the cancelling staging step", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		// A CANCELLING task is already staged: MarkTaskCancelling must NOT be called.
		task := taskInState("unit-test-task", models.TaskStateCancelling)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskCancelled(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveExecStatesFilter).
			Return(nil, nil)

		err := s.processCancelTask(utCtx, task.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("listing ongoing instances to cancel fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := taskInState("unit-test-task", models.TaskStateActive)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskCancelling(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskCancelled(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveExecStatesFilter).
			Return(nil, simErr)

		err := s.processCancelTask(utCtx, task.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("cancelling an ongoing instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := taskInState("unit-test-task", models.TaskStateActive)
		sibling := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		sibling.ExecutionState = models.TaskExecutionStateEnqueued

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskCancelling(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskCancelled(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveExecStatesFilter).
			Return([]models.TaskExecution{sibling}, nil)
		mockDatabase.EXPECT().
			MarkTaskExecCancelled(mock.Anything, sibling.ID, mock.Anything, mock.Anything).
			Return(simErr)

		err := s.processCancelTask(utCtx, task.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("happy path: pending task staged, cancelled, and siblings cancelled", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		// PENDING drives the staging path: MarkTaskCancelling then MarkTaskCancelled.
		task := taskInState("unit-test-task", models.TaskStatePending)
		sibling := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		sibling.ExecutionState = models.TaskExecutionStateEnqueued

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskCancelling(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskCancelled(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveExecStatesFilter).
			Return([]models.TaskExecution{sibling}, nil)
		mockDatabase.EXPECT().
			MarkTaskExecCancelled(mock.Anything, sibling.ID, mock.Anything, mock.Anything).
			Return(nil)

		err := s.processCancelTask(utCtx, task.ID, time.Now().UTC())
		assert.Nil(err)
	})
}

// TestProcessTaskTimeout covers the task-timeout handler: the fetch failure, the
// not-ACTIVE idempotency guard, the MarkTaskTimedOut write, and the fan-out into
// cancelOngoingExecInstancesOfTask.
func TestProcessTaskTimeout(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("fetch task fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		taskID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, taskID).Return(models.Task{}, simErr)

		err := s.processTaskTimeout(utCtx, taskID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("task not active is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		// Only an ACTIVE task can time out; a PENDING task must be skipped.
		task := taskInState("unit-test-task", models.TaskStatePending)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		// No MarkTaskTimedOut / list: the guard returns before any mutation.

		err := s.processTaskTimeout(utCtx, task.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("mark task timed out fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := taskInState("unit-test-task", models.TaskStateActive)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskTimedOut(mock.Anything, task.ID).Return(simErr)

		err := s.processTaskTimeout(utCtx, task.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("listing ongoing instances to cancel fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := taskInState("unit-test-task", models.TaskStateActive)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskTimedOut(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveExecStatesFilter).
			Return(nil, simErr)

		err := s.processTaskTimeout(utCtx, task.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("cancelling an ongoing instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := taskInState("unit-test-task", models.TaskStateActive)
		sibling := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		sibling.ExecutionState = models.TaskExecutionStateEnqueued

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskTimedOut(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveExecStatesFilter).
			Return([]models.TaskExecution{sibling}, nil)
		mockDatabase.EXPECT().
			MarkTaskExecCancelled(mock.Anything, sibling.ID, mock.Anything, mock.Anything).
			Return(simErr)

		err := s.processTaskTimeout(utCtx, task.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("happy path: task timed out and siblings cancelled", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := taskInState("unit-test-task", models.TaskStateActive)
		sibling := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		sibling.ExecutionState = models.TaskExecutionStateEnqueued

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskTimedOut(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveExecStatesFilter).
			Return([]models.TaskExecution{sibling}, nil)
		mockDatabase.EXPECT().
			MarkTaskExecCancelled(mock.Anything, sibling.ID, mock.Anything, mock.Anything).
			Return(nil)

		err := s.processTaskTimeout(utCtx, task.ID, time.Now().UTC())
		assert.Nil(err)
	})
}
