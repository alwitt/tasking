package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/common"
	"github.com/alwitt/tasking/db"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestProcessTaskExecutionStarting covers the execution-starting handler: the two
// fetch failures, the at-or-past-ENQUEUED idempotency guard, and the queue-then-
// dispatch path with each of its failure branches.
func TestProcessTaskExecutionStarting(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	// expectFetch wires GetTaskExecution + GetTask for a successful fetch of the
	// instance and its parent task.
	expectFetch := func(
		mockDatabase *mockdb.Database, instance models.TaskExecution, task models.Task,
	) {
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(task, nil)
	}

	t.Run("fetch execution instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		instanceID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(models.TaskExecution{}, simErr)

		err := s.processTaskExecutionStarting(utCtx, instanceID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("fetch parent task fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(models.Task{}, simErr)

		err := s.processTaskExecutionStarting(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("instance already at or past ENQUEUED is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		// A sender is wired but must never be touched once the guard short-circuits.
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessTestScheduler(
			mockClient, map[string]common.IPCMessageSend{"unit-test-task": mockSender},
		)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		// Already ENQUEUED: a prior delivery (or racing scan) started it.
		instance.ExecutionState = models.TaskExecutionStateEnqueued

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		// No MarkTaskExecQueued / EnqueueMessage: the guard returns before any mutation.

		err := s.processTaskExecutionStarting(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("mark queued fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecQueued(mock.Anything, instance.ID).Return(simErr)

		err := s.processTaskExecutionStarting(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("no sender for task name is a consistency error", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		// No sender registered for the task name.
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecQueued(mock.Anything, instance.ID).Return(nil)

		err := s.processTaskExecutionStarting(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var consistencyErr goutils.ConsistencyError
		assert.True(
			errors.As(err, &consistencyErr), "expected ConsistencyError, got %T: %v", err, err,
		)
	})

	t.Run("enqueue message fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessTestScheduler(
			mockClient, map[string]common.IPCMessageSend{"unit-test-task": mockSender},
		)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecQueued(mock.Anything, instance.ID).Return(nil)
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr)

		err := s.processTaskExecutionStarting(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("happy path: instance is queued and dispatched", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessTestScheduler(
			mockClient, map[string]common.IPCMessageSend{"unit-test-task": mockSender},
		)

		task := pendingTaskFixture("unit-test-task")
		// Default DEFINED state is upstream of ENQUEUED, so the guard does not fire.
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecQueued(mock.Anything, instance.ID).Return(nil)
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil)

		err := s.processTaskExecutionStarting(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})
}

// TestProcessTaskExecutionComplete covers the execution-completion handler: the two
// fetch failures, the at-or-past-FINALIZED idempotency guard, and the finalize-then-
// mark-task-complete path with each of its failure branches.
func TestProcessTaskExecutionComplete(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	// expectFetch wires GetTaskExecution + GetTask for a successful fetch of the
	// instance and its parent task.
	expectFetch := func(
		mockDatabase *mockdb.Database, instance models.TaskExecution, task models.Task,
	) {
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(task, nil)
	}

	// completedInstance a PROCESSED instance ready to be finalized (upstream of
	// FINALIZED, so the idempotency guard does not fire).
	completedInstance := func(task models.Task) models.TaskExecution {
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		instance.ExecutionState = models.TaskExecutionStateProcessed
		return instance
	}

	t.Run("fetch execution instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		instanceID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(models.TaskExecution{}, simErr)

		err := s.processTaskExecutionComplete(utCtx, instanceID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("fetch parent task fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := completedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(models.Task{}, simErr)

		err := s.processTaskExecutionComplete(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("instance already at or past FINALIZED is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		// Already FINALIZED: a prior delivery (or the maintenance backstop) handled it.
		instance.ExecutionState = models.TaskExecutionStateFinalized

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		// No MarkTaskExecFinalized / MarkTaskComplete: the guard returns before any mutation.

		err := s.processTaskExecutionComplete(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("mark execution finalized fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := completedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(simErr)

		err := s.processTaskExecutionComplete(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("mark task complete fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := completedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskComplete(mock.Anything, task.ID).Return(simErr)

		err := s.processTaskExecutionComplete(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("happy path: instance finalized and task marked complete", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := completedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskComplete(mock.Anything, task.ID).Return(nil)

		err := s.processTaskExecutionComplete(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})
}

// TestProcessTaskExecutionFailed covers the execution-failure handler: the two fetch
// failures, the at-or-past-FINALIZED idempotency guard, the finalize step, and the
// retry-decision fork that only runs while the parent task is still ACTIVE (task not
// active -> nothing further; retries exhausted -> task failed; retry available ->
// a new retry instance is defined), including each failure branch.
func TestProcessTaskExecutionFailed(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	// expectFetch wires GetTaskExecution + GetTask for a successful fetch of the
	// instance and its parent task.
	expectFetch := func(
		mockDatabase *mockdb.Database, instance models.TaskExecution, task models.Task,
	) {
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(task, nil)
	}

	// failedInstance a FAILED instance ready to be finalized (upstream of FINALIZED,
	// so the idempotency guard does not fire).
	failedInstance := func(task models.Task) models.TaskExecution {
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		instance.ExecutionState = models.TaskExecutionStateFailed
		return instance
	}

	// activeTaskWithRetries an ACTIVE task carrying the given retry parameters, so the
	// handler enters the retry-decision block.
	activeTaskWithRetries := func(retry models.TaskRetryParameters) models.Task {
		task := pendingTaskFixture("unit-test-task")
		task.TaskState = models.TaskStateActive
		task.RetryParams = retry
		return task
	}

	// retryFilter the filter the handler passes to ListTaskExecutions: only instances
	// whose terminal state is FAILED.
	retryFilter := db.TaskExecutionQueryFilter{
		TerminalStates: []models.TaskExecutionStateENUM{models.TaskExecutionStateFailed},
	}

	// nFailed a slice of n FAILED terminal-state instances, used to drive the retry
	// count the handler derives from ListTaskExecutions.
	nFailed := func(n int) []models.TaskExecution {
		out := make([]models.TaskExecution, n)
		return out
	}

	t.Run("fetch execution instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		instanceID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(models.TaskExecution{}, simErr)

		err := s.processTaskExecutionFailed(utCtx, instanceID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("fetch parent task fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := failedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(models.Task{}, simErr)

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("instance already at or past FINALIZED is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		// Already FINALIZED: a prior delivery (or the maintenance backstop) handled it.
		instance.ExecutionState = models.TaskExecutionStateFinalized

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		// No MarkTaskExecFinalized / retry logic: the guard returns before any mutation.

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("mark execution finalized fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := failedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(simErr)

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("task not active: only the instance is finalized", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		// A task already in a resting state (e.g. CANCELLED) must not enter the retry
		// block: no ListTaskExecutions / MarkTaskFailed / retry instance.
		task := pendingTaskFixture("unit-test-task")
		task.TaskState = models.TaskStateCancelled
		instance := failedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("active task: listing failed instances fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := activeTaskWithRetries(
			models.TaskRetryParameters{MaxRetries: 5, InitialDelaySec: 5, Factor: 2},
		)
		instance := failedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, retryFilter).
			Return(nil, simErr)

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("active task: retries exhausted marks the task failed", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		// MaxRetries=1 and 2 recorded failures => NextDelay(2-1)=NextDelay(1) with
		// retry(1) >= MaxRetries(1) => 0 => exhausted.
		task := activeTaskWithRetries(
			models.TaskRetryParameters{MaxRetries: 1, InitialDelaySec: 5, Factor: 2},
		)
		instance := failedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, retryFilter).
			Return(nFailed(2), nil)
		mockDatabase.EXPECT().MarkTaskFailed(mock.Anything, task.ID).Return(nil)

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("active task: marking the task failed fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := activeTaskWithRetries(
			models.TaskRetryParameters{MaxRetries: 1, InitialDelaySec: 5, Factor: 2},
		)
		instance := failedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, retryFilter).
			Return(nFailed(2), nil)
		mockDatabase.EXPECT().MarkTaskFailed(mock.Anything, task.ID).Return(simErr)

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("active task: retry available defines a retry instance", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		// MaxRetries=5 and 1 recorded failure => NextDelay(1-1)=NextDelay(0)=5s>0 =>
		// a retry is due.
		task := activeTaskWithRetries(
			models.TaskRetryParameters{MaxRetries: 5, InitialDelaySec: 5, Factor: 2},
		)
		instance := failedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, retryFilter).
			Return(nFailed(1), nil)
		mockDatabase.EXPECT().
			DefineNewTaskRetryExecInstance(mock.Anything, task, instance, mock.Anything).
			Return(models.TaskExecution{}, nil)

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("active task: defining the retry instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := activeTaskWithRetries(
			models.TaskRetryParameters{MaxRetries: 5, InitialDelaySec: 5, Factor: 2},
		)
		instance := failedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, retryFilter).
			Return(nFailed(1), nil)
		mockDatabase.EXPECT().
			DefineNewTaskRetryExecInstance(mock.Anything, task, instance, mock.Anything).
			Return(models.TaskExecution{}, simErr)

		err := s.processTaskExecutionFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})
}

// TestProcessTaskExecutionTimedOut covers the execution-timeout handler: the two
// fetch failures, the HasEnded idempotency guard, the fail-then-finalize-then-
// timeout-task sequence with each failure branch, and the fan-out into
// cancelOngoingExecInstancesOfTask (list + per-instance cancellation) it ends with.
func TestProcessTaskExecutionTimedOut(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	// expectFetch wires GetTaskExecution + GetTask for a successful fetch of the
	// instance and its parent task.
	expectFetch := func(
		mockDatabase *mockdb.Database, instance models.TaskExecution, task models.Task,
	) {
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(task, nil)
	}

	// liveInstance a still-running instance (PROCESSING) that can legitimately time
	// out, so the HasEnded guard does not fire.
	liveInstance := func(task models.Task) models.TaskExecution {
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		instance.ExecutionState = models.TaskExecutionStateProcessing
		return instance
	}

	// liveFilter the filter cancelOngoingExecInstancesOfTask passes to
	// ListTaskExecutions: every live (not-yet-ended) execution state.
	liveFilter := db.TaskExecutionQueryFilter{
		ExecStates: []models.TaskExecutionStateENUM{
			models.TaskExecutionStateDefined,
			models.TaskExecutionStateScheduled,
			models.TaskExecutionStateEnqueued,
			models.TaskExecutionStateAcquired,
			models.TaskExecutionStateProcessing,
		},
	}

	// expectTimeoutSequence wires the three unconditional writes the handler makes for
	// a live instance: fail the instance, finalize it, then time out the parent task.
	expectTimeoutSequence := func(
		mockDatabase *mockdb.Database, instance models.TaskExecution, task models.Task,
	) {
		mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, instance.ID, mock.Anything, mock.Anything).
			Return(nil)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskTimedOut(mock.Anything, task.ID).Return(nil)
	}

	t.Run("fetch execution instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		instanceID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(models.TaskExecution{}, simErr)

		err := s.processTaskExecutionTimedOut(utCtx, instanceID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("fetch parent task fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := liveInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(models.Task{}, simErr)

		err := s.processTaskExecutionTimedOut(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("instance already ended is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		// Already CANCELLED: the real outcome raced this timeout delivery.
		instance.ExecutionState = models.TaskExecutionStateCancelled

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		// No mutations: the guard returns before forcing a spurious failure.

		err := s.processTaskExecutionTimedOut(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("mark execution failed fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := liveInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, instance.ID, mock.Anything, mock.Anything).
			Return(simErr)

		err := s.processTaskExecutionTimedOut(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("mark execution finalized fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := liveInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, instance.ID, mock.Anything, mock.Anything).
			Return(nil)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(simErr)

		err := s.processTaskExecutionTimedOut(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("mark task timed out fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := liveInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, instance.ID, mock.Anything, mock.Anything).
			Return(nil)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskTimedOut(mock.Anything, task.ID).Return(simErr)

		err := s.processTaskExecutionTimedOut(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("listing ongoing instances to cancel fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := liveInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		expectTimeoutSequence(mockDatabase, instance, task)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveFilter).
			Return(nil, simErr)

		err := s.processTaskExecutionTimedOut(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("cancelling an ongoing instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := liveInstance(task)
		// A sibling live instance the cancellation fan-out will act on.
		sibling := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		sibling.ExecutionState = models.TaskExecutionStateEnqueued

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		expectTimeoutSequence(mockDatabase, instance, task)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveFilter).
			Return([]models.TaskExecution{sibling}, nil)
		mockDatabase.EXPECT().
			MarkTaskExecCancelled(mock.Anything, sibling.ID, mock.Anything, mock.Anything).
			Return(simErr)

		err := s.processTaskExecutionTimedOut(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("happy path: instance timed out and siblings cancelled", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := liveInstance(task)
		sibling := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		sibling.ExecutionState = models.TaskExecutionStateEnqueued

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		expectTimeoutSequence(mockDatabase, instance, task)
		mockDatabase.EXPECT().
			ListTaskExecutions(mock.Anything, task.ID, liveFilter).
			Return([]models.TaskExecution{sibling}, nil)
		mockDatabase.EXPECT().
			MarkTaskExecCancelled(mock.Anything, sibling.ID, mock.Anything, mock.Anything).
			Return(nil)

		err := s.processTaskExecutionTimedOut(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})
}

// TestProcessTaskExecutionEngineFailed covers the engine-failure handler: the two fetch
// failures, the at-or-past-FINALIZED idempotency guard, and the finalize -> fail task ->
// record audit event path with each of its failure branches. Unlike the execution-failure
// handler, an engine failure is terminal (no retry): the task is always marked failed.
func TestProcessTaskExecutionEngineFailed(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	// expectFetch wires GetTaskExecution + GetTask for a successful fetch of the instance
	// and its parent task.
	expectFetch := func(
		mockDatabase *mockdb.Database, instance models.TaskExecution, task models.Task,
	) {
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(task, nil)
	}

	// engineFailedInstance an instance the receiver already marked FAILED before reporting
	// the engine failure (upstream of FINALIZED, so the idempotency guard does not fire).
	engineFailedInstance := func(task models.Task) models.TaskExecution {
		instance := execInstanceFixture(task, models.TaskExecutionClassImmediate)
		instance.ExecutionState = models.TaskExecutionStateFailed
		return instance
	}

	t.Run("fetch execution instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		instanceID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(models.TaskExecution{}, simErr)

		err := s.processTaskExecutionEngineFailed(utCtx, instanceID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("fetch parent task fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := engineFailedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instance.ID).Return(instance, nil)
		mockDatabase.EXPECT().GetTask(mock.Anything, instance.TaskID).Return(models.Task{}, simErr)

		err := s.processTaskExecutionEngineFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("instance already at or past FINALIZED is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := engineFailedInstance(task)
		// Already FINALIZED: a prior delivery (or the maintenance backstop) handled it.
		instance.ExecutionState = models.TaskExecutionStateFinalized

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		// No MarkTaskExecFinalized / MarkTaskFailed / RecordTaskEngineFailure: the guard
		// returns before any mutation.

		err := s.processTaskExecutionEngineFailed(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})

	t.Run("mark instance finalized fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := engineFailedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(simErr)
		// No MarkTaskFailed / RecordTaskEngineFailure: aborts on the finalize failure.

		err := s.processTaskExecutionEngineFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("mark task failed fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := engineFailedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskFailed(mock.Anything, task.ID).Return(simErr)
		// No RecordTaskEngineFailure: aborts on the mark-failed failure.

		err := s.processTaskExecutionEngineFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("record audit event fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := engineFailedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskFailed(mock.Anything, task.ID).Return(nil)
		// The audit write is inside the transaction, so its failure propagates and rolls
		// back the finalize + fail: the whole handler returns an error.
		mockDatabase.EXPECT().
			RecordTaskEngineFailure(mock.Anything, task.ID, instance.ID, mock.Anything).
			Return(simErr)

		err := s.processTaskExecutionEngineFailed(utCtx, instance.ID, time.Now().UTC())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("happy path finalizes, fails the task, and records the audit event", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		instance := engineFailedInstance(task)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		expectFetch(mockDatabase, instance, task)
		mockDatabase.EXPECT().MarkTaskExecFinalized(mock.Anything, instance.ID).Return(nil)
		mockDatabase.EXPECT().MarkTaskFailed(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			RecordTaskEngineFailure(mock.Anything, task.ID, instance.ID, mock.Anything).
			Return(nil)

		err := s.processTaskExecutionEngineFailed(utCtx, instance.ID, time.Now().UTC())
		assert.Nil(err)
	})
}
