package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/common"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// newProcessTestScheduler build a white-box schedulerImpl suitable for driving the
// process* handlers directly. It wires the persistence client and, for each task name
// in senders, an IPC sender so processNewPendingTask can dispatch execution requests.
func newProcessTestScheduler(
	mockClient *mockdb.Client, senders map[string]common.IPCMessageSend,
) *schedulerImpl {
	if senders == nil {
		senders = map[string]common.IPCMessageSend{}
	}
	return &schedulerImpl{
		Component:      goutils.Component{LogTags: log.Fields{"module": "task"}},
		wg:             &sync.WaitGroup{},
		persistence:    mockClient,
		ipcName:        "scheduler",
		taskIPcSenders: senders,
	}
}

// pendingTaskFixture a PENDING task of the given name/class for scheduling tests.
func pendingTaskFixture(taskName string) models.Task {
	return models.Task{
		ID:                ulid.Make().String(),
		TaskName:          taskName,
		TaskScheduleClass: models.TaskScheduleClassImmediateOneShot,
		TaskState:         models.TaskStatePending,
	}
}

// execInstanceFixture an execution instance of the given class for the parent task.
func execInstanceFixture(
	task models.Task, class models.TaskExecutionClassENUM,
) models.TaskExecution {
	return models.TaskExecution{
		ID:             ulid.Make().String(),
		TaskID:         task.ID,
		ExecutionClass: class,
		ExecutionState: models.TaskExecutionStateDefined,
	}
}

// TestProcessNewPendingTask covers the new-pending-task handler: the idempotency
// guard, each persistence failure path, and the immediate-vs-scheduled dispatch fork.
func TestProcessNewPendingTask(t *testing.T) {
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

		err := s.processNewPendingTask(utCtx, taskID)
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("task not pending is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")
		task.TaskState = models.TaskStateActive

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		// No MarkTaskActive / DefineNewTaskExecInstance expectations: the guard must
		// return before any mutation. The mock asserts no unexpected calls on Cleanup.

		err := s.processNewPendingTask(utCtx, task.ID)
		assert.Nil(err)
	})

	t.Run("activate task fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskActive(mock.Anything, task.ID).Return(simErr)

		err := s.processNewPendingTask(utCtx, task.ID)
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("define execution instance fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		task := pendingTaskFixture("unit-test-task")

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskActive(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().
			DefineNewTaskExecInstance(mock.Anything, task).
			Return(models.TaskExecution{}, simErr)

		err := s.processNewPendingTask(utCtx, task.ID)
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("scheduled instance is defined but not enqueued", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		// A sender is wired but must never be used for a SCHEDULED instance.
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessTestScheduler(
			mockClient, map[string]common.IPCMessageSend{"unit-test-task": mockSender},
		)

		task := pendingTaskFixture("unit-test-task")
		instance := execInstanceFixture(task, models.TaskExecutionClassScheduled)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskActive(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().DefineNewTaskExecInstance(mock.Anything, task).Return(instance, nil)
		// No MarkTaskExecQueued / EnqueueMessage: a scheduled instance is left for the
		// maintenance loop to enqueue at its target time.

		err := s.processNewPendingTask(utCtx, task.ID)
		assert.Nil(err)
	})

	t.Run("immediate instance without a sender is a consistency error", func(t *testing.T) {
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
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskActive(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().DefineNewTaskExecInstance(mock.Anything, task).Return(instance, nil)
		mockDatabase.EXPECT().MarkTaskExecQueued(mock.Anything, instance.ID).Return(nil)

		err := s.processNewPendingTask(utCtx, task.ID)
		assert.NotNil(err)
		// The missing sender is specifically a ConsistencyError (wrapped as a
		// TaskSchedulerError). Assert on the inner type so this case is distinguished
		// from the surrounding persistence failures rather than any error passing.
		var consistencyErr goutils.ConsistencyError
		assert.True(
			errors.As(err, &consistencyErr), "expected ConsistencyError, got %T: %v", err, err,
		)
	})

	t.Run("mark queued fails", func(t *testing.T) {
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
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskActive(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().DefineNewTaskExecInstance(mock.Anything, task).Return(instance, nil)
		mockDatabase.EXPECT().MarkTaskExecQueued(mock.Anything, instance.ID).Return(simErr)

		err := s.processNewPendingTask(utCtx, task.ID)
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
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
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskActive(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().DefineNewTaskExecInstance(mock.Anything, task).Return(instance, nil)
		mockDatabase.EXPECT().MarkTaskExecQueued(mock.Anything, instance.ID).Return(nil)
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr)

		err := s.processNewPendingTask(utCtx, task.ID)
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("happy path: immediate instance is queued and dispatched", func(t *testing.T) {
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
		mockDatabase.EXPECT().GetTask(mock.Anything, task.ID).Return(task, nil)
		mockDatabase.EXPECT().MarkTaskActive(mock.Anything, task.ID).Return(nil)
		mockDatabase.EXPECT().DefineNewTaskExecInstance(mock.Anything, task).Return(instance, nil)
		mockDatabase.EXPECT().MarkTaskExecQueued(mock.Anything, instance.ID).Return(nil)
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil)

		err := s.processNewPendingTask(utCtx, task.ID)
		assert.Nil(err)
	})
}
