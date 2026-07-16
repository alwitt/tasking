package task_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	goutilsRedis "github.com/alwitt/goutils/redis"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktask "github.com/alwitt/tasking/mocks/task"
	mocktest "github.com/alwitt/tasking/mocks/test"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// stringEnvelope a goutilsRedis.QueueMessageEnvelope wrapping a fixed payload string,
// used to drive the "unparsable" buffer-drain branch.
type stringEnvelope struct {
	payload string
}

// StringPayload return the wrapped payload
func (s stringEnvelope) StringPayload() (string, error) {
	return s.payload, nil
}

// unreadableEnvelope a goutilsRedis.QueueMessageEnvelope whose StringPayload fails,
// used to drive the "unreadable message" buffer-drain branch.
type unreadableEnvelope struct {
	err error
}

// StringPayload return the configured error
func (u unreadableEnvelope) StringPayload() (string, error) {
	return "", u.err
}

// initTestReceiver bundles the receiver under test with the mocks Initialize drives.
type initTestReceiver struct {
	receiver     task.Receiver
	ipcReceiver  *mockcommon.IPCMessageReceive
	sender       *mockcommon.IPCMessageSend
	mockDatabase *mockdb.Database
}

// newInitTestReceiver build a task.Receiver via NewReceiver with all factories wired
// to return the collected mocks, so Initialize operates against them. A non-nil
// mockDatabase is returned to pass as the active DB client (making ActiveSessionWrapper
// run the reconcile closure directly against it).
func newInitTestReceiver(t *testing.T) initTestReceiver {
	cbMock := mocktest.NewUnitTestCallbackCollector(t)
	mockClient := mockdb.NewClient(t)
	mockRedis := mocktest.NewRedisClientForTest(t)
	ipcReceiver := mockcommon.NewIPCMessageReceive(t)
	sender := mockcommon.NewIPCMessageSend(t)
	executor := mocktask.NewExecutor(t)

	cbMock.EXPECT().
		NewRedisIPCMsgReceiver(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(ipcReceiver, nil)
	cbMock.EXPECT().
		NewTaskExecutor(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(executor, nil)
	cbMock.EXPECT().
		NewRedisIPCMsgSender(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(sender, nil)

	receiver, err := task.NewReceiver(
		context.Background(), baseReceiverParams(cbMock, mockClient, mockRedis),
	)
	require.NoError(t, err)

	return initTestReceiver{
		receiver:     receiver,
		ipcReceiver:  ipcReceiver,
		sender:       sender,
		mockDatabase: mockdb.NewDatabase(t),
	}
}

// withState clone a TaskExecution overriding its state and worker name.
func withState(
	exec models.TaskExecution, state models.TaskExecutionStateENUM, worker *string,
) models.TaskExecution {
	exec.ExecutionState = state
	exec.ExecutionWorkerName = worker
	return exec
}

// scriptDequeue set up DequeueBufferedMessage to return each supplied message in order,
// then nil (end of buffer) for every subsequent call.
func scriptDequeue(
	ipcReceiver *mockcommon.IPCMessageReceive, msgs ...goutilsRedis.QueueMessageEnvelope,
) {
	idx := 0
	ipcReceiver.EXPECT().
		DequeueBufferedMessage(mock.Anything, true, mock.Anything).
		RunAndReturn(func(
			_ context.Context, _ bool, _ *time.Duration,
		) (goutilsRedis.QueueMessageEnvelope, error) {
			if idx < len(msgs) {
				msg := msgs[idx]
				idx++
				return msg, nil
			}
			return nil, nil
		})
}

// TestInitializeBufferDrain covers Stage A: draining the queue buffer. Malformed and
// non-pending messages are skipped (already popped), a dequeue error is fatal, and a
// valid pending message flows into the reconcile lookup.
func TestInitializeBufferDrain(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("dequeue error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		tr := newInitTestReceiver(t)
		tr.ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, simErr)

		err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err)
	})

	type skipCase struct {
		name string
		msg  goutilsRedis.QueueMessageEnvelope
	}
	skipCases := []skipCase{
		{
			name: "unreadable message skipped",
			msg:  unreadableEnvelope{err: simErr},
		},
		{
			name: "unparsable message skipped",
			msg:  stringEnvelope{payload: "not-json"},
		},
		{
			name: "wrong-type message skipped",
			msg: models.PrepareIPCMsgTaskExecutionProcessSucceeded(
				"scheduler", ulid.Make().String(), time.Now().UTC(),
			),
		},
		{
			name: "unsupported-type message skipped",
			msg: models.PrepareIPCMsgNewPendingTask(
				"scheduler", ulid.Make().String(), time.Now().UTC(),
			),
		},
	}
	for _, tc := range skipCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			tr := newInitTestReceiver(t)
			scriptDequeue(tr.ipcReceiver, tc.msg)

			// Reconcile is trivially empty: the skipped message never becomes a request.
			tr.mockDatabase.EXPECT().
				ListAllExecutions(mock.Anything, mock.Anything).
				Return(nil, nil)

			err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
			assert.Nil(err)
		})
	}

	t.Run("valid pending buffered", func(t *testing.T) {
		assert := assert.New(t)

		instanceID := ulid.Make().String()
		taskID := ulid.Make().String()

		tr := newInitTestReceiver(t)
		scriptDequeue(
			tr.ipcReceiver,
			models.PrepareIPCMsgTaskExecutionRequested("scheduler", instanceID, time.Now().UTC()),
		)

		// A terminal-state instance requires no action, isolating the drain + lookup.
		tr.mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(withState(
				validExecution(instanceID, taskID), models.TaskExecutionStateProcessed, nil), nil,
			)
		tr.mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.Anything).
			Return(nil, nil)

		err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
		assert.Nil(err)
	})
}

// TestInitializeReconcileStates covers Stage B's per-buffered-request state switch:
// each execution state routes to retry, mark-failed+notify, or ignore.
func TestInitializeReconcileStates(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")
	otherWorker := "some-other-worker"

	type testCase struct {
		name string
		// state / worker the fetched execution instance's state + owner.
		state  models.TaskExecutionStateENUM
		worker *string
		// getErr GetTaskExecution error (fatal path).
		getErr bool
		// expectRetry / expectNotify downstream effects.
		expectRetry  bool
		expectNotify bool
		expectFatal  bool
	}

	cases := []testCase{
		{name: "defined logs only", state: models.TaskExecutionStateDefined},
		{name: "scheduled logs only", state: models.TaskExecutionStateScheduled},
		{name: "enqueued retried", state: models.TaskExecutionStateEnqueued, expectRetry: true},
		{
			name:  "acquired owned by self failed",
			state: models.TaskExecutionStateAcquired, worker: nil, expectNotify: true,
		},
		{
			name:  "processing owned by other ignored",
			state: models.TaskExecutionStateProcessing, worker: &otherWorker,
		},
		{name: "processed logs only", state: models.TaskExecutionStateProcessed},
		{name: "failed logs only", state: models.TaskExecutionStateFailed},
		{name: "finalized logs only", state: models.TaskExecutionStateFinalized},
		{name: "get execution error is fatal", getErr: true, expectFatal: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			instanceID := ulid.Make().String()
			taskID := ulid.Make().String()
			msg := models.PrepareIPCMsgTaskExecutionRequested(
				"scheduler", instanceID, time.Now().UTC(),
			)

			tr := newInitTestReceiver(t)
			scriptDequeue(tr.ipcReceiver, msg)

			if tc.getErr {
				tr.mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(models.TaskExecution{}, simErr)
			} else {
				tr.mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(withState(validExecution(instanceID, taskID), tc.state, tc.worker), nil)
				// The owned-instance list runs whenever reconcile reaches it.
				tr.mockDatabase.EXPECT().
					ListAllExecutions(mock.Anything, mock.Anything).
					Return(nil, nil)
			}

			if tc.expectRetry {
				tr.ipcReceiver.EXPECT().
					ReEnqueueOnMainQueue(mock.Anything, msg).
					Return(nil)
			}
			if tc.expectNotify {
				tr.sender.EXPECT().
					EnqueueMessage(mock.Anything, mock.Anything).
					Run(func(_ context.Context, m goutilsRedis.QueueMessageEnvelope) {
						execMsg, ok := m.(models.IPCMessageExecuteInstance)
						assert.True(ok, "expected IPCMessageExecuteInstance, got %T", m)
						assert.Equal(models.IPCMsgTypeExecuteFailed, execMsg.Type)
						assert.Equal(instanceID, execMsg.InstanceID)
					}).
					Return(nil)
			}

			err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
			if tc.expectFatal {
				assert.NotNil(err)
				var recvErr models.TaskReceiverError
				assert.True(errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err)
			} else {
				assert.Nil(err)
			}
		})
	}
}

// TestInitializeListOwnedAndNotify covers Stage B's owned-instance list + mark-failed
// loop and Stage C's scheduler-notify / re-enqueue, plus their fatal error paths.
func TestInitializeListOwnedAndNotify(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")
	self := "unit-test-worker"

	t.Run("owned instances marked failed and notified", func(t *testing.T) {
		assert := assert.New(t)

		id1 := ulid.Make().String()
		id2 := ulid.Make().String()
		owned := []models.TaskExecution{
			withState(validExecution(id1, ulid.Make().String()), models.TaskExecutionStateAcquired, &self),
			withState(validExecution(id2, ulid.Make().String()), models.TaskExecutionStateProcessing, &self),
		}

		tr := newInitTestReceiver(t)
		scriptDequeue(tr.ipcReceiver) // empty buffer

		tr.mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.Anything).
			Return(owned, nil)
		tr.mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, id1, mock.Anything).
			Return(nil)
		tr.mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, id2, mock.Anything).
			Return(nil)

		notified := map[string]bool{}
		tr.sender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Run(func(_ context.Context, m goutilsRedis.QueueMessageEnvelope) {
				execMsg, ok := m.(models.IPCMessageExecuteInstance)
				assert.True(ok, "expected IPCMessageExecuteInstance, got %T", m)
				assert.Equal(models.IPCMsgTypeExecuteFailed, execMsg.Type)
				notified[execMsg.InstanceID] = true
			}).
			Return(nil).
			Twice()

		err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
		assert.Nil(err)
		assert.True(notified[id1])
		assert.True(notified[id2])
	})

	t.Run("list executions error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		tr := newInitTestReceiver(t)
		scriptDequeue(tr.ipcReceiver)

		tr.mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.Anything).
			Return(nil, simErr)

		err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err)
	})

	t.Run("mark failed error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		id := ulid.Make().String()
		owned := []models.TaskExecution{
			withState(validExecution(id, ulid.Make().String()), models.TaskExecutionStateAcquired, &self),
		}

		tr := newInitTestReceiver(t)
		scriptDequeue(tr.ipcReceiver)

		tr.mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.Anything).
			Return(owned, nil)
		tr.mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, id, mock.Anything).
			Return(simErr)

		err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err)
	})

	t.Run("scheduler notify error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		id := ulid.Make().String()
		owned := []models.TaskExecution{
			withState(validExecution(id, ulid.Make().String()), models.TaskExecutionStateAcquired, &self),
		}

		tr := newInitTestReceiver(t)
		scriptDequeue(tr.ipcReceiver)

		tr.mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.Anything).
			Return(owned, nil)
		tr.mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, id, mock.Anything).
			Return(nil)
		tr.sender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Return(simErr)

		err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err)
	})

	t.Run("re-enqueue error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		instanceID := ulid.Make().String()
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionRequested("scheduler", instanceID, time.Now().UTC())

		tr := newInitTestReceiver(t)
		scriptDequeue(tr.ipcReceiver, msg)

		// ENQUEUED state routes to retry.
		tr.mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(withState(validExecution(instanceID, taskID), models.TaskExecutionStateEnqueued, nil), nil)
		tr.mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.Anything).
			Return(nil, nil)
		tr.ipcReceiver.EXPECT().
			ReEnqueueOnMainQueue(mock.Anything, msg).
			Return(simErr)

		err := tr.receiver.Initialize(utCtx, tr.mockDatabase)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err)
	})
}
