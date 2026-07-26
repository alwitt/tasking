package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// fixedEnvelope a minimal goutilsRedis.QueueMessageEnvelope whose payload is fixed.
// Used where a handler only needs some concrete buffered message to pass to the
// receiver's buffer operations.
type fixedEnvelope struct {
	payload string
}

// StringPayload return the wrapped payload
func (f fixedEnvelope) StringPayload() (string, error) {
	return f.payload, nil
}

// TestRecordInvalidMessage verifies the best-effort audit write for a poison IPC
// message: the audit event is recorded with the scheduler's own receiver name, and a
// failure of the audit write is swallowed (logged, never propagated).
func TestRecordInvalidMessage(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	const rawPayload = "some raw payload"
	const reason = "unparsable message"

	t.Run("records the audit event with the scheduler receiver name", func(t *testing.T) {
		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		// s.ipcName ("scheduler") is passed as the receiver so the audit record
		// attributes the poison message to the scheduler.
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "scheduler", rawPayload, reason).
			Return(nil)

		// No return value: success is the recorded call plus no panic.
		s.recordInvalidMessage(utCtx, rawPayload, reason)
	})

	t.Run("swallows an audit write failure", func(t *testing.T) {
		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newProcessTestScheduler(mockClient, nil)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "scheduler", rawPayload, reason).
			Return(simErr)

		// The failure must not propagate: the function returns nothing and must not
		// panic. An un-auditable poison message cannot become a crash/replay loop.
		s.recordInvalidMessage(utCtx, rawPayload, reason)
	})
}

// TestRecordAndDropInvalidMessage verifies the live-path variant: it records the
// audit event (via recordInvalidMessage) and then drops the message from the queue
// buffer, with the buffer delete itself being best-effort.
func TestRecordAndDropInvalidMessage(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	const rawPayload = "some raw payload"
	const reason = "unsupported message type"

	msg := fixedEnvelope{payload: rawPayload}

	t.Run("records then deletes the buffered message", func(t *testing.T) {
		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newProcessTestScheduler(mockClient, nil)
		s.ipcReceiver = ipcReceiver

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "scheduler", rawPayload, reason).
			Return(nil)
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		s.recordAndDropInvalidMessage(utCtx, msg, rawPayload, reason)
	})

	t.Run("swallows a buffer delete failure", func(t *testing.T) {
		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newProcessTestScheduler(mockClient, nil)
		s.ipcReceiver = ipcReceiver

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "scheduler", rawPayload, reason).
			Return(nil)
		// Delete fails: still best-effort, must not panic or propagate.
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(simErr)

		s.recordAndDropInvalidMessage(utCtx, msg, rawPayload, reason)
	})
}

// newRecoverTestScheduler build a white-box schedulerImpl for recoverBufferedMessages:
// a registered validator (ParseIPCMessage needs it) plus the buffer receiver mock.
func newRecoverTestScheduler(
	t *testing.T, mockClient *mockdb.Client, ipcReceiver *mockcommon.IPCMessageReceive,
) *schedulerImpl {
	validate := validator.New()
	require.NoError(t, models.RegisterWithValidator(validate))

	s := newProcessTestScheduler(mockClient, nil)
	s.validator = validate
	s.ipcReceiver = ipcReceiver
	return s
}

// TestRecoverBufferedMessages covers the startup buffer drain: a read error is fatal,
// an empty buffer is a clean stop, a poison message is recorded and skipped, a valid
// message is re-enqueued onto the main queue, and a re-enqueue error is fatal.
func TestRecoverBufferedMessages(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	// expectRecordInvalid wires the best-effort audit write recordInvalidMessage makes
	// for a poison message.
	expectRecordInvalid := func(
		mockClient *mockdb.Client, mockDatabase *mockdb.Database, rawPayload, reason string,
	) {
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase)).
			Once()
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "scheduler", rawPayload, reason).
			Return(nil).
			Once()
	}

	t.Run("read error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, simErr)

		err := s.recoverBufferedMessages(utCtx)
		assert.NotNil(err)
		assertSchedulerError(t, err)
	})

	t.Run("empty buffer is a clean stop", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		// nil message => buffer drained on the first read.
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil)

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})

	t.Run("unreadable message is recorded and skipped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		// First read: an unreadable message; second read: buffer drained.
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(unreadableBufferEnvelope{err: simErr}, nil).
			Once()
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		// The message was already popped off the buffer, so it is only recorded (no delete).
		expectRecordInvalid(mockClient, mockDatabase, "", "unreadable payload")

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})

	t.Run("unparsable message is recorded and skipped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		const garbage = "not json"

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(fixedEnvelope{payload: garbage}, nil).
			Once()
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		expectRecordInvalid(mockClient, mockDatabase, garbage, "unparsable message")

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})

	t.Run("unsupported message type is recorded and skipped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		// A PENDING_INSTANCE message parses as IPCMessageExecuteInstance but is not one
		// of the execute-instance types (SUCCEEDED/FAILED) the scheduler queue handles,
		// so it is a poison message on the receive path.
		pending := models.PrepareIPCMsgTaskExecutionRequested(
			"unit-test", ulid.Make().String(), time.Now().UTC(),
		)
		payload, err := pending.StringPayload()
		require.NoError(t, err)

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(pending, nil).
			Once()
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase)).
			Once()
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(
				mock.Anything, "scheduler", payload, mock.MatchedBy(func(reason string) bool {
					return reason == fmt.Sprintf(
						"unsupported execute-instance message type '%s'", models.IPCMsgTypePendingInstance,
					)
				}),
			).
			Return(nil).
			Once()

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})

	t.Run("valid message is re-enqueued onto the main queue", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		newTask := models.PrepareIPCMsgNewPendingTask(
			"unit-test", ulid.Make().String(), time.Now().UTC(),
		)

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(newTask, nil).
			Once()
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		// Valid message goes back onto the main queue for normal processing (this also
		// removes it from the buffer, so no separate delete).
		ipcReceiver.EXPECT().ReEnqueueOnMainQueue(mock.Anything, newTask).Return(nil).Once()

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})

	t.Run("engine-failed message is re-enqueued onto the main queue", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		// An engine-failure report is an IPCMessageExecuteInstance. Before support was
		// added it fell into the unsupported-type branch and was recorded-and-skipped;
		// now it is a valid replayable message that must go back onto the main queue.
		engineFailed := models.PrepareIPCMsgTaskExecutionEngineFailed(
			"unit-test", ulid.Make().String(), time.Now().UTC(),
		)

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(engineFailed, nil).
			Once()
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		// Re-enqueued (not recorded as invalid): a strict RecordInvalidTaskIPCMessage
		// expectation is deliberately absent, so treating it as poison would fail the mock.
		ipcReceiver.EXPECT().ReEnqueueOnMainQueue(mock.Anything, engineFailed).Return(nil).Once()

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})

	t.Run("maintenance message is re-enqueued onto the main queue", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		// A self-enqueued Task Maintenance tick left in the buffer parses as a BaseIPCMessage; it
		// is a valid replayable message (transient but idempotent) that must go back onto the main
		// queue rather than being recorded as poison.
		maintenance := models.PrepareIPCMsgTaskMaintenance("unit-test", time.Now().UTC())

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(maintenance, nil).
			Once()
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		ipcReceiver.EXPECT().ReEnqueueOnMainQueue(mock.Anything, maintenance).Return(nil).Once()

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})

	t.Run("re-enqueue error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		cancelTask := models.PrepareIPCMsgCancelTask(
			"unit-test", ulid.Make().String(), time.Now().UTC(),
		)

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(cancelTask, nil).
			Once()
		ipcReceiver.EXPECT().
			ReEnqueueOnMainQueue(mock.Anything, cancelTask).
			Return(simErr).
			Once()

		err := s.recoverBufferedMessages(utCtx)
		assert.NotNil(err)
		assertSchedulerError(t, err)
	})

	t.Run("drains multiple messages until the buffer is empty", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newRecoverTestScheduler(t, mockClient, ipcReceiver)

		poison := fixedEnvelope{payload: "not json"}
		valid := models.PrepareIPCMsgNewPendingTask(
			"unit-test", ulid.Make().String(), time.Now().UTC(),
		)

		// Sequence: poison, valid, drained.
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(poison, nil).
			Once()
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(valid, nil).
			Once()
		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		expectRecordInvalid(mockClient, mockDatabase, "not json", "unparsable message")
		ipcReceiver.EXPECT().ReEnqueueOnMainQueue(mock.Anything, valid).Return(nil).Once()

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})
}

// unreadableBufferEnvelope a goutilsRedis.QueueMessageEnvelope whose StringPayload
// fails, used to drive the "unreadable payload" recovery branch.
type unreadableBufferEnvelope struct {
	err error
}

// StringPayload return the configured error
func (u unreadableBufferEnvelope) StringPayload() (string, error) {
	return "", u.err
}

// assertSchedulerError fail unless err is (or wraps) a TaskSchedulerError.
func assertSchedulerError(t *testing.T, err error) {
	t.Helper()
	var schedErr models.TaskSchedulerError
	if !errors.As(err, &schedErr) {
		t.Fatalf("expected TaskSchedulerError, got %T: %v", err, err)
	}
}

// ipcRequestTestScheduler bundles the scheduler under test with the mocks
// processOneIPCRequest drives.
type ipcRequestTestScheduler struct {
	scheduler    *schedulerImpl
	ipcReceiver  *mockcommon.IPCMessageReceive
	mockClient   *mockdb.Client
	mockDatabase *mockdb.Database
}

// newIPCRequestTestScheduler build a white-box schedulerImpl for processOneIPCRequest: a
// registered validator (ParseIPCMessage needs it), the buffer receiver mock, and the persistence
// mocks the handlers run against. The handlers are dispatched INLINE (there is no worker), so each
// dispatch subtest drives its handler's benign idempotency no-op through the DB mock, which proves
// parse + routing while keeping the mocking minimal.
func newIPCRequestTestScheduler(t *testing.T) ipcRequestTestScheduler {
	validate := validator.New()
	require.NoError(t, models.RegisterWithValidator(validate))

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	ipcReceiver := mockcommon.NewIPCMessageReceive(t)

	s := newProcessTestScheduler(mockClient, nil)
	s.validator = validate
	s.ipcReceiver = ipcReceiver

	return ipcRequestTestScheduler{
		scheduler: s, ipcReceiver: ipcReceiver, mockClient: mockClient, mockDatabase: mockDatabase,
	}
}

// expectTx wires the handler's UseDatabaseInTransaction call to run against the mock Database.
func (h ipcRequestTestScheduler) expectTx() {
	h.mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(runTxAgainst(h.mockDatabase))
}

// TestProcessOneIPCRequest covers the live IPC processing path and its delete contract: dequeue
// read errors are fatal (no message to delete), poison messages are recorded and dropped, and a
// valid message is handled INLINE and then deleted once the handler *completes* - on success AND on
// failure alike, since only a crash (never a returned error) may strand a message for replay. A
// handler error is returned for the caller to classify (fatal SQLError vs. log-and-continue) but
// never changes the delete. A delete failure after a handled message is swallowed. Each dispatch
// subtest drives its handler's benign idempotency no-op through the DB mock, which proves parse +
// routing while keeping the mocking minimal (the full handler behavior lives in each handler's own
// test file).
func TestProcessOneIPCRequest(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("dequeue error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(nil, simErr)

		err := h.scheduler.processOneIPCRequest(utCtx)
		assert.NotNil(err)
		assertSchedulerError(t, err)
	})

	t.Run("no message is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		// nil message: nothing to process, nothing to handle or delete.
		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("unreadable message is recorded and dropped", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		msg := unreadableBufferEnvelope{err: simErr}

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		h.expectTx()
		h.mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "scheduler", "", "unreadable payload").
			Return(nil)
		// Poison drop deletes the buffered message; no handler runs.
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("unparsable message is recorded and dropped", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		msg := fixedEnvelope{payload: "not json"}

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		h.expectTx()
		h.mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "scheduler", "not json", "unparsable message").
			Return(nil)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("unsupported message type is recorded and dropped", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)

		// PENDING_INSTANCE parses as IPCMessageExecuteInstance but is not one of the
		// execute-instance types the scheduler handles on its receive queue.
		pending := models.PrepareIPCMsgTaskExecutionRequested(
			"unit-test", ulid.Make().String(), time.Now().UTC(),
		)
		payload, err := pending.StringPayload()
		require.NoError(t, err)

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(pending, nil)
		h.expectTx()
		h.mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(
				mock.Anything, "scheduler", payload,
				fmt.Sprintf(
					"unsupported execute-instance message type '%s'", models.IPCMsgTypePendingInstance,
				),
			).
			Return(nil)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, pending).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("new-task routes to the handler and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgNewPendingTask("unit-test", taskID, time.Now().UTC())

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// A task no longer PENDING makes new-pending-task a benign NOOP: the handler returns nil
		// and the dispatch loop deletes the message. This confirms parse + routing.
		h.expectTx()
		h.mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{ID: taskID, TaskState: models.TaskStateActive}, nil)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("cancel-task routes to the handler and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgCancelTask("unit-test", taskID, time.Now().UTC())

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// An already-COMPLETE task makes cancel a benign NOOP.
		h.expectTx()
		h.mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{ID: taskID, TaskState: models.TaskStateComplete}, nil)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("execution-succeeded routes to the handler and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		instanceID := ulid.Make().String()
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionProcessSucceeded(
			"unit-test", instanceID, time.Now().UTC(),
		)

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// An instance already at/past FINALIZED makes completion a benign NOOP; the handler
		// fetches instance + parent task, hits the idempotency guard, and returns nil.
		h.expectTx()
		h.mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(models.TaskExecution{
				ID: instanceID, TaskID: taskID, ExecutionState: models.TaskExecutionStateFinalized,
			}, nil)
		h.mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{ID: taskID}, nil)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("execution-failed routes to the handler and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		instanceID := ulid.Make().String()
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionProcessFailed(
			"unit-test", instanceID, nil, time.Now().UTC(),
		)

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		h.expectTx()
		h.mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(models.TaskExecution{
				ID: instanceID, TaskID: taskID, ExecutionState: models.TaskExecutionStateFinalized,
			}, nil)
		h.mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{ID: taskID}, nil)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("engine-failed routes to the handler and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		instanceID := ulid.Make().String()
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionEngineFailed(
			"unit-test", instanceID, time.Now().UTC(),
		)

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		h.expectTx()
		h.mockDatabase.EXPECT().
			GetTaskExecution(mock.Anything, instanceID).
			Return(models.TaskExecution{
				ID: instanceID, TaskID: taskID, ExecutionState: models.TaskExecutionStateFinalized,
			}, nil)
		h.mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{ID: taskID}, nil)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("maintenance routes to the sweep and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		msg := models.PrepareIPCMsgTaskMaintenance("unit-test", time.Now().UTC())

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// performMaintenance runs a series of list-and-reconcile passes; with nothing to process in
		// any pass it is a clean success, so the message is deleted. The first pass lists pending /
		// cancelling tasks; the remaining passes list executions. Returning empty slices for each
		// short-circuits every pass.
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(h.mockDatabase))
		h.mockDatabase.EXPECT().
			ListTasks(mock.Anything, mock.Anything).
			Return([]models.Task{}, nil)
		h.mockDatabase.EXPECT().
			ListAllExecutions(mock.Anything, mock.Anything).
			Return([]models.TaskExecution{}, nil)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})

	t.Run("non-fatal handler error still deletes the message and returns the error", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgNewPendingTask("unit-test", taskID, time.Now().UTC())

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// The handler fails with a non-DB error (a plain simErr, not a models.SQLError). The
		// handler nonetheless *completed*, so the message IS deleted (only a crash strands a
		// message). The error is returned for the caller to classify - and it is NOT fatal, so
		// processQueue would log and continue; the maintenance sweep re-drives the work. This is
		// the key regression guard: a failing handler must not leave a message to be replayed.
		h.expectTx()
		h.mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{}, simErr)
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		err := h.scheduler.processOneIPCRequest(utCtx)
		assert.NotNil(err)
		assertSchedulerError(t, err)
		// Non-fatal: processQueue would continue rather than log.Fatal.
		assert.False(isFatalDBError(err))
	})

	t.Run("fatal SQLError handler error still deletes the message and returns a fatal error", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgNewPendingTask("unit-test", taskID, time.Now().UTC())

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// The handler fails because the database is broken (models.SQLError). The message is still
		// deleted (the handler completed - the DB fault will recur on every message, this is not a
		// per-message replay loop), and the returned error is classified fatal so processQueue
		// stops. This confirms the delete is unconditional while the error class still surfaces.
		h.expectTx()
		h.mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{}, models.NewSQLError("database is down", simErr, true))
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		err := h.scheduler.processOneIPCRequest(utCtx)
		assert.NotNil(err)
		assertSchedulerError(t, err)
		// Fatal: processQueue would log.Fatal on this class.
		assert.True(isFatalDBError(err))
	})

	t.Run("a buffer delete failure after a handled message is swallowed", func(t *testing.T) {
		assert := assert.New(t)

		h := newIPCRequestTestScheduler(t)
		taskID := ulid.Make().String()
		msg := models.PrepareIPCMsgNewPendingTask("unit-test", taskID, time.Now().UTC())

		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// Handler is a benign NOOP (task no longer PENDING).
		h.expectTx()
		h.mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{ID: taskID, TaskState: models.TaskStateActive}, nil)
		// The message was handled; a delete failure is best-effort and must not turn a handled
		// message into a fatal error.
		h.ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(simErr)

		assert.Nil(h.scheduler.processOneIPCRequest(utCtx))
	})
}
