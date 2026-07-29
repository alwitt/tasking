package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/db"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktest "github.com/alwitt/tasking/mocks/test"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// fixedEnvelope a minimal goutilsRedis.QueueMessageEnvelope whose payload is fixed.
type fixedEnvelope struct {
	payload string
}

// StringPayload return the wrapped payload
func (f fixedEnvelope) StringPayload() (string, error) {
	return f.payload, nil
}

// unreadableBufferEnvelope a goutilsRedis.QueueMessageEnvelope whose StringPayload fails, used to
// drive the "unreadable payload" recovery branch.
type unreadableBufferEnvelope struct {
	err error
}

// StringPayload return the wrapped error
func (u unreadableBufferEnvelope) StringPayload() (string, error) {
	return "", u.err
}

// runTxAgainst returns a RunAndReturn body that invokes the transaction closure against the
// supplied mock Database, mirroring production behavior.
func runTxAgainst(
	mockDatabase *mockdb.Database,
) func(context.Context, func(context.Context, db.Database) error) error {
	return func(ctx context.Context, core func(context.Context, db.Database) error) error {
		return core(ctx, mockDatabase)
	}
}

// newDispatchTestScheduler build a white-box workflow schedulerImpl for driving
// processOneIPCRequest: a registered validator (ParseIPCMessage needs it), the persistence
// client, and the queue receiver mock.
func newDispatchTestScheduler(
	t *testing.T, mockClient *mockdb.Client, ipcReceiver *mockcommon.IPCMessageReceive,
) *schedulerImpl {
	validate := validator.New()
	require.NoError(t, models.RegisterWithValidator(validate))

	return &schedulerImpl{
		Component:   goutils.Component{LogTags: log.Fields{"module": "workflow"}},
		validator:   validate,
		wg:          &sync.WaitGroup{},
		persistence: mockClient,
		ipcName:     "workflow-scheduler",
		ipcReceiver: ipcReceiver,
	}
}

// assertWorkflowSchedulerError assert the error is a WorkflowSchedulerError.
func assertWorkflowSchedulerError(t *testing.T, err error) {
	t.Helper()
	var wfErr models.WorkflowSchedulerError
	assert.ErrorAs(t, err, &wfErr)
}

// TestProcessOneIPCRequestDequeueAndParse covers dequeue + parse: a dequeue error is fatal, an
// empty dequeue is a no-op, and unreadable / unparsable payloads are recorded and dropped from
// the buffer without touching any handler.
func TestProcessOneIPCRequestDequeueAndParse(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	t.Run("dequeue error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(nil, simErr)

		err := s.processOneIPCRequest(utCtx)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("empty dequeue is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil)

		assert.Nil(s.processOneIPCRequest(utCtx))
	})

	t.Run("unparsable message is recorded and dropped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		const garbage = "not json"
		msg := fixedEnvelope{payload: garbage}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "workflow-scheduler", garbage, "unparsable message").
			Return(nil)
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(s.processOneIPCRequest(utCtx))
	})
}

// TestProcessOneIPCRequestDispatch covers the parse -> dispatch wiring and the delete contract for
// each workflow message shape. The delete keys off handler *completion*, not success: a valid
// message is deleted once its handler returns (nil OR error); the error is returned for the caller
// to classify (fatal SQLError vs. log-and-continue) but never changes the delete. An unknown type is
// recorded and dropped. Each subtest drives its handler's benign no-op through the DB mock.
func TestProcessOneIPCRequestDispatch(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	ts := time.Now().UTC()

	// Every workflow event message shape (Process Workflow, Schedule Workflow Step, Execution Update,
	// Step Task Update, Revive, Cancel) is now implemented and routed here; each has its own handler
	// test file. The subtests below cover the parse -> dispatch wiring and the delete contract.

	t.Run("cancel routes to the handler and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		wfID := ulid.Make().String()
		payload, err := models.PrepareIPCMsgWFCancelWorkflow("unit-test", wfID, ts).StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// An already-CANCELLED workflow makes cancel a benign NOOP: the handler returns nil and the
		// dispatch loop deletes the message. This confirms parse + routing.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, wfID).
			Return(models.Workflow{ID: wfID, State: models.WorkflowStateCancelled}, nil)
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(s.processOneIPCRequest(utCtx))
	})

	t.Run("maintenance routes to the sweep and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		payload, err := models.PrepareIPCMsgWFMaintenance("unit-test", ts).StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// The sweep lists non-terminal workflows; with none to reconcile it is a clean success, so
		// the message is deleted.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			ListWorkflows(mock.Anything, mock.Anything).
			Return([]models.Workflow{}, nil)
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(s.processOneIPCRequest(utCtx))
	})

	t.Run("step task update routes to the handler and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		taskID := ulid.Make().String()
		payload, err := models.PrepareIPCMsgWFStepTaskUpdate(
			"unit-test", taskID, models.WorkflowStepStateComplete, ts,
		).StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// The handler resolves task -> step; a not-found link is a benign drop, so the handler
		// returns nil and the dispatch loop deletes the message. This confirms parse + routing.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflowStepProcessedByTask(mock.Anything, taskID).
			Return(models.WorkflowStep{}, goutils.NewNotFoundError("no step", gorm.ErrRecordNotFound, true))
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(s.processOneIPCRequest(utCtx))
	})

	t.Run("revive routes to the handler and deletes on success", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)
		s.ipcSender = mockSender

		wfID := ulid.Make().String()
		payload, err := models.PrepareIPCMsgWFReviveWorkflow("unit-test", wfID, nil, ts).StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// A FAILED workflow with no steps revives successfully: mark running, list (empty), one poke.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, wfID).
			Return(models.Workflow{ID: wfID, State: models.WorkflowStateFailed}, nil)
		mockDatabase.EXPECT().MarkWorkflowRunning(mock.Anything, wfID, mock.Anything).Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wfID).
			Return([]models.WorkflowStep{}, nil)
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil)
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(s.processOneIPCRequest(utCtx))
	})

	t.Run("bad revive is recorded and dropped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		wfID := ulid.Make().String()
		payload, err := models.PrepareIPCMsgWFReviveWorkflow("unit-test", wfID, nil, ts).StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// A RUNNING workflow cannot be revived: the handler drops it (revived=false), so the dispatch
		// loop records it as invalid and DELETES it - it must NOT be left buffered. Two transactions
		// run: the revive precondition read, then the invalid-message record.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, wfID).
			Return(models.Workflow{ID: wfID, State: models.WorkflowStateRunning}, nil)
		// recordAndDropInvalidMessage records the poison event, then deletes the message.
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, s.ipcName, payload, mock.Anything).
			Return(nil)
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(s.processOneIPCRequest(utCtx))
	})

	t.Run("unsupported message type is recorded and dropped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		// A well-formed task IPC message on the WORKFLOW queue is genuinely poison here.
		payload, err := models.PrepareIPCMsgNewPendingTask("unit-test", ulid.Make().String(), ts).
			StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(
				mock.Anything, "workflow-scheduler", payload, mock.Anything,
			).
			Return(nil)
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		assert.Nil(s.processOneIPCRequest(utCtx))
	})

	t.Run("non-fatal handler error still deletes the message and returns the error", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		wfID := ulid.Make().String()
		payload, err := models.PrepareIPCMsgWFCancelWorkflow("unit-test", wfID, ts).StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// The handler fails with a non-DB error (a plain simErr, not a models.SQLError). The handler
		// nonetheless *completed*, so the message IS deleted (only a crash strands a message). The
		// error is returned for the caller to classify - and it is NOT fatal, so processQueue would
		// log and continue; the maintenance sweep re-drives the work. This is the key regression
		// guard: a failing handler must not leave a message to be replayed.
		simErr := fmt.Errorf("simulated failure")
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, wfID).
			Return(models.Workflow{}, simErr)
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		err = s.processOneIPCRequest(utCtx)
		assert.NotNil(err)
		// Non-fatal: processQueue would continue rather than log.Fatal.
		assert.False(isFatalDBError(err))
	})

	t.Run("fatal SQLError handler error still deletes the message and returns a fatal error", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		wfID := ulid.Make().String()
		payload, err := models.PrepareIPCMsgWFCancelWorkflow("unit-test", wfID, ts).StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// The handler fails because the database is broken (models.SQLError). The message is still
		// deleted (the handler completed - the DB fault will recur on every message, this is not a
		// per-message replay loop), and the returned error is classified fatal so processQueue stops.
		simErr := fmt.Errorf("simulated failure")
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, wfID).
			Return(models.Workflow{}, goutils.NewSQLError("database is down", simErr, true))
		ipcReceiver.EXPECT().DeleteBufferedMessage(mock.Anything, msg).Return(nil)

		err = s.processOneIPCRequest(utCtx)
		assert.NotNil(err)
		// Fatal: processQueue would log.Fatal on this class.
		assert.True(isFatalDBError(err))
	})
}

// TestRecoverBufferedMessages covers the startup buffer drain: a read error is fatal, an empty
// buffer is a clean stop, a poison message (unreadable / unparsable / unsupported type) is
// recorded and skipped, a valid workflow message of each type is re-enqueued onto the main queue,
// a re-enqueue error is fatal, and the loop drains multiple messages until the buffer empties.
func TestRecoverBufferedMessages(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	ts := time.Now().UTC()
	simErr := fmt.Errorf("simulated failure")

	// expectRecordInvalid wires the best-effort audit write recordInvalidMessage makes for a
	// poison message.
	expectRecordInvalid := func(
		mockClient *mockdb.Client, mockDatabase *mockdb.Database, rawPayload, reason string,
	) {
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase)).
			Once()
		mockDatabase.EXPECT().
			RecordInvalidTaskIPCMessage(mock.Anything, "workflow-scheduler", rawPayload, reason).
			Return(nil).
			Once()
	}

	t.Run("read error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, simErr)

		err := s.recoverBufferedMessages(utCtx)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("empty buffer is a clean stop", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

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
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

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
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

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

	t.Run("non-workflow message is recorded and skipped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		// A well-formed task IPC message parses fine but is not a workflow message, so on the
		// workflow queue it is poison and must be recorded, not re-enqueued.
		newTask := models.PrepareIPCMsgNewPendingTask("unit-test", ulid.Make().String(), ts)
		payload, err := newTask.StringPayload()
		require.NoError(t, err)

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(newTask, nil).
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
				mock.Anything, "workflow-scheduler", payload, mock.Anything,
			).
			Return(nil).
			Once()

		assert.Nil(s.recoverBufferedMessages(utCtx))
	})

	// validCases: each valid workflow message must be re-enqueued onto the main queue (which also
	// removes it from the buffer, so no separate delete and - crucially - no RecordInvalid, whose
	// absence would fail the strict mock if the message were mis-classified as poison).
	validCases := []struct {
		name string
		env  goutilsRedis.QueueMessageEnvelope
	}{
		{
			name: "process workflow",
			env:  models.PrepareIPCMsgWFProcessWorkflow("unit-test", ulid.Make().String(), ts),
		},
		{
			name: "cancel workflow",
			env:  models.PrepareIPCMsgWFCancelWorkflow("unit-test", ulid.Make().String(), ts),
		},
		{
			name: "schedule step",
			env:  models.PrepareIPCMsgWFScheduleStep("unit-test", ulid.Make().String(), ts),
		},
		{
			name: "step exec update",
			env: models.PrepareIPCMsgWFStepExecUpdate(
				"unit-test", ulid.Make().String(), models.WorkflowStepStateComplete, ts,
			),
		},
		{
			name: "revive workflow",
			env:  models.PrepareIPCMsgWFReviveWorkflow("unit-test", ulid.Make().String(), nil, ts),
		},
		{
			name: "maintenance",
			env:  models.PrepareIPCMsgWFMaintenance("unit-test", ts),
		},
	}

	for _, tc := range validCases {
		t.Run(tc.name+" is re-enqueued onto the main queue", func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			ipcReceiver := mockcommon.NewIPCMessageReceive(t)
			s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

			ipcReceiver.EXPECT().
				DequeueBufferedMessage(mock.Anything, true, mock.Anything).
				Return(tc.env, nil).
				Once()
			ipcReceiver.EXPECT().
				DequeueBufferedMessage(mock.Anything, true, mock.Anything).
				Return(nil, nil).
				Once()
			ipcReceiver.EXPECT().ReEnqueueOnMainQueue(mock.Anything, tc.env).Return(nil).Once()

			assert.Nil(s.recoverBufferedMessages(utCtx))
		})
	}

	t.Run("re-enqueue error is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		cancelWF := models.PrepareIPCMsgWFCancelWorkflow("unit-test", ulid.Make().String(), ts)

		ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(cancelWF, nil).
			Once()
		ipcReceiver.EXPECT().
			ReEnqueueOnMainQueue(mock.Anything, cancelWF).
			Return(simErr).
			Once()

		err := s.recoverBufferedMessages(utCtx)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("drains multiple messages until the buffer is empty", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		poison := fixedEnvelope{payload: "not json"}
		valid := models.PrepareIPCMsgWFProcessWorkflow("unit-test", ulid.Make().String(), ts)

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

// TestSchedulerReportFatal verifies the workflow scheduler's OnFatal plumbing: reportFatal forwards
// the (reporter, err, timestamp) fault to the caller-supplied callback, and does so at most once for
// the lifetime of the scheduler even when tripped concurrently. This is the piece processQueue
// relies on to hand a fatal fault to the parent instead of calling log.Fatal directly.
func TestSchedulerReportFatal(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	simErr := fmt.Errorf("simulated failure")

	t.Run("forwards the fault to the callback", func(t *testing.T) {
		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		s := newDispatchTestScheduler(t, mockdb.NewClient(t), nil)
		s.onFatal = cbMock.OnFatal

		now := time.Now().UTC()
		cbMock.EXPECT().OnFatal("workflow-scheduler", simErr, now).Return().Once()

		s.reportFatal("workflow-scheduler", simErr, now)
	})

	t.Run("invokes the callback at most once under concurrency", func(t *testing.T) {
		// The single .Once() expectation is itself the assertion: the mock fails on any second
		// call, so 16 concurrent reportFatal calls slipping past the guard would be caught.
		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		s := newDispatchTestScheduler(t, mockdb.NewClient(t), nil)
		s.onFatal = cbMock.OnFatal

		cbMock.EXPECT().OnFatal("workflow-scheduler", simErr, mock.Anything).Return().Once()

		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.reportFatal("workflow-scheduler", simErr, time.Now())
			}()
		}
		wg.Wait()
	})
}
