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

// TestProcessOneIPCRequestDispatch covers the parse -> dispatch wiring and the delete contract
// for each workflow message shape. For this skeleton slice the five event cases are stubs that
// return a "not yet implemented" WorkflowSchedulerError, so the message is left buffered (the
// scheduler crashed on it - startup recovery will replay it); Maintenance is a NOOP, so its
// message is deleted; an unknown type is recorded and dropped.
func TestProcessOneIPCRequestDispatch(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	ts := time.Now().UTC()

	// dispatchEnvelope build the wire envelope for a workflow message via its Prepare* constructor.
	// stubCases: each remaining event message that still hits an unimplemented stub. The handler
	// returns a fatal error, so the message must be LEFT buffered (DeleteBufferedMessage NOT
	// called) for startup replay. (Process Workflow, Schedule Workflow Step, and Workflow Step
	// Execution Update are now implemented and have their own tests in scheduler_process_workflow_test.go
	// / scheduler_schedule_step_test.go / scheduler_step_exec_update_test.go, so they are not listed here.)
	stubCases := []struct {
		name string
		env  goutilsRedis.QueueMessageEnvelope
	}{
		{
			name: "cancel workflow",
			env:  models.PrepareIPCMsgWFCancelWorkflow("unit-test", ulid.Make().String(), ts),
		},
	}

	for _, tc := range stubCases {
		t.Run(tc.name+" stub leaves message buffered", func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			ipcReceiver := mockcommon.NewIPCMessageReceive(t)
			s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

			payload, err := tc.env.StringPayload()
			require.NoError(t, err)

			ipcReceiver.EXPECT().
				DequeueMessage(mock.Anything, true, mock.Anything).
				Return(fixedEnvelope{payload: payload}, nil)
			// No DeleteBufferedMessage expectation: the fatal stub error must leave it buffered.
			// No DB expectation: a valid message is not recorded as poison.

			err = s.processOneIPCRequest(utCtx)
			assert.NotNil(err)
			assertWorkflowSchedulerError(t, err)
		})
	}

	t.Run("maintenance NOOP deletes the message", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		ipcReceiver := mockcommon.NewIPCMessageReceive(t)
		s := newDispatchTestScheduler(t, mockClient, ipcReceiver)

		payload, err := models.PrepareIPCMsgWFMaintenance("unit-test", ts).StringPayload()
		require.NoError(t, err)
		msg := fixedEnvelope{payload: payload}

		ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)
		// Maintenance is handled (NOOP) successfully, so the message is deleted.
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
