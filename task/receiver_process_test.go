package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
)

// badPayloadEnvelope a goutilsRedis.QueueMessageEnvelope whose StringPayload always
// fails, used to drive the "unreadable payload" validation branch.
type badPayloadEnvelope struct {
	err error
}

// StringPayload return the configured error
func (b badPayloadEnvelope) StringPayload() (string, error) {
	return "", b.err
}

// stubExecutor a minimal Executor test double. It lives in this file (not the
// generated mocks/task package) because that package imports `task`, and a white-box
// test in `package task` importing it would form an import cycle. Only
// ProcessExecutionInstance is exercised by ProcessOneIPCRequest.
type stubExecutor struct {
	// submitErr the error ProcessExecutionInstance returns.
	submitErr error
	// submittedID captures the instance ID passed to ProcessExecutionInstance.
	submittedID string
	// submitCalls counts ProcessExecutionInstance invocations.
	submitCalls int
}

func (s *stubExecutor) ProcessExecutionInstance(_ context.Context, instanceID string) error {
	s.submitCalls++
	s.submittedID = instanceID
	return s.submitErr
}

func (s *stubExecutor) Stop(context.Context) error {
	panic("Stop not expected in ProcessOneIPCRequest tests")
}

// newProcessTestReceiver build a white-box receiverImpl suitable for driving
// ProcessOneIPCRequest directly. It wires a registered validator (required by
// ParseIPCMessage), the persistence client, and the scheduler IPC sender.
func newProcessTestReceiver(
	t *testing.T, mockClient *mockdb.Client, mockSender *mockcommon.IPCMessageSend,
) *receiverImpl {
	validate := validator.New()
	require.NoError(t, models.RegisterWithValidator(validate))

	return &receiverImpl{
		Component:                  goutils.Component{LogTags: log.Fields{"module": "task"}},
		validator:                  validate,
		config:                     models.TaskReceiverConfig{Name: "unit-test-worker"},
		support:                    ExecutorSupport{Persistence: mockClient},
		schedulerIPCSender:         mockSender,
		ipcMsgPoolLock:             &sync.Mutex{},
		execInstanceOriginalIPCMsg: map[string]goutilsRedis.QueueMessageEnvelope{},
	}
}

// runTxAgainst returns a RunAndReturn body that invokes the transaction closure
// against the supplied mock Database, mirroring production behavior.
func runTxAgainst(
	mockDatabase *mockdb.Database,
) func(context.Context, func(context.Context, db.Database) error) error {
	return func(ctx context.Context, core func(context.Context, db.Database) error) error {
		return core(ctx, mockDatabase)
	}
}

// TestProcessOneIPCRequestValidation covers Phase 0 (dequeue) and Phase 1 (request
// validation): a dequeue error is fatal, an empty dequeue is a no-op, and every
// malformed / wrong-type message is dropped from the buffer without touching the DB,
// scheduler, or executor.
func TestProcessOneIPCRequestValidation(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	queueName := "unit-test-queue"
	simErr := fmt.Errorf("simulated failure")

	type testCase struct {
		name string
		// dequeueMsg / dequeueErr the DequeueMessage return.
		dequeueMsg goutilsRedis.QueueMessageEnvelope
		dequeueErr error
		// expectDelete whether the buffered message should be discarded.
		expectDelete bool
		// expectFatal whether the method should return a non-nil (fatal) error.
		expectFatal bool
	}

	cases := []testCase{
		{
			name:        "dequeue error is fatal",
			dequeueErr:  simErr,
			expectFatal: true,
		},
		{
			name: "no message is noop",
		},
		{
			name:         "unreadable payload dropped",
			dequeueMsg:   badPayloadEnvelope{err: simErr},
			expectDelete: true,
		},
		{
			name:         "unparsable payload dropped",
			dequeueMsg:   recordedEnvelope{payload: "not-json"},
			expectDelete: true,
		},
		{
			name: "wrong message type dropped",
			dequeueMsg: models.PrepareIPCMsgTaskExecutionProcessSucceeded(
				"scheduler", ulid.Make().String(), time.Now().UTC(),
			),
			expectDelete: true,
		},
		{
			name: "unsupported parsed type dropped",
			dequeueMsg: models.PrepareIPCMsgNewPendingTask(
				"scheduler", ulid.Make().String(), time.Now().UTC(),
			),
			expectDelete: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			queueReceiver := mockcommon.NewIPCMessageReceive(t)
			executor := &stubExecutor{}

			r := newProcessTestReceiver(t, mockClient, mockSender)

			queueReceiver.EXPECT().
				DequeueMessage(mock.Anything, true, mock.Anything).
				Return(tc.dequeueMsg, tc.dequeueErr)

			if tc.expectDelete {
				// A dropped (poison) message is recorded as an audit event before it
				// is deleted from the buffer.
				mockClient.EXPECT().
					UseDatabaseInTransaction(mock.Anything, mock.Anything).
					RunAndReturn(runTxAgainst(mockDatabase))
				mockDatabase.EXPECT().
					RecordInvalidTaskIPCMessage(
						mock.Anything, r.config.Name+"/receiver", mock.Anything, mock.Anything,
					).
					Return(nil)

				queueReceiver.EXPECT().
					DeleteBufferedMessage(mock.Anything, tc.dequeueMsg).
					Return(nil)
			}

			err := r.processOneIPCRequest(utCtx, queueName, queueReceiver, executor)

			if tc.expectFatal {
				assert.NotNil(err)
				var recvErr models.TaskReceiverError
				assert.True(
					errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err,
				)
			} else {
				assert.Nil(err)
			}
			assert.Empty(r.execInstanceOriginalIPCMsg)
			// Validation never reaches the executor.
			assert.Zero(executor.submitCalls)
		})
	}
}

// TestProcessOneIPCRequestClaimOwnership covers Phase 2: claiming ownership of a
// valid execution request. A non-SQL claim failure is a request-level error (drop +
// notify scheduler, non-fatal), while a SQLError in the chain is fatal for the worker.
func TestProcessOneIPCRequestClaimOwnership(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	queueName := "unit-test-queue"
	simErr := fmt.Errorf("simulated failure")

	t.Run("claim non-fatal failure notifies scheduler", func(t *testing.T) {
		assert := assert.New(t)

		instanceID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionRequested("scheduler", instanceID, time.Now().UTC())

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		queueReceiver := mockcommon.NewIPCMessageReceive(t)
		executor := &stubExecutor{}

		r := newProcessTestReceiver(t, mockClient, mockSender)

		queueReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)

		// Claim fails with a plain (non-SQL) error -> PersistenceError, non-fatal.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			MarkTaskExecAcquired(mock.Anything, instanceID, r.config.Name).
			Return(simErr)

		// The bad request is dropped and the scheduler is notified of the engine failure.
		queueReceiver.EXPECT().
			DeleteBufferedMessage(mock.Anything, msg).
			Return(nil)
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Run(func(_ context.Context, m goutilsRedis.QueueMessageEnvelope) {
				execMsg, ok := m.(models.IPCMessageExecuteInstance)
				assert.True(ok, "expected IPCMessageExecuteInstance, got %T", m)
				assert.Equal(models.IPCMsgTypeEngineFailed, execMsg.Type)
				assert.Equal(instanceID, execMsg.InstanceID)
			}).
			Return(nil)

		err := r.processOneIPCRequest(utCtx, queueName, queueReceiver, executor)
		assert.Nil(err)
		assert.Empty(r.execInstanceOriginalIPCMsg)
		assert.Zero(executor.submitCalls)
	})

	t.Run("claim SQL failure is fatal", func(t *testing.T) {
		assert := assert.New(t)

		instanceID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionRequested("scheduler", instanceID, time.Now().UTC())

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		queueReceiver := mockcommon.NewIPCMessageReceive(t)
		executor := &stubExecutor{}

		r := newProcessTestReceiver(t, mockClient, mockSender)

		queueReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)

		// Claim fails with a SQLError in the chain -> fatal.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			MarkTaskExecAcquired(mock.Anything, instanceID, r.config.Name).
			Return(models.NewSQLError("boom", simErr, false))

		err := r.processOneIPCRequest(utCtx, queueName, queueReceiver, executor)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(
			errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err,
		)
		var sqlErr models.SQLError
		assert.True(
			errors.As(err, &sqlErr), "expected SQLError in chain, got %T: %v", err, err,
		)
		assert.Empty(r.execInstanceOriginalIPCMsg)
		assert.Zero(executor.submitCalls)
	})
}

// TestProcessOneIPCRequestSubmit covers Phase 3: submitting a claimed request to the
// executor. The happy path records the message for async completion; a submit failure
// drops the message, marks the instance failed, and notifies the scheduler (non-fatal),
// unless marking failed hits a SQLError, which is fatal.
func TestProcessOneIPCRequestSubmit(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	queueName := "unit-test-queue"
	simErr := fmt.Errorf("simulated failure")

	t.Run("happy path records and returns", func(t *testing.T) {
		assert := assert.New(t)

		instanceID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionRequested("scheduler", instanceID, time.Now().UTC())

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		queueReceiver := mockcommon.NewIPCMessageReceive(t)
		executor := &stubExecutor{submitErr: nil}

		r := newProcessTestReceiver(t, mockClient, mockSender)

		queueReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)

		// Claim succeeds.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			MarkTaskExecAcquired(mock.Anything, instanceID, r.config.Name).
			Return(nil)

		err := r.processOneIPCRequest(utCtx, queueName, queueReceiver, executor)
		assert.Nil(err)

		// Submit succeeded exactly once against the claimed instance.
		assert.Equal(1, executor.submitCalls)
		assert.Equal(instanceID, executor.submittedID)

		// The message is recorded for the async OnTaskComplete callback to clean up.
		recorded, ok := r.execInstanceOriginalIPCMsg[instanceID]
		assert.True(ok, "expected recorded pool entry for submitted instance")
		assert.Equal(msg, recorded)
	})

	t.Run("submit failure non-fatal", func(t *testing.T) {
		assert := assert.New(t)

		instanceID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionRequested("scheduler", instanceID, time.Now().UTC())

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		queueReceiver := mockcommon.NewIPCMessageReceive(t)
		executor := &stubExecutor{submitErr: simErr}

		r := newProcessTestReceiver(t, mockClient, mockSender)

		queueReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)

		// Both the claim and the mark-failed transactions run against the mock DB.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			MarkTaskExecAcquired(mock.Anything, instanceID, r.config.Name).
			Return(nil)

		// The recorded message is dropped, the instance marked failed, scheduler notified.
		queueReceiver.EXPECT().
			DeleteBufferedMessage(mock.Anything, msg).
			Return(nil)
		mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, instanceID, mock.Anything, mock.Anything, mock.Anything).
			Return(nil)
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Run(func(_ context.Context, m goutilsRedis.QueueMessageEnvelope) {
				execMsg, ok := m.(models.IPCMessageExecuteInstance)
				assert.True(ok, "expected IPCMessageExecuteInstance, got %T", m)
				assert.Equal(models.IPCMsgTypeEngineFailed, execMsg.Type)
				assert.Equal(instanceID, execMsg.InstanceID)
			}).
			Return(nil)

		err := r.processOneIPCRequest(utCtx, queueName, queueReceiver, executor)
		assert.Nil(err)
		assert.Empty(r.execInstanceOriginalIPCMsg)
		assert.Equal(1, executor.submitCalls)
	})

	t.Run("submit failure with SQL mark-failed is fatal", func(t *testing.T) {
		assert := assert.New(t)

		instanceID := ulid.Make().String()
		msg := models.PrepareIPCMsgTaskExecutionRequested("scheduler", instanceID, time.Now().UTC())

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		queueReceiver := mockcommon.NewIPCMessageReceive(t)
		executor := &stubExecutor{submitErr: simErr}

		r := newProcessTestReceiver(t, mockClient, mockSender)

		queueReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			Return(msg, nil)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().
			MarkTaskExecAcquired(mock.Anything, instanceID, r.config.Name).
			Return(nil)

		// The message is dropped (before the fatal mark), then marking failed hits a
		// SQLError -> fatal, so the scheduler is never notified.
		queueReceiver.EXPECT().
			DeleteBufferedMessage(mock.Anything, msg).
			Return(nil)
		mockDatabase.EXPECT().
			MarkTaskExecFailed(mock.Anything, instanceID, mock.Anything, mock.Anything, mock.Anything).
			Return(models.NewSQLError("boom", simErr, false))

		err := r.processOneIPCRequest(utCtx, queueName, queueReceiver, executor)
		assert.NotNil(err)
		var sqlErr models.SQLError
		assert.True(
			errors.As(err, &sqlErr), "expected SQLError in chain, got %T: %v", err, err,
		)
		assert.Empty(r.execInstanceOriginalIPCMsg)
	})
}

// TestReceiverReportFatal verifies the receiver's OnFatal plumbing: reportFatal forwards the
// (reporter, err, timestamp) fault to the caller-supplied callback, and does so at most once for
// the lifetime of the receiver even when tripped concurrently. Because the receiver runs one
// processOneQueue goroutine per task queue, several queue threads can hit the same broken-DB fault
// simultaneously; the once-guard ensures the parent's callback still fires exactly once.
func TestReceiverReportFatal(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	simErr := fmt.Errorf("simulated failure")

	t.Run("forwards the fault to the callback", func(t *testing.T) {
		assert := assert.New(t)

		var gotReporter string
		var gotErr error
		var gotTS time.Time
		var calls int
		r := newProcessTestReceiver(t, mockdb.NewClient(t), nil)
		r.onFatal = func(reporter string, err error, timestamp time.Time) {
			calls++
			gotReporter, gotErr, gotTS = reporter, err, timestamp
		}

		now := time.Now().UTC()
		r.reportFatal("unit-test-worker/receiver:some-queue", simErr, now)

		assert.Equal(1, calls)
		assert.Equal("unit-test-worker/receiver:some-queue", gotReporter)
		assert.Equal(simErr, gotErr)
		assert.Equal(now, gotTS)
	})

	t.Run("invokes the callback at most once across queue threads", func(t *testing.T) {
		assert := assert.New(t)

		var calls int32
		r := newProcessTestReceiver(t, mockdb.NewClient(t), nil)
		r.onFatal = func(_ string, _ error, _ time.Time) {
			atomic.AddInt32(&calls, 1)
		}

		// Simulate N queue threads tripping the same broken-DB fault at once.
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			queue := fmt.Sprintf("queue-%d", i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.reportFatal("unit-test-worker/receiver:"+queue, simErr, time.Now())
			}()
		}
		wg.Wait()

		assert.Equal(int32(1), atomic.LoadInt32(&calls))
	})
}
