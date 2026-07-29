package task_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	mockredis "github.com/alwitt/goutils/mocks/redis"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/db"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktest "github.com/alwitt/tasking/mocks/test"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// clientTestHarness bundles the mocks a Client test drives. The sender factory is bound to
// the callback collector; the scheduler IPC sender it hands back is `sender`, so tests set
// expectations on `sender.EnqueueMessage` to observe what is submitted to the scheduler.
type clientTestHarness struct {
	cbMock     *mocktest.UnitTestCallbackCollector
	mockClient *mockdb.Client
	mockRedis  goutilsRedis.Client
	sender     *mockcommon.IPCMessageSend
}

// newClientTestHarness build a Client backed by mocks, wiring the sender factory to return
// `sender`. `config` is used verbatim so individual tests can inject retry overrides.
func newClientTestHarness(
	utCtx context.Context, t *testing.T, config models.TaskClientConfig,
) (task.Client, *clientTestHarness) {
	h := &clientTestHarness{
		cbMock:     mocktest.NewUnitTestCallbackCollector(t),
		mockClient: mockdb.NewClient(t),
		mockRedis:  mockredis.NewClient(t),
		sender:     mockcommon.NewIPCMessageSend(t),
	}

	h.cbMock.EXPECT().
		NewRedisIPCMsgSender(mock.Anything, config.SchedulerQueue, mock.Anything, mock.Anything).
		Return(h.sender, nil).
		Once()

	client, err := task.NewClient(utCtx, task.NewClientParams{
		Name:             "unit-test-client",
		DefaultCreator:   testDefaultCreator,
		Persistence:      h.mockClient,
		Config:           config,
		Redis:            h.mockRedis,
		IPCSenderFactory: h.cbMock.NewRedisIPCMsgSender,
	})
	assert.Nil(t, err)
	assert.NotNil(t, client)
	return client, h
}

// testDefaultCreator the DefaultCreator wired into the test harness client, used to assert
// creator resolution when a submit does not supply a per-task override.
const testDefaultCreator = "unit-test-default-creator"

// baseClientConfig a minimal valid client config. RetrySettings carries a single entry
// because NewClient validates NewClientParams and TaskClientConfig.RetrySettings is
// `required,gte=1`; the entry deliberately targets a task name no test submits, so tasks
// named "unit-test-task" still fall through to the default retry parameters. Tests that
// care about retry override resolution overwrite RetrySettings entirely.
func baseClientConfig() models.TaskClientConfig {
	return models.TaskClientConfig{
		SchedulerQueue: "scheduler-q",
		RetrySettings: []models.PerTaskRetryParam{
			{
				TaskName: "unrelated-retry-task",
				Retry:    models.RetryParam{InitialDelaySec: 5, MaxRetries: 3},
			},
		},
	}
}

// runTxForClient returns a RunAndReturn body that invokes the transaction closure against the
// supplied mock Database, mirroring Client.UseDatabaseInTransaction (the nil-activeDBClient
// path taken by every test here).
func runTxForClient(
	mockDatabase *mockdb.Database,
) func(context.Context, func(context.Context, db.Database) error) error {
	return func(ctx context.Context, core func(context.Context, db.Database) error) error {
		return core(ctx, mockDatabase)
	}
}

// clientTestValidator build a validator wired with the model macros, for parsing the IPC
// messages the client enqueues.
func clientTestValidator(t *testing.T) *validator.Validate {
	v := validator.New()
	assert.Nil(t, models.RegisterWithValidator(v))
	return v
}

// parseEnqueuedSystemTask parse a submitted IPC message envelope back into an
// IPCMessageSystemTask so its type/task ID can be asserted.
func parseEnqueuedSystemTask(
	t *testing.T, msg goutilsRedis.QueueMessageEnvelope,
) models.IPCMessageSystemTask {
	payload, err := msg.StringPayload()
	assert.Nil(t, err)
	parsed, err := models.ParseIPCMessage(clientTestValidator(t), []byte(payload))
	assert.Nil(t, err)
	asSystemTask, ok := parsed.(models.IPCMessageSystemTask)
	assert.True(t, ok, "expected IPCMessageSystemTask, got %T", parsed)
	return asSystemTask
}

// TestNewClient validates the constructor's factory error branch and happy path.
func TestNewClient(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	t.Run("IPC sender factory fails", func(t *testing.T) {
		assert := assert.New(t)

		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		mockClient := mockdb.NewClient(t)
		mockRedis := mockredis.NewClient(t)

		simErr := fmt.Errorf("simulated factory failure")
		cbMock.EXPECT().
			NewRedisIPCMsgSender(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, simErr)

		client, err := task.NewClient(utCtx, task.NewClientParams{
			Name:             "unit-test-client",
			Persistence:      mockClient,
			Config:           baseClientConfig(),
			Redis:            mockRedis,
			IPCSenderFactory: cbMock.NewRedisIPCMsgSender,
		})
		assert.Nil(client)
		assert.NotNil(err)
		var clientErr models.TaskClientError
		assert.True(errors.As(err, &clientErr), "expected TaskClientError, got %T: %v", err, err)
	})

	t.Run("happy path", func(t *testing.T) {
		assert := assert.New(t)

		_, h := newClientTestHarness(utCtx, t, baseClientConfig())
		assert.NotNil(h.sender)
	})
}

// TestClientDefineAndRunImmediateOneShotTask covers the immediate one-shot submission path:
// the happy path (define + scheduler notify), retry override resolution, and the two error
// classes the caller is meant to distinguish via errors.As.
func TestClientDefineAndRunImmediateOneShotTask(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	t.Run("happy path submits a NEW_TASK for the created task", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		created := models.Task{ID: ulid.Make().String()}
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewOneShotTask(mock.Anything, mock.MatchedBy(func(p db.NewTaskParameter) bool {
				// With no override configured, the default retry parameters are used, and a
				// nil creator override resolves to the client's DefaultCreator.
				return p.Name == "unit-test-task" &&
					p.Creator == testDefaultCreator &&
					p.RetryParam.MaxRetries == models.DefaultTaskRetryParameters().MaxRetries
			})).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		h.sender.EXPECT().
			EnqueueMessage(mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
				parsed := parseEnqueuedSystemTask(t, msg)
				return parsed.Type == models.IPCMsgTypeNewTask && parsed.TaskID == created.ID
			})).
			Return(nil)

		task, err := client.DefineAndRunImmediateOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task"}, nil,
		)
		assert.Nil(err)
		assert.Equal(created.ID, task.ID)
	})

	t.Run("per-task creator override takes precedence over the default", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		override := "per-task-creator"
		created := models.Task{ID: ulid.Make().String()}
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewOneShotTask(mock.Anything, mock.MatchedBy(func(p db.NewTaskParameter) bool {
				return p.Creator == override
			})).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		h.sender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil)

		_, err := client.DefineAndRunImmediateOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task", Creator: &override}, nil,
		)
		assert.Nil(err)
	})

	t.Run("empty task name is rejected before any DB work", func(t *testing.T) {
		assert := assert.New(t)

		// No DB or sender expectations: params validation must short-circuit before touching
		// either.
		client, _ := newClientTestHarness(utCtx, t, baseClientConfig())

		task, err := client.DefineAndRunImmediateOneShotTask(
			utCtx, task.DefineTaskParams{}, nil,
		)
		assert.NotNil(err)
		assert.Empty(task.ID)
		var badInput goutils.BadInputError
		assert.True(errors.As(err, &badInput), "expected BadInputError, got %T: %v", err, err)
	})

	t.Run("configured retry override is applied", func(t *testing.T) {
		assert := assert.New(t)

		config := baseClientConfig()
		config.RetrySettings = []models.PerTaskRetryParam{
			{
				TaskName: "unit-test-task",
				Retry:    models.RetryParam{InitialDelaySec: 11, MaxRetries: 7},
			},
		}
		client, h := newClientTestHarness(utCtx, t, config)

		created := models.Task{ID: ulid.Make().String()}
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewOneShotTask(mock.Anything, mock.MatchedBy(func(p db.NewTaskParameter) bool {
				return p.RetryParam.MaxRetries == 7 && p.RetryParam.InitialDelaySec == 11
			})).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		h.sender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil)

		_, err := client.DefineAndRunImmediateOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task"}, nil,
		)
		assert.Nil(err)
	})

	t.Run("define failure yields a PersistenceError and no submit", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		simErr := fmt.Errorf("simulated define failure")
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewOneShotTask(mock.Anything, mock.Anything).
			Return(models.Task{}, simErr)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		// No EnqueueMessage expectation: the strict mock fails if a submit is attempted.

		task, err := client.DefineAndRunImmediateOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task"}, nil,
		)
		assert.NotNil(err)
		assert.Empty(task.ID)
		var persistErr goutils.PersistenceError
		assert.True(
			errors.As(err, &persistErr), "expected PersistenceError, got %T: %v", err, err,
		)
	})

	t.Run("submit failure yields an IPCMessageQueueError and returns the task", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		created := models.Task{ID: ulid.Make().String()}
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewOneShotTask(mock.Anything, mock.Anything).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		simErr := models.NewIPCMessageQueueError("simulated enqueue failure", nil, true)
		h.sender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr)

		task, err := client.DefineAndRunImmediateOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task"}, nil,
		)
		assert.NotNil(err)
		// The task row was created; the caller gets it back despite the failed submit.
		assert.Equal(created.ID, task.ID)
		var queueErr models.IPCMessageQueueError
		assert.True(
			errors.As(err, &queueErr), "expected IPCMessageQueueError, got %T: %v", err, err,
		)
	})
}

// TestClientDefineThenSubmitSplit covers the define/submit split introduced for callers that
// need to commit their own state between defining a task and submitting it (state-before-poke):
// DefineImmediateOneShotTask defines without poking, SubmitTask pokes on its own, and the
// per-submission Retry override wins over both the by-name policy and the default.
func TestClientDefineThenSubmitSplit(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	t.Run("DefineImmediateOneShotTask defines without submitting", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		created := models.Task{ID: ulid.Make().String()}
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewOneShotTask(mock.Anything, mock.Anything).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		// No EnqueueMessage expectation: the strict sender mock fails if a poke is attempted.

		defined, err := client.DefineImmediateOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task"}, nil,
		)
		assert.Nil(err)
		assert.Equal(created.ID, defined.ID)
	})

	t.Run("per-submission Retry override wins over by-name and default", func(t *testing.T) {
		assert := assert.New(t)

		// The by-name policy for "unit-test-task" is present but must be overridden by the
		// per-submission Retry.
		config := baseClientConfig()
		config.RetrySettings = []models.PerTaskRetryParam{
			{
				TaskName: "unit-test-task",
				Retry:    models.RetryParam{InitialDelaySec: 11, MaxRetries: 7},
			},
		}
		client, h := newClientTestHarness(utCtx, t, config)

		override := models.TaskRetryParameters{InitialDelaySec: 3, MaxRetries: 99, Factor: 2.5}
		created := models.Task{ID: ulid.Make().String()}
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewOneShotTask(mock.Anything, mock.MatchedBy(func(p db.NewTaskParameter) bool {
				// The full override is used verbatim, Factor included (which the by-name policy
				// cannot express).
				return p.RetryParam.MaxRetries == 99 && p.RetryParam.InitialDelaySec == 3 &&
					p.RetryParam.Factor == 2.5
			})).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		_, err := client.DefineImmediateOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task", Retry: &override}, nil,
		)
		assert.Nil(err)
	})

	t.Run("SubmitTask pokes a NEW_TASK for the given task ID", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		taskID := ulid.Make().String()
		h.sender.EXPECT().
			EnqueueMessage(mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
				parsed := parseEnqueuedSystemTask(t, msg)
				return parsed.Type == models.IPCMsgTypeNewTask && parsed.TaskID == taskID
			})).
			Return(nil)

		assert.Nil(client.SubmitTask(utCtx, taskID))
	})

	t.Run("SubmitTask failure yields an IPCMessageQueueError", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		simErr := models.NewIPCMessageQueueError("simulated enqueue failure", nil, true)
		h.sender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr)

		err := client.SubmitTask(utCtx, ulid.Make().String())
		assert.NotNil(err)
		var queueErr models.IPCMessageQueueError
		assert.True(
			errors.As(err, &queueErr), "expected IPCMessageQueueError, got %T: %v", err, err,
		)
	})
}

// TestClientDefineAndRunScheduledOneShotTask covers the scheduled one-shot submission path,
// including the deadline-before-runtime guard that short-circuits before any DB work.
func TestClientDefineAndRunScheduledOneShotTask(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	t.Run("happy path defines with the target runtime and submits", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		targetRuntime := time.Now().UTC().Add(time.Hour)
		created := models.Task{ID: ulid.Make().String()}
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewScheduledOneShotTask(
				mock.Anything,
				mock.MatchedBy(func(p db.NewTaskParameter) bool {
					// A nil creator override resolves to the client's DefaultCreator.
					return p.Creator == testDefaultCreator
				}),
				mock.MatchedBy(func(tgt time.Time) bool {
					return tgt.Equal(targetRuntime)
				}),
			).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		h.sender.EXPECT().
			EnqueueMessage(mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
				parsed := parseEnqueuedSystemTask(t, msg)
				return parsed.Type == models.IPCMsgTypeNewTask && parsed.TaskID == created.ID
			})).
			Return(nil)

		task, err := client.DefineAndRunScheduledOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task"}, targetRuntime, nil,
		)
		assert.Nil(err)
		assert.Equal(created.ID, task.ID)
	})

	t.Run("deadline before target runtime is rejected before any DB work", func(t *testing.T) {
		assert := assert.New(t)

		// No DB or sender expectations: the guard must short-circuit before touching either.
		client, _ := newClientTestHarness(utCtx, t, baseClientConfig())

		targetRuntime := time.Now().UTC().Add(time.Hour)
		deadline := targetRuntime.Add(-time.Minute)

		task, err := client.DefineAndRunScheduledOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task", Deadline: &deadline}, targetRuntime, nil,
		)
		assert.NotNil(err)
		assert.Empty(task.ID)
		var badInput goutils.BadInputError
		assert.True(errors.As(err, &badInput), "expected BadInputError, got %T: %v", err, err)
	})

	t.Run("submit failure yields an IPCMessageQueueError", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		created := models.Task{ID: ulid.Make().String()}
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewScheduledOneShotTask(mock.Anything, mock.Anything, mock.Anything).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		simErr := models.NewIPCMessageQueueError("simulated enqueue failure", nil, true)
		h.sender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr)

		task, err := client.DefineAndRunScheduledOneShotTask(
			utCtx, task.DefineTaskParams{Name: "unit-test-task"}, time.Now().UTC().Add(time.Hour), nil,
		)
		assert.NotNil(err)
		assert.Equal(created.ID, task.ID)
		var queueErr models.IPCMessageQueueError
		assert.True(
			errors.As(err, &queueErr), "expected IPCMessageQueueError, got %T: %v", err, err,
		)
	})
}

// TestClientCancelTask covers the cancel path: the happy path (confirm-then-notify), a read
// failure, and a submit failure.
func TestClientCancelTask(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	t.Run("happy path submits a CANCEL_TASK for the task", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		taskID := ulid.Make().String()
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{ID: taskID}, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		h.sender.EXPECT().
			EnqueueMessage(mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
				parsed := parseEnqueuedSystemTask(t, msg)
				return parsed.Type == models.IPCMsgTypeCancelTask && parsed.TaskID == taskID
			})).
			Return(nil)

		assert.Nil(client.CancelTask(utCtx, taskID, nil))
	})

	t.Run("read failure yields a PersistenceError and no submit", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		taskID := ulid.Make().String()
		simErr := fmt.Errorf("simulated read failure")
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{}, simErr)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		// No EnqueueMessage expectation: the strict mock fails if a submit is attempted.

		err := client.CancelTask(utCtx, taskID, nil)
		assert.NotNil(err)
		var persistErr goutils.PersistenceError
		assert.True(
			errors.As(err, &persistErr), "expected PersistenceError, got %T: %v", err, err,
		)
	})

	t.Run("submit failure yields an IPCMessageQueueError", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t, baseClientConfig())

		taskID := ulid.Make().String()
		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			GetTask(mock.Anything, taskID).
			Return(models.Task{ID: taskID}, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		simErr := models.NewIPCMessageQueueError("simulated enqueue failure", nil, true)
		h.sender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr)

		err := client.CancelTask(utCtx, taskID, nil)
		assert.NotNil(err)
		var queueErr models.IPCMessageQueueError
		assert.True(
			errors.As(err, &queueErr), "expected IPCMessageQueueError, got %T: %v", err, err,
		)
	})
}
