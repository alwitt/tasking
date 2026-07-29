package workflow_test

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
	"github.com/alwitt/tasking/workflow"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// testDefaultCreator the DefaultCreator wired into the test harness client, used to assert
// creator resolution when a define does not supply a per-workflow override.
const testDefaultCreator = "unit-test-default-creator"

// testStepType the one step Type the harness client registers as known; specs the tests define
// use this Type so they pass the up-front Type check unless a test deliberately uses another.
const testStepType = "unit-test-step-type"

// clientTestHarness bundles the mocks a Client test drives. The sender factory is bound to the
// callback collector; the scheduler IPC sender it hands back is `sender`, so tests set
// expectations on `sender.EnqueueMessage` to observe what is submitted to the scheduler.
type clientTestHarness struct {
	cbMock     *mocktest.UnitTestCallbackCollector
	mockClient *mockdb.Client
	mockRedis  goutilsRedis.Client
	sender     *mockcommon.IPCMessageSend
}

// baseClientConfig a minimal valid workflow client config.
func baseClientConfig() models.WorkflowClientConfig {
	return models.WorkflowClientConfig{SchedulerQueue: "workflow-scheduler-q"}
}

// newClientTestHarness build a Client backed by mocks, wiring the sender factory to return
// `sender`.
func newClientTestHarness(
	utCtx context.Context, t *testing.T,
) (workflow.Client, *clientTestHarness) {
	h := &clientTestHarness{
		cbMock:     mocktest.NewUnitTestCallbackCollector(t),
		mockClient: mockdb.NewClient(t),
		mockRedis:  mockredis.NewClient(t),
		sender:     mockcommon.NewIPCMessageSend(t),
	}

	config := baseClientConfig()
	h.cbMock.EXPECT().
		NewRedisIPCMsgSender(mock.Anything, config.SchedulerQueue, mock.Anything, mock.Anything).
		Return(h.sender, nil).
		Once()

	client, err := workflow.NewClient(utCtx, workflow.NewClientParams{
		Name:             "unit-test-client",
		DefaultCreator:   testDefaultCreator,
		Persistence:      h.mockClient,
		Config:           config,
		Redis:            h.mockRedis,
		IPCSenderFactory: h.cbMock.NewRedisIPCMsgSender,
		KnownStepTypes:   map[string]bool{testStepType: true},
	})
	assert.Nil(t, err)
	assert.NotNil(t, client)
	return client, h
}

// sampleWorkflowSpec build a valid workflow spec whose steps all carry stepType. The steps map is
// stepName -> parent step names.
func sampleWorkflowSpec(
	name, stepType string, deadline time.Time, steps map[string][]string,
) models.NewWorkflowParameter {
	stepParams := map[string]models.NewWorkflowStepParameter{}
	for stepName, parents := range steps {
		parentSet := map[string]bool{}
		for _, p := range parents {
			parentSet[p] = true
		}
		stepParams[stepName] = models.NewWorkflowStepParameter{
			Name:        stepName,
			Type:        stepType,
			RetryParams: models.DefaultTaskRetryParameters(),
			ParentSteps: parentSet,
		}
	}
	return models.NewWorkflowParameter{Name: name, Deadline: deadline, Steps: stepParams}
}

// runTxForClient returns a RunAndReturn body that invokes the transaction closure against the
// supplied mock Database, mirroring Client.UseDatabaseInTransaction (the nil-activeDBClient path
// taken by every test here).
func runTxForClient(
	mockDatabase *mockdb.Database,
) func(context.Context, func(context.Context, db.Database) error) error {
	return func(ctx context.Context, core func(context.Context, db.Database) error) error {
		return core(ctx, mockDatabase)
	}
}

// clientTestValidator build a validator wired with the model macros, for parsing the IPC messages
// the client enqueues.
func clientTestValidator(t *testing.T) *validator.Validate {
	v := validator.New()
	assert.Nil(t, models.RegisterWithValidator(v))
	return v
}

// parseEnqueued parse a submitted IPC message envelope back into its typed model form.
func parseEnqueued(t *testing.T, msg goutilsRedis.QueueMessageEnvelope) interface{} {
	payload, err := msg.StringPayload()
	assert.Nil(t, err)
	parsed, err := models.ParseIPCMessage(clientTestValidator(t), []byte(payload))
	assert.Nil(t, err)
	return parsed
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

		client, err := workflow.NewClient(utCtx, workflow.NewClientParams{
			Name:             "unit-test-client",
			Persistence:      mockClient,
			Config:           baseClientConfig(),
			Redis:            mockRedis,
			IPCSenderFactory: cbMock.NewRedisIPCMsgSender,
			KnownStepTypes:   map[string]bool{testStepType: true},
		})
		assert.Nil(client)
		assert.NotNil(err)
		var clientErr models.WorkflowClientError
		assert.True(errors.As(err, &clientErr), "expected WorkflowClientError, got %T: %v", err, err)
	})

	t.Run("missing known step types rejected", func(t *testing.T) {
		assert := assert.New(t)

		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		mockClient := mockdb.NewClient(t)
		mockRedis := mockredis.NewClient(t)

		client, err := workflow.NewClient(utCtx, workflow.NewClientParams{
			Name:             "unit-test-client",
			Persistence:      mockClient,
			Config:           baseClientConfig(),
			Redis:            mockRedis,
			IPCSenderFactory: cbMock.NewRedisIPCMsgSender,
			KnownStepTypes:   map[string]bool{}, // empty -> validate:"required,gt=0" fails
		})
		assert.Nil(client)
		assert.NotNil(err)
	})

	t.Run("happy path", func(t *testing.T) {
		assert := assert.New(t)
		_, h := newClientTestHarness(utCtx, t)
		assert.NotNil(h.sender)
	})
}

// TestClientDefineWorkflow covers the define-only path: it writes the workflow rows but does NOT
// poke the scheduler, and it rejects an unknown step Type / DB failure before/without a poke.
func TestClientDefineWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()
	deadline := time.Now().UTC().Add(time.Hour)

	t.Run("define-only does not poke the scheduler", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		spec := sampleWorkflowSpec("wf", testStepType, deadline, map[string][]string{"root": {}})
		created := models.Workflow{ID: ulid.Make().String()}

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewWorkflow(mock.Anything, mock.Anything, testDefaultCreator).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		// NOTE: no h.sender.EXPECT() - the sender must NOT be called on the define-only path.

		got, err := client.DefineWorkflow(utCtx, workflow.DefineWorkflowParams{Spec: spec}, nil)
		assert.Nil(err)
		assert.Equal(created.ID, got.ID)
	})

	t.Run("per-workflow creator override", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		spec := sampleWorkflowSpec("wf", testStepType, deadline, map[string][]string{"root": {}})
		override := "explicit-creator"

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewWorkflow(mock.Anything, mock.Anything, override).
			Return(models.Workflow{ID: ulid.Make().String()}, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		_, err := client.DefineWorkflow(
			utCtx, workflow.DefineWorkflowParams{Spec: spec, Creator: &override}, nil,
		)
		assert.Nil(err)
	})

	t.Run("unknown step type rejected before any DB work", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		spec := sampleWorkflowSpec("wf", "unregistered-type", deadline, map[string][]string{"root": {}})
		// NOTE: neither the DB nor the sender is expected to be called.

		_, err := client.DefineWorkflow(utCtx, workflow.DefineWorkflowParams{Spec: spec}, nil)
		assert.NotNil(err)
		var badInput goutils.BadInputError
		assert.True(errors.As(err, &badInput), "expected BadInputError, got %T: %v", err, err)
		_ = h
	})

	t.Run("DB failure surfaced as WorkflowClientError over PersistenceError", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		spec := sampleWorkflowSpec("wf", testStepType, deadline, map[string][]string{"root": {}})
		simErr := fmt.Errorf("simulated DB failure")

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewWorkflow(mock.Anything, mock.Anything, mock.Anything).
			Return(models.Workflow{}, simErr)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		_, err := client.DefineWorkflow(utCtx, workflow.DefineWorkflowParams{Spec: spec}, nil)
		assert.NotNil(err)
		var clientErr models.WorkflowClientError
		assert.True(errors.As(err, &clientErr), "expected WorkflowClientError, got %T", err)
		var persistErr goutils.PersistenceError
		assert.True(errors.As(err, &persistErr), "expected PersistenceError in chain, got %v", err)
	})
}

// TestClientSubmitWorkflow covers the submit-only path: it pokes a Process Workflow event with no
// DB work, and surfaces a send failure as a WorkflowClientError over an IPCMessageQueueError.
func TestClientSubmitWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()

	t.Run("submits a PROCESS_WORKFLOW for the given ID", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		workflowID := ulid.Make().String()

		h.sender.EXPECT().
			EnqueueMessage(
				mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
					parsed := parseEnqueued(t, msg)
					typed, ok := parsed.(models.IPCMessageWorkflow)
					return ok &&
						typed.Type == models.IPCMsgTypeWFProcessWorkflow &&
						typed.WorkflowID == workflowID
				}),
			).Return(nil)

		assert.Nil(client.SubmitWorkflow(utCtx, workflowID))
	})

	t.Run(
		"send failure surfaced as WorkflowClientError over IPCMessageQueueError",
		func(t *testing.T) {
			assert := assert.New(t)

			client, h := newClientTestHarness(utCtx, t)
			simErr := models.NewIPCMessageQueueError("simulated send failure", nil, true)
			h.sender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr)

			err := client.SubmitWorkflow(utCtx, ulid.Make().String())
			assert.NotNil(err)
			var clientErr models.WorkflowClientError
			assert.True(errors.As(err, &clientErr), "expected WorkflowClientError, got %T", err)
			var queueErr models.IPCMessageQueueError
			assert.True(errors.As(err, &queueErr), "expected IPCMessageQueueError in chain, got %v", err)
		},
	)
}

// TestClientDefineAndRunWorkflow covers the combined wrapper: define + submit both happen, and a
// poke failure still returns the created entry alongside the error.
func TestClientDefineAndRunWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()
	deadline := time.Now().UTC().Add(time.Hour)

	t.Run("define then submit", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		spec := sampleWorkflowSpec("wf", testStepType, deadline, map[string][]string{
			"root": {}, "child": {"root"},
		})
		created := models.Workflow{ID: ulid.Make().String()}

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewWorkflow(mock.Anything, mock.Anything, testDefaultCreator).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		h.sender.EXPECT().
			EnqueueMessage(
				mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
					parsed := parseEnqueued(t, msg)
					typed, ok := parsed.(models.IPCMessageWorkflow)
					return ok &&
						typed.Type == models.IPCMsgTypeWFProcessWorkflow &&
						typed.WorkflowID == created.ID
				}),
			).Return(nil)

		got, err := client.DefineAndRunWorkflow(utCtx, workflow.DefineWorkflowParams{Spec: spec}, nil)
		assert.Nil(err)
		assert.Equal(created.ID, got.ID)
	})

	t.Run("poke failure still returns the created entry", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		spec := sampleWorkflowSpec("wf", testStepType, deadline, map[string][]string{"root": {}})
		created := models.Workflow{ID: ulid.Make().String()}

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			DefineNewWorkflow(mock.Anything, mock.Anything, mock.Anything).
			Return(created, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		h.sender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Return(models.NewIPCMessageQueueError("simulated send failure", nil, true))

		got, err := client.DefineAndRunWorkflow(utCtx, workflow.DefineWorkflowParams{Spec: spec}, nil)
		assert.NotNil(err)
		assert.Equal(created.ID, got.ID) // row exists even though the poke was lost
		var queueErr models.IPCMessageQueueError
		assert.True(errors.As(err, &queueErr), "expected IPCMessageQueueError in chain, got %v", err)
	})
}

// TestClientReviveWorkflow covers the revive path: confirm existence, then poke a Revive event with
// or without a new deadline; a not-found short-circuits with no poke.
func TestClientReviveWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()

	t.Run("revive with new deadline", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		workflowID := ulid.Make().String()
		newDeadline := time.Now().UTC().Add(2 * time.Hour)

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, workflowID).
			Return(models.Workflow{ID: workflowID}, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		h.sender.EXPECT().
			EnqueueMessage(
				mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
					parsed := parseEnqueued(t, msg)
					typed, ok := parsed.(models.IPCMessageWorkflowRevive)
					return ok &&
						typed.Type == models.IPCMsgTypeWFReviveWorkflow &&
						typed.WorkflowID == workflowID &&
						typed.NewDeadline != nil && typed.NewDeadline.Equal(newDeadline)
				}),
			).Return(nil)

		assert.Nil(client.ReviveWorkflow(utCtx, workflowID, &newDeadline, nil))
	})

	t.Run("revive without new deadline", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		workflowID := ulid.Make().String()

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, workflowID).
			Return(models.Workflow{ID: workflowID}, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		h.sender.EXPECT().
			EnqueueMessage(
				mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
					parsed := parseEnqueued(t, msg)
					typed, ok := parsed.(models.IPCMessageWorkflowRevive)
					return ok && typed.WorkflowID == workflowID && typed.NewDeadline == nil
				}),
			).Return(nil)

		assert.Nil(client.ReviveWorkflow(utCtx, workflowID, nil, nil))
	})

	t.Run("not-found short-circuits with no poke", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		workflowID := ulid.Make().String()

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, workflowID).
			Return(models.Workflow{}, fmt.Errorf("not found"))
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		// NOTE: no h.sender.EXPECT() - the poke must not happen when the read fails.

		err := client.ReviveWorkflow(utCtx, workflowID, nil, nil)
		assert.NotNil(err)
		var clientErr models.WorkflowClientError
		assert.True(errors.As(err, &clientErr), "expected WorkflowClientError, got %T", err)
	})
}

// TestClientCancelWorkflow covers the cancel path: confirm existence, then poke a Cancel event; a
// not-found short-circuits with no poke.
func TestClientCancelWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()

	t.Run("cancel pokes a CANCEL_WORKFLOW", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		workflowID := ulid.Make().String()

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, workflowID).
			Return(models.Workflow{ID: workflowID}, nil)
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))
		h.sender.EXPECT().
			EnqueueMessage(
				mock.Anything, mock.MatchedBy(func(msg goutilsRedis.QueueMessageEnvelope) bool {
					parsed := parseEnqueued(t, msg)
					typed, ok := parsed.(models.IPCMessageWorkflow)
					return ok &&
						typed.Type == models.IPCMsgTypeWFCancelWorkflow &&
						typed.WorkflowID == workflowID
				}),
			).Return(nil)

		assert.Nil(client.CancelWorkflow(utCtx, workflowID, nil))
	})

	t.Run("not-found short-circuits with no poke", func(t *testing.T) {
		assert := assert.New(t)

		client, h := newClientTestHarness(utCtx, t)
		workflowID := ulid.Make().String()

		mockDatabase := mockdb.NewDatabase(t)
		mockDatabase.EXPECT().
			GetWorkflow(mock.Anything, workflowID).
			Return(models.Workflow{}, fmt.Errorf("not found"))
		h.mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxForClient(mockDatabase))

		err := client.CancelWorkflow(utCtx, workflowID, nil)
		assert.NotNil(err)
		var clientErr models.WorkflowClientError
		assert.True(errors.As(err, &clientErr), "expected WorkflowClientError, got %T", err)
	})
}
