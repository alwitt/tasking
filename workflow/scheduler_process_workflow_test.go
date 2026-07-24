package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// newProcessWorkflowTestScheduler build a white-box workflow schedulerImpl for driving
// processWorkflow directly: the persistence client plus the queue sender used for the Schedule
// Workflow Step fan-out pokes.
func newProcessWorkflowTestScheduler(
	mockClient *mockdb.Client, ipcSender *mockcommon.IPCMessageSend,
) *schedulerImpl {
	return &schedulerImpl{
		Component:   goutils.Component{LogTags: log.Fields{"module": "workflow"}},
		wg:          &sync.WaitGroup{},
		persistence: mockClient,
		ipcName:     "workflow-scheduler",
		ipcSender:   ipcSender,
	}
}

// workflowFixture a workflow of the given state for the process-workflow tests.
func workflowFixture(state models.WorkflowStateENUM) models.Workflow {
	return models.Workflow{
		ID:       ulid.Make().String(),
		Name:     "unit-test-workflow",
		Creator:  "unit-test-creator",
		State:    state,
		Deadline: time.Now().UTC().Add(time.Hour),
	}
}

// stepFixture a DEFINED workflow step belonging to the given workflow.
func stepFixture(workflowID string) models.WorkflowStep {
	return models.WorkflowStep{
		ID:         ulid.Make().String(),
		Name:       "unit-test-step-" + ulid.Make().String(),
		WorkflowID: workflowID,
		Creator:    "unit-test-creator",
		Type:       "unit-test-step-type",
		State:      models.WorkflowStepStateDefined,
	}
}

// TestProcessWorkflow covers the Process Workflow handler: the hard-stop NOOP gate, the
// PENDING -> RUNNING transition on first processing, startable-step fan-out, each persistence
// failure path, and the non-fatal emit-failure behavior (state-before-poke).
func TestProcessWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	// A hard-stop state must NOOP: fetch the workflow, then do nothing else. Mock strictness
	// asserts no MarkWorkflowRunning / ListWorkflowStepsReadyToRun / EnqueueMessage occurs.
	for _, state := range []models.WorkflowStateENUM{
		models.WorkflowStateTimedOut,
		models.WorkflowStateCancelling,
		models.WorkflowStateCancelled,
		models.WorkflowStateComplete,
	} {
		t.Run(fmt.Sprintf("hard-stop state %s is a no-op", state), func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			s := newProcessWorkflowTestScheduler(mockClient, mockSender)

			workflow := workflowFixture(state)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)

			assert.Nil(s.processWorkflow(utCtx, workflow.ID))
		})
	}

	t.Run("fetch workflow fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflowID := ulid.Make().String()

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflowID).Return(models.Workflow{}, simErr)

		err := s.processWorkflow(utCtx, workflowID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("PENDING workflow starts and fans out startable steps", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflow := workflowFixture(models.WorkflowStatePending)
		step1 := stepFixture(workflow.ID)
		step2 := stepFixture(workflow.ID)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowRunning(mock.Anything, workflow.ID, mock.Anything).
			Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowStepsReadyToRun(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{step1, step2}, nil)
		// The two startable step IDs are marked PENDING together, scoped to the workflow.
		mockDatabase.EXPECT().
			MarkWorkflowStepPending(
				mock.Anything, workflow.ID,
				mock.MatchedBy(func(ids []string) bool {
					return len(ids) == 2 &&
						((ids[0] == step1.ID && ids[1] == step2.ID) ||
							(ids[0] == step2.ID && ids[1] == step1.ID))
				}),
				mock.Anything,
			).
			Return(nil)
		// One Schedule Workflow Step poke per startable step (after the transaction commits).
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil).Twice()

		assert.Nil(s.processWorkflow(utCtx, workflow.ID))
	})

	t.Run("RUNNING workflow fans out without re-marking running", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepFixture(workflow.ID)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		// No MarkWorkflowRunning: already RUNNING. Mock strictness asserts it is not called.
		mockDatabase.EXPECT().
			ListWorkflowStepsReadyToRun(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{step}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepPending(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil).Once()

		assert.Nil(s.processWorkflow(utCtx, workflow.ID))
	})

	t.Run("FAILED workflow soft-stops and still fans out", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflow := workflowFixture(models.WorkflowStateFailed)
		step := stepFixture(workflow.ID)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		// No MarkWorkflowRunning: FAILED is a soft stop, not a re-start.
		mockDatabase.EXPECT().
			ListWorkflowStepsReadyToRun(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{step}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepPending(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil).Once()

		assert.Nil(s.processWorkflow(utCtx, workflow.ID))
	})

	t.Run("no startable steps does not mark or emit", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflow := workflowFixture(models.WorkflowStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			ListWorkflowStepsReadyToRun(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{}, nil)
		// No MarkWorkflowStepPending, no EnqueueMessage: nothing is startable.

		assert.Nil(s.processWorkflow(utCtx, workflow.ID))
	})

	t.Run("mark workflow running fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflow := workflowFixture(models.WorkflowStatePending)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowRunning(mock.Anything, workflow.ID, mock.Anything).
			Return(simErr)

		err := s.processWorkflow(utCtx, workflow.ID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("list startable steps fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflow := workflowFixture(models.WorkflowStateRunning)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			ListWorkflowStepsReadyToRun(mock.Anything, workflow.ID).
			Return(nil, simErr)

		err := s.processWorkflow(utCtx, workflow.ID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("mark steps pending fails", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepFixture(workflow.ID)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			ListWorkflowStepsReadyToRun(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{step}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepPending(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(simErr)

		err := s.processWorkflow(utCtx, workflow.ID)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})

	t.Run("emit failure is non-fatal (state-before-poke)", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newProcessWorkflowTestScheduler(mockClient, mockSender)

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepFixture(workflow.ID)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			ListWorkflowStepsReadyToRun(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{step}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepPending(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		// The poke fails, but the work is already committed: processWorkflow must still return nil.
		mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(simErr).Once()

		assert.Nil(s.processWorkflow(utCtx, workflow.ID))
	})
}
