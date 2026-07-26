package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// newReviveTestScheduler build a white-box workflow schedulerImpl for driving reviveWorkflow: the
// persistence client plus the queue sender used for the post-commit Process Workflow poke.
func newReviveTestScheduler(
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

// reviveStepFixture a workflow step of the given state belonging to workflowID.
func reviveStepFixture(workflowID string, state models.WorkflowStepStateENUM) models.WorkflowStep {
	return models.WorkflowStep{
		ID:         ulid.Make().String(),
		Name:       "unit-test-step-" + ulid.Make().String(),
		WorkflowID: workflowID,
		Creator:    "unit-test-creator",
		Type:       "unit-test-step-type",
		State:      state,
	}
}

// TestReviveWorkflow covers the Revive Failed Workflow handler: the FAILED / TIMED_OUT revert with
// and without a new deadline, the deadline re-sync ordering, each precondition drop, the persistence
// failure path, and the non-fatal post-commit emit failure (state-before-poke).
func TestReviveWorkflow(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()

	t.Run("FAILED workflow, no new deadline: revert + running + one poke", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newReviveTestScheduler(mockClient, mockSender)

		wf := workflowFixture(models.WorkflowStateFailed)
		failedStep := reviveStepFixture(wf.ID, models.WorkflowStepStateFailed)
		completeStep := reviveStepFixture(wf.ID, models.WorkflowStepStateComplete)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().MarkWorkflowRunning(mock.Anything, wf.ID, mock.Anything).Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{failedStep, completeStep}, nil)
		// Only the FAILED step is reverted; the COMPLETE one is left alone.
		mockDatabase.EXPECT().
			MarkWorkflowStepDefined(mock.Anything, wf.ID, []string{failedStep.ID}, mock.Anything).
			Return(nil)
		// No new deadline supplied -> UpdateWorkflowDeadline is NOT called.

		// Post-commit: exactly one Process Workflow poke for the revived workflow.
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.MatchedBy(func(m goutilsRedis.QueueMessageEnvelope) bool {
				typed, ok := m.(models.IPCMessageWorkflow)
				return ok &&
					typed.Type == models.IPCMsgTypeWFProcessWorkflow &&
					typed.WorkflowID == wf.ID
			})).
			Return(nil)

		revived, dropReason, err := s.reviveWorkflow(utCtx, wf.ID, nil)
		assert.NoError(err)
		assert.True(revived)
		assert.Empty(dropReason)
	})

	t.Run("TIMED_OUT workflow, future deadline: deadline re-synced after revert", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newReviveTestScheduler(mockClient, mockSender)

		wf := workflowFixture(models.WorkflowStateTimedOut)
		timedOutStep := reviveStepFixture(wf.ID, models.WorkflowStepStateTimedOut)
		newDeadline := time.Now().UTC().Add(2 * time.Hour)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)

		// Assert ordering: revert to DEFINED must precede the deadline re-sync, so the now-DEFINED
		// (non-terminal) step is included in the deadline update.
		var revertCalled bool
		mockDatabase.EXPECT().MarkWorkflowRunning(mock.Anything, wf.ID, mock.Anything).Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{timedOutStep}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepDefined(mock.Anything, wf.ID, []string{timedOutStep.ID}, mock.Anything).
			Run(func(context.Context, string, []string, time.Time) { revertCalled = true }).
			Return(nil)
		mockDatabase.EXPECT().
			UpdateWorkflowDeadline(mock.Anything, wf.ID, newDeadline).
			Run(func(context.Context, string, time.Time) {
				assert.True(revertCalled, "deadline update must run after the step revert")
			}).
			Return(nil)

		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Return(nil)

		revived, dropReason, err := s.reviveWorkflow(utCtx, wf.ID, &newDeadline)
		assert.NoError(err)
		assert.True(revived)
		assert.Empty(dropReason)
	})

	t.Run(
		"FAILED workflow, future deadline: deadline applied (optional-but-honored)",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			mockSender := mockcommon.NewIPCMessageSend(t)
			s := newReviveTestScheduler(mockClient, mockSender)

			wf := workflowFixture(models.WorkflowStateFailed)
			failedStep := reviveStepFixture(wf.ID, models.WorkflowStepStateFailed)
			newDeadline := time.Now().UTC().Add(time.Hour)

			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
			mockDatabase.EXPECT().MarkWorkflowRunning(mock.Anything, wf.ID, mock.Anything).Return(nil)
			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, wf.ID).
				Return([]models.WorkflowStep{failedStep}, nil)
			mockDatabase.EXPECT().
				MarkWorkflowStepDefined(mock.Anything, wf.ID, []string{failedStep.ID}, mock.Anything).
				Return(nil)
			mockDatabase.EXPECT().
				UpdateWorkflowDeadline(mock.Anything, wf.ID, newDeadline).
				Return(nil)
			mockSender.EXPECT().EnqueueMessage(mock.Anything, mock.Anything).Return(nil)

			revived, dropReason, err := s.reviveWorkflow(utCtx, wf.ID, &newDeadline)
			assert.NoError(err)
			assert.True(revived)
			assert.Empty(dropReason)
		},
	)

	// Precondition drops: no writes, no emit, revived=false with a reason and no error.
	dropCases := []struct {
		name        string
		state       models.WorkflowStateENUM
		newDeadline *time.Time
	}{
		{name: "wrong state RUNNING", state: models.WorkflowStateRunning, newDeadline: nil},
		{name: "wrong state COMPLETE", state: models.WorkflowStateComplete, newDeadline: nil},
		{name: "TIMED_OUT without deadline", state: models.WorkflowStateTimedOut, newDeadline: nil},
	}
	for _, tc := range dropCases {
		t.Run(tc.name+" is dropped, not applied", func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			// No sender: a poke would fail the test (nothing is emitted on a drop).
			s := newReviveTestScheduler(mockClient, mockcommon.NewIPCMessageSend(t))

			wf := workflowFixture(tc.state)
			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(runTxAgainst(mockDatabase))
			mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
			// No mark/update/list expectations: the precondition check returns before any write.

			revived, dropReason, err := s.reviveWorkflow(utCtx, wf.ID, tc.newDeadline)
			assert.NoError(err)
			assert.False(revived)
			assert.NotEmpty(dropReason)
		})
	}

	t.Run("TIMED_OUT with a past deadline is dropped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newReviveTestScheduler(mockClient, mockcommon.NewIPCMessageSend(t))

		wf := workflowFixture(models.WorkflowStateTimedOut)
		pastDeadline := time.Now().UTC().Add(-time.Hour)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)

		revived, dropReason, err := s.reviveWorkflow(utCtx, wf.ID, &pastDeadline)
		assert.NoError(err)
		assert.False(revived)
		assert.NotEmpty(dropReason)
	})

	t.Run("DB error on a mark is fatal, no emit", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newReviveTestScheduler(mockClient, mockcommon.NewIPCMessageSend(t))

		wf := workflowFixture(models.WorkflowStateFailed)
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().
			MarkWorkflowRunning(mock.Anything, wf.ID, mock.Anything).
			Return(fmt.Errorf("simulated failure"))

		revived, dropReason, err := s.reviveWorkflow(utCtx, wf.ID, nil)
		assert.Error(err)
		assertWorkflowSchedulerError(t, err)
		assert.False(revived)
		assert.Empty(dropReason)
	})

	t.Run("post-commit emit failure is non-fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newReviveTestScheduler(mockClient, mockSender)

		wf := workflowFixture(models.WorkflowStateFailed)
		failedStep := reviveStepFixture(wf.ID, models.WorkflowStepStateFailed)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, wf.ID).Return(wf, nil)
		mockDatabase.EXPECT().MarkWorkflowRunning(mock.Anything, wf.ID, mock.Anything).Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, wf.ID).
			Return([]models.WorkflowStep{failedStep}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepDefined(mock.Anything, wf.ID, []string{failedStep.ID}, mock.Anything).
			Return(nil)
		// The state is committed; a lost poke is not fatal.
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Return(fmt.Errorf("simulated enqueue failure"))

		revived, dropReason, err := s.reviveWorkflow(utCtx, wf.ID, nil)
		assert.NoError(err)
		assert.True(revived)
		assert.Empty(dropReason)
	})
}
