package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// newHelperTestScheduler build a white-box workflow schedulerImpl for driving the in-transaction
// helpers (settleWorkflowIfDone / timeOutWorkflowSteps) directly against a mock Database.
func newHelperTestScheduler(mockClient *mockdb.Client) *schedulerImpl {
	return &schedulerImpl{
		Component:   goutils.Component{LogTags: log.Fields{"module": "workflow"}},
		wg:          &sync.WaitGroup{},
		persistence: mockClient,
		ipcName:     "workflow-scheduler",
	}
}

// TestSettleWorkflowIfDone covers the unified settle helper: one case per branch of the
// current-workflow-state key, plus the two DB-error paths.
func TestSettleWorkflowIfDone(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	now := time.Now().UTC()
	simErr := fmt.Errorf("simulated failure")

	t.Run("RUNNING + all steps COMPLETE settles to COMPLETE", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{
				stepInState(workflow.ID, models.WorkflowStepStateComplete),
				stepInState(workflow.ID, models.WorkflowStepStateComplete),
			}, nil)
		mockDatabase.
			EXPECT().
			MarkWorkflowComplete(mock.Anything, workflow.ID, mock.Anything).
			Return(nil)

		settled, err := s.settleWorkflowIfDone(utCtx, mockDatabase, workflow, now)
		assert.Nil(err)
		assert.True(settled)
	})

	t.Run("RUNNING + a non-COMPLETE step does not settle", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{
				stepInState(workflow.ID, models.WorkflowStepStateComplete),
				stepInState(workflow.ID, models.WorkflowStepStateRunning),
			}, nil)
		// No MarkWorkflowComplete: not every step is COMPLETE.

		settled, err := s.settleWorkflowIfDone(utCtx, mockDatabase, workflow, now)
		assert.Nil(err)
		assert.False(settled)
	})

	t.Run("CANCELLING + no non-terminal step settles to CANCELLED", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateCancelling)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{
				stepInState(workflow.ID, models.WorkflowStepStateCancelled),
				stepInState(workflow.ID, models.WorkflowStepStateComplete),
			}, nil)
		mockDatabase.
			EXPECT().
			MarkWorkflowCancelled(mock.Anything, workflow.ID, mock.Anything).
			Return(nil)

		settled, err := s.settleWorkflowIfDone(utCtx, mockDatabase, workflow, now)
		assert.Nil(err)
		assert.True(settled)
	})

	t.Run("CANCELLING + a still-draining step does not settle", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateCancelling)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{
				stepInState(workflow.ID, models.WorkflowStepStateCancelled),
				stepInState(workflow.ID, models.WorkflowStepStateCancelling),
			}, nil)
		// No MarkWorkflowCancelled: a CANCELLING step is still draining.

		settled, err := s.settleWorkflowIfDone(utCtx, mockDatabase, workflow, now)
		assert.Nil(err)
		assert.False(settled)
	})

	t.Run("FAILED never satisfies all-COMPLETE, does not settle", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateFailed)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{
				stepInState(workflow.ID, models.WorkflowStepStateComplete),
				stepInState(workflow.ID, models.WorkflowStepStateFailed),
			}, nil)
		// No mark: a FAILED step defeats the completion predicate.

		settled, err := s.settleWorkflowIfDone(utCtx, mockDatabase, workflow, now)
		assert.Nil(err)
		assert.False(settled)
	})

	t.Run("non-settling current state (PENDING) is a no-op", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStatePending)
		// No ListWorkflowSteps, no mark: a PENDING workflow has no settle predicate.

		settled, err := s.settleWorkflowIfDone(utCtx, mockDatabase, workflow, now)
		assert.Nil(err)
		assert.False(settled)
	})

	t.Run("list steps failure is a PersistenceError", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		mockDatabase.EXPECT().ListWorkflowSteps(mock.Anything, workflow.ID).Return(nil, simErr)

		settled, err := s.settleWorkflowIfDone(utCtx, mockDatabase, workflow, now)
		assert.NotNil(err)
		assert.False(settled)
		var perr models.PersistenceError
		assert.ErrorAs(err, &perr)
	})

	t.Run("mark workflow complete failure is a PersistenceError", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{stepInState(workflow.ID, models.WorkflowStepStateComplete)}, nil)
		mockDatabase.
			EXPECT().
			MarkWorkflowComplete(mock.Anything, workflow.ID, mock.Anything).
			Return(simErr)

		settled, err := s.settleWorkflowIfDone(utCtx, mockDatabase, workflow, now)
		assert.NotNil(err)
		assert.False(settled)
		var perr models.PersistenceError
		assert.ErrorAs(err, &perr)
	})
}

// TestTimeOutWorkflowSteps covers the shared timeout helper: only non-terminal steps are flipped,
// only RUNNING steps' live task IDs are returned for cancellation, the workflow is flipped once,
// and an already-TIMED_OUT workflow is not re-flipped.
func TestTimeOutWorkflowSteps(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	now := time.Now().UTC()
	simErr := fmt.Errorf("simulated failure")

	t.Run(
		"flips non-terminal steps, returns running task IDs, flips workflow once",
		func(t *testing.T) {
			assert := assert.New(t)

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			s := newHelperTestScheduler(mockClient)

			workflow := workflowFixture(models.WorkflowStateRunning)
			running := stepInState(workflow.ID, models.WorkflowStepStateRunning)
			pending := stepInState(workflow.ID, models.WorkflowStepStatePending)
			defined := stepInState(workflow.ID, models.WorkflowStepStateDefined)
			done := stepInState(workflow.ID, models.WorkflowStepStateComplete)
			liveTask := taskInState(models.TaskStateActive)

			mockDatabase.EXPECT().
				ListWorkflowSteps(mock.Anything, workflow.ID).
				Return([]models.WorkflowStep{running, pending, defined, done}, nil)
			// Only the RUNNING step's live task is looked up.
			mockDatabase.EXPECT().
				GetWorkflowStepAndExecutorTask(mock.Anything, running.ID, true).
				Return(running, []models.Task{liveTask}, nil)
			// The three non-terminal steps (RUNNING/PENDING/DEFINED) are flipped; the COMPLETE one is not
			mockDatabase.EXPECT().
				MarkWorkflowStepTimedOut(
					mock.Anything, workflow.ID,
					mock.MatchedBy(func(ids []string) bool {
						if len(ids) != 3 {
							return false
						}
						want := map[string]bool{running.ID: true, pending.ID: true, defined.ID: true}
						for _, id := range ids {
							if !want[id] {
								return false
							}
						}
						return true
					}),
					mock.Anything,
				).
				Return(nil)
			mockDatabase.
				EXPECT().
				MarkWorkflowTimedOut(mock.Anything, workflow.ID, mock.Anything).
				Return(nil)

			cancelIDs, err := s.timeOutWorkflowSteps(utCtx, mockDatabase, workflow, now)
			assert.Nil(err)
			assert.Equal([]string{liveTask.ID}, cancelIDs)
		},
	)

	t.Run("already TIMED_OUT workflow is not re-flipped", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateTimedOut)
		// All steps already terminal: nothing to flip, no task to cancel.
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{
				stepInState(workflow.ID, models.WorkflowStepStateTimedOut),
			}, nil)
		// No MarkWorkflowStepTimedOut (no non-terminal steps), no MarkWorkflowTimedOut (already there).

		cancelIDs, err := s.timeOutWorkflowSteps(utCtx, mockDatabase, workflow, now)
		assert.Nil(err)
		assert.Empty(cancelIDs)
	})

	t.Run("list steps failure is a PersistenceError", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		s := newHelperTestScheduler(mockClient)

		workflow := workflowFixture(models.WorkflowStateRunning)
		mockDatabase.EXPECT().ListWorkflowSteps(mock.Anything, workflow.ID).Return(nil, simErr)

		_, err := s.timeOutWorkflowSteps(utCtx, mockDatabase, workflow, now)
		assert.NotNil(err)
		var perr models.PersistenceError
		assert.ErrorAs(err, &perr)
	})
}
