package workflow

import (
	"context"
	"fmt"
	"testing"

	goutils "github.com/alwitt/goutils"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktask "github.com/alwitt/tasking/mocks/task"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gorm.io/gorm"
)

// TestApplyStepTaskUpdate covers the notify fast-path middle hop: resolve task -> step then delegate
// to the execution-update reducer, a benign drop when no step is linked to the task, and a fatal
// error on a non-not-found lookup failure.
func TestApplyStepTaskUpdate(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	t.Run("resolves task -> step and delegates to the reducer", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		// Both the lookup transaction (this handler) and the reducer transaction run against the
		// same mock Database.
		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))

		workflow := workflowFixture(models.WorkflowStateRunning)
		step := stepInState(workflow.ID, models.WorkflowStepStateRunning)
		taskID := ulid.Make().String()

		// Hop 1: task -> step.
		mockDatabase.EXPECT().
			GetWorkflowStepProcessedByTask(mock.Anything, taskID).
			Return(step, nil)

		// Hop 2: the reducer applies COMPLETE. With every step COMPLETE the workflow settles, so the
		// visible effect is MarkWorkflowStepComplete + MarkWorkflowComplete (no fan-out poke).
		mockDatabase.EXPECT().GetWorkflowStep(mock.Anything, step.ID).Return(step, nil)
		mockDatabase.EXPECT().GetWorkflow(mock.Anything, workflow.ID).Return(workflow, nil)
		mockDatabase.EXPECT().
			MarkWorkflowStepComplete(mock.Anything, workflow.ID, []string{step.ID}, mock.Anything).
			Return(nil)
		mockDatabase.EXPECT().
			ListWorkflowSteps(mock.Anything, workflow.ID).
			Return([]models.WorkflowStep{{ID: step.ID, State: models.WorkflowStepStateComplete}}, nil)
		mockDatabase.EXPECT().
			MarkWorkflowComplete(mock.Anything, workflow.ID, mock.Anything).
			Return(nil)

		err := s.applyStepTaskUpdate(utCtx, taskID, models.WorkflowStepStateComplete)
		assert.Nil(err)
	})

	t.Run("no linked step is a benign drop", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))

		taskID := ulid.Make().String()
		// Mirror how the DB layer surfaces a missing row: a goutils NotFoundError wrapping
		// gorm.ErrRecordNotFound.
		notFound := goutils.NewNotFoundError(
			fmt.Sprintf("workflow step for task '%s' does not exist", taskID),
			gorm.ErrRecordNotFound, true,
		)
		mockDatabase.EXPECT().
			GetWorkflowStepProcessedByTask(mock.Anything, taskID).
			Return(models.WorkflowStep{}, notFound)

		// No reducer calls: the drop returns nil before any write.
		err := s.applyStepTaskUpdate(utCtx, taskID, models.WorkflowStepStateComplete)
		assert.Nil(err)
	})

	t.Run("lookup failure is fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockdb.NewClient(t)
		mockDatabase := mockdb.NewDatabase(t)
		mockSender := mockcommon.NewIPCMessageSend(t)
		mockTasks := mocktask.NewClient(t)
		s := newStepExecUpdateTestScheduler(mockClient, mockSender, mockTasks)

		mockClient.EXPECT().
			UseDatabaseInTransaction(mock.Anything, mock.Anything).
			RunAndReturn(runTxAgainst(mockDatabase))

		taskID := ulid.Make().String()
		mockDatabase.EXPECT().
			GetWorkflowStepProcessedByTask(mock.Anything, taskID).
			Return(models.WorkflowStep{}, fmt.Errorf("simulated failure"))

		err := s.applyStepTaskUpdate(utCtx, taskID, models.WorkflowStepStateComplete)
		assert.NotNil(err)
		assertWorkflowSchedulerError(t, err)
	})
}
