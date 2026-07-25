package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
)

/*
timeOutWorkflowSteps time out a whole workflow: flip the workflow and every one of its non-terminal
steps to TIMED_OUT, and report which still-RUNNING steps' tasks must be cancelled.

All steps share the workflow deadline, so a blown deadline times out the whole workflow at once (see
workflow/DESIGN.md "TIMED_OUT is whole-workflow"). This is the shared enforcement path used by both
the Execution Update TIMED_OUT branch and the Schedule Workflow Step past-deadline check; the
maintenance sweep will reuse it too.

It runs inside the CALLER's transaction (the caller owns state-before-poke). The task-cancel side is
a poke that must not sit inside the transaction, so this helper does NOT issue it - it returns the
task IDs of the still-RUNNING steps and the caller cancels them (via the task client) AFTER the
transaction commits. A lost cancel only leaves compute burning until the task's own deadline; the
maintenance sweep reconciles.

Idempotent: a workflow already TIMED_OUT is not re-flipped, and only non-terminal steps
(DEFINED / PENDING / RUNNING) are flipped - terminal and CANCELLING steps are left alone, so no
illegal state transition is ever attempted.

	@param dbCtx context.Context - the caller's open transaction context
	@param dbClient db.Database - the caller's open database transaction
	@param workflowEntry models.Workflow - the workflow to time out
	@param now time.Time - when the timeout occurred
	@returns the task IDs of still-RUNNING steps whose tasks the caller must cancel post-commit
*/
func (s *schedulerImpl) timeOutWorkflowSteps(
	dbCtx context.Context, dbClient db.Database, workflowEntry models.Workflow, now time.Time,
) ([]string, error) {
	steps, err := dbClient.ListWorkflowSteps(dbCtx, workflowEntry.ID)
	if err != nil {
		return nil, models.NewPersistenceError(
			fmt.Sprintf("failed to list steps of workflow %s to time out", workflowEntry.ID), err, true,
		)
	}

	// Non-terminal steps get flipped to TIMED_OUT; among them the RUNNING ones also need their live
	// task cancelled so compute stops burning against a dead deadline.
	timeOutStepIDs := []string{}
	taskIDsToCancel := []string{}
	for _, step := range steps {
		switch step.State {
		case models.WorkflowStepStateDefined,
			models.WorkflowStepStatePending,
			models.WorkflowStepStateRunning:
			timeOutStepIDs = append(timeOutStepIDs, step.ID)
		default:
			// COMPLETE / FAILED / TIMED_OUT / CANCELLING / CANCELLED: nothing to time out.
			continue
		}

		if step.State != models.WorkflowStepStateRunning {
			// For steps which are only DEFINED or PENDING, no support task has been launched yet.
			continue
		}

		// A RUNNING step has a live (non-terminal) task; collect it for cancellation.
		_, liveTasks, err := dbClient.GetWorkflowStepAndExecutorTask(dbCtx, step.ID, true)
		if err != nil {
			return nil, models.NewPersistenceError(
				fmt.Sprintf("failed to fetch live task of running step %s to cancel", step.ID),
				err, true,
			)
		}
		for _, task := range liveTasks {
			taskIDsToCancel = append(taskIDsToCancel, task.ID)
		}
	}

	if len(timeOutStepIDs) > 0 {
		if err := dbClient.MarkWorkflowStepTimedOut(
			dbCtx, workflowEntry.ID, timeOutStepIDs, now,
		); err != nil {
			return nil, models.NewPersistenceError(
				fmt.Sprintf("failed to time out steps of workflow %s", workflowEntry.ID), err, true,
			)
		}
	}

	// Flip the workflow itself - but only if it is not already TIMED_OUT (a re-flip is an illegal
	// transition). RUNNING / FAILED -> TIMED_OUT are the legal entries.
	if workflowEntry.State != models.WorkflowStateTimedOut {
		if err := dbClient.MarkWorkflowTimedOut(dbCtx, workflowEntry.ID, now); err != nil {
			return nil, models.NewPersistenceError(
				fmt.Sprintf("failed to mark workflow %s timed out", workflowEntry.ID), err, true,
			)
		}
	}

	return taskIDsToCancel, nil
}
