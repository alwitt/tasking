package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

/*
cancelWorkflow handle a Cancel Workflow event: the whole-workflow, hard-stop user cancellation (see
workflow/DESIGN.md "Cancellation").

In ONE transaction it marks the workflow CANCELLING, then per step: a RUNNING step goes CANCELLING
(and its live task is collected for a post-commit cancel request), and a DEFINED / PENDING / FAILED /
TIMED_OUT step - nothing in-flight to drain - goes straight to CANCELLED. COMPLETE / CANCELLED steps
are left as-is. If after the loop no step is left RUNNING / CANCELLING, the workflow settles straight
to CANCELLED (via settleWorkflowIfDone, whose CANCELLING branch is exactly the DESIGN's final
"no step in {RUNNING, CANCELLING}" predicate). Otherwise the still-draining steps flip the workflow
to CANCELLED later, as each drains via an Execution Update (cancellation wins over a late outcome) or
the maintenance sweep.

Idempotent hard stop: a workflow already CANCELLING / CANCELLED / COMPLETE is a benign NOOP - a
stale or re-delivered cancel writes nothing and the dispatch loop deletes it. A CANCELLING workflow
is driven to settle by Execution Updates + the sweep, not by re-running Cancel.

State-before-poke (DESIGN Invariant 8): all state changes commit in the transaction FIRST; the
task-cancel requests fire only AFTER the commit and are logged, not returned, on failure (a lost
cancel only leaves compute burning until the task's own deadline; the maintenance sweep reconciles).
Cancellation starts nothing new, so there is no Process Workflow poke.

	@param ctx context.Context - execution context
	@param workflowID string - the workflow to cancel
*/
func (s *schedulerImpl) cancelWorkflow(ctx context.Context, workflowID string) error {
	logTags := s.GetLogTagsForContext(ctx)
	now := time.Now().UTC()

	// Live tasks of the RUNNING steps, captured inside the transaction and cancelled post-commit.
	var cancelTaskIDs []string

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			workflowEntry, err := dbClient.GetWorkflow(dbCtx, workflowID)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to fetch workflow %s to cancel", workflowID), err, true,
				)
			}

			// Hard-stop / idempotency gate: a stale or re-delivered cancel on an already
			// cancelling/terminal workflow is a benign NOOP. (A CANCELLING workflow settles via
			// Execution Updates + the sweep, not by re-running Cancel.)
			switch workflowEntry.State {
			case models.WorkflowStateCancelling,
				models.WorkflowStateCancelled,
				models.WorkflowStateComplete:
				log.
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Debugf(
						"Ignoring cancel request for workflow %s in state '%s'",
						workflowEntry.ID, workflowEntry.State,
					)
				return nil
			}

			// Mark the workflow CANCELLING (any non-terminal state -> CANCELLING is legal).
			if err := dbClient.MarkWorkflowCancelling(dbCtx, workflowID, now); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to mark workflow %s cancelling", workflowID), err, true,
				)
			}

			// Partition the steps by current state. ValidNextState restricts RUNNING -> CANCELLING and
			// {DEFINED, PENDING, FAILED, TIMED_OUT} -> CANCELLED, so selecting them here is both the
			// intent and the guard; COMPLETE / CANCELLED / CANCELLING steps are left untouched.
			steps, err := dbClient.ListWorkflowSteps(dbCtx, workflowID)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to list steps of workflow %s to cancel", workflowID), err, true,
				)
			}
			cancellingStepIDs := []string{}
			cancelledStepIDs := []string{}
			for _, step := range steps {
				switch step.State {
				case models.WorkflowStepStateRunning:
					cancellingStepIDs = append(cancellingStepIDs, step.ID)
					// A RUNNING step has a live (non-terminal) task; collect it for cancellation so a
					// failed task is not retried.
					_, liveTasks, err := dbClient.GetWorkflowStepAndExecutorTask(dbCtx, step.ID, true)
					if err != nil {
						return models.NewPersistenceError(
							fmt.Sprintf("failed to fetch live task of running step %s to cancel", step.ID),
							err, true,
						)
					}
					for _, task := range liveTasks {
						cancelTaskIDs = append(cancelTaskIDs, task.ID)
					}
				case models.WorkflowStepStateDefined,
					models.WorkflowStepStatePending,
					models.WorkflowStepStateFailed,
					models.WorkflowStepStateTimedOut:
					cancelledStepIDs = append(cancelledStepIDs, step.ID)
				default:
					// COMPLETE / CANCELLED / CANCELLING: nothing to do.
					continue
				}
			}

			if len(cancellingStepIDs) > 0 {
				if err := dbClient.MarkWorkflowStepCancelling(
					dbCtx, workflowID, cancellingStepIDs, now,
				); err != nil {
					return models.NewPersistenceError(
						fmt.Sprintf("failed to mark cancelling steps of workflow %s", workflowID), err, true,
					)
				}
			}
			if len(cancelledStepIDs) > 0 {
				if err := dbClient.MarkWorkflowStepCancelled(
					dbCtx, workflowID, cancelledStepIDs, now,
				); err != nil {
					return models.NewPersistenceError(
						fmt.Sprintf("failed to mark cancelled steps of workflow %s", workflowID), err, true,
					)
				}
			}

			// Settle: with the workflow now CANCELLING, if no step is left RUNNING / CANCELLING it flips
			// straight to CANCELLED. settleWorkflowIfDone keys on the passed workflow state, so reflect
			// the CANCELLING transition just made.
			workflowEntry.State = models.WorkflowStateCancelling
			if _, err := s.settleWorkflowIfDone(dbCtx, dbClient, workflowEntry, now); err != nil {
				return err
			}

			return nil
		},
	); dbErr != nil {
		return models.NewWorkflowSchedulerError(
			fmt.Sprintf("failed to cancel workflow %s", workflowID), dbErr, true,
		)
	}

	// State-before-poke: the cancellation is committed. Request the task engine cancel each RUNNING
	// step's live task. A lost cancel only leaves compute burning until the task's own deadline - the
	// maintenance sweep reconciles - so each is logged, not returned.
	for _, taskID := range cancelTaskIDs {
		if err := s.taskClient.CancelTask(ctx, taskID, nil); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Failed to request cancel of task %s for cancelled workflow %s; "+
						"the task's own deadline / maintenance sweep will reconcile it",
					taskID, workflowID,
				)
		}
	}

	return nil
}
