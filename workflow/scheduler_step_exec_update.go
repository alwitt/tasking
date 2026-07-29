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

// isTerminalWorkflowStepState report whether a workflow step state is terminal for settle/dispatch
// purposes: COMPLETE, FAILED, TIMED_OUT, and CANCELLED. (FAILED / TIMED_OUT are "terminal" here in
// the sense that they have nothing in-flight to drain and take no further automatic transition -
// only a user revive moves them - see workflow/DESIGN.md "Terminal vs. non-terminal".)
func isTerminalWorkflowStepState(state models.WorkflowStepStateENUM) bool {
	switch state {
	case models.WorkflowStepStateComplete,
		models.WorkflowStepStateFailed,
		models.WorkflowStepStateTimedOut,
		models.WorkflowStepStateCancelled:
		return true
	default:
		return false
	}
}

/*
applyStepExecutionUpdate handle a Workflow Step Execution Update event: apply a step's
already-resolved terminal outcome and advance the DAG.

This is the inbound reducer keyed by [step ID, new step state] (see workflow/DESIGN.md "Workflow
Step Execution Update"). Its two producers - the notify callback adapter and the maintenance sweep
(both later slices) - resolve the source task event to a step and derive the new step state before
enqueueing, so the reducer never sees a task ID or a raw event type. newStepState is always one of
the terminal outcomes COMPLETE / FAILED / TIMED_OUT / CANCELLED.

The reducer is idempotent / re-entrant (DESIGN Invariant 2): it guards before every mark rather than
relying on the DB to no-op an illegal transition (it does not - updateWorkflowStepState returns a
ConsistencyError). A double-delivered broadcast or a replayed message therefore lands on a benign
no-op:

  - Workflow already CANCELLING / CANCELLED -> cancellation wins over the reported outcome: mark the
    step CANCELLED (unless already terminal) and settle the workflow if this drained the last step.
  - Step already terminal -> nothing to do.

Otherwise it applies the outcome: COMPLETE advances the step and either completes the workflow
(every step COMPLETE) or fans out newly-unblocked steps via Process Workflow; FAILED fails the step
and the workflow (soft stop - healthy tracks keep running); TIMED_OUT times out the whole workflow
via the shared timeOutWorkflowSteps helper; CANCELLED marks the step and settles the workflow if
applicable.

State-before-poke (DESIGN Invariant 8): every state change commits in one transaction FIRST; the
follow-on Process Workflow emit and any task-cancel pokes fire only AFTER the commit and are logged,
not returned, on failure (the maintenance sweep re-derives them from persisted state).

	@param ctx context.Context - execution context
	@param stepID string - the workflow step whose outcome to apply
	@param newStepState models.WorkflowStepStateENUM - the resolved terminal step state
*/
func (s *schedulerImpl) applyStepExecutionUpdate(
	ctx context.Context, stepID string, newStepState models.WorkflowStepStateENUM,
) error {
	logTags := s.GetLogTagsForContext(ctx)
	now := time.Now().UTC()

	// The reducer only ever applies a terminal outcome. The IPC validator accepts any step state, so
	// a non-terminal value here is a producer bug - reject it as fatal.
	if !isTerminalWorkflowStepState(newStepState) {
		return models.NewWorkflowSchedulerError(
			fmt.Sprintf(
				"execution update for step %s carries non-terminal state '%s'", stepID, newStepState,
			), nil, true,
		)
	}

	// Post-commit intent captured inside the transaction (state-before-poke): the workflow to fan
	// out (empty means none), and the task IDs to cancel (a TIMED_OUT outcome).
	var emitProcessWorkflowFor string
	var cancelTaskIDs []string

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			step, err := dbClient.GetWorkflowStep(dbCtx, stepID)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to fetch workflow step %s", stepID), err, true,
				)
			}

			workflowEntry, err := dbClient.GetWorkflow(dbCtx, step.WorkflowID)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to fetch workflow %s of step %s", step.WorkflowID, stepID),
					err, true,
				)
			}

			// Cancellation wins over the reported outcome: once the workflow is cancelling/cancelled,
			// a step's terminal event marks it CANCELLED regardless of what the task actually
			// reported (a task cancelled by the workflow also emits its own terminal event).
			if workflowEntry.State == models.WorkflowStateCancelling ||
				workflowEntry.State == models.WorkflowStateCancelled {
				if !isTerminalWorkflowStepState(step.State) {
					if err := dbClient.MarkWorkflowStepCancelled(
						dbCtx, workflowEntry.ID, []string{stepID}, now,
					); err != nil {
						return goutils.NewPersistenceError(
							fmt.Sprintf("failed to mark cancelled step %s", stepID), err, true,
						)
					}
				}
				if _, err := s.settleWorkflowIfDone(dbCtx, dbClient, workflowEntry, now); err != nil {
					return err
				}
				return nil
			}

			// Idempotency guard: a step already terminal has nothing to do. This is the benign no-op
			// for a double-delivered broadcast or a replayed message; ValidNextState would reject the
			// transition anyway.
			if isTerminalWorkflowStepState(step.State) {
				log.
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Debugf(
						"Ignoring execution update for step %s: already terminal (state '%s')",
						stepID, step.State,
					)
				return nil
			}

			switch newStepState {
			case models.WorkflowStepStateComplete:
				if err := dbClient.MarkWorkflowStepComplete(
					dbCtx, workflowEntry.ID, []string{stepID}, now,
				); err != nil {
					return goutils.NewPersistenceError(
						fmt.Sprintf("failed to mark step %s complete", stepID), err, true,
					)
				}
				settled, err := s.settleWorkflowIfDone(dbCtx, dbClient, workflowEntry, now)
				if err != nil {
					return err
				}
				// Not every step is COMPLETE yet: fan out any steps this completion just unblocked.
				// Fan-out is delegated to Process Workflow, not inlined.
				if !settled {
					emitProcessWorkflowFor = workflowEntry.ID
				}

			case models.WorkflowStepStateFailed:
				if err := dbClient.MarkWorkflowStepFailed(
					dbCtx, workflowEntry.ID, []string{stepID}, now,
				); err != nil {
					return goutils.NewPersistenceError(
						fmt.Sprintf("failed to mark step %s failed", stepID), err, true,
					)
				}
				// Fail the workflow (soft stop - healthy parallel tracks keep advancing). Only a
				// RUNNING workflow can enter FAILED; a workflow already FAILED stays FAILED (a
				// re-flip is an illegal transition).
				if workflowEntry.State == models.WorkflowStateRunning {
					if err := dbClient.MarkWorkflowFailed(dbCtx, workflowEntry.ID, now); err != nil {
						return goutils.NewPersistenceError(
							fmt.Sprintf("failed to mark workflow %s failed", workflowEntry.ID), err, true,
						)
					}
				}

			case models.WorkflowStepStateTimedOut:
				// A blown deadline times out the whole workflow at once (all steps share the workflow
				// deadline). The shared helper flips the workflow + all non-terminal steps and reports
				// the still-RUNNING steps' tasks to cancel after commit.
				cancelTaskIDs, err = s.timeOutWorkflowSteps(dbCtx, dbClient, workflowEntry, now)
				if err != nil {
					return err
				}

			case models.WorkflowStepStateCancelled:
				if err := dbClient.MarkWorkflowStepCancelled(
					dbCtx, workflowEntry.ID, []string{stepID}, now,
				); err != nil {
					return goutils.NewPersistenceError(
						fmt.Sprintf("failed to mark step %s cancelled", stepID), err, true,
					)
				}
				if _, err := s.settleWorkflowIfDone(dbCtx, dbClient, workflowEntry, now); err != nil {
					return err
				}
			}

			return nil
		},
	); dbErr != nil {
		return models.NewWorkflowSchedulerError(
			fmt.Sprintf("failed to apply execution update for step %s", stepID), dbErr, true,
		)
	}

	// State-before-poke: the driving state is committed above. Emit the follow-on Process Workflow
	// fan-out poke, then the task-cancel pokes. A failed poke loses only the poke - the maintenance
	// sweep re-derives it from persisted state - so each is logged, not returned.
	if emitProcessWorkflowFor != "" {
		if err := s.ipcSender.EnqueueMessage(
			ctx, models.PrepareIPCMsgWFProcessWorkflow(s.ipcName, emitProcessWorkflowFor, now),
		); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Failed to enqueue process-workflow fan-out poke for workflow %s after step %s "+
						"completed; maintenance sweep will re-drive it",
					emitProcessWorkflowFor, stepID,
				)
		}
	}
	for _, taskID := range cancelTaskIDs {
		if err := s.taskClient.CancelTask(ctx, taskID, nil); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Failed to request cancel of task %s for timed-out step of workflow; "+
						"the task's own deadline / maintenance sweep will reconcile it",
					taskID,
				)
		}
	}

	return nil
}

/*
settleWorkflowIfDone re-evaluate the workflow aggregate against its steps and advance it to a
terminal state if the current state's predicate holds. Keyed on the CURRENT workflow state:

  - RUNNING (or, harmlessly, FAILED) -> COMPLETE, iff every step is COMPLETE.
  - CANCELLING -> CANCELLED, iff no step is in a non-terminal state
    (DEFINED / PENDING / RUNNING / CANCELLING).
  - any other state -> no-op.

It re-lists the steps so that a step just written in the caller's transaction is included in the
aggregate (the current step's state is written before this check, so it participates). It returns
whether it settled the workflow, so a caller can decide whether a follow-on poke is still needed
(the Execution Update COMPLETE branch emits Process Workflow only when this did NOT settle).

This is the shared "advance to terminal state if done" reconciliation the maintenance sweep will
reuse for its post-per-step workflow-aggregate re-check. (The third sweep aggregate case,
past-deadline -> TIMED_OUT, is not a settle - TIMED_OUT is non-terminal and needs the task-cancel
side effect - so it stays the separate timeOutWorkflowSteps helper.)

	@param dbCtx context.Context - the caller's open transaction context
	@param dbClient db.Database - the caller's open database transaction
	@param workflowEntry models.Workflow - the workflow to reconcile
	@param now time.Time - when the state change occurred
	@returns whether the workflow was advanced to a terminal state
*/
func (s *schedulerImpl) settleWorkflowIfDone(
	dbCtx context.Context, dbClient db.Database, workflowEntry models.Workflow, now time.Time,
) (bool, error) {
	// Only RUNNING (completion) and CANCELLING (cancellation) have a settle predicate. A FAILED
	// workflow is deliberately excluded: it always retains a non-COMPLETE step (the failure that
	// made it FAILED), so it can never satisfy the completion predicate, and FAILED -> COMPLETE is
	// not a legal transition anyway (ValidNextState would reject it). Excluding it here makes "only
	// a RUNNING workflow completes" structural rather than resting on that upstream invariant.
	if workflowEntry.State != models.WorkflowStateRunning &&
		workflowEntry.State != models.WorkflowStateCancelling {
		return false, nil
	}

	steps, err := dbClient.ListWorkflowSteps(dbCtx, workflowEntry.ID)
	if err != nil {
		return false, goutils.NewPersistenceError(
			fmt.Sprintf("failed to list steps of workflow %s to settle", workflowEntry.ID), err, true,
		)
	}

	switch workflowEntry.State {
	case models.WorkflowStateRunning:
		// COMPLETE requires every step COMPLETE. A FAILED / TIMED_OUT step defeats this predicate, so
		// a workflow with such a step never satisfies it (and such a workflow is FAILED, already
		// excluded by the guard above).
		for _, step := range steps {
			if step.State != models.WorkflowStepStateComplete {
				return false, nil
			}
		}
		if err := dbClient.MarkWorkflowComplete(dbCtx, workflowEntry.ID, now); err != nil {
			return false, goutils.NewPersistenceError(
				fmt.Sprintf("failed to mark workflow %s complete", workflowEntry.ID), err, true,
			)
		}
		return true, nil

	case models.WorkflowStateCancelling:
		// Settled: no step is still draining (RUNNING / CANCELLING) or unstarted (DEFINED / PENDING).
		for _, step := range steps {
			switch step.State {
			case models.WorkflowStepStateDefined,
				models.WorkflowStepStatePending,
				models.WorkflowStepStateRunning,
				models.WorkflowStepStateCancelling:
				return false, nil
			}
		}
		if err := dbClient.MarkWorkflowCancelled(dbCtx, workflowEntry.ID, now); err != nil {
			return false, goutils.NewPersistenceError(
				fmt.Sprintf("failed to mark workflow %s cancelled", workflowEntry.ID), err, true,
			)
		}
		return true, nil
	}

	return false, nil
}
