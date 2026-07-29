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
reviveWorkflow handle a Revive Failed Workflow event: the single user recovery action for a FAILED
or TIMED_OUT workflow (see workflow/DESIGN.md "Revive Failed Workflow").

In ONE transaction it flips the workflow back to RUNNING, reverts every FAILED / TIMED_OUT step to
DEFINED (flagging user_restarted), and - if a new deadline is supplied - re-syncs that deadline onto
the workflow and its now-DEFINED steps. Then a single post-commit Process Workflow poke re-runs the
reverted subtree through the normal first-run DEFINED startability path (revive deliberately reuses
that path rather than a special "re-dispatch a PENDING step" case).

newDeadline policy: optional for a FAILED workflow (whose deadline has not necessarily passed),
REQUIRED and in the future for a TIMED_OUT workflow (otherwise the revived steps would immediately
re-time-out). A non-nil but already-passed newDeadline is rejected in either state.

Precondition failures (wrong workflow state, or a missing/past newDeadline) are CLIENT errors that
will never become valid on replay - a workflow's state only ever moves further from
FAILED/TIMED_OUT. So they are NOT returned as errors (which would wedge the queue on replay);
instead the handler writes nothing and returns (revived=false, dropReason=<why>, err=nil), letting
the caller record the message as invalid and delete it. A genuine DB fault returns (false, "", err)
and is fatal (the message is left buffered for startup replay).

State-before-poke (DESIGN Invariant 8): all state changes commit in the transaction FIRST; the
follow-on Process Workflow emit fires only AFTER the commit and is logged, not returned, on failure
(the maintenance sweep re-derives it from the now-DEFINED persisted state).

	@param ctx context.Context - execution context
	@param workflowID string - the workflow to revive
	@param newDeadline *time.Time - optional (FAILED) / required-future (TIMED_OUT) new deadline
	@returns revived whether the revive was applied; dropReason why it was dropped (when !revived and
		err==nil); and a fatal error on a genuine DB fault
*/
func (s *schedulerImpl) reviveWorkflow(
	ctx context.Context, workflowID string, newDeadline *time.Time,
) (revived bool, dropReason string, err error) {
	logTags := s.GetLogTagsForContext(ctx)
	now := time.Now().UTC()

	// Precondition failure captured inside the transaction: non-empty means "drop, do not replay".
	// When set, the closure writes nothing and returns nil.
	var preCondFailure string

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			workflowEntry, err := dbClient.GetWorkflow(dbCtx, workflowID)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to fetch workflow %s to revive", workflowID), err, true,
				)
			}

			// Only a FAILED or TIMED_OUT workflow may be revived.
			if workflowEntry.State != models.WorkflowStateFailed &&
				workflowEntry.State != models.WorkflowStateTimedOut {
				preCondFailure = fmt.Sprintf(
					"cannot revive workflow %s in state '%s' (only FAILED / TIMED_OUT are revivable)",
					workflowID, workflowEntry.State,
				)
				return nil
			}

			// A TIMED_OUT revive MUST carry a future deadline; otherwise the revived steps re-time-out
			// immediately. A FAILED revive may omit it, but a supplied deadline must still be future.
			if workflowEntry.State == models.WorkflowStateTimedOut &&
				(newDeadline == nil || !newDeadline.After(now)) {
				preCondFailure = fmt.Sprintf(
					"cannot revive TIMED_OUT workflow %s without a future new deadline", workflowID,
				)
				return nil
			}
			if newDeadline != nil && !newDeadline.After(now) {
				preCondFailure = fmt.Sprintf(
					"cannot revive workflow %s under an already-passed new deadline", workflowID,
				)
				return nil
			}

			// Flip the workflow back to RUNNING ({FAILED, TIMED_OUT} -> RUNNING is a legal transition).
			if err := dbClient.MarkWorkflowRunning(dbCtx, workflowID, now); err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to mark workflow %s running", workflowID), err, true,
				)
			}

			// Revert every FAILED / TIMED_OUT step to DEFINED (sets user_restarted); leave every other
			// step untouched. ValidNextState restricts the DEFINED transition to exactly these two, so
			// selecting them here is both the intent and the guard.
			steps, err := dbClient.ListWorkflowSteps(dbCtx, workflowID)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to list steps of workflow %s to revive", workflowID), err, true,
				)
			}
			revertStepIDs := []string{}
			for _, step := range steps {
				if step.State == models.WorkflowStepStateFailed ||
					step.State == models.WorkflowStepStateTimedOut {
					revertStepIDs = append(revertStepIDs, step.ID)
				}
			}
			if len(revertStepIDs) > 0 {
				if err := dbClient.MarkWorkflowStepDefined(
					dbCtx, workflowID, revertStepIDs, now,
				); err != nil {
					return goutils.NewPersistenceError(
						fmt.Sprintf("failed to revert steps of workflow %s to DEFINED", workflowID),
						err, true,
					)
				}
			}

			// Apply the new deadline AFTER the revert so the just-DEFINED (non-terminal) steps are
			// re-synced to it in the same transaction - no window under a stale deadline.
			if newDeadline != nil {
				if err := dbClient.UpdateWorkflowDeadline(dbCtx, workflowID, *newDeadline); err != nil {
					return goutils.NewPersistenceError(
						fmt.Sprintf("failed to update deadline of workflow %s on revive", workflowID),
						err, true,
					)
				}
			}

			return nil
		},
	); dbErr != nil {
		return false, "", models.NewWorkflowSchedulerError(
			fmt.Sprintf("failed to revive workflow %s", workflowID), dbErr, true,
		)
	}

	// A precondition failure wrote nothing: report it for a record-and-drop (never replay).
	if preCondFailure != "" {
		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Warnf("Dropping revive request: %s", preCondFailure)
		return false, preCondFailure, nil
	}

	// State-before-poke: the revert is committed. Emit the single Process Workflow poke that re-runs
	// the reverted subtree through the normal DEFINED path. A lost poke loses only the poke - the
	// maintenance sweep re-drives it from the persisted DEFINED steps - so it is logged, not returned.
	if err := s.ipcSender.EnqueueMessage(
		ctx, models.PrepareIPCMsgWFProcessWorkflow(s.ipcName, workflowID, now),
	); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Errorf(
				"Failed to enqueue process-workflow poke for revived workflow %s; "+
					"maintenance sweep will re-drive it",
				workflowID,
			)
	}

	return true, "", nil
}
