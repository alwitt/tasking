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
processWorkflow handle a Process Workflow event: start the workflow on first receipt and fan out
its startable steps.

This is the DAG fan-out reducer and the workflow's entry point (see workflow/DESIGN.md "Process
Workflow"). It is the sole producer of the workflow PENDING -> RUNNING transition and of the step
DEFINED -> PENDING transition:

  - Hard-stop states (TIMED_OUT, CANCELLING, CANCELLED, COMPLETE) -> NOOP. A stale queued event that
    arrives after the workflow reached one of these harmlessly no-ops here.
  - Otherwise (PENDING, RUNNING, or FAILED): a PENDING workflow is moved to RUNNING; then every
    startable step (DEFINED with all parents COMPLETE) is moved to PENDING and a Schedule Workflow
    Step event is emitted for it. FAILED is a soft stop - healthy parallel tracks keep advancing.

State-before-poke (DESIGN Invariant 8): the state changes (workflow RUNNING, steps PENDING) are
committed to the DB first, in one transaction; the Schedule Workflow Step pokes are emitted only
after that commit. A failed emit therefore loses only the poke, never the work - the maintenance
sweep re-derives Schedule from the persisted PENDING step - so an enqueue failure is logged, not
returned.

	@param ctx context.Context - execution context
	@param workflowID string - the workflow to process
*/
func (s *schedulerImpl) processWorkflow(ctx context.Context, workflowID string) error {
	logTags := s.GetLogTagsForContext(ctx)
	now := time.Now().UTC()

	// Startable step IDs that were marked PENDING inside the transaction. Populated in the closure
	// and read by the emit loop below, which runs after the transaction commits (state-before-poke).
	var startableStepIDs []string

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			workflowEntry, err := dbClient.GetWorkflow(dbCtx, workflowID)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to fetch workflow %s", workflowID), err, true,
				)
			}

			// Hard stop: nothing new starts. A stale event that finds the workflow already in one
			// of these states harmlessly no-ops (the step fan-out below never runs).
			switch workflowEntry.State {
			case models.WorkflowStateTimedOut,
				models.WorkflowStateCancelling,
				models.WorkflowStateCancelled,
				models.WorkflowStateComplete:
				log.
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Debugf(
						"Ignoring process-workflow request for workflow %s in hard-stop state '%s'",
						workflowEntry.ID, workflowEntry.State,
					)
				return nil
			}

			// PENDING -> RUNNING on first processing. This is the only site of that transition;
			// a RUNNING or FAILED workflow is re-processed without it.
			if workflowEntry.State == models.WorkflowStatePending {
				if err := dbClient.MarkWorkflowRunning(dbCtx, workflowEntry.ID, now); err != nil {
					return goutils.NewPersistenceError(
						fmt.Sprintf("failed to mark workflow %s running", workflowEntry.ID), err, true,
					)
				}
			}

			// Fan out: every startable step (DEFINED with all parents COMPLETE). This query is
			// step-level only and does not gate on workflow state, so the hard/soft-stop policy
			// above is what governs whether we reach here.
			startableSteps, err := dbClient.ListWorkflowStepsReadyToRun(dbCtx, workflowEntry.ID)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to list startable steps of workflow %s", workflowEntry.ID),
					err, true,
				)
			}
			if len(startableSteps) == 0 {
				return nil
			}

			stepIDs := make([]string, 0, len(startableSteps))
			for _, step := range startableSteps {
				stepIDs = append(stepIDs, step.ID)
			}

			// DEFINED -> PENDING for the startable steps. This is the only site of that transition.
			err = dbClient.MarkWorkflowStepPending(dbCtx, workflowEntry.ID, stepIDs, now)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to mark workflow %s steps pending", workflowEntry.ID), err, true,
				)
			}

			startableStepIDs = stepIDs
			return nil
		},
	); dbErr != nil {
		return models.NewWorkflowSchedulerError(
			fmt.Sprintf("failed to process workflow %s", workflowID), dbErr, true,
		)
	}

	// State-before-poke: the driving state changes are committed above. Emit one Schedule Workflow
	// Step poke per startable step. A failed enqueue loses only the poke - the persisted PENDING
	// step is re-driven by the maintenance sweep - so it is logged, not returned.
	for _, stepID := range startableStepIDs {
		if err := s.ipcSender.EnqueueMessage(
			ctx, models.PrepareIPCMsgWFScheduleStep(s.ipcName, stepID, now),
		); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Failed to enqueue schedule-step poke for workflow %s step %s; "+
						"maintenance sweep will re-drive it",
					workflowID, stepID,
				)
		}
	}

	return nil
}
