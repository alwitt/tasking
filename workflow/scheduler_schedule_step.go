package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
)

/*
scheduleWorkflowStep handle a Schedule Workflow Step event: dispatch one specific step to the
task engine.

This is the sole setter of the step PENDING -> RUNNING transition (see workflow/DESIGN.md
"Schedule Workflow Step"). Dispatch is idempotent (DESIGN Invariant 3): it never submits a
duplicate task for a step that already has a live (non-terminal) linked task. Guard cases below
all no-op harmlessly - a stale or superseded poke simply finds nothing to do:

  - Workflow in a hard-stop state (TIMED_OUT, CANCELLING, CANCELLED, COMPLETE) -> do not dispatch.
  - Step is not PENDING (already advanced, or reverted) -> do not dispatch.
  - A live linked task already exists -> do not dispatch a duplicate; the existing task's terminal
    event (or the maintenance sweep) drives the step forward.

On a clean dispatch it defines the step's execution task (Name WorkflowExecutionTaskName, a
TaskParameterExecuteWorkflowStep payload carrying only the step ID, the step's own Deadline and
RetryParams, and the fixed WorkflowExecutionTaskCreator that routes the task's feedback back here),
links the step to that task, and marks the step RUNNING - all in one transaction.

State-before-poke (DESIGN Invariant 8): the driving state (task row, step<->task link, step
RUNNING) is committed first; the task is submitted to the task-scheduler queue only after that
commit. A failed submit loses only the poke - the task row is committed PENDING so the task
engine's own maintenance re-drives it, and the step is committed RUNNING so the workflow
maintenance sweep can reconcile it against that task - so a submit failure is logged, not returned.

Before dispatching, the workflow deadline is checked: if it has passed, the whole workflow is timed
out instead (via the shared timeOutWorkflowSteps helper, also used by the Execution Update handler),
its still-running step tasks are cancelled, and no dispatch occurs.

	@param ctx context.Context - execution context
	@param stepID string - the workflow step to dispatch
*/
func (s *schedulerImpl) scheduleWorkflowStep(ctx context.Context, stepID string) error {
	logTags := s.GetLogTagsForContext(ctx)
	now := time.Now().UTC()

	// The task to submit after the transaction commits (state-before-poke). Empty means nothing
	// was dispatched (a guard no-op'd), so there is no poke to send.
	var submitTaskID string
	// Task IDs to cancel after commit, if the workflow deadline passed and we timed it out instead
	// of dispatching (state-before-poke, mirrors submitTaskID).
	var cancelTaskIDs []string

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			// Load the step together with its live (non-terminal) linked tasks - the idempotency
			// guard - in one query.
			step, liveTasks, err := dbClient.GetWorkflowStepAndExecutorTask(dbCtx, stepID, true)
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

			// Hard stop: nothing new starts. A stale schedule poke that finds the workflow in one
			// of these states harmlessly no-ops.
			switch workflowEntry.State {
			case models.WorkflowStateTimedOut,
				models.WorkflowStateCancelling,
				models.WorkflowStateCancelled,
				models.WorkflowStateComplete:
				log.
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Debugf(
						"Ignoring schedule-step request for step %s: workflow %s in hard-stop state '%s'",
						stepID, workflowEntry.ID, workflowEntry.State,
					)
				return nil
			}

			// Step must be PENDING to dispatch. A superseded/stale poke (step already RUNNING,
			// terminal, or reverted to DEFINED) no-ops; ValidNextState would reject the transition
			// anyway.
			if step.State != models.WorkflowStepStatePending {
				log.
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Debugf(
						"Ignoring schedule-step request for step %s: not PENDING (state '%s')",
						stepID, step.State,
					)
				return nil
			}

			// Idempotency guard (DESIGN Invariant 3): a live linked task already runs this step, so
			// do not dispatch a duplicate. Its terminal event / the sweep drives the step forward.
			if len(liveTasks) > 0 {
				log.
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Debugf(
						"Ignoring schedule-step request for step %s: %d live task(s) already exist",
						stepID, len(liveTasks),
					)
				return nil
			}

			// Deadline enforcement: if the workflow deadline has passed, do not dispatch - time out
			// the whole workflow (and its non-terminal steps) instead, and cancel any still-running
			// step tasks after commit. This is the same shared helper the Execution Update handler
			// uses. Leaving submitTaskID empty means no dispatch poke is sent below.
			if now.After(workflowEntry.Deadline) {
				log.
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Debugf(
						"Not dispatching step %s: workflow %s deadline has passed; timing it out",
						stepID, workflowEntry.ID,
					)
				var err error
				cancelTaskIDs, err = s.timeOutWorkflowSteps(dbCtx, dbClient, workflowEntry, now)
				if err != nil {
					return err
				}
				return nil
			}

			// Dispatch: define the step's execution task in this same transaction. The task carries
			// only the step ID as its Parameters; the step's own Deadline + RetryParams are handed
			// to the task; the fixed workflow-engine Creator is what routes the task's terminal
			// feedback back to this scheduler over notify.
			creator := models.WorkflowExecutionTaskCreator
			deadline := step.Deadline
			retry := step.RetryParams
			stepTask, err := s.taskClient.DefineImmediateOneShotTask(
				dbCtx,
				task.DefineTaskParams{
					Name:       models.WorkflowExecutionTaskName,
					Parameters: models.TaskParameterExecuteWorkflowStep{StepID: stepID},
					Creator:    &creator,
					Deadline:   &deadline,
					Retry:      &retry,
				},
				dbClient,
			)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to define execution task for step %s", stepID), err, true,
				)
			}

			// Persist the step<->task linkage (load-bearing for feedback resolution and both
			// recovery layers).
			if err := dbClient.LinkWorkflowStepWithExecutorTask(dbCtx, stepID, stepTask.ID); err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf(
						"failed to link step %s with execution task %s", stepID, stepTask.ID,
					), err, true,
				)
			}

			// PENDING -> RUNNING. This is the only site of that transition.
			if err := dbClient.MarkWorkflowStepRunning(
				dbCtx, step.WorkflowID, []string{stepID}, now,
			); err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to mark step %s running", stepID), err, true,
				)
			}

			submitTaskID = stepTask.ID
			return nil
		},
	); dbErr != nil {
		return models.NewWorkflowSchedulerError(
			fmt.Sprintf("failed to schedule workflow step %s", stepID), dbErr, true,
		)
	}

	// State-before-poke: the dispatch state is committed above. Submit the task now. A failed
	// submit loses only the poke - the task row is committed PENDING (task engine maintenance
	// re-drives) and the step is committed RUNNING (workflow sweep reconciles) - so it is logged,
	// not returned.
	if submitTaskID != "" {
		if err := s.taskClient.SubmitTask(ctx, submitTaskID); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Failed to submit execution task %s for step %s; "+
						"task engine / maintenance sweep will re-drive it",
					submitTaskID, stepID,
				)
		}
	}

	// State-before-poke: if we timed the workflow out instead of dispatching, cancel the still-running
	// step tasks now (after commit). A failed cancel loses only the poke - the task's own deadline /
	// the maintenance sweep reconciles it - so it is logged, not returned.
	for _, taskID := range cancelTaskIDs {
		if err := s.taskClient.CancelTask(ctx, taskID, nil); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Failed to request cancel of task %s for timed-out workflow; "+
						"the task's own deadline / maintenance sweep will reconcile it",
					taskID,
				)
		}
	}

	return nil
}
