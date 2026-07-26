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

// nonTerminalWorkflowStates are the workflow states the maintenance sweep reconciles: everything
// except the terminal COMPLETE / CANCELLED. CANCELLING is included - it is non-terminal and its
// steps must be drained to reach CANCELLED.
var nonTerminalWorkflowStates = []models.WorkflowStateENUM{
	models.WorkflowStatePending,
	models.WorkflowStateRunning,
	models.WorkflowStateFailed,
	models.WorkflowStateTimedOut,
	models.WorkflowStateCancelling,
}

/*
mapTaskStateToStepState map a task's PERSISTED state to the step outcome it drives, reporting
whether the task is terminal. The maintenance sweep reconciles a step against its linked task's
persisted state (the RUNNING / CANCELLING classifiers), whereas mapTaskEventToStepState maps a task
EVENT type (the notify fast path); this is the state-keyed sibling.

A live task (PENDING / ACTIVE / CANCELLING) has no resolved outcome yet -> (_, false).

	@param taskState models.TaskStateENUM - the task's persisted state
	@returns the driven step state, and whether the task is terminal
*/
func mapTaskStateToStepState(
	taskState models.TaskStateENUM,
) (models.WorkflowStepStateENUM, bool) {
	switch taskState {
	case models.TaskStateComplete:
		return models.WorkflowStepStateComplete, true
	case models.TaskStateFailed:
		return models.WorkflowStepStateFailed, true
	case models.TaskStateTimeout:
		return models.WorkflowStepStateTimedOut, true
	case models.TaskStateCancelled:
		return models.WorkflowStepStateCancelled, true
	default:
		// PENDING / ACTIVE / CANCELLING: still live, no resolved outcome.
		return "", false
	}
}

/*
runMaintenanceSweep handle a Workflow State Maintenance event: the periodic Layer 2 recovery /
liveness sweep (see workflow/DESIGN.md "Layer 2 - Maintenance Sweep"). It is load-bearing for
correctness, not merely a safety net: because notify feedback is best-effort with no buffer, this
sweep is the only thing that recovers dropped feedback, failed enqueues, and crashes between two DB
writes. It trusts no queue message and re-derives everything from persisted state.

It fetches all non-terminal workflows first (one read), then reconciles each in its OWN transaction
(reconcileWorkflow). A failure reconciling one workflow is logged and skipped so it does not stop \
the others; only a failure of the listing itself is fatal (returned, so the maintenance message
replays). The list-then-per-workflow-tx shape has a benign TOCTOU window: the sweep is idempotent
and runs again next interval.

	@param ctx context.Context - execution context
*/
func (s *schedulerImpl) runMaintenanceSweep(ctx context.Context) error {
	logTags := s.GetLogTagsForContext(ctx)

	// Fetch all non-terminal workflows first, in their own read-only transaction. Each is then
	// reconciled in its own separate transaction below (per-workflow isolation).
	var workflows []models.Workflow
	if err := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			listed, err := dbClient.ListWorkflows(dbCtx, db.WorkflowQueryFilter{
				TargetStates: nonTerminalWorkflowStates,
			})
			if err != nil {
				return models.NewPersistenceError(
					"failed to list non-terminal workflows for maintenance sweep", err, true,
				)
			}
			workflows = listed
			return nil
		},
	); err != nil {
		return models.NewWorkflowSchedulerError(
			"failed to list non-terminal workflows for maintenance sweep", err, true,
		)
	}

	for _, workflow := range workflows {
		// Per-workflow failure isolation: one workflow's failure must not stop the others. The sweep
		// is idempotent, so a skipped workflow is reconciled on the next tick.
		if err := s.reconcileWorkflow(ctx, workflow.ID); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Maintenance sweep failed to reconcile workflow %s; skipping (next sweep retries)",
					workflow.ID,
				)
		}
	}

	return nil
}

// execUpdatePoke is a synthesized Execution Update the sweep emits post-commit: [step ID, resolved
// terminal step state], applied by a later tick through the shared Execution Update reducer.
type execUpdatePoke struct {
	stepID       string
	newStepState models.WorkflowStepStateENUM
}

/*
reconcileWorkflow reconcile a single non-terminal workflow against its persisted steps + linked
tasks + deadline, in ONE transaction (see workflow/DESIGN.md reconciliation table).

Workflow-level short-circuit: a past-deadline workflow is timed out whole (timeOutWorkflowSteps
flips the workflow + all non-terminal steps to TIMED_OUT and reports the still-RUNNING tasks to
cancel), and the per-step pass is skipped - every non-terminal step is about to become TIMED_OUT
anyway.

Otherwise, per non-terminal step it derives the drive to issue (post-commit): a DEFINED step or a
PENDING workflow needs one Process Workflow re-drive (collapsed to a single poke); a PENDING step
needs a Schedule Step re-emit; a RUNNING step is classified against its task (live -> leave;
terminal -> synthesized Execution Update from the task's state; zombie -> synthesized FAILED); a
CANCELLING step is classified against its task (terminal/missing -> synthesized CANCELLED; live ->
re-cancel). Finally the workflow aggregate is re-checked (settleWorkflowIfDone) as a backstop.

State-before-poke (DESIGN Invariant 8): the only in-tx writes are the aggregate re-check
(settle / timeout, which have no owning handler); every per-step drive is a post-commit poke applied
by a later tick. The pokes are emitted in a significant order - all Execution Updates, then all
Schedule Steps, then one Process Workflow - so the trailing Process Workflow sees the drained /
unblocked DAG and re-drives fewer already-handled steps (fewer duplicate scheduler events later).

	@param ctx context.Context - execution context
	@param workflowID string - the workflow to reconcile
*/
func (s *schedulerImpl) reconcileWorkflow(ctx context.Context, workflowID string) error {
	logTags := s.GetLogTagsForContext(ctx)
	now := time.Now().UTC()

	// Post-commit intent captured inside the transaction (state-before-poke).
	var needProcessWorkflow bool
	var scheduleStepIDs []string
	var execUpdatePokes []execUpdatePoke
	var cancelTaskIDs []string

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			workflowEntry, err := dbClient.GetWorkflow(dbCtx, workflowID)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to fetch workflow %s to reconcile", workflowID), err, true,
				)
			}

			// Workflow-level short-circuit: past its deadline -> time out the whole workflow (all steps
			// share the deadline) and skip the per-step pass. The still-RUNNING tasks are cancelled
			// post-commit.
			if !workflowEntry.Deadline.After(now) {
				cancelTaskIDs, err = s.timeOutWorkflowSteps(dbCtx, dbClient, workflowEntry, now)
				if err != nil {
					return err
				}
				return nil
			}

			// PENDING workflow: its start poke was lost. One Process Workflow re-drive does the
			// PENDING -> RUNNING transition + the initial fan-out (and collapses with the DEFINED-step
			// case below).
			if workflowEntry.State == models.WorkflowStatePending {
				needProcessWorkflow = true
			}

			steps, err := dbClient.ListWorkflowSteps(dbCtx, workflowID)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to list steps of workflow %s to reconcile", workflowID), err, true,
				)
			}

			for _, step := range steps {
				switch step.State {
				case models.WorkflowStepStateDefined:
					// Re-evaluate startability = re-drive Process Workflow. One poke per workflow covers
					// all its DEFINED steps.
					needProcessWorkflow = true

				case models.WorkflowStepStatePending:
					// Re-emit Schedule Step (idempotent via its non-terminal-task guard).
					scheduleStepIDs = append(scheduleStepIDs, step.ID)

				case models.WorkflowStepStateRunning:
					poke, err := s.classifyRunningStep(dbCtx, dbClient, step)
					if err != nil {
						return err
					}
					if poke != nil {
						execUpdatePokes = append(execUpdatePokes, *poke)
					}

				case models.WorkflowStepStateCancelling:
					poke, cancel, err := s.classifyCancellingStep(dbCtx, dbClient, step)
					if err != nil {
						return err
					}
					if poke != nil {
						execUpdatePokes = append(execUpdatePokes, *poke)
					}
					cancelTaskIDs = append(cancelTaskIDs, cancel...)

				default:
					// COMPLETE / FAILED / TIMED_OUT / CANCELLED: terminal, nothing per-step.
					continue
				}
			}

			// Aggregate re-check (backstop): all COMPLETE -> COMPLETE; settled CANCELLING -> CANCELLED.
			// (Past-deadline -> TIMED_OUT was handled by the short-circuit above.)
			if _, err := s.settleWorkflowIfDone(dbCtx, dbClient, workflowEntry, now); err != nil {
				return err
			}

			return nil
		},
	); dbErr != nil {
		return models.NewWorkflowSchedulerError(
			fmt.Sprintf("failed to reconcile workflow %s", workflowID), dbErr, true,
		)
	}

	// State-before-poke: the reconciled state is committed. Emit the drives in the significant order
	// - all Execution Updates, then all Schedule Steps, then one Process Workflow - so the trailing
	// Process Workflow sees the drained / unblocked DAG and re-drives fewer already-handled steps.
	// The task-cancel pokes are pure side-effects (no scheduler event) and are issued alongside.
	// All are logged, not returned: a lost poke is re-derived by the next sweep.
	for _, poke := range execUpdatePokes {
		if err := s.ipcSender.EnqueueMessage(
			ctx,
			models.PrepareIPCMsgWFStepExecUpdate(s.ipcName, poke.stepID, poke.newStepState, now),
		); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Maintenance sweep failed to enqueue synthesized execution update for step %s "+
						"(state '%s'); next sweep re-derives it",
					poke.stepID, poke.newStepState,
				)
		}
	}
	for _, stepID := range scheduleStepIDs {
		if err := s.ipcSender.EnqueueMessage(
			ctx, models.PrepareIPCMsgWFScheduleStep(s.ipcName, stepID, now),
		); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Maintenance sweep failed to enqueue schedule-step poke for step %s; "+
						"next sweep re-derives it",
					stepID,
				)
		}
	}
	if needProcessWorkflow {
		if err := s.ipcSender.EnqueueMessage(
			ctx, models.PrepareIPCMsgWFProcessWorkflow(s.ipcName, workflowID, now),
		); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Maintenance sweep failed to enqueue process-workflow poke for workflow %s; "+
						"next sweep re-derives it",
					workflowID,
				)
		}
	}
	for _, taskID := range cancelTaskIDs {
		if err := s.taskClient.CancelTask(ctx, taskID, nil); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Maintenance sweep failed to request cancel of task %s for workflow %s; "+
						"the task's own deadline / next sweep reconciles it",
					taskID, workflowID,
				)
		}
	}

	return nil
}

/*
classifyRunningStep classify a RUNNING step against its linked task(s) and report the drive to issue
post-commit (see workflow/DESIGN.md RUNNING row):

  - a live task exists (PENDING / ACTIVE) -> leave alone, feedback will come -> no poke;
  - a terminal task exists but the step is still RUNNING -> feedback was lost -> synthesized
    Execution Update from the task's persisted terminal state;
  - no task at all (zombie) -> synthesized FAILED (the deadline is the ultimate backstop, but a
    zombie with no discoverable task must still flow through the FAILED path so the workflow is
    not stuck).

Runs inside the caller's transaction (reads only). Unlike a CANCELLING step, a RUNNING step never
returns a task to cancel: a live task is left running (its feedback will come), and the
past-deadline cancel is handled wholesale by reconcileWorkflow's short-circuit - so the only output
is the synthesized Execution Update (or none).

	@returns the synthesized Execution Update to emit (nil if none)
*/
func (s *schedulerImpl) classifyRunningStep(
	dbCtx context.Context, dbClient db.Database, step models.WorkflowStep,
) (*execUpdatePoke, error) {
	_, tasks, err := dbClient.GetWorkflowStepAndExecutorTask(dbCtx, step.ID, false)
	if err != nil {
		return nil, models.NewPersistenceError(
			fmt.Sprintf("failed to fetch tasks of running step %s to reconcile", step.ID), err, true,
		)
	}

	// A live task means the step is legitimately still running - leave it alone.
	for _, task := range tasks {
		if _, terminal := mapTaskStateToStepState(task.TaskState); !terminal {
			return nil, nil
		}
	}

	// No live task. Zombie (no task at all) -> FAILED; otherwise the (terminal) task's outcome was
	// never fed back -> synthesize it.
	if len(tasks) == 0 {
		return &execUpdatePoke{stepID: step.ID, newStepState: models.WorkflowStepStateFailed}, nil
	}

	// Every linked task is terminal and feedback was lost. A revived-and-re-run step accumulates the
	// prior attempt's terminal task alongside the current one, so drive the outcome of the MOST
	// RECENT task - GetWorkflowStepAndExecutorTask orders most-recent-first, so tasks[0] is the
	// current attempt.
	newStepState, _ := mapTaskStateToStepState(tasks[0].TaskState)
	return &execUpdatePoke{stepID: step.ID, newStepState: newStepState}, nil
}

/*
classifyCancellingStep classify a CANCELLING step against its linked task(s) (see workflow/DESIGN.md
CANCELLING row):

  - task terminal or missing -> synthesized Execution Update CANCELLED (the reducer's
    cancellation-wins branch marks the step CANCELLED and settles the workflow if applicable);
  - a live task exists -> re-issue cancel post-commit and wait.

Runs inside the caller's transaction (reads only).

	@returns the synthesized Execution Update to emit (nil if none), and task IDs to cancel
	    post-commit
*/
func (s *schedulerImpl) classifyCancellingStep(
	dbCtx context.Context, dbClient db.Database, step models.WorkflowStep,
) (*execUpdatePoke, []string, error) {
	_, tasks, err := dbClient.GetWorkflowStepAndExecutorTask(dbCtx, step.ID, false)
	if err != nil {
		return nil, nil, models.NewPersistenceError(
			fmt.Sprintf("failed to fetch tasks of cancelling step %s to reconcile", step.ID), err, true,
		)
	}

	// A live task means cancellation is still in progress - re-issue the cancel and wait.
	liveTaskIDs := []string{}
	for _, task := range tasks {
		if _, terminal := mapTaskStateToStepState(task.TaskState); !terminal {
			liveTaskIDs = append(liveTaskIDs, task.ID)
		}
	}
	if len(liveTaskIDs) > 0 {
		return nil, liveTaskIDs, nil
	}

	// Task terminal or missing -> the cancel has landed; drive the step to CANCELLED.
	return &execUpdatePoke{stepID: step.ID, newStepState: models.WorkflowStepStateCancelled}, nil, nil
}
