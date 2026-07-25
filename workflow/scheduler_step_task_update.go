package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

/*
applyStepTaskUpdate handle a Workflow Step Task Update event: the notify fast-path feedback, keyed by
task ID. It is the middle hop between the notify adapter (onNotification) and the step-keyed
execution-update reducer (applyStepExecutionUpdate).

The notify callback runs on the subscriber's reader goroutine and does no DB work, so it can only
carry the task ID. This handler runs on the single-threaded scheduler worker, where a DB read is
safe: it resolves task -> step (step<->task linkage), then delegates the actual outcome + DAG
advancement to the reducer (which owns its own transaction and post-commit pokes).

A task with no linked step is a benign drop (return nil): a notification for a task this engine's DB
does not link to a step - a stale, foreign, or already-cleaned-up task - must not wedge the queue,
mirroring the reducer's idempotent-no-op posture. Correctness never rests on the notification; the
maintenance sweep reconciles from persisted state.

	@param ctx context.Context - execution context
	@param taskID string - the task whose terminal event drove this update
	@param newStepState models.WorkflowStepStateENUM - the resolved terminal step state
*/
func (s *schedulerImpl) applyStepTaskUpdate(
	ctx context.Context, taskID string, newStepState models.WorkflowStepStateENUM,
) error {
	logTags := s.GetLogTagsForContext(ctx)

	// Resolve task -> step in its own short read transaction. The reducer opens its own transaction
	// for the writes, matching how the other handlers separate the resolve from the apply.
	var step models.WorkflowStep
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			resolved, err := dbClient.GetWorkflowStepProcessedByTask(dbCtx, taskID)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to resolve step for task %s", taskID), err, true,
				)
			}
			step = resolved
			return nil
		},
	); dbErr != nil {
		// A missing task->step link is benign: drop the feedback, don't wedge the queue.
		var notFound goutils.NotFoundError
		if errors.As(dbErr, &notFound) {
			log.
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Warnf("Dropping step task update: no workflow step linked to task %s", taskID)
			return nil
		}
		return models.NewWorkflowSchedulerError(
			fmt.Sprintf("failed to resolve step for task %s", taskID), dbErr, true,
		)
	}

	// Delegate to the step-keyed reducer.
	return s.applyStepExecutionUpdate(ctx, step.ID, newStepState)
}
