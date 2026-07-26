package task

import (
	"context"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

// cancelOngoingExecInstancesOfTask cancel any ongoing execution instances of a task
func (s *schedulerImpl) cancelOngoingExecInstancesOfTask(
	ctx context.Context, dbClient db.Database, taskEntry models.Task, timestamp time.Time,
) error {
	// Cancel any ongoing execution instances as well
	execInstances, err := dbClient.ListTaskExecutions(
		ctx, taskEntry.ID, db.TaskExecutionQueryFilter{
			ExecStates: []models.TaskExecutionStateENUM{
				models.TaskExecutionStateDefined,
				models.TaskExecutionStateScheduled,
				models.TaskExecutionStateEnqueued,
				models.TaskExecutionStateAcquired,
				models.TaskExecutionStateProcessing,
			},
		},
	)
	if err != nil {
		return models.NewPersistenceError(
			fmt.Sprintf("failed to fetch task %s execution instances", taskEntry.ID), err, true,
		)
	}
	for _, oneInstance := range execInstances {
		err = dbClient.MarkTaskExecCancelled(ctx, oneInstance.ID, "parent task cancelled", timestamp)
		if err != nil {
			return models.NewPersistenceError(
				fmt.Sprintf(
					"failed to cancel execution instance %s task %s", oneInstance.ID, taskEntry.ID,
				), err, true,
			)
		}
	}
	return nil
}

/*
processCancelTask process a cancelled task

	@param ctx context.Context - execution context
	@param taskID string - the task ID
	@param timestamp time.Time - timestamp when the cancellation was requested
*/
func (s *schedulerImpl) processCancelTask(
	ctx context.Context, taskID string, timestamp time.Time,
) error {
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			taskEntry, err := dbClient.GetTask(dbCtx, taskID)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to fetch task %s", taskID), err, true,
				)
			}

			// Only a task which is still running can be cancelled. A task which has
			// already reached a resting state (CANCELLED, COMPLETE, FAILED, or
			// TIMED_OUT) has nothing left to cancel - acting on it here would attempt
			// an illegal state transition. Skipping makes the handler idempotent
			// against duplicate CANCEL_TASK messages and cancel requests that race a
			// natural completion.
			switch taskEntry.TaskState {
			case models.TaskStatePending, models.TaskStateActive, models.TaskStateCancelling:
				// still cancellable - fall through to the cancellation below
			default:
				log.WithFields(goutils.UpdateCodePositionInTags(s.GetLogTagsForContext(dbCtx))).
					Warnf(
						"Ignoring cancel request for task %s already in resting state '%s'",
						taskEntry.ID, taskEntry.TaskState,
					)
				return nil
			}

			// A task can only transition into CANCELLED from CANCELLING. A cancel
			// request may however arrive while the task is still PENDING or ACTIVE
			// (nothing drives it through CANCELLING first), so stage it through
			// CANCELLING here before finalizing the cancellation.
			if taskEntry.TaskState != models.TaskStateCancelling {
				if err := dbClient.MarkTaskCancelling(dbCtx, taskEntry.ID); err != nil {
					return models.NewPersistenceError(
						fmt.Sprintf("failed to mark task %s cancelling", taskEntry.ID), err, true,
					)
				}
			}

			if err := dbClient.MarkTaskCancelled(dbCtx, taskEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to cancel task %s", taskEntry.ID), err, true,
				)
			}

			err = s.cancelOngoingExecInstancesOfTask(dbCtx, dbClient, taskEntry, timestamp)
			if err != nil {
				return err
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskSchedulerError(
			fmt.Sprintf("failed to cancel task %s", taskID), dbErr, true,
		)
	}
	return nil
}

/*
processTaskTimeout process a task timing out

	@param ctx context.Context - execution context
	@param taskID string - the task ID
	@param timestamp time.Time - timestamp when execution timed out
*/
func (s *schedulerImpl) processTaskTimeout(
	ctx context.Context, taskID string, timestamp time.Time,
) error {
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			taskEntry, err := dbClient.GetTask(dbCtx, taskID)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to fetch task %s", taskID), err, true,
				)
			}

			// Only an ACTIVE task can time out. A task which is no longer active has
			// already been dispatched to another terminal state (or has yet to become
			// active), so marking it timed out here would be an illegal transition.
			// Skipping makes the handler idempotent against duplicate timeout requests
			// and timeouts that race a natural completion.
			if taskEntry.TaskState != models.TaskStateActive {
				log.WithFields(goutils.UpdateCodePositionInTags(s.GetLogTagsForContext(dbCtx))).
					Warnf(
						"Ignoring timeout request for task %s not in state 'ACTIVE' (currently '%s')",
						taskEntry.ID, taskEntry.TaskState,
					)
				return nil
			}

			if err := dbClient.MarkTaskTimedOut(dbCtx, taskEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to mark task %s timed out", taskEntry.ID), err, true,
				)
			}

			err = s.cancelOngoingExecInstancesOfTask(dbCtx, dbClient, taskEntry, timestamp)
			if err != nil {
				return err
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskSchedulerError(
			fmt.Sprintf("failed to process timeout of task %s", taskID), dbErr, true,
		)
	}
	return nil
}
