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

// schedulerWorkReqTaskExecutionStarting [worker request] task execution scheduled to start
type schedulerWorkReqTaskExecutionStarting struct {
	InstanceID string
	Timestamp  time.Time
}

// fetchExecInstanceAndParentTask fetch an execution instance and its parent task
func (s *schedulerImpl) fetchExecInstanceAndParentTask(
	ctx context.Context, dbClient db.Database, instanceID string,
) (models.TaskExecution, models.Task, error) {
	execInstanceEntry, err := dbClient.GetTaskExecution(ctx, instanceID)
	if err != nil {
		return models.TaskExecution{},
			models.Task{},
			models.NewPersistenceError(
				fmt.Sprintf("failed to fetch task execution instance %s", instanceID), err, true,
			)
	}

	taskEntry, err := dbClient.GetTask(ctx, execInstanceEntry.TaskID)
	if err != nil {
		return models.TaskExecution{},
			models.Task{},
			models.NewPersistenceError(
				fmt.Sprintf(
					"failed to fetch parent task %s of execution instance %s",
					execInstanceEntry.TaskID,
					instanceID,
				), err, true,
			)
	}

	return execInstanceEntry, taskEntry, nil
}

/*
processTaskExecutionStarting process task execution scheduled to start

	@param ctx context.Context - execution context
	@param instanceID string - the execution instance ID
	@param timestamp time.Time - timestamp when execution completed
*/
func (s *schedulerImpl) processTaskExecutionStarting(
	ctx context.Context, instanceID string, timestamp time.Time,
) error {
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			execInstanceEntry, taskEntry, err := s.fetchExecInstanceAndParentTask(
				dbCtx, dbClient, instanceID,
			)
			if err != nil {
				return err
			}

			// Idempotency guard: this handler drives a SCHEDULED instance to ENQUEUED. If the
			// instance is already at or past ENQUEUED, a prior delivery (or a racing scan)
			// already started it - treat this as a no-op. A state upstream of ENQUEUED is not
			// masked and falls through to MarkTaskExecQueued, whose ValidNextState check
			// surfaces any genuine ordering violation.
			if execInstanceEntry.IsStateAtOrPast(models.TaskExecutionStateEnqueued) {
				log.
					WithFields(goutils.UpdateCodePositionInTags(s.GetLogTagsForContext(dbCtx))).
					Warnf(
						"Ignoring start request for execution instance %s already in state '%s'",
						execInstanceEntry.ID, execInstanceEntry.ExecutionState,
					)
				return nil
			}

			if err = dbClient.MarkTaskExecQueued(dbCtx, execInstanceEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf(
						"failed to mark task execution instance %s queued", execInstanceEntry.ID,
					), err, true,
				)
			}

			ipcSender, ok := s.taskIPcSenders[taskEntry.TaskName]
			if !ok {
				return goutils.NewConsistencyError(fmt.Sprintf(
					"no task IPC sender defined for '%s' tasks", taskEntry.TaskName,
				), nil, true)
			}
			if err = ipcSender.EnqueueMessage(dbCtx, models.PrepareIPCMsgTaskExecutionRequested(
				s.ipcName, execInstanceEntry.ID, timestamp,
			)); err != nil {
				return models.NewIPCMessageQueueError(
					fmt.Sprintf(
						"failed to enqueue request to process execution instance %s task %s",
						execInstanceEntry.ID,
						taskEntry.ID,
					), err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskSchedulerError(
			fmt.Sprintf("failed to start execution instance %s", instanceID), dbErr, true,
		)
	}
	return nil
}

// schedulerWorkReqTaskExecutionComplete [worker request] task execution instance completed
type schedulerWorkReqTaskExecutionComplete struct {
	InstanceID string
	Timestamp  time.Time
}

/*
processTaskExecutionComplete process completion of task execution

	@param ctx context.Context - execution context
	@param instanceID string - the execution instance ID
	@param timestamp time.Time - timestamp when execution completed
*/
func (s *schedulerImpl) processTaskExecutionComplete(
	ctx context.Context, instanceID string, _ time.Time,
) error {
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			execInstanceEntry, taskEntry, err := s.fetchExecInstanceAndParentTask(
				dbCtx, dbClient, instanceID,
			)
			if err != nil {
				return err
			}

			// Idempotency guard: this handler finalizes a PROCESSED instance. If it is already
			// at or past FINALIZED, a prior delivery (or the maintenance backstop racing the
			// worker's ExecuteSucceeded message) already handled it - treat this as a no-op. A
			// state upstream of FINALIZED falls through to MarkTaskExecFinalized, whose
			// ValidNextState check surfaces any genuine ordering violation.
			if execInstanceEntry.IsStateAtOrPast(models.TaskExecutionStateFinalized) {
				log.
					WithFields(goutils.UpdateCodePositionInTags(s.GetLogTagsForContext(dbCtx))).
					Warnf(
						"Ignoring completion of execution instance %s already in state '%s'",
						execInstanceEntry.ID, execInstanceEntry.ExecutionState,
					)
				return nil
			}

			if err = dbClient.MarkTaskExecFinalized(dbCtx, execInstanceEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf(
						"failed to mark task execution instance %s finalized", execInstanceEntry.ID,
					), err, true,
				)
			}

			if err = dbClient.MarkTaskComplete(dbCtx, taskEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to mark task %s complete", taskEntry.ID), err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskSchedulerError(
			fmt.Sprintf("failed to process completion of execution instance %s", instanceID), dbErr, true,
		)
	}
	return nil
}

// schedulerWorkReqTaskExecutionFailed [worker request] task execution instance failed
type schedulerWorkReqTaskExecutionFailed struct {
	InstanceID string
	Timestamp  time.Time
}

/*
processTaskExecutionFailed process failure of task execution

	@param ctx context.Context - execution context
	@param instanceID string - the execution instance ID
	@param timestamp time.Time - timestamp when execution completed
*/
func (s *schedulerImpl) processTaskExecutionFailed(
	ctx context.Context, instanceID string, timestamp time.Time,
) error {
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			execInstanceEntry, taskEntry, err := s.fetchExecInstanceAndParentTask(
				dbCtx, dbClient, instanceID,
			)
			if err != nil {
				return err
			}

			// Idempotency guard: this handler finalizes a FAILED instance (and decides on a
			// retry). If it is already at or past FINALIZED, a prior delivery (or the
			// maintenance backstop racing the worker's ExecuteFailed message) already handled
			// it - treat this as a no-op, so the retry decision is not run twice. A state
			// upstream of FINALIZED falls through to MarkTaskExecFinalized, whose
			// ValidNextState check surfaces any genuine ordering violation.
			if execInstanceEntry.IsStateAtOrPast(models.TaskExecutionStateFinalized) {
				log.
					WithFields(goutils.UpdateCodePositionInTags(s.GetLogTagsForContext(dbCtx))).
					Warnf(
						"Ignoring failure of execution instance %s already in state '%s'",
						execInstanceEntry.ID, execInstanceEntry.ExecutionState,
					)
				return nil
			}

			if err = dbClient.MarkTaskExecFinalized(dbCtx, execInstanceEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf(
						"failed to mark task execution instance %s finalized", execInstanceEntry.ID,
					), err, true,
				)
			}

			// If task is still active, try it.
			if taskEntry.TaskState == models.TaskStateActive {
				// Count the failed execution instances so far. The `terminal_state` is
				// captured when an instance ends and is preserved through the later move
				// to FINALIZED, so it (unlike `state`) reliably reports the outcome. The
				// current instance was marked with a FAILED terminal state before this
				// handler ran, so it is included in the count. `NextDelay` is 0-based on
				// the retry attempt: with N total failures the next retry is attempt N-1.
				failedExecInstances, err := dbClient.ListTaskExecutions(
					dbCtx, taskEntry.ID, db.TaskExecutionQueryFilter{
						TerminalStates: []models.TaskExecutionStateENUM{models.TaskExecutionStateFailed},
					},
				)
				if err != nil {
					return models.NewPersistenceError(
						fmt.Sprintf("failed to list failed execution instances of task %s", taskEntry.ID),
						err,
						true,
					)
				}

				retryDelay := taskEntry.RetryParams.NextDelay(len(failedExecInstances) - 1)
				if retryDelay <= 0 {
					// Exhausted retries
					if err = dbClient.MarkTaskFailed(dbCtx, taskEntry.ID); err != nil {
						return models.NewPersistenceError(
							fmt.Sprintf("failed to mark task %s failed", taskEntry.ID), err, true,
						)
					}
				} else {
					// Try again
					_, err := dbClient.DefineNewTaskRetryExecInstance(
						dbCtx, taskEntry, execInstanceEntry, timestamp.Add(retryDelay),
					)
					if err != nil {
						return models.NewPersistenceError(
							fmt.Sprintf(
								"failed to define retry execution instance for task %s", taskEntry.ID,
							), err, true,
						)
					}
				}
			}

			return nil
		},
	); dbErr != nil {
		return models.NewTaskSchedulerError(
			fmt.Sprintf("failed to process failure of execution instance %s", instanceID), dbErr, true,
		)
	}
	return nil
}

// schedulerWorkReqTaskExecutionTimedOut [worker request] task execution instance timed out
type schedulerWorkReqTaskExecutionTimedOut struct {
	InstanceID string
	Timestamp  time.Time
}

/*
processTaskExecutionTimedOut process timed out of task execution

	@param ctx context.Context - execution context
	@param instanceID string - the execution instance ID
	@param timestamp time.Time - timestamp when execution timed out
*/
func (s *schedulerImpl) processTaskExecutionTimedOut(
	ctx context.Context, instanceID string, timestamp time.Time,
) error {
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			execInstanceEntry, taskEntry, err := s.fetchExecInstanceAndParentTask(
				dbCtx, dbClient, instanceID,
			)
			if err != nil {
				return err
			}

			// Idempotency guard: timing out only applies to a still-live instance. If it has
			// already reached an outcome (completed, failed, or cancelled - possibly because
			// the real completion raced this timeout delivery), there is nothing to time out;
			// treat this as a no-op rather than forcing a spurious failure. A still-live
			// instance falls through to MarkTaskExecFailed, whose ValidNextState check guards
			// the actual transition.
			if execInstanceEntry.HasEnded() {
				log.
					WithFields(goutils.UpdateCodePositionInTags(s.GetLogTagsForContext(dbCtx))).
					Warnf(
						"Ignoring timeout of execution instance %s already in state '%s'",
						execInstanceEntry.ID, execInstanceEntry.ExecutionState,
					)
				return nil
			}

			if err = dbClient.MarkTaskExecFailed(
				dbCtx, instanceID, fmt.Sprintf("timed out at %s", timestamp.String()), timestamp,
			); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf(
						"failed to mark task execution instance %s failed", execInstanceEntry.ID,
					), err, true,
				)
			}

			if err = dbClient.MarkTaskExecFinalized(dbCtx, execInstanceEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf(
						"failed to mark task execution instance %s finalized", execInstanceEntry.ID,
					), err, true,
				)
			}

			// Since the execution instance has timed out, the task has also timed out
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
			fmt.Sprintf("failed to process timeout of execution instance %s", instanceID), dbErr, true,
		)
	}
	return nil
}
