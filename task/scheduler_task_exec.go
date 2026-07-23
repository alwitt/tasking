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
decideExecutionRetry decides whether a failed execution instance should be retried, and after
what delay. It is the single decision point shared by processTaskExecutionFailed's two callers
(the EXECUTE_FAILED IPC handler and the maintenance backstop), so the "skip retry on a
non-retryable failure" rule cannot drift between them.

A NON_RETRYABLE disposition (persisted on the instance by the executor when a processor returned
a NonRecoverableError) short-circuits to "no retry" regardless of remaining retry budget. Any
other disposition (including nil, the retryable default) applies the task's exponential backoff.

	@param failedInstance models.TaskExecution - the execution instance that failed
	@param retryParams models.TaskRetryParameters - the parent task's retry parameters
	@param priorFailureCount int - total number of failed executions for the task (including this one)
	@returns retryDelay time.Duration - delay before the retry (0 when not retrying)
	@returns shouldRetry bool - whether a retry should be scheduled
*/
func decideExecutionRetry(
	failedInstance models.TaskExecution,
	retryParams models.TaskRetryParameters,
	priorFailureCount int,
) (time.Duration, bool) {
	if failedInstance.FailureDisposition != nil &&
		*failedInstance.FailureDisposition == models.TaskFailureDispositionNonRetryable {
		return 0, false
	}
	// NextDelay is 0-based on the retry attempt: with N total failures the next retry is attempt N-1.
	retryDelay := retryParams.NextDelay(priorFailureCount - 1)
	return retryDelay, retryDelay > 0
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
				// handler ran, so it is included in the count.
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

				retryDelay, shouldRetry := decideExecutionRetry(
					execInstanceEntry, taskEntry.RetryParams, len(failedExecInstances),
				)
				if !shouldRetry {
					// Non-retryable failure, or retries exhausted
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

// schedulerWorkReqTaskExecutionEngineFailed [worker request] the core task engine failed to
// operate on an execution instance (e.g. the receiver could not claim it, or could not submit
// it to the executor)
type schedulerWorkReqTaskExecutionEngineFailed struct {
	InstanceID string
	Timestamp  time.Time
}

/*
processTaskExecutionEngineFailed process an engine-level failure on a task execution instance.

Unlike processTaskExecutionFailed, an engine-level failure (the receiver could not claim the
instance, or could not submit it to the executor) is not retried: the instance is finalized
and the parent task is marked failed. An audit event recording the engine failure is written
in the same transaction so it commits atomically with the state changes. The receiver has
already marked the instance FAILED before sending this, so this handler finalizes it.

	@param ctx context.Context - execution context
	@param instanceID string - the execution instance ID
	@param timestamp time.Time - timestamp when the engine failure was reported
*/
func (s *schedulerImpl) processTaskExecutionEngineFailed(
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

			// Idempotency guard: this handler finalizes the instance and fails the task. If the
			// instance is already at or past FINALIZED, a prior delivery (or the maintenance
			// backstop racing this message) already handled it - treat this as a no-op. A state
			// upstream of FINALIZED falls through to MarkTaskExecFinalized, whose ValidNextState
			// check surfaces any genuine ordering violation.
			if execInstanceEntry.IsStateAtOrPast(models.TaskExecutionStateFinalized) {
				log.
					WithFields(goutils.UpdateCodePositionInTags(s.GetLogTagsForContext(dbCtx))).
					Warnf(
						"Ignoring engine failure of execution instance %s already in state '%s'",
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

			// An engine-level failure is terminal for the task - no retry.
			if err = dbClient.MarkTaskFailed(dbCtx, taskEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to mark task %s failed", taskEntry.ID), err, true,
				)
			}

			// Record the engine failure as an audit event. This commits atomically with the
			// state changes above, so a task marked failed for this reason always has its
			// matching audit entry.
			if err = dbClient.RecordTaskEngineFailure(
				dbCtx, taskEntry, execInstanceEntry.ID,
				fmt.Sprintf(
					"task engine failed to operate on execution instance %s", execInstanceEntry.ID,
				),
			); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf(
						"failed to record engine failure audit event for task %s", taskEntry.ID,
					), err, true,
				)
			}

			return nil
		},
	); dbErr != nil {
		return models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to process engine failure of execution instance %s", instanceID,
			), dbErr, true,
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
				dbCtx, instanceID, fmt.Sprintf("timed out at %s", timestamp.String()), nil, timestamp,
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
