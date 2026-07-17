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

// schedulerWorkReqNewPendingTask [worker request] new pending task
type schedulerWorkReqNewPendingTask struct {
	TaskID string
}

/*
processNewPendingTask process a new pending task needing scheduling

	@param ctx context.Context - execution context
	@param taskID string - the task ID
*/
func (s *schedulerImpl) processNewPendingTask(ctx context.Context, taskID string) error {
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			taskEntry, err := dbClient.GetTask(dbCtx, taskID)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to fetch task %s", taskID), err, true,
				)
			}

			// Only a PENDING task needs scheduling. A task which is already ACTIVE (or in
			// any other state) has been scheduled before - re-processing it here would
			// define a duplicate execution instance for a task that may already have one
			// scheduled or in-flight. This makes the handler idempotent against duplicate
			// NEW_TASK messages.
			if taskEntry.TaskState != models.TaskStatePending {
				log.
					WithFields(goutils.UpdateCodePositionInTags(s.GetLogTagsForContext(dbCtx))).
					Warnf(
						"Ignoring new-pending-task request for task %s already in state '%s'",
						taskEntry.ID, taskEntry.TaskState,
					)
				return nil
			}

			if err := dbClient.MarkTaskActive(dbCtx, taskEntry.ID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to activate task %s", taskEntry.ID), err, true,
				)
			}

			execInstanceEntry, err := dbClient.DefineNewTaskExecInstance(dbCtx, taskEntry)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to define execution instance for task %s", taskEntry.ID), err, true,
				)
			}

			// Enqueue execution instance for processing
			if execInstanceEntry.ExecutionClass == models.TaskExecutionClassImmediate {
				if err = dbClient.MarkTaskExecQueued(dbCtx, execInstanceEntry.ID); err != nil {
					return models.NewPersistenceError(
						fmt.Sprintf(
							"failed to mark execution instance %s task %s queued",
							execInstanceEntry.ID,
							taskEntry.ID,
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
					s.ipcName, execInstanceEntry.ID, time.Now().UTC(),
				)); err != nil {
					return models.NewIPCMessageQueueError(
						fmt.Sprintf(
							"failed to enqueue request to process execution instance %s task %s",
							execInstanceEntry.ID,
							taskEntry.ID,
						), err, true,
					)
				}
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskSchedulerError(
			fmt.Sprintf("failed to schedule new pending task %s", taskID), dbErr, true,
		)
	}
	return nil
}
