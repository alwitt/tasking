// Package db - database controllers for system persistence
package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/models"
	"github.com/oklog/ulid/v2"
	"gorm.io/datatypes"
)

// ======================================================================================
// Task

/*
DefineNewOneShotTask define a new one-shot task for immediate execution

	@param ctx context.Context - execution context
	@param params NewTaskParameter - new task parameters
	@returns task instance
*/
func (c *databaseImpl) DefineNewOneShotTask(
	_ context.Context, params NewTaskParameter,
) (models.Task, error) {
	if params.Parameters != nil {
		if err := c.validator.Struct(params.Parameters); err != nil {
			return models.Task{}, goutils.NewValidationError(
				fmt.Sprintf("new one shot task '%s' parameters entry is not valid", params.Name), err, true,
			)
		}
	}
	if params.Metadata != nil {
		if err := c.validator.Struct(params.Metadata); err != nil {
			return models.Task{}, goutils.NewValidationError(
				fmt.Sprintf("new one shot task '%s' metadata entry is not valid", params.Name), err, true,
			)
		}
	}

	parametersStr, _ := json.Marshal(&params.Parameters)
	metadataStr, _ := json.Marshal(&params.Metadata)

	newEntry := taskEntry{
		Task: models.Task{
			ID:                ulid.Make().String(),
			TaskName:          params.Name,
			Creator:           params.Creator,
			TaskScheduleClass: models.TaskScheduleClassImmediateOneShot,
			TaskState:         models.TaskStatePending,
			Parameters:        datatypes.JSON(parametersStr),
			Metadata:          datatypes.JSON(metadataStr),
			Deadline:          params.Deadline,
			RetryParams:       params.RetryParam,
		},
	}

	if err := c.validator.Struct(&newEntry); err != nil {
		return models.Task{}, goutils.NewValidationError(
			fmt.Sprintf("new one shot task '%s' entry is not valid", params.Name), err, true,
		)
	}

	if tmp := c.db.Create(&newEntry); tmp.Error != nil {
		return models.Task{}, models.NewSQLError(
			fmt.Sprintf("new one shot task '%s' insert failed", params.Name), tmp.Error, true,
		)
	}

	return newEntry.Task, nil
}

/*
DefineNewScheduledOneShotTask define a new one-shot task for scheduled execution

	@param ctx context.Context - execution context
	@param params NewTaskParameter - new task parameters
	@param targetRuntime time.Time - target time when the task should run
	@returns task instance
*/
func (c *databaseImpl) DefineNewScheduledOneShotTask(
	_ context.Context, params NewTaskParameter, targetRuntime time.Time,
) (models.Task, error) {
	if params.Parameters != nil {
		if err := c.validator.Struct(params.Parameters); err != nil {
			return models.Task{}, goutils.NewValidationError(
				fmt.Sprintf("new scheduled one shot task '%s' parameters entry is not valid", params.Name),
				err, true,
			)
		}
	}
	if params.Metadata != nil {
		if err := c.validator.Struct(params.Metadata); err != nil {
			return models.Task{}, goutils.NewValidationError(
				fmt.Sprintf("new scheduled one shot task '%s' metadata entry is not valid", params.Name),
				err, true,
			)
		}
	}

	parametersStr, _ := json.Marshal(&params.Parameters)
	metadataStr, _ := json.Marshal(&params.Metadata)

	newEntry := taskEntry{
		Task: models.Task{
			ID:                ulid.Make().String(),
			TaskName:          params.Name,
			Creator:           params.Creator,
			TaskScheduleClass: models.TaskScheduleClassScheduledOneShot,
			TaskState:         models.TaskStatePending,
			Parameters:        datatypes.JSON(parametersStr),
			Metadata:          datatypes.JSON(metadataStr),
			TargetRunTime:     &targetRuntime,
			Deadline:          params.Deadline,
			RetryParams:       params.RetryParam,
		},
	}

	if err := c.validator.Struct(&newEntry); err != nil {
		return models.Task{}, goutils.NewValidationError(
			fmt.Sprintf("new scheduled one shot task '%s' entry is not valid", params.Name), err, true,
		)
	}

	if tmp := c.db.Create(&newEntry); tmp.Error != nil {
		return models.Task{}, models.NewSQLError(
			fmt.Sprintf("new scheduled one shot task '%s' insert failed", params.Name), tmp.Error, true,
		)
	}

	return newEntry.Task, nil
}

// getTaskDBEntry helper function to fetch one task
func (c *databaseImpl) getTaskDBEntry(taskID string) (taskEntry, error) {
	var entry taskEntry
	tmp := c.db.Model(&taskEntry{}).Where("id = ?", taskID).First(&entry)
	return entry, notFoundOrError(tmp.Error, "task", taskID)
}

/*
GetTask fetch task by ID

	@param ctx context.Context - execution context
	@param taskID string - task ID
	@returns the task entry
*/
func (c *databaseImpl) GetTask(_ context.Context, taskID string) (models.Task, error) {
	entry, err := c.getTaskDBEntry(taskID)
	return entry.Task, err
}

// updateTaskState update task state
func (c *databaseImpl) updateTaskState(
	ctx context.Context, taskID string, nextState models.TaskStateENUM,
) error {
	entry, err := c.getTaskDBEntry(taskID)
	if err != nil {
		return err
	}

	if err := entry.ValidNextState(nextState); err != nil {
		return goutils.NewConsistencyError(
			fmt.Sprintf("task %s can't transition to '%s'", taskID, nextState), err, true,
		)
	}

	tmp := c.db.
		Model(&taskEntry{}).
		Where("id = ?", entry.ID).
		UpdateColumn("state", nextState)
	if tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to record task %s with new state '%s'", taskID, nextState),
			tmp.Error, true,
		)
	}

	// Record task state change event
	eventTypeMap := map[models.TaskStateENUM]models.SystemEventTypeENUM{
		models.TaskStateActive:    models.SystemEventTypeActivateTask,
		models.TaskStateComplete:  models.SystemEventTypeCompleteTask,
		models.TaskStateFailed:    models.SystemEventTypeFailedTask,
		models.TaskStateCancelled: models.SystemEventTypeCancelledTask,
		models.TaskStateTimeout:   models.SystemEventTypeTimedOutTask,
	}
	if eventType, found := eventTypeMap[nextState]; found {
		if _, err := c.defineNewSystemEvent(
			ctx, eventType, &models.SystemEventTaskEvents{TaskID: taskID, Creator: entry.Creator},
		); err != nil {
			return models.NewSQLError(
				fmt.Sprintf(
					"failed to record task %s change state to '%s' system event", taskID, nextState,
				), err, true,
			)
		}
	}

	return nil
}

/*
MarkTaskActive mark a task as active

	@param ctx context.Context - execution context
	@param taskID string - task ID
*/
func (c *databaseImpl) MarkTaskActive(ctx context.Context, taskID string) error {
	return c.updateTaskState(ctx, taskID, models.TaskStateActive)
}

/*
MarkTaskComplete mark a task as complete

	@param ctx context.Context - execution context
	@param taskID string - task ID
*/
func (c *databaseImpl) MarkTaskComplete(ctx context.Context, taskID string) error {
	return c.updateTaskState(ctx, taskID, models.TaskStateComplete)
}

/*
MarkTaskFailed mark a task as failed

	@param ctx context.Context - execution context
	@param taskID string - task ID
*/
func (c *databaseImpl) MarkTaskFailed(ctx context.Context, taskID string) error {
	return c.updateTaskState(ctx, taskID, models.TaskStateFailed)
}

/*
MarkTaskCancelling mark a task as cancelling

	@param ctx context.Context - execution context
	@param taskID string - task ID
*/
func (c *databaseImpl) MarkTaskCancelling(ctx context.Context, taskID string) error {
	return c.updateTaskState(ctx, taskID, models.TaskStateCancelling)
}

/*
MarkTaskCancelled mark a task as cancelled

	@param ctx context.Context - execution context
	@param taskID string - task ID
*/
func (c *databaseImpl) MarkTaskCancelled(ctx context.Context, taskID string) error {
	return c.updateTaskState(ctx, taskID, models.TaskStateCancelled)
}

/*
MarkTaskTimedOut mark a task as timed out

	@param ctx context.Context - execution context
	@param taskID string - task ID
*/
func (c *databaseImpl) MarkTaskTimedOut(ctx context.Context, taskID string) error {
	return c.updateTaskState(ctx, taskID, models.TaskStateTimeout)
}

/*
UpdateTaskDeadline set deadline for a task

	@param ctx context.Context - execution context
	@param taskID string - task ID
	@param deadline time.Time - task deadline
*/
func (c *databaseImpl) UpdateTaskDeadline(
	_ context.Context, taskID string, deadline time.Time,
) error {
	entry, err := c.getTaskDBEntry(taskID)
	if err != nil {
		return err
	}

	tmp := c.db.Model(&taskEntry{}).Where("id = ?", entry.ID).UpdateColumn("deadline", &deadline)
	if tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to update task %s deadline", taskID), tmp.Error, true,
		)
	}

	return nil
}

/*
ListTasks list tasks

	@param ctx context.Context - execution context
	@param filters TaskQueryFilter - query filtering conditions
	@returns list of tasks
*/
func (c *databaseImpl) ListTasks(
	_ context.Context, filters TaskQueryFilter,
) ([]models.Task, error) {
	if err := c.validator.Struct(&filters); err != nil {
		return nil, goutils.NewValidationError("task query filter is not valid", err, true)
	}

	query := c.db.Model(&taskEntry{})

	if len(filters.TargetIDs) > 0 {
		query = query.Where("id in ?", filters.TargetIDs)
	}

	if len(filters.TaskNames) > 0 {
		query = query.Where("name in ?", filters.TaskNames)
	}

	if len(filters.TaskScheduleClasses) > 0 {
		query = query.Where("schedule_class in ?", filters.TaskScheduleClasses)
	}

	if len(filters.TaskStates) > 0 {
		query = query.Where("state in ?", filters.TaskStates)
	}

	if filters.TargetDeadline != nil {
		query = query.Where("(deadline is not null and deadline <= ?)", *filters.TargetDeadline)
	}

	if filters.Limit != nil {
		query = query.Limit(*filters.Limit)
	}
	if filters.Offset != nil {
		query = query.Offset(*filters.Offset)
	}

	query = query.Order("created_at")

	var entries []taskEntry
	if tmp := query.Find(&entries); tmp.Error != nil {
		return nil, models.NewSQLError("failed to list tasks", tmp.Error, true)
	}

	result := []models.Task{}
	for _, entry := range entries {
		result = append(result, entry.Task)
	}

	return result, nil
}

/*
DeleteTask delete task entry

	@param ctx context.Context - execution context
	@param taskID string - task ID
*/
func (c *databaseImpl) DeleteTask(ctx context.Context, taskID string) error {
	entry, err := c.getTaskDBEntry(taskID)
	if err != nil {
		return err
	}

	// Refuse to delete a task that is executing a workflow step. Such a task is the
	// workflow's execution-history store, so it must leave only WITH its workflow (see the
	// workflow DESIGN's "Failure history and its retention"). The privileged workflow-teardown
	// path (DeleteWorkflow) reaps these tasks directly, bypassing this guard.
	var linkCount int64
	if tmp := c.db.
		Model(&workflowStepRunnerTask{}).
		Where("task_id = ?", taskID).
		Count(&linkCount); tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to check workflow-step linkage of task %s", taskID), tmp.Error, true,
		)
	}
	if linkCount > 0 {
		return goutils.NewConsistencyError(
			fmt.Sprintf(
				"task %s is linked to a workflow step and cannot be deleted directly; "+
					"delete its workflow instead", taskID,
			), nil, true,
		)
	}

	tmp := c.db.Where("id = ?", entry.ID).Delete(&taskEntry{})
	if tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to delete task %s", taskID), tmp.Error, true,
		)
	}

	// Record delete event
	if _, err := c.defineNewSystemEvent(
		ctx,
		models.SystemEventTypeDeleteTask,
		&models.SystemEventTaskEvents{TaskID: taskID, Creator: entry.Creator},
	); err != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to record delete task %s system event", taskID), err, true,
		)
	}

	return nil
}

// ======================================================================================
// Task execution instance

/*
DefineNewTaskExecInstance define a new execution instance for a task

	@param ctx context.Context - execution context
	@param task models.Task - the task entry
	@return new task exec instance
*/
func (c *databaseImpl) DefineNewTaskExecInstance(
	_ context.Context, task models.Task,
) (models.TaskExecution, error) {
	newEntry := taskExecutionEntry{
		TaskExecution: models.TaskExecution{
			ID:       ulid.Make().String(),
			TaskID:   task.ID,
			Deadline: task.Deadline,
		},
	}

	// Fill in information based on the task type
	switch task.TaskScheduleClass {
	case models.TaskScheduleClassImmediateOneShot:
		newEntry.ExecutionClass = models.TaskExecutionClassImmediate
		newEntry.ExecutionState = models.TaskExecutionStateDefined
	case models.TaskScheduleClassScheduledOneShot:
		newEntry.ExecutionClass = models.TaskExecutionClassScheduled
		newEntry.ExecutionState = models.TaskExecutionStateScheduled
		newEntry.TargetEnqueueTime = task.TargetRunTime
	default:
		return models.TaskExecution{}, goutils.NewBadInputError(
			fmt.Sprintf(
				"parent task %s has invalid scheduling class %s", task.ID, task.TaskScheduleClass,
			), nil, true,
		)
	}

	if err := c.validator.Struct(&newEntry); err != nil {
		return models.TaskExecution{}, goutils.NewValidationError(
			fmt.Sprintf("new task %s exec instance entry is not valid", task.ID), err, true,
		)
	}

	if tmp := c.db.Create(&newEntry); tmp.Error != nil {
		return models.TaskExecution{}, models.NewSQLError(
			fmt.Sprintf("new task %s exec instance entry insert failed", task.ID), tmp.Error, true,
		)
	}

	return newEntry.TaskExecution, nil
}

/*
DefineNewTaskRetryExecInstance define a new retry execution instance for a task

	@param ctx context.Context - execution context
	@param task models.Task - the task entry
	@param failedEntry models.TaskExecution - the failed task execution instance
	@param targetRunTime time.Time - target runtime for this retry
	@return new task exec instance
*/
func (c *databaseImpl) DefineNewTaskRetryExecInstance(
	_ context.Context,
	task models.Task,
	failedEntry models.TaskExecution,
	targetRunTime time.Time,
) (models.TaskExecution, error) {
	newEntry := taskExecutionEntry{
		TaskExecution: models.TaskExecution{
			ID:                     ulid.Make().String(),
			TaskID:                 task.ID,
			ExecutionClass:         models.TaskExecutionClassRetry,
			ExecutionState:         models.TaskExecutionStateScheduled,
			TargetEnqueueTime:      &targetRunTime,
			RetryParentExecutionID: &failedEntry.ID,
			Deadline:               task.Deadline,
		},
	}

	if err := c.validator.Struct(&newEntry); err != nil {
		return models.TaskExecution{}, goutils.NewValidationError(
			fmt.Sprintf("new task %s retry exec instance entry is not valid", task.ID), err, true,
		)
	}

	if tmp := c.db.Create(&newEntry); tmp.Error != nil {
		return models.TaskExecution{}, models.NewSQLError(
			fmt.Sprintf("new task %s retry exec instance entry insert failed", task.ID), tmp.Error, true,
		)
	}

	return newEntry.TaskExecution, nil
}

// getTaskExecDBEntry helper function to fetch one task exec instance
func (c *databaseImpl) getTaskExecDBEntry(instanceID string) (taskExecutionEntry, error) {
	var entry taskExecutionEntry
	tmp := c.db.Model(&taskExecutionEntry{}).Where("id = ?", instanceID).First(&entry)
	return entry, notFoundOrError(tmp.Error, "task execution", instanceID)
}

/*
GetTaskExecution fetch task exec by ID

	@param ctx context.Context - execution context
	@param instanceID string - task exec instance ID
	@returns task exec instance entry
*/
func (c *databaseImpl) GetTaskExecution(
	_ context.Context, instanceID string,
) (models.TaskExecution, error) {
	entry, err := c.getTaskExecDBEntry(instanceID)
	return entry.TaskExecution, err
}

/*
ListAllExecutions list task execution instances

	@param ctx context.Context - execution context
	@param filters TaskExecutionQueryFilter - query filtering conditions
	@returns list of task executions
*/
func (c *databaseImpl) ListAllExecutions(
	_ context.Context, filters TaskExecutionQueryFilter,
) ([]models.TaskExecution, error) {
	if err := c.validator.Struct(&filters); err != nil {
		return nil,
			goutils.NewValidationError("task execution instance query filter is not valid", err, true)
	}

	query := c.db.Model(&taskExecutionEntry{})

	if filters.ParentTaskID != nil {
		query = query.Where("task_id = ?", *filters.ParentTaskID)
	}

	if filters.ExecutionWorkerName != nil {
		query = query.Where("worker_name = ?", *filters.ExecutionWorkerName)
	}

	if len(filters.ExecClasses) > 0 {
		query = query.Where("execution_class in ?", filters.ExecClasses)
	}

	if len(filters.ExecStates) > 0 {
		query = query.Where("state in ?", filters.ExecStates)
	}

	if len(filters.TerminalStates) > 0 {
		query = query.Where("terminal_state in ?", filters.TerminalStates)
	}

	if filters.TargetDeadline != nil {
		query = query.Where("(deadline is not null and deadline <= ?)", *filters.TargetDeadline)
	}

	if filters.TargetStart != nil {
		query = query.Where("(execute_at is not null and execute_at <= ?)", *filters.TargetStart)
	}

	if filters.Limit != nil {
		query = query.Limit(*filters.Limit)
	}
	if filters.Offset != nil {
		query = query.Offset(*filters.Offset)
	}

	query = query.Order("created_at")

	var entries []taskExecutionEntry
	if tmp := query.Find(&entries); tmp.Error != nil {
		return nil, models.NewSQLError("failed to list task execution instances", tmp.Error, true)
	}

	result := []models.TaskExecution{}
	for _, entry := range entries {
		result = append(result, entry.TaskExecution)
	}

	return result, nil
}

/*
ListTaskExecutions list task execution instances of a particular task

	@param ctx context.Context - execution context
	@param taskID string - parent task
	@param filters TaskExecutionQueryFilter - query filtering conditions
	@returns list of task executions
*/
func (c *databaseImpl) ListTaskExecutions(
	ctx context.Context, taskID string, filters TaskExecutionQueryFilter,
) ([]models.TaskExecution, error) {
	filters.ParentTaskID = &taskID
	return c.ListAllExecutions(ctx, filters)
}

// updateTaskExecutionState helper function to update execution state
func (c *databaseImpl) updateTaskExecutionState(
	instanceID string,
	nextState models.TaskExecutionStateENUM,
	workerName *string,
	errorMsg *string,
	disposition *models.TaskFailureDispositionENUM,
	terminatedAt *time.Time,
) error {
	entry, err := c.getTaskExecDBEntry(instanceID)
	if err != nil {
		return err
	}

	if err := entry.ValidNextState(nextState); err != nil {
		return goutils.NewConsistencyError(
			fmt.Sprintf("task exec instance %s can't transition to '%s'", instanceID, nextState),
			err, true,
		)
	}

	theUpdate := map[string]any{"state": nextState}
	switch nextState {
	// Record the worker that acquired the task
	case models.TaskExecutionStateAcquired:
		if workerName == nil {
			return goutils.NewConsistencyError(
				fmt.Sprintf(
					"task exec instance %s can't transition to '%s' without worker name",
					instanceID, nextState,
				), nil, true,
			)
		}
		theUpdate["worker_name"] = workerName

	// Record the terminal outcome. Processed / Failed / Cancelled are the states an
	// instance can reach before finalization; capture which one and when so the
	// outcome survives the later transition to FINALIZED.
	case models.TaskExecutionStateProcessed:
		theUpdate["terminal_state"] = nextState
		theUpdate["terminated_at"] = terminatedAt

	case models.TaskExecutionStateFailed:
		theUpdate["error_msg"] = errorMsg
		theUpdate["terminal_state"] = nextState
		theUpdate["terminated_at"] = terminatedAt
		// Persist the retry disposition (nil = retryable) so the scheduler's maintenance
		// backstop honors a NON_RETRYABLE outcome even if the EXECUTE_FAILED IPC is lost.
		theUpdate["failure_disposition"] = disposition

	case models.TaskExecutionStateCancelled:
		theUpdate["error_msg"] = errorMsg
		theUpdate["terminal_state"] = nextState
		theUpdate["terminated_at"] = terminatedAt
	}

	tmp := c.db.Model(&taskExecutionEntry{}).Where("id = ?", entry.ID).Updates(theUpdate)
	if tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to update task exec instance %s state", instanceID), tmp.Error, true,
		)
	}

	return nil
}

/*
MarkTaskExecQueued mark a task execution instance is enqueued

	@param ctx context.Context - execution context
	@param instanceID string - task exec instance ID
*/
func (c *databaseImpl) MarkTaskExecQueued(_ context.Context, instanceID string) error {
	return c.updateTaskExecutionState(
		instanceID, models.TaskExecutionStateEnqueued, nil, nil, nil, nil,
	)
}

/*
MarkTaskExecAcquired mark a task execution instance is acquired by a worker

	@param ctx context.Context - execution context
	@param instanceID string - task exec instance ID
	@param workerName string - worker name
*/
func (c *databaseImpl) MarkTaskExecAcquired(
	_ context.Context, instanceID string, workerName string,
) error {
	return c.updateTaskExecutionState(
		instanceID, models.TaskExecutionStateAcquired, &workerName, nil, nil, nil,
	)
}

/*
MarkTaskExecProcessing mark a task execution instance is being processed

	@param ctx context.Context - execution context
	@param instanceID string - task exec instance ID
*/
func (c *databaseImpl) MarkTaskExecProcessing(_ context.Context, instanceID string) error {
	return c.updateTaskExecutionState(
		instanceID, models.TaskExecutionStateProcessing, nil, nil, nil, nil,
	)
}

/*
MarkTaskExecProcessed mark a task execution instance is processed

	@param ctx context.Context - execution context
	@param instanceID string - task exec instance ID
	@param terminatedAt time.Time - timestamp when the instance reached this terminal state
*/
func (c *databaseImpl) MarkTaskExecProcessed(
	_ context.Context, instanceID string, terminatedAt time.Time,
) error {
	return c.updateTaskExecutionState(
		instanceID, models.TaskExecutionStateProcessed, nil, nil, nil, &terminatedAt,
	)
}

/*
MarkTaskExecFailed mark a task execution instance is failed to process

	@param ctx context.Context - execution context
	@param instanceID string - task exec instance ID
	@param errorMsg string - error message associated with the failure
	@param disposition *models.TaskFailureDispositionENUM - whether the failure is retryable
	    (nil = retryable)
	@param terminatedAt time.Time - timestamp when the instance reached this terminal state
*/
func (c *databaseImpl) MarkTaskExecFailed(
	_ context.Context,
	instanceID string,
	errorMsg string,
	disposition *models.TaskFailureDispositionENUM,
	terminatedAt time.Time,
) error {
	return c.updateTaskExecutionState(
		instanceID, models.TaskExecutionStateFailed, nil, &errorMsg, disposition, &terminatedAt,
	)
}

/*
MarkTaskExecFinalized mark a task execution instance is finalized

	@param ctx context.Context - execution context
	@param instanceID string - task exec instance ID
*/
func (c *databaseImpl) MarkTaskExecFinalized(_ context.Context, instanceID string) error {
	return c.updateTaskExecutionState(
		instanceID, models.TaskExecutionStateFinalized, nil, nil, nil, nil,
	)
}

/*
MarkTaskExecCancelled mark a task execution instance is cancelled

	@param ctx context.Context - execution context
	@param instanceID string - task exec instance ID
	@param cancelMsg string - cancellation message associated with the failure
	@param terminatedAt time.Time - timestamp when the instance reached this terminal state
*/
func (c *databaseImpl) MarkTaskExecCancelled(
	_ context.Context, instanceID string, cancelMsg string, terminatedAt time.Time,
) error {
	return c.updateTaskExecutionState(
		instanceID, models.TaskExecutionStateCancelled, nil, &cancelMsg, nil, &terminatedAt,
	)
}
