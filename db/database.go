// Package db - database controllers for system persistence
package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"
)

// CommonListEntryQueryFilter common query filter when listing data entries
type CommonListEntryQueryFilter struct {
	Limit  *int `validate:"omitempty,gt=0"`
	Offset *int `validate:"omitempty,gte=0"`
}

// SystemEventQueryFilter audit event query filter conditions
type SystemEventQueryFilter struct {
	CommonListEntryQueryFilter
	// EventTypes the specific event types to query for
	EventTypes []models.SystemEventTypeENUM `validate:"omitempty,dive,system_event_type"`
	// EventsAfter filter for events after this timestamp
	EventsAfter *time.Time
	// EventsBefore filter for events before this timestamp
	EventsBefore *time.Time
}

// TaskQueryFilter query filter conditions to list tasks
type TaskQueryFilter struct {
	CommonListEntryQueryFilter
	// TargetIDs the specific task ID set to query for
	TargetIDs []string
	// TaskNames the specific task purpose names to query for
	TaskNames []string
	// TaskScheduleClasses the specific task schedule classes to query for
	TaskScheduleClasses []models.TaskScheduleClassENUM `validate:"omitempty,dive,task_schedule_class"`
	// TaskStates the specific task states to query for
	TaskStates []models.TaskStateENUM `validate:"omitempty,dive,task_state"`
	// TargetDeadline tasks with deadline set before this timestamp
	TargetDeadline *time.Time
}

// TaskExecutionQueryFilter query filter conditions to list task executions
type TaskExecutionQueryFilter struct {
	CommonListEntryQueryFilter
	// ParentTaskID fetch task execution instances belonging to this parent
	ParentTaskID *string
	// ExecutionWorkerName fetch task execution instances acquired by this worker
	ExecutionWorkerName *string
	// ExecClasses the specific execution classes to query for
	ExecClasses []models.TaskExecutionClassENUM `validate:"omitempty,dive,task_execute_class"`
	// ExecStates the specific execution states to query for
	ExecStates []models.TaskExecutionStateENUM `validate:"omitempty,dive,task_execute_state"`
	// TerminalStates the specific terminal states to query for
	TerminalStates []models.TaskExecutionStateENUM `validate:"omitempty,dive,task_execute_state"`
	// TargetDeadline execution instances with deadline set before this timestamp
	TargetDeadline *time.Time
	// TargetStart execution instances set to start before this timestamp
	TargetStart *time.Time
}

// NewTaskParameter new task parameters
type NewTaskParameter struct {
	// Name task name, used to indicate which execution processor should pick up the work
	Name string
	// Parameters task processing parameters
	Parameters interface{}
	// Metadata associated metadata
	Metadata interface{}
	// RetryParam retry parameter in case of failure
	RetryParam models.TaskRetryParameters
	// Deadline if specified, the task must complete by this dead line.
	Deadline *time.Time
}

// Database the database handle to interacting with the data base.
//
// All methods must be invoked within a transaction. A `Database` instance can only be
// obtained via `Client.UseDatabaseInTransaction`, which runs the caller's logic inside a
// transaction; there is no way to construct one outside of that scope. Consequently each
// method may assume it is already running in a transaction, and multi-statement operations
// (e.g. a state change plus its audit event) are committed or rolled back atomically.
type Database interface {
	// ------------------------------------------------------------------------------------
	// Audit

	/*
		ListSystemEvents list captured system events

			@param ctx context.Context - execution context
			@param filters SystemEventQueryFilter - entry listing filter
			@return list of system events
	*/
	ListSystemEvents(
		ctx context.Context, filters SystemEventQueryFilter,
	) ([]models.SystemEventAudit, error)

	/*
		RecordInvalidTaskIPCMessage record an audit event for a task IPC message that could not
		be processed (unreadable, unparsable, or of an unsupported/unknown type).

			@param ctx context.Context - execution context
			@param receiver string - name of the IPC receiver that rejected the message
			@param rawMessage string - the raw message payload, if it was readable
			@param reason string - human-readable reason the message was rejected
	*/
	RecordInvalidTaskIPCMessage(ctx context.Context, receiver, rawMessage, reason string) error

	// ------------------------------------------------------------------------------------
	// Task

	/*
		DefineNewOneShotTask define a new one-shot task for immediate execution

			@param ctx context.Context - execution context
			@param params NewTaskParameter - new task parameters
			@returns task instance
	*/
	DefineNewOneShotTask(ctx context.Context, params NewTaskParameter) (models.Task, error)

	/*
		DefineNewScheduledOneShotTask define a new one-shot task for scheduled execution

			@param ctx context.Context - execution context
			@param params NewTaskParameter - new task parameters
			@param targetRuntime time.Time - target time when the task should run
			@returns task instance
	*/
	DefineNewScheduledOneShotTask(
		ctx context.Context, params NewTaskParameter, targetRuntime time.Time,
	) (models.Task, error)

	/*
		GetTask fetch task by ID

			@param ctx context.Context - execution context
			@param taskID string - task ID
			@returns the task entry
	*/
	GetTask(ctx context.Context, taskID string) (models.Task, error)

	/*
		MarkTaskActive mark a task as active

			@param ctx context.Context - execution context
			@param taskID string - task ID
	*/
	MarkTaskActive(ctx context.Context, taskID string) error

	/*
		MarkTaskComplete mark a task as complete

			@param ctx context.Context - execution context
			@param taskID string - task ID
	*/
	MarkTaskComplete(ctx context.Context, taskID string) error

	/*
		MarkTaskFailed mark a task as failed

			@param ctx context.Context - execution context
			@param taskID string - task ID
	*/
	MarkTaskFailed(ctx context.Context, taskID string) error

	/*
		MarkTaskCancelling mark a task as cancelling

			@param ctx context.Context - execution context
			@param taskID string - task ID
	*/
	MarkTaskCancelling(ctx context.Context, taskID string) error

	/*
		MarkTaskCancelled mark a task as cancelled

			@param ctx context.Context - execution context
			@param taskID string - task ID
	*/
	MarkTaskCancelled(ctx context.Context, taskID string) error

	/*
		ListTasks list tasks

			@param ctx context.Context - execution context
			@param filters TaskQueryFilter - query filtering conditions
			@returns list of tasks
	*/
	ListTasks(ctx context.Context, filters TaskQueryFilter) ([]models.Task, error)

	/*
		MarkTaskTimedOut mark a task as timed out

			@param ctx context.Context - execution context
			@param taskID string - task ID
	*/
	MarkTaskTimedOut(ctx context.Context, taskID string) error

	/*
		UpdateTaskDeadline set deadline for a task

			@param ctx context.Context - execution context
			@param taskID string - task ID
			@param deadline time.Time - task deadline
	*/
	UpdateTaskDeadline(ctx context.Context, taskID string, deadline time.Time) error

	/*
		DeleteTask delete task entry

			@param ctx context.Context - execution context
			@param taskID string - task ID
	*/
	DeleteTask(ctx context.Context, taskID string) error

	// ------------------------------------------------------------------------------------
	// Task execution instance

	/*
		DefineNewTaskExecInstance define a new execution instance for a task

			@param ctx context.Context - execution context
			@param task models.Task - the task entry
			@return new task exec instance
	*/
	DefineNewTaskExecInstance(ctx context.Context, task models.Task) (models.TaskExecution, error)

	/*
		DefineNewTaskRetryExecInstance define a new retry execution instance for a task

			@param ctx context.Context - execution context
			@param task models.Task - the task entry
			@param failedEntry models.TaskExecution - the failed task execution instance
			@param targetRunTime time.Time - target runtime for this retry
			@return new task exec instance
	*/
	DefineNewTaskRetryExecInstance(
		ctx context.Context,
		task models.Task,
		failedEntry models.TaskExecution,
		targetRunTime time.Time,
	) (models.TaskExecution, error)

	/*
		GetTaskExecution fetch task exec by ID

			@param ctx context.Context - execution context
			@param instanceID string - task exec instance ID
			@returns task exec instance entry
	*/
	GetTaskExecution(ctx context.Context, instanceID string) (models.TaskExecution, error)

	/*
		ListAllExecutions list task execution instances

			@param ctx context.Context - execution context
			@param filters TaskExecutionQueryFilter - query filtering conditions
			@returns list of task executions
	*/
	ListAllExecutions(
		ctx context.Context, filters TaskExecutionQueryFilter,
	) ([]models.TaskExecution, error)

	/*
		ListTaskExecutions list task execution instances of a particular task

			@param ctx context.Context - execution context
			@param taskID string - parent task
			@param filters TaskExecutionQueryFilter - query filtering conditions
			@returns list of task executions
	*/
	ListTaskExecutions(
		ctx context.Context, taskID string, filters TaskExecutionQueryFilter,
	) ([]models.TaskExecution, error)

	/*
		MarkTaskExecQueued mark a task execution instance is enqueued

			@param ctx context.Context - execution context
			@param instanceID string - task exec instance ID
	*/
	MarkTaskExecQueued(ctx context.Context, instanceID string) error

	/*
		MarkTaskExecAcquired mark a task execution instance is acquired by a worker

			@param ctx context.Context - execution context
			@param instanceID string - task exec instance ID
			@param workerName string - worker name
	*/
	MarkTaskExecAcquired(ctx context.Context, instanceID string, workerName string) error

	/*
		MarkTaskExecProcessing mark a task execution instance is being processed

			@param ctx context.Context - execution context
			@param instanceID string - task exec instance ID
	*/
	MarkTaskExecProcessing(ctx context.Context, instanceID string) error

	/*
		MarkTaskExecProcessed mark a task execution instance is processed

			@param ctx context.Context - execution context
			@param instanceID string - task exec instance ID
			@param terminatedAt time.Time - timestamp when the instance reached this terminal state
	*/
	MarkTaskExecProcessed(ctx context.Context, instanceID string, terminatedAt time.Time) error

	/*
		MarkTaskExecFailed mark a task execution instance is failed to process

			@param ctx context.Context - execution context
			@param instanceID string - task exec instance ID
			@param errorMsg string - error message associated with the failure
			@param terminatedAt time.Time - timestamp when the instance reached this terminal state
	*/
	MarkTaskExecFailed(
		ctx context.Context, instanceID string, errorMsg string, terminatedAt time.Time,
	) error

	/*
		MarkTaskExecFinalized mark a task execution instance is finalized

			@param ctx context.Context - execution context
			@param instanceID string - task exec instance ID
	*/
	MarkTaskExecFinalized(ctx context.Context, instanceID string) error

	/*
		MarkTaskExecCancelled mark a task execution instance is cancelled

			@param ctx context.Context - execution context
			@param instanceID string - task exec instance ID
			@param cancelMsg string - cancellation message associated with the failure
			@param terminatedAt time.Time - timestamp when the instance reached this terminal state
	*/
	MarkTaskExecCancelled(
		ctx context.Context, instanceID string, cancelMsg string, terminatedAt time.Time,
	) error
}

// databaseImpl implements Database
type databaseImpl struct {
	goutils.Component
	db        *gorm.DB
	validator *validator.Validate
}

// newDatabase define a new database client
func newDatabase(_ context.Context, sqlClient *gorm.DB) (Database, error) {
	logTags := log.Fields{"package": "tasking", "module": "db", "component": "db-client"}

	instance := &databaseImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		db:        sqlClient,
		validator: validator.New(),
	}

	if err := models.RegisterWithValidator(instance.validator); err != nil {
		return nil, goutils.NewRuntimeError("failed to install custom validation macros", err, true)
	}

	return instance, nil
}

// notFoundOrError translates the error returned by a single-entry fetch into a
// goutils.NotFoundError when GORM reports that the record does not exist. Any other
// error is returned unchanged, and a nil error stays nil.
func notFoundOrError(err error, entity, id string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return goutils.NewNotFoundError(
			fmt.Sprintf("%s '%s' does not exist", entity, id), err, true,
		)
	}
	return models.NewSQLError(fmt.Sprintf("failed to fetch %s '%s'", entity, id), err, true)
}
