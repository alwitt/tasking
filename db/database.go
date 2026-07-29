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
	// OnlyNotBroadcast when true, return only events not yet broadcast (broadcast_at IS
	// NULL) — the notification producer's poll for its work queue.
	OnlyNotBroadcast bool
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
	// Creator opaque identity of the entity creating the task
	Creator string
	// Parameters task processing parameters
	Parameters interface{}
	// Metadata associated metadata
	Metadata interface{}
	// RetryParam retry parameter in case of failure
	RetryParam models.TaskRetryParameters
	// Deadline if specified, the task must complete by this dead line.
	Deadline *time.Time
}

// WorkflowQueryFilter query filter conditions to list workflow
type WorkflowQueryFilter struct {
	CommonListEntryQueryFilter
	// TargetIDs target specific set of workflows to query for
	TargetIDs []string
	// TargetNames target specific set of workflow names to query for
	TargetNames []string
	// TargetStates target workflow states to query for
	TargetStates []models.WorkflowStateENUM `validate:"omitempty,dive,workflow_state"`
	// TargetDeadline workflows with a deadline at or before this timestamp
	TargetDeadline *time.Time
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
		MarkSystemEventsBroadcast stamp a set of audit events as broadcast by the
		notification producer. The broadcast_at IS NULL guard keeps the stamp idempotent:
		re-stamping (or a concurrent producer) is a no-op, not an overwrite.

			@param ctx context.Context - execution context
			@param eventIDs []string - IDs of the audit events to stamp
			@param broadcastAt time.Time - broadcast timestamp to record
	*/
	MarkSystemEventsBroadcast(ctx context.Context, eventIDs []string, broadcastAt time.Time) error

	/*
		RecordInvalidTaskIPCMessage record an audit event for a task IPC message that could not
		be processed (unreadable, unparsable, or of an unsupported/unknown type).

			@param ctx context.Context - execution context
			@param receiver string - name of the IPC receiver that rejected the message
			@param rawMessage string - the raw message payload, if it was readable
			@param reason string - human-readable reason the message was rejected
	*/
	RecordInvalidTaskIPCMessage(ctx context.Context, receiver, rawMessage, reason string) error

	/*
		RecordTaskEngineFailure record an audit event for a task whose execution instance the
		core task engine failed to operate on (e.g. the receiver could not claim it, or could
		not submit it to the executor).

			@param ctx context.Context - execution context
			@param task models.Task - the task that was failed (supplies its ID and creator)
			@param instanceID string - ID of the execution instance the engine failed to operate on
			@param reason string - human-readable reason the engine reported the failure
	*/
	RecordTaskEngineFailure(ctx context.Context, task models.Task, instanceID, reason string) error

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
		DeleteTask delete task entry.

		A task that is linked to a workflow step (via workflow_step_runner_tasks) is the
		workflow's execution-history store and is refused: it can only be deleted as part of
		deleting its workflow (see DeleteWorkflow).

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
			@param disposition *models.TaskFailureDispositionENUM - whether the failure is retryable
			    (nil = retryable)
			@param terminatedAt time.Time - timestamp when the instance reached this terminal state
	*/
	MarkTaskExecFailed(
		ctx context.Context,
		instanceID string,
		errorMsg string,
		disposition *models.TaskFailureDispositionENUM,
		terminatedAt time.Time,
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

	// ------------------------------------------------------------------------------------
	// Workflow

	/*
		DefineNewWorkflow define a new workflow

			@param ctx context.Context - execution context
			@param workflowSpec models.NewWorkflowParameter - the workflow specification
			@param creator string - the entity defining the workflow
			@returns new workflow entry
	*/
	DefineNewWorkflow(
		ctx context.Context, workflowSpec models.NewWorkflowParameter, creator string,
	) (models.Workflow, error)

	/*
		GetWorkflow fetch a workflow entry

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@returns workflow entry
	*/
	GetWorkflow(ctx context.Context, workflowID string) (models.Workflow, error)

	/*
		MarkWorkflowPending mark workflow is pending execution

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowPending(ctx context.Context, workflowID string, timestamp time.Time) error

	/*
		MarkWorkflowRunning mark workflow is running

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowRunning(ctx context.Context, workflowID string, timestamp time.Time) error

	/*
		MarkWorkflowComplete mark workflow is complete

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowComplete(ctx context.Context, workflowID string, timestamp time.Time) error

	/*
		MarkWorkflowFailed mark workflow has failed

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowFailed(ctx context.Context, workflowID string, timestamp time.Time) error

	/*
		MarkWorkflowTimedOut mark workflow has timed out

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowTimedOut(ctx context.Context, workflowID string, timestamp time.Time) error

	/*
		MarkWorkflowCancelling mark workflow is being cancelled

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowCancelling(ctx context.Context, workflowID string, timestamp time.Time) error

	/*
		MarkWorkflowCancelled mark workflow is cancelled

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowCancelled(ctx context.Context, workflowID string, timestamp time.Time) error

	/*
		ListWorkflows list workflows

			@param ctx context.Context - execution context
			@param filters WorkflowQueryFilter - query filtering conditions
			@returns list of workflows
	*/
	ListWorkflows(ctx context.Context, filters WorkflowQueryFilter) ([]models.Workflow, error)

	/*
		DeleteWorkflow delete a terminal workflow and reap the tasks that executed its steps.

		Only a terminal workflow (COMPLETE / CANCELLED) may be deleted. Deleting it reaps the
		tasks linked to its steps and their task_executions history (a workflow-owned task never
		outlives its workflow); this is the privileged path that bypasses the DeleteTask linkage
		guard.

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
	*/
	DeleteWorkflow(ctx context.Context, workflowID string) error

	/*
		UpdateWorkflowDeadline set a new deadline for a workflow and re-sync it onto the workflow's
		steps.

		Step deadlines are derived from (and mirror) the workflow deadline, so the new deadline is
		applied to every step which has not yet reached a terminal state (COMPLETE or CANCELLED). A
		terminal step's deadline is left untouched.

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@param deadline time.Time - the new deadline
	*/
	UpdateWorkflowDeadline(ctx context.Context, workflowID string, deadline time.Time) error

	// ------------------------------------------------------------------------------------
	// Workflow Steps

	/*
		GetWorkflowStep fetch a workflow step entry

			@param ctx context.Context - execution context
			@param stepID string - workflow step ID
			@returns workflow step entry
	*/
	GetWorkflowStep(ctx context.Context, stepID string) (models.WorkflowStep, error)

	/*
		ListWorkflowSteps list the workflow steps associated with a workflow.

		The steps are returned after topological sort and in alphabetical order for nodes at the
		same depth.

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@returns list of workflow steps
	*/
	ListWorkflowSteps(ctx context.Context, workflowID string) ([]models.WorkflowStep, error)

	/*
		ListWorkflowStepsReadyToRun list the workflow steps of a workflow which are ready to run.

		A step is ready to run when it is in the DEFINED state and all of its
		parent steps (if any) have completed.

		This considers only step-level readiness; it does NOT gate on the parent workflow's state.
		The caller (scheduler) is responsible for the soft-stop / hard-stop policy — e.g. not
		dispatching startable steps of a TIMED_OUT / CANCELLING workflow (see the workflow DESIGN's
		Process Workflow handler).

			@param ctx context.Context - execution context
			@param workflowID string - workflow ID
			@returns list of workflow steps ready to run
	*/
	ListWorkflowStepsReadyToRun(
		ctx context.Context, workflowID string,
	) ([]models.WorkflowStep, error)

	/*
		MarkWorkflowStepDefined revert a group of workflow steps to DEFINED, i.e. revive them.

		Only FAILED / TIMED_OUT steps may transition to DEFINED. Each reverted step is flagged as
		user-restarted.

			@param ctx context.Context - execution context
			@param workflowID string - the parent workflow ID
			@param stepIDs []string - the workflow step IDs
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowStepDefined(
		ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
	) error

	/*
		MarkWorkflowStepPending mark a group of workflow steps are pending execution

			@param ctx context.Context - execution context
			@param workflowID string - the parent workflow ID
			@param stepIDs []string - the workflow step IDs
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowStepPending(
		ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
	) error

	/*
		MarkWorkflowStepRunning mark a group of workflow steps are running

			@param ctx context.Context - execution context
			@param workflowID string - the parent workflow ID
			@param stepIDs []string - the workflow step IDs
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowStepRunning(
		ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
	) error

	/*
		MarkWorkflowStepComplete mark a group of workflow steps are complete

			@param ctx context.Context - execution context
			@param workflowID string - the parent workflow ID
			@param stepIDs []string - the workflow step IDs
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowStepComplete(
		ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
	) error

	/*
		MarkWorkflowStepFailed mark a group of workflow steps have failed

			@param ctx context.Context - execution context
			@param workflowID string - the parent workflow ID
			@param stepIDs []string - the workflow step IDs
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowStepFailed(
		ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
	) error

	/*
		MarkWorkflowStepTimedOut mark a group of workflow steps have timed out

			@param ctx context.Context - execution context
			@param workflowID string - the parent workflow ID
			@param stepIDs []string - the workflow step IDs
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowStepTimedOut(
		ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
	) error

	/*
		MarkWorkflowStepCancelling mark a group of workflow steps are being cancelled

			@param ctx context.Context - execution context
			@param workflowID string - the parent workflow ID
			@param stepIDs []string - the workflow step IDs
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowStepCancelling(
		ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
	) error

	/*
		MarkWorkflowStepCancelled mark a group of workflow steps are cancelled

			@param ctx context.Context - execution context
			@param workflowID string - the parent workflow ID
			@param stepIDs []string - the workflow step IDs
			@param timestamp time.Time - when the state change occurred
	*/
	MarkWorkflowStepCancelled(
		ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
	) error

	// ------------------------------------------------------------------------------------
	// Workflow Steps <=> Executor Task Linkage

	/*
		LinkWorkflowStepWithExecutorTask record that a task worked on a workflow step.

		A step may be linked to multiple tasks over its lifetime (its first run plus each
		user-initiated revive); each task executes exactly one step.

			@param ctx context.Context - execution context
			@param stepID string - the workflow step ID
			@param taskID string - the ID of the task which worked on the step
	*/
	LinkWorkflowStepWithExecutorTask(ctx context.Context, stepID string, taskID string) error

	/*
		GetWorkflowStepAndExecutorTask fetch a workflow step along with the tasks which worked on it.

			@param ctx context.Context - execution context
			@param stepID string - the workflow step ID
			@param activeTask bool - when true, only return live (non-terminal) tasks, i.e. tasks in
			the PENDING or ACTIVE state
			@returns the workflow step, and the tasks linked to it
	*/
	GetWorkflowStepAndExecutorTask(
		cxt context.Context, stepID string, activeTask bool,
	) (models.WorkflowStep, []models.Task, error)

	/*
		GetWorkflowStepProcessedByTask fetch the workflow step a task worked on, if any.

			@param ctx context.Context - execution context
			@param taskID string - the task ID
			@returns the workflow step linked to the task
	*/
	GetWorkflowStepProcessedByTask(ctx context.Context, taskID string) (models.WorkflowStep, error)
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
	return goutils.NewSQLError(fmt.Sprintf("failed to fetch %s '%s'", entity, id), err, true)
}
