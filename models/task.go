// Package models - system data models
package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/alwitt/goutils"
	"gorm.io/datatypes"
)

// ======================================================================================
// Task retry

// MaxRetryDelay hard upper bound on the delay between retries. Any delay longer
// than this is not sensible, so all computed delays are clamped to it regardless
// of a task's configured MaxDelay.
const MaxRetryDelay = 5 * time.Minute

// TaskRetryParameters task failure retry parameters
type TaskRetryParameters struct {
	// MaxRetries max number of retry allowed for failed execution. `-1` is infinite.
	MaxRetries int `json:"max_retries" validate:"gte=-1"`

	// InitialDelaySec initial delay seconds
	InitialDelaySec int `json:"initial_delay" validate:"gte=1"`

	// MaxDelaySec max delay seconds
	MaxDelaySec *int `json:"max_delay,omitempty" validate:"omitempty,gte=1"`

	// Factor multiplicative factor or base
	Factor float64 `json:"factor" validate:"gte=1"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (s *TaskRetryParameters) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("core value is not byte slice")
	}

	var parsed TaskRetryParameters
	if err := json.Unmarshal(bytes, &parsed); err != nil {
		return err
	}
	*s = parsed
	return nil
}

// Value return json value, implement driver.Valuer interface
func (s TaskRetryParameters) Value() (driver.Value, error) {
	return json.Marshal(&s)
}

// InitialDelay convert `InitialDelaySec` to time duration
func (s TaskRetryParameters) InitialDelay() time.Duration {
	return time.Second * time.Duration(s.InitialDelaySec)
}

// MaxDelay convert `MaxDelaySec` to time duration
func (s TaskRetryParameters) MaxDelay() time.Duration {
	if s.MaxDelaySec == nil {
		return 0
	}
	return time.Second * time.Duration(*s.MaxDelaySec)
}

// DefaultTaskRetryParameters get the default task retry parameters
func DefaultTaskRetryParameters() TaskRetryParameters {
	maxDelaySec := 30
	return TaskRetryParameters{
		MaxRetries: 5, InitialDelaySec: 5, MaxDelaySec: &maxDelaySec, Factor: 1.43125,
	}
}

/*
NextDelay returns the wait duration before the next retry.

`retry` is 0‑based (the first retry after the initial attempt has retry==0).

When `retry` >= MaxRetries (or no retries left), it returns 0, signalling that
the caller should stop retrying.

	@param retry int - the retry attempt to compute delay for
	@returns - the wait duration
*/
func (s TaskRetryParameters) NextDelay(retry int) time.Duration {
	// stop if maxRetries reached (unless unlimited)
	if s.MaxRetries >= 0 && retry >= s.MaxRetries {
		return 0
	}

	// effective ceiling: the package-level MaxRetryDelay, further lowered by the
	// task's own MaxDelay if it sets a smaller one.
	ceiling := MaxRetryDelay
	if maxDelay := s.MaxDelay(); maxDelay > 0 && maxDelay < ceiling {
		ceiling = maxDelay
	}

	// exponential growth: initial * factor^retry
	//
	// Computed in float64 and capped before the time.Duration conversion. With
	// unlimited retries (or a very large `retry`) the product can reach +Inf or
	// exceed math.MaxInt64; converting such a value to time.Duration is undefined
	// in Go. Because `ceiling` is always finite, any such value simply clamps to it.
	delayF := float64(s.InitialDelay()) * math.Pow(s.Factor, float64(retry))
	if math.IsInf(delayF, 1) || delayF >= float64(ceiling) {
		return ceiling
	}

	return time.Duration(delayF)
}

// ======================================================================================
// Task

// TaskScheduleClassENUM task schedule class ENUM value
type TaskScheduleClassENUM string

const (
	// TaskScheduleClassImmediateOneShot one-shot task for immediate execution
	TaskScheduleClassImmediateOneShot TaskScheduleClassENUM = "IMMEDIATE_ONE_SHOT"
	// TaskScheduleClassScheduledOneShot one-shot task for scheduled future execution
	TaskScheduleClassScheduledOneShot TaskScheduleClassENUM = "SCHEDULED_ONE_SHOT"
)

// Values all valid TaskScheduleClassENUM values
func (TaskScheduleClassENUM) Values() []TaskScheduleClassENUM {
	return []TaskScheduleClassENUM{
		TaskScheduleClassImmediateOneShot,
		TaskScheduleClassScheduledOneShot,
	}
}

// TaskStateENUM task state ENUM value type
type TaskStateENUM string

const (
	// TaskStatePending the task is initialized and await scheduling
	TaskStatePending TaskStateENUM = "PENDING"
	// TaskStateActive the task is active and will be executed
	TaskStateActive TaskStateENUM = "ACTIVE"
	// TaskStateComplete the task completed
	TaskStateComplete TaskStateENUM = "COMPLETE"
	// TaskStateFailed the task failed (e.g. exhausted all retry attempts)
	TaskStateFailed TaskStateENUM = "FAILED"
	// TaskStateCancelling the task await cancelling
	TaskStateCancelling TaskStateENUM = "CANCELLING"
	// TaskStateCancelled the task has been cancelled
	TaskStateCancelled TaskStateENUM = "CANCELLED"
	// TaskStateTimeout the task did not wrap up before a deadline
	TaskStateTimeout TaskStateENUM = "TIMED_OUT"
)

// Values all valid TaskStateENUM values
func (TaskStateENUM) Values() []TaskStateENUM {
	return []TaskStateENUM{
		TaskStatePending,
		TaskStateActive,
		TaskStateComplete,
		TaskStateFailed,
		TaskStateCancelling,
		TaskStateCancelled,
		TaskStateTimeout,
	}
}

// Task performed by the system in the background
type Task struct {
	// ID task ID
	ID string `json:"id" gorm:"column:id;primaryKey;unique" validate:"required"`

	// TaskName name of the task being executed. This is used to locate the execution processor
	// for this task name
	TaskName string `json:"name" gorm:"column:name;not null" validate:"required"`

	// TaskScheduleClass execution scheduling class of the task
	TaskScheduleClass TaskScheduleClassENUM `json:"schedule_class" gorm:"column:schedule_class;not null" validate:"required,task_schedule_class"`

	// TaskState state of the task
	TaskState TaskStateENUM `json:"state" gorm:"column:state;not null" validate:"required,task_state"`

	// Parameters optional parameters needed for processing the task
	Parameters datatypes.JSON `json:"parameters,omitempty" gorm:"column:parameters;default:null"`

	// Metadata a metadata relating to the task
	Metadata datatypes.JSON `json:"metadata,omitempty" gorm:"column:metadata;default:null"`

	// TargetRunTime for scheduled one-shot tasks, when this task must run
	TargetRunTime *time.Time `json:"target_runtime,omitempty" gorm:"column:target_runtime;default:null"`

	// Deadline if specified, the task must be completed by this deadline
	Deadline *time.Time `json:"deadline,omitempty" gorm:"column:deadline;default:null"`

	// RetryParams retry parameters in case of failure
	RetryParams TaskRetryParameters `json:"retry_params" gorm:"column:retry_params;not null" validate:"required"`

	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// ValidNextState verify the task can transition to new state
func (t Task) ValidNextState(newState TaskStateENUM) error {
	statesWithTransitions := map[TaskStateENUM]map[TaskStateENUM]bool{
		TaskStatePending: {
			TaskStatePending:    true,
			TaskStateActive:     true,
			TaskStateCancelling: true,
		},
		TaskStateActive: {
			TaskStateActive:     true,
			TaskStateComplete:   true,
			TaskStateFailed:     true,
			TaskStateCancelling: true,
			TaskStateTimeout:    true,
		},
		TaskStateCancelling: {
			TaskStateCancelling: true,
			TaskStateCancelled:  true,
		},
	}

	availableNextStates, ok := statesWithTransitions[t.TaskState]
	if !ok {
		return goutils.NewConsistencyError(
			fmt.Sprintf("task can't transition out of state '%s'", t.TaskState), nil, true,
		)
	}

	if _, ok := availableNextStates[newState]; !ok {
		return goutils.NewConsistencyError(
			fmt.Sprintf("task can't transition from '%s' to '%s'", t.TaskState, newState), nil, true,
		)
	}

	return nil
}

// ======================================================================================
// Task execution instance

// TaskExecutionClassENUM task execution scheduling class ENUM value
type TaskExecutionClassENUM string

const (
	// TaskExecutionClassImmediate execute task as soon as possible
	TaskExecutionClassImmediate TaskExecutionClassENUM = "IMMEDIATE_EXECUTION"
	// TaskExecutionClassScheduled execute task at around a target time
	TaskExecutionClassScheduled TaskExecutionClassENUM = "SCHEDULED_EXECUTION"
	// TaskExecutionClassRetry retry of previously failed execution instance at
	// around a target time
	TaskExecutionClassRetry TaskExecutionClassENUM = "RETRY_EXECUTION"
)

// Values all valid TaskExecutionClassENUM values
func (TaskExecutionClassENUM) Values() []TaskExecutionClassENUM {
	return []TaskExecutionClassENUM{
		TaskExecutionClassImmediate,
		TaskExecutionClassScheduled,
		TaskExecutionClassRetry,
	}
}

// TaskExecutionStateENUM task execution state ENUM value
type TaskExecutionStateENUM string

const (
	// TaskExecutionStateDefined execution instance is defined
	TaskExecutionStateDefined TaskExecutionStateENUM = "EXECUTION_DEFINED"
	// TaskExecutionStateScheduled execution instance scheduled for future run
	TaskExecutionStateScheduled TaskExecutionStateENUM = "EXECUTION_SCHEDULED"
	// TaskExecutionStateEnqueued execution instance enqueued for worker pickup
	TaskExecutionStateEnqueued TaskExecutionStateENUM = "EXECUTION_ENQUEUED"
	// TaskExecutionStateAcquired execution instance acquired by a worker
	TaskExecutionStateAcquired TaskExecutionStateENUM = "EXECUTION_ACQUIRED"
	// TaskExecutionStateProcessing execution instance being processed by a worker
	TaskExecutionStateProcessing TaskExecutionStateENUM = "EXECUTION_PROCESSING"
	// TaskExecutionStateProcessed execution instance processed successfully by a worker
	TaskExecutionStateProcessed TaskExecutionStateENUM = "EXECUTION_PROCESSED"
	// TaskExecutionStateFailed execution instance failed during execution
	TaskExecutionStateFailed TaskExecutionStateENUM = "EXECUTION_FAILED"
	// TaskExecutionStateFinalized the scheduler has decide what actions to take next
	// after the execution instance succeeded or failed.
	TaskExecutionStateFinalized TaskExecutionStateENUM = "EXECUTION_FINALIZED"
	// TaskExecutionStateCancelled the execution instance was cancelled
	TaskExecutionStateCancelled TaskExecutionStateENUM = "EXECUTION_CANCELLED"
)

// Values all valid TaskExecutionStateENUM values
func (TaskExecutionStateENUM) Values() []TaskExecutionStateENUM {
	return []TaskExecutionStateENUM{
		TaskExecutionStateDefined,
		TaskExecutionStateScheduled,
		TaskExecutionStateEnqueued,
		TaskExecutionStateAcquired,
		TaskExecutionStateProcessing,
		TaskExecutionStateProcessed,
		TaskExecutionStateFailed,
		TaskExecutionStateFinalized,
		TaskExecutionStateCancelled,
	}
}

/*
TaskExecution one execution instances for a task. Workers are processing execution instances
of tasks, not the tasks themselves.

This model is tracking several things
* One-shot tasks - immediate execution instance
* Scheduled tasks - pending execution instance
* Periodic tasks - upcoming pending execution instance
* Retry on failure - pending retry execution instance

Upon completion of one execution instance, the scheduler decides on the next step
to take for each task.
*/
type TaskExecution struct {
	// ID task execution instance ID
	ID string `json:"id" gorm:"column:id;primaryKey;unique" validate:"required"`

	// TaskID the parent task ID
	TaskID string `json:"task_id" gorm:"column:task_id;not null" validate:"required"`

	// ExecutionClass execution instance scheduling class
	ExecutionClass TaskExecutionClassENUM `json:"execution_class" gorm:"column:execution_class;not null" validate:"required,task_execute_class"`
	// ExecutionState execution instance state type
	ExecutionState TaskExecutionStateENUM `json:"state" gorm:"column:state;not null" validate:"required,task_execute_state"`

	// TargetEnqueueTime target time to enqueue instance for worker to pickup
	//
	// This can't dictate when the task will execute, rather when it will be queue to execute
	TargetEnqueueTime *time.Time `json:"execute_at,omitempty" gorm:"column:execute_at;default:null"`

	// ExecutionWorkerName the worker which acquired the execution instance for processing
	ExecutionWorkerName *string `json:"worker_name,omitempty" gorm:"column:worker_name;default:null;" validate:"omitempty"`

	// RetryParentExecutionID for retry type execution instance, the original
	// execution instance ID
	RetryParentExecutionID *string `json:"retry_parent_id,omitempty" gorm:"column:retry_parent_id;default:null;" validate:"omitempty"`

	// ErrorMessage in case of processing failure, the error message
	ErrorMessage *string `json:"error_msg,omitempty" gorm:"column:error_msg;default:null;" validate:"omitempty"`

	// Deadline if specified, the execution instance must be completed by this deadline
	Deadline *time.Time `json:"deadline,omitempty" gorm:"column:deadline;default:null"`

	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// ValidNextState verify the task execution can transition to new state
func (e TaskExecution) ValidNextState(newState TaskExecutionStateENUM) error {
	statesWithTransitions := map[TaskExecutionStateENUM]map[TaskExecutionStateENUM]bool{
		TaskExecutionStateDefined: {
			TaskExecutionStateScheduled: true,
			TaskExecutionStateEnqueued:  true,
			TaskExecutionStateCancelled: true,
		},
		TaskExecutionStateScheduled: {
			TaskExecutionStateEnqueued:  true,
			TaskExecutionStateCancelled: true,
		},
		TaskExecutionStateEnqueued: {
			TaskExecutionStateAcquired:  true,
			TaskExecutionStateFailed:    true,
			TaskExecutionStateCancelled: true,
		},
		TaskExecutionStateAcquired: {
			TaskExecutionStateProcessing: true,
			TaskExecutionStateProcessed:  true,
			TaskExecutionStateFailed:     true,
			TaskExecutionStateCancelled:  true,
		},
		TaskExecutionStateProcessing: {
			TaskExecutionStateProcessed: true,
			TaskExecutionStateFailed:    true,
			TaskExecutionStateCancelled: true,
		},
		TaskExecutionStateProcessed: {
			TaskExecutionStateFinalized: true,
		},
		TaskExecutionStateFailed: {
			TaskExecutionStateFinalized: true,
		},
	}

	availableNextStates, ok := statesWithTransitions[e.ExecutionState]
	if !ok {
		return goutils.NewConsistencyError(
			fmt.Sprintf(
				"task execution can't transition out of state '%s'", e.ExecutionState,
			), nil, true,
		)
	}

	if _, ok := availableNextStates[newState]; !ok {
		return goutils.NewConsistencyError(
			fmt.Sprintf(
				"task execution can't transition from '%s' to '%s'", e.ExecutionState, newState,
			), nil, true,
		)
	}

	return nil
}

// ======================================================================================
// Task execution processor

// TaskExecutionProcessor execution processor for a particular
type TaskExecutionProcessor interface {
	/*
		ProcessTaskExecution process a task specific to this processor

			@param ctx context.Context - execution context
			@param taskEntry Task - task entry
			@param executeEntry TaskExecution - task execution instance
	*/
	ProcessTaskExecution(ctx context.Context, taskEntry Task, executeEntry TaskExecution) error
}
