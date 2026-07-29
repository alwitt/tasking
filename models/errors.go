// Package models - system data models
package models

import "github.com/alwitt/goutils"

// ======================================================================================
// IPC Message Queue Error

// IPCMessageQueueError encountered error operating the IPC message queue
type IPCMessageQueueError struct{ goutils.BaseError }

// NewIPCMessageQueueError build a IPCMessageQueueError, optionally capturing the call stack.
func NewIPCMessageQueueError(message string, core error, getCallStack bool) IPCMessageQueueError {
	base := goutils.BaseError{Name: "IPCMessageQueueError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return IPCMessageQueueError{BaseError: base}
}

// ======================================================================================
// Task Engine Errors

// TaskPreprocessError error encountered during task pre-processing
type TaskPreprocessError struct{ goutils.BaseError }

// NewTaskPreprocessError builds a TaskPreprocessError, optionally capturing the call stack.
func NewTaskPreprocessError(message string, core error, getCallStack bool) TaskPreprocessError {
	base := goutils.BaseError{Name: "TaskPreprocessError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return TaskPreprocessError{BaseError: base}
}

// NonRecoverableError a task execution failure a TaskExecutionProcessor considers permanent:
// retrying it will not help (e.g. malformed input, a resource that will never exist). A processor
// wraps its returned error in a NonRecoverableError to opt the failure out of the task's retry
// policy; the failure is still an ordinary task-execution failure (reported as EXECUTE_FAILED with
// a NON_RETRYABLE disposition), distinct from an engine-level failure.
type NonRecoverableError struct{ goutils.BaseError }

// NewNonRecoverableError builds a NonRecoverableError, optionally capturing the call stack.
func NewNonRecoverableError(message string, core error, getCallStack bool) NonRecoverableError {
	base := goutils.BaseError{Name: "NonRecoverableError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return NonRecoverableError{BaseError: base}
}

// TaskExecutionError error encountered during task processing
type TaskExecutionError struct{ goutils.BaseError }

// NewTaskExecutionError builds a TaskExecutionError, optionally capturing the call stack.
func NewTaskExecutionError(message string, core error, getCallStack bool) TaskExecutionError {
	base := goutils.BaseError{Name: "TaskExecutionError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return TaskExecutionError{BaseError: base}
}

// TaskExecutorError error operating the task executor component machinery
// (worker-pool setup, execution instance submission, and shutdown), as opposed
// to errors encountered while processing an individual task execution instance.
type TaskExecutorError struct{ goutils.BaseError }

// NewTaskExecutorError builds a TaskExecutorError, optionally capturing the call stack.
func NewTaskExecutorError(message string, core error, getCallStack bool) TaskExecutorError {
	base := goutils.BaseError{Name: "TaskExecutorError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return TaskExecutorError{BaseError: base}
}

// TaskReceiverError error operating the task receiver component machinery
// (queue receiver/executor/sender setup, crash-recovery orchestration, IPC
// message dequeue, claim-ownership, and submission to the executor), as opposed
// to errors encountered while processing an individual task execution instance.
type TaskReceiverError struct{ goutils.BaseError }

// NewTaskReceiverError builds a TaskReceiverError, optionally capturing the call stack.
func NewTaskReceiverError(message string, core error, getCallStack bool) TaskReceiverError {
	base := goutils.BaseError{Name: "TaskReceiverError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return TaskReceiverError{BaseError: base}
}

// TaskSchedulerError error operating the task scheduler component: both the
// machinery (request-processor and maintenance-timer setup, handler registration,
// queue receiver/sender setup, worker/timer startup) and the per-task scheduling
// logic (processing pending/cancelled/timed-out tasks and execution-instance state
// transitions). Wraps the underlying cause (e.g. PersistenceError,
// ConsistencyError, TaskQueueError) as Core.
type TaskSchedulerError struct{ goutils.BaseError }

// NewTaskSchedulerError builds a TaskSchedulerError, optionally capturing the call stack.
func NewTaskSchedulerError(message string, core error, getCallStack bool) TaskSchedulerError {
	base := goutils.BaseError{Name: "TaskSchedulerError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return TaskSchedulerError{BaseError: base}
}

// NotifyProducerError error operating the notification producer component: both the
// machinery (validator/timer setup, lifecycle start/stop) and the poll→publish→stamp loop
// (audit-log polling, channel routing, pub/sub publishing, broadcast stamping). Wraps the
// underlying cause (e.g. PersistenceError, RedisError, ConsistencyError) as Core.
type NotifyProducerError struct{ goutils.BaseError }

// NewNotifyProducerError builds a NotifyProducerError, optionally capturing the call stack.
func NewNotifyProducerError(message string, core error, getCallStack bool) NotifyProducerError {
	base := goutils.BaseError{Name: "NotifyProducerError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return NotifyProducerError{BaseError: base}
}

// NotifyConsumerError error operating the notification consumer component: both the
// machinery (subscription setup, lifecycle start/stop) and the deliver path (deserializing a
// received pub/sub payload into a NotificationEvent before invoking the caller's callback).
// Wraps the underlying cause (e.g. RedisError, ConsistencyError) as Core.
type NotifyConsumerError struct{ goutils.BaseError }

// NewNotifyConsumerError builds a NotifyConsumerError, optionally capturing the call stack.
func NewNotifyConsumerError(message string, core error, getCallStack bool) NotifyConsumerError {
	base := goutils.BaseError{Name: "NotifyConsumerError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return NotifyConsumerError{BaseError: base}
}

// TaskMaintenanceError error encountered during the scheduler's periodic maintenance
// sweep (performMaintenance). Used only by performMaintenance to wrap failures of the
// list-scan transactions and of the per-item handler calls it drives.
type TaskMaintenanceError struct{ goutils.BaseError }

// NewTaskMaintenanceError builds a TaskMaintenanceError, optionally capturing the call stack.
func NewTaskMaintenanceError(message string, core error, getCallStack bool) TaskMaintenanceError {
	base := goutils.BaseError{Name: "TaskMaintenanceError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return TaskMaintenanceError{BaseError: base}
}

// TaskClientError error operating the task client: the public task-submission API
// the rest of the system uses to enqueue work with the scheduler. Distinct from
// the task engine's internal component errors. Wraps the underlying cause (e.g.
// PersistenceError, TaskQueueError) as Core.
type TaskClientError struct{ goutils.BaseError }

// NewTaskClientError builds a TaskClientError, optionally capturing the call stack.
func NewTaskClientError(message string, core error, getCallStack bool) TaskClientError {
	base := goutils.BaseError{Name: "TaskClientError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return TaskClientError{BaseError: base}
}

// TaskPostprocessError error encountered during task post-processing
type TaskPostprocessError struct{ goutils.BaseError }

// NewTaskPostprocessError builds a TaskPostprocessError, optionally capturing the call stack.
func NewTaskPostprocessError(message string, core error, getCallStack bool) TaskPostprocessError {
	base := goutils.BaseError{Name: "TaskPostprocessError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return TaskPostprocessError{BaseError: base}
}

// ======================================================================================
// Workflow Step Runner Errors

// StepPreprocessError error encountered during workflow step execution pre-processing: the
// Step Runner's DB lookup of the workflow step or its parent workflow. Potentially transient
// (e.g. a failed DB read), so it is returned verbatim and left subject to the task engine's
// normal per-attempt retry - it is deliberately NOT wrapped in a NonRecoverableError.
type StepPreprocessError struct{ goutils.BaseError }

// NewStepPreprocessError builds a StepPreprocessError, optionally capturing the call stack.
func NewStepPreprocessError(message string, core error, getCallStack bool) StepPreprocessError {
	base := goutils.BaseError{Name: "StepPreprocessError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return StepPreprocessError{BaseError: base}
}

// StepExecutionError error encountered while the Step Runner runs the WorkflowStepProcessor
// registered for a step's Type. It wraps the processor's returned error as Core so errors.As can
// still find any error the processor wrapped (including a NonRecoverableError). Potentially
// transient, so subject to the task engine's normal per-attempt retry.
type StepExecutionError struct{ goutils.BaseError }

// NewStepExecutionError builds a StepExecutionError, optionally capturing the call stack.
func NewStepExecutionError(message string, core error, getCallStack bool) StepExecutionError {
	base := goutils.BaseError{Name: "StepExecutionError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return StepExecutionError{BaseError: base}
}

// ======================================================================================
// Workflow Scheduler Errors

// WorkflowSchedulerError error operating the workflow scheduler component: both the machinery
// (validator/timer setup, queue receiver/sender setup, lifecycle start/stop, IPC message
// parse/dispatch) and the per-event workflow/step state-transition logic. Wraps the underlying
// cause (e.g. PersistenceError, ConsistencyError, IPCMessageQueueError) as Core. Mirrors
// TaskSchedulerError for the workflow scheduler.
type WorkflowSchedulerError struct{ goutils.BaseError }

// NewWorkflowSchedulerError builds a WorkflowSchedulerError, optionally capturing the call stack.
func NewWorkflowSchedulerError(
	message string, core error, getCallStack bool,
) WorkflowSchedulerError {
	base := goutils.BaseError{Name: "WorkflowSchedulerError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return WorkflowSchedulerError{BaseError: base}
}

// WorkflowClientError error operating the workflow client: the public workflow submission /
// user-mutation API (define, revive, cancel) an embedding application uses to drive the workflow
// engine. Distinct from the workflow engine's internal WorkflowSchedulerError. Wraps the
// underlying cause (e.g. PersistenceError for a failed row write, IPCMessageQueueError for a lost
// scheduler poke) as Core, so callers can errors.As to distinguish "row not written" from
// "written but poke lost". Mirrors TaskClientError for the workflow engine.
type WorkflowClientError struct{ goutils.BaseError }

// NewWorkflowClientError builds a WorkflowClientError, optionally capturing the call stack.
func NewWorkflowClientError(message string, core error, getCallStack bool) WorkflowClientError {
	base := goutils.BaseError{Name: "WorkflowClientError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return WorkflowClientError{BaseError: base}
}
