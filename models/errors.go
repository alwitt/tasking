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
// Persistence Layer Errors - SQL

// PersistenceError encountered when operating the persistence layer (e.g. SQL statement failed)
//
// Not recoverable
type PersistenceError struct{ goutils.BaseError }

// NewPersistenceError builds a PersistenceError, optionally capturing the call stack.
func NewPersistenceError(message string, core error, getCallStack bool) PersistenceError {
	base := goutils.BaseError{Name: "PersistenceError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return PersistenceError{BaseError: base}
}

// SQLError wraps an error returned by the GORM layer, indicating a SQL statement failed
type SQLError struct{ goutils.BaseError }

// NewSQLError builds a SQLError, optionally capturing the call stack.
func NewSQLError(message string, core error, getCallStack bool) SQLError {
	base := goutils.BaseError{Name: "SQLError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return SQLError{BaseError: base}
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
