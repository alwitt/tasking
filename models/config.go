package models

import "time"

// RetryParam retry parameters
type RetryParam struct {
	// InitialDelaySec initial delay seconds
	InitialDelaySec int `mapstructure:"initDelaySec" json:"initDelaySec" validate:"gte=1"`
	// MaxRetries max number of retry allowed for failed execution. `-1` is infinite.
	MaxRetries int `mapstructure:"maxRetries" json:"maxRetries" validate:"gte=-1"`
	// MaxDelaySec max delay seconds
	MaxDelaySec *int `mapstructure:"maxDelaySec,omitempty" json:"maxDelaySec,omitempty" validate:"omitempty,gte=1"`
}

// ======================================================================================
// Task processing

// TaskQueueConfig task execution queue
type TaskQueueConfig struct {
	// Name of the task queue
	Name string `mapstructure:"name" json:"name" validate:"required"`
	// Workers number of workers to process tasks from the queue in parallel
	Workers int `mapstructure:"workers" json:"workers" validate:"required,gte=1"`
	// BufferRequests number of requests a receiver can locally buffer for execution
	BufferRequests int `mapstructure:"bufferRequests" json:"bufferRequests" validate:"required,gte=1"`
}

// TaskReceiverConfig task receiver config
type TaskReceiverConfig struct {
	// Name of the task receiver
	Name string `mapstructure:"name" json:"name" validate:"required"`
	// Queues the receiver will fetch tasks from
	Queues []TaskQueueConfig `mapstructure:"queues" json:"queues" validate:"required,gte=1,dive"`
	// SchedulerQueue scheduler IPC queue name
	SchedulerQueue string `mapstructure:"schedulerQueue" json:"schedulerQueue" validate:"required"`
}

// TaskQueueMapping mapping between a task name and task execution queue name
type TaskQueueMapping struct {
	// TaskName task name
	TaskName string `mapstructure:"name" json:"name" validate:"required"`
	// ExecutionQueue task execution queue name
	ExecutionQueue string `mapstructure:"queue" json:"queue" validate:"required"`
}

// TaskSchedulerConfig task scheduler config
type TaskSchedulerConfig struct {
	// MaintenanceTimerIntSecs maintenance timer interval in seconds
	MaintenanceTimerIntSecs int `mapstructure:"maintenanceTimerIntSecs" json:"maintenanceTimerIntSecs" validate:"required,gte=10"`
	// SchedulerQueue scheduler IPC queue name
	SchedulerQueue string `mapstructure:"schedulerQueue" json:"schedulerQueue" validate:"required"`
	// TaskMappings set of mapping between a task name and task execution queue name
	TaskMappings []TaskQueueMapping `mapstructure:"taskMapping" json:"taskMapping" validate:"required,gte=1,dive"`
}

// MaintenanceTimerInt convert MaintenanceTimerIntSecs to duration
func (c TaskSchedulerConfig) MaintenanceTimerInt() time.Duration {
	return time.Second * time.Duration(c.MaintenanceTimerIntSecs)
}

// NotificationProducerConfig config for the notification producer, the component that polls
// the audit log for un-broadcast events and publishes them over Redis pub/sub channels.
type NotificationProducerConfig struct {
	// PollIntervalSecs interval, in seconds, between audit-log polls
	PollIntervalSecs int `mapstructure:"pollIntervalSecs" json:"pollIntervalSecs" validate:"required,gte=1"`
	// BatchSize max number of un-broadcast events to process per poll
	BatchSize int `mapstructure:"batchSize" json:"batchSize" validate:"required,gte=1"`
	// EmitFirehose when true, publish every event on the `notify:all` firehose channel
	EmitFirehose bool `mapstructure:"emitFirehose" json:"emitFirehose"`
	// EmitTypeChan when true, publish every event on its `notify:type:<type>` channel
	EmitTypeChan bool `mapstructure:"emitTypeChan" json:"emitTypeChan"`
	// EmitCreator when true, publish creator-bearing events on their `notify:creator:<creator>`
	// and `notify:creator:<creator>:type:<type>` channels
	EmitCreator bool `mapstructure:"emitCreator" json:"emitCreator"`
	// the subject channel (`notify:subject:<subject-type>:<subject-id>`) is always emitted
}

// PollInterval convert PollIntervalSecs to duration
func (c NotificationProducerConfig) PollInterval() time.Duration {
	return time.Second * time.Duration(c.PollIntervalSecs)
}

// PerTaskRetryParam retry parameters to apply to a specific task name
type PerTaskRetryParam struct {
	// TaskName task name
	TaskName string `mapstructure:"name" json:"name" validate:"required"`
	// Retry the retry parameters for the queue
	Retry RetryParam `mapstructure:"retry" json:"retry" validate:"required"`
}

// TaskClientConfig task client config
type TaskClientConfig struct {
	// SchedulerQueue scheduler IPC queue name
	SchedulerQueue string `mapstructure:"schedulerQueue" json:"schedulerQueue" validate:"required"`
	// RetrySettings retry parameters to apply to specific task names
	RetrySettings []PerTaskRetryParam `mapstructure:"retrySettings,omitempty" json:"retrySettings,omitempty" validate:"omitempty,dive"`
}
