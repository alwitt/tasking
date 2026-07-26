package models

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
)

// IPCMessageTypeEnum IPC message type ENUM
type IPCMessageTypeEnum string

const (
	// IPCMsgTypeNewTask IPC message indicating new task entry is defined and
	// need scheduling for execution.
	IPCMsgTypeNewTask IPCMessageTypeEnum = "IPC_TASK_ENG_NEW_TASK"
	// IPCMsgTypeCancelTask IPC message cancelling a system task
	IPCMsgTypeCancelTask IPCMessageTypeEnum = "IPC_TASK_ENG_CANCEL_TASK"
	// IPCMsgTypePendingInstance IPC message containing an execution instance
	// pending processing
	IPCMsgTypePendingInstance IPCMessageTypeEnum = "IPC_TASK_ENG_PENDING_INSTANCE"
	// IPCMsgTypeExecuteSucceeded IPC message indicating an execution instance
	// complete successfully
	IPCMsgTypeExecuteSucceeded IPCMessageTypeEnum = "IPC_TASK_ENG_EXECUTE_SUCCEEDED"
	// IPCMsgTypeExecuteFailed IPC message indicating an execution instance failed
	// during processing
	IPCMsgTypeExecuteFailed IPCMessageTypeEnum = "IPC_TASK_ENG_EXECUTE_FAILED"
	// IPCMsgTypeEngineFailed IPC message indicating the core task engine failed
	// to operate correctly on an execution instance (e.g. the receiver could not
	// claim it, or could not submit it to the executor). This is an engine-level
	// failure, distinct from IPCMsgTypeExecuteFailed which reports a failure
	// during actual task execution.
	IPCMsgTypeEngineFailed IPCMessageTypeEnum = "IPC_TASK_ENG_ENGINE_FAILED"
	// IPCMsgTypeTaskMaintenance IPC message triggering the task scheduler's periodic
	// maintenance sweep. Self-enqueued by the maintenance interval timer onto the
	// scheduler's own queue; carries no payload beyond the base message.
	IPCMsgTypeTaskMaintenance IPCMessageTypeEnum = "IPC_TASK_ENG_MAINTENANCE"

	// ----------------------------------------------------------------------------------
	// Workflow scheduler IPC message types. These drive the workflow scheduler's single-thread
	// event loop over its own dedicated IPC queue (distinct from the task scheduler queue). See
	// workflow/DESIGN.md "Scheduler Events".

	// IPCMsgTypeWFProcessWorkflow IPC message asking the workflow scheduler to process a
	// workflow: start it (PENDING -> RUNNING) on first receipt and fan out startable steps.
	IPCMsgTypeWFProcessWorkflow IPCMessageTypeEnum = "IPC_WF_ENG_PROCESS_WORKFLOW"
	// IPCMsgTypeWFScheduleStep IPC message asking the workflow scheduler to dispatch one
	// workflow step to the task engine.
	IPCMsgTypeWFScheduleStep IPCMessageTypeEnum = "IPC_WF_ENG_SCHEDULE_STEP"
	// IPCMsgTypeWFStepExecUpdate IPC message delivering a workflow step's already-resolved
	// terminal outcome to the scheduler's execution-update reducer. Produced by the notify
	// callback adapter (task terminal event -> step state) and by the maintenance sweep.
	IPCMsgTypeWFStepExecUpdate IPCMessageTypeEnum = "IPC_WF_ENG_STEP_EXEC_UPDATE"
	// IPCMsgTypeWFStepTaskUpdate IPC message delivering a step's terminal outcome keyed by the
	// executing TASK ID - the notify fast-path feedback. Produced by the notify callback adapter
	// (which has only the task ID and does no DB work); the scheduler worker resolves task -> step
	// before invoking the step-keyed execution-update reducer.
	IPCMsgTypeWFStepTaskUpdate IPCMessageTypeEnum = "IPC_WF_ENG_STEP_TASK_UPDATE"
	// IPCMsgTypeWFReviveWorkflow IPC message asking the workflow scheduler to revive a
	// FAILED/TIMED_OUT workflow (optionally with a new deadline).
	IPCMsgTypeWFReviveWorkflow IPCMessageTypeEnum = "IPC_WF_ENG_REVIVE_WORKFLOW"
	// IPCMsgTypeWFCancelWorkflow IPC message asking the workflow scheduler to cancel a workflow.
	IPCMsgTypeWFCancelWorkflow IPCMessageTypeEnum = "IPC_WF_ENG_CANCEL_WORKFLOW"
	// IPCMsgTypeWFMaintenance IPC message triggering the workflow scheduler's periodic
	// recovery/liveness maintenance sweep. Self-enqueued by the maintenance interval timer;
	// carries no payload beyond the base message.
	IPCMsgTypeWFMaintenance IPCMessageTypeEnum = "IPC_WF_ENG_MAINTENANCE"
)

// Values all valid IPCMessageTypeEnum values
func (IPCMessageTypeEnum) Values() []IPCMessageTypeEnum {
	return []IPCMessageTypeEnum{
		IPCMsgTypeNewTask,
		IPCMsgTypeCancelTask,
		IPCMsgTypePendingInstance,
		IPCMsgTypeExecuteSucceeded,
		IPCMsgTypeExecuteFailed,
		IPCMsgTypeEngineFailed,
		IPCMsgTypeTaskMaintenance,
		IPCMsgTypeWFProcessWorkflow,
		IPCMsgTypeWFScheduleStep,
		IPCMsgTypeWFStepExecUpdate,
		IPCMsgTypeWFStepTaskUpdate,
		IPCMsgTypeWFReviveWorkflow,
		IPCMsgTypeWFCancelWorkflow,
		IPCMsgTypeWFMaintenance,
	}
}

// BaseIPCMessage base IPC message object
type BaseIPCMessage struct {
	// ID message ID
	ID string `json:"id" validate:"required"`
	// Type message type
	Type IPCMessageTypeEnum `json:"type" validate:"required,ipc_msg_type"`
	// Sender name of the sending entity
	Sender string `json:"sender" validate:"required"`
	// Timestamp message timestamp
	Timestamp time.Time `json:"timestamp"`
}

// StringPayload return its payload as a string. This lets BaseIPCMessage be sent directly for
// payload-less message types (e.g. IPCMsgTypeWFMaintenance).
func (q BaseIPCMessage) StringPayload() (string, error) {
	t, err := json.Marshal(&q)
	return string(t), err
}

// ParseIPCMessage parse the IPC message based on type
func ParseIPCMessage(validator *validator.Validate, msg []byte) (interface{}, error) {
	var asBaseMsg BaseIPCMessage
	if err := json.Unmarshal(msg, &asBaseMsg); err != nil {
		return nil, goutils.NewConsistencyError(
			"failed to parse IPC message as base message", err, true,
		)
	}
	if err := validator.Struct(&asBaseMsg); err != nil {
		return nil, goutils.NewValidationError("base IPC message invalid", err, true)
	}
	validate := func(parsed interface{}) error {
		if err := validator.Struct(parsed); err != nil {
			return goutils.NewValidationError(
				fmt.Sprintf("IPC message '%s' validation failed", asBaseMsg.Type), err, true,
			)
		}
		return nil
	}
	switch asBaseMsg.Type {
	case IPCMsgTypeNewTask:
		fallthrough
	case IPCMsgTypeCancelTask:
		var parsed IPCMessageSystemTask
		if err := json.Unmarshal(msg, &parsed); err != nil {
			return nil, goutils.NewConsistencyError(
				fmt.Sprintf("IPC message '%s' parse failed", asBaseMsg.Type), err, true,
			)
		}
		return parsed, validate(&parsed)

	case IPCMsgTypePendingInstance:
		fallthrough
	case IPCMsgTypeExecuteSucceeded:
		fallthrough
	case IPCMsgTypeExecuteFailed:
		fallthrough
	case IPCMsgTypeEngineFailed:
		var parsed IPCMessageExecuteInstance
		if err := json.Unmarshal(msg, &parsed); err != nil {
			return nil, goutils.NewConsistencyError(
				fmt.Sprintf("IPC message '%s' parse failed", asBaseMsg.Type), err, true,
			)
		}
		return parsed, validate(&parsed)

	case IPCMsgTypeWFProcessWorkflow:
		fallthrough
	case IPCMsgTypeWFCancelWorkflow:
		var parsed IPCMessageWorkflow
		if err := json.Unmarshal(msg, &parsed); err != nil {
			return nil, goutils.NewConsistencyError(
				fmt.Sprintf("IPC message '%s' parse failed", asBaseMsg.Type), err, true,
			)
		}
		return parsed, validate(&parsed)

	case IPCMsgTypeWFScheduleStep:
		var parsed IPCMessageWorkflowStep
		if err := json.Unmarshal(msg, &parsed); err != nil {
			return nil, goutils.NewConsistencyError(
				fmt.Sprintf("IPC message '%s' parse failed", asBaseMsg.Type), err, true,
			)
		}
		return parsed, validate(&parsed)

	case IPCMsgTypeWFStepExecUpdate:
		var parsed IPCMessageWorkflowStepExecUpdate
		if err := json.Unmarshal(msg, &parsed); err != nil {
			return nil, goutils.NewConsistencyError(
				fmt.Sprintf("IPC message '%s' parse failed", asBaseMsg.Type), err, true,
			)
		}
		return parsed, validate(&parsed)

	case IPCMsgTypeWFStepTaskUpdate:
		var parsed IPCMessageWorkflowStepTaskUpdate
		if err := json.Unmarshal(msg, &parsed); err != nil {
			return nil, goutils.NewConsistencyError(
				fmt.Sprintf("IPC message '%s' parse failed", asBaseMsg.Type), err, true,
			)
		}
		return parsed, validate(&parsed)

	case IPCMsgTypeWFReviveWorkflow:
		var parsed IPCMessageWorkflowRevive
		if err := json.Unmarshal(msg, &parsed); err != nil {
			return nil, goutils.NewConsistencyError(
				fmt.Sprintf("IPC message '%s' parse failed", asBaseMsg.Type), err, true,
			)
		}
		return parsed, validate(&parsed)

	case IPCMsgTypeTaskMaintenance:
		fallthrough
	case IPCMsgTypeWFMaintenance:
		// Carries no payload beyond the base message, already parsed and validated above.
		return asBaseMsg, nil

	default:
		return nil, goutils.NewConsistencyError(
			fmt.Sprintf("unknown IPC message type %s", asBaseMsg.Type), nil, true,
		)
	}
}

// IPCMessageSystemTask system task related IPC message object
type IPCMessageSystemTask struct {
	BaseIPCMessage
	// TaskID ID of the task referenced
	TaskID string `json:"task_id" validate:"required"`
}

// StringPayload return its payload as a string
func (q IPCMessageSystemTask) StringPayload() (string, error) {
	t, err := json.Marshal(&q)
	return string(t), err
}

// IPCMessageExecuteInstance system task execution instance related IPC message object
type IPCMessageExecuteInstance struct {
	BaseIPCMessage
	// InstanceID ID of the task execution instance referenced
	InstanceID string `json:"instance_id" validate:"required"`
	// Disposition for an EXECUTE_FAILED message, whether the failure is retryable. Nil (the only
	// value for non-failure messages, and the default for a failure) is treated as retryable.
	Disposition *TaskFailureDispositionENUM `json:"disposition,omitempty" validate:"omitempty,task_failure_disposition"`
}

// StringPayload return its payload as a string
func (q IPCMessageExecuteInstance) StringPayload() (string, error) {
	t, err := json.Marshal(&q)
	return string(t), err
}

// ======================================================================================
// Workflow scheduler IPC message objects

// IPCMessageWorkflow a workflow-scoped IPC message referencing a single workflow by ID. Shared
// by IPCMsgTypeWFProcessWorkflow and IPCMsgTypeWFCancelWorkflow (same shape, distinguished by
// Type), mirroring how IPCMessageSystemTask is shared by the new/cancel task messages.
type IPCMessageWorkflow struct {
	BaseIPCMessage
	// WorkflowID ID of the workflow referenced
	WorkflowID string `json:"workflow_id" validate:"required"`
}

// StringPayload return its payload as a string
func (q IPCMessageWorkflow) StringPayload() (string, error) {
	t, err := json.Marshal(&q)
	return string(t), err
}

// IPCMessageWorkflowStep a step-scoped IPC message referencing a single workflow step by ID.
// Used by IPCMsgTypeWFScheduleStep.
type IPCMessageWorkflowStep struct {
	BaseIPCMessage
	// StepID ID of the workflow step referenced
	StepID string `json:"step_id" validate:"required"`
}

// StringPayload return its payload as a string
func (q IPCMessageWorkflowStep) StringPayload() (string, error) {
	t, err := json.Marshal(&q)
	return string(t), err
}

// IPCMessageWorkflowStepExecUpdate delivers a workflow step's already-resolved terminal outcome
// to the scheduler's execution-update reducer (keyed [step ID, new step state]). The producers
// (the notify callback adapter and the maintenance sweep) resolve the task-event-type -> step
// state mapping before enqueueing, so the reducer never sees raw task event types. Used by
// IPCMsgTypeWFStepExecUpdate.
type IPCMessageWorkflowStepExecUpdate struct {
	BaseIPCMessage
	// StepID ID of the workflow step referenced
	StepID string `json:"step_id" validate:"required"`
	// NewStepState the resolved new step state. The workflow_step_state macro accepts any step
	// state; the reducer additionally enforces that it is a terminal outcome
	// (COMPLETE/FAILED/TIMED_OUT/CANCELLED) at handling time.
	NewStepState WorkflowStepStateENUM `json:"new_step_state" validate:"required,workflow_step_state"`
}

// StringPayload return its payload as a string
func (q IPCMessageWorkflowStepExecUpdate) StringPayload() (string, error) {
	t, err := json.Marshal(&q)
	return string(t), err
}

// IPCMessageWorkflowStepTaskUpdate delivers a step's terminal outcome keyed by the executing
// TASK ID - the notify fast-path feedback. The notify callback adapter produces it from a terminal
// task event (mapping the event type -> step state) while holding only the task ID and doing no DB
// work; the scheduler worker later resolves task -> step and invokes the step-keyed execution-update
// reducer. Used by IPCMsgTypeWFStepTaskUpdate.
type IPCMessageWorkflowStepTaskUpdate struct {
	BaseIPCMessage
	// TaskID ID of the task whose terminal event drove this update
	TaskID string `json:"task_id" validate:"required"`
	// NewStepState the resolved new step state. The workflow_step_state macro accepts any step
	// state; the reducer additionally enforces that it is a terminal outcome
	// (COMPLETE/FAILED/TIMED_OUT/CANCELLED) at handling time.
	NewStepState WorkflowStepStateENUM `json:"new_step_state" validate:"required,workflow_step_state"`
}

// StringPayload return its payload as a string
func (q IPCMessageWorkflowStepTaskUpdate) StringPayload() (string, error) {
	t, err := json.Marshal(&q)
	return string(t), err
}

// IPCMessageWorkflowRevive asks the scheduler to revive a FAILED/TIMED_OUT workflow. NewDeadline
// is optional: nil/absent means no deadline change (valid for a FAILED revive); present extends
// the deadline (required for a TIMED_OUT revive). The "required iff TIMED_OUT" rule is enforced
// in the scheduler handler against live workflow state, not by a struct tag. Used by
// IPCMsgTypeWFReviveWorkflow.
type IPCMessageWorkflowRevive struct {
	BaseIPCMessage
	// WorkflowID ID of the workflow to revive
	WorkflowID string `json:"workflow_id" validate:"required"`
	// NewDeadline optional new workflow deadline
	NewDeadline *time.Time `json:"new_deadline,omitempty"`
}

// StringPayload return its payload as a string
func (q IPCMessageWorkflowRevive) StringPayload() (string, error) {
	t, err := json.Marshal(&q)
	return string(t), err
}

// ======================================================================================
// Helper function for defining IPC messages

// PrepareIPCMsgNewPendingTask build IPC message `IPC_TASK_ENG_NEW_TASK`
func PrepareIPCMsgNewPendingTask(
	sender string, taskID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageSystemTask{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeNewTask,
			Sender:    sender,
			Timestamp: timestamp,
		},
		TaskID: taskID,
	}
}

// PrepareIPCMsgCancelTask build IPC message `IPC_TASK_ENG_CANCEL_TASK`
func PrepareIPCMsgCancelTask(
	sender string, taskID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageSystemTask{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeCancelTask,
			Sender:    sender,
			Timestamp: timestamp,
		},
		TaskID: taskID,
	}
}

// PrepareIPCMsgTaskExecutionRequested build IPC message `IPC_TASK_ENG_PENDING_INSTANCE`
func PrepareIPCMsgTaskExecutionRequested(
	sender string, instanceID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageExecuteInstance{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypePendingInstance,
			Sender:    sender,
			Timestamp: timestamp,
		},
		InstanceID: instanceID,
	}
}

// PrepareIPCMsgTaskExecutionProcessSucceeded build IPC message `IPC_TASK_ENG_EXECUTE_SUCCEEDED`
func PrepareIPCMsgTaskExecutionProcessSucceeded(
	sender string, instanceID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageExecuteInstance{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeExecuteSucceeded,
			Sender:    sender,
			Timestamp: timestamp,
		},
		InstanceID: instanceID,
	}
}

// PrepareIPCMsgTaskExecutionProcessFailed build IPC message `IPC_TASK_ENG_EXECUTE_FAILED`. A nil
// disposition is treated as retryable by the scheduler.
func PrepareIPCMsgTaskExecutionProcessFailed(
	sender string,
	instanceID string,
	disposition *TaskFailureDispositionENUM,
	timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageExecuteInstance{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeExecuteFailed,
			Sender:    sender,
			Timestamp: timestamp,
		},
		InstanceID:  instanceID,
		Disposition: disposition,
	}
}

// PrepareIPCMsgTaskExecutionEngineFailed build IPC message `IPC_TASK_ENG_ENGINE_FAILED`
func PrepareIPCMsgTaskExecutionEngineFailed(
	sender string, instanceID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageExecuteInstance{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeEngineFailed,
			Sender:    sender,
			Timestamp: timestamp,
		},
		InstanceID: instanceID,
	}
}

// ----------------------------------------------------------------------------------
// Workflow scheduler IPC message constructors

// PrepareIPCMsgWFProcessWorkflow build IPC message `IPC_WF_ENG_PROCESS_WORKFLOW`
func PrepareIPCMsgWFProcessWorkflow(
	sender string, workflowID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageWorkflow{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeWFProcessWorkflow,
			Sender:    sender,
			Timestamp: timestamp,
		},
		WorkflowID: workflowID,
	}
}

// PrepareIPCMsgWFCancelWorkflow build IPC message `IPC_WF_ENG_CANCEL_WORKFLOW`
func PrepareIPCMsgWFCancelWorkflow(
	sender string, workflowID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageWorkflow{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeWFCancelWorkflow,
			Sender:    sender,
			Timestamp: timestamp,
		},
		WorkflowID: workflowID,
	}
}

// PrepareIPCMsgWFScheduleStep build IPC message `IPC_WF_ENG_SCHEDULE_STEP`
func PrepareIPCMsgWFScheduleStep(
	sender string, stepID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageWorkflowStep{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeWFScheduleStep,
			Sender:    sender,
			Timestamp: timestamp,
		},
		StepID: stepID,
	}
}

// PrepareIPCMsgWFStepExecUpdate build IPC message `IPC_WF_ENG_STEP_EXEC_UPDATE`. newStepState is
// the already-resolved terminal step outcome.
func PrepareIPCMsgWFStepExecUpdate(
	sender string, stepID string, newStepState WorkflowStepStateENUM, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageWorkflowStepExecUpdate{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeWFStepExecUpdate,
			Sender:    sender,
			Timestamp: timestamp,
		},
		StepID:       stepID,
		NewStepState: newStepState,
	}
}

// PrepareIPCMsgWFStepTaskUpdate build IPC message `IPC_WF_ENG_STEP_TASK_UPDATE`. newStepState is
// the terminal step outcome mapped from the source task event; the worker resolves taskID -> step.
func PrepareIPCMsgWFStepTaskUpdate(
	sender string, taskID string, newStepState WorkflowStepStateENUM, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageWorkflowStepTaskUpdate{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeWFStepTaskUpdate,
			Sender:    sender,
			Timestamp: timestamp,
		},
		TaskID:       taskID,
		NewStepState: newStepState,
	}
}

// PrepareIPCMsgWFReviveWorkflow build IPC message `IPC_WF_ENG_REVIVE_WORKFLOW`. A nil newDeadline
// means no deadline change.
func PrepareIPCMsgWFReviveWorkflow(
	sender string, workflowID string, newDeadline *time.Time, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageWorkflowRevive{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeWFReviveWorkflow,
			Sender:    sender,
			Timestamp: timestamp,
		},
		WorkflowID:  workflowID,
		NewDeadline: newDeadline,
	}
}

// PrepareIPCMsgTaskMaintenance build IPC message `IPC_TASK_ENG_MAINTENANCE`
func PrepareIPCMsgTaskMaintenance(
	sender string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return BaseIPCMessage{
		ID:        ulid.Make().String(),
		Type:      IPCMsgTypeTaskMaintenance,
		Sender:    sender,
		Timestamp: timestamp,
	}
}

// PrepareIPCMsgWFMaintenance build IPC message `IPC_WF_ENG_MAINTENANCE`
func PrepareIPCMsgWFMaintenance(
	sender string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return BaseIPCMessage{
		ID:        ulid.Make().String(),
		Type:      IPCMsgTypeWFMaintenance,
		Sender:    sender,
		Timestamp: timestamp,
	}
}
