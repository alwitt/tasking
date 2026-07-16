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
}

// StringPayload return its payload as a string
func (q IPCMessageExecuteInstance) StringPayload() (string, error) {
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

// PrepareIPCMsgTaskExecutionProcessFailed build IPC message `IPC_TASK_ENG_EXECUTE_FAILED`
func PrepareIPCMsgTaskExecutionProcessFailed(
	sender string, instanceID string, timestamp time.Time,
) goutilsRedis.QueueMessageEnvelope {
	return IPCMessageExecuteInstance{
		BaseIPCMessage: BaseIPCMessage{
			ID:        ulid.Make().String(),
			Type:      IPCMsgTypeExecuteFailed,
			Sender:    sender,
			Timestamp: timestamp,
		},
		InstanceID: instanceID,
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
