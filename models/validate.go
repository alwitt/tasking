// Package models - system data models
package models

import (
	"github.com/alwitt/goutils"
	"github.com/go-playground/validator/v10"
)

/*
RegisterWithValidator register with the validator this custom validation support

	@param v *validator.Validate - the validator to register against
	@return whether successful
*/
func RegisterWithValidator(v *validator.Validate) error {
	if err := goutils.RegisterENUMInValidator(
		v, "system_event_type", goutils.ValidateStringENUM[SystemEventTypeENUM](),
	); err != nil {
		return err
	}

	if err := goutils.RegisterENUMInValidator(
		v, "task_schedule_class", goutils.ValidateStringENUM[TaskScheduleClassENUM](),
	); err != nil {
		return err
	}

	if err := goutils.RegisterENUMInValidator(
		v, "task_state", goutils.ValidateStringENUM[TaskStateENUM](),
	); err != nil {
		return err
	}

	if err := goutils.RegisterENUMInValidator(
		v, "task_execute_class", goutils.ValidateStringENUM[TaskExecutionClassENUM](),
	); err != nil {
		return err
	}

	if err := goutils.RegisterENUMInValidator(
		v, "task_execute_state", goutils.ValidateStringENUM[TaskExecutionStateENUM](),
	); err != nil {
		return err
	}

	if err := goutils.RegisterENUMInValidator(
		v, "task_failure_disposition", goutils.ValidateStringENUM[TaskFailureDispositionENUM](),
	); err != nil {
		return err
	}

	if err := goutils.RegisterENUMInValidator(
		v, "ipc_msg_type", goutils.ValidateStringENUM[IPCMessageTypeEnum](),
	); err != nil {
		return err
	}

	if err := goutils.RegisterENUMInValidator(
		v, "workflow_state", goutils.ValidateStringENUM[WorkflowStateENUM](),
	); err != nil {
		return err
	}

	if err := goutils.RegisterENUMInValidator(
		v, "workflow_step_state", goutils.ValidateStringENUM[WorkflowStepStateENUM](),
	); err != nil {
		return err
	}

	return goutils.RegisterWithValidator(v)
}
