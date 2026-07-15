// Package db - database controllers for system persistence
package db

import "github.com/alwitt/tasking/models"

// --------------------------------------------------------------------------------------
// Audit

// systemEventAuditEntry system event audit entry
type systemEventAuditEntry struct {
	models.SystemEventAudit
}

// TableName hard code table name
func (systemEventAuditEntry) TableName() string {
	return "audit_system_events"
}

// --------------------------------------------------------------------------------------
// Tasks

// taskEntry task DB entry
type taskEntry struct {
	models.Task
}

// TableName hard code table name
func (taskEntry) TableName() string {
	return "tasks"
}

// taskExecutionEntry task execution DB entry
type taskExecutionEntry struct {
	models.TaskExecution
	Task   taskEntry           `gorm:"constraint:OnDelete:CASCADE;foreignKey:TaskID" validate:"-"`
	Parent *taskExecutionEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:RetryParentExecutionID" validate:"-"`
}

// TableName hard code table name
func (taskExecutionEntry) TableName() string {
	return "task_executions"
}
