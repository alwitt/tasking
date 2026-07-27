// Package db - database controllers for system persistence
package db

import (
	"time"

	"github.com/alwitt/tasking/models"
)

// --------------------------------------------------------------------------------------
// Audit

// SystemEventAuditEntry system event audit entry
type SystemEventAuditEntry struct {
	models.SystemEventAudit
}

// TableName hard code table name
func (SystemEventAuditEntry) TableName() string {
	return "audit_system_events"
}

// --------------------------------------------------------------------------------------
// Tasks

// TaskEntry task DB entry
type TaskEntry struct {
	models.Task
}

// TableName hard code table name
func (TaskEntry) TableName() string {
	return "tasks"
}

// TaskExecutionEntry task execution DB entry
type TaskExecutionEntry struct {
	models.TaskExecution
	Task   TaskEntry           `gorm:"constraint:OnDelete:CASCADE;foreignKey:TaskID" validate:"-"`
	Parent *TaskExecutionEntry `gorm:"constraint:OnDelete:SET NULL;foreignKey:RetryParentExecutionID" validate:"-"`
}

// TableName hard code table name
func (TaskExecutionEntry) TableName() string {
	return "task_executions"
}

// --------------------------------------------------------------------------------------
// Workflow

// WorkflowEntry workflow DB entry
type WorkflowEntry struct {
	models.Workflow
}

// TableName hard code table name
func (WorkflowEntry) TableName() string {
	return "workflows"
}

// WorkflowStepEntry workflow step DB entry
type WorkflowStepEntry struct {
	models.WorkflowStep
	Workflow WorkflowEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:WorkflowID" validate:"-"`
}

// TableName hard code table name
func (WorkflowStepEntry) TableName() string {
	return "workflow_steps"
}

// WorkflowStepDependency records a directed dependency between two workflow steps,
// forming an edge of the workflow DAG. Step `StepID` depends on `DependsOnID`, meaning
// `DependsOnID` must complete before `StepID` becomes eligible to run.
type WorkflowStepDependency struct {
	// StepID the dependent (downstream) workflow step
	StepID string            `json:"step_id" gorm:"column:step_id;primaryKey" validate:"required"`
	Step   WorkflowStepEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:StepID" validate:"-"`
	// DependsOnID the prerequisite (upstream) workflow step which must complete first
	DependsOnID string            `json:"depends_on_id" gorm:"column:depends_on_id;primaryKey" validate:"required"`
	DependsOn   WorkflowStepEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:DependsOnID" validate:"-"`
	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName hard code table name
func (WorkflowStepDependency) TableName() string {
	return "workflow_step_dependencies"
}

// =======================================
// Linking workflow step with the tasks executing it

// WorkflowStepRunnerTask links a workflow step to the set of tasks which worked on it.
// A step may be associated with multiple tasks across its lifetime (e.g. the original
// attempt and subsequent manual retries).
type WorkflowStepRunnerTask struct {
	// StepID the workflow step
	StepID string            `json:"step_id" gorm:"column:step_id;primaryKey" validate:"required"`
	Step   WorkflowStepEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:StepID" validate:"-"`
	// TaskID the task which worked on the step
	TaskID string    `json:"task_id" gorm:"column:task_id;primaryKey" validate:"required"`
	Task   TaskEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:TaskID" validate:"-"`
	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName hard code table name
func (WorkflowStepRunnerTask) TableName() string {
	return "workflow_step_runner_tasks"
}
