// Package db - database controllers for system persistence
package db

import (
	"time"

	"github.com/alwitt/tasking/models"
)

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
	Parent *taskExecutionEntry `gorm:"constraint:OnDelete:SET NULL;foreignKey:RetryParentExecutionID" validate:"-"`
}

// TableName hard code table name
func (taskExecutionEntry) TableName() string {
	return "task_executions"
}

// --------------------------------------------------------------------------------------
// Workflow

// workflowEntry workflow DB entry
type workflowEntry struct {
	models.Workflow
}

// TableName hard code table name
func (workflowEntry) TableName() string {
	return "workflows"
}

// workflowStepEntry workflow step DB entry
type workflowStepEntry struct {
	models.WorkflowStep
	Workflow workflowEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:WorkflowID" validate:"-"`
}

// TableName hard code table name
func (workflowStepEntry) TableName() string {
	return "workflow_steps"
}

// workflowStepDependency records a directed dependency between two workflow steps,
// forming an edge of the workflow DAG. Step `StepID` depends on `DependsOnID`, meaning
// `DependsOnID` must complete before `StepID` becomes eligible to run.
type workflowStepDependency struct {
	// StepID the dependent (downstream) workflow step
	StepID string            `json:"step_id" gorm:"column:step_id;primaryKey" validate:"required"`
	Step   workflowStepEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:StepID" validate:"-"`
	// DependsOnID the prerequisite (upstream) workflow step which must complete first
	DependsOnID string            `json:"depends_on_id" gorm:"column:depends_on_id;primaryKey" validate:"required"`
	DependsOn   workflowStepEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:DependsOnID" validate:"-"`
	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName hard code table name
func (workflowStepDependency) TableName() string {
	return "workflow_step_dependencies"
}

// =======================================
// Linking workflow step with the tasks executing it

// workflowStepRunnerTask links a workflow step to the set of tasks which worked on it.
// A step may be associated with multiple tasks across its lifetime (e.g. the original
// attempt and subsequent manual retries).
type workflowStepRunnerTask struct {
	// StepID the workflow step
	StepID string            `json:"step_id" gorm:"column:step_id;primaryKey" validate:"required"`
	Step   workflowStepEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:StepID" validate:"-"`
	// TaskID the task which worked on the step
	TaskID string    `json:"task_id" gorm:"column:task_id;primaryKey" validate:"required"`
	Task   taskEntry `gorm:"constraint:OnDelete:CASCADE;foreignKey:TaskID" validate:"-"`
	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName hard code table name
func (workflowStepRunnerTask) TableName() string {
	return "workflow_step_runner_tasks"
}
