package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	"github.com/go-playground/validator/v10"
	"gorm.io/datatypes"
)

// ======================================================================================
// Workflow

const (
	// WorkflowExecutionTaskCreator the hard coded Creator stamped on every Task the workflow
	// scheduler submits to the Task Engine to run workflow step executions.
	//
	// **IMPORTANT:** Library users must never submit their own Tasks under this Creator. The
	// workflow scheduler subscribes to notify:creator:<this> to receive step-task feedback,
	// so any foreign task using it would be misread as workflow step feedback.
	WorkflowExecutionTaskCreator = "__TASKING_WORKFLOW_ENGINE_SCHEDULER__"
)

// WorkflowStateENUM workflow state ENUM value type
type WorkflowStateENUM string

const (
	// WorkflowStatePending workflow is pending execution
	WorkflowStatePending WorkflowStateENUM = "PENDING"

	// WorkflowStateRunning workflow is running
	WorkflowStateRunning WorkflowStateENUM = "RUNNING"

	// WorkflowStateComplete workflow finished
	WorkflowStateComplete WorkflowStateENUM = "COMPLETE"

	// WorkflowStateFailed workflow failed. Once it reached a failed state, it can be brought
	// back to the running state to reattempt the failed step. Or the workflow can be cancelled.
	WorkflowStateFailed WorkflowStateENUM = "FAILED"

	// WorkflowStateTimedOut workflow failed to complete before deadline Once it reached a
	// timed out state, it can be brought
	// back to the running state to reattempt the failed step. Or the workflow can be cancelled.
	WorkflowStateTimedOut WorkflowStateENUM = "TIMED_OUT"

	// WorkflowStateCancelling workflow is being cancelled
	WorkflowStateCancelling WorkflowStateENUM = "CANCELLING"

	// WorkflowStateCancelled workflow is cancelled
	WorkflowStateCancelled WorkflowStateENUM = "CANCELLED"
)

// Values all valid WorkflowStateENUM values
func (WorkflowStateENUM) Values() []WorkflowStateENUM {
	return []WorkflowStateENUM{
		WorkflowStatePending,
		WorkflowStateRunning,
		WorkflowStateComplete,
		WorkflowStateFailed,
		WorkflowStateTimedOut,
		WorkflowStateCancelling,
		WorkflowStateCancelled,
	}
}

// Workflow long running multi-step workflow built on a DAG of steps
type Workflow struct {
	// ID workflow ID
	ID string `json:"id" gorm:"column:id;primaryKey;unique" validate:"required"`

	// Name of the workflow
	Name string `json:"name" gorm:"column:name;not null;" validate:"required"`

	// Creator opaque identity of the entity that created this workflow. Set by the
	// submitting Client. tasking never interprets it; it is the routing key for the
	// workflow's audit events and notifications, so a creator can subscribe to the
	// workflows they created. Multi-tenancy/isolation is the embedding application's
	// responsibility.
	Creator string `json:"creator" gorm:"column:creator;index" validate:"required"`

	// State state of the workflow
	State WorkflowStateENUM `json:"state" gorm:"column:state;not null" validate:"required,workflow_state"`

	// Metadata relating to the workflow
	Metadata datatypes.JSON `json:"metadata,omitempty" gorm:"column:metadata;default:null"`

	// Deadline the workflow must be completed by this deadline
	Deadline time.Time `json:"deadline" gorm:"column:deadline;not null" validate:"required"`

	// StartedAt when the workflow started
	StartedAt *time.Time `json:"started_at,omitempty" gorm:"column:started_at;default:null"`

	// StoppedAt when the workflow reached termination points (i.e. completed or cancelled)
	StoppedAt *time.Time `json:"stopped_at,omitempty" gorm:"column:stopped_at;default:null"`

	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// ValidNextState verify the workflow can transition to new state
func (w Workflow) ValidNextState(newState WorkflowStateENUM) error {
	statesWithTransitions := map[WorkflowStateENUM]map[WorkflowStateENUM]bool{
		WorkflowStatePending: {
			WorkflowStateRunning:    true,
			WorkflowStateCancelling: true,
		},
		WorkflowStateRunning: {
			WorkflowStateComplete:   true,
			WorkflowStateFailed:     true,
			WorkflowStateTimedOut:   true,
			WorkflowStateCancelling: true,
		},
		WorkflowStateFailed: {
			WorkflowStateRunning:    true,
			WorkflowStateCancelling: true,
		},
		WorkflowStateTimedOut: {
			WorkflowStateRunning:    true,
			WorkflowStateCancelling: true,
		},
		WorkflowStateCancelling: {
			WorkflowStateCancelled: true,
		},
	}

	availableNextStates, ok := statesWithTransitions[w.State]
	if !ok {
		return goutils.NewConsistencyError(
			fmt.Sprintf("workflow can't transition out of state '%s'", w.State), nil, true,
		)
	}

	if _, ok := availableNextStates[newState]; !ok {
		return goutils.NewConsistencyError(
			fmt.Sprintf("workflow can't transition from '%s' to '%s'", w.State, newState), nil, true,
		)
	}

	return nil
}

// ======================================================================================
// Workflow Step

// WorkflowStepStateENUM workflow step state ENUM value type
type WorkflowStepStateENUM string

const (
	// WorkflowStepStateDefined a newly defined workflow step
	WorkflowStepStateDefined WorkflowStepStateENUM = "DEFINED"

	// WorkflowStepStatePending workflow step is pending execution
	WorkflowStepStatePending WorkflowStepStateENUM = "PENDING"

	// WorkflowStepStateRunning workflow step is running
	WorkflowStepStateRunning WorkflowStepStateENUM = "RUNNING"

	// WorkflowStepStateComplete workflow step finished
	WorkflowStepStateComplete WorkflowStepStateENUM = "COMPLETE"

	// WorkflowStepStateFailed workflow step failed. Once it reached a failed state, it can
	// be retried, or cancelled along with the rest of workflow.
	WorkflowStepStateFailed WorkflowStepStateENUM = "FAILED"

	// WorkflowStepStateTimedOut workflow step failed to complete before deadline. Once it reached
	// a timed out state, it can be retried, or cancelled along with the rest of workflow.
	WorkflowStepStateTimedOut WorkflowStepStateENUM = "TIMED_OUT"

	// WorkflowStepStateCancelling workflow step being cancelled
	WorkflowStepStateCancelling WorkflowStepStateENUM = "CANCELLING"

	// WorkflowStepStateCancelled workflow step being cancelled
	WorkflowStepStateCancelled WorkflowStepStateENUM = "CANCELLED"
)

// Values all valid WorkflowStepStateENUM values
func (WorkflowStepStateENUM) Values() []WorkflowStepStateENUM {
	return []WorkflowStepStateENUM{
		WorkflowStepStateDefined,
		WorkflowStepStatePending,
		WorkflowStepStateRunning,
		WorkflowStepStateComplete,
		WorkflowStepStateFailed,
		WorkflowStepStateTimedOut,
		WorkflowStepStateCancelling,
		WorkflowStepStateCancelled,
	}
}

// WorkflowStepParents structure
type WorkflowStepParents struct {
	// ParentStepNames name of the parent steps
	ParentStepNames []string `json:"parents,omitempty"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (s *WorkflowStepParents) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("core value is not byte slice")
	}

	var parsed WorkflowStepParents
	err := json.Unmarshal(bytes, &parsed)
	*s = parsed
	return err
}

// Value return json value, implement driver.Valuer interface
func (s WorkflowStepParents) Value() (driver.Value, error) {
	return json.Marshal(&s)
}

// WorkflowStep one step of a long running multi-step workflow
type WorkflowStep struct {
	// ID workflow step ID
	ID string `json:"id" gorm:"column:id;primaryKey;unique" validate:"required"`

	// Name of the workflow step.
	Name string `json:"name" gorm:"column:name;not null;index:unique_step_in_workflow,unique;" validate:"required"`

	// WorkflowID parent workflow ID
	WorkflowID string `json:"workflow_id" gorm:"column:workflow_id;not null;index:unique_step_in_workflow,unique;" validate:"required"`

	// Creator opaque identity of the entity that created the parent workflow. Denormalized
	// from the workflow so step audit events can route without a join. Set at definition.
	Creator string `json:"creator" gorm:"column:creator;index" validate:"required"`

	// Type workflow step type which indicates which processor should run the step
	Type string `json:"type" gorm:"column:type;not null" validate:"required"`

	// State state of the workflow step
	State WorkflowStepStateENUM `json:"state" gorm:"column:state;not null" validate:"required,workflow_step_state"`

	// UserRestarted whether the user has revived this step (pure metadata, not a state)
	UserRestarted bool `json:"user_restarted" gorm:"column:user_restarted;not null;default:false"`

	// Parents steps this step depends on
	Parents WorkflowStepParents `json:"parents" gorm:"column:parents;not null"`

	// Parameters optional parameters needed for processing the step
	Parameters datatypes.JSON `json:"parameters,omitempty" gorm:"column:parameters;default:null"`

	// Metadata a metadata relating to the workflow step
	Metadata datatypes.JSON `json:"metadata,omitempty" gorm:"column:metadata;default:null"`

	// RetryParams retry parameters in case of failure
	RetryParams TaskRetryParameters `json:"retry_params" gorm:"column:retry_params;not null" validate:"required"`

	// Deadline the workflow step must be completed by this deadline
	Deadline time.Time `json:"deadline" gorm:"column:deadline;not null" validate:"required"`

	// StartedAt when the workflow step started
	StartedAt *time.Time `json:"started_at,omitempty" gorm:"column:started_at;default:null"`

	// StoppedAt when the workflow step reached termination points (i.e. completed or cancelled)
	StoppedAt *time.Time `json:"stopped_at,omitempty" gorm:"column:stopped_at;default:null"`

	// CreatedAt entry creation timestamp
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt entry update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// ValidNextState verify the workflow step can transition to new state
func (s WorkflowStep) ValidNextState(newState WorkflowStepStateENUM) error {
	statesWithTransitions := map[WorkflowStepStateENUM]map[WorkflowStepStateENUM]bool{
		WorkflowStepStateDefined: {
			WorkflowStepStatePending:   true,
			WorkflowStepStateCancelled: true,
		},
		WorkflowStepStatePending: {
			WorkflowStepStateRunning:   true,
			WorkflowStepStateCancelled: true,
		},
		WorkflowStepStateRunning: {
			WorkflowStepStateComplete:   true,
			WorkflowStepStateFailed:     true,
			WorkflowStepStateTimedOut:   true,
			WorkflowStepStateCancelling: true,
		},
		WorkflowStepStateFailed: {
			WorkflowStepStateDefined:   true,
			WorkflowStepStateCancelled: true,
		},
		WorkflowStepStateTimedOut: {
			WorkflowStepStateDefined:   true,
			WorkflowStepStateCancelled: true,
		},
		WorkflowStepStateCancelling: {
			WorkflowStepStateCancelled: true,
		},
	}

	availableNextStates, ok := statesWithTransitions[s.State]
	if !ok {
		return goutils.NewConsistencyError(
			fmt.Sprintf("workflow step can't transition out of state '%s'", s.State), nil, true,
		)
	}

	if _, ok := availableNextStates[newState]; !ok {
		return goutils.NewConsistencyError(
			fmt.Sprintf(
				"workflow step can't transition from '%s' to '%s'", s.State, newState,
			), nil, true,
		)
	}

	return nil
}

// ======================================================================================
// Workflow Setup Struct

// NewWorkflowStepParameter new workflow step parameters
type NewWorkflowStepParameter struct {
	// Name of the workflow step
	Name string `json:"name" validate:"required"`

	// Type workflow step type which indicates which processor should run the step
	Type string `json:"type" validate:"required"`

	// Parameters optional parameters needed for processing the step
	Parameters interface{} `json:"parameters,omitempty"`

	// Metadata relating to the workflow step
	Metadata interface{} `json:"metadata,omitempty"`

	// RetryParams retry parameters in case of failure
	RetryParams TaskRetryParameters `json:"retry_params" validate:"required"`

	// ParentSteps parent steps this step depends on. Parent steps are specified by name.
	ParentSteps map[string]bool `json:"parent_steps"`
}

// NewWorkflowParameter new workflow parameters
type NewWorkflowParameter struct {
	// Name of the workflow
	Name string `json:"name" validate:"required"`

	// Metadata relating to the workflow
	Metadata interface{} `json:"metadata,omitempty"`

	// Deadline the workflow must be completed by this deadline
	Deadline time.Time `json:"deadline" validate:"required"`

	// Steps of the workflow keyed by each step's name
	Steps map[string]NewWorkflowStepParameter `json:"steps" validate:"required,gte=1,dive"`
}

/*
IsValid verifies this new workflow parameter is valid
  - The steps did not depend on self
  - The steps did not depend on unknown parent steps
  - The step dependencies formed a DAG
*/
func (p NewWorkflowParameter) IsValid(validator *validator.Validate) error {
	// Verify the basic data is good
	if err := validator.Struct(&p); err != nil {
		return goutils.NewValidationError("new workflow parameters is not valid", err, true)
	}

	// Prepare for topological sort
	inDegreePerNode := map[string]int{}
	processQueue := []string{}
	childOfStep := map[string][]string{}
	for stepName, oneStep := range p.Steps {
		if stepName != oneStep.Name {
			return goutils.NewConsistencyError(
				fmt.Sprintf(
					"step '%s' is keyed under mismatched name '%s'", oneStep.Name, stepName,
				), nil, true,
			)
		}
		// Basic sanity check on the step
		for oneParentName := range oneStep.ParentSteps {
			// Can't depend on self
			if oneParentName == oneStep.Name {
				return goutils.NewConsistencyError(
					fmt.Sprintf("workflow step %s depends on self", oneStep.Name), nil, true,
				)
			}
			// Parent step does not belong to the workflow
			if _, ok := p.Steps[oneParentName]; !ok {
				return goutils.NewConsistencyError(
					fmt.Sprintf(
						"workflow step %s depends on unknown step %s", oneStep.Name, oneParentName,
					), nil, true,
				)
			}
			// Record which parents this node depend on
			childOfStep[oneParentName] = append(childOfStep[oneParentName], oneStep.Name)
		}
		inDegrees := len(oneStep.ParentSteps)
		// Is a root node
		if inDegrees == 0 {
			processQueue = append(processQueue, oneStep.Name)
		}
		// Record node's in-degree
		inDegreePerNode[oneStep.Name] = inDegrees
	}

	// Run topological sort
	processed := 0
	for len(processQueue) > 0 {
		currentStep := processQueue[0]
		processQueue = processQueue[1:]
		processed++

		// Decrease in-degree of all child nodes
		if children, haveChild := childOfStep[currentStep]; haveChild {
			for _, oneChild := range children {
				inDegreePerNode[oneChild]--
				// Child has in-degree of zero
				if inDegreePerNode[oneChild] == 0 {
					processQueue = append(processQueue, oneChild)
				}
			}
		}
	}

	// Did not decrease all step in-degrees to zero.
	if processed < len(p.Steps) {
		return goutils.NewConsistencyError("workflow is not a DAG", nil, true)
	}

	return nil
}
