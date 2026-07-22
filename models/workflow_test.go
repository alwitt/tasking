package models_test

import (
	"testing"
	"time"

	"github.com/alwitt/tasking/models"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowValidNextState(t *testing.T) {
	assert := assert.New(t)

	allStates := models.WorkflowStateENUM("").Values()

	// The complete set of permitted transitions per the workflow state machine.
	allowed := map[models.WorkflowStateENUM]map[models.WorkflowStateENUM]bool{
		models.WorkflowStatePending: {
			models.WorkflowStateRunning:    true,
			models.WorkflowStateCancelling: true,
		},
		models.WorkflowStateRunning: {
			models.WorkflowStateComplete:   true,
			models.WorkflowStateFailed:     true,
			models.WorkflowStateTimedOut:   true,
			models.WorkflowStateCancelling: true,
		},
		models.WorkflowStateFailed: {
			models.WorkflowStateRunning:    true,
			models.WorkflowStateCancelling: true,
		},
		models.WorkflowStateTimedOut: {
			models.WorkflowStateRunning:    true,
			models.WorkflowStateCancelling: true,
		},
		models.WorkflowStateCancelling: {
			models.WorkflowStateCancelled: true,
		},
		// COMPLETE and CANCELLED are terminal: no outgoing transitions.
		models.WorkflowStateComplete:  {},
		models.WorkflowStateCancelled: {},
	}

	// Verify every ordered pair of states against the allowed table.
	for _, from := range allStates {
		for _, to := range allStates {
			entry := models.Workflow{State: from}
			err := entry.ValidNextState(to)
			if allowed[from][to] {
				assert.NoErrorf(err, "'%s' -> '%s' should be allowed", from, to)
			} else {
				assert.Errorf(err, "'%s' -> '%s' should be rejected", from, to)
			}
		}
	}
}

func TestWorkflowValidNextStateUnknownState(t *testing.T) {
	assert := assert.New(t)

	entry := models.Workflow{State: models.WorkflowStateENUM("bogus")}
	assert.Error(entry.ValidNextState(models.WorkflowStateRunning))
}

func TestWorkflowStepValidNextState(t *testing.T) {
	assert := assert.New(t)

	allStates := models.WorkflowStepStateENUM("").Values()

	// The complete set of permitted transitions per the workflow step state machine.
	allowed := map[models.WorkflowStepStateENUM]map[models.WorkflowStepStateENUM]bool{
		models.WorkflowStepStateDefined: {
			models.WorkflowStepStatePending:   true,
			models.WorkflowStepStateCancelled: true,
		},
		models.WorkflowStepStatePending: {
			models.WorkflowStepStateRunning:   true,
			models.WorkflowStepStateCancelled: true,
		},
		models.WorkflowStepStateRunning: {
			models.WorkflowStepStateComplete:   true,
			models.WorkflowStepStateFailed:     true,
			models.WorkflowStepStateTimedOut:   true,
			models.WorkflowStepStateCancelling: true,
		},
		// FAILED / TIMED_OUT can be revived to DEFINED or cancelled directly (nothing to drain).
		models.WorkflowStepStateFailed: {
			models.WorkflowStepStateDefined:   true,
			models.WorkflowStepStateCancelled: true,
		},
		models.WorkflowStepStateTimedOut: {
			models.WorkflowStepStateDefined:   true,
			models.WorkflowStepStateCancelled: true,
		},
		models.WorkflowStepStateCancelling: {
			models.WorkflowStepStateCancelled: true,
		},
		// COMPLETE and CANCELLED are terminal: no outgoing transitions.
		models.WorkflowStepStateComplete:  {},
		models.WorkflowStepStateCancelled: {},
	}

	// Verify every ordered pair of states against the allowed table.
	for _, from := range allStates {
		for _, to := range allStates {
			entry := models.WorkflowStep{State: from}
			err := entry.ValidNextState(to)
			if allowed[from][to] {
				assert.NoErrorf(err, "'%s' -> '%s' should be allowed", from, to)
			} else {
				assert.Errorf(err, "'%s' -> '%s' should be rejected", from, to)
			}
		}
	}
}

func TestWorkflowStepValidNextStateUnknownState(t *testing.T) {
	assert := assert.New(t)

	entry := models.WorkflowStep{State: models.WorkflowStepStateENUM("bogus")}
	assert.Error(entry.ValidNextState(models.WorkflowStepStateDefined))
}

func TestNewWorkflowParameterIsValid(t *testing.T) {
	assert := assert.New(t)

	v := validator.New()
	require.NoError(t, models.RegisterWithValidator(v))

	// mkStep builds a step named `name` depending on `parents`.
	mkStep := func(name string, parents ...string) models.NewWorkflowStepParameter {
		parentSet := map[string]bool{}
		for _, parent := range parents {
			parentSet[parent] = true
		}
		return models.NewWorkflowStepParameter{
			Name:        name,
			Type:        "a step",
			RetryParams: models.DefaultTaskRetryParameters(),
			ParentSteps: parentSet,
		}
	}

	// mkWorkflow builds a workflow whose step map is keyed by each step's name.
	mkWorkflow := func(
		name string, steps ...models.NewWorkflowStepParameter,
	) models.NewWorkflowParameter {
		stepMap := map[string]models.NewWorkflowStepParameter{}
		for _, step := range steps {
			stepMap[step.Name] = step
		}
		return models.NewWorkflowParameter{Name: name, Steps: stepMap, Deadline: time.Now().UTC()}
	}

	type testCase struct {
		name    string
		param   models.NewWorkflowParameter
		isValid bool
	}

	tests := []testCase{
		// --- valid topologies ---
		{
			name:    "single root step",
			param:   mkWorkflow("wf", mkStep("a")),
			isValid: true,
		},
		{
			name: "linear chain",
			param: mkWorkflow(
				"wf", mkStep("a"), mkStep("b", "a"), mkStep("c", "b"),
			),
			isValid: true,
		},
		{
			name: "diamond fan-out and fan-in",
			param: mkWorkflow(
				"wf", mkStep("a"), mkStep("b", "a"), mkStep("c", "a"), mkStep("d", "b", "c"),
			),
			isValid: true,
		},
		{
			name: "multiple independent roots",
			param: mkWorkflow(
				"wf", mkStep("a"), mkStep("b"), mkStep("c", "a", "b"),
			),
			isValid: true,
		},
		{
			// A root drains and feeds real downstream work: the acyclic counterpart to the
			// "valid root with downstream cycle" case below.
			name: "root feeding a deeper acyclic subgraph",
			param: mkWorkflow(
				"wf",
				mkStep("root"), mkStep("x", "root"),
				mkStep("a", "x"), mkStep("b", "a"), mkStep("c", "b"),
			),
			isValid: true,
		},
		// --- structural validation failures ---
		{
			name:    "no steps",
			param:   mkWorkflow("wf"),
			isValid: false,
		},
		// --- dependency graph failures ---
		{
			name:    "step depends on self",
			param:   mkWorkflow("wf", mkStep("a", "a")),
			isValid: false,
		},
		{
			name:    "step depends on unknown parent",
			param:   mkWorkflow("wf", mkStep("a"), mkStep("b", "ghost")),
			isValid: false,
		},
		{
			name: "two node cycle",
			param: mkWorkflow(
				"wf", mkStep("a", "b"), mkStep("b", "a"),
			),
			isValid: false,
		},
		{
			name: "three node cycle",
			param: mkWorkflow(
				"wf", mkStep("a", "c"), mkStep("b", "a"), mkStep("c", "b"),
			),
			isValid: false,
		},
		{
			// A valid prefix (root -> x) drains fully, then the topological sort stalls on a
			// downstream cycle (a -> b -> c -> a) hanging off x that never enters the queue.
			name: "valid root with downstream cycle",
			param: mkWorkflow(
				"wf",
				mkStep("root"), mkStep("x", "root"),
				mkStep("a", "x", "b"), mkStep("b", "c"), mkStep("c", "a"),
			),
			isValid: false,
		},
		{
			// step map key does not match the step's own Name field
			name: "key name mismatch",
			param: models.NewWorkflowParameter{
				Name: "wf",
				Steps: map[string]models.NewWorkflowStepParameter{
					"x": mkStep("a"),
				},
			},
			isValid: false,
		},
	}

	for _, oneTest := range tests {
		err := oneTest.param.IsValid(v)
		if oneTest.isValid {
			assert.NoError(err, "case '%s' expected valid", oneTest.name)
		} else {
			assert.Error(err, "case '%s' expected invalid", oneTest.name)
		}
	}
}
