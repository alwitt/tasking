// Package workflow - DAG workflow engine layered on the task engine
package workflow

import (
	"context"
	"fmt"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// RunWorkflowStepTaskProcessor the Step Runner: the single models.TaskExecutionProcessor the
// workflow engine registers with the task engine under models.WorkflowExecutionTaskName
// (__EXECUTE_WORKFLOW_STEP__). Every workflow step, regardless of its Type, runs as a task of
// that one name executed by this one processor. The Runner loads the step referenced by the
// task's Parameters, derives its parent workflow, and dispatches to the WorkflowStepProcessor
// registered for the step's Type. See workflow/DESIGN.md "Step Execution".
type RunWorkflowStepTaskProcessor struct {
	goutils.Component
	persistence db.Client
	validate    *validator.Validate
	// handlers maps a step Type to the processor which runs it. Fixed at construction and never
	// mutated afterwards, so it needs no lock.
	handlers map[string]models.WorkflowStepProcessor
}

/*
NewRunWorkflowStepTaskProcessor define a new workflow Step Runner.

	@param persistence db.Client - persistence client
	@param handlers map[string]models.WorkflowStepProcessor - step Type to processor mapping this
	    Runner supports. Fixed at construction; must contain no nil processors.
	@returns new Step Runner
*/
func NewRunWorkflowStepTaskProcessor(
	persistence db.Client,
	handlers map[string]models.WorkflowStepProcessor,
) (*RunWorkflowStepTaskProcessor, error) {
	logTags := log.Fields{
		"package": "tasking", "module": "workflow", "component": "workflow-step-runner",
	}

	if persistence == nil {
		return nil, goutils.NewBadInputError("persistence client is required", nil, true)
	}

	// Copy the handler mapping so the Runner owns an immutable snapshot, and reject nil handlers
	// up front rather than discovering them at dispatch time.
	ownedHandlers := make(map[string]models.WorkflowStepProcessor, len(handlers))
	for stepType, handler := range handlers {
		if handler == nil {
			return nil, goutils.NewBadInputError(
				fmt.Sprintf("can't register nil processor for step type %q", stepType), nil, true,
			)
		}
		ownedHandlers[stepType] = handler
	}

	validate := validator.New()
	if err := models.RegisterWithValidator(validate); err != nil {
		return nil, goutils.NewRuntimeError(
			"failed to install custom validation macros", err, true,
		)
	}

	return &RunWorkflowStepTaskProcessor{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		persistence: persistence,
		validate:    validate,
		handlers:    ownedHandlers,
	}, nil
}

/*
ProcessTaskExecution execute one workflow step. This implements models.TaskExecutionProcessor;
its return value becomes the task's terminal state. The Runner never reports to the workflow
scheduler - feedback flows entirely through the notify path.

	@param ctx context.Context - execution context
	@param taskEntry models.Task - the __EXECUTE_WORKFLOW_STEP__ task, whose Parameters carry the
	    workflow step ID
	@param executeEntry models.TaskExecution - the task execution instance (unused: the Runner
	    keys entirely off the task Parameters)
*/
func (r *RunWorkflowStepTaskProcessor) ProcessTaskExecution(
	ctx context.Context, taskEntry models.Task, _ models.TaskExecution,
) error {
	logTags := r.GetLogTagsForContext(ctx)

	// ------------------------------------------------------------------------------------
	// Recover the step ID from the task Parameters.
	//
	// A malformed blob is not transient: the task Parameters are Runner-owned plumbing written by
	// the workflow scheduler, so a payload that won't parse is a wiring/code bug that retrying can
	// never fix. Fail it immediately by wrapping in a NonRecoverableError rather than burning the
	// retry budget on a hopeless attempt.
	stepParam, err := models.ParseTaskParameterExecuteWorkflowStep(taskEntry.Parameters, r.validate)
	if err != nil {
		log.WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			WithField("task", taskEntry.ID).
			Error("workflow step execution task has malformed parameters")
		return models.NewNonRecoverableError(
			fmt.Sprintf("task %s has malformed workflow step execution parameters", taskEntry.ID),
			err,
			true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Load the step and its parent workflow.
	//
	// The workflow is derived from step.WorkflowID (the scheduler does not pass the workflow ID),
	// so the two can never disagree. A DB read failure here is potentially transient and is
	// returned verbatim as a StepPreprocessError, leaving the failure subject to the task engine's
	// normal per-attempt retry.
	var step models.WorkflowStep
	var workflow models.Workflow
	if dbErr := r.persistence.UseDatabaseInTransaction(
		ctx,
		func(dbCtx context.Context, dbClient db.Database) error {
			var err error
			if step, err = dbClient.GetWorkflowStep(dbCtx, stepParam.StepID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to fetch workflow step %s", stepParam.StepID), err, true,
				)
			}
			if workflow, err = dbClient.GetWorkflow(dbCtx, step.WorkflowID); err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf(
						"failed to fetch workflow %s for step %s", step.WorkflowID, stepParam.StepID,
					), err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewStepPreprocessError(
			fmt.Sprintf("failed to load workflow step %s for execution", stepParam.StepID),
			dbErr,
			true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Dispatch to the handler for the step's Type.
	//
	// No handler is a configuration error, never transient: retrying cannot make a handler appear.
	// Wrap in a NonRecoverableError so the task engine marks the task FAILED immediately.
	handler, found := r.handlers[step.Type]
	if !found {
		log.WithFields(goutils.UpdateCodePositionInTags(logTags)).
			WithField("step", step.ID).
			WithField("step-type", step.Type).
			Error("no workflow step processor registered for step type")
		return models.NewNonRecoverableError(
			fmt.Sprintf("no handler registered for workflow step type %q", step.Type), nil, true,
		)
	}

	// The handler's error is carried as the Core of the wrapping StepExecutionError, so errors.As
	// can still find any error the handler wrapped. It is potentially transient, so it stays
	// subject to the task engine's normal per-attempt retry.
	if err := handler.ProcessWorkflowStep(ctx, workflow, step); err != nil {
		return models.NewStepExecutionError(
			fmt.Sprintf("failed to process workflow step %s (type %q)", step.ID, step.Type), err, true,
		)
	}

	return nil
}
