package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/models"
	"github.com/oklog/ulid/v2"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// ======================================================================================
// Workflows

/*
DefineNewWorkflow define a new workflow

	@param ctx context.Context - execution context
	@param workflowSpec models.NewWorkflowParameter - the workflow specification
	@param creator string - the entity defining the workflow
	@returns new workflow entry
*/
func (c *databaseImpl) DefineNewWorkflow(
	ctx context.Context,
	workflowSpec models.NewWorkflowParameter,
	creator string,
) (models.Workflow, error) {
	if err := workflowSpec.IsValid(c.validator); err != nil {
		return models.Workflow{}, err
	}

	// Construct the workflow first
	workflowMetadataStr, _ := json.Marshal(&workflowSpec.Metadata)
	newWorkflow := workflowEntry{
		Workflow: models.Workflow{
			ID:       ulid.Make().String(),
			Name:     workflowSpec.Name,
			Creator:  creator,
			State:    models.WorkflowStatePending,
			Deadline: workflowSpec.Deadline,
			Metadata: datatypes.JSON(workflowMetadataStr),
		},
	}

	if err := c.validator.Struct(&newWorkflow); err != nil {
		return models.Workflow{}, goutils.NewValidationError(
			fmt.Sprintf("new workflow entry '%s' is not valid", workflowSpec.Name), err, true,
		)
	}

	// Define the workflow steps
	steps := []workflowStepEntry{}
	stepsByName := map[string]workflowStepEntry{}
	for _, oneStep := range workflowSpec.Steps {
		paramsStr, _ := json.Marshal(&oneStep.Parameters)
		stepMetadataStr, _ := json.Marshal(&oneStep.Metadata)

		parentSteps := models.WorkflowStepParents{ParentStepNames: []string{}}
		for oneParent := range oneStep.ParentSteps {
			parentSteps.ParentStepNames = append(parentSteps.ParentStepNames, oneParent)
		}

		newStep := workflowStepEntry{
			WorkflowStep: models.WorkflowStep{
				ID:          ulid.Make().String(),
				Name:        oneStep.Name,
				WorkflowID:  newWorkflow.ID,
				Creator:     creator,
				Type:        oneStep.Type,
				State:       models.WorkflowStepStateDefined,
				Parameters:  datatypes.JSON(paramsStr),
				Metadata:    datatypes.JSON(stepMetadataStr),
				RetryParams: oneStep.RetryParams,
				Parents:     parentSteps,
				Deadline:    workflowSpec.Deadline,
			},
		}

		if err := c.validator.Struct(&newStep); err != nil {
			return models.Workflow{}, goutils.NewValidationError(
				fmt.Sprintf("new workflow step entry '%s' is not valid", oneStep.Name), err, true,
			)
		}

		steps = append(steps, newStep)
		stepsByName[newStep.Name] = newStep
	}

	// Define the links between parent and child steps. NOTE: a step's parents are persisted
	// in two forms written together here and never mutated afterward: the denormalized
	// WorkflowStep.Parents JSON blob (read by ListWorkflowSteps) and the workflow_step_
	// dependencies edge rows (read by ListWorkflowStepsReadyToRun). They must stay in sync;
	// the edge rows are authoritative for DAG queries.
	childToParentStepLinks := []workflowStepDependency{}
	for _, childStep := range steps {
		// Generate a link for each child-parent tuple
		for _, oneParent := range childStep.Parents.ParentStepNames {
			parentStep, ok := stepsByName[oneParent]
			if !ok {
				return models.Workflow{}, goutils.NewConsistencyError(
					fmt.Sprintf(
						"child step %s referencing undefined parent step %s", childStep.Name, oneParent,
					),
					nil,
					true,
				)
			}
			childToParentStepLinks = append(childToParentStepLinks, workflowStepDependency{
				StepID: childStep.ID, DependsOnID: parentStep.ID,
			})
		}
	}

	// Record the workflow
	if tmp := c.db.Create(&newWorkflow); tmp.Error != nil {
		return models.Workflow{}, models.NewSQLError(
			fmt.Sprintf("failed to define new workflow '%s'", newWorkflow.Name), tmp.Error, true,
		)
	}
	// Record the workflow steps
	if tmp := c.db.Create(&steps); tmp.Error != nil {
		return models.Workflow{}, models.NewSQLError(
			fmt.Sprintf("failed to define new workflow '%s' steps", newWorkflow.Name), tmp.Error, true,
		)
	}
	// Record the links between workflow steps
	if len(childToParentStepLinks) > 0 {
		if tmp := c.db.Create(&childToParentStepLinks); tmp.Error != nil {
			return models.Workflow{}, models.NewSQLError(
				fmt.Sprintf("failed to define new workflow '%s' step dependencies", newWorkflow.Name),
				tmp.Error, true,
			)
		}
	}

	// Record audit event
	if _, err := c.defineNewSystemEvent(
		ctx, models.SystemEventTypeDefineWorkflow,
		&models.SystemEventWorkflowEvents{WorkflowID: newWorkflow.ID, Creator: creator},
	); err != nil {
		return models.Workflow{}, models.NewSQLError(
			fmt.Sprintf("failed to record define workflow '%s' system event", newWorkflow.Name),
			err, true,
		)
	}

	return newWorkflow.Workflow, nil
}

// getWorkflowDBEntry fetch a workflow entry
func (c *databaseImpl) getWorkflowDBEntry(workflowID string) (workflowEntry, error) {
	var entry workflowEntry
	tmp := c.db.Model(&workflowEntry{}).Where("id = ?", workflowID).First(&entry)
	return entry, notFoundOrError(tmp.Error, "workflow", workflowID)
}

/*
GetWorkflow fetch a workflow entry

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@returns workflow entry
*/
func (c *databaseImpl) GetWorkflow(
	_ context.Context, workflowID string,
) (models.Workflow, error) {
	entry, err := c.getWorkflowDBEntry(workflowID)
	if err != nil {
		return models.Workflow{}, err
	}
	return entry.Workflow, nil
}

// updateWorkflowState update the workflow state
func (c *databaseImpl) updateWorkflowState(
	ctx context.Context,
	workflowID string,
	newState models.WorkflowStateENUM,
	timestamp time.Time,
) error {
	entry, err := c.getWorkflowDBEntry(workflowID)
	if err != nil {
		return err
	}

	if err := entry.ValidNextState(newState); err != nil {
		return goutils.NewConsistencyError(
			fmt.Sprintf("can't transition workflow %s to state %s", workflowID, newState), err, true,
		)
	}

	var tmp *gorm.DB
	switch newState {
	case models.WorkflowStateRunning:
		if entry.State == models.WorkflowStatePending {
			// Record the start time
			tmp = c.db.
				Model(&workflowEntry{}).
				Where("id = ?", workflowID).
				Updates(workflowEntry{
					Workflow: models.Workflow{State: newState, StartedAt: &timestamp},
				})
		} else {
			tmp = c.db.Model(&workflowEntry{}).Where("id = ?", workflowID).Update("state", newState)
		}

	case models.WorkflowStateComplete:
		fallthrough
	case models.WorkflowStateCancelled:
		// Record the stop time
		tmp = c.db.
			Model(&workflowEntry{}).
			Where("id = ?", workflowID).
			Updates(workflowEntry{
				Workflow: models.Workflow{State: newState, StoppedAt: &timestamp},
			})

	default:
		tmp = c.db.Model(&workflowEntry{}).Where("id = ?", workflowID).Update("state", newState)
	}

	if tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("workflow %s state update failed", workflowID), tmp.Error, true,
		)
	}

	// Record workflow state change event. PENDING is the birth state (set at definition,
	// covered by DEFINE_WORKFLOW) and is never transitioned to afterward, so it has no event.
	eventTypeMap := map[models.WorkflowStateENUM]models.SystemEventTypeENUM{
		models.WorkflowStateRunning:    models.SystemEventTypeWorkflowRunning,
		models.WorkflowStateComplete:   models.SystemEventTypeWorkflowComplete,
		models.WorkflowStateFailed:     models.SystemEventTypeWorkflowFailed,
		models.WorkflowStateTimedOut:   models.SystemEventTypeWorkflowTimedOut,
		models.WorkflowStateCancelling: models.SystemEventTypeWorkflowCancelling,
		models.WorkflowStateCancelled:  models.SystemEventTypeWorkflowCancelled,
	}
	if eventType, found := eventTypeMap[newState]; found {
		if _, err := c.defineNewSystemEvent(
			ctx, eventType,
			&models.SystemEventWorkflowEvents{WorkflowID: workflowID, Creator: entry.Creator},
		); err != nil {
			return models.NewSQLError(
				fmt.Sprintf(
					"failed to record workflow %s change state to '%s' system event",
					workflowID, newState,
				), err, true,
			)
		}
	}

	return nil
}

/*
MarkWorkflowPending mark workflow is pending execution

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowPending(
	ctx context.Context, workflowID string, timestamp time.Time,
) error {
	return c.updateWorkflowState(ctx, workflowID, models.WorkflowStatePending, timestamp)
}

/*
MarkWorkflowRunning mark workflow is running

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowRunning(
	ctx context.Context, workflowID string, timestamp time.Time,
) error {
	return c.updateWorkflowState(ctx, workflowID, models.WorkflowStateRunning, timestamp)
}

/*
MarkWorkflowComplete mark workflow is complete

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowComplete(
	ctx context.Context, workflowID string, timestamp time.Time,
) error {
	return c.updateWorkflowState(ctx, workflowID, models.WorkflowStateComplete, timestamp)
}

/*
MarkWorkflowFailed mark workflow has failed

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowFailed(
	ctx context.Context, workflowID string, timestamp time.Time,
) error {
	return c.updateWorkflowState(ctx, workflowID, models.WorkflowStateFailed, timestamp)
}

/*
MarkWorkflowTimedOut mark workflow has timed out

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowTimedOut(
	ctx context.Context, workflowID string, timestamp time.Time,
) error {
	return c.updateWorkflowState(ctx, workflowID, models.WorkflowStateTimedOut, timestamp)
}

/*
MarkWorkflowCancelling mark workflow is being cancelled

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowCancelling(
	ctx context.Context, workflowID string, timestamp time.Time,
) error {
	return c.updateWorkflowState(ctx, workflowID, models.WorkflowStateCancelling, timestamp)
}

/*
MarkWorkflowCancelled mark workflow is cancelled

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowCancelled(
	ctx context.Context, workflowID string, timestamp time.Time,
) error {
	return c.updateWorkflowState(ctx, workflowID, models.WorkflowStateCancelled, timestamp)
}

/*
ListWorkflows list workflows

	@param ctx context.Context - execution context
	@param filters WorkflowQueryFilter - query filtering conditions
	@returns list of workflows
*/
func (c *databaseImpl) ListWorkflows(
	_ context.Context, filters WorkflowQueryFilter,
) ([]models.Workflow, error) {
	if err := c.validator.Struct(&filters); err != nil {
		return nil, goutils.NewValidationError("workflow query filter is not valid", err, true)
	}

	query := c.db.Model(&workflowEntry{})

	if len(filters.TargetIDs) > 0 {
		query = query.Where("id in ?", filters.TargetIDs)
	}

	if len(filters.TargetNames) > 0 {
		query = query.Where("name in ?", filters.TargetNames)
	}

	if len(filters.TargetStates) > 0 {
		query = query.Where("state in ?", filters.TargetStates)
	}

	if filters.TargetDeadline != nil {
		query = query.Where("deadline <= ?", *filters.TargetDeadline)
	}

	if filters.Limit != nil {
		query = query.Limit(*filters.Limit)
	}
	if filters.Offset != nil {
		query = query.Offset(*filters.Offset)
	}

	query = query.Order("created_at")

	var entries []workflowEntry
	if tmp := query.Find(&entries); tmp.Error != nil {
		return nil, models.NewSQLError("failed to list workflows", tmp.Error, true)
	}

	result := []models.Workflow{}
	for _, entry := range entries {
		result = append(result, entry.Workflow)
	}

	return result, nil
}

// isTerminalWorkflowState reports whether a workflow state admits no further transition,
// i.e. COMPLETE or CANCELLED (see models.Workflow.ValidNextState).
func isTerminalWorkflowState(state models.WorkflowStateENUM) bool {
	return state == models.WorkflowStateComplete || state == models.WorkflowStateCancelled
}

/*
DeleteWorkflow delete a workflow and reap the tasks that executed its steps.

A workflow-owned task is the workflow's execution-history store, so a task never outlives its
workflow (see the workflow DESIGN's "Failure history and its retention"). This is the
privileged teardown path that deletes those tasks directly, bypassing the DeleteTask linkage
guard.

Only a terminal workflow (COMPLETE / CANCELLED) may be deleted — a non-terminal workflow must
be cancelled first, which guarantees no step task is still in-flight when it is reaped.

The reap is capture-then-cascade: the step -> task pointers live in workflow_step_runner_tasks,
whose rows cascade away when EITHER the step or the task is deleted. So the linked task IDs are
read BEFORE any delete; then the tasks are deleted (cascading their task_executions history and
the link rows), and finally the workflow is deleted (cascading its steps and dependency edges).
The caller runs this inside a single transaction (UseDatabaseInTransaction).

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
*/
func (c *databaseImpl) DeleteWorkflow(ctx context.Context, workflowID string) error {
	// Fetch the workflow first to capture its details for the audit record and to gate on state
	entry, err := c.getWorkflowDBEntry(workflowID)
	if err != nil {
		return err
	}

	// Terminal gate: a live workflow may still have in-flight step tasks and racing scheduler
	// activity; require it be cancelled (or complete) before teardown.
	if !isTerminalWorkflowState(entry.State) {
		return goutils.NewConsistencyError(
			fmt.Sprintf(
				"workflow %s in state %s is not terminal; cancel it before deleting",
				workflowID, entry.State,
			), nil, true,
		)
	}

	// Capture the workflow's steps, then the tasks linked to those steps — BEFORE any delete,
	// since deleting the workflow (or its steps) cascades away the link rows that are the only
	// pointers from steps to tasks.
	steps, err := c.ListWorkflowSteps(ctx, workflowID)
	if err != nil {
		return err
	}
	if len(steps) > 0 {
		stepIDs := make([]string, 0, len(steps))
		for _, step := range steps {
			stepIDs = append(stepIDs, step.ID)
		}

		var linkEntries []workflowStepRunnerTask
		if tmp := c.db.
			Model(&workflowStepRunnerTask{}).
			Where("step_id in ?", stepIDs).
			Find(&linkEntries); tmp.Error != nil {
			return models.NewSQLError(
				fmt.Sprintf("failed to list task links of workflow %s steps", workflowID),
				tmp.Error, true,
			)
		}

		taskIDSet := map[string]bool{}
		for _, link := range linkEntries {
			taskIDSet[link.TaskID] = true
		}
		if len(taskIDSet) > 0 {
			taskIDs := make([]string, 0, len(taskIDSet))
			for taskID := range taskIDSet {
				taskIDs = append(taskIDs, taskID)
			}
			// Reap the step tasks. This cascades their task_executions (history) and the
			// workflow_step_runner_tasks link rows via the task-side FK cascade.
			if tmp := c.db.Where("id in ?", taskIDs).Delete(&taskEntry{}); tmp.Error != nil {
				return models.NewSQLError(
					fmt.Sprintf("failed to reap tasks of workflow %s", workflowID), tmp.Error, true,
				)
			}
		}
	}

	// Delete the workflow. This cascades its steps, their dependency edges, and any remaining
	// link rows via the step-side FK cascade.
	tmp := c.db.Where("id = ?", workflowID).Delete(&workflowEntry{})
	if tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to delete workflow %s", workflowID), tmp.Error, true,
		)
	}

	// Record audit event
	if _, err := c.defineNewSystemEvent(
		ctx, models.SystemEventTypeDeleteWorkflow,
		&models.SystemEventWorkflowEvents{WorkflowID: workflowID, Creator: entry.Creator},
	); err != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to record delete workflow %s system event", workflowID), err, true,
		)
	}

	return nil
}

/*
UpdateWorkflowDeadline set a new deadline for a workflow and re-sync it onto the workflow's
steps.

Step deadlines are derived from (and mirror) the workflow deadline, so the new deadline is
applied to every step which has not yet reached a terminal state (COMPLETE or CANCELLED). A
terminal step's deadline is left untouched.

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@param deadline time.Time - the new deadline
*/
func (c *databaseImpl) UpdateWorkflowDeadline(
	ctx context.Context, workflowID string, deadline time.Time,
) error {
	// Ensure the workflow exists before mutating anything (also supplies Creator for the audit)
	entry, err := c.getWorkflowDBEntry(workflowID)
	if err != nil {
		return err
	}

	// Update the workflow deadline
	if tmp := c.db.
		Model(&workflowEntry{}).
		Where("id = ?", workflowID).
		UpdateColumn("deadline", &deadline); tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to update workflow %s deadline", workflowID), tmp.Error, true,
		)
	}

	// Re-sync the deadline onto every non-terminal step of the workflow
	if tmp := c.db.
		Model(&workflowStepEntry{}).
		Where("workflow_id = ? and state not in ?", workflowID, []models.WorkflowStepStateENUM{
			models.WorkflowStepStateComplete,
			models.WorkflowStepStateCancelled,
		}).
		UpdateColumn("deadline", &deadline); tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to update workflow %s step deadlines", workflowID), tmp.Error, true,
		)
	}

	// Record audit event
	if _, err := c.defineNewSystemEvent(
		ctx, models.SystemEventTypeWorkflowDeadlineUpdate,
		&models.SystemEventWorkflowEvents{WorkflowID: workflowID, Creator: entry.Creator},
	); err != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to record workflow %s deadline update system event", workflowID),
			err, true,
		)
	}

	return nil
}

// ======================================================================================
// Workflow Steps

// getWorkflowStepDBEntry fetch a workflow step entry
func (c *databaseImpl) getWorkflowStepDBEntry(stepID string) (workflowStepEntry, error) {
	var entry workflowStepEntry
	tmp := c.db.Model(&workflowStepEntry{}).Where("id = ?", stepID).First(&entry)
	return entry, notFoundOrError(tmp.Error, "workflow step", stepID)
}

/*
GetWorkflowStep fetch a workflow step entry

	@param ctx context.Context - execution context
	@param stepID string - workflow step ID
	@returns workflow step entry
*/
func (c *databaseImpl) GetWorkflowStep(
	_ context.Context, stepID string,
) (models.WorkflowStep, error) {
	entry, err := c.getWorkflowStepDBEntry(stepID)
	if err != nil {
		return models.WorkflowStep{}, err
	}
	return entry.WorkflowStep, nil
}

/*
ListWorkflowSteps list the workflow steps associated with a workflow.

The steps are returned after topological sort and in alphabetical order for nodes at the
same depth.

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@returns list of workflow steps
*/
func (c *databaseImpl) ListWorkflowSteps(
	_ context.Context, workflowID string,
) ([]models.WorkflowStep, error) {
	var entries []workflowStepEntry
	if tmp := c.db.
		Model(&workflowStepEntry{}).
		Where("workflow_id = ?", workflowID).
		Find(&entries); tmp.Error != nil {
		return nil, models.NewSQLError(
			fmt.Sprintf("failed to list workflow %s steps", workflowID), tmp.Error, true,
		)
	}

	// Index the steps by name and prepare for a level-by-level topological sort. Each step
	// records the names of the parent steps it depends on, mirroring the DAG edges.
	stepByName := map[string]models.WorkflowStep{}
	inDegreePerStep := map[string]int{}
	childrenOfStep := map[string][]string{}
	currentLevel := []string{}
	for _, entry := range entries {
		stepByName[entry.Name] = entry.WorkflowStep
		inDegree := len(entry.Parents.ParentStepNames)
		inDegreePerStep[entry.Name] = inDegree
		for _, parent := range entry.Parents.ParentStepNames {
			childrenOfStep[parent] = append(childrenOfStep[parent], entry.Name)
		}
		// Root steps make up the first level
		if inDegree == 0 {
			currentLevel = append(currentLevel, entry.Name)
		}
	}

	// Walk the DAG one depth at a time, emitting steps within a level in alphabetical order.
	result := []models.WorkflowStep{}
	for len(currentLevel) > 0 {
		sort.Strings(currentLevel)
		nextLevel := []string{}
		for _, stepName := range currentLevel {
			result = append(result, stepByName[stepName])
			for _, child := range childrenOfStep[stepName] {
				inDegreePerStep[child]--
				if inDegreePerStep[child] == 0 {
					nextLevel = append(nextLevel, child)
				}
			}
		}
		currentLevel = nextLevel
	}

	// A failure to emit every step means the persisted steps did not form a DAG.
	if len(result) != len(entries) {
		return nil, goutils.NewConsistencyError(
			fmt.Sprintf("workflow %s steps do not form a DAG", workflowID), nil, true,
		)
	}

	return result, nil
}

/*
ListWorkflowStepsReadyToRun list the workflow steps of a workflow which are ready to run.

A step is ready to run when it is in the DEFINED state and all of its
parent steps (if any) have completed.

This considers only step-level readiness; it does NOT gate on the parent workflow's state.
The caller (scheduler) is responsible for the soft-stop / hard-stop policy — e.g. not
dispatching startable steps of a TIMED_OUT / CANCELLING workflow (see the workflow DESIGN's
Process Workflow handler).

	@param ctx context.Context - execution context
	@param workflowID string - workflow ID
	@returns list of workflow steps ready to run
*/
func (c *databaseImpl) ListWorkflowStepsReadyToRun(
	_ context.Context, workflowID string,
) ([]models.WorkflowStep, error) {
	// Walk each candidate step out to its parents through the dependency edges. The LEFT
	// JOINs keep root steps (which have no dependency rows, leaving parent.state NULL) in
	// the result set. A step qualifies only when none of its parents are incomplete; since
	// COUNT ignores NULLs, root steps naturally pass the HAVING clause.
	var entries []workflowStepEntry
	tmp := c.db.
		Table("workflow_steps as step").
		Select("step.*").
		Joins("LEFT JOIN workflow_step_dependencies AS dep ON dep.step_id = step.id").
		Joins("LEFT JOIN workflow_steps AS parent ON parent.id = dep.depends_on_id").
		Where("step.workflow_id = ?", workflowID).
		Where("step.state in ?", []models.WorkflowStepStateENUM{
			models.WorkflowStepStateDefined,
		}).
		Group("step.id").
		Having("COUNT(CASE WHEN parent.state <> ? THEN 1 END) = 0", models.WorkflowStepStateComplete).
		Find(&entries)
	if tmp.Error != nil {
		return nil, models.NewSQLError(
			fmt.Sprintf("failed to list ready-to-run steps of workflow %s", workflowID), tmp.Error, true,
		)
	}

	result := []models.WorkflowStep{}
	for _, entry := range entries {
		result = append(result, entry.WorkflowStep)
	}

	return result, nil
}

// updateWorkflowStepState update the state of a group of workflow steps belonging to the
// same workflow. All requested steps must exist within the workflow and be able to make
// the requested state transition, otherwise no change is made.
func (c *databaseImpl) updateWorkflowStepState(
	ctx context.Context,
	workflowID string,
	stepIDs []string,
	newState models.WorkflowStepStateENUM,
	timestamp time.Time,
) error {
	deDuppedStepIDs := []string{}
	{
		stepIDSeen := map[string]bool{}
		for _, oneID := range stepIDs {
			stepIDSeen[oneID] = true
		}
		for oneID := range stepIDSeen {
			deDuppedStepIDs = append(deDuppedStepIDs, oneID)
		}
	}

	// Fetch the targeted steps, constrained to the parent workflow
	var entries []workflowStepEntry
	if tmp := c.db.
		Model(&workflowStepEntry{}).
		Where("id in ? and workflow_id = ?", deDuppedStepIDs, workflowID).
		Find(&entries); tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to fetch workflow %s steps", workflowID), tmp.Error, true,
		)
	}

	// Every requested step must belong to the workflow
	if len(entries) != len(deDuppedStepIDs) {
		return goutils.NewConsistencyError(
			fmt.Sprintf(
				"not all requested steps belong to workflow %s", workflowID,
			),
			nil,
			true,
		)
	}

	// Verify every step can make the transition before writing anything
	for _, entry := range entries {
		if err := entry.ValidNextState(newState); err != nil {
			return goutils.NewConsistencyError(
				fmt.Sprintf("can't transition workflow step %s to state %s", entry.ID, newState),
				err, true,
			)
		}
	}

	// Apply the given column values to a group of steps, scoped to the parent workflow.
	applyUpdate := func(ids []string, values workflowStepEntry) error {
		if len(ids) == 0 {
			return nil
		}
		tmp := c.db.
			Model(&workflowStepEntry{}).
			Where("id in ? and workflow_id = ?", ids, workflowID).
			Updates(values)
		if tmp.Error != nil {
			return models.NewSQLError(
				fmt.Sprintf("workflow %s steps state update failed", workflowID), tmp.Error, true,
			)
		}
		return nil
	}

	switch newState {
	case models.WorkflowStepStateDefined:
		// The only transitions into DEFINED are {FAILED, TIMED_OUT} -> DEFINED (enforced by
		// ValidNextState), i.e. a user reviving the step. Record that the user restarted it.
		// (user_restarted is only ever set true, never cleared, so the zero-value skip in a
		// struct Updates is not a concern here.)
		if err := applyUpdate(deDuppedStepIDs, workflowStepEntry{
			WorkflowStep: models.WorkflowStep{State: newState, UserRestarted: true},
		}); err != nil {
			return err
		}

	case models.WorkflowStepStateRunning:
		// A step only ever enters RUNNING from PENDING (enforced by ValidNextState), so every
		// step here records a fresh start time. A user-revived step re-runs from DEFINED (where
		// user_restarted was set) and flows through PENDING as well, so nothing extra is needed
		// here.
		if err := applyUpdate(deDuppedStepIDs, workflowStepEntry{
			WorkflowStep: models.WorkflowStep{State: newState, StartedAt: &timestamp},
		}); err != nil {
			return err
		}

	case models.WorkflowStepStateComplete:
		fallthrough
	case models.WorkflowStepStateCancelled:
		// Record the stop time
		if err := applyUpdate(deDuppedStepIDs, workflowStepEntry{
			WorkflowStep: models.WorkflowStep{State: newState, StoppedAt: &timestamp},
		}); err != nil {
			return err
		}

	default:
		if err := applyUpdate(deDuppedStepIDs, workflowStepEntry{
			WorkflowStep: models.WorkflowStep{State: newState},
		}); err != nil {
			return err
		}
	}

	// Nothing was changed (no steps requested); skip the audit record
	if len(entries) == 0 {
		return nil
	}

	// Record one step state change event per step. The fetched entries carry each step's ID,
	// parent workflow ID, and creator (denormalized) needed to route the event.
	stepEventTypeMap := map[models.WorkflowStepStateENUM]models.SystemEventTypeENUM{
		models.WorkflowStepStateDefined:    models.SystemEventTypeWorkflowStepDefined,
		models.WorkflowStepStatePending:    models.SystemEventTypeWorkflowStepPending,
		models.WorkflowStepStateRunning:    models.SystemEventTypeWorkflowStepRunning,
		models.WorkflowStepStateComplete:   models.SystemEventTypeWorkflowStepComplete,
		models.WorkflowStepStateFailed:     models.SystemEventTypeWorkflowStepFailed,
		models.WorkflowStepStateTimedOut:   models.SystemEventTypeWorkflowStepTimedOut,
		models.WorkflowStepStateCancelling: models.SystemEventTypeWorkflowStepCancelling,
		models.WorkflowStepStateCancelled:  models.SystemEventTypeWorkflowStepCancelled,
	}
	eventType, found := stepEventTypeMap[newState]
	if !found {
		return goutils.NewConsistencyError(
			fmt.Sprintf("no system event type for workflow step state '%s'", newState), nil, true,
		)
	}
	for _, entry := range entries {
		if _, err := c.defineNewSystemEvent(
			ctx, eventType, &models.SystemEventWorkflowStepEvents{
				StepID: entry.ID, WorkflowID: entry.WorkflowID, Creator: entry.Creator,
			},
		); err != nil {
			return models.NewSQLError(
				fmt.Sprintf(
					"failed to record workflow step %s change state to '%s' system event",
					entry.ID, newState,
				), err, true,
			)
		}
	}

	return nil
}

/*
MarkWorkflowStepDefined revert a group of workflow steps to DEFINED, i.e. revive them.

Only FAILED / TIMED_OUT steps may transition to DEFINED. Each reverted step is flagged as
user-restarted.

	@param ctx context.Context - execution context
	@param workflowID string - the parent workflow ID
	@param stepIDs []string - the workflow step IDs
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowStepDefined(
	ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
) error {
	return c.updateWorkflowStepState(
		ctx, workflowID, stepIDs, models.WorkflowStepStateDefined, timestamp,
	)
}

/*
MarkWorkflowStepPending mark a group of workflow steps are pending execution

	@param ctx context.Context - execution context
	@param workflowID string - the parent workflow ID
	@param stepIDs []string - the workflow step IDs
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowStepPending(
	ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
) error {
	return c.updateWorkflowStepState(
		ctx, workflowID, stepIDs, models.WorkflowStepStatePending, timestamp,
	)
}

/*
MarkWorkflowStepRunning mark a group of workflow steps are running

	@param ctx context.Context - execution context
	@param workflowID string - the parent workflow ID
	@param stepIDs []string - the workflow step IDs
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowStepRunning(
	ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
) error {
	return c.updateWorkflowStepState(
		ctx, workflowID, stepIDs, models.WorkflowStepStateRunning, timestamp,
	)
}

/*
MarkWorkflowStepComplete mark a group of workflow steps are complete

	@param ctx context.Context - execution context
	@param workflowID string - the parent workflow ID
	@param stepIDs []string - the workflow step IDs
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowStepComplete(
	ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
) error {
	return c.updateWorkflowStepState(
		ctx, workflowID, stepIDs, models.WorkflowStepStateComplete, timestamp,
	)
}

/*
MarkWorkflowStepFailed mark a group of workflow steps have failed

	@param ctx context.Context - execution context
	@param workflowID string - the parent workflow ID
	@param stepIDs []string - the workflow step IDs
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowStepFailed(
	ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
) error {
	return c.updateWorkflowStepState(
		ctx, workflowID, stepIDs, models.WorkflowStepStateFailed, timestamp,
	)
}

/*
MarkWorkflowStepTimedOut mark a group of workflow steps have timed out

	@param ctx context.Context - execution context
	@param workflowID string - the parent workflow ID
	@param stepIDs []string - the workflow step IDs
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowStepTimedOut(
	ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
) error {
	return c.updateWorkflowStepState(
		ctx, workflowID, stepIDs, models.WorkflowStepStateTimedOut, timestamp,
	)
}

/*
MarkWorkflowStepCancelling mark a group of workflow steps are being cancelled

	@param ctx context.Context - execution context
	@param workflowID string - the parent workflow ID
	@param stepIDs []string - the workflow step IDs
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowStepCancelling(
	ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
) error {
	return c.updateWorkflowStepState(
		ctx, workflowID, stepIDs, models.WorkflowStepStateCancelling, timestamp,
	)
}

/*
MarkWorkflowStepCancelled mark a group of workflow steps are cancelled

	@param ctx context.Context - execution context
	@param workflowID string - the parent workflow ID
	@param stepIDs []string - the workflow step IDs
	@param timestamp time.Time - when the state change occurred
*/
func (c *databaseImpl) MarkWorkflowStepCancelled(
	ctx context.Context, workflowID string, stepIDs []string, timestamp time.Time,
) error {
	return c.updateWorkflowStepState(
		ctx, workflowID, stepIDs, models.WorkflowStepStateCancelled, timestamp,
	)
}

// ======================================================================================
// Workflow Step <=> Executor Task Linkage

/*
LinkWorkflowStepWithExecutorTask record that a task worked on a workflow step.

A step may be linked to multiple tasks over its lifetime (its first run plus each
user-initiated revive); each task executes exactly one step.

	@param ctx context.Context - execution context
	@param stepID string - the workflow step ID
	@param taskID string - the ID of the task which worked on the step
*/
func (c *databaseImpl) LinkWorkflowStepWithExecutorTask(
	_ context.Context, stepID string, taskID string,
) error {
	entry := workflowStepRunnerTask{StepID: stepID, TaskID: taskID}
	if tmp := c.db.Create(&entry); tmp.Error != nil {
		return models.NewSQLError(
			fmt.Sprintf("failed to link workflow step %s with task %s", stepID, taskID),
			tmp.Error, true,
		)
	}
	return nil
}

/*
GetWorkflowStepAndExecutorTask fetch a workflow step along with the tasks which worked on it.

	@param ctx context.Context - execution context
	@param stepID string - the workflow step ID
	@param activeTask bool - when true, only return live (non-terminal) tasks, i.e. tasks in
	the PENDING or ACTIVE state
	@returns the workflow step, and the tasks linked to it
*/
func (c *databaseImpl) GetWorkflowStepAndExecutorTask(
	_ context.Context, stepID string, activeTask bool,
) (models.WorkflowStep, []models.Task, error) {
	step, err := c.getWorkflowStepDBEntry(stepID)
	if err != nil {
		return models.WorkflowStep{}, nil, err
	}

	// Walk the linkage table out to the tasks which worked on this step. A step is linked to tasks
	// one-to-many (a revived step re-runs under a fresh task while the prior attempt's task and link
	// persist), so order most-recent-first: callers that key on a single task (e.g. the maintenance
	// sweep reconciling a step against its current attempt) get the latest task deterministically.
	query := c.db.
		Table("tasks as task").
		Select("task.*").
		Joins(
			"INNER JOIN workflow_step_runner_tasks AS link ON link.task_id = task.id",
		).
		Where("link.step_id = ?", stepID).
		Order("task.created_at DESC")
	if activeTask {
		query = query.Where("task.state in ?", []models.TaskStateENUM{
			models.TaskStatePending,
			models.TaskStateActive,
		})
	}

	var taskEntries []taskEntry
	if tmp := query.Find(&taskEntries); tmp.Error != nil {
		return models.WorkflowStep{}, nil, models.NewSQLError(
			fmt.Sprintf("failed to fetch tasks linked to workflow step %s", stepID), tmp.Error, true,
		)
	}

	tasks := []models.Task{}
	for _, entry := range taskEntries {
		tasks = append(tasks, entry.Task)
	}

	return step.WorkflowStep, tasks, nil
}

/*
GetWorkflowStepProcessedByTask fetch the workflow step a task worked on, if any.

	@param ctx context.Context - execution context
	@param taskID string - the task ID
	@returns the workflow step linked to the task
*/
func (c *databaseImpl) GetWorkflowStepProcessedByTask(
	_ context.Context, taskID string,
) (models.WorkflowStep, error) {
	var entry workflowStepEntry
	tmp := c.db.
		Table("workflow_steps as step").
		Select("step.*").
		Joins(
			"INNER JOIN workflow_step_runner_tasks AS link ON link.step_id = step.id",
		).
		Where("link.task_id = ?", taskID).
		First(&entry)
	return entry.WorkflowStep, notFoundOrError(tmp.Error, "workflow step for task", taskID)
}
