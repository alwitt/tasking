package db_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

// sampleWorkflowSpec build a valid NewWorkflowParameter with the given steps. Each entry of
// steps maps a step name to the names of its parent steps.
func sampleWorkflowSpec(
	name string, deadline time.Time, steps map[string][]string,
) models.NewWorkflowParameter {
	stepParams := map[string]models.NewWorkflowStepParameter{}
	for stepName, parents := range steps {
		parentSet := map[string]bool{}
		for _, p := range parents {
			parentSet[p] = true
		}
		stepParams[stepName] = models.NewWorkflowStepParameter{
			Name:        stepName,
			Type:        "unit-test-step-type",
			RetryParams: models.DefaultTaskRetryParameters(),
			ParentSteps: parentSet,
		}
	}
	return models.NewWorkflowParameter{
		Name:     name,
		Deadline: deadline,
		Steps:    stepParams,
	}
}

func TestWorkflowDefineNewWorkflow(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	creator := "unit-test-creator"

	// Define a workflow with a small DAG: root -> {childA, childB}
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"root":   {},
		"childA": {"root"},
		"childB": {"root"},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, creator)
			return err
		},
	))
	assert.NotEmpty(workflow.ID)
	assert.Equal("unit-test-workflow", workflow.Name)
	assert.Equal(creator, workflow.Creator)
	assert.Equal(models.WorkflowStatePending, workflow.State)
	assert.WithinDuration(deadline, workflow.Deadline, time.Second)

	// The steps persisted, each carrying the denormalized creator, in DEFINED state, with the
	// workflow deadline mirrored onto them.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			assert.Len(steps, 3)
			for _, s := range steps {
				assert.Equal(workflow.ID, s.WorkflowID)
				assert.Equal(creator, s.Creator)
				assert.Equal(models.WorkflowStepStateDefined, s.State)
				assert.False(s.UserRestarted)
				assert.WithinDuration(deadline, s.Deadline, time.Second)
			}
			// The topological sort emits the root first, then its two children alphabetically.
			assert.Equal("root", steps[0].Name)
			assert.Equal("childA", steps[1].Name)
			assert.Equal("childB", steps[2].Name)

			// Only the root is ready to run; its children wait on it.
			ready, err := dbClient.ListWorkflowStepsReadyToRun(ctx, workflow.ID)
			if err != nil {
				return err
			}
			assert.Len(ready, 1)
			assert.Equal("root", ready[0].Name)
			return nil
		},
	))

	// A DEFINE_WORKFLOW audit event was recorded, routed to the workflow's creator.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{
					models.SystemEventTypeDefineWorkflow,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)

			parsed, err := events[0].ParseMetadata(validatorInstance(t))
			assert.Nil(err)
			meta, ok := parsed.(models.SystemEventWorkflowEvents)
			assert.True(ok, "expected SystemEventWorkflowEvents, got %T", parsed)
			assert.Equal(workflow.ID, meta.WorkflowID)
			assert.Equal(creator, meta.Creator)
			return nil
		},
	))
}

func TestWorkflowDefineNewWorkflowInvalidSpec(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)

	// A spec whose steps do not form a DAG (a depends on b, b depends on a) is rejected.
	spec := sampleWorkflowSpec("cyclic-workflow", deadline, map[string][]string{
		"a": {"b"},
		"b": {"a"},
	})

	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			_, err := dbClient.DefineNewWorkflow(ctx, spec, "unit-test-creator")
			return err
		},
	)
	assert.NotNil(err)
	var consistencyErr goutils.ConsistencyError
	assert.ErrorAs(err, &consistencyErr)
}

func TestWorkflowGetWorkflow(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	creator := "unit-test-creator"
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"only": {},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, creator)
			return err
		},
	))

	// Read it back
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			readBack, err := dbClient.GetWorkflow(ctx, workflow.ID)
			if err != nil {
				return err
			}
			assert.Equal(workflow.ID, readBack.ID)
			assert.Equal(workflow.Name, readBack.Name)
			assert.Equal(creator, readBack.Creator)
			assert.Equal(models.WorkflowStatePending, readBack.State)
			return nil
		},
	))

	// A missing workflow is a not-found error
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			_, err := dbClient.GetWorkflow(ctx, "does-not-exist")
			return err
		},
	)
	assert.NotNil(err)
	var notFoundErr goutils.NotFoundError
	assert.ErrorAs(err, &notFoundErr)
}

// markWorkflowState drive a single workflow state transition through the state-specific
// Mark* method, using the given timestamp for the transitions that record one.
func markWorkflowState(
	ctx context.Context,
	dbClient db.Database,
	workflowID string,
	newState models.WorkflowStateENUM,
	timestamp time.Time,
) error {
	switch newState {
	case models.WorkflowStateRunning:
		return dbClient.MarkWorkflowRunning(ctx, workflowID, timestamp)
	case models.WorkflowStateComplete:
		return dbClient.MarkWorkflowComplete(ctx, workflowID, timestamp)
	case models.WorkflowStateFailed:
		return dbClient.MarkWorkflowFailed(ctx, workflowID, timestamp)
	case models.WorkflowStateTimedOut:
		return dbClient.MarkWorkflowTimedOut(ctx, workflowID, timestamp)
	case models.WorkflowStateCancelling:
		return dbClient.MarkWorkflowCancelling(ctx, workflowID, timestamp)
	case models.WorkflowStateCancelled:
		return dbClient.MarkWorkflowCancelled(ctx, workflowID, timestamp)
	default:
		return fmt.Errorf("unsupported workflow state %s", newState)
	}
}

// countSystemEventsByType tally the recorded audit events by their event type.
func countSystemEventsByType(
	ctx context.Context, dbClient db.Database,
) (map[models.SystemEventTypeENUM]int, error) {
	events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{})
	if err != nil {
		return nil, err
	}
	counts := map[models.SystemEventTypeENUM]int{}
	for _, e := range events {
		counts[e.EventType]++
	}
	return counts, nil
}

// workflowStateEventType the state-change audit event type each workflow state emits.
var workflowStateEventType = map[models.WorkflowStateENUM]models.SystemEventTypeENUM{
	models.WorkflowStateRunning:    models.SystemEventTypeWorkflowRunning,
	models.WorkflowStateComplete:   models.SystemEventTypeWorkflowComplete,
	models.WorkflowStateFailed:     models.SystemEventTypeWorkflowFailed,
	models.WorkflowStateTimedOut:   models.SystemEventTypeWorkflowTimedOut,
	models.WorkflowStateCancelling: models.SystemEventTypeWorkflowCancelling,
	models.WorkflowStateCancelled:  models.SystemEventTypeWorkflowCancelled,
}

// runWorkflowTransitionTrain define a workflow, drive it through the given ordered chain of
// states, and assert the side effects and audit events along the way:
//   - StartedAt is stamped once, on the first PENDING -> RUNNING, and never moves afterwards.
//   - StoppedAt is stamped when the workflow reaches a terminal state (COMPLETE / CANCELLED).
//   - every transition emits its own distinctly-typed audit event, routed to the creator, and
//     the tallied event counts match the number of times the train enters each state.
func runWorkflowTransitionTrain(
	t *testing.T, train []models.WorkflowStateENUM,
) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	creator := "unit-test-creator"
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"only": {},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, creator)
			return err
		},
	))

	var firstStartedAt time.Time
	expectedCounts := map[models.SystemEventTypeENUM]int{
		models.SystemEventTypeDefineWorkflow: 1,
	}

	for i, newState := range train {
		// Space the timestamps out so a moved StartedAt would be detectable.
		timestamp := time.Now().UTC().Add(time.Duration(i) * time.Minute)
		if newState == models.WorkflowStateRunning && firstStartedAt.IsZero() {
			firstStartedAt = timestamp
		}
		expectedCounts[workflowStateEventType[newState]]++

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return markWorkflowState(ctx, dbClient, workflow.ID, newState, timestamp)
			},
		), "transition #%d to %s should succeed", i, newState)

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				readBack, err := dbClient.GetWorkflow(ctx, workflow.ID)
				if err != nil {
					return err
				}
				assert.Equal(newState, readBack.State, "state after transition #%d", i)

				// StartedAt is set once the workflow first runs, and never afterwards moves.
				assert.NotNil(readBack.StartedAt, "StartedAt set after transition #%d", i)
				assert.WithinDuration(
					firstStartedAt, *readBack.StartedAt, time.Second,
					"StartedAt unchanged after transition #%d", i,
				)

				// StoppedAt is set only upon reaching a terminal state.
				switch newState {
				case models.WorkflowStateComplete, models.WorkflowStateCancelled:
					assert.NotNil(readBack.StoppedAt, "StoppedAt set after terminal transition #%d", i)
					assert.WithinDuration(timestamp, *readBack.StoppedAt, time.Second)
				default:
					assert.Nil(readBack.StoppedAt, "StoppedAt still unset after transition #%d", i)
				}
				return nil
			},
		))
	}

	// Every transition emitted its own audit event; the tallies match how many times the train
	// entered each state, plus the one DEFINE_WORKFLOW from creation.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			counts, err := countSystemEventsByType(ctx, dbClient)
			if err != nil {
				return err
			}
			assert.Equal(expectedCounts, counts)

			// Spot-check that the state-change events carry the workflow's routing metadata.
			for eventType := range expectedCounts {
				if eventType == models.SystemEventTypeDefineWorkflow {
					continue
				}
				events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
					EventTypes: []models.SystemEventTypeENUM{eventType},
				})
				if err != nil {
					return err
				}
				for _, e := range events {
					parsed, err := e.ParseMetadata(validatorInstance(t))
					assert.Nil(err)
					meta, ok := parsed.(models.SystemEventWorkflowEvents)
					assert.True(ok, "expected SystemEventWorkflowEvents, got %T", parsed)
					assert.Equal(workflow.ID, meta.WorkflowID)
					assert.Equal(creator, meta.Creator)
				}
			}
			return nil
		},
	))
}

func TestWorkflowStateUpdateReviveToComplete(t *testing.T) {
	// A workflow that fails, is revived, times out, is revived again, and finally completes:
	// PENDING -> RUNNING -> FAILED -> RUNNING -> TIMED_OUT -> RUNNING -> COMPLETE
	runWorkflowTransitionTrain(t, []models.WorkflowStateENUM{
		models.WorkflowStateRunning,
		models.WorkflowStateFailed,
		models.WorkflowStateRunning,
		models.WorkflowStateTimedOut,
		models.WorkflowStateRunning,
		models.WorkflowStateComplete,
	})
}

func TestWorkflowStateUpdateReviveToCancelled(t *testing.T) {
	// A workflow that fails, is revived, then is cancelled by the user:
	// PENDING -> RUNNING -> FAILED -> RUNNING -> CANCELLING -> CANCELLED
	runWorkflowTransitionTrain(t, []models.WorkflowStateENUM{
		models.WorkflowStateRunning,
		models.WorkflowStateFailed,
		models.WorkflowStateRunning,
		models.WorkflowStateCancelling,
		models.WorkflowStateCancelled,
	})
}

func TestWorkflowStateUpdateInvalidTransition(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"only": {},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, "unit-test-creator")
			return err
		},
	))

	// A fresh PENDING workflow cannot jump straight to COMPLETE.
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkWorkflowComplete(ctx, workflow.ID, time.Now().UTC())
		},
	)
	assert.NotNil(err)
	var consistencyErr goutils.ConsistencyError
	assert.ErrorAs(err, &consistencyErr)

	// The workflow is unchanged: still PENDING, and no state-change audit event was recorded.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			readBack, err := dbClient.GetWorkflow(ctx, workflow.ID)
			if err != nil {
				return err
			}
			assert.Equal(models.WorkflowStatePending, readBack.State)

			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{
					models.SystemEventTypeWorkflowComplete,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(events, 0)
			return nil
		},
	))
}

func TestWorkflowUpdateDeadline(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	oldDeadline := time.Now().UTC().Add(time.Hour)
	creator := "unit-test-creator"
	// Independent steps so each can be driven to whichever state the test needs.
	spec := sampleWorkflowSpec("unit-test-workflow", oldDeadline, map[string][]string{
		"live":      {},
		"done":      {},
		"cancelled": {},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, creator)
			return err
		},
	))

	// Resolve the step IDs by name.
	var stepIDs map[string]string
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			stepIDs = map[string]string{}
			for _, s := range steps {
				stepIDs[s.Name] = s.ID
				// Every step starts out mirroring the original workflow deadline.
				assert.WithinDuration(oldDeadline, s.Deadline, time.Second)
			}
			return nil
		},
	))

	// Drive "done" to COMPLETE (DEFINED -> PENDING -> RUNNING -> COMPLETE) and "cancelled" to
	// CANCELLED (DEFINED -> CANCELLED); "live" stays in DEFINED (non-terminal).
	now := time.Now().UTC()
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			doneID := []string{stepIDs["done"]}
			if err := dbClient.MarkWorkflowStepPending(ctx, workflow.ID, doneID, now); err != nil {
				return err
			}
			if err := dbClient.MarkWorkflowStepRunning(ctx, workflow.ID, doneID, now); err != nil {
				return err
			}
			if err := dbClient.MarkWorkflowStepComplete(ctx, workflow.ID, doneID, now); err != nil {
				return err
			}
			return dbClient.MarkWorkflowStepCancelled(
				ctx, workflow.ID, []string{stepIDs["cancelled"]}, now,
			)
		},
	))

	// Update the workflow deadline.
	newDeadline := oldDeadline.Add(2 * time.Hour)
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.UpdateWorkflowDeadline(ctx, workflow.ID, newDeadline)
		},
	))

	// The workflow deadline moved, along with every non-terminal step's deadline; terminal
	// (COMPLETE / CANCELLED) steps keep their original deadline.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			readBack, err := dbClient.GetWorkflow(ctx, workflow.ID)
			if err != nil {
				return err
			}
			assert.WithinDuration(newDeadline, readBack.Deadline, time.Second)

			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			byName := map[string]models.WorkflowStep{}
			for _, s := range steps {
				byName[s.Name] = s
			}
			// The still-live step tracks the new deadline.
			assert.WithinDuration(newDeadline, byName["live"].Deadline, time.Second)
			// Terminal steps keep the original deadline.
			assert.WithinDuration(oldDeadline, byName["done"].Deadline, time.Second)
			assert.WithinDuration(oldDeadline, byName["cancelled"].Deadline, time.Second)
			return nil
		},
	))

	// A WORKFLOW_DEADLINE_UPDATE audit event was recorded, routed to the creator.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{
					models.SystemEventTypeWorkflowDeadlineUpdate,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			parsed, err := events[0].ParseMetadata(validatorInstance(t))
			assert.Nil(err)
			meta, ok := parsed.(models.SystemEventWorkflowEvents)
			assert.True(ok, "expected SystemEventWorkflowEvents, got %T", parsed)
			assert.Equal(workflow.ID, meta.WorkflowID)
			assert.Equal(creator, meta.Creator)
			return nil
		},
	))

	// Updating a non-existent workflow is a not-found error.
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.UpdateWorkflowDeadline(ctx, "does-not-exist", newDeadline)
		},
	)
	assert.NotNil(err)
	var notFoundErr goutils.NotFoundError
	assert.ErrorAs(err, &notFoundErr)
}

func TestWorkflowDelete(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	creator := "unit-test-creator"
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"root":  {},
		"child": {"root"},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, creator)
			return err
		},
	))

	// Capture the step IDs before deletion.
	var stepIDs []string
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			assert.Len(steps, 2)
			for _, s := range steps {
				stepIDs = append(stepIDs, s.ID)
			}
			return nil
		},
	))

	// defineTask create a fresh one-shot task and return its ID.
	defineTask := func(name string) string {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
					Name:       name,
					Creator:    "unit-test-creator",
					RetryParam: models.DefaultTaskRetryParameters(),
				})
				return err
			},
		))
		return task.ID
	}

	// Link a task to each step; give one task a child execution instance so the reap of its
	// task_executions history can be asserted.
	taskForStep0 := defineTask("task-for-step-0")
	taskForStep1 := defineTask("task-for-step-1")
	var execOfTask0 models.TaskExecution
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			if err := dbClient.LinkWorkflowStepWithExecutorTask(ctx, stepIDs[0], taskForStep0); err != nil {
				return err
			}
			if err := dbClient.LinkWorkflowStepWithExecutorTask(ctx, stepIDs[1], taskForStep1); err != nil {
				return err
			}
			task0, err := dbClient.GetTask(ctx, taskForStep0)
			if err != nil {
				return err
			}
			execOfTask0, err = dbClient.DefineNewTaskExecInstance(ctx, task0)
			return err
		},
	))
	assert.NotEmpty(execOfTask0.ID)

	// A non-terminal workflow cannot be deleted: it must be cancelled (or complete) first. The
	// workflow is still PENDING here, so the delete is refused with a ConsistencyError and
	// nothing is torn down.
	{
		err := persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.DeleteWorkflow(ctx, workflow.ID)
			},
		)
		assert.NotNil(err)
		var consistencyErr goutils.ConsistencyError
		assert.ErrorAs(err, &consistencyErr)

		// Nothing was deleted: the workflow, its steps, and its step tasks all still exist.
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				_, err := dbClient.GetWorkflow(ctx, workflow.ID)
				assert.Nil(err)
				steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
				assert.Nil(err)
				assert.Len(steps, 2)
				_, err = dbClient.GetTask(ctx, taskForStep0)
				assert.Nil(err)
				return nil
			},
		))
	}

	// Drive the workflow to a terminal state (CANCELLED) so it may be deleted.
	now := time.Now().UTC()
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			if err := dbClient.MarkWorkflowCancelling(ctx, workflow.ID, now); err != nil {
				return err
			}
			return dbClient.MarkWorkflowCancelled(ctx, workflow.ID, now)
		},
	))

	// Delete the workflow.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.DeleteWorkflow(ctx, workflow.ID)
		},
	))

	// The step tasks were reaped along with the workflow, and their execution history with them.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			for _, taskID := range []string{taskForStep0, taskForStep1} {
				_, err := dbClient.GetTask(ctx, taskID)
				var taskNotFound goutils.NotFoundError
				assert.ErrorAs(err, &taskNotFound, "task %s should be reaped", taskID)
			}
			_, err := dbClient.GetTaskExecution(ctx, execOfTask0.ID)
			var execNotFound goutils.NotFoundError
			assert.ErrorAs(err, &execNotFound, "task execution history should be reaped")
			return nil
		},
	))

	// The workflow is gone, and its steps were cascade-deleted along with it.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			_, err := dbClient.GetWorkflow(ctx, workflow.ID)
			var notFoundErr goutils.NotFoundError
			assert.ErrorAs(err, &notFoundErr)

			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			assert.Len(steps, 0)

			for _, stepID := range stepIDs {
				_, err := dbClient.GetWorkflowStep(ctx, stepID)
				var stepNotFound goutils.NotFoundError
				assert.ErrorAs(err, &stepNotFound, "step %s should be deleted", stepID)
			}
			return nil
		},
	))

	// A DELETE_WORKFLOW audit event was recorded, routed to the creator.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{
					models.SystemEventTypeDeleteWorkflow,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			parsed, err := events[0].ParseMetadata(validatorInstance(t))
			assert.Nil(err)
			meta, ok := parsed.(models.SystemEventWorkflowEvents)
			assert.True(ok, "expected SystemEventWorkflowEvents, got %T", parsed)
			assert.Equal(workflow.ID, meta.WorkflowID)
			assert.Equal(creator, meta.Creator)
			return nil
		},
	))

	// Deleting a workflow that does not exist is a not-found error (matching DeleteTask): the
	// pre-delete fetch that supplies the audit Creator fails before any deletion happens.
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.DeleteWorkflow(ctx, "does-not-exist")
		},
	)
	assert.NotNil(err)
	var notFoundErr goutils.NotFoundError
	assert.ErrorAs(err, &notFoundErr)

	// No extra audit event was recorded; still just the one from the real delete.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{
					models.SystemEventTypeDeleteWorkflow,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			return nil
		},
	))
}

// workflowIDSet collect the IDs of a slice of workflows into a set, for order-independent
// membership assertions.
func workflowIDSet(workflows []models.Workflow) map[string]bool {
	out := map[string]bool{}
	for _, w := range workflows {
		out[w.ID] = true
	}
	return out
}

func TestWorkflowList(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	now := time.Now().UTC()
	nearDeadline := now.Add(time.Hour)
	farDeadline := now.Add(24 * time.Hour)

	// Seed a spread of workflows differing by name, deadline, and (eventually) state.
	//   alpha   : name "alpha",   near deadline, driven to RUNNING
	//   beta    : name "beta",    near deadline, stays PENDING
	//   gamma   : name "gamma",   far  deadline, stays PENDING
	//   gamma-2 : name "gamma",   far  deadline, driven to RUNNING (shares a name with gamma)
	type seed struct {
		key      string
		name     string
		deadline time.Time
		running  bool
	}
	seeds := []seed{
		{key: "alpha", name: "alpha", deadline: nearDeadline, running: true},
		{key: "beta", name: "beta", deadline: nearDeadline, running: false},
		{key: "gamma", name: "gamma", deadline: farDeadline, running: false},
		{key: "gamma-2", name: "gamma", deadline: farDeadline, running: true},
	}

	ids := map[string]string{}
	for _, s := range seeds {
		spec := sampleWorkflowSpec(s.name, s.deadline, map[string][]string{"only": {}})
		var wf models.Workflow
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				wf, err = dbClient.DefineNewWorkflow(ctx, spec, "unit-test-creator")
				return err
			},
		))
		ids[s.key] = wf.ID
		if s.running {
			assert.Nil(persistence.UseDatabaseInTransaction(
				utCtx, func(ctx context.Context, dbClient db.Database) error {
					return dbClient.MarkWorkflowRunning(ctx, wf.ID, now)
				},
			))
		}
	}

	// listWith run ListWorkflows with the given filter and return the resulting ID set.
	listWith := func(filter db.WorkflowQueryFilter) map[string]bool {
		var result map[string]bool
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				workflows, err := dbClient.ListWorkflows(ctx, filter)
				if err != nil {
					return err
				}
				result = workflowIDSet(workflows)
				return nil
			},
		))
		return result
	}

	// ------------------------------------------------------------------------------------
	// Single-condition filters

	// No filter lists everything.
	assert.Equal(
		map[string]bool{
			ids["alpha"]: true, ids["beta"]: true, ids["gamma"]: true, ids["gamma-2"]: true,
		},
		listWith(db.WorkflowQueryFilter{}),
	)

	// TargetIDs narrows to the requested IDs.
	assert.Equal(
		map[string]bool{ids["alpha"]: true, ids["gamma"]: true},
		listWith(db.WorkflowQueryFilter{TargetIDs: []string{ids["alpha"], ids["gamma"]}}),
	)

	// TargetNames matches by name; "gamma" spans two workflows.
	assert.Equal(
		map[string]bool{ids["gamma"]: true, ids["gamma-2"]: true},
		listWith(db.WorkflowQueryFilter{TargetNames: []string{"gamma"}}),
	)

	// TargetStates filters by workflow state.
	assert.Equal(
		map[string]bool{ids["alpha"]: true, ids["gamma-2"]: true},
		listWith(db.WorkflowQueryFilter{
			TargetStates: []models.WorkflowStateENUM{models.WorkflowStateRunning},
		}),
	)
	assert.Equal(
		map[string]bool{ids["beta"]: true, ids["gamma"]: true},
		listWith(db.WorkflowQueryFilter{
			TargetStates: []models.WorkflowStateENUM{models.WorkflowStatePending},
		}),
	)

	// TargetDeadline returns workflows with a deadline at or before the cutoff. A cutoff at the
	// near deadline includes the near-deadline workflows but excludes the far-deadline ones.
	nearCutoff := nearDeadline.Add(time.Minute)
	assert.Equal(
		map[string]bool{ids["alpha"]: true, ids["beta"]: true},
		listWith(db.WorkflowQueryFilter{TargetDeadline: &nearCutoff}),
	)
	// A cutoff past the far deadline sweeps in all of them.
	farCutoff := farDeadline.Add(time.Minute)
	assert.Equal(
		map[string]bool{
			ids["alpha"]: true, ids["beta"]: true, ids["gamma"]: true, ids["gamma-2"]: true,
		},
		listWith(db.WorkflowQueryFilter{TargetDeadline: &farCutoff}),
	)

	// Limit caps the number of results.
	limit := 2
	assert.Len(
		listWith(db.WorkflowQueryFilter{
			CommonListEntryQueryFilter: db.CommonListEntryQueryFilter{Limit: &limit},
		}),
		2,
	)

	// ------------------------------------------------------------------------------------
	// Paired conditions (non-exhaustive)

	// Name + state: the "gamma" name plus the RUNNING state isolates gamma-2.
	assert.Equal(
		map[string]bool{ids["gamma-2"]: true},
		listWith(db.WorkflowQueryFilter{
			TargetNames:  []string{"gamma"},
			TargetStates: []models.WorkflowStateENUM{models.WorkflowStateRunning},
		}),
	)

	// Deadline + state: near-deadline plus PENDING isolates beta (alpha is near but RUNNING).
	assert.Equal(
		map[string]bool{ids["beta"]: true},
		listWith(db.WorkflowQueryFilter{
			TargetDeadline: &nearCutoff,
			TargetStates:   []models.WorkflowStateENUM{models.WorkflowStatePending},
		}),
	)

	// IDs + name: an ID set intersected with a name; only gamma satisfies both.
	assert.Equal(
		map[string]bool{ids["gamma"]: true},
		listWith(db.WorkflowQueryFilter{
			TargetIDs:   []string{ids["alpha"], ids["gamma"]},
			TargetNames: []string{"gamma"},
		}),
	)

	// ------------------------------------------------------------------------------------
	// Invalid filter

	// An unknown workflow state fails the `workflow_state` validation macro.
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			_, err := dbClient.ListWorkflows(ctx, db.WorkflowQueryFilter{
				TargetStates: []models.WorkflowStateENUM{"NOT_A_REAL_STATE"},
			})
			return err
		},
	)
	assert.NotNil(err)
	var validationErr goutils.ValidationError
	assert.ErrorAs(err, &validationErr)
}

// markWorkflowStepState drive a single workflow-step group state transition through the
// state-specific Mark* method, using the given timestamp for the transitions that record one.
func markWorkflowStepState(
	ctx context.Context,
	dbClient db.Database,
	workflowID string,
	stepIDs []string,
	newState models.WorkflowStepStateENUM,
	timestamp time.Time,
) error {
	switch newState {
	case models.WorkflowStepStateDefined:
		return dbClient.MarkWorkflowStepDefined(ctx, workflowID, stepIDs, timestamp)
	case models.WorkflowStepStatePending:
		return dbClient.MarkWorkflowStepPending(ctx, workflowID, stepIDs, timestamp)
	case models.WorkflowStepStateRunning:
		return dbClient.MarkWorkflowStepRunning(ctx, workflowID, stepIDs, timestamp)
	case models.WorkflowStepStateComplete:
		return dbClient.MarkWorkflowStepComplete(ctx, workflowID, stepIDs, timestamp)
	case models.WorkflowStepStateFailed:
		return dbClient.MarkWorkflowStepFailed(ctx, workflowID, stepIDs, timestamp)
	case models.WorkflowStepStateTimedOut:
		return dbClient.MarkWorkflowStepTimedOut(ctx, workflowID, stepIDs, timestamp)
	case models.WorkflowStepStateCancelling:
		return dbClient.MarkWorkflowStepCancelling(ctx, workflowID, stepIDs, timestamp)
	case models.WorkflowStepStateCancelled:
		return dbClient.MarkWorkflowStepCancelled(ctx, workflowID, stepIDs, timestamp)
	default:
		return fmt.Errorf("unsupported workflow step state %s", newState)
	}
}

// workflowStepStateEventType the state-change audit event type each workflow step state emits.
var workflowStepStateEventType = map[models.WorkflowStepStateENUM]models.SystemEventTypeENUM{
	models.WorkflowStepStateDefined:    models.SystemEventTypeWorkflowStepDefined,
	models.WorkflowStepStatePending:    models.SystemEventTypeWorkflowStepPending,
	models.WorkflowStepStateRunning:    models.SystemEventTypeWorkflowStepRunning,
	models.WorkflowStepStateComplete:   models.SystemEventTypeWorkflowStepComplete,
	models.WorkflowStepStateFailed:     models.SystemEventTypeWorkflowStepFailed,
	models.WorkflowStepStateTimedOut:   models.SystemEventTypeWorkflowStepTimedOut,
	models.WorkflowStepStateCancelling: models.SystemEventTypeWorkflowStepCancelling,
	models.WorkflowStepStateCancelled:  models.SystemEventTypeWorkflowStepCancelled,
}

// runWorkflowStepTransitionTrain define a workflow with two independent steps, drive BOTH
// steps together through the given ordered chain of states, and assert the side effects and
// audit events along the way:
//   - StartedAt is (re)stamped on every PENDING -> RUNNING entry (a revived step re-runs and
//     records a fresh start time), and is otherwise unchanged.
//   - StoppedAt is stamped when the step reaches a terminal state (COMPLETE / CANCELLED).
//   - UserRestarted flips true the first time a step is revived to DEFINED (from FAILED /
//     TIMED_OUT) and stays true thereafter.
//   - each transition emits one distinctly-typed audit event PER step, routed to the creator,
//     and the tallied event counts match (number of steps) x (times the train enters a state).
func runWorkflowStepTransitionTrain(
	t *testing.T, train []models.WorkflowStepStateENUM,
) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	creator := "unit-test-creator"
	// Two independent steps, driven together so the per-step audit fan-out is exercised.
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"stepA": {},
		"stepB": {},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, creator)
			return err
		},
	))

	// Resolve the step IDs.
	var stepIDs []string
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			for _, s := range steps {
				stepIDs = append(stepIDs, s.ID)
			}
			return nil
		},
	))
	assert.Len(stepIDs, 2)

	var latestStartedAt time.Time
	restarted := false
	// Definition records a single DEFINE_WORKFLOW event, not a per-step DEFINED event, so the
	// only pre-existing event before the train runs is that one.
	expectedCounts := map[models.SystemEventTypeENUM]int{
		models.SystemEventTypeDefineWorkflow: 1,
	}

	for i, newState := range train {
		// Space the timestamps out so a moved StartedAt / StoppedAt is detectable.
		timestamp := time.Now().UTC().Add(time.Duration(i) * time.Minute)
		if newState == models.WorkflowStepStateRunning {
			latestStartedAt = timestamp
		}
		if newState == models.WorkflowStepStateDefined {
			// Every DEFINED entry in these trains is a revive from FAILED / TIMED_OUT.
			restarted = true
		}
		// One event per step for this transition.
		expectedCounts[workflowStepStateEventType[newState]] += len(stepIDs)

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return markWorkflowStepState(
					ctx, dbClient, workflow.ID, stepIDs, newState, timestamp,
				)
			},
		), "transition #%d to %s should succeed", i, newState)

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				for _, stepID := range stepIDs {
					readBack, err := dbClient.GetWorkflowStep(ctx, stepID)
					if err != nil {
						return err
					}
					assert.Equal(newState, readBack.State, "step %s state after transition #%d", stepID, i)
					assert.Equal(
						restarted, readBack.UserRestarted,
						"step %s UserRestarted after transition #%d", stepID, i,
					)

					// StartedAt is set once the step first runs, and tracks the latest run.
					if latestStartedAt.IsZero() {
						assert.Nil(readBack.StartedAt, "StartedAt still unset after transition #%d", i)
					} else {
						assert.NotNil(readBack.StartedAt, "StartedAt set after transition #%d", i)
						assert.WithinDuration(
							latestStartedAt, *readBack.StartedAt, time.Second,
							"StartedAt tracks latest run after transition #%d", i,
						)
					}

					// StoppedAt is set only upon reaching a terminal state.
					switch newState {
					case models.WorkflowStepStateComplete, models.WorkflowStepStateCancelled:
						assert.NotNil(
							readBack.StoppedAt, "StoppedAt set after terminal transition #%d", i,
						)
						assert.WithinDuration(timestamp, *readBack.StoppedAt, time.Second)
					default:
						assert.Nil(
							readBack.StoppedAt, "StoppedAt still unset after transition #%d", i,
						)
					}
				}
				return nil
			},
		))
	}

	// Every transition emitted one audit event per step; the tallies match (step count) x (how
	// many times the train entered each state), plus the one DEFINE_WORKFLOW from creation.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			counts, err := countSystemEventsByType(ctx, dbClient)
			if err != nil {
				return err
			}
			assert.Equal(expectedCounts, counts)

			// Spot-check that the step state-change events carry each step's routing metadata.
			stepIDSet := map[string]bool{}
			for _, id := range stepIDs {
				stepIDSet[id] = true
			}
			for eventType := range expectedCounts {
				if eventType == models.SystemEventTypeDefineWorkflow {
					continue
				}
				events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
					EventTypes: []models.SystemEventTypeENUM{eventType},
				})
				if err != nil {
					return err
				}
				for _, e := range events {
					parsed, err := e.ParseMetadata(validatorInstance(t))
					assert.Nil(err)
					meta, ok := parsed.(models.SystemEventWorkflowStepEvents)
					assert.True(ok, "expected SystemEventWorkflowStepEvents, got %T", parsed)
					assert.Equal(workflow.ID, meta.WorkflowID)
					assert.Equal(creator, meta.Creator)
					assert.True(stepIDSet[meta.StepID], "unexpected step id %s", meta.StepID)
				}
			}
			return nil
		},
	))
}

func TestWorkflowStepStateUpdateReviveTwiceToComplete(t *testing.T) {
	// A step that fails, is revived, times out, is revived again, and finally completes:
	// DEFINED -> PENDING -> RUNNING -> FAILED -> DEFINED -> PENDING -> RUNNING -> TIMED_OUT ->
	// DEFINED -> PENDING -> RUNNING -> COMPLETE
	runWorkflowStepTransitionTrain(t, []models.WorkflowStepStateENUM{
		models.WorkflowStepStatePending,
		models.WorkflowStepStateRunning,
		models.WorkflowStepStateFailed,
		models.WorkflowStepStateDefined,
		models.WorkflowStepStatePending,
		models.WorkflowStepStateRunning,
		models.WorkflowStepStateTimedOut,
		models.WorkflowStepStateDefined,
		models.WorkflowStepStatePending,
		models.WorkflowStepStateRunning,
		models.WorkflowStepStateComplete,
	})
}

func TestWorkflowStepStateUpdateCancelFromDefined(t *testing.T) {
	// DEFINED -> CANCELLED
	runWorkflowStepTransitionTrain(t, []models.WorkflowStepStateENUM{
		models.WorkflowStepStateCancelled,
	})
}

func TestWorkflowStepStateUpdateCancelFromPending(t *testing.T) {
	// DEFINED -> PENDING -> CANCELLED
	runWorkflowStepTransitionTrain(t, []models.WorkflowStepStateENUM{
		models.WorkflowStepStatePending,
		models.WorkflowStepStateCancelled,
	})
}

func TestWorkflowStepStateUpdateCancelFromRunning(t *testing.T) {
	// DEFINED -> PENDING -> RUNNING -> CANCELLING -> CANCELLED
	runWorkflowStepTransitionTrain(t, []models.WorkflowStepStateENUM{
		models.WorkflowStepStatePending,
		models.WorkflowStepStateRunning,
		models.WorkflowStepStateCancelling,
		models.WorkflowStepStateCancelled,
	})
}

func TestWorkflowStepStateUpdateCancelFromFailed(t *testing.T) {
	// DEFINED -> PENDING -> RUNNING -> FAILED -> CANCELLED
	runWorkflowStepTransitionTrain(t, []models.WorkflowStepStateENUM{
		models.WorkflowStepStatePending,
		models.WorkflowStepStateRunning,
		models.WorkflowStepStateFailed,
		models.WorkflowStepStateCancelled,
	})
}

func TestWorkflowStepStateUpdateCancelFromTimedOut(t *testing.T) {
	// DEFINED -> PENDING -> RUNNING -> TIMED_OUT -> CANCELLED
	runWorkflowStepTransitionTrain(t, []models.WorkflowStepStateENUM{
		models.WorkflowStepStatePending,
		models.WorkflowStepStateRunning,
		models.WorkflowStepStateTimedOut,
		models.WorkflowStepStateCancelled,
	})
}

func TestWorkflowStepStateUpdateInvalidTransition(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"stepA": {},
		"stepB": {},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, "unit-test-creator")
			return err
		},
	))

	stepIDByName := map[string]string{}
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			for _, s := range steps {
				stepIDByName[s.Name] = s.ID
			}
			return nil
		},
	))
	stepA := stepIDByName["stepA"]
	stepB := stepIDByName["stepB"]

	// A fresh DEFINED step cannot jump straight to RUNNING (must go through PENDING first).
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkWorkflowStepRunning(ctx, workflow.ID, []string{stepA}, time.Now().UTC())
		},
	)
	assert.NotNil(err)
	var consistencyErr goutils.ConsistencyError
	assert.ErrorAs(err, &consistencyErr)

	// The step is unchanged, and no state-change audit event was recorded for it.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			readBack, err := dbClient.GetWorkflowStep(ctx, stepA)
			if err != nil {
				return err
			}
			assert.Equal(models.WorkflowStepStateDefined, readBack.State)

			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{
					models.SystemEventTypeWorkflowStepRunning,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(events, 0)
			return nil
		},
	))

	// The transition is validated for the whole group before anything is written: advance only
	// stepA to PENDING, then request PENDING->COMPLETE for BOTH steps. stepB (still DEFINED)
	// can't make that jump, so neither step changes — an all-or-nothing guard.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkWorkflowStepPending(ctx, workflow.ID, []string{stepA}, time.Now().UTC())
		},
	))
	err = persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkWorkflowStepComplete(
				ctx, workflow.ID, []string{stepA, stepB}, time.Now().UTC(),
			)
		},
	)
	assert.NotNil(err)
	assert.ErrorAs(err, &consistencyErr)

	// Neither step moved: stepA is still PENDING (not COMPLETE), stepB still DEFINED.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			a, err := dbClient.GetWorkflowStep(ctx, stepA)
			if err != nil {
				return err
			}
			assert.Equal(models.WorkflowStepStatePending, a.State)
			b, err := dbClient.GetWorkflowStep(ctx, stepB)
			if err != nil {
				return err
			}
			assert.Equal(models.WorkflowStepStateDefined, b.State)
			return nil
		},
	))

	// Requesting a step that does not belong to the workflow is a consistency error.
	err = persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkWorkflowStepPending(
				ctx, workflow.ID, []string{stepB, "does-not-exist"}, time.Now().UTC(),
			)
		},
	)
	assert.NotNil(err)
	assert.ErrorAs(err, &consistencyErr)
}

func TestWorkflowListStepsSortOrder(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	// A DAG with three depth levels, with several nodes sharing a level so the alphabetical
	// tie-break within a level is exercised:
	//
	//   level 0 (roots):  m, a
	//   level 1:          zebra (<- m), beta (<- a), alpha (<- a)
	//   level 2:          omega (<- zebra, beta)
	//
	// Expected order: roots alphabetically (a, m), then level 1 alphabetically
	// (alpha, beta, zebra), then level 2 (omega).
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"m":     {},
		"a":     {},
		"zebra": {"m"},
		"beta":  {"a"},
		"alpha": {"a"},
		"omega": {"zebra", "beta"},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, "unit-test-creator")
			return err
		},
	))

	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			names := make([]string, 0, len(steps))
			for _, s := range steps {
				names = append(names, s.Name)
			}
			assert.Equal(
				[]string{"a", "m", "alpha", "beta", "zebra", "omega"}, names,
			)
			return nil
		},
	))
}

func TestWorkflowListStepsReadyToRun(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	// A diamond DAG:  root -> {left, right} -> join
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"root":  {},
		"left":  {"root"},
		"right": {"root"},
		"join":  {"left", "right"},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, "unit-test-creator")
			return err
		},
	))

	stepIDByName := map[string]string{}
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			for _, s := range steps {
				stepIDByName[s.Name] = s.ID
			}
			return nil
		},
	))

	// readyNames return the names of the steps currently ready to run, as an order-independent
	// set.
	readyNames := func() map[string]bool {
		var out map[string]bool
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				ready, err := dbClient.ListWorkflowStepsReadyToRun(ctx, workflow.ID)
				if err != nil {
					return err
				}
				out = map[string]bool{}
				for _, s := range ready {
					out[s.Name] = true
				}
				return nil
			},
		))
		return out
	}

	// advance drive a single step through DEFINED -> PENDING -> RUNNING -> COMPLETE.
	advance := func(name string) {
		id := []string{stepIDByName[name]}
		now := time.Now().UTC()
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				if err := dbClient.MarkWorkflowStepPending(ctx, workflow.ID, id, now); err != nil {
					return err
				}
				if err := dbClient.MarkWorkflowStepRunning(ctx, workflow.ID, id, now); err != nil {
					return err
				}
				return dbClient.MarkWorkflowStepComplete(ctx, workflow.ID, id, now)
			},
		))
	}

	// Initially only the root is ready; the rest wait on unmet dependencies.
	assert.Equal(map[string]bool{"root": true}, readyNames())

	// A step that is in-flight (moved out of DEFINED) is not "ready" even though its own
	// dependencies are met: mark root PENDING and it drops out of the ready set.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkWorkflowStepPending(
				ctx, workflow.ID, []string{stepIDByName["root"]}, time.Now().UTC(),
			)
		},
	))
	assert.Equal(map[string]bool{}, readyNames())

	// Complete the root: both of its children become ready.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			id := []string{stepIDByName["root"]}
			now := time.Now().UTC()
			if err := dbClient.MarkWorkflowStepRunning(ctx, workflow.ID, id, now); err != nil {
				return err
			}
			return dbClient.MarkWorkflowStepComplete(ctx, workflow.ID, id, now)
		},
	))
	assert.Equal(map[string]bool{"left": true, "right": true}, readyNames())

	// Complete only "left": "join" still waits on "right", so nothing new is ready; "right"
	// remains ready and "left" is no longer DEFINED.
	advance("left")
	assert.Equal(map[string]bool{"right": true}, readyNames())

	// Complete "right": both parents of "join" are now COMPLETE, so "join" becomes ready.
	advance("right")
	assert.Equal(map[string]bool{"join": true}, readyNames())

	// Complete "join": the DAG is fully processed, nothing is left to run.
	advance("join")
	assert.Equal(map[string]bool{}, readyNames())
}

// taskIDSet collect the IDs of a slice of tasks into a set, for order-independent membership
// assertions.
func taskIDSet(tasks []models.Task) map[string]bool {
	out := map[string]bool{}
	for _, t := range tasks {
		out[t.ID] = true
	}
	return out
}

func TestWorkflowStepExecutorTaskLinkage(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	deadline := time.Now().UTC().Add(time.Hour)
	spec := sampleWorkflowSpec("unit-test-workflow", deadline, map[string][]string{
		"stepA": {},
		"stepB": {},
	})

	var workflow models.Workflow
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			workflow, err = dbClient.DefineNewWorkflow(ctx, spec, "unit-test-creator")
			return err
		},
	))

	stepIDByName := map[string]string{}
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			steps, err := dbClient.ListWorkflowSteps(ctx, workflow.ID)
			if err != nil {
				return err
			}
			for _, s := range steps {
				stepIDByName[s.Name] = s.ID
			}
			return nil
		},
	))
	stepA := stepIDByName["stepA"]
	stepB := stepIDByName["stepB"]

	// defineTask create a fresh one-shot task and return its ID.
	defineTask := func(name string) string {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
					Name:       name,
					Creator:    "unit-test-creator",
					RetryParam: models.DefaultTaskRetryParameters(),
				})
				return err
			},
		))
		return task.ID
	}

	// stepA is worked on by three tasks over its lifetime (its first run plus two revives).
	// Leave them in a spread of states so the active-task filter can be exercised:
	//   taskPending  : stays PENDING              (active)
	//   taskActive   : PENDING -> ACTIVE          (active)
	//   taskComplete : PENDING -> ACTIVE -> COMPLETE (terminal)
	taskPending := defineTask("task-pending")
	taskActive := defineTask("task-active")
	taskComplete := defineTask("task-complete")
	// stepB is worked on by a single task, to confirm links are scoped per step.
	taskForB := defineTask("task-for-b")

	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			for _, taskID := range []string{taskPending, taskActive, taskComplete} {
				if err := dbClient.LinkWorkflowStepWithExecutorTask(ctx, stepA, taskID); err != nil {
					return err
				}
			}
			return dbClient.LinkWorkflowStepWithExecutorTask(ctx, stepB, taskForB)
		},
	))

	// Drive taskActive to ACTIVE, and taskComplete through to COMPLETE.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			if err := dbClient.MarkTaskActive(ctx, taskActive); err != nil {
				return err
			}
			if err := dbClient.MarkTaskActive(ctx, taskComplete); err != nil {
				return err
			}
			return dbClient.MarkTaskComplete(ctx, taskComplete)
		},
	))

	// All tasks linked to stepA are returned when not filtering on active.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			step, tasks, err := dbClient.GetWorkflowStepAndExecutorTask(ctx, stepA, false)
			if err != nil {
				return err
			}
			assert.Equal(stepA, step.ID)
			assert.Equal(
				map[string]bool{taskPending: true, taskActive: true, taskComplete: true},
				taskIDSet(tasks),
			)
			return nil
		},
	))

	// Only the live (PENDING / ACTIVE) tasks are returned when filtering on active; the
	// COMPLETE task is excluded.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			step, tasks, err := dbClient.GetWorkflowStepAndExecutorTask(ctx, stepA, true)
			if err != nil {
				return err
			}
			assert.Equal(stepA, step.ID)
			assert.Equal(
				map[string]bool{taskPending: true, taskActive: true},
				taskIDSet(tasks),
			)
			return nil
		},
	))

	// Links are scoped per step: stepB sees only its own single task.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			step, tasks, err := dbClient.GetWorkflowStepAndExecutorTask(ctx, stepB, false)
			if err != nil {
				return err
			}
			assert.Equal(stepB, step.ID)
			assert.Equal(map[string]bool{taskForB: true}, taskIDSet(tasks))
			return nil
		},
	))

	// The reverse lookup: each task resolves back to the step it worked on.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			for _, taskID := range []string{taskPending, taskActive, taskComplete} {
				step, err := dbClient.GetWorkflowStepProcessedByTask(ctx, taskID)
				if err != nil {
					return err
				}
				assert.Equal(stepA, step.ID, "task %s should resolve to stepA", taskID)
			}
			step, err := dbClient.GetWorkflowStepProcessedByTask(ctx, taskForB)
			if err != nil {
				return err
			}
			assert.Equal(stepB, step.ID)
			return nil
		},
	))

	// A task with no linked step is a not-found error.
	unlinkedTask := defineTask("unlinked")
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			_, err := dbClient.GetWorkflowStepProcessedByTask(ctx, unlinkedTask)
			return err
		},
	)
	assert.NotNil(err)
	var notFoundErr goutils.NotFoundError
	assert.ErrorAs(err, &notFoundErr)

	// A step that exists but has no linked tasks returns the step and an empty task list. Use a
	// fresh step (from a second workflow) that was never linked to any task.
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			otherSpec := sampleWorkflowSpec(
				"unit-test-workflow-2", deadline, map[string][]string{"lonely": {}},
			)
			otherWorkflow, err := dbClient.DefineNewWorkflow(ctx, otherSpec, "unit-test-creator")
			if err != nil {
				return err
			}
			steps, err := dbClient.ListWorkflowSteps(ctx, otherWorkflow.ID)
			if err != nil {
				return err
			}
			assert.Len(steps, 1)
			step, tasks, err := dbClient.GetWorkflowStepAndExecutorTask(ctx, steps[0].ID, false)
			if err != nil {
				return err
			}
			assert.Equal(steps[0].ID, step.ID)
			assert.Len(tasks, 0)
			return nil
		},
	))
}
