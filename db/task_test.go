package db_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// getUnitTestPersistence spin up a fresh Sqlite-backed persistence client with the
// schema migrated, for use in a single unit test.
func getUnitTestPersistence(ctx context.Context, t *testing.T, dbFile string) db.Client {
	assert := assert.New(t)

	persistence, err := db.NewConnection(db.GetSqliteDialector(dbFile), logger.Info)
	assert.Nil(err)

	// migrate the schema
	assert.Nil(persistence.RunSQLInTransaction(
		ctx, func(ctx context.Context, tx *gorm.DB) error {
			return db.DefineTables(ctx, tx)
		},
	))

	return persistence
}

func TestTaskDefineNewOneShotTask(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// Case 0: define an immediate one-shot task
	var task0 models.Task
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			task0, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
				Name:       "unit-test-task",
				RetryParam: models.DefaultTaskRetryParameters(),
			})
			return err
		},
	))
	assert.NotEmpty(task0.ID)
	assert.Equal("unit-test-task", task0.TaskName)
	assert.Equal(models.TaskScheduleClassImmediateOneShot, task0.TaskScheduleClass)
	assert.Equal(models.TaskStatePending, task0.TaskState)
	assert.Nil(task0.TargetRunTime)

	// Case 1: read the task back and confirm it persisted
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			readBack, err := dbClient.GetTask(ctx, task0.ID)
			if err != nil {
				return err
			}
			assert.Equal(task0.ID, readBack.ID)
			assert.Equal(task0.TaskName, readBack.TaskName)
			assert.Equal(models.TaskScheduleClassImmediateOneShot, readBack.TaskScheduleClass)
			assert.Equal(models.TaskStatePending, readBack.TaskState)
			return nil
		},
	))

	// Case 2: task with a deadline is preserved
	deadline := time.Now().UTC().Add(time.Hour)
	var task2 models.Task
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			task2, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
				Name:       "unit-test-task-deadline",
				RetryParam: models.DefaultTaskRetryParameters(),
				Deadline:   &deadline,
			})
			return err
		},
	))
	assert.NotNil(task2.Deadline)
	assert.WithinDuration(deadline, *task2.Deadline, time.Second)
}

func TestTaskDefineNewScheduledOneShotTask(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// Case 0: define a scheduled one-shot task
	targetRuntime := time.Now().UTC().Add(time.Minute * 30)
	var task0 models.Task
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			task0, err = dbClient.DefineNewScheduledOneShotTask(
				ctx,
				db.NewTaskParameter{
					Name:       "unit-test-scheduled-task",
					RetryParam: models.DefaultTaskRetryParameters(),
				},
				targetRuntime,
			)
			return err
		},
	))
	assert.NotEmpty(task0.ID)
	assert.Equal(models.TaskScheduleClassScheduledOneShot, task0.TaskScheduleClass)
	assert.Equal(models.TaskStatePending, task0.TaskState)
	assert.NotNil(task0.TargetRunTime)
	assert.WithinDuration(targetRuntime, *task0.TargetRunTime, time.Second)

	// Case 1: read back and confirm the target runtime persisted
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			readBack, err := dbClient.GetTask(ctx, task0.ID)
			if err != nil {
				return err
			}
			assert.Equal(models.TaskScheduleClassScheduledOneShot, readBack.TaskScheduleClass)
			assert.NotNil(readBack.TargetRunTime)
			assert.WithinDuration(targetRuntime, *readBack.TargetRunTime, time.Second)
			return nil
		},
	))
}

func TestTaskGetTaskNotFound(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// Fetching an unknown task must surface a goutils.NotFoundError, not a SQLError.
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			_, err := dbClient.GetTask(ctx, ulid.Make().String())
			return err
		},
	)
	assert.NotNil(err)
	var notFound goutils.NotFoundError
	assert.True(errors.As(err, &notFound), "expected NotFoundError, got %T", err)
}

func TestTaskStateTransitions(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// helper: define a fresh PENDING task and return its ID
	defineTask := func() string {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
					Name:       "unit-test-transition",
					RetryParam: models.DefaultTaskRetryParameters(),
				})
				return err
			},
		))
		return task.ID
	}

	// helper: read current task state
	readState := func(taskID string) models.TaskStateENUM {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.GetTask(ctx, taskID)
				return err
			},
		))
		return task.TaskState
	}

	// helper: count recorded system events, optionally filtered by type
	countEvents := func(eventTypes ...models.SystemEventTypeENUM) int {
		var events []models.SystemEventAudit
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				events, err = dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
					EventTypes: eventTypes,
				})
				return err
			},
		))
		return len(events)
	}

	// Case 0: PENDING -> ACTIVE -> COMPLETE (happy path)
	// The intermediate ACTIVE transition and the terminal COMPLETE each record an event.
	{
		activateBefore := countEvents(models.SystemEventTypeActivateTask)

		taskID := defineTask()
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskActive(ctx, taskID)
			},
		))
		assert.Equal(models.TaskStateActive, readState(taskID))
		// intermediate ACTIVE event recorded
		assert.Equal(activateBefore+1, countEvents(models.SystemEventTypeActivateTask))

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskComplete(ctx, taskID)
			},
		))
		assert.Equal(models.TaskStateComplete, readState(taskID))
		// terminal COMPLETE event recorded
		assert.Equal(1, countEvents(models.SystemEventTypeCompleteTask))
	}

	// Case 1: PENDING -> ACTIVE -> FAILED
	// The intermediate ACTIVE transition and the terminal FAILED each record an event.
	{
		activateBefore := countEvents(models.SystemEventTypeActivateTask)

		taskID := defineTask()
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskActive(ctx, taskID)
			},
		))
		assert.Equal(activateBefore+1, countEvents(models.SystemEventTypeActivateTask))

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskFailed(ctx, taskID)
			},
		))
		assert.Equal(models.TaskStateFailed, readState(taskID))

		assert.Equal(1, countEvents(models.SystemEventTypeFailedTask))
	}

	// Case 2: PENDING -> ACTIVE -> TIMED_OUT
	// The intermediate ACTIVE transition and the terminal TIMED_OUT each record an event.
	{
		activateBefore := countEvents(models.SystemEventTypeActivateTask)

		taskID := defineTask()
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskActive(ctx, taskID)
			},
		))
		assert.Equal(activateBefore+1, countEvents(models.SystemEventTypeActivateTask))

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskTimedOut(ctx, taskID)
			},
		))
		assert.Equal(models.TaskStateTimeout, readState(taskID))

		assert.Equal(1, countEvents(models.SystemEventTypeTimedOutTask))
	}

	// Case 3: PENDING -> CANCELLING -> CANCELLED
	// CANCELLING is an intermediate state that intentionally records NO event; only
	// the terminal CANCELLED does.
	{
		totalBefore := countEvents()

		taskID := defineTask()
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskCancelling(ctx, taskID)
			},
		))
		assert.Equal(models.TaskStateCancelling, readState(taskID))
		// CANCELLING recorded no event
		assert.Equal(totalBefore, countEvents())

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskCancelled(ctx, taskID)
			},
		))
		assert.Equal(models.TaskStateCancelled, readState(taskID))

		// CANCELLING records no event; only CANCELLED does
		assert.Equal(1, countEvents(models.SystemEventTypeCancelledTask))
	}

	// events so far: ACTIVATE x3 (cases 0-2), COMPLETE, FAILED, TIMED_OUT, CANCELLED
	assert.Equal(7, countEvents())

	// Case 4: illegal transition PENDING -> COMPLETE is rejected as a consistency error
	{
		taskID := defineTask()
		err := persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskComplete(ctx, taskID)
			},
		)
		assert.NotNil(err)
		var consistency goutils.ConsistencyError
		assert.True(errors.As(err, &consistency), "expected ConsistencyError, got %T", err)
		// state must be unchanged
		assert.Equal(models.TaskStatePending, readState(taskID))
		// a rejected transition records no event
		assert.Equal(7, countEvents())
	}

	// Case 5: transition on a non-existent task surfaces NotFoundError
	{
		err := persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskActive(ctx, ulid.Make().String())
			},
		)
		assert.NotNil(err)
		var notFound goutils.NotFoundError
		assert.True(errors.As(err, &notFound), "expected NotFoundError, got %T", err)
		// a not-found transition records no event
		assert.Equal(7, countEvents())
	}
}

func TestTaskListTasks(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// helper: run ListTasks with a filter and return the tasks
	listTasks := func(f db.TaskQueryFilter) []models.Task {
		var out []models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				out, err = dbClient.ListTasks(ctx, f)
				return err
			},
		))
		return out
	}

	// helper: collect returned task IDs into a set
	idSet := func(tasks []models.Task) map[string]bool {
		out := map[string]bool{}
		for _, task := range tasks {
			out[task.ID] = true
		}
		return out
	}

	// helper: define an immediate one-shot task with the given name / optional deadline
	defineImmediate := func(name string, deadline *time.Time) models.Task {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
					Name:       name,
					RetryParam: models.DefaultTaskRetryParameters(),
					Deadline:   deadline,
				})
				return err
			},
		))
		return task
	}

	// helper: define a scheduled one-shot task with the given name / optional deadline
	defineScheduled := func(name string, deadline *time.Time) models.Task {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewScheduledOneShotTask(
					ctx,
					db.NewTaskParameter{
						Name:       name,
						RetryParam: models.DefaultTaskRetryParameters(),
						Deadline:   deadline,
					},
					time.Now().UTC().Add(time.Hour),
				)
				return err
			},
		))
		return task
	}

	// helper: drive a task PENDING -> ACTIVE
	markActive := func(taskID string) {
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskActive(ctx, taskID)
			},
		))
	}

	// ----------------------------------------------------------------------------------
	// Seed a known corpus. Tasks are created one-per-transaction; the sleep keeps each
	// CreatedAt distinct so `created_at` ordering (and thus pagination) is deterministic.
	//
	// name     | class      | deadline           | state
	// ---------+------------+--------------------+--------
	// alpha    | immediate  | -                  | ACTIVE
	// alpha    | immediate  | -                  | PENDING
	// beta     | immediate  | now+30m (early)    | PENDING
	// beta     | scheduled  | now+90m (late)     | ACTIVE
	// gamma    | scheduled  | now+30m (early)    | PENDING

	early := time.Now().UTC().Add(30 * time.Minute)
	late := time.Now().UTC().Add(90 * time.Minute)

	var corpus []models.Task
	seed := func(task models.Task) models.Task {
		corpus = append(corpus, task)
		time.Sleep(2 * time.Millisecond)
		return task
	}

	alphaActive := seed(defineImmediate("alpha", nil))
	alphaPending := seed(defineImmediate("alpha", nil))
	betaPending := seed(defineImmediate("beta", &early))
	betaActive := seed(defineScheduled("beta", &late))
	gammaPending := seed(defineScheduled("gamma", &early))

	markActive(alphaActive.ID)
	markActive(betaActive.ID)

	// corpus creation order (== created_at order)
	orderedIDs := []string{
		alphaActive.ID, alphaPending.ID, betaPending.ID, betaActive.ID, gammaPending.ID,
	}

	// ----------------------------------------------------------------------------------
	// Isolation cases

	// Case 0: TargetIDs - exactly the requested subset comes back
	{
		want := []string{alphaPending.ID, gammaPending.ID}
		got := idSet(listTasks(db.TaskQueryFilter{TargetIDs: want}))
		assert.Len(got, 2)
		assert.True(got[alphaPending.ID])
		assert.True(got[gammaPending.ID])

		// negative: an ID that exists nowhere yields an empty result
		none := listTasks(db.TaskQueryFilter{TargetIDs: []string{ulid.Make().String()}})
		assert.Empty(none)
	}

	// Case 1: TaskNames - a name mapping to multiple tasks, nothing else leaks in
	{
		got := listTasks(db.TaskQueryFilter{TaskNames: []string{"alpha"}})
		assert.Len(got, 2)
		for _, task := range got {
			assert.Equal("alpha", task.TaskName)
		}
	}

	// Case 2: TaskScheduleClasses - only scheduled tasks returned
	{
		got := listTasks(db.TaskQueryFilter{
			TaskScheduleClasses: []models.TaskScheduleClassENUM{
				models.TaskScheduleClassScheduledOneShot,
			},
		})
		gotSet := idSet(got)
		assert.Len(got, 2)
		assert.True(gotSet[betaActive.ID])
		assert.True(gotSet[gammaPending.ID])
		for _, task := range got {
			assert.Equal(models.TaskScheduleClassScheduledOneShot, task.TaskScheduleClass)
		}
	}

	// Case 3: TaskStates - only the transitioned ACTIVE tasks returned
	{
		got := listTasks(db.TaskQueryFilter{
			TaskStates: []models.TaskStateENUM{models.TaskStateActive},
		})
		gotSet := idSet(got)
		assert.Len(got, 2)
		assert.True(gotSet[alphaActive.ID])
		assert.True(gotSet[betaActive.ID])
		for _, task := range got {
			assert.Equal(models.TaskStateActive, task.TaskState)
		}
	}

	// Case 4: TargetDeadline - only non-null deadlines <= cutoff; deadline-less excluded
	{
		// cutoff between `early` and `late`: catches the two early-deadline tasks only
		cutoff := time.Now().UTC().Add(60 * time.Minute)
		got := listTasks(db.TaskQueryFilter{TargetDeadline: &cutoff})
		gotSet := idSet(got)
		assert.Len(got, 2)
		assert.True(gotSet[betaPending.ID])
		assert.True(gotSet[gammaPending.ID])
		// the late-deadline task and the deadline-less tasks are excluded
		assert.False(gotSet[betaActive.ID])
		assert.False(gotSet[alphaActive.ID])
		assert.False(gotSet[alphaPending.ID])
	}

	// Case 5: pagination (Limit + Offset combined) over the full corpus, created_at order
	{
		limit := 2
		offset := 1
		got := listTasks(db.TaskQueryFilter{
			CommonListEntryQueryFilter: db.CommonListEntryQueryFilter{
				Limit: &limit, Offset: &offset,
			},
		})
		assert.Len(got, 2)
		// window is orderedIDs[1:3]
		assert.Equal(orderedIDs[1], got[0].ID)
		assert.Equal(orderedIDs[2], got[1].ID)
	}

	// ----------------------------------------------------------------------------------
	// Paired (non-exhaustive) cases

	// Case 6: TaskNames + TaskStates - intersection only
	{
		// name "alpha" has an ACTIVE and a PENDING task; combined with state ACTIVE, only
		// alphaActive should return. betaActive is ACTIVE but wrong name -> excluded.
		got := listTasks(db.TaskQueryFilter{
			TaskNames:  []string{"alpha"},
			TaskStates: []models.TaskStateENUM{models.TaskStateActive},
		})
		assert.Len(got, 1)
		assert.Equal(alphaActive.ID, got[0].ID)
	}

	// Case 7: TaskScheduleClasses + TargetDeadline - intersection only
	{
		// scheduled tasks: betaActive (late deadline), gammaPending (early deadline).
		// cutoff between early and late -> only gammaPending qualifies.
		cutoff := time.Now().UTC().Add(60 * time.Minute)
		got := listTasks(db.TaskQueryFilter{
			TaskScheduleClasses: []models.TaskScheduleClassENUM{
				models.TaskScheduleClassScheduledOneShot,
			},
			TargetDeadline: &cutoff,
		})
		assert.Len(got, 1)
		assert.Equal(gammaPending.ID, got[0].ID)
	}

	// Case 8: pagination + TaskNames - window taken from the filtered, ordered subset
	{
		// "alpha" filtered set in created_at order: [alphaActive, alphaPending].
		// Offset 1, Limit 1 -> just alphaPending (not the whole-corpus row at that offset).
		limit := 1
		offset := 1
		got := listTasks(db.TaskQueryFilter{
			CommonListEntryQueryFilter: db.CommonListEntryQueryFilter{
				Limit: &limit, Offset: &offset,
			},
			TaskNames: []string{"alpha"},
		})
		assert.Len(got, 1)
		assert.Equal(alphaPending.ID, got[0].ID)
	}
}

func TestTaskDeleteTask(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// helper: define a fresh PENDING task and return it
	defineTask := func() models.Task {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
					Name:       "unit-test-delete",
					RetryParam: models.DefaultTaskRetryParameters(),
				})
				return err
			},
		))
		return task
	}

	// helper: count recorded DELETE_TASK events
	countDeleteEvents := func() int {
		var events []models.SystemEventAudit
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				events, err = dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
					EventTypes: []models.SystemEventTypeENUM{models.SystemEventTypeDeleteTask},
				})
				return err
			},
		))
		return len(events)
	}

	// Case 0: delete an existing task - it is gone and a DELETE_TASK event is recorded
	{
		task := defineTask()
		deleteBefore := countDeleteEvents()

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.DeleteTask(ctx, task.ID)
			},
		))

		// the delete event was recorded
		assert.Equal(deleteBefore+1, countDeleteEvents())

		// the task is no longer retrievable
		err := persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				_, err := dbClient.GetTask(ctx, task.ID)
				return err
			},
		)
		assert.NotNil(err)
		var notFound goutils.NotFoundError
		assert.True(errors.As(err, &notFound), "expected NotFoundError, got %T", err)
	}

	// Case 1: deleting a non-existent task surfaces NotFoundError and records no event
	{
		deleteBefore := countDeleteEvents()

		err := persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.DeleteTask(ctx, ulid.Make().String())
			},
		)
		assert.NotNil(err)
		var notFound goutils.NotFoundError
		assert.True(errors.As(err, &notFound), "expected NotFoundError, got %T", err)

		// no delete event recorded for a rejected delete
		assert.Equal(deleteBefore, countDeleteEvents())
	}

	// Case 2: deleting a task cascades to its child execution instances
	{
		task := defineTask()

		// seed a child execution instance for the task
		var exec models.TaskExecution
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				exec, err = dbClient.DefineNewTaskExecInstance(ctx, task)
				return err
			},
		))
		assert.NotEmpty(exec.ID)

		// sanity: the child is retrievable before delete
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				_, err := dbClient.GetTaskExecution(ctx, exec.ID)
				return err
			},
		))

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.DeleteTask(ctx, task.ID)
			},
		))

		// the child execution instance was cascade-deleted
		err := persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				_, err := dbClient.GetTaskExecution(ctx, exec.ID)
				return err
			},
		)
		assert.NotNil(err)
		var notFound goutils.NotFoundError
		assert.True(errors.As(err, &notFound), "expected NotFoundError, got %T", err)
	}
}

func TestTaskExecDefineNewTaskExecInstance(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// Case 0: exec instance for an immediate one-shot task
	{
		deadline := time.Now().UTC().Add(time.Hour)
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
					Name:       "unit-test-exec-immediate",
					RetryParam: models.DefaultTaskRetryParameters(),
					Deadline:   &deadline,
				})
				return err
			},
		))

		var exec models.TaskExecution
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				exec, err = dbClient.DefineNewTaskExecInstance(ctx, task)
				return err
			},
		))
		assert.NotEmpty(exec.ID)
		assert.Equal(task.ID, exec.TaskID)
		assert.Equal(models.TaskExecutionClassImmediate, exec.ExecutionClass)
		assert.Equal(models.TaskExecutionStateDefined, exec.ExecutionState)
		// immediate exec has no scheduled enqueue time
		assert.Nil(exec.TargetEnqueueTime)
		// deadline is inherited from the parent task
		assert.NotNil(exec.Deadline)
		assert.WithinDuration(deadline, *exec.Deadline, time.Second)

		// read back and confirm it persisted
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				readBack, err := dbClient.GetTaskExecution(ctx, exec.ID)
				if err != nil {
					return err
				}
				assert.Equal(exec.ID, readBack.ID)
				assert.Equal(models.TaskExecutionClassImmediate, readBack.ExecutionClass)
				assert.Equal(models.TaskExecutionStateDefined, readBack.ExecutionState)
				return nil
			},
		))
	}

	// Case 1: exec instance for a scheduled one-shot task carries the target enqueue time
	{
		targetRuntime := time.Now().UTC().Add(30 * time.Minute)
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewScheduledOneShotTask(
					ctx,
					db.NewTaskParameter{
						Name:       "unit-test-exec-scheduled",
						RetryParam: models.DefaultTaskRetryParameters(),
					},
					targetRuntime,
				)
				return err
			},
		))

		var exec models.TaskExecution
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				exec, err = dbClient.DefineNewTaskExecInstance(ctx, task)
				return err
			},
		))
		assert.Equal(models.TaskExecutionClassScheduled, exec.ExecutionClass)
		assert.Equal(models.TaskExecutionStateScheduled, exec.ExecutionState)
		// scheduled exec inherits the parent's target run time as its enqueue time
		assert.NotNil(exec.TargetEnqueueTime)
		assert.WithinDuration(targetRuntime, *exec.TargetEnqueueTime, time.Second)
	}
}

func TestTaskExecDefineNewTaskRetryExecInstance(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// define a task and an initial (immediate) execution instance, then drive it to FAILED
	var task models.Task
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			task, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
				Name:       "unit-test-exec-retry",
				RetryParam: models.DefaultTaskRetryParameters(),
			})
			return err
		},
	))

	var failedExec models.TaskExecution
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			failedExec, err = dbClient.DefineNewTaskExecInstance(ctx, task)
			if err != nil {
				return err
			}
			// DEFINED -> ENQUEUED -> FAILED
			if err := dbClient.MarkTaskExecQueued(ctx, failedExec.ID); err != nil {
				return err
			}
			return dbClient.MarkTaskExecFailed(ctx, failedExec.ID, "boom")
		},
	))

	// define a retry execution instance off the failed one
	targetRuntime := time.Now().UTC().Add(10 * time.Minute)
	var retry models.TaskExecution
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			retry, err = dbClient.DefineNewTaskRetryExecInstance(
				ctx, task, failedExec, targetRuntime,
			)
			return err
		},
	))
	assert.NotEmpty(retry.ID)
	assert.NotEqual(failedExec.ID, retry.ID)
	assert.Equal(task.ID, retry.TaskID)
	assert.Equal(models.TaskExecutionClassRetry, retry.ExecutionClass)
	assert.Equal(models.TaskExecutionStateScheduled, retry.ExecutionState)
	assert.NotNil(retry.TargetEnqueueTime)
	assert.WithinDuration(targetRuntime, *retry.TargetEnqueueTime, time.Second)
	// the retry points back at the failed parent execution instance
	assert.NotNil(retry.RetryParentExecutionID)
	assert.Equal(failedExec.ID, *retry.RetryParentExecutionID)

	// read back and confirm the parent link persisted
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			readBack, err := dbClient.GetTaskExecution(ctx, retry.ID)
			if err != nil {
				return err
			}
			assert.Equal(models.TaskExecutionClassRetry, readBack.ExecutionClass)
			assert.NotNil(readBack.RetryParentExecutionID)
			assert.Equal(failedExec.ID, *readBack.RetryParentExecutionID)
			return nil
		},
	))
}

func TestTaskExecStateTransitions(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// helper: define a parent task once, reused for all exec instances
	var parentTask models.Task
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			var err error
			parentTask, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
				Name:       "unit-test-exec-transition",
				RetryParam: models.DefaultTaskRetryParameters(),
			})
			return err
		},
	))

	// helper: define a fresh DEFINED exec instance and return its ID
	defineExec := func() string {
		var exec models.TaskExecution
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				exec, err = dbClient.DefineNewTaskExecInstance(ctx, parentTask)
				return err
			},
		))
		return exec.ID
	}

	// helper: read a full exec instance
	readExec := func(id string) models.TaskExecution {
		var exec models.TaskExecution
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				exec, err = dbClient.GetTaskExecution(ctx, id)
				return err
			},
		))
		return exec
	}

	// Case 0: full happy path DEFINED -> ENQUEUED -> ACQUIRED -> PROCESSING -> PROCESSED
	//         -> FINALIZED. Acquiring records the worker name side effect.
	{
		id := defineExec()

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecQueued(ctx, id)
			},
		))
		assert.Equal(models.TaskExecutionStateEnqueued, readExec(id).ExecutionState)

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecAcquired(ctx, id, "worker-1")
			},
		))
		acquired := readExec(id)
		assert.Equal(models.TaskExecutionStateAcquired, acquired.ExecutionState)
		// worker name side effect recorded
		assert.NotNil(acquired.ExecutionWorkerName)
		assert.Equal("worker-1", *acquired.ExecutionWorkerName)

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecProcessing(ctx, id)
			},
		))
		assert.Equal(models.TaskExecutionStateProcessing, readExec(id).ExecutionState)

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecProcessed(ctx, id)
			},
		))
		assert.Equal(models.TaskExecutionStateProcessed, readExec(id).ExecutionState)

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecFinalized(ctx, id)
			},
		))
		assert.Equal(models.TaskExecutionStateFinalized, readExec(id).ExecutionState)
	}

	// Case 1: failure path records the error message; FAILED -> FINALIZED
	{
		id := defineExec()

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecQueued(ctx, id)
			},
		))
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecFailed(ctx, id, "processing blew up")
			},
		))
		failed := readExec(id)
		assert.Equal(models.TaskExecutionStateFailed, failed.ExecutionState)
		// error message side effect recorded
		assert.NotNil(failed.ErrorMessage)
		assert.Equal("processing blew up", *failed.ErrorMessage)

		// FAILED can be finalized
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecFinalized(ctx, id)
			},
		))
		assert.Equal(models.TaskExecutionStateFinalized, readExec(id).ExecutionState)
	}

	// Case 2: cancellation records the cancel message; DEFINED -> CANCELLED
	{
		id := defineExec()

		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecCancelled(ctx, id, "user aborted")
			},
		))
		cancelled := readExec(id)
		assert.Equal(models.TaskExecutionStateCancelled, cancelled.ExecutionState)
		// cancel message is stored in the same error_msg column
		assert.NotNil(cancelled.ErrorMessage)
		assert.Equal("user aborted", *cancelled.ErrorMessage)
	}

	// Case 3: illegal transition DEFINED -> PROCESSED is rejected as a consistency error
	{
		id := defineExec()
		err := persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecProcessed(ctx, id)
			},
		)
		assert.NotNil(err)
		var consistency goutils.ConsistencyError
		assert.True(errors.As(err, &consistency), "expected ConsistencyError, got %T", err)
		// state unchanged
		assert.Equal(models.TaskExecutionStateDefined, readExec(id).ExecutionState)
	}

	// Case 4: transition on a non-existent exec instance surfaces NotFoundError
	{
		err := persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				return dbClient.MarkTaskExecQueued(ctx, ulid.Make().String())
			},
		)
		assert.NotNil(err)
		var notFound goutils.NotFoundError
		assert.True(errors.As(err, &notFound), "expected NotFoundError, got %T", err)
	}
}

func TestTaskExecListAllExecutions(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// helper: run ListAllExecutions with a filter
	listExecs := func(f db.TaskExecutionQueryFilter) []models.TaskExecution {
		var out []models.TaskExecution
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				out, err = dbClient.ListAllExecutions(ctx, f)
				return err
			},
		))
		return out
	}

	// helper: collect returned exec IDs into a set
	idSet := func(execs []models.TaskExecution) map[string]bool {
		out := map[string]bool{}
		for _, exec := range execs {
			out[exec.ID] = true
		}
		return out
	}

	// ----------------------------------------------------------------------------------
	// Parent tasks. taskImm (immediate, early deadline) and taskSched (scheduled, late
	// deadline, target run time `startEarly`). Deadlines are inherited by exec instances.

	early := time.Now().UTC().Add(30 * time.Minute) // deadline cutoff falls above this
	late := time.Now().UTC().Add(90 * time.Minute)
	startEarly := time.Now().UTC().Add(20 * time.Minute) // TargetStart cutoff above this
	startLate := time.Now().UTC().Add(80 * time.Minute)

	defineImmParent := func(deadline time.Time) models.Task {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewOneShotTask(ctx, db.NewTaskParameter{
					Name:       "unit-test-list-exec-imm",
					RetryParam: models.DefaultTaskRetryParameters(),
					Deadline:   &deadline,
				})
				return err
			},
		))
		return task
	}
	defineSchedParent := func(deadline, targetRun time.Time) models.Task {
		var task models.Task
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				task, err = dbClient.DefineNewScheduledOneShotTask(
					ctx,
					db.NewTaskParameter{
						Name:       "unit-test-list-exec-sched",
						RetryParam: models.DefaultTaskRetryParameters(),
						Deadline:   &deadline,
					},
					targetRun,
				)
				return err
			},
		))
		return task
	}

	taskImm := defineImmParent(early)
	taskSched := defineSchedParent(late, startEarly)

	// ----------------------------------------------------------------------------------
	// Seed exec instances one-per-transaction (2ms apart -> distinct created_at ordering).
	//
	// var             | parent    | class      | state      | worker   | deadline | execute_at
	// ----------------+-----------+------------+------------+----------+----------+-----------
	// immDefined      | taskImm   | IMMEDIATE  | DEFINED    | -        | early    | NULL
	// immEnqueued     | taskImm   | IMMEDIATE  | ENQUEUED   | -        | early    | NULL
	// immAcquired     | taskImm   | IMMEDIATE  | ACQUIRED   | worker-1 | early    | NULL
	// schedScheduled  | taskSched | SCHEDULED  | SCHEDULED  | -        | late     | startEarly
	// failedForRetry  | taskSched | SCHEDULED  | FAILED     | -        | late     | startEarly
	// retryInst       | taskSched | RETRY      | SCHEDULED  | -        | late     | startLate

	var created []string
	newExec := func(parent models.Task) models.TaskExecution {
		var exec models.TaskExecution
		assert.Nil(persistence.UseDatabaseInTransaction(
			utCtx, func(ctx context.Context, dbClient db.Database) error {
				var err error
				exec, err = dbClient.DefineNewTaskExecInstance(ctx, parent)
				return err
			},
		))
		created = append(created, exec.ID)
		time.Sleep(2 * time.Millisecond)
		return exec
	}
	drive := func(fn func(ctx context.Context, dbClient db.Database) error) {
		assert.Nil(persistence.UseDatabaseInTransaction(utCtx, fn))
	}

	immDefined := newExec(taskImm)

	immEnqueued := newExec(taskImm)
	drive(func(ctx context.Context, dbClient db.Database) error {
		return dbClient.MarkTaskExecQueued(ctx, immEnqueued.ID)
	})

	immAcquired := newExec(taskImm)
	drive(func(ctx context.Context, dbClient db.Database) error {
		if err := dbClient.MarkTaskExecQueued(ctx, immAcquired.ID); err != nil {
			return err
		}
		return dbClient.MarkTaskExecAcquired(ctx, immAcquired.ID, "worker-1")
	})

	schedScheduled := newExec(taskSched)

	failedForRetry := newExec(taskSched)
	drive(func(ctx context.Context, dbClient db.Database) error {
		if err := dbClient.MarkTaskExecQueued(ctx, failedForRetry.ID); err != nil {
			return err
		}
		return dbClient.MarkTaskExecFailed(ctx, failedForRetry.ID, "boom")
	})

	var retryInst models.TaskExecution
	drive(func(ctx context.Context, dbClient db.Database) error {
		var err error
		retryInst, err = dbClient.DefineNewTaskRetryExecInstance(
			ctx, taskSched, failedForRetry, startLate,
		)
		return err
	})
	created = append(created, retryInst.ID)

	// full corpus in created_at order
	orderedIDs := []string{
		immDefined.ID, immEnqueued.ID, immAcquired.ID,
		schedScheduled.ID, failedForRetry.ID, retryInst.ID,
	}
	assert.Equal(orderedIDs, created)

	// ----------------------------------------------------------------------------------
	// Isolation cases

	// Case 0: ParentTaskID - only that parent's instances
	{
		got := idSet(listExecs(db.TaskExecutionQueryFilter{ParentTaskID: &taskImm.ID}))
		assert.Len(got, 3)
		assert.True(got[immDefined.ID])
		assert.True(got[immEnqueued.ID])
		assert.True(got[immAcquired.ID])

		// negative: a parent that owns nothing yields empty
		none := ulid.Make().String()
		assert.Empty(listExecs(db.TaskExecutionQueryFilter{ParentTaskID: &none}))
	}

	// Case 1: ExecutionWorkerName - only the instance acquired by that worker
	{
		worker := "worker-1"
		got := listExecs(db.TaskExecutionQueryFilter{ExecutionWorkerName: &worker})
		assert.Len(got, 1)
		assert.Equal(immAcquired.ID, got[0].ID)
		assert.NotNil(got[0].ExecutionWorkerName)
		assert.Equal("worker-1", *got[0].ExecutionWorkerName)
	}

	// Case 2: ExecClasses - only retries
	{
		got := listExecs(db.TaskExecutionQueryFilter{
			ExecClasses: []models.TaskExecutionClassENUM{models.TaskExecutionClassRetry},
		})
		assert.Len(got, 1)
		assert.Equal(retryInst.ID, got[0].ID)
		assert.Equal(models.TaskExecutionClassRetry, got[0].ExecutionClass)
	}

	// Case 3: ExecStates - only enqueued
	{
		got := listExecs(db.TaskExecutionQueryFilter{
			ExecStates: []models.TaskExecutionStateENUM{models.TaskExecutionStateEnqueued},
		})
		assert.Len(got, 1)
		assert.Equal(immEnqueued.ID, got[0].ID)
		assert.Equal(models.TaskExecutionStateEnqueued, got[0].ExecutionState)
	}

	// Case 4: TargetDeadline - only non-null deadline <= cutoff; late-deadline excluded
	{
		// cutoff between early and late: catches the three immediate instances only
		cutoff := time.Now().UTC().Add(60 * time.Minute)
		got := idSet(listExecs(db.TaskExecutionQueryFilter{TargetDeadline: &cutoff}))
		assert.Len(got, 3)
		assert.True(got[immDefined.ID])
		assert.True(got[immEnqueued.ID])
		assert.True(got[immAcquired.ID])
		// the scheduled/retry instances carry the late deadline -> excluded
		assert.False(got[schedScheduled.ID])
		assert.False(got[failedForRetry.ID])
		assert.False(got[retryInst.ID])
	}

	// Case 5: TargetStart - only non-null execute_at <= cutoff; NULL execute_at excluded
	{
		// cutoff between startEarly (now+20m) and startLate (now+80m): catches the two
		// scheduled instances with startEarly; retry (startLate) and immediate (NULL) excluded
		cutoff := time.Now().UTC().Add(50 * time.Minute)
		got := idSet(listExecs(db.TaskExecutionQueryFilter{TargetStart: &cutoff}))
		assert.Len(got, 2)
		assert.True(got[schedScheduled.ID])
		assert.True(got[failedForRetry.ID])
		// retry has startLate -> excluded
		assert.False(got[retryInst.ID])
		// immediate instances have NULL execute_at -> excluded
		assert.False(got[immDefined.ID])
		assert.False(got[immEnqueued.ID])
		assert.False(got[immAcquired.ID])
	}

	// Case 6: pagination (Limit + Offset combined) over the full corpus, created_at order
	{
		limit := 2
		offset := 2
		got := listExecs(db.TaskExecutionQueryFilter{
			CommonListEntryQueryFilter: db.CommonListEntryQueryFilter{
				Limit: &limit, Offset: &offset,
			},
		})
		assert.Len(got, 2)
		// window is orderedIDs[2:4]
		assert.Equal(orderedIDs[2], got[0].ID)
		assert.Equal(orderedIDs[3], got[1].ID)
	}

	// ----------------------------------------------------------------------------------
	// Paired (non-exhaustive) cases

	// Case 7: ParentTaskID + ExecStates - intersection only
	{
		// taskSched owns schedScheduled (SCHEDULED), failedForRetry (FAILED),
		// retryInst (SCHEDULED). Combined with state SCHEDULED -> the two scheduled ones.
		got := idSet(listExecs(db.TaskExecutionQueryFilter{
			ParentTaskID: &taskSched.ID,
			ExecStates:   []models.TaskExecutionStateENUM{models.TaskExecutionStateScheduled},
		}))
		assert.Len(got, 2)
		assert.True(got[schedScheduled.ID])
		assert.True(got[retryInst.ID])
		// failedForRetry is this parent's but wrong state -> excluded
		assert.False(got[failedForRetry.ID])
	}

	// Case 8: ExecClasses + ExecStates - intersection only
	{
		// SCHEDULED class instances: schedScheduled (SCHEDULED), failedForRetry (FAILED).
		// Combined with state FAILED -> only failedForRetry. retryInst is FAILED-adjacent
		// but RETRY class -> excluded.
		got := listExecs(db.TaskExecutionQueryFilter{
			ExecClasses: []models.TaskExecutionClassENUM{models.TaskExecutionClassScheduled},
			ExecStates:  []models.TaskExecutionStateENUM{models.TaskExecutionStateFailed},
		})
		assert.Len(got, 1)
		assert.Equal(failedForRetry.ID, got[0].ID)
	}

	// Case 9: ExecutionWorkerName + ExecStates - intersection only
	{
		worker := "worker-1"
		got := listExecs(db.TaskExecutionQueryFilter{
			ExecutionWorkerName: &worker,
			ExecStates:          []models.TaskExecutionStateENUM{models.TaskExecutionStateAcquired},
		})
		assert.Len(got, 1)
		assert.Equal(immAcquired.ID, got[0].ID)
	}
}
