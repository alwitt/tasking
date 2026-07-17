package task_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mockmodels "github.com/alwitt/tasking/mocks/models"
	mocktest "github.com/alwitt/tasking/mocks/test"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// validExecution build a TaskExecution which passes validation and is in the
// EXECUTION_ACQUIRED state (i.e. ready to transition into PROCESSING).
func validExecution(instanceID, taskID string) models.TaskExecution {
	return models.TaskExecution{
		ID:             instanceID,
		TaskID:         taskID,
		ExecutionClass: models.TaskExecutionClassImmediate,
		ExecutionState: models.TaskExecutionStateAcquired,
	}
}

// validTask build a Task which passes validation.
func validTask(taskID, taskName string) models.Task {
	return models.Task{
		ID:                taskID,
		TaskName:          taskName,
		TaskScheduleClass: models.TaskScheduleClassImmediateOneShot,
		TaskState:         models.TaskStateActive,
		RetryParams:       models.DefaultTaskRetryParameters(),
	}
}

// waitForOnComplete block on the completion channel with a timeout guard, so a
// hung worker fails the test instead of hanging the suite.
func waitForOnComplete(t *testing.T, complete <-chan error) error {
	t.Helper()
	select {
	case err := <-complete:
		return err
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for OnComplete callback")
		return nil
	}
}

// TestExecutorPreprocessingFailures validates that each pre-processing failure
// branch in processExecutionInstance surfaces the correct wrapped error via the
// OnComplete callback.
func TestExecutorPreprocessingFailures(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	dbFailure := fmt.Errorf("simulated DB failure")

	type testCase struct {
		name string
		// setupDB wires up the mock Database expectations for the case. It is
		// invoked within the mocked UseDatabaseInTransaction.
		setupDB func(mockDatabase *mockdb.Database, instanceID, taskID, taskName string)
		// checkErr assert the error handed to OnComplete is the expected type.
		checkErr func(assert *assert.Assertions, err error)
	}

	// assertPreprocessWrapping confirm the error is a TaskPreprocessError whose
	// Core is of the expected type.
	assertPreprocessCore := func(assert *assert.Assertions, err error, coreTarget any) {
		var preErr models.TaskPreprocessError
		assert.True(errors.As(err, &preErr), "expected TaskPreprocessError, got %T: %v", err, err)
		assert.NotNil(preErr.Core)
		assert.ErrorAs(preErr.Core, coreTarget)
	}

	cases := []testCase{
		{
			name: "GetTaskExecution fails",
			setupDB: func(mockDatabase *mockdb.Database, instanceID, _, _ string) {
				mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(models.TaskExecution{}, dbFailure)
			},
			checkErr: func(assert *assert.Assertions, err error) {
				var core models.PersistenceError
				assertPreprocessCore(assert, err, &core)
			},
		},
		{
			name: "fetched execution instance invalid",
			setupDB: func(mockDatabase *mockdb.Database, instanceID, _, _ string) {
				// missing required fields -> validator.Struct fails
				mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(models.TaskExecution{}, nil)
			},
			checkErr: func(assert *assert.Assertions, err error) {
				var core goutils.ConsistencyError
				assertPreprocessCore(assert, err, &core)
			},
		},
		{
			name: "invalid state transition",
			setupDB: func(mockDatabase *mockdb.Database, instanceID, taskID, _ string) {
				exec := validExecution(instanceID, taskID)
				// DEFINED can't transition into PROCESSING
				exec.ExecutionState = models.TaskExecutionStateDefined
				mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(exec, nil)
			},
			checkErr: func(assert *assert.Assertions, err error) {
				var core goutils.ConsistencyError
				assertPreprocessCore(assert, err, &core)
			},
		},
		{
			name: "GetTask fails",
			setupDB: func(mockDatabase *mockdb.Database, instanceID, taskID, _ string) {
				mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(validExecution(instanceID, taskID), nil)
				mockDatabase.EXPECT().
					GetTask(mock.Anything, taskID).
					Return(models.Task{}, dbFailure)
			},
			checkErr: func(assert *assert.Assertions, err error) {
				var core models.PersistenceError
				assertPreprocessCore(assert, err, &core)
			},
		},
		{
			name: "fetched task invalid",
			setupDB: func(mockDatabase *mockdb.Database, instanceID, taskID, _ string) {
				mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(validExecution(instanceID, taskID), nil)
				// missing required fields -> validator.Struct fails
				mockDatabase.EXPECT().
					GetTask(mock.Anything, taskID).
					Return(models.Task{}, nil)
			},
			checkErr: func(assert *assert.Assertions, err error) {
				var core goutils.ConsistencyError
				assertPreprocessCore(assert, err, &core)
			},
		},
		{
			name: "MarkTaskExecProcessing fails",
			setupDB: func(mockDatabase *mockdb.Database, instanceID, taskID, taskName string) {
				mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(validExecution(instanceID, taskID), nil)
				mockDatabase.EXPECT().
					GetTask(mock.Anything, taskID).
					Return(validTask(taskID, taskName), nil)
				mockDatabase.EXPECT().
					MarkTaskExecProcessing(mock.Anything, instanceID).
					Return(dbFailure)
			},
			checkErr: func(assert *assert.Assertions, err error) {
				var core models.PersistenceError
				assertPreprocessCore(assert, err, &core)
			},
		},
		{
			name: "processor not found",
			setupDB: func(mockDatabase *mockdb.Database, instanceID, taskID, taskName string) {
				mockDatabase.EXPECT().
					GetTaskExecution(mock.Anything, instanceID).
					Return(validExecution(instanceID, taskID), nil)
				mockDatabase.EXPECT().
					GetTask(mock.Anything, taskID).
					Return(validTask(taskID, taskName), nil)
				mockDatabase.EXPECT().
					MarkTaskExecProcessing(mock.Anything, instanceID).
					Return(nil)
			},
			// deliberately do NOT register a processor
			checkErr: func(assert *assert.Assertions, err error) {
				// bare TaskPreprocessError with nil core
				var preErr models.TaskPreprocessError
				assert.True(errors.As(err, &preErr), "expected TaskPreprocessError, got %T: %v", err, err)
				assert.Nil(preErr.Core)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			instanceID := ulid.Make().String()
			taskID := ulid.Make().String()
			taskName := "unit-test-task"

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			cbMock := mocktest.NewUnitTestCallbackCollector(t)

			// Run the pre-processing coreLogic against the mock Database.
			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(func(
					ctx context.Context, core func(context.Context, db.Database) error,
				) error {
					return core(ctx, mockDatabase)
				})

			tc.setupDB(mockDatabase, instanceID, taskID, taskName)

			// Observe the async result via the OnComplete callback.
			complete := make(chan error, 1)
			cbMock.EXPECT().
				OnComplete(mock.Anything, instanceID, mock.Anything, mock.Anything).
				Run(func(_ context.Context, _ string, err error, _ time.Time) {
					complete <- err
				}).
				Return()

			executor, err := task.NewExecutor(utCtx, "unit-test-queue", 1, 1, task.ExecutorSupport{
				Persistence:  mockClient,
				OnCompleteCB: cbMock.OnComplete,
			})
			assert.Nil(err)
			defer func() { assert.Nil(executor.Stop(utCtx)) }()

			assert.Nil(executor.ProcessExecutionInstance(utCtx, instanceID))

			gotErr := waitForOnComplete(t, complete)
			assert.NotNil(gotErr)
			tc.checkErr(assert, gotErr)
		})
	}
}

// TestExecutorExecutionFailure validates that when the registered processor's
// ProcessTaskExecution returns an error, the executor wraps it in a
// TaskExecutionError, records the failure via MarkTaskExecFailed, and surfaces
// the wrapped error through the OnComplete callback.
func TestExecutorExecutionFailure(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	instanceID := ulid.Make().String()
	taskID := ulid.Make().String()
	taskName := "unit-test-task"

	processorErr := fmt.Errorf("simulated processor failure")

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	cbMock := mocktest.NewUnitTestCallbackCollector(t)
	processor := mockmodels.NewTaskExecutionProcessor(t)

	// Both the pre- and post-processing transactions run their coreLogic against
	// the mock Database.
	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(func(
			ctx context.Context, core func(context.Context, db.Database) error,
		) error {
			return core(ctx, mockDatabase)
		})

	// Pre-processing succeeds.
	execEntry := validExecution(instanceID, taskID)
	taskEntry := validTask(taskID, taskName)
	mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instanceID).Return(execEntry, nil)
	mockDatabase.EXPECT().GetTask(mock.Anything, taskID).Return(taskEntry, nil)
	mockDatabase.EXPECT().MarkTaskExecProcessing(mock.Anything, instanceID).Return(nil)

	// The processor fails execution.
	processor.EXPECT().
		ProcessTaskExecution(mock.Anything, taskEntry, execEntry).
		Return(processorErr)

	// Post-processing records the failure.
	mockDatabase.EXPECT().
		MarkTaskExecFailed(mock.Anything, instanceID, mock.Anything, mock.Anything).
		Return(nil)

	// Observe the async result via the OnComplete callback.
	complete := make(chan error, 1)
	cbMock.EXPECT().
		OnComplete(mock.Anything, instanceID, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, err error, _ time.Time) {
			complete <- err
		}).
		Return()

	executor, err := task.NewExecutor(utCtx, "unit-test-queue", 1, 1, task.ExecutorSupport{
		Persistence:  mockClient,
		OnCompleteCB: cbMock.OnComplete,
	})
	assert.Nil(err)
	defer func() { assert.Nil(executor.Stop(utCtx)) }()

	assert.Nil(executor.RegisterTaskProcessor(taskName, processor))

	assert.Nil(executor.ProcessExecutionInstance(utCtx, instanceID))

	gotErr := waitForOnComplete(t, complete)
	assert.NotNil(gotErr)

	// The error handed to OnComplete is a TaskExecutionError wrapping the
	// processor's error.
	var execErr models.TaskExecutionError
	assert.True(
		errors.As(gotErr, &execErr), "expected TaskExecutionError, got %T: %v", gotErr, gotErr,
	)
	assert.ErrorIs(execErr.Core, processorErr)
}

// TestExecutorPostprocessingFailures validates that when the post-processing
// state-marking fails, the executor surfaces a TaskPostprocessError (wrapping a
// PersistenceError) via the OnComplete callback, for both the task-succeeded and
// task-failed branches.
func TestExecutorPostprocessingFailures(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	dbFailure := fmt.Errorf("simulated DB failure")
	processorErr := fmt.Errorf("simulated processor failure")

	type testCase struct {
		name string
		// processorErr the error the registered processor returns (nil means the
		// task succeeded).
		processorErr error
		// setupPostProcess wires up the failing post-processing mark call.
		setupPostProcess func(mockDatabase *mockdb.Database, instanceID string)
	}

	cases := []testCase{
		{
			name:         "task succeeded, MarkTaskExecProcessed fails",
			processorErr: nil,
			setupPostProcess: func(mockDatabase *mockdb.Database, instanceID string) {
				mockDatabase.EXPECT().
					MarkTaskExecProcessed(mock.Anything, instanceID, mock.Anything).
					Return(dbFailure)
			},
		},
		{
			name:         "task failed, MarkTaskExecFailed fails",
			processorErr: processorErr,
			setupPostProcess: func(mockDatabase *mockdb.Database, instanceID string) {
				mockDatabase.EXPECT().
					MarkTaskExecFailed(mock.Anything, instanceID, mock.Anything, mock.Anything).
					Return(dbFailure)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			instanceID := ulid.Make().String()
			taskID := ulid.Make().String()
			taskName := "unit-test-task"

			mockClient := mockdb.NewClient(t)
			mockDatabase := mockdb.NewDatabase(t)
			cbMock := mocktest.NewUnitTestCallbackCollector(t)
			processor := mockmodels.NewTaskExecutionProcessor(t)

			// Both the pre- and post-processing transactions run their coreLogic
			// against the mock Database.
			mockClient.EXPECT().
				UseDatabaseInTransaction(mock.Anything, mock.Anything).
				RunAndReturn(func(
					ctx context.Context, core func(context.Context, db.Database) error,
				) error {
					return core(ctx, mockDatabase)
				})

			// Pre-processing succeeds.
			execEntry := validExecution(instanceID, taskID)
			taskEntry := validTask(taskID, taskName)
			mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instanceID).Return(execEntry, nil)
			mockDatabase.EXPECT().GetTask(mock.Anything, taskID).Return(taskEntry, nil)
			mockDatabase.EXPECT().MarkTaskExecProcessing(mock.Anything, instanceID).Return(nil)

			// The processor runs (succeeds or fails per the case).
			processor.EXPECT().
				ProcessTaskExecution(mock.Anything, taskEntry, execEntry).
				Return(tc.processorErr)

			// Post-processing state-marking fails.
			tc.setupPostProcess(mockDatabase, instanceID)

			// Observe the async result via the OnComplete callback.
			complete := make(chan error, 1)
			cbMock.EXPECT().
				OnComplete(mock.Anything, instanceID, mock.Anything, mock.Anything).
				Run(func(_ context.Context, _ string, err error, _ time.Time) {
					complete <- err
				}).
				Return()

			executor, err := task.NewExecutor(utCtx, "unit-test-queue", 1, 1, task.ExecutorSupport{
				Persistence:  mockClient,
				OnCompleteCB: cbMock.OnComplete,
			})
			assert.Nil(err)
			defer func() { assert.Nil(executor.Stop(utCtx)) }()

			assert.Nil(executor.RegisterTaskProcessor(taskName, processor))

			assert.Nil(executor.ProcessExecutionInstance(utCtx, instanceID))

			gotErr := waitForOnComplete(t, complete)
			assert.NotNil(gotErr)

			// The error handed to OnComplete is a TaskPostprocessError wrapping a
			// PersistenceError.
			var postErr models.TaskPostprocessError
			assert.True(
				errors.As(gotErr, &postErr),
				"expected TaskPostprocessError, got %T: %v", gotErr, gotErr,
			)
			assert.NotNil(postErr.Core)
			var core models.PersistenceError
			assert.ErrorAs(postErr.Core, &core)
		})
	}
}

// TestExecutorHappyPath validates the full success path end-to-end: pre-processing
// succeeds, the registered processor completes without error, post-processing marks
// the instance processed, and the OnComplete callback fires with a nil error.
func TestExecutorHappyPath(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	instanceID := ulid.Make().String()
	taskID := ulid.Make().String()
	taskName := "unit-test-task"

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	cbMock := mocktest.NewUnitTestCallbackCollector(t)
	processor := mockmodels.NewTaskExecutionProcessor(t)

	// Both the pre- and post-processing transactions run their coreLogic against
	// the mock Database.
	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(func(
			ctx context.Context, core func(context.Context, db.Database) error,
		) error {
			return core(ctx, mockDatabase)
		})

	// Pre-processing succeeds.
	execEntry := validExecution(instanceID, taskID)
	taskEntry := validTask(taskID, taskName)
	mockDatabase.EXPECT().GetTaskExecution(mock.Anything, instanceID).Return(execEntry, nil)
	mockDatabase.EXPECT().GetTask(mock.Anything, taskID).Return(taskEntry, nil)
	mockDatabase.EXPECT().MarkTaskExecProcessing(mock.Anything, instanceID).Return(nil)

	// The processor completes successfully.
	processor.EXPECT().
		ProcessTaskExecution(mock.Anything, taskEntry, execEntry).
		Return(nil)

	// Post-processing marks the instance processed.
	mockDatabase.EXPECT().MarkTaskExecProcessed(mock.Anything, instanceID, mock.Anything).Return(nil)

	// Observe the async result via the OnComplete callback.
	complete := make(chan error, 1)
	cbMock.EXPECT().
		OnComplete(mock.Anything, instanceID, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, err error, _ time.Time) {
			complete <- err
		}).
		Return()

	executor, err := task.NewExecutor(utCtx, "unit-test-queue", 1, 1, task.ExecutorSupport{
		Persistence:  mockClient,
		OnCompleteCB: cbMock.OnComplete,
	})
	assert.Nil(err)
	defer func() { assert.Nil(executor.Stop(utCtx)) }()

	assert.Nil(executor.RegisterTaskProcessor(taskName, processor))

	assert.Nil(executor.ProcessExecutionInstance(utCtx, instanceID))

	// The callback fires with no error.
	gotErr := waitForOnComplete(t, complete)
	assert.Nil(gotErr)
}
