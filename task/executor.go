// Package task - task processing engine
package task

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// ExecutionCompleteCallback callback to upon completing execution instance processing
type ExecutionCompleteCallback func(
	ctx context.Context, instanceID string, err error, timestamp time.Time,
)

// ExecutorSupport task executor support data
type ExecutorSupport struct {
	// Persistence persistence client
	Persistence db.Client `validate:"required"`

	// OnCompleteCB completion callback
	OnCompleteCB ExecutionCompleteCallback `validate:"required"`
}

// Executor process task execution instances
type Executor interface {
	/*
		ProcessExecutionInstance submit a new task execution instance for processing

			@param ctx context.Context - execution context
			@param instanceID string - execution instance ID
	*/
	ProcessExecutionInstance(ctx context.Context, instanceID string) error

	/*
		Stop the queue executor

			@param ctx context.Context - execution context
	*/
	Stop(ctx context.Context) error
}

// executorImpl implements Executor
type executorImpl struct {
	goutils.Component
	validator *validator.Validate
	queue     string
	support   ExecutorSupport

	wg              *sync.WaitGroup
	workerCtx       context.Context
	workerCtxCancel context.CancelFunc
	workers         goutils.TaskProcessor

	// availableProcessors map of available processors for various supported tasks. Fixed at
	// construction and never mutated afterwards, so it needs no lock.
	availableProcessors map[string]models.TaskExecutionProcessor
}

/*
NewExecutor define new task executor for a particular task queue

	@param parentCtx context.Context - the parent execution context
	@param taskQueue string - task queue being supported
	@param workerCount int - number of workers to spawn
	@param requestBufferLen int - number of execution requests to buffer for the worker pool
	@param support ExecutorSupport - execution support package
	@param processors map[string]models.TaskExecutionProcessor - task-name to processor mapping
	    this executor supports. Fixed at construction; must contain no nil processors.
	@returns new task executor
*/
func NewExecutor(
	parentCtx context.Context,
	taskQueue string,
	workerCount int,
	requestBufferLen int,
	support ExecutorSupport,
	processors map[string]models.TaskExecutionProcessor,
) (Executor, error) {
	logTags := log.Fields{
		"package": "tasking", "module": "task", "component": "task-executor", "queue": taskQueue,
	}

	validate := validator.New()
	if err := validate.Struct(&support); err != nil {
		return nil, goutils.NewBadInputError("execution support package is invalid", err, true)
	}

	// Copy the processor mapping so the executor owns an immutable snapshot, and reject nil
	// processors up front rather than discovering them at dispatch time.
	availableProcessors := make(map[string]models.TaskExecutionProcessor, len(processors))
	for taskName, processor := range processors {
		if processor == nil {
			return nil, goutils.NewBadInputError(
				fmt.Sprintf("can't register nil processor for task name %s", taskName), nil, true,
			)
		}
		availableProcessors[taskName] = processor
	}

	instance := &executorImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		validator:           validate,
		queue:               taskQueue,
		support:             support,
		wg:                  &sync.WaitGroup{},
		availableProcessors: availableProcessors,
	}
	instance.workerCtx, instance.workerCtxCancel = context.WithCancel(parentCtx)
	if err := models.RegisterWithValidator(instance.validator); err != nil {
		return nil, models.NewTaskExecutorError(
			"failed to install custom validation macros", err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Define the worker pool

	workersLogTags := log.Fields{
		"module":        "task",
		"component":     "task-executor",
		"queue":         taskQueue,
		"sub-component": "worker-pool",
	}
	var err error
	instance.workers, err = goutils.GetNewTaskProcessorInstance(
		instance.workerCtx,
		fmt.Sprintf("'%s'-task-workers", taskQueue),
		requestBufferLen,
		workersLogTags,
		nil,
	)
	if err != nil {
		return nil, models.NewTaskExecutorError(
			fmt.Sprintf("failed to define worker pool for queue %s", taskQueue), err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Install support function

	if err := instance.workers.AddToTaskExecutionMap(
		reflect.TypeOf(executorWorkReq{}),
		func(taskParam interface{}) error {
			request, ok := taskParam.(executorWorkReq)
			if ok {
				return instance.processExecutionInstance(request.InstanceID)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskExecutorError(
			fmt.Sprintf("failed to register task with queue %s worker pool", taskQueue), err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Start workers

	for itr := 0; itr < workerCount; itr++ {
		if err := instance.workers.StartEventLoop(instance.wg); err != nil {
			return nil, models.NewTaskExecutorError(
				fmt.Sprintf("failed to start worker %d", itr), err, true,
			)
		}
	}

	return instance, nil
}

/*
Stop the queue executor

	@param ctx context.Context - execution context
*/
func (e *executorImpl) Stop(ctx context.Context) error {
	if err := e.workers.StopEventLoop(); err != nil {
		return models.NewTaskExecutorError(
			fmt.Sprintf("failed to stop queue %s executor worker pool", e.queue), err, true,
		)
	}
	e.workerCtxCancel()
	if err := goutils.TimeBoundedWaitGroupWait(ctx, e.wg, time.Second*5); err != nil {
		return models.NewTaskExecutorError(
			fmt.Sprintf("queue %s executor did not stop in time", e.queue), err, true,
		)
	}
	return nil
}

// notifyOnComplete invoke the completion callback if one was provided
func (e *executorImpl) notifyOnComplete(
	ctx context.Context, instanceID string, err error, timestamp time.Time,
) {
	if e.support.OnCompleteCB != nil {
		e.support.OnCompleteCB(ctx, instanceID, err, timestamp)
	}
}

// executorWorkReq [worker request] new task execution instance to process
type executorWorkReq struct {
	InstanceID string
}

/*
ProcessExecutionInstance submit a new task execution instance for processing

	@param ctx context.Context - execution context
	@param instanceID string - execution instance ID
*/
func (e *executorImpl) ProcessExecutionInstance(
	ctx context.Context, instanceID string,
) error {
	if err := e.workers.Submit(ctx, executorWorkReq{
		InstanceID: instanceID,
	}); err != nil {
		return models.NewTaskExecutorError(
			"failed to submit task execution instance ID for processing", err, true,
		)
	}
	return nil
}

// processExecutionInstance process a task execution instance
func (e *executorImpl) processExecutionInstance(
	instanceID string,
) error {
	// ------------------------------------------------------------------------------------
	// Pre-processing

	var taskExecutionEntry models.TaskExecution
	var taskEntry models.Task
	if dbErr := e.support.Persistence.UseDatabaseInTransaction(
		e.workerCtx,
		func(dbCtx context.Context, dbClient db.Database) error {
			var err error

			// Fetch the execution instance
			taskExecutionEntry, err = dbClient.GetTaskExecution(dbCtx, instanceID)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf("failed to fetch execution instance %s", instanceID), err, true,
				)
			}

			// Verify the execution instance is valid
			if err := e.validator.Struct(&taskExecutionEntry); err != nil {
				return goutils.NewConsistencyError(
					fmt.Sprintf("fetched execution instance %s is not valid", instanceID), err, true,
				)
			}

			// Verify the instance can transition into the PROCESSING state
			if err := taskExecutionEntry.ValidNextState(
				models.TaskExecutionStateProcessing,
			); err != nil {
				return goutils.NewConsistencyError(fmt.Sprintf(
					"execution instance %s can't be processed from '%s' state",
					instanceID,
					taskExecutionEntry.ExecutionState,
				), err, true)
			}

			// Fetch the parent task
			taskEntry, err = dbClient.GetTask(dbCtx, taskExecutionEntry.TaskID)
			if err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf(
						"failed to fetch parent task %s of execution instance %s",
						taskExecutionEntry.TaskID,
						instanceID,
					), err, true,
				)
			}

			// Verify the parent task is valid
			if err := e.validator.Struct(&taskEntry); err != nil {
				return goutils.NewConsistencyError(
					fmt.Sprintf(
						"fetched parent task %s of execution instance %s is not valid",
						taskExecutionEntry.TaskID,
						instanceID,
					), err, true,
				)
			}

			// Mark that task is being worked on
			if err := dbClient.MarkTaskExecProcessing(dbCtx, instanceID); err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf(
						"failed to mark execution instance %s in progress", instanceID,
					), err, true,
				)
			}

			return nil
		},
	); dbErr != nil {
		// Notify system of processing failure
		finalErr := models.NewTaskPreprocessError(
			fmt.Sprintf("failed task execution instance %s pre-processing", instanceID), dbErr, true,
		)
		e.notifyOnComplete(e.workerCtx, instanceID, finalErr, time.Now().UTC())
		return finalErr
	}

	// ------------------------------------------------------------------------------------
	// Process the task

	// taskErr holds the outcome of the processing attempt. It is nil on success, and is set
	// to the failure that drives the post-processing defer below (which marks the instance
	// FAILED and notifies). It is declared here, above the processor lookup, so a missing
	// processor is terminated through the same post-processing path rather than short-circuiting
	// before the defer is registered (which would leave the row stuck in PROCESSING).
	var taskErr error

	// Define the post-processing steps
	defer func() {
		completedAt := time.Now().UTC()
		postProcessCtx, postProcessCtxCancel := context.WithTimeout(context.Background(), time.Second*5)
		defer postProcessCtxCancel()
		if dbErr := e.support.Persistence.UseDatabaseInTransaction(
			postProcessCtx,
			func(dbCtx context.Context, dbClient db.Database) error {
				// Mark that task completed
				if taskErr != nil {
					if err := dbClient.MarkTaskExecFailed(
						dbCtx, instanceID, taskErr.Error(), failureDisposition(taskErr), completedAt,
					); err != nil {
						return goutils.NewPersistenceError(
							fmt.Sprintf("failed to mark execution instance %s failed", instanceID), err, true,
						)
					}
				} else {
					if err := dbClient.MarkTaskExecProcessed(dbCtx, instanceID, completedAt); err != nil {
						return goutils.NewPersistenceError(
							fmt.Sprintf("failed to mark execution instance %s complete", instanceID), err, true,
						)
					}
				}
				return nil
			},
		); dbErr != nil {
			// Notify system of processing failure
			finalErr := models.NewTaskPostprocessError(
				fmt.Sprintf("failed task execution instance %s post-processing", instanceID), dbErr, true,
			)
			e.notifyOnComplete(e.workerCtx, instanceID, finalErr, time.Now().UTC())
			return
		}

		// Notify system of completion
		e.notifyOnComplete(e.workerCtx, instanceID, taskErr, time.Now().UTC())
	}()

	// Locate the processor. A missing processor means this queue was handed a task name it has
	// no processor for (a misrouted PENDING_INSTANCE / engine misconfiguration). This is an
	// engine-level failure, not a task-execution failure: a TaskExecutorError is the marker the
	// receiver's onTaskComplete recognizes to report ENGINE_FAILED. Because taskErr is set, the
	// post-processing defer marks the instance FAILED before notifying, keeping the DB and the
	// scheduler in agreement.
	processor, found := e.availableProcessors[taskEntry.TaskName]
	if !found {
		taskErr = models.NewTaskExecutorError(
			fmt.Sprintf(
				"execution instance %s requested missing processor for task name %s",
				instanceID,
				taskEntry.TaskName,
			), nil, true,
		)
		return taskErr
	}

	// Derive the execution context, honoring the execution instance's deadline if set
	var theCtx context.Context
	var theCtxCancel context.CancelFunc
	if taskExecutionEntry.Deadline != nil {
		theCtx, theCtxCancel = context.WithDeadline(e.workerCtx, *taskExecutionEntry.Deadline)
	} else {
		theCtx, theCtxCancel = context.WithCancel(e.workerCtx)
	}
	defer theCtxCancel()

	// Execute the task based on task type. The processor's error is carried as the Core of the
	// wrapping TaskExecutionError, so errors.As can still find a NonRecoverableError a processor
	// returned to opt this failure out of retry.
	if err := processor.ProcessTaskExecution(theCtx, taskEntry, taskExecutionEntry); err != nil {
		taskErr = models.NewTaskExecutionError(
			fmt.Sprintf("failed to execute task %s instance %s", taskEntry.ID, taskExecutionEntry.ID),
			err,
			true,
		)
	}

	return taskErr
}

// failureDisposition derives the retry disposition to persist for a failed execution instance
// from the failure error. A processor may wrap its error in a models.NonRecoverableError to opt
// the failure out of retry; that is the only case that yields NON_RETRYABLE. Every other failure
// (including a missing processor / engine failure) is left nil, which the scheduler treats as
// retryable - engine failures are instead prevented from retrying structurally, by being reported
// as ENGINE_FAILED rather than EXECUTE_FAILED.
func failureDisposition(taskErr error) *models.TaskFailureDispositionENUM {
	var nonRecoverable models.NonRecoverableError
	if errors.As(taskErr, &nonRecoverable) {
		disposition := models.TaskFailureDispositionNonRetryable
		return &disposition
	}
	return nil
}
