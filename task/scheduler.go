package task

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/common"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// Scheduler task scheduler
type Scheduler interface {
	/*
		Start the scheduler processing units

			@param ctx context.Context - execution context
	*/
	Start(ctx context.Context) error

	/*
		Stop the task scheduler processing units

			@param ctx context.Context - execution context
	*/
	Stop(ctx context.Context) error
}

// schedulerImpl implements Scheduler
type schedulerImpl struct {
	goutils.Component
	validator *validator.Validate

	config models.TaskSchedulerConfig

	persistence db.Client

	wg              *sync.WaitGroup
	worker          goutils.TaskProcessor
	workerCtx       context.Context
	workerCtxCancel context.CancelFunc

	maintenanceTimer goutils.IntervalTimer

	ipcName        string
	ipcReceiver    common.IPCMessageReceive
	taskIPcSenders map[string]common.IPCMessageSend
}

// NewSchedulerParams init parameters for a task scheduler
type NewSchedulerParams struct {
	// Persistence persistence client
	Persistence db.Client
	// Config task scheduler config
	Config models.TaskSchedulerConfig
	// Redis REDIS client
	Redis goutilsRedis.Client
	// IPCReceiverFactory factory function to define Redis based IPC message receivers
	IPCReceiverFactory IPCMsgReceiverFactoryCB
	// IPCSenderFactory factory function to define Redis based IPC message senders
	IPCSenderFactory IPCMsgSenderFactoryCB
}

/*
NewScheduler define new scheduler

	@param parentCtx context.Context - the parent execution context
	@param params NewSchedulerParams - parameters of the task scheduler
	@returns new task scheduler
*/
func NewScheduler(
	parentCtx context.Context,
	params NewSchedulerParams,
) (Scheduler, error) {
	logTags := log.Fields{"package": "tasking", "module": "task", "component": "scheduler"}

	instance := &schedulerImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		validator:      validator.New(),
		config:         params.Config,
		wg:             &sync.WaitGroup{},
		persistence:    params.Persistence,
		ipcName:        "scheduler",
		taskIPcSenders: map[string]common.IPCMessageSend{},
	}
	instance.workerCtx, instance.workerCtxCancel = context.WithCancel(parentCtx)
	if err := models.RegisterWithValidator(instance.validator); err != nil {
		return nil, goutils.NewRuntimeError(
			"failed to install custom validation macros", err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Prepare IPC message processing worker

	var err error
	instance.worker, err = goutils.GetNewTaskProcessorInstance(
		instance.workerCtx, "schedule-request-processor", 10, log.Fields{
			"module": "task", "component": "task-scheduler", "sub-component": "request-processor",
		}, nil,
	)
	if err != nil {
		return nil, models.NewTaskSchedulerError(
			"failed to define core scheduling request processor", err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Install worker functions

	// New pending task needing scheduling
	if err := instance.worker.AddToTaskExecutionMap(
		reflect.TypeOf(schedulerWorkReqNewPendingTask{}),
		func(taskParam interface{}) error {
			newRequest, ok := taskParam.(schedulerWorkReqNewPendingTask)
			if ok {
				return instance.processNewPendingTask(instance.workerCtx, newRequest.TaskID)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to register '%s' handler with worker",
				reflect.TypeOf(schedulerWorkReqNewPendingTask{}),
			), err, true,
		)
	}

	// task being cancelled
	if err := instance.worker.AddToTaskExecutionMap(
		reflect.TypeOf(schedulerWorkReqCancelTask{}),
		func(taskParam interface{}) error {
			newRequest, ok := taskParam.(schedulerWorkReqCancelTask)
			if ok {
				return instance.processCancelTask(
					instance.workerCtx, newRequest.TaskID, newRequest.Timestamp,
				)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to register '%s' handler with worker",
				reflect.TypeOf(schedulerWorkReqCancelTask{}),
			), err, true,
		)
	}

	// task timed out
	if err := instance.worker.AddToTaskExecutionMap(
		reflect.TypeOf(schedulerWorkReqTaskTimedOut{}),
		func(taskParam interface{}) error {
			newRequest, ok := taskParam.(schedulerWorkReqTaskTimedOut)
			if ok {
				return instance.processTaskTimeout(
					instance.workerCtx, newRequest.TaskID, newRequest.Timestamp,
				)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to register '%s' handler with worker",
				reflect.TypeOf(schedulerWorkReqTaskTimedOut{}),
			), err, true,
		)
	}

	// task execution scheduled to start
	if err := instance.worker.AddToTaskExecutionMap(
		reflect.TypeOf(schedulerWorkReqTaskExecutionStarting{}),
		func(taskParam interface{}) error {
			newRequest, ok := taskParam.(schedulerWorkReqTaskExecutionStarting)
			if ok {
				return instance.processTaskExecutionStarting(
					instance.workerCtx, newRequest.InstanceID, newRequest.Timestamp,
				)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to register '%s' handler with worker",
				reflect.TypeOf(schedulerWorkReqTaskExecutionStarting{}),
			), err, true,
		)
	}

	// task execution completed
	if err := instance.worker.AddToTaskExecutionMap(
		reflect.TypeOf(schedulerWorkReqTaskExecutionComplete{}),
		func(taskParam interface{}) error {
			newRequest, ok := taskParam.(schedulerWorkReqTaskExecutionComplete)
			if ok {
				return instance.processTaskExecutionComplete(
					instance.workerCtx, newRequest.InstanceID, newRequest.Timestamp,
				)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to register '%s' handler with worker",
				reflect.TypeOf(schedulerWorkReqTaskExecutionComplete{}),
			), err, true,
		)
	}

	// task execution failed
	if err := instance.worker.AddToTaskExecutionMap(
		reflect.TypeOf(schedulerWorkReqTaskExecutionFailed{}),
		func(taskParam interface{}) error {
			newRequest, ok := taskParam.(schedulerWorkReqTaskExecutionFailed)
			if ok {
				return instance.processTaskExecutionFailed(
					instance.workerCtx, newRequest.InstanceID, newRequest.Timestamp,
				)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to register '%s' handler with worker",
				reflect.TypeOf(schedulerWorkReqTaskExecutionFailed{}),
			), err, true,
		)
	}

	// task execution timed out
	if err := instance.worker.AddToTaskExecutionMap(
		reflect.TypeOf(schedulerWorkReqTaskExecutionTimedOut{}),
		func(taskParam interface{}) error {
			newRequest, ok := taskParam.(schedulerWorkReqTaskExecutionTimedOut)
			if ok {
				return instance.processTaskExecutionTimedOut(
					instance.workerCtx, newRequest.InstanceID, newRequest.Timestamp,
				)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to register '%s' handler with worker",
				reflect.TypeOf(schedulerWorkReqTaskExecutionTimedOut{}),
			), err, true,
		)
	}

	// Periodic maintenance
	if err := instance.worker.AddToTaskExecutionMap(
		reflect.TypeOf(schedulerWorkReqRunMaintenance{}),
		func(taskParam interface{}) error {
			_, ok := taskParam.(schedulerWorkReqRunMaintenance)
			if ok {
				return instance.performMaintenance(instance.workerCtx)
			}
			return goutils.NewConsistencyError(fmt.Sprintf(
				"received unexpected call parameters: %s", reflect.TypeOf(taskParam),
			), nil, true)
		},
	); err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to register '%s' handler with worker",
				reflect.TypeOf(schedulerWorkReqRunMaintenance{}),
			), err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Prepare periodic maintenance timer

	instance.maintenanceTimer, err = goutils.GetIntervalTimerInstance(
		instance.workerCtx, instance.wg, log.Fields{
			"module": "task", "component": "task-scheduler", "sub-component": "maintenance-timer",
		},
	)
	if err != nil {
		return nil, models.NewTaskSchedulerError(
			"failed to define periodic maintenance timer", err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Prepare IPC message queue handles

	// Define the scheduler queue receiver
	{
		receiver, err := params.IPCReceiverFactory(
			instance.workerCtx, params.Config.SchedulerQueue, params.Redis, instance.ipcName,
		)
		if err != nil {
			return nil, models.NewTaskSchedulerError(
				fmt.Sprintf(
					"failed to initialize scheduler queue receiver for queue '%s'",
					params.Config.SchedulerQueue,
				), err, true,
			)
		}
		instance.ipcReceiver = receiver
	}

	// Define the task queue senders
	for _, oneQueue := range params.Config.TaskMappings {
		sender, err := params.IPCSenderFactory(
			instance.workerCtx, oneQueue.ExecutionQueue, params.Redis, instance.ipcName,
		)
		if err != nil {
			return nil, models.NewTaskSchedulerError(
				fmt.Sprintf(
					"failed to initialize task queue sender for queue '%s'", oneQueue.ExecutionQueue,
				), err, true,
			)
		}
		instance.taskIPcSenders[oneQueue.TaskName] = sender
	}

	return instance, nil
}

/*
Start the scheduler processing units

	@param ctx context.Context - execution context
*/
func (s *schedulerImpl) Start(_ context.Context) error {
	// Recover any messages left in the queue buffer by a previous run before we begin
	// consuming the main queue. Must happen before processQueue starts.
	if err := s.recoverBufferedMessages(s.workerCtx); err != nil {
		return models.NewTaskSchedulerError("failed to recover buffered queue messages", err, true)
	}

	// Start the worker
	if err := s.worker.StartEventLoop(s.wg); err != nil {
		return models.NewTaskSchedulerError("failed to start worker", err, true)
	}

	// Start maintenance timer
	if err := s.maintenanceTimer.Start(s.config.MaintenanceTimerInt(), func() error {
		if err := s.worker.Submit(s.workerCtx, schedulerWorkReqRunMaintenance{}); err != nil {
			return models.NewTaskSchedulerError(
				"failed to submit new maintenance request", err, true,
			)
		}
		return nil
	}, false); err != nil {
		return models.NewTaskSchedulerError("failed to start maintenance timer", err, true)
	}

	// Start the queue receiver task
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.processQueue()
	}()

	return nil
}

/*
Stop the task scheduler processing units

	@param ctx context.Context - execution context
*/
func (s *schedulerImpl) Stop(ctx context.Context) error {
	logTags := s.GetLogTagsForContext(ctx)
	s.workerCtxCancel()

	// Stop the maintenance timer
	if err := s.maintenanceTimer.Stop(); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to stop maintenance timer")
	}

	// Stop the worker
	if err := s.worker.StopEventLoop(); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to stop processing worker")
	}

	// Wait for all threads to finish
	return goutils.TimeBoundedWaitGroupWait(ctx, s.wg, time.Second*5)
}
