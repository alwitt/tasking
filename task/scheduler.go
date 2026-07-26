package task

import (
	"context"
	"fmt"
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

	wg           *sync.WaitGroup
	runCtx       context.Context
	runCtxCancel context.CancelFunc

	// onFatal is invoked (at most once, guarded by onFatalOnce) when the processing goroutine
	// hits an unrecoverable fault, instead of terminating the process directly. Defaulted in the
	// constructor to a log.Fatal wrapper when the caller supplies nothing.
	onFatal     models.OnFatalCB
	onFatalOnce sync.Once

	maintenanceTimer goutils.IntervalTimer

	ipcName     string
	ipcReceiver common.IPCMessageReceive
	// ipcSender enqueues onto the SAME scheduler queue as ipcReceiver - used by the maintenance
	// timer to post Task Maintenance ticks so maintenance rides the same serial path as every event.
	ipcSender common.IPCMessageSend
	// taskIPcSenders per-task execution-queue senders the handlers dispatch execution requests
	// through, keyed by task name (distinct from ipcSender, which targets the scheduler's own queue).
	taskIPcSenders map[string]common.IPCMessageSend
}

// NewSchedulerParams init parameters for a task scheduler
type NewSchedulerParams struct {
	// Persistence persistence client
	Persistence db.Client `validate:"required"`
	// Config task scheduler config
	Config models.TaskSchedulerConfig `validate:"required"`
	// Redis REDIS client
	Redis goutilsRedis.Client `validate:"required"`
	// IPCReceiverFactory factory function to define Redis based IPC message receivers
	IPCReceiverFactory IPCMsgReceiverFactoryCB `validate:"required"`
	// IPCSenderFactory factory function to define Redis based IPC message senders
	IPCSenderFactory IPCMsgSenderFactoryCB `validate:"required"`
	// OnFatal, when set, is invoked (at most once) when the processing goroutine hits an
	// unrecoverable fault instead of terminating the process. reporter identifies the faulting
	// thread; err carries the cause (with code position in its chain); timestamp is when it was
	// detected. When nil, the default logs and calls log.Fatal, preserving prior behavior. The
	// goroutine exits after the callback runs.
	OnFatal models.OnFatalCB
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

	validate := validator.New()
	if err := models.RegisterWithValidator(validate); err != nil {
		return nil, goutils.NewRuntimeError(
			"failed to install custom validation macros", err, true,
		)
	}

	if err := validate.Struct(&params); err != nil {
		return nil, goutils.NewBadInputError("scheduler param is invalid", err, true)
	}

	// Default the fatal-fault callback to the prior behavior (log and terminate the process) when
	// the caller supplies nothing.
	onFatal := params.OnFatal
	if onFatal == nil {
		onFatal = defaultOnFatal
	}

	instance := &schedulerImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		validator:      validate,
		config:         params.Config,
		wg:             &sync.WaitGroup{},
		persistence:    params.Persistence,
		onFatal:        onFatal,
		ipcName:        "task-scheduler",
		taskIPcSenders: map[string]common.IPCMessageSend{},
	}
	instance.runCtx, instance.runCtxCancel = context.WithCancel(parentCtx)

	// ------------------------------------------------------------------------------------
	// Prepare periodic maintenance timer

	var err error
	instance.maintenanceTimer, err = goutils.GetIntervalTimerInstance(
		instance.runCtx, instance.wg, log.Fields{
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
	instance.ipcReceiver, err = params.IPCReceiverFactory(
		instance.runCtx, params.Config.SchedulerQueue, params.Redis, instance.ipcName,
	)
	if err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to initialize scheduler queue receiver for queue '%s'",
				params.Config.SchedulerQueue,
			), err, true,
		)
	}

	// Define the scheduler queue sender - the same queue as the receiver, so the maintenance timer
	// can enqueue its own Task Maintenance ticks onto the serial processing path.
	instance.ipcSender, err = params.IPCSenderFactory(
		instance.runCtx, params.Config.SchedulerQueue, params.Redis, instance.ipcName,
	)
	if err != nil {
		return nil, models.NewTaskSchedulerError(
			fmt.Sprintf(
				"failed to initialize scheduler queue sender for queue '%s'",
				params.Config.SchedulerQueue,
			), err, true,
		)
	}

	// Define the task queue senders
	for _, oneQueue := range params.Config.TaskMappings {
		sender, err := params.IPCSenderFactory(
			instance.runCtx, oneQueue.ExecutionQueue, params.Redis, instance.ipcName,
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
	if err := s.recoverBufferedMessages(s.runCtx); err != nil {
		return models.NewTaskSchedulerError("failed to recover buffered queue messages", err, true)
	}

	// Start maintenance timer. On each tick it enqueues a Task Maintenance message onto the
	// scheduler queue rather than invoking maintenance directly, so the single processQueue loop
	// runs maintenance in the same serial path as every other event.
	if err := s.maintenanceTimer.Start(s.config.MaintenanceTimerInt(), func() error {
		if err := s.ipcSender.EnqueueMessage(
			s.runCtx, models.PrepareIPCMsgTaskMaintenance(s.ipcName, time.Now()),
		); err != nil {
			return models.NewTaskSchedulerError(
				"failed to enqueue task maintenance message", err, true,
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

	// Stop the maintenance timer
	if err := s.maintenanceTimer.Stop(); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to stop maintenance timer")
	}

	s.runCtxCancel()
	// Wait for all threads to finish
	return goutils.TimeBoundedWaitGroupWait(ctx, s.wg, time.Second*5)
}

// reportFatal invokes the parent's OnFatal callback at most once for the lifetime of this
// scheduler, regardless of how many times a fatal fault is encountered.
func (s *schedulerImpl) reportFatal(reporter string, err error, timestamp time.Time) {
	s.onFatalOnce.Do(func() { s.onFatal(reporter, err, timestamp) })
}
