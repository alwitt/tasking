package workflow

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
	"github.com/alwitt/tasking/notify"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// NotifyConsumerFactoryCB factory function signature for defining the scheduler's notification
// consumer. Defaults to notify.NewConsumer; overridable for tests.
type NotifyConsumerFactoryCB func(
	parentCtx context.Context, params notify.NewConsumerParams,
) (notify.Consumer, error)

// Scheduler workflow scheduler: the single point of truth and the only mutator of workflow /
// step state. See workflow/DESIGN.md "Workflow Scheduler".
type Scheduler interface {
	/*
		Start the scheduler processing units

			@param ctx context.Context - execution context
	*/
	Start(ctx context.Context) error

	/*
		Stop the workflow scheduler processing units

			@param ctx context.Context - execution context
	*/
	Stop(ctx context.Context) error
}

// schedulerImpl implements Scheduler.
//
// Unlike the task scheduler, the workflow scheduler is a single-serial-consumer: it has no
// goutils.TaskProcessor worker and no per-task execution-queue senders. One support goroutine
// (processQueue) dequeues a scheduler event off its dedicated IPC queue, parses it, and runs the
// associated logic INLINE. The maintenance interval timer is the only other running component,
// and it does not invoke maintenance directly - it enqueues a Workflow State Maintenance message
// onto the same queue (ipcSender), so maintenance rides the same serial path as every event.
type schedulerImpl struct {
	goutils.Component
	validator *validator.Validate

	config models.WorkflowSchedulerConfig

	persistence db.Client

	wg           *sync.WaitGroup
	runCtx       context.Context
	runCtxCancel context.CancelFunc

	maintenanceTimer goutils.IntervalTimer

	ipcName string
	// ipcReceiver receives scheduler events off the workflow scheduler queue.
	ipcReceiver common.IPCMessageReceive
	// ipcSender enqueues onto the SAME workflow scheduler queue - used by the maintenance timer to
	// post Workflow State Maintenance ticks, and (in later slices) for the scheduler's self-emitted
	// Process Workflow / Schedule Workflow Step / forwarded Execution Update events.
	ipcSender common.IPCMessageSend

	// taskClient commands the task engine - the scheduler's one outbound channel to it. Schedule
	// Workflow Step defines + submits a step's execution task through this client; later slices
	// (timeout / cancel) also cancel step tasks through it. Feedback comes back the other way, over
	// notify pub/sub (not this client).
	taskClient task.Client

	// notifyConsumer the long-lived notify subscriber on the engine's creator channel. Its callback
	// (onNotification) adapts each task terminal event into a task-keyed Step Task Update on the
	// scheduler queue. Started/stopped with the scheduler; the Redis client and factory that built
	// it are not retained.
	notifyConsumer notify.Consumer
}

// NewWorkflowSchedulerParams init parameters for a workflow scheduler
type NewWorkflowSchedulerParams struct {
	// Persistence persistence client
	Persistence db.Client `validate:"required"`
	// TaskClient task engine client, used to dispatch (and later cancel) step execution tasks
	TaskClient task.Client `validate:"required"`
	// Config workflow scheduler config
	Config models.WorkflowSchedulerConfig `validate:"required"`
	// Redis REDIS client
	Redis goutilsRedis.Client `validate:"required"`
	// IPCReceiverFactory factory function to define Redis based IPC message receivers
	IPCReceiverFactory task.IPCMsgReceiverFactoryCB `validate:"required"`
	// IPCSenderFactory factory function to define Redis based IPC message senders
	IPCSenderFactory task.IPCMsgSenderFactoryCB `validate:"required"`
	// NotifyConsumerFactory factory function to define the notification consumer the scheduler
	// subscribes task-engine feedback with. Neither this factory nor Redis is retained after
	// construction - the resulting Consumer is.
	NotifyConsumerFactory NotifyConsumerFactoryCB `validate:"required"`
}

/*
NewWorkflowScheduler define a new workflow scheduler

	@param parentCtx context.Context - the parent execution context
	@param params NewWorkflowSchedulerParams - parameters of the workflow scheduler
	@returns new workflow scheduler
*/
func NewWorkflowScheduler(
	parentCtx context.Context,
	params NewWorkflowSchedulerParams,
) (Scheduler, error) {
	logTags := log.Fields{"package": "tasking", "module": "workflow", "component": "scheduler"}

	validate := validator.New()
	if err := models.RegisterWithValidator(validate); err != nil {
		return nil, goutils.NewRuntimeError(
			"failed to install custom validation macros", err, true,
		)
	}

	if err := validate.Struct(&params); err != nil {
		return nil, goutils.NewBadInputError("workflow scheduler param is invalid", err, true)
	}

	instance := &schedulerImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		validator:   validate,
		config:      params.Config,
		wg:          &sync.WaitGroup{},
		persistence: params.Persistence,
		taskClient:  params.TaskClient,
		ipcName:     "workflow-scheduler",
	}
	instance.runCtx, instance.runCtxCancel = context.WithCancel(parentCtx)

	// ------------------------------------------------------------------------------------
	// Prepare periodic maintenance timer

	var err error
	instance.maintenanceTimer, err = goutils.GetIntervalTimerInstance(
		instance.runCtx, instance.wg, log.Fields{
			"module":        "workflow",
			"component":     "workflow-scheduler",
			"sub-component": "maintenance-timer",
		},
	)
	if err != nil {
		return nil, models.NewWorkflowSchedulerError(
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
		return nil, models.NewWorkflowSchedulerError(
			fmt.Sprintf(
				"failed to initialize scheduler queue receiver for queue '%s'",
				params.Config.SchedulerQueue,
			), err, true,
		)
	}

	// Define the scheduler queue sender - the same queue as the receiver, so the scheduler can
	// enqueue its own events (maintenance ticks now; self-emitted events later).
	instance.ipcSender, err = params.IPCSenderFactory(
		instance.runCtx, params.Config.SchedulerQueue, params.Redis, instance.ipcName,
	)
	if err != nil {
		return nil, models.NewWorkflowSchedulerError(
			fmt.Sprintf(
				"failed to initialize scheduler queue sender for queue '%s'",
				params.Config.SchedulerQueue,
			), err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Prepare the notification consumer for task-engine feedback.
	//
	// One static subscription on the engine's creator channel: every step task is submitted with the
	// reserved WorkflowExecutionTaskCreator, so all their terminal events land here. onNotification
	// adapts each into a task-keyed Step Task Update on the scheduler queue. Neither Redis nor the
	// factory is retained - only the resulting Consumer.
	instance.notifyConsumer, err = params.NotifyConsumerFactory(
		instance.runCtx, notify.NewConsumerParams{
			Redis: params.Redis,
			Name:  instance.ipcName + "-notify",
			Topics: []string{
				models.BuildNotifyCreatorChannelName(models.WorkflowExecutionTaskCreator),
			},
			Callback: instance.onNotification,
		},
	)
	if err != nil {
		return nil, models.NewWorkflowSchedulerError(
			"failed to initialize notification consumer", err, true,
		)
	}

	return instance, nil
}

/*
Start the scheduler processing units

	@param ctx context.Context - execution context
*/
func (s *schedulerImpl) Start(_ context.Context) error {
	// Recover any messages left in the queue buffer by a previous run before we begin consuming
	// the main queue. Must happen before processQueue starts.
	if err := s.recoverBufferedMessages(s.runCtx); err != nil {
		return models.NewWorkflowSchedulerError(
			"failed to recover buffered queue messages", err, true,
		)
	}

	// Start maintenance timer. On each tick it enqueues a Workflow State Maintenance message onto
	// the scheduler queue rather than invoking maintenance directly, so the single processQueue
	// loop runs maintenance in the same serial path as every other event.
	if err := s.maintenanceTimer.Start(s.config.MaintenanceTimerInt(), func() error {
		if err := s.ipcSender.EnqueueMessage(
			s.runCtx, models.PrepareIPCMsgWFMaintenance(s.ipcName, time.Now()),
		); err != nil {
			return models.NewWorkflowSchedulerError(
				"failed to enqueue workflow state maintenance message", err, true,
			)
		}
		return nil
	}, false); err != nil {
		return models.NewWorkflowSchedulerError("failed to start maintenance timer", err, true)
	}

	// Start the queue receiver task
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.processQueue()
	}()

	// Start consuming task-engine feedback. The callback only enqueues onto the scheduler queue, so
	// starting it after processQueue is fine: a poke that lands before the loop is fully up simply
	// waits in the queue.
	if err := s.notifyConsumer.Start(s.runCtx); err != nil {
		return models.NewWorkflowSchedulerError("failed to start notification consumer", err, true)
	}

	return nil
}

/*
Stop the workflow scheduler processing units

	@param ctx context.Context - execution context
*/
func (s *schedulerImpl) Stop(ctx context.Context) error {
	logTags := s.GetLogTagsForContext(ctx)

	// Stop inbound feedback first so no new pokes are enqueued while we shut down.
	if err := s.notifyConsumer.Stop(ctx); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to stop notification consumer")
	}

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
