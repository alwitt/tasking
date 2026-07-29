package task

import (
	"context"
	"errors"
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

// ExecutorFactoryCB factory function signature for defining new task executors
type ExecutorFactoryCB func(
	parentCtx context.Context,
	taskQueue string,
	workerCount int,
	requestBufferLen int,
	support ExecutorSupport,
	processors map[string]models.TaskExecutionProcessor,
) (Executor, error)

// IPCMsgReceiverFactoryCB factory function signature for defining new Redis
// based IPC message receivers
type IPCMsgReceiverFactoryCB func(
	ctx context.Context, queueName string, redis goutilsRedis.Client, reader string,
) (common.IPCMessageReceive, error)

// IPCMsgSenderFactoryCB factory function signature for defining new Redis
// based IPC message senders
type IPCMsgSenderFactoryCB func(
	ctx context.Context, queueName string, redis goutilsRedis.Client, sender string,
) (common.IPCMessageSend, error)

// Receiver receive task execution instances
type Receiver interface {
	/*
		Initialize perform complete initialization of a task worker instance.
		It consist of several steps.

			@param ctx context.Context - execution context
			@param activeDBClient db.Database - active database client session
	*/
	Initialize(ctx context.Context, activeDBClient db.Database) error

	/*
		Start the task queue processing threads

			@param ctx context.Context - execution context
	*/
	Start(ctx context.Context) error

	/*
		Stop the task queue processing threads

			@param ctx context.Context - execution context
	*/
	Stop(ctx context.Context) error
}

// receiverImpl implements receiver
type receiverImpl struct {
	goutils.Component
	validator *validator.Validate

	config models.TaskReceiverConfig

	support ExecutorSupport

	wg              *sync.WaitGroup
	workerCtx       context.Context
	workerCtxCancel context.CancelFunc

	executors          map[string]Executor
	ipcReceivers       map[string]common.IPCMessageReceive
	schedulerIPCSender common.IPCMessageSend

	ipcMsgPoolLock             *sync.Mutex
	execInstanceOriginalIPCMsg map[string]goutilsRedis.QueueMessageEnvelope

	// onFatal is invoked (at most once, guarded by onFatalOnce) when a queue-processing goroutine
	// hits an unrecoverable fault, instead of terminating the process directly. Defaulted in the
	// constructor to a log.Fatal wrapper when the caller supplies nothing.
	onFatal     models.OnFatalCB
	onFatalOnce sync.Once
}

// NewReceiverParams init parameters for a task receiver
type NewReceiverParams struct {
	// Support execution support package
	Support ExecutorSupport `validate:"required"`
	// Config task receiver config
	Config models.TaskReceiverConfig `validate:"required"`
	// ExecutorFactory factory function to define task executors
	ExecutorFactory ExecutorFactoryCB `validate:"required"`
	// Processors task-name to processor mapping per queue name. Each configured queue must have
	// an entry (possibly empty), and every queue key must be a configured queue. Fixed at
	// construction and handed to that queue's executor.
	Processors map[string]map[string]models.TaskExecutionProcessor `validate:"required"`
	// Redis REDIS client
	Redis goutilsRedis.Client `validate:"required"`
	// IPCReceiverFactory factory function to define Redis based IPC message receivers
	IPCReceiverFactory IPCMsgReceiverFactoryCB `validate:"required"`
	// IPCSenderFactory factory function to define Redis based IPC message senders
	IPCSenderFactory IPCMsgSenderFactoryCB `validate:"required"`
	// OnFatal, when set, is invoked (at most once) when a queue-processing goroutine hits an
	// unrecoverable fault instead of terminating the process. reporter identifies the faulting
	// queue thread; err carries the cause (with code position in its chain); timestamp is when it
	// was detected. When nil, the default logs and calls log.Fatal, preserving prior behavior. The
	// goroutine exits after the callback runs.
	OnFatal models.OnFatalCB
}

/*
NewReceiver define new task receiver

	@param parentCtx context.Context - the parent execution context
	@param params NewReceiverParams - parameters of the task receiver
	@returns new task receiver
*/
func NewReceiver(
	parentCtx context.Context, params NewReceiverParams,
) (Receiver, error) {
	logTags := log.Fields{
		"package":   "tasking",
		"module":    "task",
		"component": "task-receiver",
		"name":      params.Config.Name,
	}

	validate := validator.New()
	if err := validate.Struct(&params); err != nil {
		return nil, goutils.NewBadInputError("receiver param is invalid", err, true)
	}

	// Default the fatal-fault callback to the prior behavior (log and terminate the process) when
	// the caller supplies nothing.
	onFatal := params.OnFatal
	if onFatal == nil {
		onFatal = defaultOnFatal
	}

	instance := &receiverImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		validator:                  validate,
		config:                     params.Config,
		support:                    params.Support,
		wg:                         &sync.WaitGroup{},
		onFatal:                    onFatal,
		executors:                  map[string]Executor{},
		ipcReceivers:               map[string]common.IPCMessageReceive{},
		ipcMsgPoolLock:             &sync.Mutex{},
		execInstanceOriginalIPCMsg: map[string]goutilsRedis.QueueMessageEnvelope{},
	}
	instance.workerCtx, instance.workerCtxCancel = context.WithCancel(parentCtx)
	if err := models.RegisterWithValidator(instance.validator); err != nil {
		return nil, goutils.NewRuntimeError(
			"failed to install custom validation macros", err, true,
		)
	}

	// Define the task queue receivers
	for _, oneQueue := range instance.config.Queues {
		receiver, err := params.IPCReceiverFactory(
			instance.workerCtx, oneQueue.Name, params.Redis, instance.config.Name,
		)
		if err != nil {
			return nil, models.NewTaskReceiverError(
				fmt.Sprintf(
					"failed to initialize task queue receiver for queue '%s'", oneQueue.Name,
				), err, true,
			)
		}
		instance.ipcReceivers[oneQueue.Name] = receiver
	}

	// Validate the processor mapping against the configured queues: every processor must belong
	// to a queue this receiver serves. A processor bound to an unknown queue is a wiring mistake
	// that would otherwise be silently ignored.
	configuredQueues := map[string]bool{}
	for _, oneQueue := range instance.config.Queues {
		configuredQueues[oneQueue.Name] = true
	}
	for queueName := range params.Processors {
		if !configuredQueues[queueName] {
			return nil, goutils.NewBadInputError(
				fmt.Sprintf("processors provided for unconfigured queue '%s'", queueName), nil, true,
			)
		}
	}

	// Define the executors for each task queue
	for _, oneQueue := range instance.config.Queues {
		executor, err := params.ExecutorFactory(
			instance.workerCtx, oneQueue.Name, oneQueue.Workers, oneQueue.BufferRequests, ExecutorSupport{
				Persistence: instance.support.Persistence,
				OnCompleteCB: func(
					ctx context.Context, instanceID string, err error, timestamp time.Time,
				) {
					instance.onTaskComplete(
						ctx, instance.ipcReceivers[oneQueue.Name], instanceID, err, timestamp,
					)
				},
			},
			params.Processors[oneQueue.Name],
		)
		if err != nil {
			return nil, models.NewTaskReceiverError(
				fmt.Sprintf("failed to initialize task executor for queue '%s'", oneQueue.Name), err, true,
			)
		}
		instance.executors[oneQueue.Name] = executor
	}

	// Define the scheduler queue sender
	{
		sender, err := params.IPCSenderFactory(
			instance.workerCtx, instance.config.SchedulerQueue, params.Redis, instance.config.Name,
		)
		if err != nil {
			return nil, models.NewTaskReceiverError(
				fmt.Sprintf(
					"failed to initialize scheduler IPC queue '%s' sender", instance.config.SchedulerQueue,
				), err, true,
			)
		}
		instance.schedulerIPCSender = sender
	}

	return instance, nil
}

// reportTaskExecutionProcessingCompleted helper function to report task execution
// processing completed
func (r *receiverImpl) reportTaskExecutionProcessingCompleted(
	ctx context.Context, instanceID string, timestamp time.Time,
) error {
	msg := models.PrepareIPCMsgTaskExecutionProcessSucceeded(r.config.Name, instanceID, timestamp)
	return r.schedulerIPCSender.EnqueueMessage(ctx, msg)
}

// reportTaskExecutionProcessingFailed helper function to report task execution
// processing failure. The disposition, when non-nil, tells the scheduler whether the failure
// is retryable; a nil disposition is treated as retryable.
func (r *receiverImpl) reportTaskExecutionProcessingFailed(
	ctx context.Context,
	instanceID string,
	disposition *models.TaskFailureDispositionENUM,
	timestamp time.Time,
) error {
	msg := models.PrepareIPCMsgTaskExecutionProcessFailed(
		r.config.Name, instanceID, disposition, timestamp,
	)
	return r.schedulerIPCSender.EnqueueMessage(ctx, msg)
}

// reportTaskExecutionEngineFailed helper function to report that the core task
// engine failed to operate correctly on an execution instance (e.g. the receiver
// could not claim it, or could not submit it to the executor)
func (r *receiverImpl) reportTaskExecutionEngineFailed(
	ctx context.Context, instanceID string, timestamp time.Time,
) error {
	msg := models.PrepareIPCMsgTaskExecutionEngineFailed(r.config.Name, instanceID, timestamp)
	return r.schedulerIPCSender.EnqueueMessage(ctx, msg)
}

/*
recordInvalidMessage record an audit event for a poison IPC message (one that can never be
processed) received on a task queue. The audit write is best-effort: if it fails, the failure
is logged but not propagated, so an un-auditable poison message cannot stall the receiver.

It runs through db.ActiveSessionWrapper so it works both on the live processing path (pass a
nil activeDBClient - a fresh transaction is opened) and during Initialize's reconciliation
(pass the active session so it joins the ongoing work).

	@param ctx context.Context - execution context
	@param activeDBClient db.Database - active database session, or nil to open a new one
	@param rawPayload string - the raw message payload, if it was readable ("" otherwise)
	@param reason string - human-readable reason the message was rejected
*/
func (r *receiverImpl) recordInvalidMessage(
	ctx context.Context, activeDBClient db.Database, rawPayload, reason string,
) {
	logTags := r.GetLogTagsForContext(ctx)

	if dbErr := db.ActiveSessionWrapper(
		ctx, activeDBClient, r.support.Persistence,
		func(dbCtx context.Context, dbClient db.Database) error {
			return dbClient.RecordInvalidTaskIPCMessage(
				dbCtx, r.config.Name+"/receiver", rawPayload, reason,
			)
		},
	); dbErr != nil {
		log.
			WithError(dbErr).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Errorf("Failed to record audit event for invalid IPC message (%s)", reason)
	}
}

// isFatalDBError report whether the error chain contains a models.SQLError, which
// indicates the database or the connection to it has failed. Such errors are fatal
// for the receiver and must stop the worker rather than be recovered per-request.
func isFatalDBError(err error) bool {
	var sqlErr goutils.SQLError
	return errors.As(err, &sqlErr)
}

/*
onTaskComplete callback used by task executors to report status of processing

	@param ctx context.Context - execution context
	@param queueReceiver common.IPCMessageReceive - original IPC message queue the task execution
	    request was received on
	@param instanceID string - task execution instance ID
	@param err error - any errors encountered while executing the task
	@param timestamp time.Time - task execution completion timestamp
*/
func (r *receiverImpl) onTaskComplete(
	ctx context.Context,
	queueReceiver common.IPCMessageReceive,
	instanceID string,
	err error,
	timestamp time.Time,
) {
	logTags := r.GetLogTagsForContext(ctx)
	if err != nil {
		// Classify the failure. A TaskExecutorError marks an engine-level failure (e.g. this
		// queue has no processor for the task name) - it is reported as ENGINE_FAILED and is
		// never retried. Any other failure is a task-execution failure reported as EXECUTE_FAILED;
		// a processor may have wrapped its error in a NonRecoverableError to opt out of retry, in
		// which case a NON_RETRYABLE disposition rides along so the scheduler skips the retry.
		var executorErr models.TaskExecutorError
		if errors.As(err, &executorErr) {
			callErr := r.reportTaskExecutionEngineFailed(ctx, instanceID, timestamp)
			if callErr != nil {
				log.
					WithError(callErr).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					WithField("exec-id", instanceID).
					Error("Failed to report task execution engine failure")
			}
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("exec-id", instanceID).
				Error("Task execution instance hit an engine failure during processing")
		} else {
			var disposition *models.TaskFailureDispositionENUM
			var nonRecoverable models.NonRecoverableError
			if errors.As(err, &nonRecoverable) {
				d := models.TaskFailureDispositionNonRetryable
				disposition = &d
			}
			callErr := r.reportTaskExecutionProcessingFailed(ctx, instanceID, disposition, timestamp)
			if callErr != nil {
				log.
					WithError(callErr).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					WithField("exec-id", instanceID).
					Error("Failed to report task execution failure")
			}
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("exec-id", instanceID).
				Error("Task execution instance failed during processing")
		}
	} else {
		callErr := r.reportTaskExecutionProcessingCompleted(ctx, instanceID, timestamp)
		if callErr != nil {
			log.
				WithError(callErr).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("exec-id", instanceID).
				Error("Failed to report task execution completed")
		}
		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			WithField("exec-id", instanceID).
			Info("Task execution instance processed")
	}

	// Remove the original IPC message from the queue buffer
	original := func() goutilsRedis.QueueMessageEnvelope {
		r.ipcMsgPoolLock.Lock()
		defer r.ipcMsgPoolLock.Unlock()
		val, ok := r.execInstanceOriginalIPCMsg[instanceID]
		if ok {
			// Recorded message served its purpose
			delete(r.execInstanceOriginalIPCMsg, instanceID)
			return val
		}
		return nil
	}()

	if delErr := queueReceiver.DeleteBufferedMessage(ctx, original); delErr != nil {
		log.
			WithError(delErr).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			WithField("exec-id", instanceID).
			Error("Failed to delete completed execution request from queue buffer")
	}
}

/*
Initialize perform complete initialization of a task worker instance.
It consist of several steps.

	@param ctx context.Context - execution context
	@param activeDBClient db.Database - active database client session
*/
func (r *receiverImpl) Initialize(
	ctx context.Context, activeDBClient db.Database,
) error {
	logTags := r.GetLogTagsForContext(ctx)

	perQueueInit := func(queueName string) error {
		ipcReceiver := r.ipcReceivers[queueName]

		bufferedExecReq := map[string]models.IPCMessageExecuteInstance{}

		// Go through all the messages in the message queue buffer
		for {
			msg, err := ipcReceiver.DequeueBufferedMessage(ctx, true, nil)

			if err != nil {
				return models.NewTaskReceiverError(
					fmt.Sprintf("failed to read queue '%s' buffer", queueName), err, true,
				)
			}

			// Processed last message
			if msg == nil {
				break
			}

			// Note: DequeueBufferedMessage pops the message off the buffer, so a bad
			// message read here is already removed. Discard it and continue draining
			// rather than crashing the worker on a single poison message.
			payload, err := msg.StringPayload()
			if err != nil {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf("Discarding unreadable message from queue '%s' buffer", queueName)
				r.recordInvalidMessage(ctx, activeDBClient, "", "unreadable payload")
				continue
			}
			parsed, err := models.ParseIPCMessage(r.validator, []byte(payload))
			if err != nil {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf("Discarding unparsable message '%s' from queue '%s' buffer", payload, queueName)
				r.recordInvalidMessage(ctx, activeDBClient, payload, "unparsable message")
				continue
			}

			switch typed := parsed.(type) {
			case models.IPCMessageExecuteInstance:
				if typed.Type == models.IPCMsgTypePendingInstance {
					bufferedExecReq[typed.InstanceID] = typed
				} else {
					log.
						WithFields(goutils.UpdateCodePositionInTags(logTags)).
						Infof("Ignoring buffered IPC message '%s'", payload)
				}

			default:
				log.
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf(
						"Discarding unsupported message type '%s' from queue '%s' buffer",
						reflect.TypeOf(typed), queueName,
					)
				r.recordInvalidMessage(
					ctx, activeDBClient, payload,
					fmt.Sprintf("unsupported message type '%s'", reflect.TypeOf(typed)),
				)
				continue
			}
		}

		failedReq := []string{}
		retryReq := []models.IPCMessageExecuteInstance{}

		// Go through the execution requests
		if err := db.ActiveSessionWrapper(
			ctx,
			activeDBClient,
			r.support.Persistence,
			func(dbCtx context.Context, dbClient db.Database) error {
				for instanceID, origMsg := range bufferedExecReq {
					execInstance, err := dbClient.GetTaskExecution(dbCtx, instanceID)
					if err != nil {
						return goutils.NewPersistenceError(
							fmt.Sprintf("failed to fetch task execution instance '%s'", instanceID), err, true,
						)
					}
					/*
						Next steps for this execution instance depends on its current state
					*/
					switch execInstance.ExecutionState {
					case models.TaskExecutionStateDefined:
						fallthrough
					case models.TaskExecutionStateScheduled:
						// This does not make sense
						log.
							WithFields(goutils.UpdateCodePositionInTags(logTags)).
							WithField("task-id", execInstance.TaskID).
							WithField("exec-id", execInstance.ID).
							WithField("exec-state", execInstance.ExecutionState).
							Error("Encountered unscheduled task execution instance")

					case models.TaskExecutionStateEnqueued:
						// Retry execution
						log.
							WithFields(goutils.UpdateCodePositionInTags(logTags)).
							WithField("task-id", execInstance.TaskID).
							WithField("exec-id", execInstance.ID).
							WithField("exec-state", execInstance.ExecutionState).
							Info("Requeue request to retry execution instance")
						retryReq = append(retryReq, origMsg)

					case models.TaskExecutionStateAcquired:
						fallthrough
					case models.TaskExecutionStateProcessing:
						// If this worker was processing it, the execution failed
						if execInstance.ExecutionWorkerName != nil &&
							*execInstance.ExecutionWorkerName != r.config.Name {
							log.
								WithFields(goutils.UpdateCodePositionInTags(logTags)).
								WithField("task-id", execInstance.TaskID).
								WithField("exec-id", execInstance.ID).
								WithField("exec-state", execInstance.ExecutionState).
								WithField("executor", *execInstance.ExecutionWorkerName).
								Info("Ignoring execution instance being worked on by another worker")
						} else {
							log.
								WithFields(goutils.UpdateCodePositionInTags(logTags)).
								WithField("task-id", execInstance.TaskID).
								WithField("exec-id", execInstance.ID).
								WithField("exec-state", execInstance.ExecutionState).
								Error("Task execution instance did not complete")
							failedReq = append(failedReq, execInstance.ID)
						}

					case models.TaskExecutionStateProcessed:
						fallthrough
					case models.TaskExecutionStateFailed:
						fallthrough
					case models.TaskExecutionStateFinalized:
						// Ignore already completed execution instance
						log.
							WithFields(goutils.UpdateCodePositionInTags(logTags)).
							WithField("task-id", execInstance.TaskID).
							WithField("exec-id", execInstance.ID).
							WithField("exec-state", execInstance.ExecutionState).
							Error("Encountered completed task execution instance")
					}
				}

				// Find other execution instances which are being processed by this worker
				// and mark them as failed
				otherWorkerOwnedInstance, err := dbClient.ListAllExecutions(
					dbCtx, db.TaskExecutionQueryFilter{
						ExecutionWorkerName: &r.config.Name,
						ExecStates: []models.TaskExecutionStateENUM{
							models.TaskExecutionStateAcquired,
							models.TaskExecutionStateProcessing,
						},
					},
				)
				if err != nil {
					return goutils.NewPersistenceError(
						fmt.Sprintf(
							"failed to list task execution instance owned by '%s'", r.config.Name,
						), err, true,
					)
				}
				for _, instance := range otherWorkerOwnedInstance {
					if err := dbClient.MarkTaskExecFailed(
						dbCtx,
						instance.ID,
						"execution worker restarted before completion",
						nil, // crash mid-run is retryable per the task's policy
						time.Now().UTC(),
					); err != nil {
						return goutils.NewPersistenceError(
							fmt.Sprintf(
								"failed to mark task execution instance '%s' failed", instance.ID,
							), err, true,
						)
					}
					log.
						WithFields(goutils.UpdateCodePositionInTags(logTags)).
						WithField("task-id", instance.TaskID).
						WithField("exec-id", instance.ID).
						WithField("exec-state", instance.ExecutionState).
						Error("Task execution instance did not complete")
					failedReq = append(failedReq, instance.ID)
				}

				return nil
			},
		); err != nil {
			return models.NewTaskReceiverError(
				"failed to process buffered execution request due to DB failure", err, true,
			)
		}

		currentTime := time.Now().UTC()

		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Infof("Notify scheduler %d execution requests failed", len(failedReq))
		for _, instanceID := range failedReq {
			// A crash-orphaned instance defaults to retryable (nil disposition): a worker that
			// died mid-run is exactly the case that should be retried per the task's policy.
			if err := r.reportTaskExecutionProcessingFailed(
				ctx, instanceID, nil, currentTime,
			); err != nil {
				return models.NewTaskReceiverError(
					fmt.Sprintf(
						"failed to report to scheduler execution instance '%s' failed", instanceID,
					), err, true,
				)
			}
		}

		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Infof("Re-enqueue %d execution requests for retry", len(retryReq))
		// Re-enqueue execution request for retry
		for _, request := range retryReq {
			if err := ipcReceiver.ReEnqueueOnMainQueue(ctx, request); err != nil {
				return models.NewTaskReceiverError(
					fmt.Sprintf(
						"failed to enqueue execution instance '%s' for retry", request.InstanceID,
					), err, true,
				)
			}
		}

		return nil
	}

	for queueName := range r.ipcReceivers {
		if err := perQueueInit(queueName); err != nil {
			return models.NewTaskReceiverError(
				fmt.Sprintf("queue '%s' initialization failed", queueName), err, true,
			)
		}
	}

	return nil
}

/*
processOneIPCRequest primary logic which processes one IPC requests on a particular task queue

	@param ctx context.Context - execution context
	@param queueName string - queue name
	@param receiver common.IPCMessageReceive - IPC task queue receiver
	@param executor Executor - IPC task queue executor
*/
func (r *receiverImpl) processOneIPCRequest(
	ctx context.Context,
	queueName string,
	receiver common.IPCMessageReceive,
	executor Executor,
) error {
	logTags := r.GetLogTagsForContext(ctx)
	logTags["queue"] = queueName

	msg, err := receiver.DequeueMessage(ctx, true, nil)
	if err != nil {
		// FATAL
		return models.NewTaskReceiverError("failed to read queue", err, true)
	}

	// ------------------------------------------------------------------------------------
	// Request validation

	// No message to process
	if msg == nil {
		// NOOP
		return nil
	}

	// discardBadMessage drop a bad request from the queue buffer and log any delete failure
	discardBadMessage := func() {
		if delErr := receiver.DeleteBufferedMessage(ctx, msg); delErr != nil {
			log.
				WithError(delErr).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Error("Failed to delete discarded message from queue buffer")
		}
	}

	payload, err := msg.StringPayload()
	if err != nil {
		// Bad request: record an audit event, drop the message and move on
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Discarding unreadable message from queue")
		r.recordInvalidMessage(ctx, nil, "", "unreadable payload")
		discardBadMessage()
		return nil
	}

	parsed, err := models.ParseIPCMessage(r.validator, []byte(payload))
	if err != nil {
		// Bad request: record an audit event, drop the message and move on
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Errorf("Discarding unparsable message '%s' from queue", payload)
		r.recordInvalidMessage(ctx, nil, payload, "unparsable message")
		discardBadMessage()
		return nil
	}

	var execRequest models.IPCMessageExecuteInstance
	switch typed := parsed.(type) {
	case models.IPCMessageExecuteInstance:
		if typed.Type == models.IPCMsgTypePendingInstance {
			execRequest = typed
		} else {
			// Bad request: record an audit event, drop the message and move on
			log.
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Infof("Discarding invalid IPC message '%s'", payload)
			r.recordInvalidMessage(
				ctx, nil, payload,
				fmt.Sprintf("unsupported execute-instance message type '%s'", typed.Type),
			)
			discardBadMessage()
			return nil
		}

	default:
		// Bad request: record an audit event, drop the message and move on
		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Errorf("Discarding unsupported message type '%s' on queue", reflect.TypeOf(typed))
		r.recordInvalidMessage(
			ctx, nil, payload,
			fmt.Sprintf("unsupported message type '%s'", reflect.TypeOf(typed)),
		)
		discardBadMessage()
		return nil
	}

	// ------------------------------------------------------------------------------------
	// Claim ownership of the execution request

	// Claim ownership of the execution instance
	if dbErr := r.support.Persistence.UseDatabaseInTransaction(
		ctx,
		func(dbCtx context.Context, dbClient db.Database) error {
			if err := dbClient.MarkTaskExecAcquired(
				dbCtx, execRequest.InstanceID, r.config.Name,
			); err != nil {
				return goutils.NewPersistenceError(
					fmt.Sprintf(
						"failed to mark execution instance %s acquired", execRequest.InstanceID,
					), err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		// A SQL layer failure means the database itself is broken: FATAL
		if isFatalDBError(dbErr) {
			return models.NewTaskReceiverError(
				fmt.Sprintf(
					"failed to claim ownership of execution instance %s", execRequest.InstanceID,
				), dbErr, true,
			)
		}

		// Otherwise this is a request-level failure. Drop the buffered message and
		// forward the failure to the scheduler to decide next steps.
		err := models.NewTaskReceiverError(
			fmt.Sprintf(
				"failed to claim ownership of execution instance %s", execRequest.InstanceID,
			), dbErr, true,
		)
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to claim execution request, notifying scheduler...")
		if delErr := receiver.DeleteBufferedMessage(ctx, msg); delErr != nil {
			log.
				WithError(delErr).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("exec-id", execRequest.InstanceID).
				Error("Failed to delete buffered message for un-claimable execution request")
		}
		if reportErr := r.reportTaskExecutionEngineFailed(
			ctx, execRequest.InstanceID, time.Now().UTC(),
		); reportErr != nil {
			log.
				WithError(reportErr).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("exec-id", execRequest.InstanceID).
				Error("Failed to report task execution engine failure")
		}
		// NOOP
		return nil
	}

	// ------------------------------------------------------------------------------------
	// Submit for processing

	// Record the message so it can be removed from the queue buffer once the executor
	// reports completion via OnTaskComplete. Recorded before submission so a fast
	// worker completing before this returns still finds the message to clean up.
	{
		r.ipcMsgPoolLock.Lock()
		r.execInstanceOriginalIPCMsg[execRequest.InstanceID] = msg
		r.ipcMsgPoolLock.Unlock()
	}

	// Run processing
	if submitErr := executor.ProcessExecutionInstance(
		ctx, execRequest.InstanceID,
	); submitErr != nil {
		submitErr = models.NewTaskReceiverError("unable to submit task to executor", submitErr, true)
		log.
			WithError(submitErr).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			WithField("exec-id", execRequest.InstanceID).
			Error("Failed to submit execution request to executor, notifying scheduler...")

		// The executor never took the request, so it will never report completion.
		// Drop the recorded message and remove it from the queue buffer directly.
		original := func() goutilsRedis.QueueMessageEnvelope {
			r.ipcMsgPoolLock.Lock()
			defer r.ipcMsgPoolLock.Unlock()
			val, ok := r.execInstanceOriginalIPCMsg[execRequest.InstanceID]
			if ok {
				delete(r.execInstanceOriginalIPCMsg, execRequest.InstanceID)
				return val
			}
			return nil
		}()
		if delErr := receiver.DeleteBufferedMessage(ctx, original); delErr != nil {
			log.
				WithError(delErr).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("exec-id", execRequest.InstanceID).
				Error("Failed to delete buffered message for un-submitted execution request")
		}

		// Mark the instance failed so the state machine continues to flow correctly
		if dbErr := r.support.Persistence.UseDatabaseInTransaction(
			ctx,
			func(dbCtx context.Context, dbClient db.Database) error {
				if err := dbClient.MarkTaskExecFailed(
					dbCtx, execRequest.InstanceID, submitErr.Error(), nil, time.Now().UTC(),
				); err != nil {
					return goutils.NewPersistenceError(
						fmt.Sprintf(
							"failed to mark execution instance %s failed", execRequest.InstanceID,
						), err, true,
					)
				}
				return nil
			},
		); dbErr != nil {
			// A SQL layer failure means the database itself is broken: FATAL
			if isFatalDBError(dbErr) {
				return dbErr
			}
			// Otherwise the maintenance loop is the backstop; log and continue
			log.
				WithError(dbErr).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("exec-id", execRequest.InstanceID).
				Error("Failed to mark un-submitted execution instance failed")
		}

		// This is an engine-level failure; let the scheduler decide next steps
		if reportErr := r.reportTaskExecutionEngineFailed(
			ctx, execRequest.InstanceID, time.Now().UTC(),
		); reportErr != nil {
			log.
				WithError(reportErr).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("exec-id", execRequest.InstanceID).
				Error("Failed to report task execution engine failure")
		}
		// NOOP
		return nil
	}

	return nil
}

// processOneQueue execution thread for driving one task queue
func (r *receiverImpl) processOneQueue(
	queueName string, receiver common.IPCMessageReceive, executor Executor,
) {
	logTags := r.GetLogTagsForContext(r.workerCtx)
	logTags["queue"] = queueName

	log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Info("Starting task queue message processing")
	defer log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Info("Stopped task queue message processing")

	for {
		// verify whether to stop
		if err := r.workerCtx.Err(); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Error("Stopping queue processing due to receiver worker context error")
			}
			break
		}

		if err := r.processOneIPCRequest(r.workerCtx, queueName, receiver, executor); err != nil {
			// The error only decides whether to stop this queue thread: a broken database
			// (models.SQLError) is fatal - every message would fail identically, so continuing is
			// meaningless. Any other failure is logged and processing continues.
			if isFatalDBError(err) {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf("Encountered fatal error while processing IPC messages:\n%+v", err)
				// Hand the fault to the parent application and stop this queue thread.
				r.reportFatal(r.config.Name+"/receiver:"+queueName, err, time.Now())
				return
			}
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf("Failed to process IPC message on queue '%s' (continuing):\n%+v", queueName, err)
		}
	}
}

// reportFatal invokes the parent's OnFatal callback at most once for the lifetime of this
// receiver, regardless of how many queue threads trip a fatal fault simultaneously.
func (r *receiverImpl) reportFatal(reporter string, err error, timestamp time.Time) {
	r.onFatalOnce.Do(func() { r.onFatal(reporter, err, timestamp) })
}

/*
Start the task queue processing threads

	@param ctx context.Context - execution context
*/
func (r *receiverImpl) Start(_ context.Context) error {
	for queueName, receiver := range r.ipcReceivers {
		executor, ok := r.executors[queueName]
		if !ok {
			return goutils.NewConsistencyError(
				fmt.Sprintf("queue '%s' is missing match executor", queueName), nil, true,
			)
		}

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.processOneQueue(queueName, receiver, executor)
		}()
	}

	return nil
}

/*
Stop the task queue processing threads

	@param ctx context.Context - execution context
*/
func (r *receiverImpl) Stop(ctx context.Context) error {
	logTags := r.GetLogTagsForContext(ctx)

	// Stop all the executors
	for queueName, executor := range r.executors {
		if err := executor.Stop(ctx); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				WithField("queue", queueName).
				Error("Executor stop failed")
		}
	}

	r.workerCtxCancel()

	// Wait for all support threads to stop
	if err := goutils.TimeBoundedWaitGroupWait(ctx, r.wg, time.Second*10); err != nil {
		return goutils.NewRuntimeError(
			"task receiver "+r.config.Name+" support task didn't stop in time", err, true,
		)
	}
	return nil
}
