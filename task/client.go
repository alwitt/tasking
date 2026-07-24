package task

import (
	"context"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/common"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// DefineTaskParams the per-task parameters shared by the task submission entry points. It
// carries everything needed to define a task except the execution context, the active DB
// transaction, and (for scheduled tasks) the target runtime, which are passed separately so
// this struct stays reusable across the immediate and scheduled variants.
type DefineTaskParams struct {
	// Name the task name. This is used to match the task with the appropriate task execution
	// processor to run the task.
	Name string `validate:"required"`
	// Parameters task processing parameters
	Parameters any
	// Metadata associated metadata
	Metadata any
	// Creator optional per-task creator override; nil uses the client's DefaultCreator. The
	// resolved value must be non-empty or the submit fails validation.
	Creator *string
	// Deadline if specified, the task must complete by this dead line.
	Deadline *time.Time
	// Retry optional per-task retry policy, carried in full (Factor included). When nil the client
	// resolves retry the usual way (the by-task-name policy from the client config, else the
	// default). When non-nil it is used verbatim, overriding both. This is the escape hatch for
	// callers whose retry policy is per-submission rather than a static property of the task name -
	// e.g. the workflow engine, where every step task shares the one name __EXECUTE_WORKFLOW_STEP__
	// but each step carries its own TaskRetryParameters.
	Retry *models.TaskRetryParameters
}

// Client task engine client
type Client interface {
	/*
		DefineAndRunImmediateOneShotTask define and submit an immediate one-shot class task.

		The task entry is defined in a database transaction, then submitted to the scheduler for
		execution. The submit happens after the entry is persisted and is not part of that
		transaction. On error, inspect the returned error with `errors.As`: a
		`models.PersistenceError` means the database operation failed and no task was created; a
		`models.IPCMessageQueueError` means the task was created but could not be submitted to the
		scheduler.

			@param ctx context.Context - execution context
			@param params DefineTaskParams - the task definition parameters
			@param activeDBClient db.Database - an existing open data base transaction to continue in
			@return the newly defined task entry
	*/
	DefineAndRunImmediateOneShotTask(
		ctx context.Context,
		params DefineTaskParams,
		activeDBClient db.Database,
	) (models.Task, error)

	/*
		DefineAndRunScheduledOneShotTask define and submit a scheduled one-shot class task.

		The task entry is defined in a database transaction, then submitted to the scheduler for
		execution. The submit happens after the entry is persisted and is not part of that
		transaction. On error, inspect the returned error with `errors.As`: a
		`models.PersistenceError` means the database operation failed and no task was created; a
		`models.IPCMessageQueueError` means the task was created but could not be submitted to the
		scheduler.

			@param ctx context.Context - execution context
			@param params DefineTaskParams - the task definition parameters
			@param targetRuntime time.Time - target time when the task should run
			@param activeDBClient db.Database - an existing open data base transaction to continue in
			@return the newly defined task entry
	*/
	DefineAndRunScheduledOneShotTask(
		ctx context.Context,
		params DefineTaskParams,
		targetRuntime time.Time,
		activeDBClient db.Database,
	) (models.Task, error)

	/*
		DefineImmediateOneShotTask define (but do NOT submit) an immediate one-shot class task.

		This is the define-only half of DefineAndRunImmediateOneShotTask: it persists the task
		entry and returns it, without poking the scheduler. The caller is responsible for calling
		SubmitTask afterwards to actually dispatch it. This split lets a caller commit its own
		additional state between the define and the submit - e.g. the workflow scheduler links the
		step to this task and marks the step RUNNING in the same transaction, then submits only
		after that commits (state-before-poke). On error the returned error `errors.As` a
		`models.PersistenceError`: the DB define failed and no task was created.

			@param ctx context.Context - execution context
			@param params DefineTaskParams - the task definition parameters
			@param activeDBClient db.Database - an existing open data base transaction to continue in
			@return the newly defined task entry
	*/
	DefineImmediateOneShotTask(
		ctx context.Context,
		params DefineTaskParams,
		activeDBClient db.Database,
	) (models.Task, error)

	/*
		DefineScheduledOneShotTask define (but do NOT submit) a scheduled one-shot class task.

		The define-only half of DefineAndRunScheduledOneShotTask; see DefineImmediateOneShotTask
		for the define/submit split rationale. On error the returned error `errors.As` a
		`models.PersistenceError`: the DB define failed and no task was created.

			@param ctx context.Context - execution context
			@param params DefineTaskParams - the task definition parameters
			@param targetRuntime time.Time - target time when the task should run
			@param activeDBClient db.Database - an existing open data base transaction to continue in
			@return the newly defined task entry
	*/
	DefineScheduledOneShotTask(
		ctx context.Context,
		params DefineTaskParams,
		targetRuntime time.Time,
		activeDBClient db.Database,
	) (models.Task, error)

	/*
		SubmitTask submit an already-defined task to the scheduler for execution.

		This is the submit-only half of the DefineAndRun* methods: it pokes the scheduler queue for
		a task whose entry has already been persisted (via DefineImmediateOneShotTask /
		DefineScheduledOneShotTask). It runs no database work. A failed submit `errors.As` a
		`models.IPCMessageQueueError`: the task entry still exists and the scheduler's own
		maintenance will eventually pick it up, so a caller following state-before-poke may treat a
		submit failure as a lost poke rather than a lost task.

			@param ctx context.Context - execution context
			@param taskID string - ID of the already-defined task to submit
	*/
	SubmitTask(ctx context.Context, taskID string) error

	/*
		CancelTask request scheduler cancel a task

		The task is read in a database transaction to confirm it exists, then a cancel request is
		submitted to the scheduler. The submit happens after the read and is not part of that
		transaction. On error, inspect the returned error with `errors.As`: a `models.PersistenceError`
		means the task could not be read; a `models.IPCMessageQueueError` means the cancel request could
		not be submitted to the scheduler.

			@param ctx context.Context - execution context
			@param taskID string - ID of task to cancel
			@param activeDBClient db.Database - an existing open data base transaction to continue in
	*/
	CancelTask(ctx context.Context, taskID string, activeDBClient db.Database) error
}

// clientImpl implements Client
type clientImpl struct {
	goutils.Component
	validator *validator.Validate

	config models.TaskClientConfig

	defaultCreator string

	retryForTaskName map[string]models.RetryParam

	workerCtx context.Context

	persistence db.Client

	schedulerIPCSender common.IPCMessageSend
	ipcSender          string
}

// NewClientParams init parameters for a task client
type NewClientParams struct {
	// Name of the client
	Name string `validate:"required"`
	// DefaultCreator opaque creator identity stamped on tasks submitted through this
	// client when the submit call does not provide a per-task override. tasking never
	// interprets it; it is the notification routing key (see notify/DESIGN.md).
	DefaultCreator string
	// Persistence persistence client
	Persistence db.Client `validate:"required"`
	// Config task client config
	Config models.TaskClientConfig `validate:"required"`
	// Redis REDIS client
	Redis goutilsRedis.Client `validate:"required"`
	// IPCSenderFactory factory function to define Redis based IPC message senders
	IPCSenderFactory IPCMsgSenderFactoryCB `validate:"required"`
}

/*
NewClient define new task client

	@param parentCtx context.Context - the parent execution context
	@param params NewClientParams - parameters of the new client
	@returns the new task client
*/
func NewClient(
	parentCtx context.Context, params NewClientParams,
) (Client, error) {
	logTags := log.Fields{
		"package": "tasking", "module": "task", "component": "client", "instance": params.Name,
	}

	validate := validator.New()
	if err := models.RegisterWithValidator(validate); err != nil {
		return nil, goutils.NewRuntimeError(
			"failed to install custom validation macros", err, true,
		)
	}
	if err := validate.Struct(&params); err != nil {
		return nil, goutils.NewBadInputError("client param is invalid", err, true)
	}

	instance := &clientImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		validator:        validate,
		config:           params.Config,
		defaultCreator:   params.DefaultCreator,
		retryForTaskName: make(map[string]models.RetryParam),
		persistence:      params.Persistence,
		workerCtx:        parentCtx,
	}

	for _, retrySetting := range params.Config.RetrySettings {
		instance.retryForTaskName[retrySetting.TaskName] = retrySetting.Retry
	}

	// ------------------------------------------------------------------------------------
	// Prepare IPC message queue handles

	// Define the scheduler queue sender
	{
		sender, err := params.IPCSenderFactory(
			instance.workerCtx, instance.config.SchedulerQueue, params.Redis, params.Name,
		)
		if err != nil {
			return nil, models.NewTaskClientError(
				fmt.Sprintf(
					"failed to initialize scheduler IPC queue '%s' sender", instance.config.SchedulerQueue,
				), err, true,
			)
		}
		instance.schedulerIPCSender = sender
		instance.ipcSender = params.Name
	}

	return instance, nil
}

/*
DefineAndRunImmediateOneShotTask define and submit an immediate one-shot class task.

The task entry is defined in a database transaction, then submitted to the scheduler for
execution. The submit happens after the entry is persisted and is not part of that
transaction. On error, inspect the returned error with `errors.As`:

  - a `models.PersistenceError` means the database operation failed and no task was created.

  - a `models.IPCMessageQueueError` means the task was created but could not be submitted to
    the scheduler.

    @param ctx context.Context - execution context
    @param params DefineTaskParams - the task definition parameters
    @param activeDBClient db.Database - an existing open data base transaction to continue in
    @return the newly defined task entry
*/
func (c *clientImpl) DefineAndRunImmediateOneShotTask(
	ctx context.Context,
	params DefineTaskParams,
	activeDBClient db.Database,
) (models.Task, error) {
	taskEntry, err := c.DefineImmediateOneShotTask(ctx, params, activeDBClient)
	if err != nil {
		return taskEntry, err
	}
	if err := c.SubmitTask(ctx, taskEntry.ID); err != nil {
		return taskEntry, err
	}
	return taskEntry, nil
}

/*
DefineImmediateOneShotTask define (but do NOT submit) an immediate one-shot class task.
See the Client interface for the define/submit split rationale.

	@param ctx context.Context - execution context
	@param params DefineTaskParams - the task definition parameters
	@param activeDBClient db.Database - an existing open data base transaction to continue in
	@return the newly defined task entry
*/
func (c *clientImpl) DefineImmediateOneShotTask(
	ctx context.Context,
	params DefineTaskParams,
	activeDBClient db.Database,
) (models.Task, error) {
	if err := c.validator.Struct(&params); err != nil {
		return models.Task{}, goutils.NewBadInputError("task definition param is invalid", err, true)
	}

	return c.defineOneShotTask(
		ctx, params.Name, params.Retry, activeDBClient,
		func(
			dbCtx context.Context, dbClient db.Database, retry models.TaskRetryParameters,
		) (models.Task, error) {
			return dbClient.DefineNewOneShotTask(dbCtx, db.NewTaskParameter{
				Name:       params.Name,
				Creator:    c.resolveCreator(params.Creator),
				Parameters: params.Parameters,
				Metadata:   params.Metadata,
				RetryParam: retry,
				Deadline:   params.Deadline,
			})
		},
	)
}

/*
DefineAndRunScheduledOneShotTask define and submit a scheduled one-shot class task.

The task entry is defined in a database transaction, then submitted to the scheduler for
execution. The submit happens after the entry is persisted and is not part of that
transaction. On error, inspect the returned error with `errors.As`:

  - a `models.PersistenceError` means the database operation failed and no task was created.

  - a `models.IPCMessageQueueError` means the task was created but could not be submitted to
    the scheduler.

    @param ctx context.Context - execution context
    @param params DefineTaskParams - the task definition parameters
    @param targetRuntime time.Time - target time when the task should run
    @param activeDBClient db.Database - an existing open data base transaction to continue in
    @return the newly defined task entry
*/
func (c *clientImpl) DefineAndRunScheduledOneShotTask(
	ctx context.Context,
	params DefineTaskParams,
	targetRuntime time.Time,
	activeDBClient db.Database,
) (models.Task, error) {
	taskEntry, err := c.DefineScheduledOneShotTask(ctx, params, targetRuntime, activeDBClient)
	if err != nil {
		return taskEntry, err
	}
	if err := c.SubmitTask(ctx, taskEntry.ID); err != nil {
		return taskEntry, err
	}
	return taskEntry, nil
}

/*
DefineScheduledOneShotTask define (but do NOT submit) a scheduled one-shot class task.
See the Client interface for the define/submit split rationale.

	@param ctx context.Context - execution context
	@param params DefineTaskParams - the task definition parameters
	@param targetRuntime time.Time - target time when the task should run
	@param activeDBClient db.Database - an existing open data base transaction to continue in
	@return the newly defined task entry
*/
func (c *clientImpl) DefineScheduledOneShotTask(
	ctx context.Context,
	params DefineTaskParams,
	targetRuntime time.Time,
	activeDBClient db.Database,
) (models.Task, error) {
	if err := c.validator.Struct(&params); err != nil {
		return models.Task{}, goutils.NewBadInputError("task definition param is invalid", err, true)
	}

	if params.Deadline != nil && targetRuntime.After(*params.Deadline) {
		return models.Task{}, goutils.NewBadInputError(
			"task deadline must come after target runtime", nil, true,
		)
	}

	return c.defineOneShotTask(
		ctx, params.Name, params.Retry, activeDBClient,
		func(
			dbCtx context.Context, dbClient db.Database, retry models.TaskRetryParameters,
		) (models.Task, error) {
			return dbClient.DefineNewScheduledOneShotTask(dbCtx, db.NewTaskParameter{
				Name:       params.Name,
				Creator:    c.resolveCreator(params.Creator),
				Parameters: params.Parameters,
				Metadata:   params.Metadata,
				RetryParam: retry,
				Deadline:   params.Deadline,
			}, targetRuntime)
		},
	)
}

// resolveCreator returns the effective creator for a submit: the per-task override when
// provided, otherwise the client's DefaultCreator. An empty result is left as-is and is
// rejected downstream by the Task entry's `validate:"required"` on Creator.
func (c *clientImpl) resolveCreator(override *string) string {
	if override != nil {
		return *override
	}
	return c.defaultCreator
}

/*
defineOneShotTask defines a one-shot task entry, WITHOUT notifying the scheduler.

The `define` callback runs inside the (possibly caller-supplied) database transaction and is
responsible for the one differing DB call between the immediate and scheduled variants. The
resolved retry passed to it is: the per-submission `overrideRetry` when non-nil, else the
client's by-name policy for `name`, else the default. Submitting the defined task to the
scheduler is a separate step (SubmitTask), so a caller can commit its own additional state
between the define and the submit. Error contract for the returned (wrapped) error:

  - `errors.As` -> `models.PersistenceError`: the DB define failed; no task row was created.

    @param ctx context.Context - execution context
    @param name string - the task name, used for retry lookup and error messages
    @param overrideRetry *models.RetryParam - optional per-submission retry override; nil uses
    the by-name/default policy
    @param activeDBClient db.Database - an existing open data base transaction to continue in
    @param define - callback performing the variant-specific task definition
    @return the newly defined task entry
*/
func (c *clientImpl) defineOneShotTask(
	ctx context.Context,
	name string,
	overrideRetry *models.TaskRetryParameters,
	activeDBClient db.Database,
	define func(
		dbCtx context.Context, dbClient db.Database, retry models.TaskRetryParameters,
	) (models.Task, error),
) (models.Task, error) {
	var taskEntry models.Task
	if dbErr := db.ActiveSessionWrapper(
		ctx, activeDBClient, c.persistence, func(dbCtx context.Context, dbClient db.Database) error {
			// Define the task entry with the resolved retry policy.
			var err error
			taskEntry, err = define(dbCtx, dbClient, c.resolveRetry(name, overrideRetry))
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to define new one-shot task entry for '%s'", name), err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.Task{}, models.NewTaskClientError(
			"failed to define one-shot '"+name+"' task", dbErr, true,
		)
	}

	return taskEntry, nil
}

// resolveRetry resolves the retry policy for a submission. A per-submission override wins (used
// verbatim, Factor included); else the client's by-task-name policy is layered onto the default
// (matching the historical behavior, which only carries InitialDelaySec/MaxDelaySec/MaxRetries);
// else the default is used.
func (c *clientImpl) resolveRetry(
	name string, override *models.TaskRetryParameters,
) models.TaskRetryParameters {
	if override != nil {
		return *override
	}
	retry := models.DefaultTaskRetryParameters()
	if byName, ok := c.retryForTaskName[name]; ok {
		retry.InitialDelaySec = byName.InitialDelaySec
		retry.MaxDelaySec = byName.MaxDelaySec
		retry.MaxRetries = byName.MaxRetries
	}
	return retry
}

/*
SubmitTask submit an already-defined task to the scheduler for execution. See the Client
interface for the define/submit split rationale.

The send is not part of any database transaction, so a failure here leaves the (already
created) task entry in place; the returned error `errors.As` a `models.IPCMessageQueueError`.

	@param ctx context.Context - execution context
	@param taskID string - ID of the already-defined task to submit
*/
func (c *clientImpl) SubmitTask(ctx context.Context, taskID string) error {
	if sendErr := c.schedulerIPCSender.EnqueueMessage(
		ctx, models.PrepareIPCMsgNewPendingTask(c.ipcSender, taskID, time.Now().UTC()),
	); sendErr != nil {
		return models.NewTaskClientError(
			"task "+taskID+" created but failed to submit to the scheduler", sendErr, true,
		)
	}
	return nil
}

/*
CancelTask request scheduler cancel a task

The task is read in a database transaction to confirm it exists, then a cancel request is
submitted to the scheduler. The submit happens after the read and is not part of that
transaction. On error, inspect the returned error with `errors.As`: a `models.PersistenceError`
means the task could not be read; a `models.IPCMessageQueueError` means the cancel request could
not be submitted to the scheduler.

	@param ctx context.Context - execution context
	@param taskID string - ID of task to cancel
	@param activeDBClient db.Database - an existing open data base transaction to continue in
*/
func (c *clientImpl) CancelTask(
	ctx context.Context, taskID string, activeDBClient db.Database,
) error {
	// Confirm the task exists before asking the scheduler to cancel it.
	if dbErr := db.ActiveSessionWrapper(
		ctx, activeDBClient, c.persistence, func(dbCtx context.Context, dbClient db.Database) error {
			if _, err := dbClient.GetTask(dbCtx, taskID); err != nil {
				return models.NewPersistenceError("failed to read task "+taskID, err, true)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskClientError(
			"failed to read task "+taskID+" to cancel", dbErr, true,
		)
	}

	// Notify the scheduler AFTER confirming the task exists. This send is not part of the
	// database transaction above; the returned error carries the IPCMessageQueueError as its Core.
	if sendErr := c.schedulerIPCSender.EnqueueMessage(
		ctx, models.PrepareIPCMsgCancelTask(c.ipcSender, taskID, time.Now().UTC()),
	); sendErr != nil {
		return models.NewTaskClientError(
			"failed to submit cancel request for task "+taskID+" to the scheduler", sendErr, true,
		)
	}
	return nil
}
