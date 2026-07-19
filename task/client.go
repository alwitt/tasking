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
)

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
			@param name string - the task name. This is used to match the task with the appropriate
			    task execution processor to run the task.
			@param parameters any - task processing parameters
			@param metadata any - associated metadata
			@param deadline *time.Time - if specified, the task must complete by this dead line.
			@param activeDBClient db.Database - an existing open data base transaction to continue in
			@return the newly defined task entry
	*/
	DefineAndRunImmediateOneShotTask(
		ctx context.Context,
		name string,
		parameters any,
		metadata any,
		deadline *time.Time,
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
			@param name string - the task name. This is used to match the task with the appropriate
			    task execution processor to run the task.
			@param parameters any - task processing parameters
			@param metadata any - associated metadata
			@param targetRuntime time.Time - target time when the task should run
			@param deadline *time.Time - if specified, the task must complete by this dead line.
			@param activeDBClient db.Database - an existing open data base transaction to continue in
			@return the newly defined task entry
	*/
	DefineAndRunScheduledOneShotTask(
		ctx context.Context,
		name string,
		parameters any,
		metadata any,
		targetRuntime time.Time,
		deadline *time.Time,
		activeDBClient db.Database,
	) (models.Task, error)

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

	config models.TaskClientConfig

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

	instance := &clientImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		config:           params.Config,
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
    @param name string - the task name. This is used to match the task with the appropriate
    task execution processor to run the task.
    @param parameters any - task processing parameters
    @param metadata any - associated metadata
    @param deadline *time.Time - if specified, the task must complete by this dead line.
    @param activeDBClient db.Database - an existing open data base transaction to continue in
    @return the newly defined task entry
*/
func (c *clientImpl) DefineAndRunImmediateOneShotTask(
	ctx context.Context,
	name string,
	parameters any,
	metadata any,
	deadline *time.Time,
	activeDBClient db.Database,
) (models.Task, error) {
	return c.defineAndRunOneShotTask(
		ctx, name, activeDBClient,
		func(
			dbCtx context.Context, dbClient db.Database, retry models.TaskRetryParameters,
		) (models.Task, error) {
			return dbClient.DefineNewOneShotTask(dbCtx, db.NewTaskParameter{
				Name:       name,
				Parameters: parameters,
				Metadata:   metadata,
				RetryParam: retry,
				Deadline:   deadline,
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
    @param name string - the task name. This is used to match the task with the appropriate
    task execution processor to run the task.
    @param parameters any - task processing parameters
    @param metadata any - associated metadata
    @param targetRuntime time.Time - target time when the task should run
    @param deadline *time.Time - if specified, the task must complete by this dead line.
    @param activeDBClient db.Database - an existing open data base transaction to continue in
    @return the newly defined task entry
*/
func (c *clientImpl) DefineAndRunScheduledOneShotTask(
	ctx context.Context,
	name string,
	parameters any,
	metadata any,
	targetRuntime time.Time,
	deadline *time.Time,
	activeDBClient db.Database,
) (models.Task, error) {
	if deadline != nil && targetRuntime.After(*deadline) {
		return models.Task{}, goutils.NewBadInputError(
			"task deadline must come after target runtime", nil, true,
		)
	}

	return c.defineAndRunOneShotTask(
		ctx, name, activeDBClient,
		func(
			dbCtx context.Context, dbClient db.Database, retry models.TaskRetryParameters,
		) (models.Task, error) {
			return dbClient.DefineNewScheduledOneShotTask(dbCtx, db.NewTaskParameter{
				Name:       name,
				Parameters: parameters,
				Metadata:   metadata,
				RetryParam: retry,
				Deadline:   deadline,
			}, targetRuntime)
		},
	)
}

/*
defineAndRunOneShotTask defines a one-shot task entry then notifies the scheduler.

The `define` callback runs inside the (possibly caller-supplied) database transaction and is
responsible for the one differing DB call between the immediate and scheduled variants. The
scheduler IPC send runs AFTER that transaction work returns successfully, so it is NOT rolled
back with the row. Error contract for the returned (wrapped) error:

  - `errors.As` -> `models.PersistenceError`: the DB define failed; no task row was created.

  - `errors.As` -> `models.IPCMessageQueueError`: the row was created, but notifying the
    scheduler failed.

    @param ctx context.Context - execution context
    @param name string - the task name, used for retry lookup and error messages
    @param activeDBClient db.Database - an existing open data base transaction to continue in
    @param define - callback performing the variant-specific task definition
    @return the newly defined task entry
*/
func (c *clientImpl) defineAndRunOneShotTask(
	ctx context.Context,
	name string,
	activeDBClient db.Database,
	define func(
		dbCtx context.Context, dbClient db.Database, retry models.TaskRetryParameters,
	) (models.Task, error),
) (models.Task, error) {
	var taskEntry models.Task
	if dbErr := db.ActiveSessionWrapper(
		ctx, activeDBClient, c.persistence, func(dbCtx context.Context, dbClient db.Database) error {
			retry := models.DefaultTaskRetryParameters()
			if customRetry, ok := c.retryForTaskName[name]; ok {
				retry.InitialDelaySec = customRetry.InitialDelaySec
				retry.MaxDelaySec = customRetry.MaxDelaySec
				retry.MaxRetries = customRetry.MaxRetries
			}

			// Define the task entry
			var err error
			taskEntry, err = define(dbCtx, dbClient, retry)
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

	// Notify the scheduler AFTER the task entry is persisted. This send is not part of the
	// database transaction above, so a failure here leaves the (already created) task entry in
	// place; the returned error carries the IPCMessageQueueError as its Core.
	if sendErr := c.schedulerIPCSender.EnqueueMessage(
		ctx, models.PrepareIPCMsgNewPendingTask(c.ipcSender, taskEntry.ID, time.Now().UTC()),
	); sendErr != nil {
		return taskEntry, models.NewTaskClientError(
			"one-shot '"+name+"' task created but failed to submit to the scheduler", sendErr, true,
		)
	}

	return taskEntry, nil
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
