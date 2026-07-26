package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/common"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// DefineWorkflowParams the per-workflow parameters for the workflow definition entry points. It
// carries the workflow spec and an optional creator override.
type DefineWorkflowParams struct {
	// Spec the workflow specification (name, deadline, metadata, and the DAG of steps).
	Spec models.NewWorkflowParameter `validate:"required"`
	// Creator optional per-workflow creator override; nil uses the client's DefaultCreator. The
	// resolved value must be non-empty or the define fails validation downstream (the Workflow
	// entry's Creator is `validate:"required"`).
	Creator *string
}

// Client workflow engine client - the user-facing submission / user-mutation API. Every operation
// is "write DB rows (definition only), then poke the scheduler": the Client never performs a
// workflow state transition (the scheduler is the single writer of live state), it only writes the
// initial DEFINED/PENDING definition rows and enqueues scheduler events. See workflow/DESIGN.md
// "Workflow Client". Mirrors the task engine's task.Client, including the
// define / submit / define-and-run split.
type Client interface {
	/*
		DefineWorkflow define (but do NOT start) a workflow.

		The workflow row (born PENDING) and its step rows (born DEFINED) are written in a database
		transaction; the scheduler is NOT poked. The caller is responsible for calling
		SubmitWorkflow afterwards to actually start it. This split lets a caller commit its own
		additional state between the define and the submit (state-before-poke). Before any DB write
		the workflow spec is validated as a DAG and every step Type is checked against the client's
		registered step-type set. On error, inspect the returned error with `errors.As`: a
		`models.PersistenceError` means the database operation failed and no workflow was created; a
		`goutils.ValidationError`/`BadInputError` means the spec (or an unknown step Type) was
		rejected before any DB work.

			@param ctx context.Context - execution context
			@param params DefineWorkflowParams - the workflow definition parameters
			@param activeDBClient db.Database - an existing open database transaction to continue in
			@return the newly defined workflow entry
	*/
	DefineWorkflow(
		ctx context.Context, params DefineWorkflowParams, activeDBClient db.Database,
	) (models.Workflow, error)

	/*
		SubmitWorkflow start an already-defined workflow by poking the scheduler.

		This is the submit-only half of DefineAndRunWorkflow: it emits a Process Workflow event for
		a workflow whose rows have already been persisted (via DefineWorkflow). It runs no database
		work. A failed submit `errors.As` a `models.IPCMessageQueueError`: the workflow rows still
		exist and the scheduler's maintenance sweep will eventually drive the still-PENDING workflow,
		so a caller following state-before-poke may treat a submit failure as a lost poke rather than
		a lost workflow.

			@param ctx context.Context - execution context
			@param workflowID string - ID of the already-defined workflow to start
	*/
	SubmitWorkflow(ctx context.Context, workflowID string) error

	/*
		DefineAndRunWorkflow define and start a workflow.

		The combined convenience wrapper: it calls DefineWorkflow, then SubmitWorkflow. The submit
		happens after the rows are persisted and is not part of that transaction. On error, inspect
		the returned error with `errors.As`: a `models.PersistenceError` means the database operation
		failed and no workflow was created; a `models.IPCMessageQueueError` means the workflow was
		created but could not be submitted to the scheduler (the returned entry is still valid).

			@param ctx context.Context - execution context
			@param params DefineWorkflowParams - the workflow definition parameters
			@param activeDBClient db.Database - an existing open database transaction to continue in
			@return the newly defined workflow entry
	*/
	DefineAndRunWorkflow(
		ctx context.Context, params DefineWorkflowParams, activeDBClient db.Database,
	) (models.Workflow, error)

	/*
		ReviveWorkflow request the scheduler revive a FAILED / TIMED_OUT workflow.

		The workflow is read in a database transaction to confirm it exists, then a Revive Failed
		Workflow event is emitted. The client does NOT check the workflow state or the newDeadline
		rule ("required iff TIMED_OUT") - the scheduler's revive handler owns those against live
		state, preserving the single-writer invariant. On error, inspect the returned error with
		`errors.As`: a `models.PersistenceError` means the workflow could not be read; a
		`models.IPCMessageQueueError` means the revive request could not be submitted to the
		scheduler.

			@param ctx context.Context - execution context
			@param workflowID string - ID of the workflow to revive
			@param newDeadline *time.Time - optional new workflow deadline (required by the scheduler
			for a TIMED_OUT workflow)
			@param activeDBClient db.Database - an existing open database transaction to continue in
	*/
	ReviveWorkflow(
		ctx context.Context, workflowID string, newDeadline *time.Time, activeDBClient db.Database,
	) error

	/*
		CancelWorkflow request the scheduler cancel a workflow.

		The workflow is read in a database transaction to confirm it exists, then a Cancel Workflow
		event is emitted. On error, inspect the returned error with `errors.As`: a
		`models.PersistenceError` means the workflow could not be read; a
		`models.IPCMessageQueueError` means the cancel request could not be submitted to the
		scheduler.

			@param ctx context.Context - execution context
			@param workflowID string - ID of the workflow to cancel
			@param activeDBClient db.Database - an existing open database transaction to continue in
	*/
	CancelWorkflow(ctx context.Context, workflowID string, activeDBClient db.Database) error
}

// clientImpl implements Client
type clientImpl struct {
	goutils.Component
	validator *validator.Validate

	name string

	defaultCreator string

	// knownStepTypes the set of step Types with a registered handler, used for the up-front Define
	// validation. An owned snapshot copied at construction; never mutated.
	knownStepTypes map[string]bool

	workerCtx context.Context

	persistence db.Client

	schedulerIPCSender common.IPCMessageSend
}

// NewClientParams init parameters for a workflow client
type NewClientParams struct {
	// Name of the client - also the IPC sender identity stamped on scheduler pokes.
	Name string `validate:"required"`
	// DefaultCreator opaque creator identity stamped on workflows submitted through this client when
	// the define call does not provide a per-workflow override. tasking never interprets it; it is
	// the routing key for the workflow's audit events and notifications.
	DefaultCreator string
	// Persistence persistence client
	Persistence db.Client `validate:"required"`
	// Config workflow client config
	Config models.WorkflowClientConfig `validate:"required"`
	// Redis REDIS client
	Redis goutilsRedis.Client `validate:"required"`
	// IPCSenderFactory factory function to define Redis based IPC message senders
	IPCSenderFactory task.IPCMsgSenderFactoryCB `validate:"required"`
	// KnownStepTypes the set of step Types with a registered handler (the same registration the Step
	// Runner holds). DefineWorkflow rejects, up front, a workflow containing a step whose Type is not
	// in this set - a fail-fast validation far friendlier than a mid-run failure. Must be non-empty.
	KnownStepTypes map[string]bool `validate:"required,gt=0"`
}

/*
NewClient define new workflow client

	@param parentCtx context.Context - the parent execution context
	@param params NewClientParams - parameters of the new client
	@returns the new workflow client
*/
func NewClient(
	parentCtx context.Context, params NewClientParams,
) (Client, error) {
	logTags := log.Fields{
		"package": "tasking", "module": "workflow", "component": "client", "instance": params.Name,
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

	// Copy the known step-type set so the client owns an immutable snapshot.
	ownedStepTypes := make(map[string]bool, len(params.KnownStepTypes))
	for stepType := range params.KnownStepTypes {
		ownedStepTypes[stepType] = true
	}

	instance := &clientImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		validator:      validate,
		name:           params.Name,
		defaultCreator: params.DefaultCreator,
		knownStepTypes: ownedStepTypes,
		persistence:    params.Persistence,
		workerCtx:      parentCtx,
	}

	// ------------------------------------------------------------------------------------
	// Prepare the workflow scheduler queue sender - the client's one outbound channel, onto which it
	// pokes Process / Revive / Cancel Workflow events (the same queue the scheduler receives on).
	sender, err := params.IPCSenderFactory(
		instance.workerCtx, params.Config.SchedulerQueue, params.Redis, params.Name,
	)
	if err != nil {
		return nil, models.NewWorkflowClientError(
			fmt.Sprintf(
				"failed to initialize scheduler IPC queue '%s' sender", params.Config.SchedulerQueue,
			), err, true,
		)
	}
	instance.schedulerIPCSender = sender

	return instance, nil
}

/*
DefineWorkflow define (but do NOT start) a workflow. See the Client interface for the
define/submit split rationale.

	@param ctx context.Context - execution context
	@param params DefineWorkflowParams - the workflow definition parameters
	@param activeDBClient db.Database - an existing open database transaction to continue in
	@return the newly defined workflow entry
*/
func (c *clientImpl) DefineWorkflow(
	ctx context.Context, params DefineWorkflowParams, activeDBClient db.Database,
) (models.Workflow, error) {
	if err := c.validator.Struct(&params); err != nil {
		return models.Workflow{}, goutils.NewBadInputError(
			"workflow definition param is invalid", err, true,
		)
	}

	// Validate the spec (DAG / self-dep / unknown-parent) before touching the DB - a clean rejection
	// ahead of the DB write, matching the task client.
	if err := params.Spec.IsValid(c.validator); err != nil {
		return models.Workflow{}, err
	}

	// Up-front step-Type check: reject any step whose Type has no registered handler before writing
	// any rows. The runtime MissingHandler guard in the Step Runner remains the authoritative
	// backstop.
	for stepName, step := range params.Spec.Steps {
		if !c.knownStepTypes[step.Type] {
			return models.Workflow{}, goutils.NewBadInputError(
				fmt.Sprintf(
					"workflow step '%s' has unregistered type '%s'", stepName, step.Type,
				), nil, true,
			)
		}
	}

	var workflowEntry models.Workflow
	if dbErr := db.ActiveSessionWrapper(
		ctx, activeDBClient, c.persistence, func(dbCtx context.Context, dbClient db.Database) error {
			var err error
			workflowEntry, err = dbClient.DefineNewWorkflow(
				dbCtx, params.Spec, c.resolveCreator(params.Creator),
			)
			if err != nil {
				return models.NewPersistenceError(
					fmt.Sprintf("failed to define new workflow '%s'", params.Spec.Name), err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.Workflow{}, models.NewWorkflowClientError(
			"failed to define workflow '"+params.Spec.Name+"'", dbErr, true,
		)
	}

	return workflowEntry, nil
}

/*
SubmitWorkflow start an already-defined workflow by poking the scheduler. See the Client
interface for the define/submit split rationale.

The send is not part of any database transaction, so a failure here leaves the (already created)
workflow rows in place; the returned error `errors.As` a `models.IPCMessageQueueError`.

	@param ctx context.Context - execution context
	@param workflowID string - ID of the already-defined workflow to start
*/
func (c *clientImpl) SubmitWorkflow(ctx context.Context, workflowID string) error {
	if sendErr := c.schedulerIPCSender.EnqueueMessage(
		ctx, models.PrepareIPCMsgWFProcessWorkflow(c.name, workflowID, time.Now().UTC()),
	); sendErr != nil {
		return models.NewWorkflowClientError(
			"workflow "+workflowID+" created but failed to submit to the scheduler", sendErr, true,
		)
	}
	return nil
}

/*
DefineAndRunWorkflow define and start a workflow. The combined convenience wrapper over
DefineWorkflow + SubmitWorkflow; see the Client interface for the error contract.

	@param ctx context.Context - execution context
	@param params DefineWorkflowParams - the workflow definition parameters
	@param activeDBClient db.Database - an existing open database transaction to continue in
	@return the newly defined workflow entry
*/
func (c *clientImpl) DefineAndRunWorkflow(
	ctx context.Context, params DefineWorkflowParams, activeDBClient db.Database,
) (models.Workflow, error) {
	workflowEntry, err := c.DefineWorkflow(ctx, params, activeDBClient)
	if err != nil {
		return workflowEntry, err
	}
	if err := c.SubmitWorkflow(ctx, workflowEntry.ID); err != nil {
		return workflowEntry, err
	}
	return workflowEntry, nil
}

/*
ReviveWorkflow request the scheduler revive a FAILED / TIMED_OUT workflow. See the Client
interface for the state/deadline-precondition division of labour with the scheduler.

	@param ctx context.Context - execution context
	@param workflowID string - ID of the workflow to revive
	@param newDeadline *time.Time - optional new workflow deadline
	@param activeDBClient db.Database - an existing open database transaction to continue in
*/
func (c *clientImpl) ReviveWorkflow(
	ctx context.Context, workflowID string, newDeadline *time.Time, activeDBClient db.Database,
) error {
	// Confirm the workflow exists before asking the scheduler to revive it.
	if dbErr := db.ActiveSessionWrapper(
		ctx, activeDBClient, c.persistence, func(dbCtx context.Context, dbClient db.Database) error {
			if _, err := dbClient.GetWorkflow(dbCtx, workflowID); err != nil {
				return models.NewPersistenceError("failed to read workflow "+workflowID, err, true)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewWorkflowClientError(
			"failed to read workflow "+workflowID+" to revive", dbErr, true,
		)
	}

	// Notify the scheduler AFTER confirming the workflow exists. Not part of the transaction above;
	// the returned error carries the IPCMessageQueueError as its Core.
	if sendErr := c.schedulerIPCSender.EnqueueMessage(
		ctx, models.PrepareIPCMsgWFReviveWorkflow(c.name, workflowID, newDeadline, time.Now().UTC()),
	); sendErr != nil {
		return models.NewWorkflowClientError(
			"failed to submit revive request for workflow "+workflowID+" to the scheduler", sendErr, true,
		)
	}
	return nil
}

/*
CancelWorkflow request the scheduler cancel a workflow.

	@param ctx context.Context - execution context
	@param workflowID string - ID of the workflow to cancel
	@param activeDBClient db.Database - an existing open database transaction to continue in
*/
func (c *clientImpl) CancelWorkflow(
	ctx context.Context, workflowID string, activeDBClient db.Database,
) error {
	// Confirm the workflow exists before asking the scheduler to cancel it.
	if dbErr := db.ActiveSessionWrapper(
		ctx, activeDBClient, c.persistence, func(dbCtx context.Context, dbClient db.Database) error {
			if _, err := dbClient.GetWorkflow(dbCtx, workflowID); err != nil {
				return models.NewPersistenceError("failed to read workflow "+workflowID, err, true)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewWorkflowClientError(
			"failed to read workflow "+workflowID+" to cancel", dbErr, true,
		)
	}

	// Notify the scheduler AFTER confirming the workflow exists.
	if sendErr := c.schedulerIPCSender.EnqueueMessage(
		ctx, models.PrepareIPCMsgWFCancelWorkflow(c.name, workflowID, time.Now().UTC()),
	); sendErr != nil {
		return models.NewWorkflowClientError(
			"failed to submit cancel request for workflow "+workflowID+" to the scheduler", sendErr, true,
		)
	}
	return nil
}

// resolveCreator returns the effective creator for a define: the per-workflow override when
// provided, otherwise the client's DefaultCreator. An empty result is left as-is and is rejected
// downstream by the Workflow entry's `validate:"required"` on Creator.
func (c *clientImpl) resolveCreator(override *string) string {
	if override != nil {
		return *override
	}
	return c.defaultCreator
}
