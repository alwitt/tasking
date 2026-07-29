package workflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

// isFatalDBError report whether the error chain contains a models.SQLError, which indicates the
// database or the connection to it has failed. Such errors are fatal for the scheduler and must
// stop the consumer rather than be recovered per-request. Mirrors the task package's helper of the
// same name (errors.As sees the SQLError through the wrapped scheduler/persistence error chain).
func isFatalDBError(err error) bool {
	var sqlErr goutils.SQLError
	return errors.As(err, &sqlErr)
}

// processQueue process the messages on the workflow scheduler queue
func (s *schedulerImpl) processQueue() {
	logTags := s.GetLogTagsForContext(s.runCtx)

	log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Info("Starting workflow scheduler queue message processing")
	defer log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Info("Stopped workflow scheduler queue message processing")

	for {
		// verify whether to stop
		if err := s.runCtx.Err(); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf("Stopping queue processing due to receiver context error:\n%+v", err)
			}
			break
		}

		if err := s.processOneIPCRequest(s.runCtx); err != nil {
			// The message has already been dealt with (a completed handler always deletes its
			// message; only a crash strands one for replay). The error only decides whether to
			// stop: a broken database (models.SQLError) is fatal - every message would fail
			// identically, so continuing is meaningless. Any other failure is logged and
			// processing continues; the maintenance sweep re-drives the stranded work from the
			// DB (the source of truth), so a single failing message never wedges the scheduler.
			if isFatalDBError(err) {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf("Encountered fatal error while processing IPC messages:\n%+v", err)
				// Hand the fault to the parent application and stop this thread; the DB is broken,
				// so every subsequent message would fail identically.
				s.reportFatal(s.ipcName, err, time.Now())
				return
			}
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf("Failed to process IPC message (continuing):\n%+v", err)
		}
	}
}

/*
recordInvalidMessage record an audit event for a poison IPC message (one that can never be
processed). The audit write is best-effort: if it fails, the failure is logged but not
propagated, so an un-auditable poison message cannot become an infinite crash/replay loop.

	@param ctx context.Context - execution context
	@param rawPayload string - the raw message payload, if it was readable ("" otherwise)
	@param reason string - human-readable reason the message was rejected
*/
func (s *schedulerImpl) recordInvalidMessage(ctx context.Context, rawPayload, reason string) {
	logTags := s.GetLogTagsForContext(ctx)

	log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Errorf("Discarding invalid IPC message (%s): %s", reason, rawPayload)

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			return dbClient.RecordInvalidTaskIPCMessage(dbCtx, s.ipcName, rawPayload, reason)
		},
	); dbErr != nil {
		log.
			WithError(dbErr).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Errorf("Failed to record audit event for invalid IPC message (%s)", reason)
	}
}

/*
recordAndDropInvalidMessage record an audit event for a poison IPC message (via
recordInvalidMessage) and then remove it from the queue buffer so it is not replayed. Used
on the live processing path, where the message is still staged in the buffer. The buffer
delete is best-effort (logged on failure) for the same reason the audit write is.

	@param ctx context.Context - execution context
	@param msg goutilsRedis.QueueMessageEnvelope - the buffered message to drop
	@param rawPayload string - the raw message payload, if it was readable ("" otherwise)
	@param reason string - human-readable reason the message was rejected
*/
func (s *schedulerImpl) recordAndDropInvalidMessage(
	ctx context.Context, msg goutilsRedis.QueueMessageEnvelope, rawPayload, reason string,
) {
	logTags := s.GetLogTagsForContext(ctx)

	s.recordInvalidMessage(ctx, rawPayload, reason)

	if err := s.ipcReceiver.DeleteBufferedMessage(ctx, msg); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to delete invalid IPC message from queue buffer")
	}
}

/*
recoverBufferedMessages drain and replay any messages left in the scheduler queue buffer
from a previous run.

The scheduler queue is a reliable queue: a message stays in the buffer until we are done
with it. If the scheduler crashed (or was killed on a fatal error) after a message was
dequeued but before it was deleted, that message is stranded in the buffer. This must run at
startup, before processQueue begins consuming the main queue, so those messages are replayed
rather than lost or leaked.

For each buffered message:

  - A poison message (unreadable, unparsable, or unsupported type) is recorded as an audit
    event and discarded. DequeueBufferedMessage already popped it off the buffer, so there is
    nothing more to delete.

  - A valid message is re-enqueued onto the main queue (ReEnqueueOnMainQueue also removes it
    from the buffer) so it is processed normally by processQueue once that starts. This keeps
    a single processing path - recovered messages get the same handling as fresh ones. The
    self-generated maintenance message is transient but idempotent, so replaying it is harmless.

    @param ctx context.Context - execution context
*/
func (s *schedulerImpl) recoverBufferedMessages(ctx context.Context) error {
	logTags := s.GetLogTagsForContext(ctx)

	for {
		msg, err := s.ipcReceiver.DequeueBufferedMessage(ctx, true, nil)
		if err != nil {
			// FATAL
			return models.NewWorkflowSchedulerError("failed to read queue buffer", err, true)
		}

		// Buffer drained
		if msg == nil {
			break
		}

		// Note: DequeueBufferedMessage already popped this message off the buffer, so a poison
		// message here just needs to be recorded and discarded (nothing left to delete).
		payload, err := msg.StringPayload()
		if err != nil {
			s.recordInvalidMessage(ctx, "", "unreadable payload")
			continue
		}

		parsed, err := models.ParseIPCMessage(s.validator, []byte(payload))
		if err != nil {
			s.recordInvalidMessage(ctx, payload, "unparsable message")
			continue
		}

		// Only messages the workflow scheduler queue actually handles should be replayed.
		if !s.isSupportedWorkflowMessage(ctx, payload, parsed) {
			continue
		}

		if err := s.ipcReceiver.ReEnqueueOnMainQueue(ctx, msg); err != nil {
			// FATAL
			return models.NewWorkflowSchedulerError(
				"failed to re-enqueue buffered message onto main queue", err, true,
			)
		}
		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Infof("Recovered buffered IPC message '%s' onto main queue", payload)
	}

	return nil
}

/*
isSupportedWorkflowMessage report whether a parsed IPC message is one the workflow scheduler
queue handles. A message of any other shape/type on this queue is genuinely poison, so it is
recorded (best-effort) and rejected. Shared by recoverBufferedMessages (replay filter) so the
replay path and the live path agree on what is a valid workflow message.

	@param ctx context.Context - execution context
	@param payload string - the raw payload, for the audit record
	@param parsed interface{} - the parsed message
	@return whether the message is a supported workflow scheduler message
*/
func (s *schedulerImpl) isSupportedWorkflowMessage(
	ctx context.Context, payload string, parsed interface{},
) bool {
	switch typed := parsed.(type) {
	case models.IPCMessageWorkflow:
		switch typed.Type {
		case models.IPCMsgTypeWFProcessWorkflow, models.IPCMsgTypeWFCancelWorkflow:
			return true
		default:
			s.recordInvalidMessage(
				ctx, payload,
				fmt.Sprintf("unsupported workflow message type '%s'", typed.Type),
			)
			return false
		}
	case models.IPCMessageWorkflowStep:
		if typed.Type == models.IPCMsgTypeWFScheduleStep {
			return true
		}
		s.recordInvalidMessage(
			ctx, payload,
			fmt.Sprintf("unsupported workflow-step message type '%s'", typed.Type),
		)
		return false
	case models.IPCMessageWorkflowStepExecUpdate:
		if typed.Type == models.IPCMsgTypeWFStepExecUpdate {
			return true
		}
		s.recordInvalidMessage(
			ctx, payload,
			fmt.Sprintf("unsupported workflow-step-exec-update message type '%s'", typed.Type),
		)
		return false
	case models.IPCMessageWorkflowStepTaskUpdate:
		if typed.Type == models.IPCMsgTypeWFStepTaskUpdate {
			return true
		}
		s.recordInvalidMessage(
			ctx, payload,
			fmt.Sprintf("unsupported workflow-step-task-update message type '%s'", typed.Type),
		)
		return false
	case models.IPCMessageWorkflowRevive:
		if typed.Type == models.IPCMsgTypeWFReviveWorkflow {
			return true
		}
		s.recordInvalidMessage(
			ctx, payload,
			fmt.Sprintf("unsupported workflow-revive message type '%s'", typed.Type),
		)
		return false
	case models.BaseIPCMessage:
		if typed.Type == models.IPCMsgTypeWFMaintenance {
			return true
		}
		s.recordInvalidMessage(
			ctx, payload, fmt.Sprintf("unsupported base message type '%s'", typed.Type),
		)
		return false
	default:
		s.recordInvalidMessage(
			ctx, payload, fmt.Sprintf("unsupported message type '%s'", reflect.TypeOf(typed)),
		)
		return false
	}
}

/*
processOneIPCRequest process one IPC request on the workflow scheduler queue.

The scheduler queue is a reliable queue: DequeueMessage moves the message into a buffer queue,
and the message is only removed from that buffer once we are done with it. The buffer guards
against exactly one thing - an application crash/panic mid-handling, where the handler never
runs to completion. So the delete contract is keyed off *completion*, not success:

  - Handler ran to completion (returned nil OR an error) -> delete from the buffer. We are done
    with the message; only a crash (never a returned error) should strand one for replay.
  - Poison message (unreadable, unparsable, or unsupported type) -> record an audit event and
    delete from buffer; it can never be processed, so replaying it would loop forever.

Unlike the task scheduler this is the whole engine's only consumer, but the handling is the
same: the handler runs INLINE on this goroutine (there is no worker to Submit to).

A handler error is returned to the caller (processQueue) *after* the delete. The error class
does NOT affect the delete - it only lets the caller decide whether to stop (a broken DB) or
log and continue (the maintenance sweep will re-drive the work from the DB). A DequeueMessage
read error is an engine-level fault with no message to delete, so it is returned directly.

	@param ctx context.Context - execution context
*/
func (s *schedulerImpl) processOneIPCRequest(ctx context.Context) error {
	logTags := s.GetLogTagsForContext(ctx)
	msg, err := s.ipcReceiver.DequeueMessage(ctx, true, nil)
	if err != nil {
		// FATAL: could not read the queue. No message was staged, so nothing to delete.
		return models.NewWorkflowSchedulerError("failed to read queue", err, true)
	}

	// No message to process
	if msg == nil {
		// NOOP
		return nil
	}

	payload, err := msg.StringPayload()
	if err != nil {
		// Poison message: unreadable payload. Record and drop; never fatal.
		s.recordAndDropInvalidMessage(ctx, msg, "", "unreadable payload")
		return nil
	}

	parsed, err := models.ParseIPCMessage(s.validator, []byte(payload))
	if err != nil {
		// Poison message: unparsable payload. Record and drop; never fatal.
		s.recordAndDropInvalidMessage(ctx, msg, payload, "unparsable message")
		return nil
	}

	// handlerErr captures the outcome of the dispatched handler. Whether it is nil or not, the
	// message below is deleted (the handler completed); handlerErr is only returned afterward so
	// the caller can classify fatal-vs-continue. A poison/unsupported message returns early (it
	// is dropped, not handled) and never reaches the shared delete below.
	var handlerErr error

	switch typed := parsed.(type) {
	case models.IPCMessageWorkflow:
		switch typed.Type {
		case models.IPCMsgTypeWFProcessWorkflow:
			// Start the workflow (PENDING -> RUNNING on first receipt) and fan out startable steps.
			handlerErr = s.processWorkflow(ctx, typed.WorkflowID)

		case models.IPCMsgTypeWFCancelWorkflow:
			// Mark the workflow CANCELLING, cancel in-flight step tasks, and settle if nothing drains.
			handlerErr = s.cancelWorkflow(ctx, typed.WorkflowID)

		default:
			// Poison message: unsupported message type. Record and drop.
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported workflow message type '%s'", typed.Type),
			)
			return nil
		}

	case models.IPCMessageWorkflowStep:
		switch typed.Type {
		case models.IPCMsgTypeWFScheduleStep:
			// Dispatch this step to the task engine (PENDING -> RUNNING + submit its task).
			handlerErr = s.scheduleWorkflowStep(ctx, typed.StepID)

		default:
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported workflow-step message type '%s'", typed.Type),
			)
			return nil
		}

	case models.IPCMessageWorkflowStepExecUpdate:
		switch typed.Type {
		case models.IPCMsgTypeWFStepExecUpdate:
			// Apply the step's resolved terminal outcome and advance the DAG.
			handlerErr = s.applyStepExecutionUpdate(ctx, typed.StepID, typed.NewStepState)

		default:
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported workflow-step-exec-update message type '%s'", typed.Type),
			)
			return nil
		}

	case models.IPCMessageWorkflowStepTaskUpdate:
		switch typed.Type {
		case models.IPCMsgTypeWFStepTaskUpdate:
			// notify fast-path feedback: resolve task -> step, then apply the outcome.
			handlerErr = s.applyStepTaskUpdate(ctx, typed.TaskID, typed.NewStepState)

		default:
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported workflow-step-task-update message type '%s'", typed.Type),
			)
			return nil
		}

	case models.IPCMessageWorkflowRevive:
		switch typed.Type {
		case models.IPCMsgTypeWFReviveWorkflow:
			// Revive a FAILED / TIMED_OUT workflow: revert its failed/timed-out steps to DEFINED
			// (+ new deadline) and re-run via a Process Workflow poke.
			revived, dropReason, err := s.reviveWorkflow(ctx, typed.WorkflowID, typed.NewDeadline)
			if err != nil {
				// Handler failure: capture, delete, and let the caller classify (same as any handler).
				handlerErr = err
			} else if !revived {
				// Precondition failure (wrong state / missing-or-past new deadline): a client error
				// that will never become valid on replay, so record + drop rather than wedge the queue.
				s.recordAndDropInvalidMessage(ctx, msg, payload, dropReason)
				return nil
			}

		default:
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported workflow-revive message type '%s'", typed.Type),
			)
			return nil
		}

	case models.BaseIPCMessage:
		switch typed.Type {
		case models.IPCMsgTypeWFMaintenance:
			// Run the Layer 2 recovery / liveness reconciliation sweep over all non-terminal workflows.
			handlerErr = s.runMaintenanceSweep(ctx)

		default:
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported base message type '%s'", typed.Type),
			)
			return nil
		}

	default:
		// Poison message: unsupported top-level message type. Record and drop.
		s.recordAndDropInvalidMessage(
			ctx, msg, payload,
			fmt.Sprintf("unsupported message type '%s'", reflect.TypeOf(typed)),
		)
		return nil
	}

	// The handler ran to completion (success or failure) - done with the message, delete it from
	// the buffer. The delete is best-effort and must not mask handlerErr.
	if err := s.ipcReceiver.DeleteBufferedMessage(ctx, msg); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to delete processed IPC message from queue buffer")
	}

	return handlerErr
}
