package task

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

// processQueue process the messages on the scheduler queue
func (s *schedulerImpl) processQueue() {
	logTags := s.GetLogTagsForContext(s.runCtx)

	log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Info("Starting scheduler queue message processing")
	defer log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Info("Stopped scheduler queue message processing")

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
    a single processing path - recovered messages get the same handling as fresh ones.

    @param ctx context.Context - execution context
*/
func (s *schedulerImpl) recoverBufferedMessages(ctx context.Context) error {
	logTags := s.GetLogTagsForContext(ctx)

	for {
		msg, err := s.ipcReceiver.DequeueBufferedMessage(ctx, true, nil)
		if err != nil {
			// FATAL
			return models.NewTaskSchedulerError("failed to read queue buffer", err, true)
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

		// Only messages the scheduler queue actually handles should be replayed.
		if !s.isSupportedTaskMessage(ctx, payload, parsed) {
			continue
		}

		if err := s.ipcReceiver.ReEnqueueOnMainQueue(ctx, msg); err != nil {
			// FATAL
			return models.NewTaskSchedulerError(
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
isSupportedTaskMessage report whether a parsed IPC message is one the task scheduler queue
handles. A message of any other shape/type on this queue is genuinely poison, so it is recorded
(best-effort) and rejected. Shared by recoverBufferedMessages (replay filter) so the replay path
and the live path agree on what is a valid task scheduler message.

	@param ctx context.Context - execution context
	@param payload string - the raw payload, for the audit record
	@param parsed interface{} - the parsed message
	@return whether the message is a supported task scheduler message
*/
func (s *schedulerImpl) isSupportedTaskMessage(
	ctx context.Context, payload string, parsed interface{},
) bool {
	switch typed := parsed.(type) {
	case models.IPCMessageSystemTask:
		switch typed.Type {
		case models.IPCMsgTypeNewTask, models.IPCMsgTypeCancelTask:
			return true
		default:
			s.recordInvalidMessage(
				ctx, payload,
				fmt.Sprintf("unsupported system-task message type '%s'", typed.Type),
			)
			return false
		}
	case models.IPCMessageExecuteInstance:
		switch typed.Type {
		case models.IPCMsgTypeExecuteSucceeded,
			models.IPCMsgTypeExecuteFailed,
			models.IPCMsgTypeEngineFailed:
			return true
		default:
			s.recordInvalidMessage(
				ctx, payload,
				fmt.Sprintf("unsupported execute-instance message type '%s'", typed.Type),
			)
			return false
		}
	case models.BaseIPCMessage:
		if typed.Type == models.IPCMsgTypeTaskMaintenance {
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
processOneIPCRequest process one IPC request on the scheduler queue.

The scheduler queue is a reliable queue: DequeueMessage moves the message into a buffer queue,
and the message is only removed from that buffer once we are done with it. The buffer guards
against exactly one thing - an application crash/panic mid-handling, where the handler never
runs to completion. So the delete contract is keyed off *completion*, not success:

  - Handler ran to completion (returned nil OR an error) -> delete from the buffer. We are done
    with the message; only a crash (never a returned error) should strand one for replay.
  - Poison message (unreadable, unparsable, or unsupported type) -> record an audit event and
    delete from buffer; it can never be processed, so replaying it would loop forever.

The handler runs INLINE on this goroutine: there is no worker to submit to. The maintenance
path (performMaintenance) calls the same process* handlers directly; it arrives here as a
self-enqueued Task Maintenance message, so it rides the exact same serial path as every event.

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
		return models.NewTaskSchedulerError("failed to read queue", err, true)
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
	case models.IPCMessageSystemTask:
		switch typed.Type {
		case models.IPCMsgTypeNewTask:
			// New task pending scheduling
			handlerErr = s.processNewPendingTask(ctx, typed.TaskID)

		case models.IPCMsgTypeCancelTask:
			// Task being cancelled
			handlerErr = s.processCancelTask(ctx, typed.TaskID, typed.Timestamp)

		default:
			// Poison message: unsupported message type. Record and drop.
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported system-task message type '%s'", typed.Type),
			)
			return nil
		}

	case models.IPCMessageExecuteInstance:
		switch typed.Type {
		case models.IPCMsgTypeExecuteSucceeded:
			// Task execution completed
			handlerErr = s.processTaskExecutionComplete(ctx, typed.InstanceID, typed.Timestamp)

		case models.IPCMsgTypeExecuteFailed:
			// Task execution failed. The message carries the retry disposition, but the handler
			// reads it from the persisted instance (FailureDisposition) rather than the message:
			// the executor writes the column in the same step that sends this message, and that
			// persisted value is the single source of truth - so the maintenance backstop, which
			// has no message, reaches the same decision (DB is source of truth).
			handlerErr = s.processTaskExecutionFailed(ctx, typed.InstanceID, typed.Timestamp)

		case models.IPCMsgTypeEngineFailed:
			// Core task engine failed to operate on the execution instance
			handlerErr = s.processTaskExecutionEngineFailed(ctx, typed.InstanceID, typed.Timestamp)

		default:
			// Poison message: unsupported message type. Record and drop.
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported execute-instance message type '%s'", typed.Type),
			)
			return nil
		}

	case models.BaseIPCMessage:
		switch typed.Type {
		case models.IPCMsgTypeTaskMaintenance:
			// Run the periodic maintenance sweep - the universal backstop for lost pokes and the
			// only path that fires scheduled/retry executions (which have no triggering message).
			handlerErr = s.performMaintenance(ctx)

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
