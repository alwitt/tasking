package task

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

// processQueue process the messages on the scheduler queue
func (s *schedulerImpl) processQueue() {
	logTags := s.GetLogTagsForContext(s.workerCtx)

	log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Info("Starting scheduler queue message processing")
	defer log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Info("Stopped scheduler queue message processing")

	for {
		// verify whether to stop
		if err := s.workerCtx.Err(); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Error("Stopping queue processing due to receiver context error")
			}
			break
		}

		if err := s.processOneIPCRequest(s.workerCtx); err != nil {
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Fatal("Encountered fatal error while processing IPC messages")
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
		switch typed := parsed.(type) {
		case models.IPCMessageSystemTask:
			switch typed.Type {
			case models.IPCMsgTypeNewTask, models.IPCMsgTypeCancelTask:
				// valid - fall through to re-enqueue below
			default:
				s.recordInvalidMessage(
					ctx, payload,
					fmt.Sprintf("unsupported system-task message type '%s'", typed.Type),
				)
				continue
			}
		case models.IPCMessageExecuteInstance:
			switch typed.Type {
			case models.IPCMsgTypeExecuteSucceeded,
				models.IPCMsgTypeExecuteFailed,
				models.IPCMsgTypeEngineFailed:
				// valid - fall through to re-enqueue below
			default:
				s.recordInvalidMessage(
					ctx, payload,
					fmt.Sprintf("unsupported execute-instance message type '%s'", typed.Type),
				)
				continue
			}
		default:
			s.recordInvalidMessage(
				ctx, payload, fmt.Sprintf("unsupported message type '%s'", reflect.TypeOf(typed)),
			)
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
processOneIPCRequest process one IPC request on the scheduler queue.

The scheduler queue is a reliable queue: DequeueMessage moves the message into a buffer
queue, and the message is only removed from that buffer once we are done with it. The
delete contract here is therefore explicit, not a blanket defer:

  - Valid message, submitted to the worker successfully -> delete from buffer (done with it).
  - Poison message (unreadable, unparsable, or unsupported type) -> record an audit event
    and delete from buffer; it can never be processed, so replaying it would loop forever.
  - Valid message but Submit failed -> Submit only fails while the scheduler is shutting
    down, so leave the message in the buffer (do NOT delete) and return the error; startup
    buffer recovery (recoverBufferedMessages) will replay it on the next start.

Note the messages are Submit-ed to the worker rather than handled inline: processQueue runs
on its own goroutine, so the worker's event loop is a separate consumer that drains what we
submit. This differs from performMaintenance, which calls the process* handlers directly
because it runs ON the worker; the two must not be unified.

	@param ctx context.Context - execution context
*/
func (s *schedulerImpl) processOneIPCRequest(ctx context.Context) error {
	logTags := s.GetLogTagsForContext(ctx)
	msg, err := s.ipcReceiver.DequeueMessage(ctx, true, nil)
	if err != nil {
		// FATAL
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

	switch typed := parsed.(type) {
	case models.IPCMessageSystemTask:
		switch typed.Type {
		case models.IPCMsgTypeNewTask:
			// New task pending scheduling
			if err := s.worker.Submit(
				ctx, schedulerWorkReqNewPendingTask{TaskID: typed.TaskID},
			); err != nil {
				// Submit only fails on shutdown: leave the message buffered for recovery.
				return models.NewTaskSchedulerError(
					"failed to submit new pending task request", err, true,
				)
			}

		case models.IPCMsgTypeCancelTask:
			// Task being cancelled
			if err := s.worker.Submit(ctx, schedulerWorkReqCancelTask{
				TaskID: typed.TaskID, Timestamp: typed.Timestamp,
			}); err != nil {
				return models.NewTaskSchedulerError(
					"failed to submit cancel task request", err, true,
				)
			}

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
			if err := s.worker.Submit(ctx, schedulerWorkReqTaskExecutionComplete{
				InstanceID: typed.InstanceID, Timestamp: typed.Timestamp,
			}); err != nil {
				return models.NewTaskSchedulerError(
					"failed to submit process completed execution request", err, true,
				)
			}

		case models.IPCMsgTypeExecuteFailed:
			// Task execution failed. The message carries the retry disposition, but the handler
			// reads it from the persisted instance (FailureDisposition) rather than the message:
			// the executor writes the column in the same step that sends this message, and that
			// persisted value is the single source of truth - so the maintenance backstop, which
			// has no message, reaches the same decision (DB is source of truth).
			if err := s.worker.Submit(ctx, schedulerWorkReqTaskExecutionFailed{
				InstanceID: typed.InstanceID, Timestamp: typed.Timestamp,
			}); err != nil {
				return models.NewTaskSchedulerError(
					"failed to submit process failed execution request", err, true,
				)
			}

		case models.IPCMsgTypeEngineFailed:
			// Core task engine failed to operate on the execution instance
			if err := s.worker.Submit(ctx, schedulerWorkReqTaskExecutionEngineFailed{
				InstanceID: typed.InstanceID, Timestamp: typed.Timestamp,
			}); err != nil {
				return models.NewTaskSchedulerError(
					"failed to submit process engine failure request", err, true,
				)
			}

		default:
			// Poison message: unsupported message type. Record and drop.
			s.recordAndDropInvalidMessage(
				ctx, msg, payload,
				fmt.Sprintf("unsupported execute-instance message type '%s'", typed.Type),
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

	// Valid message submitted successfully - done with it, delete from the buffer.
	if err := s.ipcReceiver.DeleteBufferedMessage(ctx, msg); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to delete processed IPC message from queue buffer")
	}

	return nil
}
