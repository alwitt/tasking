// Package common - common utility structs and functions
package common //revive:disable-line:var-naming

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

// ======================================================================================
// IPC Message Receiver
// ======================================================================================

// IPCMessageReceive client for receiving message on a queue
type IPCMessageReceive interface {
	/*
		DequeueMessage dequeue message from main queue.

		The message will staged on the buffer queue.

			@param ctx context.Context - execution context
			@param blocking bool - whether this is a blocking operation
			@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
			@return message from main queue, or nil if queue empty
	*/
	DequeueMessage(
		ctx context.Context, blocking bool, maxWait *time.Duration,
	) (goutilsRedis.QueueMessageEnvelope, error)

	/*
		DequeueBufferedMessage dequeue message from buffer queue.

			@param ctx context.Context - execution context
			@param blocking bool - whether this is a blocking operation
			@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
			@return message from buffer queue, or nil if queue empty
	*/
	DequeueBufferedMessage(
		ctx context.Context, blocking bool, maxWait *time.Duration,
	) (goutilsRedis.QueueMessageEnvelope, error)

	/*
		DeleteBufferedMessage delete from buffer queue.

			@param ctx context.Context - execution context
			@param msg goutilsRedis.QueueMessageEnvelope - the message to delete
	*/
	DeleteBufferedMessage(ctx context.Context, msg goutilsRedis.QueueMessageEnvelope) error

	/*
		ReEnqueueOnMainQueue re-enqueue message on main queue.

		The message will be deleted from the buffer queue.

			@param ctx context.Context - execution context
			@param msg goutilsRedis.QueueMessageEnvelope - the message to enqueue
	*/
	ReEnqueueOnMainQueue(
		ctx context.Context, msg goutilsRedis.QueueMessageEnvelope,
	) error
}

// ======================================================================================
// REDIS based IPC message receiver

// redisIPCMsgReceive implements IPCMessageReceive for REDIS queues.
//
// - LEFT of QUEUE is NEW
//
// - RIGHT of QUEUE is OLD
type redisIPCMsgReceive struct {
	goutils.Component

	// mainQueue primary queue to receive messages on
	mainQueue goutilsRedis.Queue
	// bufferQueue buffer queue to be used with the primary queue to support
	// only once message processing
	bufferQueue goutilsRedis.Queue

	reader string
}

// BuildIPCMessageQueueName helper function to build IPC queue name
func BuildIPCMessageQueueName(queueName string) string {
	return strings.Join(
		[]string{"ipc", strings.ToLower(strings.TrimSpace(queueName))}, ":",
	)
}

// BuildIPCMessageBufferQueueName helper function to build IPC reader buffer queue name
func BuildIPCMessageBufferQueueName(queueName string, reader string) string {
	return strings.Join(
		[]string{
			"ipc_buffer",
			strings.ToLower(strings.TrimSpace(queueName)),
			"reader",
			strings.ToLower(strings.TrimSpace(reader)),
		}, ":",
	)
}

/*
NewRedisIPCMessageReceive define a new IPC message receiver for a REDIS queue

The intended usage case of the receive is:

- Move a message from the main queue to the reader buffer queue

- Subscriber verify they can take ownership of message
  - If yes, process the message, then delete from the reader buffer queue.
  - If no, move the message back to the main queue.

Each reader must have exclusive ownership of it's own buffer queue.

On initialization, reader process any existing messages in the buffer queue. For each
existing message in the buffer queue:

- Subscriber verify its ownership of message
  - If yes, process the message.
  - If no, move the message back to the main queue.

At application start

	@param ctx context.Context - execution context
	@param queueName string - read message from queue
	@param redis goutilsRedis.Client - REDIS client
	@param reader string - queue reader name
	@returns new receiver
*/
func NewRedisIPCMessageReceive(
	ctx context.Context, queueName string, redis goutilsRedis.Client, reader string,
) (IPCMessageReceive, error) {
	logTags := log.Fields{
		"module":    "task",
		"component": "redis-ipc-msg-receive",
		"queue":     queueName,
		"reader":    reader,
	}

	mainQueueName := BuildIPCMessageQueueName(queueName)
	bufferQueueName := BuildIPCMessageBufferQueueName(queueName, reader)

	mainQueue, err := redis.GetQueueHandle(ctx, mainQueueName)
	if err != nil {
		return nil, models.NewIPCMessageQueueError(
			fmt.Sprintf("failed to get handle to REDIS queue %s", mainQueueName), err, true,
		)
	}
	bufferQueue, err := redis.GetQueueHandle(ctx, bufferQueueName)
	if err != nil {
		return nil, models.NewIPCMessageQueueError(
			fmt.Sprintf("failed to get handle to REDIS queue %s", bufferQueueName), err, true,
		)
	}

	instance := &redisIPCMsgReceive{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		mainQueue:   mainQueue,
		bufferQueue: bufferQueue,
		reader:      reader,
	}

	return instance, nil
}

/*
DequeueMessage dequeue message from main queue.

The message will staged on the buffer queue.

	@param ctx context.Context - execution context
	@param blocking bool - whether this is a blocking operation
	@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
	@return message from main queue, or nil if queue empty
*/
func (r *redisIPCMsgReceive) DequeueMessage(
	ctx context.Context, blocking bool, maxWait *time.Duration,
) (goutilsRedis.QueueMessageEnvelope, error) {
	newMsg, err := r.mainQueue.PopLeftAndMove(
		ctx, r.bufferQueue.QueueName(), false, blocking, maxWait,
	)
	if err != nil {
		return nil, models.NewIPCMessageQueueError(
			fmt.Sprintf("failed to dequeue from %s", r.mainQueue.QueueName()), err, true,
		)
	}
	return newMsg, nil
}

/*
DequeueBufferedMessage dequeue message from buffer queue.

	@param ctx context.Context - execution context
	@param blocking bool - whether this is a blocking operation
	@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
	@return message from buffer queue, or nil if queue empty
*/
func (r *redisIPCMsgReceive) DequeueBufferedMessage(
	ctx context.Context, blocking bool, maxWait *time.Duration,
) (goutilsRedis.QueueMessageEnvelope, error) {
	buffered, err := r.bufferQueue.PopLeft(ctx, blocking, maxWait)
	if err != nil {
		return nil, models.NewIPCMessageQueueError(
			fmt.Sprintf("failed to dequeue from %s", r.bufferQueue.QueueName()), err, true,
		)
	}
	return buffered, nil
}

/*
DeleteBufferedMessage delete from buffer queue.

	@param ctx context.Context - execution context
	@param msg goutilsRedis.QueueMessageEnvelope - the message to delete
*/
func (r *redisIPCMsgReceive) DeleteBufferedMessage(
	ctx context.Context, msg goutilsRedis.QueueMessageEnvelope,
) error {
	if err := r.bufferQueue.Remove(ctx, msg); err != nil {
		return models.NewIPCMessageQueueError(
			fmt.Sprintf("failed to delete msg from %s", r.bufferQueue.QueueName()), err, true,
		)
	}
	return nil
}

/*
ReEnqueueOnMainQueue re-enqueue message as the newest one on the main queue.

The message will be deleted from the buffer queue.

	@param ctx context.Context - execution context
	@param msg goutilsRedis.QueueMessageEnvelope - the message to enqueue
*/
func (r *redisIPCMsgReceive) ReEnqueueOnMainQueue(
	ctx context.Context, msg goutilsRedis.QueueMessageEnvelope,
) error {
	if err := r.bufferQueue.RemoveAndMove(ctx, r.mainQueue.QueueName(), msg, true); err != nil {
		return models.NewIPCMessageQueueError(
			fmt.Sprintf(
				"failed to enqueue message back to top of %s", r.mainQueue.QueueName(),
			), err, true,
		)
	}
	return nil
}

// ======================================================================================
// IPC Message Sender
// ======================================================================================

// IPCMessageSend client for sending message on a queue
type IPCMessageSend interface {
	/*
		EnqueueMessage enqueue message on queue

			@param ctx context.Context - execution context
			@param msg goutilsRedis.QueueMessageEnvelope - the message to enqueue
	*/
	EnqueueMessage(
		ctx context.Context, msg goutilsRedis.QueueMessageEnvelope,
	) error
}

// ======================================================================================
// REDIS based IPC message sender

// redisIPCMsgSend implements IPCMessageSend for REDIS queues.
//
// - LEFT of QUEUE is NEW
//
// - RIGHT of QUEUE is OLD
type redisIPCMsgSend struct {
	goutils.Component

	// mainQueue primary queue to receive messages on
	mainQueue goutilsRedis.Queue

	sender string
}

/*
NewRedisIPCMessageSend define a new IPC message sender for a REDIS queue

	@param ctx context.Context - execution context
	@param queueName string - write messages to queue
	@param redis goutilsRedis.Client - REDIS client
	@param sender string - queue sender name
	@returns new sender
*/
func NewRedisIPCMessageSend(
	ctx context.Context, queueName string, redis goutilsRedis.Client, sender string,
) (IPCMessageSend, error) {
	logTags := log.Fields{
		"module":    "task",
		"component": "redis-ipc-msg-send",
		"queue":     queueName,
		"sender":    sender,
	}

	mainQueueName := BuildIPCMessageQueueName(queueName)

	mainQueue, err := redis.GetQueueHandle(ctx, mainQueueName)
	if err != nil {
		return nil, models.NewIPCMessageQueueError(
			fmt.Sprintf("failed to get handle to REDIS queue %s", mainQueueName), err, true,
		)
	}

	instance := &redisIPCMsgSend{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		mainQueue: mainQueue,
		sender:    sender,
	}

	return instance, nil
}

/*
EnqueueMessage enqueue message on queue

	@param ctx context.Context - execution context
	@param msg goutilsRedis.QueueMessageEnvelope - the message to enqueue
*/
func (s *redisIPCMsgSend) EnqueueMessage(
	ctx context.Context, msg goutilsRedis.QueueMessageEnvelope,
) error {
	if _, err := s.mainQueue.PushRight(ctx, msg, nil); err != nil {
		return models.NewIPCMessageQueueError(
			fmt.Sprintf("failed to enqueue message on %s", s.mainQueue.QueueName()), err, true,
		)
	}
	return nil
}
