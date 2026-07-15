package common_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/common"
	mocktest "github.com/alwitt/tasking/mocks/test"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// testIPCMessage a minimal `goutilsRedis.QueueMessageEnvelope` implementation used to drive
// the IPC message queue unit tests.
type testIPCMessage struct {
	payload string
}

// StringPayload return its payload as a string
func (m testIPCMessage) StringPayload() (string, error) {
	return m.payload, nil
}

func TestRedisIPCMessageReceiveNew(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	queueName := uuid.NewString()
	reader := uuid.NewString()

	mainQueueName := common.BuildIPCMessageQueueName(queueName)
	bufferQueueName := common.BuildIPCMessageBufferQueueName(queueName, reader)

	// Case 0: both queue handles resolve successfully
	{
		mockClient := mocktest.NewRedisClientForTest(t)
		mainQueue := mocktest.NewRedisQueueForTest(t)
		bufferQueue := mocktest.NewRedisQueueForTest(t)

		mockClient.EXPECT().
			GetQueueHandle(utCtx, mainQueueName).
			Return(mainQueue, nil).
			Once()
		mockClient.EXPECT().
			GetQueueHandle(utCtx, bufferQueueName).
			Return(bufferQueue, nil).
			Once()

		uut, err := common.NewRedisIPCMessageReceive(utCtx, queueName, mockClient, reader)
		assert.Nil(err)
		assert.NotNil(uut)
	}

	// Case 1: failure fetching the main queue handle
	{
		mockClient := mocktest.NewRedisClientForTest(t)

		mockClient.EXPECT().
			GetQueueHandle(utCtx, mainQueueName).
			Return(nil, fmt.Errorf("dummy error")).
			Once()

		uut, err := common.NewRedisIPCMessageReceive(utCtx, queueName, mockClient, reader)
		assert.NotNil(err)
		assert.Nil(uut)
	}

	// Case 2: failure fetching the buffer queue handle
	{
		mockClient := mocktest.NewRedisClientForTest(t)
		mainQueue := mocktest.NewRedisQueueForTest(t)

		mockClient.EXPECT().
			GetQueueHandle(utCtx, mainQueueName).
			Return(mainQueue, nil).
			Once()
		mockClient.EXPECT().
			GetQueueHandle(utCtx, bufferQueueName).
			Return(nil, fmt.Errorf("dummy error")).
			Once()

		uut, err := common.NewRedisIPCMessageReceive(utCtx, queueName, mockClient, reader)
		assert.NotNil(err)
		assert.Nil(uut)
	}
}

// buildTestReceiver wire up an `IPCMessageReceive` backed by the provided main and buffer
// queue mocks.
func buildTestReceiver(
	utCtx context.Context,
	assert *assert.Assertions,
	t *testing.T,
	queueName string,
	reader string,
	mainQueue *mocktest.RedisQueueForTest,
	bufferQueue *mocktest.RedisQueueForTest,
) common.IPCMessageReceive {
	mockClient := mocktest.NewRedisClientForTest(t)

	mainQueueName := common.BuildIPCMessageQueueName(queueName)
	bufferQueueName := common.BuildIPCMessageBufferQueueName(queueName, reader)

	mockClient.EXPECT().GetQueueHandle(utCtx, mainQueueName).Return(mainQueue, nil).Once()
	mockClient.EXPECT().GetQueueHandle(utCtx, bufferQueueName).Return(bufferQueue, nil).Once()

	uut, err := common.NewRedisIPCMessageReceive(utCtx, queueName, mockClient, reader)
	assert.Nil(err)
	assert.NotNil(uut)
	return uut
}

func TestRedisIPCMessageReceiveDequeueMessage(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	queueName := uuid.NewString()
	reader := uuid.NewString()
	mainQueueName := common.BuildIPCMessageQueueName(queueName)
	bufferQueueName := common.BuildIPCMessageBufferQueueName(queueName, reader)

	mainQueue := mocktest.NewRedisQueueForTest(t)
	bufferQueue := mocktest.NewRedisQueueForTest(t)
	// The receiver reads the buffer queue name when moving the message off the main queue,
	// and the main queue name when building error messages.
	bufferQueue.EXPECT().QueueName().Return(bufferQueueName)
	mainQueue.EXPECT().QueueName().Return(mainQueueName)

	uut := buildTestReceiver(utCtx, assert, t, queueName, reader, mainQueue, bufferQueue)

	maxWait := time.Second * 3

	// Case 0: successfully dequeue a message; it is atomically moved onto the buffer queue
	{
		expected := testIPCMessage{payload: uuid.NewString()}
		mainQueue.EXPECT().
			PopLeftAndMove(utCtx, bufferQueueName, false, true, &maxWait).
			Return(expected, nil).
			Once()

		msg, err := uut.DequeueMessage(utCtx, true, &maxWait)
		assert.Nil(err)
		payload, err := msg.StringPayload()
		assert.Nil(err)
		assert.Equal(expected.payload, payload)
	}

	// Case 1: main queue is empty; nothing to move
	{
		mainQueue.EXPECT().
			PopLeftAndMove(utCtx, bufferQueueName, false, false, (*time.Duration)(nil)).
			Return(nil, nil).
			Once()

		msg, err := uut.DequeueMessage(utCtx, false, nil)
		assert.Nil(err)
		assert.Nil(msg)
	}

	// Case 2: the underlying move fails
	{
		mainQueue.EXPECT().
			PopLeftAndMove(utCtx, bufferQueueName, false, false, (*time.Duration)(nil)).
			Return(nil, fmt.Errorf("dummy error")).
			Once()

		msg, err := uut.DequeueMessage(utCtx, false, nil)
		assert.NotNil(err)
		assert.Nil(msg)
	}
}

func TestRedisIPCMessageReceiveDequeueBufferedMessage(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	queueName := uuid.NewString()
	reader := uuid.NewString()
	bufferQueueName := common.BuildIPCMessageBufferQueueName(queueName, reader)

	mainQueue := mocktest.NewRedisQueueForTest(t)
	bufferQueue := mocktest.NewRedisQueueForTest(t)
	// The buffer queue name is read when building error messages.
	bufferQueue.EXPECT().QueueName().Return(bufferQueueName)

	uut := buildTestReceiver(utCtx, assert, t, queueName, reader, mainQueue, bufferQueue)

	maxWait := time.Second * 3

	// Case 0: successfully read a message from the buffer queue
	{
		expected := testIPCMessage{payload: uuid.NewString()}
		bufferQueue.EXPECT().
			PopLeft(utCtx, true, &maxWait).
			Return(expected, nil).
			Once()

		msg, err := uut.DequeueBufferedMessage(utCtx, true, &maxWait)
		assert.Nil(err)
		payload, err := msg.StringPayload()
		assert.Nil(err)
		assert.Equal(expected.payload, payload)
	}

	// Case 1: buffer queue is empty
	{
		bufferQueue.EXPECT().
			PopLeft(utCtx, false, (*time.Duration)(nil)).
			Return(nil, nil).
			Once()

		msg, err := uut.DequeueBufferedMessage(utCtx, false, nil)
		assert.Nil(err)
		assert.Nil(msg)
	}

	// Case 2: read from the buffer queue fails
	{
		bufferQueue.EXPECT().
			PopLeft(utCtx, false, (*time.Duration)(nil)).
			Return(nil, fmt.Errorf("dummy error")).
			Once()

		msg, err := uut.DequeueBufferedMessage(utCtx, false, nil)
		assert.NotNil(err)
		assert.Nil(msg)
	}
}

func TestRedisIPCMessageReceiveDeleteBufferedMessage(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	queueName := uuid.NewString()
	reader := uuid.NewString()
	bufferQueueName := common.BuildIPCMessageBufferQueueName(queueName, reader)

	mainQueue := mocktest.NewRedisQueueForTest(t)
	bufferQueue := mocktest.NewRedisQueueForTest(t)
	// The buffer queue name is read when building error messages.
	bufferQueue.EXPECT().QueueName().Return(bufferQueueName)

	uut := buildTestReceiver(utCtx, assert, t, queueName, reader, mainQueue, bufferQueue)

	// Case 0: the message is deleted from the buffer queue
	{
		msg := testIPCMessage{payload: uuid.NewString()}
		bufferQueue.EXPECT().
			Remove(utCtx, msg).
			Return(nil).
			Once()

		assert.Nil(uut.DeleteBufferedMessage(utCtx, msg))
	}

	// Case 1: delete from the buffer queue fails
	{
		msg := testIPCMessage{payload: uuid.NewString()}
		bufferQueue.EXPECT().
			Remove(utCtx, msg).
			Return(fmt.Errorf("dummy error")).
			Once()

		assert.NotNil(uut.DeleteBufferedMessage(utCtx, msg))
	}
}

func TestRedisIPCMessageReceiveReEnqueueOnMainQueue(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	queueName := uuid.NewString()
	reader := uuid.NewString()
	mainQueueName := common.BuildIPCMessageQueueName(queueName)

	mainQueue := mocktest.NewRedisQueueForTest(t)
	bufferQueue := mocktest.NewRedisQueueForTest(t)
	// The receiver reads the main queue name as the destination for the re-enqueue.
	mainQueue.EXPECT().QueueName().Return(mainQueueName)

	uut := buildTestReceiver(utCtx, assert, t, queueName, reader, mainQueue, bufferQueue)

	// Case 0: the message is atomically moved from the buffer queue back to the front of
	// the main queue (insertOnLeft == true)
	{
		msg := testIPCMessage{payload: uuid.NewString()}
		bufferQueue.EXPECT().
			RemoveAndMove(utCtx, mainQueueName, msg, true).
			Return(nil).
			Once()

		assert.Nil(uut.ReEnqueueOnMainQueue(utCtx, msg))
	}

	// Case 1: the re-enqueue fails (e.g. the message was no longer in the buffer queue)
	{
		msg := testIPCMessage{payload: uuid.NewString()}
		bufferQueue.EXPECT().
			RemoveAndMove(utCtx, mainQueueName, msg, true).
			Return(fmt.Errorf("dummy error")).
			Once()

		assert.NotNil(uut.ReEnqueueOnMainQueue(utCtx, msg))
	}
}

func TestRedisIPCMessageSendNew(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	queueName := uuid.NewString()
	sender := uuid.NewString()

	mainQueueName := common.BuildIPCMessageQueueName(queueName)

	// Case 0: the main queue handle resolves successfully
	{
		mockClient := mocktest.NewRedisClientForTest(t)
		mainQueue := mocktest.NewRedisQueueForTest(t)

		mockClient.EXPECT().
			GetQueueHandle(utCtx, mainQueueName).
			Return(mainQueue, nil).
			Once()

		uut, err := common.NewRedisIPCMessageSend(utCtx, queueName, mockClient, sender)
		assert.Nil(err)
		assert.NotNil(uut)
	}

	// Case 1: failure fetching the main queue handle
	{
		mockClient := mocktest.NewRedisClientForTest(t)

		mockClient.EXPECT().
			GetQueueHandle(utCtx, mainQueueName).
			Return(nil, fmt.Errorf("dummy error")).
			Once()

		uut, err := common.NewRedisIPCMessageSend(utCtx, queueName, mockClient, sender)
		assert.NotNil(err)
		assert.Nil(uut)
	}
}

func TestRedisIPCMessageSendEnqueueMessage(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	queueName := uuid.NewString()
	sender := uuid.NewString()
	mainQueueName := common.BuildIPCMessageQueueName(queueName)

	mockClient := mocktest.NewRedisClientForTest(t)
	mainQueue := mocktest.NewRedisQueueForTest(t)
	// The main queue name is read when building error messages.
	mainQueue.EXPECT().QueueName().Return(mainQueueName)

	mockClient.EXPECT().GetQueueHandle(utCtx, mainQueueName).Return(mainQueue, nil).Once()

	uut, err := common.NewRedisIPCMessageSend(utCtx, queueName, mockClient, sender)
	assert.Nil(err)
	assert.NotNil(uut)

	// Case 0: the message is pushed onto the tail (right / oldest end) of the main queue
	{
		msg := testIPCMessage{payload: uuid.NewString()}
		mainQueue.EXPECT().
			PushRight(utCtx, msg, (*time.Duration)(nil)).
			Return(uint64(1), nil).
			Once()

		assert.Nil(uut.EnqueueMessage(utCtx, msg))
	}

	// Case 1: the push fails
	{
		msg := testIPCMessage{payload: uuid.NewString()}
		mainQueue.EXPECT().
			PushRight(utCtx, msg, (*time.Duration)(nil)).
			Return(uint64(0), fmt.Errorf("dummy error")).
			Once()

		assert.NotNil(uut.EnqueueMessage(utCtx, msg))
	}
}

var _ goutilsRedis.QueueMessageEnvelope = testIPCMessage{}
