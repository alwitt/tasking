package task_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alwitt/goutils"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktask "github.com/alwitt/tasking/mocks/task"
	mocktest "github.com/alwitt/tasking/mocks/test"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// validReceiverConfig build a TaskReceiverConfig which passes validation.
func validReceiverConfig() models.TaskReceiverConfig {
	return models.TaskReceiverConfig{
		Name:           "unit-test-worker",
		SchedulerQueue: "scheduler-q",
		Queues: []models.TaskQueueConfig{
			{Name: "unit-test-queue", Workers: 1, BufferRequests: 1},
		},
	}
}

// baseReceiverParams build a fully-populated, valid NewReceiverParams with every
// factory bound to the callback collector. Individual tests mutate a single field
// (or a single factory expectation) to drive a specific branch.
func baseReceiverParams(
	cbMock *mocktest.UnitTestCallbackCollector,
	mockClient *mockdb.Client,
	mockRedis *mocktest.RedisClientForTest,
) task.NewReceiverParams {
	return task.NewReceiverParams{
		Support: task.ExecutorSupport{
			Persistence:  mockClient,
			OnCompleteCB: cbMock.OnComplete,
		},
		Config:             validReceiverConfig(),
		ExecutorFactory:    cbMock.NewTaskExecutor,
		Redis:              mockRedis,
		IPCReceiverFactory: cbMock.NewRedisIPCMsgReceiver,
		IPCSenderFactory:   cbMock.NewRedisIPCMsgSender,
	}
}

// TestNewReceiver validates the constructor's error branches and happy path.
func TestNewReceiver(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	simErr := fmt.Errorf("simulated factory failure")

	t.Run("invalid params", func(t *testing.T) {
		assert := assert.New(t)

		// Empty params fail required-field validation.
		receiver, err := task.NewReceiver(utCtx, task.NewReceiverParams{})
		assert.Nil(receiver)
		assert.NotNil(err)
		var badInput goutils.BadInputError
		assert.True(
			errors.As(err, &badInput), "expected BadInputError, got %T: %v", err, err,
		)
	})

	t.Run("IPC receiver factory fails", func(t *testing.T) {
		assert := assert.New(t)

		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		mockClient := mockdb.NewClient(t)
		mockRedis := mocktest.NewRedisClientForTest(t)

		cbMock.EXPECT().
			NewRedisIPCMsgReceiver(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, simErr)

		receiver, err := task.NewReceiver(utCtx, baseReceiverParams(cbMock, mockClient, mockRedis))
		assert.Nil(receiver)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(
			errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err,
		)
	})

	t.Run("executor factory fails", func(t *testing.T) {
		assert := assert.New(t)

		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		mockClient := mockdb.NewClient(t)
		mockRedis := mocktest.NewRedisClientForTest(t)

		// Receiver factory succeeds so the executor loop is reached.
		cbMock.EXPECT().
			NewRedisIPCMsgReceiver(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(mockcommon.NewIPCMessageReceive(t), nil)
		cbMock.EXPECT().
			NewTaskExecutor(
				mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			).
			Return(nil, simErr)

		receiver, err := task.NewReceiver(utCtx, baseReceiverParams(cbMock, mockClient, mockRedis))
		assert.Nil(receiver)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(
			errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err,
		)
	})

	t.Run("sender factory fails", func(t *testing.T) {
		assert := assert.New(t)

		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		mockClient := mockdb.NewClient(t)
		mockRedis := mocktest.NewRedisClientForTest(t)

		// Receiver + executor factories succeed so the sender step is reached.
		cbMock.EXPECT().
			NewRedisIPCMsgReceiver(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(mockcommon.NewIPCMessageReceive(t), nil)
		cbMock.EXPECT().
			NewTaskExecutor(
				mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			).
			Return(mocktask.NewExecutor(t), nil)
		cbMock.EXPECT().
			NewRedisIPCMsgSender(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, simErr)

		receiver, err := task.NewReceiver(utCtx, baseReceiverParams(cbMock, mockClient, mockRedis))
		assert.Nil(receiver)
		assert.NotNil(err)
		var recvErr models.TaskReceiverError
		assert.True(
			errors.As(err, &recvErr), "expected TaskReceiverError, got %T: %v", err, err,
		)
	})

	t.Run("happy path", func(t *testing.T) {
		assert := assert.New(t)

		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		mockClient := mockdb.NewClient(t)
		mockRedis := mocktest.NewRedisClientForTest(t)

		cbMock.EXPECT().
			NewRedisIPCMsgReceiver(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(mockcommon.NewIPCMessageReceive(t), nil)
		cbMock.EXPECT().
			NewTaskExecutor(
				mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			).
			Return(mocktask.NewExecutor(t), nil)
		cbMock.EXPECT().
			NewRedisIPCMsgSender(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(mockcommon.NewIPCMessageSend(t), nil)

		receiver, err := task.NewReceiver(utCtx, baseReceiverParams(cbMock, mockClient, mockRedis))
		assert.Nil(err)
		assert.NotNil(receiver)
	})
}
