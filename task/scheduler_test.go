package task_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alwitt/goutils"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocktest "github.com/alwitt/tasking/mocks/test"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/task"
	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// validSchedulerConfig build a TaskSchedulerConfig which passes validation. The
// single task mapping means exactly one IPC sender factory call is expected.
func validSchedulerConfig() models.TaskSchedulerConfig {
	return models.TaskSchedulerConfig{
		MaintenanceTimerIntSecs: 10,
		SchedulerQueue:          "scheduler-q",
		TaskMappings: []models.TaskQueueMapping{
			{TaskName: "unit-test-task", ExecutionQueue: "unit-test-queue"},
		},
	}
}

// baseSchedulerParams build a fully-populated NewSchedulerParams with both IPC
// factories bound to the callback collector. Individual tests mutate a single
// factory expectation to drive a specific branch.
func baseSchedulerParams(
	cbMock *mocktest.UnitTestCallbackCollector,
	mockClient *mockdb.Client,
	mockRedis *mocktest.RedisClientForTest,
) task.NewSchedulerParams {
	return task.NewSchedulerParams{
		Persistence:        mockClient,
		Config:             validSchedulerConfig(),
		Redis:              mockRedis,
		IPCReceiverFactory: cbMock.NewRedisIPCMsgReceiver,
		IPCSenderFactory:   cbMock.NewRedisIPCMsgSender,
	}
}

// TestNewScheduler validates the constructor's factory error branches and happy path.
func TestNewScheduler(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	simErr := fmt.Errorf("simulated factory failure")

	t.Run("invalid params", func(t *testing.T) {
		assert := assert.New(t)

		// Empty params fail required-field validation.
		scheduler, err := task.NewScheduler(utCtx, task.NewSchedulerParams{})
		assert.Nil(scheduler)
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

		scheduler, err := task.NewScheduler(utCtx, baseSchedulerParams(cbMock, mockClient, mockRedis))
		assert.Nil(scheduler)
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(
			errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err,
		)
	})

	t.Run("IPC sender factory fails", func(t *testing.T) {
		assert := assert.New(t)

		cbMock := mocktest.NewUnitTestCallbackCollector(t)
		mockClient := mockdb.NewClient(t)
		mockRedis := mocktest.NewRedisClientForTest(t)

		// Receiver factory succeeds so the per-mapping sender loop is reached.
		cbMock.EXPECT().
			NewRedisIPCMsgReceiver(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(mockcommon.NewIPCMessageReceive(t), nil)
		cbMock.EXPECT().
			NewRedisIPCMsgSender(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, simErr)

		scheduler, err := task.NewScheduler(utCtx, baseSchedulerParams(cbMock, mockClient, mockRedis))
		assert.Nil(scheduler)
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(
			errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err,
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
			NewRedisIPCMsgSender(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(mockcommon.NewIPCMessageSend(t), nil)

		scheduler, err := task.NewScheduler(utCtx, baseSchedulerParams(cbMock, mockClient, mockRedis))
		assert.Nil(err)
		assert.NotNil(scheduler)
	})
}
