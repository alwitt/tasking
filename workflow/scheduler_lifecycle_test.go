package workflow

import (
	"context"
	"testing"

	mockredis "github.com/alwitt/goutils/mocks/redis"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/common"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	mocknotify "github.com/alwitt/tasking/mocks/notify"
	mocktask "github.com/alwitt/tasking/mocks/task"
	"github.com/alwitt/tasking/models"
	"github.com/alwitt/tasking/notify"
	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestNewWorkflowSchedulerNotifyWiring covers that the constructor builds the notify consumer on the
// engine's creator channel with the scheduler's onNotification callback, and that Start/Stop drive
// the consumer's Start/Stop.
func TestNewWorkflowSchedulerNotifyWiring(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()

	mockClient := mockdb.NewClient(t)
	mockTasks := mocktask.NewClient(t)
	mockRedis := mockredis.NewClient(t)
	mockConsumer := mocknotify.NewConsumer(t)

	// Permissive receiver: Start runs recoverBufferedMessages (drains the buffer once) then
	// processQueue on a goroutine (blocks reading the main queue). Return nil from both so the
	// buffer looks empty and the main-queue read is a harmless no-op until Stop cancels the context.
	mockReceiver := mockcommon.NewIPCMessageReceive(t)
	mockReceiver.EXPECT().
		DequeueBufferedMessage(mock.Anything, true, mock.Anything).
		Return(nil, nil).
		Maybe()
	mockReceiver.EXPECT().
		DequeueMessage(mock.Anything, true, mock.Anything).
		Return(nil, nil).
		Maybe()

	// Test-double IPC factories: return the mocks without touching Redis, so the constructor's queue
	// setup succeeds without a live server.
	receiverFactory := func(
		_ context.Context, _ string, _ goutilsRedis.Client, _ string,
	) (common.IPCMessageReceive, error) {
		return mockReceiver, nil
	}
	senderFactory := func(
		_ context.Context, _ string, _ goutilsRedis.Client, _ string,
	) (common.IPCMessageSend, error) {
		return mockcommon.NewIPCMessageSend(t), nil
	}

	// Capture the NewConsumerParams the scheduler builds so we can assert the subscription channel
	// and that a callback was supplied.
	var capturedParams notify.NewConsumerParams
	notifyFactory := func(
		_ context.Context, params notify.NewConsumerParams,
	) (notify.Consumer, error) {
		capturedParams = params
		return mockConsumer, nil
	}

	scheduler, err := NewWorkflowScheduler(utCtx, NewWorkflowSchedulerParams{
		Persistence: mockClient,
		TaskClient:  mockTasks,
		Config: models.WorkflowSchedulerConfig{
			MaintenanceTimerIntSecs: 30,
			SchedulerQueue:          "unit-test-wf-queue",
		},
		Redis:                 mockRedis,
		IPCReceiverFactory:    receiverFactory,
		IPCSenderFactory:      senderFactory,
		NotifyConsumerFactory: notifyFactory,
	})
	require.NoError(t, err)
	require.NotNil(t, scheduler)

	// The subscription is the single creator channel for the engine's reserved creator.
	assert.Equal(t,
		[]string{models.BuildNotifyCreatorChannelName(models.WorkflowExecutionTaskCreator)},
		capturedParams.Topics,
	)
	assert.NotNil(t, capturedParams.Callback)
	assert.NotEmpty(t, capturedParams.Name)

	// Start drives the consumer's Start. (The maintenance timer + processQueue also start, but the
	// consumer is what this test asserts.)
	mockConsumer.EXPECT().Start(mock.Anything).Return(nil)
	require.NoError(t, scheduler.Start(utCtx))

	// Stop drives the consumer's Stop.
	mockConsumer.EXPECT().Stop(mock.Anything).Return(nil)
	require.NoError(t, scheduler.Stop(utCtx))
}
