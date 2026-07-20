// Package test - various support components used in unit-testing.
package test

import (
	"context"
	"time"

	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/common"
	"github.com/alwitt/tasking/task"
)

// UnitTestCallbackCollector unit-testing interface for collecting callbacks
type UnitTestCallbackCollector interface {
	// OnComplete called when task execution completes
	OnComplete(ctx context.Context, instanceID string, err error, timestamp time.Time)

	// NewTaskExecutor factory function for defining new task executors
	NewTaskExecutor(
		parentCtx context.Context,
		taskQueue string,
		workerCount int,
		requestBufferLen int,
		support task.ExecutorSupport,
	) (task.Executor, error)

	// NewRedisIPCMsgReceiver factory function for defining new Redis based IPC message receivers
	NewRedisIPCMsgReceiver(
		ctx context.Context, queueName string, redis goutilsRedis.Client, reader string,
	) (common.IPCMessageReceive, error)

	// NewRedisIPCMsgSender factory function for defining new Redis based IPC message senders
	NewRedisIPCMsgSender(
		ctx context.Context, queueName string, redis goutilsRedis.Client, sender string,
	) (common.IPCMessageSend, error)
}
