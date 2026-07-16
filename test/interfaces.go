// Package test - various support components used in unit-testing.
package test

import (
	"context"
	"time"

	goutilsRedis "github.com/alwitt/goutils/redis"
)

// UnitTestCallbackCollector unit-testing interface for collecting callbacks
type UnitTestCallbackCollector interface {
	// OnComplete called when task execution completes
	OnComplete(ctx context.Context, instanceID string, err error, timestamp time.Time)
}

// RedisClientForTest wrapper interface for generating a mock of `goutilsRedis.Client`
type RedisClientForTest interface {
	goutilsRedis.Client
}

// RedisQueueForTest wrapper interface for generating a mock of `goutilsRedis.Queue`
type RedisQueueForTest interface {
	goutilsRedis.Queue
}
