// Package test - various support components used in unit-testing.
package test

import (
	goutilsRedis "github.com/alwitt/goutils/redis"
)

// RedisClientForTest wrapper interface for generating a mock of `goutilsRedis.Client`
type RedisClientForTest interface {
	goutilsRedis.Client
}

// RedisQueueForTest wrapper interface for generating a mock of `goutilsRedis.Queue`
type RedisQueueForTest interface {
	goutilsRedis.Queue
}
