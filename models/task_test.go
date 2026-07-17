package models_test

import (
	"math"
	"testing"
	"time"

	"github.com/alwitt/tasking/models"
	"github.com/stretchr/testify/assert"
)

func TestTaskRetryParametersNextDelay(t *testing.T) {
	assert := assert.New(t)

	intPtr := func(v int) *int { return &v }

	type testCase struct {
		name     string
		params   models.TaskRetryParameters
		retry    int
		expected time.Duration
	}

	tests := []testCase{
		{
			// retry budget exhausted (retry == MaxRetries) => stop
			name:     "budget exhausted",
			params:   models.TaskRetryParameters{MaxRetries: 3, InitialDelaySec: 5, Factor: 2},
			retry:    3,
			expected: 0,
		},
		{
			// retry beyond budget => stop
			name:     "beyond budget",
			params:   models.TaskRetryParameters{MaxRetries: 3, InitialDelaySec: 5, Factor: 2},
			retry:    10,
			expected: 0,
		},
		{
			// first retry: initial * factor^0 == initial
			name:     "first retry equals initial",
			params:   models.TaskRetryParameters{MaxRetries: 5, InitialDelaySec: 5, Factor: 2},
			retry:    0,
			expected: 5 * time.Second,
		},
		{
			// exponential growth: 5s * 2^2 == 20s
			name:     "exponential growth",
			params:   models.TaskRetryParameters{MaxRetries: 5, InitialDelaySec: 5, Factor: 2},
			retry:    2,
			expected: 20 * time.Second,
		},
		{
			// task-configured MaxDelay lower than the package ceiling caps the value
			name: "task max delay caps",
			params: models.TaskRetryParameters{
				MaxRetries: 10, InitialDelaySec: 5, MaxDelaySec: intPtr(30), Factor: 2,
			},
			retry:    5,
			expected: 30 * time.Second,
		},
		{
			// task MaxDelay larger than package ceiling is ignored; ceiling wins
			name: "package ceiling caps oversized task max delay",
			params: models.TaskRetryParameters{
				MaxRetries: 100, InitialDelaySec: 5, MaxDelaySec: intPtr(3600), Factor: 3,
			},
			retry:    50,
			expected: models.MaxRetryDelay,
		},
		{
			// infinite retries (MaxRetries == -1) with a huge retry index: the
			// float product overflows to +Inf but clamps to the finite ceiling.
			name:     "infinite retry overflow clamps to ceiling",
			params:   models.TaskRetryParameters{MaxRetries: -1, InitialDelaySec: 5, Factor: 2},
			retry:    10000,
			expected: models.MaxRetryDelay,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(_ *testing.T) {
			got := tc.params.NextDelay(tc.retry)
			assert.Equal(tc.expected, got)
			// invariant: a non-stop delay never exceeds the package ceiling and
			// never overflows negative.
			if got != 0 {
				assert.LessOrEqual(got, models.MaxRetryDelay)
				assert.Positive(got)
			}
		})
	}
}

func TestTaskRetryParametersNextDelayNeverOverflows(t *testing.T) {
	assert := assert.New(t)

	// walk a range of retries under infinite-retry with a big factor; the result
	// must stay finite and within the ceiling at every step (guards against the
	// undefined float64 -> time.Duration conversion for +Inf / > MaxInt64).
	params := models.TaskRetryParameters{MaxRetries: -1, InitialDelaySec: 10, Factor: 5}
	for retry := 0; retry < 1000; retry++ {
		got := params.NextDelay(retry)
		assert.GreaterOrEqual(int64(got), int64(0))
		assert.LessOrEqual(got, models.MaxRetryDelay)
		assert.Less(int64(got), int64(math.MaxInt64))
	}
}

func TestTaskExecutionIsStateAtOrPast(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		name      string
		current   models.TaskExecutionStateENUM
		reference models.TaskExecutionStateENUM
		expected  bool
	}

	tests := []testCase{
		// Same state is always at-or-past itself.
		{"same-defined", models.TaskExecutionStateDefined, models.TaskExecutionStateDefined, true},
		{
			"same-finalized",
			models.TaskExecutionStateFinalized, models.TaskExecutionStateFinalized, true,
		},
		// Directly downstream.
		{
			"scheduled-past-defined",
			models.TaskExecutionStateScheduled, models.TaskExecutionStateDefined, true,
		},
		{
			"finalized-past-processed",
			models.TaskExecutionStateFinalized, models.TaskExecutionStateProcessed, true,
		},
		// Multiple hops downstream.
		{
			"finalized-past-enqueued",
			models.TaskExecutionStateFinalized, models.TaskExecutionStateEnqueued, true,
		},
		{
			"processed-past-defined",
			models.TaskExecutionStateProcessed, models.TaskExecutionStateDefined, true,
		},
		// Strictly upstream must be false so real out-of-order deliveries are not masked.
		{
			"enqueued-not-past-processed",
			models.TaskExecutionStateEnqueued, models.TaskExecutionStateProcessed, false,
		},
		{
			"defined-not-past-scheduled",
			models.TaskExecutionStateDefined, models.TaskExecutionStateScheduled, false,
		},
		// Off-branch: PROCESSED is not downstream of FAILED (parallel outcomes), and vice versa.
		{
			"processed-not-past-failed",
			models.TaskExecutionStateProcessed, models.TaskExecutionStateFailed, false,
		},
		{
			"failed-not-past-processed",
			models.TaskExecutionStateFailed, models.TaskExecutionStateProcessed, false,
		},
	}

	for _, oneCase := range tests {
		entry := models.TaskExecution{ExecutionState: oneCase.current}
		assert.Equalf(
			oneCase.expected, entry.IsStateAtOrPast(oneCase.reference), "case '%s'", oneCase.name,
		)
	}
}

func TestTaskExecutionHasEnded(t *testing.T) {
	assert := assert.New(t)

	// Live states have not ended.
	for _, live := range []models.TaskExecutionStateENUM{
		models.TaskExecutionStateDefined,
		models.TaskExecutionStateScheduled,
		models.TaskExecutionStateEnqueued,
		models.TaskExecutionStateAcquired,
		models.TaskExecutionStateProcessing,
	} {
		entry := models.TaskExecution{ExecutionState: live}
		assert.Falsef(entry.HasEnded(), "state '%s' should be live", live)
	}

	// Terminal-outcome states (and downstream FINALIZED) have ended.
	for _, ended := range []models.TaskExecutionStateENUM{
		models.TaskExecutionStateProcessed,
		models.TaskExecutionStateFailed,
		models.TaskExecutionStateCancelled,
		models.TaskExecutionStateFinalized,
	} {
		entry := models.TaskExecution{ExecutionState: ended}
		assert.Truef(entry.HasEnded(), "state '%s' should be ended", ended)
	}
}
