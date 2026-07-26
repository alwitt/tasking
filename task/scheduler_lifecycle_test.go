package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	mockgoutils "github.com/alwitt/goutils/mocks/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// lifecycleTestScheduler bundles the scheduler under test with the mocks Start/Stop drive.
type lifecycleTestScheduler struct {
	scheduler   *schedulerImpl
	ipcReceiver *mockcommon.IPCMessageReceive
	ipcSender   *mockcommon.IPCMessageSend
	timer       *mockgoutils.IntervalTimer
}

// newLifecycleTestScheduler build a white-box schedulerImpl wired for Start/Stop: a real
// cancelable run context (Stop cancels it, which is how processQueue is told to exit), plus
// mock interval timer, mock queue receiver, and mock self-sender (the maintenance timer
// enqueues onto it). There is no worker - the single processQueue goroutine is the only thread.
func newLifecycleTestScheduler(t *testing.T) lifecycleTestScheduler {
	mockClient := mockdb.NewClient(t)
	ipcReceiver := mockcommon.NewIPCMessageReceive(t)
	ipcSender := mockcommon.NewIPCMessageSend(t)
	timer := mockgoutils.NewIntervalTimer(t)

	s := newProcessTestScheduler(mockClient, nil)
	s.config = models.TaskSchedulerConfig{MaintenanceTimerIntSecs: 10}
	s.ipcReceiver = ipcReceiver
	s.ipcSender = ipcSender
	s.maintenanceTimer = timer
	s.runCtx, s.runCtxCancel = context.WithCancel(context.Background())

	return lifecycleTestScheduler{
		scheduler: s, ipcReceiver: ipcReceiver, ipcSender: ipcSender, timer: timer,
	}
}

// TestSchedulerStartStop exercises the full lifecycle: Start recovers the buffer, starts the
// maintenance timer, and launches the processQueue goroutine; Stop stops the timer, cancels
// the run context (unblocking processQueue), and waits for the goroutine to finish. Stop is
// asserted in the same test so the launched goroutine is always torn down.
func TestSchedulerStartStop(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	simErr := fmt.Errorf("simulated failure")

	t.Run("happy path: start launches the loop, stop tears it down", func(t *testing.T) {
		assert := assert.New(t)

		h := newLifecycleTestScheduler(t)

		// Start: empty buffer recovery, then timer start.
		h.ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		h.timer.EXPECT().
			Start(h.scheduler.config.MaintenanceTimerInt(), mock.Anything, false).
			Return(nil).
			Once()

		// The processQueue goroutine blocks in DequeueMessage until Stop cancels the
		// run context; on cancellation it returns (nil, nil), which processQueue
		// treats as a no-op, loops, sees the cancelled context, and exits cleanly.
		// entered is closed once the goroutine reaches the blocking read, so the test
		// can wait for the loop to actually be running before calling Stop (deterministic,
		// and proves processQueue ran rather than being torn down before its first read).
		entered := make(chan struct{})
		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			RunAndReturn(func(
				ctx context.Context, _ bool, _ *time.Duration,
			) (goutilsRedis.QueueMessageEnvelope, error) {
				close(entered)
				<-ctx.Done()
				return nil, nil
			}).
			Once()

		assert.Nil(h.scheduler.Start(context.Background()))

		// Wait until the goroutine is blocked in the read before tearing it down.
		<-entered

		// Stop: stops the timer, cancels the run context, waits for the goroutine.
		h.timer.EXPECT().Stop().Return(nil).Once()

		assert.Nil(h.scheduler.Stop(context.Background()))
	})

	t.Run("buffer recovery failure aborts start", func(t *testing.T) {
		assert := assert.New(t)

		h := newLifecycleTestScheduler(t)

		// Recovery reads the buffer first; a read error is fatal to Start, so the timer is
		// not started and no goroutine is launched.
		h.ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, simErr).
			Once()

		err := h.scheduler.Start(context.Background())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("maintenance timer start failure aborts start", func(t *testing.T) {
		assert := assert.New(t)

		h := newLifecycleTestScheduler(t)

		h.ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		// Timer fails to start: Start aborts before launching the processQueue goroutine,
		// so DequeueMessage is never called.
		h.timer.EXPECT().
			Start(h.scheduler.config.MaintenanceTimerInt(), mock.Anything, false).
			Return(simErr).
			Once()

		err := h.scheduler.Start(context.Background())
		assert.NotNil(err)
		var schedErr models.TaskSchedulerError
		assert.True(errors.As(err, &schedErr), "expected TaskSchedulerError, got %T: %v", err, err)
	})

	t.Run("stop tolerates a timer stop failure", func(t *testing.T) {
		assert := assert.New(t)

		h := newLifecycleTestScheduler(t)

		h.ipcReceiver.EXPECT().
			DequeueBufferedMessage(mock.Anything, true, mock.Anything).
			Return(nil, nil).
			Once()
		h.timer.EXPECT().
			Start(h.scheduler.config.MaintenanceTimerInt(), mock.Anything, false).
			Return(nil).
			Once()
		entered := make(chan struct{})
		h.ipcReceiver.EXPECT().
			DequeueMessage(mock.Anything, true, mock.Anything).
			RunAndReturn(func(
				ctx context.Context, _ bool, _ *time.Duration,
			) (goutilsRedis.QueueMessageEnvelope, error) {
				close(entered)
				<-ctx.Done()
				return nil, nil
			}).
			Once()

		assert.Nil(h.scheduler.Start(context.Background()))
		<-entered

		// The timer stop fails: Stop logs it but still cancels the context and waits for the
		// goroutine, so it returns nil once the goroutine has drained.
		h.timer.EXPECT().Stop().Return(simErr).Once()

		assert.Nil(h.scheduler.Stop(context.Background()))
	})
}
