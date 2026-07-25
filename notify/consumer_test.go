package notify

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	mockredis "github.com/alwitt/goutils/mocks/redis"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// rawEnvelope a minimal goutilsRedis.QueueMessageEnvelope carrying a fixed string payload, for
// feeding onMessage a received pub/sub message without a live subscriber.
type rawEnvelope struct{ payload string }

// StringPayload return its payload as a string
func (e rawEnvelope) StringPayload() (string, error) { return e.payload, nil }

// newTestConsumer builds a white-box consumerImpl wired with the supplied callback and (optional)
// subscriber, for driving onMessage / Start / Stop directly without a live Redis.
func newTestConsumer(
	callback NotificationCallback, subscriber goutilsRedis.Subscriber,
) *consumerImpl {
	instance := &consumerImpl{
		Component:  goutils.Component{LogTags: log.Fields{"module": "notify"}},
		callback:   callback,
		subscriber: subscriber,
	}
	instance.workerCtx, instance.workerCtxCancel = context.WithCancel(context.Background())
	return instance
}

// subjectNotification builds a task-subject NotificationEvent for round-trip assertions.
func subjectNotification() models.NotificationEvent {
	creator := "creator-a"
	subjectType := "task"
	subjectID := ulid.Make().String()
	return models.NotificationEvent{
		ID:          ulid.Make().String(),
		EventType:   models.SystemEventTypeCompleteTask,
		CreatedAt:   time.Now().UTC().Truncate(time.Second),
		Creator:     &creator,
		SubjectType: &subjectType,
		SubjectID:   &subjectID,
	}
}

func TestNewConsumer(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()

	validParams := func(
		redisClient goutilsRedis.Client,
	) NewConsumerParams {
		return NewConsumerParams{
			Redis:    redisClient,
			Topics:   []string{models.BuildNotifySubjectChannelName("task", "*")},
			Callback: func(context.Context, models.NotificationEvent) {},
			Name:     "unit-test-consumer",
		}
	}

	t.Run("happy path subscribes and retains the runner", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockredis.NewClient(t)
		mockSub := mockredis.NewSubscriber(t)
		mockClient.EXPECT().
			Subscribe(mock.Anything, "unit-test-consumer", []string{
				models.BuildNotifySubjectChannelName("task", "*"),
			}).
			Return(mockSub, nil)

		consumer, err := NewConsumer(utCtx, validParams(mockClient))
		assert.Nil(err)
		assert.NotNil(consumer)
		assert.Same(mockSub, consumer.(*consumerImpl).subscriber)
	})

	t.Run("subscribe failure is a NotifyConsumerError", func(t *testing.T) {
		assert := assert.New(t)

		mockClient := mockredis.NewClient(t)
		mockClient.EXPECT().
			Subscribe(mock.Anything, mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("simulated failure"))

		consumer, err := NewConsumer(utCtx, validParams(mockClient))
		assert.Nil(consumer)
		assert.NotNil(err)
		var cerr models.NotifyConsumerError
		assert.ErrorAs(err, &cerr)
	})

	t.Run("invalid params are rejected without subscribing", func(t *testing.T) {
		// A nil Redis / nil Callback / empty Topics / empty Name each fails validation before any
		// Subscribe call. The mock client asserts no unexpected calls (NewClient(t) fails on any).
		cases := map[string]NewConsumerParams{
			"nil redis": {
				Redis:    nil,
				Topics:   []string{"notify:all"},
				Callback: func(context.Context, models.NotificationEvent) {},
				Name:     "n",
			},
			"nil callback": {
				Redis:    mockredis.NewClient(t),
				Topics:   []string{"notify:all"},
				Callback: nil,
				Name:     "n",
			},
			"empty topics": {
				Redis:    mockredis.NewClient(t),
				Topics:   []string{},
				Callback: func(context.Context, models.NotificationEvent) {},
				Name:     "n",
			},
			"empty name": {
				Redis:    mockredis.NewClient(t),
				Topics:   []string{"notify:all"},
				Callback: func(context.Context, models.NotificationEvent) {},
				Name:     "",
			},
		}
		for name, params := range cases {
			t.Run(name, func(t *testing.T) {
				assert := assert.New(t)
				consumer, err := NewConsumer(utCtx, params)
				assert.Nil(consumer)
				assert.NotNil(err)
			})
		}
	})
}

func TestConsumerStartStop(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()

	t.Run("Start delegates to the subscriber", func(t *testing.T) {
		assert := assert.New(t)

		mockSub := mockredis.NewSubscriber(t)
		mockSub.EXPECT().Start(mock.Anything, mock.Anything).Return(nil)

		consumer := newTestConsumer(func(context.Context, models.NotificationEvent) {}, mockSub)
		assert.Nil(consumer.Start(utCtx))
	})

	t.Run("Start failure is a NotifyConsumerError", func(t *testing.T) {
		assert := assert.New(t)

		mockSub := mockredis.NewSubscriber(t)
		mockSub.EXPECT().
			Start(mock.Anything, mock.Anything).
			Return(fmt.Errorf("simulated failure"))

		consumer := newTestConsumer(func(context.Context, models.NotificationEvent) {}, mockSub)
		err := consumer.Start(utCtx)
		assert.NotNil(err)
		var cerr models.NotifyConsumerError
		assert.ErrorAs(err, &cerr)
	})

	t.Run("Stop delegates to the subscriber and cancels the working ctx", func(t *testing.T) {
		assert := assert.New(t)

		mockSub := mockredis.NewSubscriber(t)
		mockSub.EXPECT().Stop(mock.Anything).Return(nil)

		consumer := newTestConsumer(func(context.Context, models.NotificationEvent) {}, mockSub)
		assert.Nil(consumer.Stop(utCtx))
		// The working context is cancelled so an in-flight callback observing it can unblock.
		assert.Error(consumer.workerCtx.Err())
	})

	t.Run("Stop tolerates a subscriber Stop error", func(t *testing.T) {
		assert := assert.New(t)

		mockSub := mockredis.NewSubscriber(t)
		mockSub.EXPECT().Stop(mock.Anything).Return(fmt.Errorf("simulated failure"))

		consumer := newTestConsumer(func(context.Context, models.NotificationEvent) {}, mockSub)
		// A subscriber Stop failure is logged, not returned.
		assert.Nil(consumer.Stop(utCtx))
		assert.Error(consumer.workerCtx.Err())
	})
}

func TestConsumerOnMessage(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	t.Run("deserializes and dispatches to the callback", func(t *testing.T) {
		assert := assert.New(t)

		var got []models.NotificationEvent
		consumer := newTestConsumer(
			func(_ context.Context, event models.NotificationEvent) {
				got = append(got, event)
			}, nil,
		)

		want := subjectNotification()
		payload, err := want.StringPayload()
		assert.Nil(err)

		consumer.onMessage(consumer.workerCtx, goutilsRedis.PubSubMessage{
			Topic:   models.BuildNotifySubjectChannelName(*want.SubjectType, *want.SubjectID),
			Message: rawEnvelope{payload: payload},
		})

		assert.Len(got, 1)
		assert.Equal(want.ID, got[0].ID)
		assert.Equal(want.EventType, got[0].EventType)
		assert.Equal(want.Creator, got[0].Creator)
		assert.Equal(want.SubjectType, got[0].SubjectType)
		assert.Equal(want.SubjectID, got[0].SubjectID)
		assert.True(want.CreatedAt.Equal(got[0].CreatedAt))
	})

	t.Run("drops an undeserializable payload without invoking the callback", func(t *testing.T) {
		assert := assert.New(t)

		invoked := false
		consumer := newTestConsumer(
			func(context.Context, models.NotificationEvent) { invoked = true }, nil,
		)

		// Must not panic, must not invoke the callback, must return normally.
		assert.NotPanics(func() {
			consumer.onMessage(consumer.workerCtx, goutilsRedis.PubSubMessage{
				Topic:   "notify:subject:task:whatever",
				Message: rawEnvelope{payload: "}{ not json"},
			})
		})
		assert.False(invoked)
	})
}
