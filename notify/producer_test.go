package notify

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	mockredis "github.com/alwitt/goutils/mocks/redis"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/db"
	mockdb "github.com/alwitt/tasking/mocks/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// runTxAgainst returns a RunAndReturn body that invokes the transaction closure against the
// supplied mock Database, mirroring production UseDatabaseInTransaction behavior.
func runTxAgainst(
	mockDatabase *mockdb.Database,
) func(context.Context, func(context.Context, db.Database) error) error {
	return func(ctx context.Context, core func(context.Context, db.Database) error) error {
		return core(ctx, mockDatabase)
	}
}

// newTestProducer build a white-box producerImpl wired with a registered validator and the
// supplied mock persistence + redis clients, for driving produceNotifications directly.
func newTestProducer(
	t *testing.T,
	config models.NotificationProducerConfig,
	mockClient *mockdb.Client,
	mockRedis *mockredis.Client,
) *producerImpl {
	validate := validator.New()
	require.NoError(t, models.RegisterWithValidator(validate))

	return &producerImpl{
		Component:   goutils.Component{LogTags: log.Fields{"module": "notify"}},
		validator:   validate,
		config:      config,
		persistence: mockClient,
		redis:       mockRedis,
		wg:          &sync.WaitGroup{},
	}
}

// taskEventEntry build a persisted task-state audit event with the given creator.
func taskEventEntry(
	t *testing.T, eventType models.SystemEventTypeENUM, taskID, creator string,
) models.SystemEventAudit {
	meta, err := json.Marshal(models.SystemEventTaskEvents{TaskID: taskID, Creator: creator})
	require.NoError(t, err)
	return models.SystemEventAudit{
		ID:        ulid.Make().String(),
		EventType: eventType,
		CreatedAt: time.Now().UTC(),
		Metadata:  meta,
	}
}

// ipcInvalidEntry build a persisted (creator-less) INVALID_TASK_IPC_MESSAGE audit event.
func ipcInvalidEntry(t *testing.T) models.SystemEventAudit {
	meta, err := json.Marshal(models.SystemEventInvalidTaskIPCMessage{
		Receiver: "scheduler", Reason: "bad message",
	})
	require.NoError(t, err)
	return models.SystemEventAudit{
		ID:        ulid.Make().String(),
		EventType: models.SystemEventTypeInvalidTaskIPCMessage,
		CreatedAt: time.Now().UTC(),
		Metadata:  meta,
	}
}

// TestProduceNotificationsFanOut a task-state event with all Emit* enabled fans out to all
// five channel families, and is stamped exactly once.
func TestProduceNotificationsFanOut(t *testing.T) {
	assert := assert.New(t)
	utCtx := context.Background()

	config := models.NotificationProducerConfig{
		PollIntervalSecs: 1, BatchSize: 10,
		EmitFirehose: true, EmitTypeChan: true, EmitCreator: true,
	}

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	mockRedis := mockredis.NewClient(t)
	uut := newTestProducer(t, config, mockClient, mockRedis)

	event := taskEventEntry(t, models.SystemEventTypeCompleteTask, "task-1", "creator-1")

	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(runTxAgainst(mockDatabase))

	mockDatabase.EXPECT().
		ListSystemEvents(mock.Anything, mock.MatchedBy(func(f db.SystemEventQueryFilter) bool {
			return f.OnlyNotBroadcast && f.Limit != nil && *f.Limit == 10
		})).
		Return([]models.SystemEventAudit{event}, nil).
		Once()

	// Capture every published topic.
	publishedTopics := map[string]bool{}
	mockRedis.EXPECT().
		Publish(mock.Anything, mock.Anything).
		Run(func(_ context.Context, msg goutilsRedis.PubSubMessage) {
			publishedTopics[msg.Topic] = true
		}).
		Return(nil)

	// Exactly the one event is stamped.
	mockDatabase.EXPECT().
		MarkSystemEventsBroadcast(
			mock.Anything, []string{event.ID}, mock.Anything,
		).
		Return(nil).
		Once()

	require.NoError(t, uut.produceNotifications(utCtx))

	assert.True(publishedTopics[models.BuildNotifyFirehoseChannelName()])
	assert.True(publishedTopics[models.BuildNotifyTypeChannelName(event.EventType)])
	assert.True(publishedTopics[models.BuildNotifyCreatorChannelName("creator-1")])
	assert.True(publishedTopics[models.BuildNotifyCreatorTypeChannelName("creator-1", event.EventType)])
	assert.True(publishedTopics[models.BuildNotifySubjectChannelName("task", "task-1")])
	assert.Len(publishedTopics, 5)
}

// TestProduceNotificationsConfigGating with the optional families disabled, a task event
// reaches only the always-on subject channel; a creator-less event reaches nothing.
func TestProduceNotificationsConfigGating(t *testing.T) {
	assert := assert.New(t)
	utCtx := context.Background()

	config := models.NotificationProducerConfig{
		PollIntervalSecs: 1, BatchSize: 10,
		EmitFirehose: false, EmitTypeChan: false, EmitCreator: false,
	}

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	mockRedis := mockredis.NewClient(t)
	uut := newTestProducer(t, config, mockClient, mockRedis)

	taskEvent := taskEventEntry(t, models.SystemEventTypeFailedTask, "task-2", "creator-2")
	ipcEvent := ipcInvalidEntry(t)

	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(runTxAgainst(mockDatabase))

	mockDatabase.EXPECT().
		ListSystemEvents(mock.Anything, mock.Anything).
		Return([]models.SystemEventAudit{taskEvent, ipcEvent}, nil).
		Once()

	publishedTopics := map[string]bool{}
	mockRedis.EXPECT().
		Publish(mock.Anything, mock.Anything).
		Run(func(_ context.Context, msg goutilsRedis.PubSubMessage) {
			publishedTopics[msg.Topic] = true
		}).
		Return(nil)

	// Both events publish cleanly (the IPC event to zero channels), so both are stamped.
	mockDatabase.EXPECT().
		MarkSystemEventsBroadcast(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, ids []string, _ time.Time) {
			assert.ElementsMatch([]string{taskEvent.ID, ipcEvent.ID}, ids)
		}).
		Return(nil).
		Once()

	require.NoError(t, uut.produceNotifications(utCtx))

	// Only the task event's subject channel; the creator-less IPC event routes nowhere.
	assert.Equal(
		map[string]bool{models.BuildNotifySubjectChannelName("task", "task-2"): true},
		publishedTopics,
	)
}

// TestProduceNotificationsPublishFailure an event whose publish fails on any channel is NOT
// stamped, so it is re-published on a later poll.
func TestProduceNotificationsPublishFailure(t *testing.T) {
	utCtx := context.Background()

	config := models.NotificationProducerConfig{
		PollIntervalSecs: 1, BatchSize: 10, EmitFirehose: true,
	}

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	mockRedis := mockredis.NewClient(t)
	uut := newTestProducer(t, config, mockClient, mockRedis)

	good := taskEventEntry(t, models.SystemEventTypeCompleteTask, "task-good", "creator-x")
	bad := taskEventEntry(t, models.SystemEventTypeCompleteTask, "task-bad", "creator-x")

	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(runTxAgainst(mockDatabase))

	mockDatabase.EXPECT().
		ListSystemEvents(mock.Anything, mock.Anything).
		Return([]models.SystemEventAudit{good, bad}, nil).
		Once()

	// The bad event's firehose publish fails; everything else succeeds.
	mockRedis.EXPECT().
		Publish(mock.Anything, mock.MatchedBy(func(msg goutilsRedis.PubSubMessage) bool {
			return msg.Topic == models.BuildNotifyFirehoseChannelName() &&
				payloadID(msg) == bad.ID
		})).
		Return(errors.New("redis down")).
		Once()
	mockRedis.EXPECT().
		Publish(mock.Anything, mock.Anything).
		Return(nil)

	// Only the good event is stamped.
	mockDatabase.EXPECT().
		MarkSystemEventsBroadcast(mock.Anything, []string{good.ID}, mock.Anything).
		Return(nil).
		Once()

	require.NoError(t, uut.produceNotifications(utCtx))
}

// TestProduceNotificationsEmptyPoll an empty batch publishes nothing and stamps nothing.
func TestProduceNotificationsEmptyPoll(t *testing.T) {
	utCtx := context.Background()

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	mockRedis := mockredis.NewClient(t)
	uut := newTestProducer(t, models.NotificationProducerConfig{
		PollIntervalSecs: 1, BatchSize: 10, EmitFirehose: true,
	}, mockClient, mockRedis)

	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(runTxAgainst(mockDatabase))

	mockDatabase.EXPECT().
		ListSystemEvents(mock.Anything, mock.Anything).
		Return([]models.SystemEventAudit{}, nil).
		Once()

	// No Publish, no MarkSystemEventsBroadcast expected — the mocks assert this via NewX(t).
	require.NoError(t, uut.produceNotifications(utCtx))
}

// TestProduceNotificationsPayloadRoundTrip the published payload round-trips back into a
// NotificationEvent carrying the derived routing keys.
func TestProduceNotificationsPayloadRoundTrip(t *testing.T) {
	assert := assert.New(t)
	utCtx := context.Background()

	mockClient := mockdb.NewClient(t)
	mockDatabase := mockdb.NewDatabase(t)
	mockRedis := mockredis.NewClient(t)
	uut := newTestProducer(t, models.NotificationProducerConfig{
		PollIntervalSecs: 1, BatchSize: 10, EmitFirehose: true,
	}, mockClient, mockRedis)

	event := taskEventEntry(t, models.SystemEventTypeActivateTask, "task-rt", "creator-rt")

	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(runTxAgainst(mockDatabase))
	mockDatabase.EXPECT().
		ListSystemEvents(mock.Anything, mock.Anything).
		Return([]models.SystemEventAudit{event}, nil).
		Once()
	mockDatabase.EXPECT().
		MarkSystemEventsBroadcast(mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Once()

	var captured goutilsRedis.PubSubMessage
	mockRedis.EXPECT().
		Publish(mock.Anything, mock.Anything).
		Run(func(_ context.Context, msg goutilsRedis.PubSubMessage) {
			captured = msg
		}).
		Return(nil)

	require.NoError(t, uut.produceNotifications(utCtx))

	payloadStr, err := captured.Message.StringPayload()
	require.NoError(t, err)
	var parsed models.NotificationEvent
	require.NoError(t, json.Unmarshal([]byte(payloadStr), &parsed))

	assert.Equal(event.ID, parsed.ID)
	assert.Equal(models.SystemEventTypeActivateTask, parsed.EventType)
	require.NotNil(t, parsed.Creator)
	assert.Equal("creator-rt", *parsed.Creator)
	require.NotNil(t, parsed.SubjectType)
	assert.Equal("task", *parsed.SubjectType)
	require.NotNil(t, parsed.SubjectID)
	assert.Equal("task-rt", *parsed.SubjectID)
}

// TestNewProducerValidation NewProducer rejects missing required params.
func TestNewProducerValidation(t *testing.T) {
	utCtx := context.Background()

	goodConfig := models.NotificationProducerConfig{PollIntervalSecs: 1, BatchSize: 1}

	// Missing Redis.
	_, err := NewProducer(utCtx, NewProducerParams{
		Persistence: mockdb.NewClient(t), Config: goodConfig,
	})
	assert.Error(t, err)

	// Missing Persistence.
	_, err = NewProducer(utCtx, NewProducerParams{
		Redis: mockredis.NewClient(t), Config: goodConfig,
	})
	assert.Error(t, err)

	// Missing Config.
	_, err = NewProducer(utCtx, NewProducerParams{
		Persistence: mockdb.NewClient(t), Redis: mockredis.NewClient(t),
	})
	assert.Error(t, err)
}

// TestProducerStartStop Start then Stop completes without deadlock.
func TestProducerStartStop(t *testing.T) {
	utCtx := context.Background()

	mockClient := mockdb.NewClient(t)
	mockRedis := mockredis.NewClient(t)

	// The poll may or may not fire before Stop; allow (but don't require) one empty poll.
	mockClient.EXPECT().
		UseDatabaseInTransaction(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ func(context.Context, db.Database) error) error {
			return nil
		}).
		Maybe()

	uut, err := NewProducer(utCtx, NewProducerParams{
		Persistence: mockClient,
		Redis:       mockRedis,
		Config:      models.NotificationProducerConfig{PollIntervalSecs: 1, BatchSize: 1},
	})
	require.NoError(t, err)

	require.NoError(t, uut.Start(utCtx))
	require.NoError(t, uut.Stop(utCtx))
}

// payloadID decode a published message's NotificationEvent id (test helper).
func payloadID(msg goutilsRedis.PubSubMessage) string {
	s, err := msg.Message.StringPayload()
	if err != nil {
		return ""
	}
	var ev models.NotificationEvent
	if err := json.Unmarshal([]byte(s), &ev); err != nil {
		return ""
	}
	return ev.ID
}
