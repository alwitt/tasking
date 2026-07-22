// Package notify - subscribable notification framework for the task engine.
//
// The producer turns the durable audit log into best-effort Redis pub/sub notifications: an
// interval timer periodically polls the audit table for events not yet broadcast, publishes
// each to its routed channels, then stamps them as broadcast. Production is at-least-once
// (the broadcast marker survives restarts and re-publishes the crash window); delivery is
// best-effort (Redis pub/sub drops messages for offline subscribers). See notify/DESIGN.md.
package notify

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// Producer notification producer
type Producer interface {
	/*
		Start the notification producer's poll loop

			@param ctx context.Context - execution context
	*/
	Start(ctx context.Context) error

	/*
		Stop the notification producer's poll loop

			@param ctx context.Context - execution context
	*/
	Stop(ctx context.Context) error
}

// producerImpl implements Producer
type producerImpl struct {
	goutils.Component
	validator *validator.Validate

	config models.NotificationProducerConfig

	persistence db.Client
	redis       goutilsRedis.Client

	wg              *sync.WaitGroup
	workerCtx       context.Context
	workerCtxCancel context.CancelFunc

	pollTimer goutils.IntervalTimer
}

// NewProducerParams init parameters for a notification producer
type NewProducerParams struct {
	// Persistence persistence client
	Persistence db.Client `validate:"required"`
	// Config notification producer config
	Config models.NotificationProducerConfig `validate:"required"`
	// Redis REDIS client
	Redis goutilsRedis.Client `validate:"required"`
}

/*
NewProducer define a new notification producer

	@param parentCtx context.Context - the parent execution context
	@param params NewProducerParams - parameters of the notification producer
	@returns new notification producer
*/
func NewProducer(parentCtx context.Context, params NewProducerParams) (Producer, error) {
	logTags := log.Fields{"package": "tasking", "module": "notify", "component": "producer"}

	validate := validator.New()
	if err := models.RegisterWithValidator(validate); err != nil {
		return nil, goutils.NewRuntimeError(
			"failed to install custom validation macros", err, true,
		)
	}

	if err := validate.Struct(&params); err != nil {
		return nil, goutils.NewBadInputError("notification producer param is invalid", err, true)
	}

	instance := &producerImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		validator:   validate,
		config:      params.Config,
		persistence: params.Persistence,
		redis:       params.Redis,
		wg:          &sync.WaitGroup{},
	}
	instance.workerCtx, instance.workerCtxCancel = context.WithCancel(parentCtx)

	// Prepare the poll timer
	var err error
	instance.pollTimer, err = goutils.GetIntervalTimerInstance(
		instance.workerCtx, instance.wg, log.Fields{
			"package":       "tasking",
			"module":        "notify",
			"component":     "producer",
			"sub-component": "poll-timer",
		},
	)
	if err != nil {
		return nil, models.NewNotifyProducerError(
			"failed to define notification poll timer", err, true,
		)
	}

	return instance, nil
}

/*
Start the notification producer's poll loop

	@param ctx context.Context - execution context
*/
func (p *producerImpl) Start(_ context.Context) error {
	if err := p.pollTimer.Start(p.config.PollInterval(), func() error {
		return p.produceNotifications(p.workerCtx)
	}, false); err != nil {
		return models.NewNotifyProducerError("failed to start notification poll timer", err, true)
	}
	return nil
}

/*
Stop the notification producer's poll loop

	@param ctx context.Context - execution context
*/
func (p *producerImpl) Stop(ctx context.Context) error {
	logTags := p.GetLogTagsForContext(ctx)

	if err := p.pollTimer.Stop(); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to stop notification poll timer")
	}

	p.workerCtxCancel()
	return goutils.TimeBoundedWaitGroupWait(ctx, p.wg, time.Second*5)
}

/*
produceNotifications one poll cycle: fetch a batch of un-broadcast audit events, publish each
to its routed channels, then stamp the successfully-published events as broadcast.

A publish failure for one channel is logged and that event is dropped from the stamp set, so
it is re-published on a later poll (delivery is best-effort, production at-least-once). A
listing or stamping failure aborts the cycle; the batch is re-polled on the next tick.

	@param ctx context.Context - execution context
*/
func (p *producerImpl) produceNotifications(ctx context.Context) error {
	logTags := p.GetLogTagsForContext(ctx)

	batchSize := p.config.BatchSize

	// Fetch the next batch of events not yet broadcast (ordered by id).
	var batch []models.SystemEventAudit
	if dbErr := p.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			var err error
			batch, err = dbClient.ListSystemEvents(dbCtx, db.SystemEventQueryFilter{
				CommonListEntryQueryFilter: db.CommonListEntryQueryFilter{Limit: &batchSize},
				OnlyNotBroadcast:           true,
			})
			if err != nil {
				return models.NewPersistenceError("failed to list un-broadcast events", err, true)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewNotifyProducerError("failed to poll audit log", dbErr, true)
	}

	if len(batch) == 0 {
		return nil
	}

	// Publish each event to its routed channels; collect the IDs that published cleanly.
	publishedIDs := make([]string, 0, len(batch))
	for _, event := range batch {
		channels, payload, err := p.routeEvent(event)
		if err != nil {
			// The event can't be routed (bad/unknown metadata); log and skip so a poison
			// row does not stall the whole batch. It stays un-broadcast for later review.
			log.
				WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf("Failed to route notification for event %s; skipping", event.ID)
			continue
		}

		publishErr := false
		for _, channel := range channels {
			if err := p.redis.Publish(ctx, goutilsRedis.PubSubMessage{
				Topic: channel, Message: payload,
			}); err != nil {
				publishErr = true
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf(
						"Failed to publish event %s on channel %s; will retry next poll",
						event.ID, channel,
					)
			}
		}

		// Only stamp the event if all of its channels published cleanly, so a partial
		// failure re-publishes the whole event next poll (subscribers dedupe on id).
		if !publishErr {
			publishedIDs = append(publishedIDs, event.ID)
		}
	}

	if len(publishedIDs) == 0 {
		return nil
	}

	// Stamp the successfully-published events as broadcast.
	broadcastAt := time.Now().UTC()
	if dbErr := p.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			if err := dbClient.MarkSystemEventsBroadcast(dbCtx, publishedIDs, broadcastAt); err != nil {
				return models.NewPersistenceError("failed to mark events broadcast", err, true)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewNotifyProducerError("failed to stamp broadcast events", dbErr, true)
	}

	return nil
}

/*
routeEvent compute the set of pub/sub channels an event fans out to (§4.3, config-gated) and
build the shared payload (§4.4).

	@param event models.SystemEventAudit - the audit event to route
	@return the channels to publish on, the payload to publish, and any routing error
*/
func (p *producerImpl) routeEvent(
	event models.SystemEventAudit,
) ([]string, models.NotificationEvent, error) {
	creator, err := p.deriveCreator(event)
	if err != nil {
		return nil, models.NotificationEvent{}, err
	}
	subjectType, subjectID, hasSubject := p.deriveSubject(event)

	channels := []string{}

	if p.config.EmitFirehose {
		channels = append(channels, models.BuildNotifyFirehoseChannelName())
	}
	if p.config.EmitTypeChan {
		channels = append(channels, models.BuildNotifyTypeChannelName(event.EventType))
	}
	if p.config.EmitCreator && creator != "" {
		channels = append(
			channels,
			models.BuildNotifyCreatorChannelName(creator),
			models.BuildNotifyCreatorTypeChannelName(creator, event.EventType),
		)
	}
	if hasSubject {
		channels = append(
			channels, models.BuildNotifySubjectChannelName(subjectType, subjectID),
		)
	}

	payload := models.NotificationEvent{
		ID:        event.ID,
		EventType: event.EventType,
		CreatedAt: event.CreatedAt,
		Metadata:  json.RawMessage(event.Metadata),
	}
	if creator != "" {
		payload.Creator = &creator
	}
	if hasSubject {
		payload.SubjectType = &subjectType
		payload.SubjectID = &subjectID
	}

	return channels, payload, nil
}

/*
deriveCreator extract the routing creator from an event's metadata, empty when the event
type carries no creator (e.g. INVALID_TASK_IPC_MESSAGE).

	@param event models.SystemEventAudit - the audit event
	@return the creator, or "" when the event has none
*/
func (p *producerImpl) deriveCreator(event models.SystemEventAudit) (string, error) {
	parsed, err := event.ParseMetadata(p.validator)
	if err != nil {
		return "", err
	}
	switch meta := parsed.(type) {
	case models.SystemEventTaskEvents:
		return meta.Creator, nil
	case models.SystemEventEngineFailedTask:
		return meta.Creator, nil
	case models.SystemEventWorkflowEvents:
		return meta.Creator, nil
	case models.SystemEventWorkflowStepEvents:
		return meta.Creator, nil
	default:
		return "", nil
	}
}

/*
deriveSubject map an event to the subject it is about (§4.3). Task-bearing events are about
the task; creator-less events (e.g. INVALID_TASK_IPC_MESSAGE) have no subject.

	@param event models.SystemEventAudit - the audit event
	@return subject type, subject id, and whether a subject was derivable
*/
func (p *producerImpl) deriveSubject(event models.SystemEventAudit) (string, string, bool) {
	parsed, err := event.ParseMetadata(p.validator)
	if err != nil {
		return "", "", false
	}
	switch meta := parsed.(type) {
	case models.SystemEventTaskEvents:
		return "task", meta.TaskID, true
	case models.SystemEventEngineFailedTask:
		return "task", meta.TaskID, true
	case models.SystemEventWorkflowEvents:
		return "workflow", meta.WorkflowID, true
	case models.SystemEventWorkflowStepEvents:
		// A step event is about its parent workflow (subject:workflow:<id>), per DESIGN §7.
		return "workflow", meta.WorkflowID, true
	default:
		return "", "", false
	}
}
