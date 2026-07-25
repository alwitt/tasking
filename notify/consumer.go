package notify

import (
	"context"
	"encoding/json"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
)

// NotificationCallback receives each deserialized notification.
//
// ctx is the subscriber's working context - it is cancelled when the Consumer is stopped, so a
// long-running callback can observe it and bail out early. The callback is invoked serially (from
// the underlying subscriber's single reader goroutine) and MUST return promptly: a slow callback
// stalls consumption and can cause messages to be dropped. Offload heavy work (DB reconciliation,
// DAG advancement) onto the callback owner's own goroutine or queue.
type NotificationCallback func(ctx context.Context, event models.NotificationEvent)

// Consumer notification consumer - the subscriber counterpart to Producer.
//
// A Consumer subscribes to a set of notification topics (literal channels or glob patterns, e.g.
// notify:subject:task:*) and delivers each received notification, deserialized into a
// models.NotificationEvent, to a caller-supplied callback. It owns the PubSubMessage ->
// NotificationEvent conversion internally, so the caller never touches the raw pub/sub envelope.
//
// Delivery inherits the framework's two contracts: duplicates happen (dedupe on event.ID) and
// delivery is best-effort (a Consumer offline at broadcast time simply misses the event; catch up
// by reading the audit log directly). See notify/DESIGN.md.
type Consumer interface {
	/*
		Start the notification consumer's subscription reader

			@param ctx context.Context - execution context
	*/
	Start(ctx context.Context) error

	/*
		Stop the notification consumer's subscription reader

			@param ctx context.Context - execution context
	*/
	Stop(ctx context.Context) error
}

// consumerImpl implements Consumer.
//
// Unlike producerImpl it keeps no WaitGroup and spawns no goroutine of its own: the underlying
// goutilsRedis.Subscriber owns the single reader goroutine and its own bounded Stop-drain.
type consumerImpl struct {
	goutils.Component

	callback   NotificationCallback
	subscriber goutilsRedis.Subscriber

	workerCtx       context.Context
	workerCtxCancel context.CancelFunc
}

// NewConsumerParams init parameters for a notification consumer
type NewConsumerParams struct {
	// Redis REDIS client
	Redis goutilsRedis.Client `validate:"required"`
	// Topics the notification topics to subscribe on. Literal channels or glob patterns (the
	// underlying subscriber uses PSUBSCRIBE), built via the models.BuildNotify*ChannelName helpers.
	Topics []string `validate:"required,min=1,dive,required"`
	// Callback invoked for each deserialized notification
	Callback NotificationCallback `validate:"required"`
	// Name subscriber name for the underlying goutils Subscriber (log/debug identity)
	Name string `validate:"required"`
}

/*
NewConsumer define a new notification consumer

	@param parentCtx context.Context - the parent execution context
	@param params NewConsumerParams - parameters of the notification consumer
	@returns new notification consumer
*/
func NewConsumer(parentCtx context.Context, params NewConsumerParams) (Consumer, error) {
	logTags := log.Fields{
		"package": "tasking", "module": "notify", "component": "consumer", "instance": params.Name,
	}

	validate := validator.New()
	if err := models.RegisterWithValidator(validate); err != nil {
		return nil, goutils.NewRuntimeError(
			"failed to install custom validation macros", err, true,
		)
	}

	if err := validate.Struct(&params); err != nil {
		return nil, goutils.NewBadInputError("notification consumer param is invalid", err, true)
	}

	instance := &consumerImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		callback: params.Callback,
	}
	instance.workerCtx, instance.workerCtxCancel = context.WithCancel(parentCtx)

	// Prepare the subscription runner (topics fixed at subscribe time).
	subscriber, err := params.Redis.Subscribe(instance.workerCtx, params.Name, params.Topics)
	if err != nil {
		instance.workerCtxCancel()
		return nil, models.NewNotifyConsumerError(
			"failed to define notification subscription", err, true,
		)
	}
	instance.subscriber = subscriber

	return instance, nil
}

/*
Start the notification consumer's subscription reader

	@param ctx context.Context - execution context
*/
func (c *consumerImpl) Start(_ context.Context) error {
	if err := c.subscriber.Start(c.workerCtx, c.onMessage); err != nil {
		return models.NewNotifyConsumerError("failed to start notification subscription", err, true)
	}
	return nil
}

/*
Stop the notification consumer's subscription reader

	@param ctx context.Context - execution context
*/
func (c *consumerImpl) Stop(ctx context.Context) error {
	logTags := c.GetLogTagsForContext(ctx)

	// The subscriber owns the reader goroutine and a bounded drain; stop it first, then cancel the
	// working context so any in-flight callback observing it can unblock.
	if err := c.subscriber.Stop(ctx); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("Failed to stop notification subscription")
	}

	c.workerCtxCancel()
	return nil
}

/*
onMessage the intermediate PubSubMessageHandler handed to the subscriber: deserialize the received
pub/sub payload into a NotificationEvent, then invoke the caller's callback.

A payload that cannot be deserialized (malformed, or a foreign message on a subscribed channel) is
logged and dropped - it must never tear down the subscription, mirroring the producer's poison-row
posture. Delivery is best-effort; a dropped junk message is no worse than a missed one.

	@param ctx context.Context - the subscriber's working context
	@param msg goutilsRedis.PubSubMessage - the received pub/sub message
*/
func (c *consumerImpl) onMessage(ctx context.Context, msg goutilsRedis.PubSubMessage) {
	logTags := c.GetLogTagsForContext(ctx)

	payload, err := msg.Message.StringPayload()
	if err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Errorf("Dropping notification on channel %s: payload unreadable", msg.Topic)
		return
	}

	var event models.NotificationEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Errorf("Dropping notification on channel %s: undeserializable payload", msg.Topic)
		return
	}

	c.callback(ctx, event)
}
