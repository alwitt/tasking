# `notify` — subscribable notifications for the task engine

`notify` turns the task engine's durable audit log into a stream of best-effort
notifications over Redis pub/sub, so interested parties can react to task (and, later,
workflow) state changes without polling the database themselves.

`tasking` is a library embedded by other applications, and this package follows that
posture: it produces notifications and defines the channel/payload conventions, but it
does **not** own multi-tenancy, access control, or leader election — those belong to the
embedding application (see [DESIGN.md](DESIGN.md) for the rationale).

> For the design record — the reliability model, the broadcast-marker loop, the channel
> taxonomy, and the deferred pieces — see [DESIGN.md](DESIGN.md). This README covers what
> the package provides and how to use it.

## What it provides

- **`Producer`** — the component that polls the audit log for un-broadcast events,
  publishes each to its routed Redis channels, and stamps them as broadcast. Constructed
  with `NewProducer`; driven by `Start`/`Stop`.
- **`models.NotificationEvent`** — the JSON wire payload carried on every channel (the
  audit event plus the derived `creator`/`subject` routing keys). It implements
  `goutilsRedis.QueueMessageEnvelope`, so subscribers deserialize it directly.
- **`models.BuildNotify*ChannelName` helpers** — the single source of truth for channel
  names, so producer and subscriber agree on the wire convention without hardcoding
  strings.

## Reliability model (in one paragraph)

Production is **at-least-once and durable**: the audit table is the log, and the producer
resumes from a `broadcast_at IS NULL` marker across restarts, re-broadcasting anything that
occurred (or wasn't stamped) while it was down. Delivery is **best-effort**: Redis pub/sub
drops messages for subscribers that aren't connected at broadcast time. The practical
consequences for a subscriber are two: **duplicates happen** (dedupe on the event `id`),
and **gaps happen** (an offline subscriber misses events — read the audit log directly if
you need catch-up).

## Channels

Every channel carries the identical `NotificationEvent` payload; the channel name is only a
routing selector. Build names via the helpers rather than by hand:

| Helper                                | Channel                                      | Emitted when            |
| ------------------------------------- | -------------------------------------------- | ----------------------- |
| `BuildNotifyFirehoseChannelName()`    | `notify:all`                                 | `EmitFirehose`          |
| `BuildNotifyTypeChannelName(t)`       | `notify:type:<type>`                         | `EmitTypeChan`          |
| `BuildNotifyCreatorChannelName(c)`    | `notify:creator:<creator>`                   | `EmitCreator` + creator |
| `BuildNotifyCreatorTypeChannelName(c,t)` | `notify:creator:<creator>:type:<type>`    | `EmitCreator` + creator |
| `BuildNotifySubjectChannelName(st,sid)` | `notify:subject:<subject-type>:<subject-id>` | always (when derivable) |

The firehose/type/creator families are toggled by `NotificationProducerConfig`; the
subject channel is always emitted for events that have a subject (e.g. task events →
`notify:subject:task:<taskID>`). Creator-less events (e.g. `INVALID_TASK_IPC_MESSAGE`)
reach only the firehose/type channels.

## Running the producer

The embedding application constructs one `Producer` and owns its lifecycle:

```go
producer, err := notify.NewProducer(ctx, notify.NewProducerParams{
    Persistence: dbClient,     // db.Client
    Redis:       redisClient,  // goutilsRedis.Client
    Config: models.NotificationProducerConfig{
        PollIntervalSecs: 5,
        BatchSize:        100,
        EmitFirehose:     true,
        EmitTypeChan:     true,
        EmitCreator:      true,
        // subject channel is always emitted
    },
})
if err != nil {
    return err
}

if err := producer.Start(ctx); err != nil {
    return err
}
defer producer.Stop(ctx) // stops the poll timer and drains within a bounded wait
```

Notes:

- **Single-active by contract.** Running more than one producer against the same audit log
  is safe but wasteful — the broadcast marker prevents loss or corruption, so concurrent
  producers merely double-publish. If you want exactly one, elect a leader in the app.
- **Poison rows don't stall the batch.** An event whose metadata can't be routed is logged
  and left un-broadcast; the rest of the batch proceeds.
- **Partial publish failures re-publish.** If any channel for an event fails to publish,
  that event is not stamped and is retried on the next poll (hence the duplicate contract).

## Consuming notifications

- **`Consumer`** — the subscriber counterpart to `Producer`. It subscribes to a set of
  notification topics, deserializes each received payload into a `models.NotificationEvent`, and
  hands it to a caller-supplied callback. The caller never touches the raw pub/sub envelope.
  Constructed with `NewConsumer`; driven by `Start`/`Stop`.

Topics may be **literal channels or glob patterns** (the underlying subscriber uses `PSUBSCRIBE`),
built via the `BuildNotify*ChannelName` helpers — so a single `Consumer` can follow, say, every
task subject at once with `notify:subject:task:*`.

```go
consumer, err := notify.NewConsumer(ctx, notify.NewConsumerParams{
    Redis: redisClient, // goutilsRedis.Client
    Name:  "workflow-engine-feedback",
    Topics: []string{
        models.BuildNotifySubjectChannelName("task", "*"),        // notify:subject:task:*
        models.BuildNotifyTypeChannelName(models.SystemEventTypeCompleteTask), // a literal channel
    },
    Callback: func(ctx context.Context, event models.NotificationEvent) {
        // return promptly; offload heavy work
    },
})
if err != nil {
    return err
}

if err := consumer.Start(ctx); err != nil {
    return err
}
defer consumer.Stop(ctx) // stops the subscription reader within a bounded wait
```

Notes:

- **Return promptly from the callback.** It is invoked serially from the subscriber's single reader
  goroutine; a slow callback stalls consumption and can drop messages. Offload heavy work (DB
  reconciliation, DAG advancement) onto your own goroutine or queue.
- **Duplicates happen** — production is at-least-once, so the same event may be delivered more than
  once. Dedupe on `event.ID`.
- **Delivery is best-effort** — a `Consumer` offline at broadcast time misses the event. If you need
  catch-up, read the durable, `id`-ordered audit log directly.
- **Undeserializable payloads are dropped**, not fatal — a malformed or foreign message on a
  subscribed channel is logged and skipped; the subscription keeps running.
