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

## Consuming notifications (TBD)

Subscriber-side support is **not yet decided**. Today a consumer can subscribe directly
with the `goutils/redis` PubSub client, using the `BuildNotify*ChannelName` helpers to name
the topics it cares about. Whether `notify` grows its own subscriber helpers — in
particular for **wildcard/pattern** subscriptions, whose signature depends on how
subscription setup actually shakes out — is deferred until there is real consuming code to
shape the API against.

This section will be filled in once that support (if any) exists.
