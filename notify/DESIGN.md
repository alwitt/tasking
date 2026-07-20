# Task Engine — Subscribable Notification Framework (Design)

> Status: first draft. This document is the authoritative design record for the
> notification framework. Update this file (not scattered notes) when decisions change.

## 1. Motivation & context

Applications that submit tasks want to know when those tasks change state
(completed, failed, timed out, cancelled). The task engine already records every
meaningful state change as a durable `SystemEventAudit` row, written **in the same
DB transaction** as the state change (`db.updateTaskState`). This design adds a
**best-effort, Redis pub/sub notification layer on top of that durable audit log**,
so interested parties can subscribe to updates.

`tasking` is a **library** (an async task and — soon — workflow engine) embedded by
other projects. Two consequences run through every decision:

- It **cannot dictate cardinality** of creators or impose **multi-tenancy** — those
  belong to the embedding application. Creator is an opaque string; channel names are
  conventions, not access-controlled.
- The **workflow engine** (next major piece, *not* designed here) is a first-class
  consumer: it needs task state-change feedback and will emit its own audit events. So
  the framework is designed **subject-generalized**, not task-specific.

## 2. Architecture overview

```
  task/workflow state change
        │  (same DB txn)
        ▼
  ┌──────────────────┐     durable, append-only, enriched
  │ audit table      │     rows: id(ULID PK), type,
  │ (source of truth)│     metadata{creator,subject}, broadcast_at
  └──────────────────┘
        │  poll WHERE broadcast_at IS NULL ORDER BY id
        ▼
  ┌──────────────────┐     library component, app-wired,
  │ NotificationProd │     single-active by contract
  │  (new)           │     at-least-once production
  └──────────────────┘
        │  PUBLISH (native Redis pub/sub, new goutils API)
        ▼
   Redis channels  ──▶  subscribers (apps, workflow engine, ops)
   best-effort delivery; offline subscriber misses events
```

**Two-tier reliability split:**

- **Production is at-least-once & durable** — the audit table is the log; the producer
  resumes from the `broadcast_at IS NULL` marker across restarts and will re-broadcast
  events that occurred while it was down.
- **Delivery is best-effort** — Redis pub/sub drops messages for subscribers that
  aren't currently connected. A subscriber offline during a broadcast simply misses it.

## 3. Data-model changes

### 3.1 `Task.Creator`

```go
// Creator opaque identity of the entity that created this task. Set by the
// submitting Client. tasking never interprets it; multi-tenancy/isolation is
// the embedding application's responsibility.
Creator string `json:"creator" gorm:"column:creator;index" validate:"required"`
```

Threaded `Client → NewTaskParameter → models.Task`. Set via **client-level default +
per-task override** (§5).

### 3.2 `SystemEventAudit` additions

```go
// BroadcastAt when the notification producer broadcast this event. NULL until
// broadcast. The producer's work-queue marker: SELECT … WHERE broadcast_at IS NULL.
BroadcastAt *time.Time `json:"broadcast_at,omitempty" gorm:"column:broadcast_at;index;default:null"`
```

> Note on ordering: the audit `id` is a **ULID** (`primaryKey`), which is
> lexicographically sortable by generation time and carries a monotonic component,
> so `ORDER BY id` already yields a deterministic total order and a stable
> intra-batch tie-breaker — no dedicated sequence column is needed. Correctness of
> the producer never depends on this ordering: it resumes from the
> **broadcast-marker** cursor (`broadcast_at IS NULL`, §4.2), which is immune to the
> "commit-out-of-order" race that a high-water-mark cursor over `id` (or
> `created_at`) would suffer — a row with a lower `id` can commit *after* a
> higher-`id` row is already visible, so a high-water-mark would skip it. The marker
> column sidesteps this entirely; `ORDER BY id` is used only for a stable timeline
> within the fetched batch.

### 3.3 Event metadata enrichment (creator routing key on the row)

The producer must derive an event's routing keys from the audit row alone (no joins).
Those keys are **creator** and **subject** — but they are supplied differently:

- **Creator** is *data* the producer cannot otherwise know, so it is denormalized onto
  the event metadata at write time. `updateTaskState` already holds the full task
  `entry` (via `getTaskDBEntry`), so `creator` is in scope at no extra cost:

  ```go
  type SystemEventTaskEvents struct {
      TaskID  string `json:"task_id" validate:"required"`
      Creator string `json:"creator"` // new — routing key
  }
  ```

  The engine-failure event carries `TaskID` too and must route to the creator the
  same way, so it gains the same field:

  ```go
  type SystemEventEngineFailedTask struct {
      TaskID     string `json:"task_id" validate:"required"`
      InstanceID string `json:"instance_id" validate:"required"`
      Reason     string `json:"reason"`
      Creator    string `json:"creator"` // new — routing key
  }
  ```

  Wherever these events are recorded, the writer must have the task `entry` in scope
  to populate `Creator` (the same `getTaskDBEntry` source used for task-state events).

- **Subject** is *not* stored on the event. For task events the subject is fully
  determined by the event: `subject_type` is always `"task"` (implied by the
  `SystemEventType…` event type) and `subject_id` is always the `TaskID` already in the
  metadata. Denormalizing them would just restate the event type and duplicate
  `TaskID`. Instead, the **producer derives the subject** from `EventType` + metadata
  (§4.3). When workflow events arrive, their own metadata type carries `workflow_id`,
  and the producer maps that event-type family to `subject:workflow:<id>` the same way
  it maps task event types to `subject:task:<TaskID>`.

Creator-less events (e.g. `INVALID_TASK_IPC_MESSAGE`) simply omit `creator` and only
reach the firehose/type channels (§4.3).

Enrichment is written in the **same transaction** as the state change (unchanged from
today), so an event and its creator commit atomically.

## 4. The notification producer

A new library component `notify.Producer` (or `task.NotificationProducer`), sibling to
`Scheduler`/`Client`, **wired and lifecycle-owned by the embedding app**.

### 4.1 Construction & lifecycle

```go
type NewProducerParams struct {
    Persistence db.Client                         `validate:"required"`
    Redis       goutilsRedis.Client               `validate:"required"`
    Config      models.NotificationProducerConfig `validate:"required"`
}
// Start(ctx)/Stop(ctx), same pattern as schedulerImpl.
```

The producer depends on the `goutilsRedis.Client` **directly** and calls its `Publish`
(§6) — no intermediate `common.NotifyPublisher` wrapper. (An early draft proposed such a
wrapper; it was dropped as unnecessary indirection since the producer is the sole publisher
and tests mock `goutilsRedis.Client` directly.)

Driven by an interval timer (like the scheduler's maintenance timer) plus a bounded
batch size per poll.

### 4.2 The loop (broadcast-marker)

```
every PollInterval:
  batch = persistence: SELECT * FROM audit
          WHERE broadcast_at IS NULL ORDER BY id LIMIT BatchSize
  published = []
  for event in batch:
      channels = routeChannels(event, config)        // §4.3
      ok = true
      for ch in channels:
          if Redis.Publish(ctx, PubSubMessage{Topic: ch, Message: payload(event)}) fails:
              ok = false                              // logged; retried next poll
      if ok: published.append(event.id)
  // batch-stamp all cleanly-published events in one call, after the loop
  persistence: MarkSystemEventsBroadcast(published, now())   // AND broadcast_at IS NULL
```

**Stamp point**: `broadcast_at` is stamped **once per poll, after the batch loop**, via
`MarkSystemEventsBroadcast(published, now())` — a single `UPDATE … WHERE id IN (…) AND
broadcast_at IS NULL`. Only events that published cleanly on **every** routed channel are
included; an event whose publish failed on any channel is left un-broadcast and re-published
next poll (subscribers dedupe on `id`). A crash after publishing but before the stamp
re-publishes that whole batch next run.

- **Race-immune by construction**: a late-committing lower-`id` row is still `NULL`
  and is picked up on a later poll. No cursor to overshoot.
- **At-least-once / crash window**: if the process dies *after* `Publish` but *before*
  the stamp, the row is re-selected and **re-published** next run. Duplicates are
  expected. → **Subscriber contract: notifications are idempotent; dedupe by event
  `id`.**
- **Ordering on the wire**: within a batch, `ORDER BY id`; a late row lands in a later
  batch, so there is **no strict global ordering** guarantee. Fine for best-effort
  task/workflow signals; documented so no one assumes total order.
- **Single-active by contract**: two concurrent producers merely double-publish (the
  marker prevents corruption/loss). Leader election, if wanted, is the app's concern —
  mirroring how `tasking` pushes multi-tenancy up to the application.

### 4.3 Channel set (subject-generalized, configurable)

Identical payload on every channel; the channel is only a routing selector. Per event,
emit to the **enabled** families among:

| Channel                                        | Emitted when          | Lens                                                |
| ---------------------------------------------- | --------------------- | --------------------------------------------------- |
| `notify:all`                                   | always                | firehose / debugging / bridges                      |
| `notify:type:<EVENT_TYPE>`                     | always                | ops / monitoring                                    |
| `notify:creator:<creator>`                     | event has a creator   | "my app's stuff"                                    |
| `notify:creator:<creator>:type:<EVENT_TYPE>`   | event has a creator   | creator ∩ type (e.g. *my failures*)                 |
| `notify:subject:<subject-type>:<subject-id>`   | always (subject req.) | the specific task/workflow — workflow feedback path |

The producer computes `<subject-type>`/`<subject-id>` from the event's `EventType` +
metadata (§3.3) — they are not stored on the row. `subject` is effectively always-on
(it is the internal integration seam for the workflow engine); the other families
toggle via config:

```go
type NotificationProducerConfig struct {
    PollInterval  time.Duration
    BatchSize     int
    EmitFirehose  bool   // notify:all
    EmitTypeChan  bool   // notify:type:*
    EmitCreator   bool   // notify:creator:* and notify:creator:*:type:*
    // subject always emitted
}
```

Creator-less events (no task, e.g. `INVALID_TASK_IPC_MESSAGE`) reach only `notify:all`
and `notify:type:*`. `ENGINE_FAILED_TASK` has a task (hence a creator, once enriched)
and reaches the creator/composite channels too.

### 4.4 Payload

The payload is a **`models.NotificationEvent`**: the full event (`id`, `type`,
`created_at`, the typed `metadata`) enriched with the derived routing keys — `creator` and
`subject_type`/`subject_id`. The routing keys are optional pointers (absent ⇒ omitted from
the JSON) and are computed at broadcast time, not stored on the audit row. Subscribers may
filter further client-side even after channel-level routing.

`models.NotificationEvent` implements `goutilsRedis.QueueMessageEnvelope` (a `StringPayload()`
that JSON-marshals it), so it is carried directly as the `PubSubMessage.Message` envelope
(§6); subscribers deserialize it in their handler.

## 5. Creator API (`Client`)

- `NewClientParams` gains `DefaultCreator string`.
- Submit methods accept an **optional per-task override** (`creator *string`; `nil` ⇒
  client default), flowing into `NewTaskParameter.Creator → Task.Creator`. `CancelTask`
  needs no creator (routing comes from the task's stored creator).
- One-creator-per-Client is the easy path; many creators through one Client remains
  expressible (satisfies the "cardinality may grow" constraint).

## 6. `goutils/redis` PubSub (implemented)

Native Redis PubSub has been **added to `goutils/redis.Client`** The landed API — which this
framework is the first consumer of — is:

```go
// PubSubMessage is the publish/deliver envelope; Message is the existing
// QueueMessageEnvelope (a string payload), so the notification payload (§4.4) is
// carried as its serialized JSON.
type PubSubMessage struct {
    Topic   string
    Message QueueMessageEnvelope
}

// Publish onto a topic (best-effort; zero subscribers → silent no-op success).
Publish(ctx context.Context, msg PubSubMessage) error

// Subscribe returns a Subscriber runner bound to a fixed set of topics.
Subscribe(ctx context.Context, subName string, topics []string) (Subscriber, error)
```

The `Subscriber` is a **callback-driven runner**, not a channel:

```go
type PubSubMessageHandler func(ctx context.Context, msg PubSubMessage)

type Subscriber interface {
    Start(parentCtx context.Context, handler PubSubMessageHandler) error
    Stop(ctx context.Context) error
}
```

Behavioral contract relevant to this framework:

- **Callback, invoked serially** from the subscriber's single reader goroutine — no
  per-subscriber locking needed for ordering, and no second drain goroutine to babysit.
- The handler **must return promptly**; a slow handler stalls consumption and can drop
  messages. Heavy work (DB reconciliation, DAG advancement) is offloaded by the consumer.
- The handler receives the subscriber's **working context**, cancelled on `Stop`, so a
  long-running handler can bail out on shutdown.
- The runner is **cancellation-responsive** (clean `Stop`), **closes the subscription**
  on exit, **rejects a nil handler**, and **isolates a panicking handler** via `recover`
  so one bad callback can't tear down the subscriber.
- **Topics are fixed at `Subscribe`** time. The workflow engine (§7), whose subject set
  changes as steps go live/complete, therefore manages a **set of `Subscriber`s** (or a
  wildcard/pattern strategy) rather than mutating one subscription — noted for that
  later design.

`tasking` still wraps these behind a small `common.NotifyPublisher` / subscriber helper
so the engine depends on an interface, not `goutils` directly (mockable — a generated
`Subscriber` mock and a `UnitTestCallbackCollector` handler mock already exist in
`goutils` — and consistent with the existing `IPCMessageSend`/`IPCMessageReceive`).

## 7. Workflow integration (context only — designed later)

- The workflow engine **subscribes `notify:subject:task:<taskID>`** for each task
  backing a live step to get state-change feedback (fast path). Because `Subscribe`
  fixes its topic set (§6), the engine either manages one `Subscriber` per live step or
  subscribes once to the broader `notify:type:*`/`notify:all` firehose and filters in its
  handler — a trade-off to settle in the workflow design.
- Because delivery is best-effort, correctness rests on a **DB reconciliation
  backstop**: the workflow engine periodically reads live step-tasks' states directly
  and advances the DAG, catching any dropped notification. A dropped "task completed"
  delays, never stalls, a workflow.
- Workflow audit events flow through the **same** producer with `subject:workflow:<id>`
  routing; workflow creators subscribe by creator/subject like task creators. Full
  workflow design is a separate session.

## 8. Non-goals / explicit deferrals

- **Migrations** — this doc states the target schema only; schema evolution is handled
  near release.
- **Durable/replayable delivery, per-subscriber cursors, ordered guarantees** — out of
  scope by the best-effort choice. Subscribers needing catch-up read the audit log
  directly (it is durable and `id`-ordered).
- **Channel-level auth / multi-tenancy isolation** — the embedding application's
  responsibility.
- **Producer leader election** — the app's concern; the framework is safe
  (double-publish only, never loss/corruption) if run multiple times.
