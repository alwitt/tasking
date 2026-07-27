# `task` — async background task execution engine

`task` is the core of `tasking`: an embedding application submits a **task** — a unit of
background work — and the engine reliably runs it, **retries** it on failure, **times it out**
on a deadline, and **cancels** it on request, surviving process restarts and worker crashes.

`tasking` is a library embedded by other applications, and this package follows that posture:
it orchestrates execution, but it does **not** own multi-tenancy, access control, or scheduler
leader election — those belong to the embedding application (see [DESIGN.md](DESIGN.md) for the
rationale).

> For the design record — the Task vs. TaskExecution distinction, the state machines, the
> reliable-queue IPC model, the maintenance backstop, and the deferred pieces — see
> [DESIGN.md](DESIGN.md). This README covers what the package provides and how to use it.

## The core idea in one paragraph

A **`Task`** is what you submit; a **`TaskExecution`** is one *attempt* at running it. A task
has one or more executions (extra ones come from retries), and **workers process executions,
not tasks**. The engine is split into four components that never call each other directly —
they exchange messages over Redis IPC queues and share a database. The **database is the
source of truth**; every IPC message is a best-effort *poke* that lets a component act sooner
than the periodic maintenance scan would. Lose a poke and work is *delayed*, never *lost*.

## Components

| Component   | What it does                                                        | Lifecycle                     |
| ----------- | ------------------------------------------------------------------- | ----------------------------- |
| `Client`    | Submission API — define tasks, request cancellation                 | construct, then call          |
| `Scheduler` | The state machine — owns every Task/TaskExecution transition        | `Start` / `Stop`; single-active |
| `Receiver`  | Per-worker-host consumer — claims executions, runs them, reports back | `Initialize` → `Start` → `Stop` |
| `Executor`  | Worker pool for one queue — runs your `TaskExecutionProcessor`      | created started; `Stop`       |

You wire these together in your application, injecting the Redis-backed factory callbacks
(`common.NewRedisIPCMessage*`, `NewExecutor`). Tests inject mocks instead.

## Submitting work (`Client`)

```go
client, err := task.NewClient(ctx, task.NewClientParams{
    Name:             "my-app",
    DefaultCreator:   "my-app",          // opaque; the notify routing key
    Persistence:      dbClient,          // db.Client
    Config:           clientConfig,      // models.TaskClientConfig
    Redis:            redisClient,       // goutilsRedis.Client
    IPCSenderFactory: common.NewRedisIPCMessageSend,
})

// Immediate one-shot
t, err := client.DefineAndRunImmediateOneShotTask(ctx, task.DefineTaskParams{
    Name:       "resize-image",          // matched to a registered processor
    Parameters: myParams,                // arbitrary, JSON-serialized
    Deadline:   &someDeadline,           // optional
}, nil /* or an active db.Database txn to join */)

// Scheduled one-shot
t, err = client.DefineAndRunScheduledOneShotTask(ctx, params, targetRunTime, nil)

// Cancel
err = client.CancelTask(ctx, t.ID, nil)
```

**Error contract** (inspect with `errors.As`):

- `models.PersistenceError` — the task row was **not** created; nothing happened.
- `models.IPCMessageQueueError` — the task row **was** created, but the scheduler poke
  failed. The task is not lost: the scheduler's maintenance sweep will pick it up. (Same
  contract for `CancelTask`.)

## Running work (`Receiver` + `Executor` + a processor)

Implement `models.TaskExecutionProcessor` for each task name and let the receiver drive it.
Processors are supplied **declaratively** at construction — there is no runtime registration call.
You give the receiver a per-queue `task name → processor` map (`NewReceiverParams.Processors`); the
receiver hands each queue's inner map to that queue's `Executor` through your `ExecutorFactory`,
which just forwards it to `task.NewExecutor`. The map is fixed at construction and immutable
thereafter:

```go
type resizeProcessor struct{}

func (p resizeProcessor) ProcessTaskExecution(
    ctx context.Context, taskEntry models.Task, exec models.TaskExecution,
) error {
    // do the work; honor ctx (it carries the execution's deadline)
    return nil // or an error → the execution FAILS (and may be retried)
}

// Per-queue processor mapping: queue name → (task name → processor). Every key must be a queue
// this receiver is configured to serve; a nil processor is rejected up front by NewExecutor.
processors := map[string]map[string]models.TaskExecutionProcessor{
    "default-queue": {
        "resize-image": resizeProcessor{},
    },
}

// The factory just forwards the per-queue processor map to NewExecutor.
executorFactory := func(
    parentCtx context.Context, queue string, workers, bufLen int,
    support task.ExecutorSupport, queueProcessors map[string]models.TaskExecutionProcessor,
) (task.Executor, error) {
    return task.NewExecutor(parentCtx, queue, workers, bufLen, support, queueProcessors)
}

receiver, err := task.NewReceiver(ctx, task.NewReceiverParams{
    Support:            task.ExecutorSupport{Persistence: dbClient /* OnCompleteCB set internally */},
    Config:             receiverConfig,     // models.TaskReceiverConfig — must configure "default-queue"
    ExecutorFactory:    executorFactory,
    Processors:         processors,         // per-queue task-name → processor mapping
    Redis:              redisClient,
    IPCReceiverFactory: common.NewRedisIPCMessageReceive,
    IPCSenderFactory:   common.NewRedisIPCMessageSend,
})

// Reconcile buffered work, then start consuming:
if err := receiver.Initialize(ctx, nil); err != nil { /* ... */ }
if err := receiver.Start(ctx); err != nil { /* ... */ }
defer receiver.Stop(ctx)
```

`Initialize` **must** run before `Start`: it reconciles any execution requests left buffered
by a previous run (re-queue, fail, or ignore, based on their DB state) so a crash doesn't
strand work.

## Running the scheduler

Exactly **one** scheduler runs against a given database (it is the sole writer of state):

```go
scheduler, err := task.NewScheduler(ctx, task.NewSchedulerParams{
    Persistence:        dbClient,
    Config:             schedulerConfig,   // models.TaskSchedulerConfig
    Redis:              redisClient,
    IPCReceiverFactory: common.NewRedisIPCMessageReceive,
    IPCSenderFactory:   common.NewRedisIPCMessageSend,
})
if err := scheduler.Start(ctx); err != nil { /* ... */ }
defer scheduler.Stop(ctx)
```

`Start` first recovers any messages stranded in the scheduler queue buffer, then starts the
worker, the maintenance timer, and the queue consumer.

## Reliability model (what you can rely on)

- **At-least-once, self-healing execution.** Work is driven by IPC pokes *and* a periodic
  maintenance sweep that re-scans the DB for anything a lost poke would strand. A dropped
  message delays work; it never loses it. Scheduled and retry executions fire from this sweep.
- **Crash-safe queues.** IPC uses reliable Redis queues: a message stays buffered until the
  reader is done with it, and both consumers reconcile their buffers at startup.
- **Idempotent handlers.** Duplicate or racing deliveries collapse to safe no-ops.
- **Poison messages are quarantined.** Unprocessable messages become
  `INVALID_TASK_IPC_MESSAGE` audit events and are dropped — never a crash loop.
- **Retries with backoff.** Failed executions retry per the task's policy (exponential,
  clamped); exhausting retries fails the task.
- **Engine failures don't retry.** If the framework itself can't run an instance
  (`ENGINE_FAILED`), the task fails and an audit event is written for the app to review.

## Configuration

- **`models.TaskClientConfig`** — scheduler queue name; optional per-task retry overrides.
- **`models.TaskSchedulerConfig`** — maintenance interval (≥ 10s); scheduler queue name;
  task-name → execution-queue mappings.
- **`models.TaskReceiverConfig`** — receiver name; served queues (worker count + buffer
  length each); scheduler queue name.

The task-name → queue mapping is the routing fabric: the scheduler dispatches an execution to
the queue configured for its task name, and the receiver serving that queue runs it.

## Notifications

Every state change is recorded as a durable, creator-tagged `SystemEventAudit` row in the
same transaction as the change. The [`notify`](../notify/README.md) package turns those rows
into a best-effort Redis pub/sub notification stream. This package produces the events; it
does not broadcast them.

## Not yet supported

Periodic/recurring tasks (only immediate and scheduled one-shot exist today), multi-tenancy
isolation, and scheduler leader election are out of scope here — see
[DESIGN.md §11](DESIGN.md).
