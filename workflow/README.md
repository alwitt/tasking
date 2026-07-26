# `workflow` — DAG workflow execution engine

`workflow` runs long-running, multi-step **DAG workflows** on top of the
[`task`](../task/README.md) engine. An embedding application submits a **workflow** — a set of
steps with dependency edges — and the engine drives it to completion: it dispatches each step
once its parents finish, fans out steps that can run in parallel, enforces a workflow-wide
deadline, and lets you **revive** a failed workflow or **cancel** one, surviving process
restarts and worker crashes.

The workflow engine owns **DAG orchestration and state**; the task engine owns the **execution**
of each individual step — per-attempt retry and per-attempt timeout are the task engine's job.
Like the rest of `tasking`, this is a library embedded by other applications: it orchestrates,
but does not own multi-tenancy, access control, or scheduler leader election — those belong to
the embedding application (see [DESIGN.md](DESIGN.md)).

> For the design record — the Workflow/Step state machines, the single-writer scheduler, the
> `notify`-based feedback path, the deadline/timeout model, and crash recovery — see
> [DESIGN.md](DESIGN.md). This README covers what the package provides and how to use it.

## The core idea in one paragraph

A **workflow** is a DAG of **steps**. Each step declares its parent steps; the edges form the
DAG, and a step becomes runnable only when all its parents are `COMPLETE`. Every step, whatever
its `Type`, is executed as a single task on the task engine (task name `__EXECUTE_WORKFLOW_STEP__`),
so the workflow engine gets the task engine's reliability for free. The engine has three
components that never call each other directly: they exchange messages over Redis and share a
database. The **database is the source of truth**; commands to the scheduler are best-effort
*pokes* that let it act sooner than its periodic maintenance sweep would. Lose a poke and work is
*delayed*, never *lost*.

## Components

| Component     | What it does                                                             | Lifecycle                |
| ------------- | ------------------------------------------------------------------------ | ------------------------ |
| `Client`      | Submission / mutation API — define, revive, cancel workflows             | construct, then call     |
| `Scheduler`   | The state machine — owns every Workflow/Step transition, dispatches steps | `Start` / `Stop`; single-active |
| Step Runner   | The one task processor that runs a step by dispatching to your handler   | register with the task engine |

The scheduler talks to the task engine as a client (to submit and cancel step tasks) and listens
for step results over the [`notify`](../notify/README.md) pub/sub stream — it is **not** wired to
your step handlers directly. You wire these together in your application, injecting the
Redis-backed factory callbacks (`common.NewRedisIPCMessage*`, `notify.NewConsumer`). Tests inject
mocks instead.

## Submitting work (`Client`)

The `Client` mirrors the task engine's, including the **define / submit / define-and-run** split:
`DefineWorkflow` writes the rows but does not start the workflow, `SubmitWorkflow` starts an
already-defined one, and `DefineAndRunWorkflow` does both. The split lets a caller commit its own
additional state (or run its own transaction) between writing the workflow and starting it.

```go
client, err := workflow.NewClient(ctx, workflow.NewClientParams{
    Name:             "my-app",
    DefaultCreator:   "my-app",          // opaque; the notify routing key
    Persistence:      dbClient,          // db.Client
    Config:           clientConfig,      // models.WorkflowClientConfig
    Redis:            redisClient,       // goutilsRedis.Client
    IPCSenderFactory: common.NewRedisIPCMessageSend,
    KnownStepTypes:   knownStepTypes,    // map[string]bool — step Types with a registered handler
})

// Define and start in one call:
wf, err := client.DefineAndRunWorkflow(ctx, workflow.DefineWorkflowParams{
    Spec: models.NewWorkflowParameter{
        Name:     "publish-article",
        Deadline: someDeadline,          // mandatory; steps inherit it as their per-attempt timeout
        Steps: map[string]models.NewWorkflowStepParameter{
            "render":  {Type: "render-html", RetryParams: retry},
            "thumbs":  {Type: "make-thumbnails", RetryParams: retry},
            "publish": {Type: "push-cdn", RetryParams: retry,
                ParentSteps: map[string]bool{"render": true, "thumbs": true}},
        },
    },
    // Creator: &perCallCreator,          // optional; overrides DefaultCreator
}, nil /* or an active db.Database txn to join */)

// Or split the two halves — commit your own state in between:
wf, err = client.DefineWorkflow(ctx, params, nil)
// ... commit related application state ...
err = client.SubmitWorkflow(ctx, wf.ID)

// Revive a FAILED / TIMED_OUT workflow (newDeadline required when it TIMED_OUT):
err = client.ReviveWorkflow(ctx, wf.ID, &newDeadline, nil)

// Cancel:
err = client.CancelWorkflow(ctx, wf.ID, nil)
```

**Up-front step-Type validation.** `KnownStepTypes` is the set of step `Type`s that have a
registered handler (the same registration the Step Runner holds). `DefineWorkflow` rejects — with
a `BadInputError`, *before writing any rows* — a workflow containing a step whose `Type` has no
handler, turning a mid-run failure into a fail-fast at submission. The DAG itself (self-dependency,
unknown parents, cycles) is validated at the same point.

**Error contract** (inspect with `errors.As`):

- `goutils.BadInputError` / `goutils.ValidationError` — the spec was rejected (bad DAG, or an
  unregistered step `Type`); nothing was written.
- `models.PersistenceError` — the workflow rows were **not** created; nothing happened.
- `models.IPCMessageQueueError` — the workflow rows **were** created (or, for revive/cancel, the
  workflow exists), but the scheduler poke failed. The workflow is not lost: the scheduler's
  maintenance sweep drives a still-`PENDING` workflow, and `DefineAndRunWorkflow` still returns the
  created entry. (Same contract for `SubmitWorkflow`, `ReviveWorkflow`, `CancelWorkflow`.)

Every client operation is **"write rows, then poke the scheduler"** — the Client only ever writes
*definition* data and enqueues a scheduler event; it never mutates the live state of a workflow in
flight. That is the scheduler's exclusive job, so there is a single writer of state.

## Running steps (Step Runner + your handlers)

Implement `models.WorkflowStepProcessor` for each step `Type`, then register them all with **one**
Step Runner and hand that runner to the task engine as the processor for the reserved task name
`models.WorkflowExecutionTaskName`. Every workflow step — regardless of `Type` — runs as a task of
that one name; the runner loads the step, reads its `Type`, and dispatches to your handler:

```go
type renderHandler struct{}

func (h renderHandler) ProcessWorkflowStep(
    ctx context.Context, wf models.Workflow, step models.WorkflowStep,
) error {
    // do the work; honor ctx (it carries the step's deadline)
    return nil // or an error → the step FAILS
}

runner, err := workflow.NewRunWorkflowStepTaskProcessor(dbClient, map[string]models.WorkflowStepProcessor{
    "render-html":     renderHandler{},
    "make-thumbnails": thumbnailHandler{},
    "push-cdn":        cdnHandler{},
})

// Register the runner with the task engine under the reserved workflow task name, in the
// executor factory you pass to task.NewReceiver:
exec.RegisterTaskProcessor(models.WorkflowExecutionTaskName, runner)
```

The runner never talks to the workflow scheduler about results. When a step's task reaches a
terminal state the task engine writes an audit event, `notify` broadcasts it, and the scheduler —
subscribed on the engine's creator channel — turns it into the step's outcome. The runner and the
task engine remain unaware of workflow semantics.

Because step tasks are ordinary tasks, route `models.WorkflowExecutionTaskName` to a task
execution queue in your `TaskSchedulerConfig` task-name → queue mapping, and have a receiver serve
that queue, exactly as for any other task.

## Running the scheduler

Exactly **one** scheduler runs against a given database (it is the sole writer of workflow state).
It needs a **task client** to dispatch step tasks and a **`notify` consumer** to receive their
results:

```go
scheduler, err := workflow.NewWorkflowScheduler(ctx, workflow.NewWorkflowSchedulerParams{
    Persistence:           dbClient,
    TaskClient:            taskClient,        // task.Client — dispatches / cancels step tasks
    Config:                schedulerConfig,   // models.WorkflowSchedulerConfig
    Redis:                 redisClient,
    IPCReceiverFactory:    common.NewRedisIPCMessageReceive,
    IPCSenderFactory:      common.NewRedisIPCMessageSend,
    NotifyConsumerFactory: notify.NewConsumer,
})
if err := scheduler.Start(ctx); err != nil { /* ... */ }
defer scheduler.Stop(ctx)
```

`Start` recovers any messages stranded in the scheduler queue buffer, subscribes for step
feedback, and starts the serial event consumer and the maintenance timer.

> **Required deployment wiring — the `notify` producer must emit creator channels.** The
> scheduler receives step results by subscribing to `notify:creator:<engine-creator>`. That
> channel is only populated when the [`notify`](../notify/README.md) **producer** running against
> the same database is configured with **`EmitCreator: true`** (`models.NotificationProducerConfig`).
> If it is `false`, the scheduler's fast-path feedback subscription receives **nothing**: the
> engine stays *correct* — the maintenance sweep reconciles every step against its task's persisted
> state — but every step outcome is delayed by up to one maintenance interval, on every step,
> silently (no error is logged anywhere). Treat `EmitCreator: true` as a hard requirement for the
> feedback fast path, not an optimization. See [DESIGN.md](DESIGN.md) "Task Engine → Workflow
> Scheduler (feedback)".

## Reliability model (what you can rely on)

- **Self-healing progress.** The DAG is advanced by IPC pokes *and* a periodic maintenance sweep
  that re-scans the DB for anything a lost poke would strand (a workflow to start, a step to
  dispatch, a deadline to enforce). A dropped message delays progress; it never loses it.
- **Single writer of state.** Only the scheduler mutates workflow/step state, and it does so
  single-threaded off one serial queue — so there are no intra-engine state races to reason about.
- **Steps inherit the task engine's guarantees.** Per-step retry (per the step's `RetryParams`)
  and per-step timeout are enforced by the task engine; crash-safe reliable queues, idempotent
  handlers, and poison-message quarantine all come from there.
- **No automatic workflow-level retry.** A step that exhausts its task-level retries fails, and its
  workflow moves to `FAILED`. Advancing past that is a deliberate, user-initiated **revive** — the
  workflow engine never re-runs a failed step on its own.
- **Deadlines are workflow-wide.** A step's deadline is *derived* from the workflow deadline (it is
  not authored per step); when the deadline passes, in-flight and not-yet-run steps are timed out
  and the workflow becomes `TIMED_OUT`. Reviving a `TIMED_OUT` workflow requires a new deadline.

## Configuration

- **`models.WorkflowClientConfig`** — the workflow scheduler's queue name (the client's only IPC
  target). No retry settings: step retry is a per-step property carried in the spec.
- **`models.WorkflowSchedulerConfig`** — maintenance interval (≥ 10s) and the scheduler queue name.
  This is a **dedicated** IPC queue, distinct from the task scheduler's queue.

The workflow scheduler dispatches step tasks *through the task client*, so it has no execution-queue
mappings of its own — step routing lives in the task engine's `TaskSchedulerConfig` (route
`__EXECUTE_WORKFLOW_STEP__` to a queue there).

## Notifications

Every workflow/step state change is recorded as a durable, creator-tagged `SystemEventAudit` row
in the same transaction as the change. The [`notify`](../notify/README.md) package turns those rows
into a best-effort Redis pub/sub stream. This package produces the events; it does not broadcast
them. (The scheduler is itself a `notify` subscriber — that is how step results reach it.)

## Not yet supported

Per-step (as opposed to workflow-wide, derived) deadlines, and any automatic workflow-level retry,
are out of scope by design. Multi-tenancy isolation and scheduler leader election, as elsewhere in
`tasking`, belong to the embedding application — see [DESIGN.md](DESIGN.md).
