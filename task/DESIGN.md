# Task Engine — Async Background Task Execution (Design)

> Status: first draft. This document is the authoritative design record for the `task`
> package. Update this file (not scattered notes) when decisions change.

## 1. Motivation & context

`tasking` is a **library** — an async task engine (and, soon, a workflow engine) embedded by
other applications. The `task` package is its core: it lets an embedding app **submit a unit
of background work and have it reliably run, retried on failure, timed out on a deadline, or
cancelled**, across process restarts and worker crashes, without the app writing any of that
orchestration itself.

The design follows the same library posture as the sibling [`notify`](../notify/DESIGN.md)
package. Two consequences run through every decision:

- It **cannot dictate cardinality** of task creators or impose **multi-tenancy** — those
  belong to the embedding application. `Creator` is an opaque string (the notification
  routing key; see [`notify`](../notify/DESIGN.md)); `tasking` never interprets it.
- The engine's components are **wired and lifecycle-owned by the app**, not by the package.
  There is no god-object; components are constructed separately and communicate only through
  Redis IPC queues and a shared database. This keeps each component independently
  deployable and testable.

The engine is built on one durable invariant: **the database is the source of truth**. Every
IPC message is a best-effort *poke* that merely lets a component act sooner than the periodic
maintenance scan would. Losing a poke delays work; it never loses it.

## 2. Two units of work: Task vs. TaskExecution

The single most important distinction in the package — everything else follows from it:

- **`Task`** — the unit of work the *app* submits (`models.Task`). One-shot, either
  immediate or scheduled for a future time. It is the thing that "completes", "fails",
  "times out", or is "cancelled".
- **`TaskExecution`** — **one attempt** at running a task (`models.TaskExecution`). A task
  has **1..N** executions: the first attempt plus one per retry. **Workers process
  executions, not tasks.** After each execution ends, the scheduler decides the task's next
  step (done? retry? give up?).

```
   Task (COMPLETE/FAILED/TIMED_OUT/CANCELLED)
     │
     ├── TaskExecution #1  (attempt 1 → FAILED)
     ├── TaskExecution #2  (retry     → FAILED)
     └── TaskExecution #3  (retry     → PROCESSED)   ⇒ Task COMPLETE
```

Because every ended execution is moved to a single `FINALIZED` state, `ExecutionState` alone
can no longer report the *outcome* — so `TerminalState` (Processed / Failed / Cancelled) is
captured when the instance ends and preserved through finalization. The retry-counting logic
depends on this (it counts executions whose `TerminalState == FAILED`).

## 3. Architecture overview

Four components, each an interface with an `impl`, wired by the app. They never call one
another directly — they exchange typed messages over **Redis reliable IPC queues** (§7) and
read/write shared state through the **`db`** persistence layer.

```
        ┌──────────┐  NEW_TASK / CANCEL_TASK
        │  Client  │ ──────────────────────────────┐
        └──────────┘  (writes Task row first)      │
             defines Task (PENDING) in DB          ▼
                                          ┌───────────────────┐
                                          │  [scheduler queue]│
                                          └───────────────────┘
                                                   │ dequeue
                                                   ▼
   maintenance timer ─(periodic backstop)─▶ ┌────────────┐
                                            │ Scheduler  │  the state machine:
   EXECUTE_SUCCEEDED / EXECUTE_FAILED /     │            │  owns every Task &
   ENGINE_FAILED  ─────────────────────────▶│ (single    │  TaskExecution transition
                                            │  active)   │
                                            └────────────┘
                                                   │ PENDING_INSTANCE
                                                   ▼
                                      ┌────────────────────────┐
                                      │ [task-name→exec queue] │
                                      └────────────────────────┘
                                                   │ dequeue
                                                   ▼
                                            ┌────────────┐  per worker host:
                                            │  Receiver  │  claim, hand to Executor,
                                            │            │  report outcome to scheduler
                                            └────────────┘
                                                   │ submit
                                                   ▼
                                            ┌────────────┐  worker pool for one queue:
                                            │  Executor  │  runs the app's
                                            │            │  TaskExecutionProcessor
                                            └────────────┘
```

| Component   | Role                                                              | Lifecycle                 |
| ----------- | ----------------------------------------------------------------- | ------------------------- |
| `Client`    | Submission API: define `Task` rows, poke the scheduler            | constructed & called      |
| `Scheduler` | The brain / state machine; owns all state transitions; **single-active** | `Start` / `Stop`   |
| `Receiver`  | Per-worker-host consumer: claim executions, run them, report back | `Initialize`/`Start`/`Stop` |
| `Executor`  | Worker pool for one task queue; runs the user processor           | `Stop` (started at construction) |

Supporting layers:

- **`models`** — the `Task`/`TaskExecution` state machines, IPC message types, and audit
  events (the seam into `notify`).
- **`db`** — persistence. State-change methods (`MarkTask*`) write a `SystemEventAudit` row
  **in the same transaction** as the state change, so an event and its state commit
  atomically.
- **`common`** — the Redis reliable-queue IPC send/receive clients (§7).

### 3.1 Wiring by factory injection

Each component is constructed with **factory callbacks** rather than concrete dependencies:
`ExecutorFactoryCB`, `IPCMsgSenderFactoryCB`, `IPCMsgReceiverFactoryCB`. The app passes the
real Redis-backed factories (`common.NewRedisIPCMessage*`, `NewExecutor`); tests pass mocks.
This is how the components stay decoupled and independently testable despite the tight
choreography between them.

**Processors are supplied at construction, not registered at runtime.** The app hands the
`Receiver` a `map[queueName]map[taskName]TaskExecutionProcessor` (`NewReceiverParams.Processors`);
the constructor validates it (every processor's queue must be a configured queue, no nil
processors) and passes each queue's inner map to that queue's `NewExecutor`. The mapping is then
immutable for the component's lifetime — there is no `RegisterTaskProcessor`, so no lock guarding
it and no window where a `PENDING_INSTANCE` can arrive before its processor is registered. A task
name that still has no processor at dispatch time is treated as an engine failure (§8).

## 4. Task lifecycle

State machine (`models.Task.ValidNextState`):

```
                    ┌─────────────────────────────────────────────┐
                    │                                             ▼
  submit ─▶ PENDING ─▶ ACTIVE ─▶ COMPLETE                      CANCELLED
              │  │        │  ├──▶ FAILED                          ▲
              │  │        │  └──▶ TIMED_OUT                       │
              │  └────────┴──────────────────────▶ CANCELLING ────┘
              └───────────────────────────────────────▲
   (PENDING or ACTIVE may go to CANCELLING on a cancel request)
```

| Transition               | Driven by                          | Trigger                                     |
| ------------------------ | ---------------------------------- | ------------------------------------------- |
| *(none)* → **PENDING**   | `Client` (`DefineAndRun…`)         | app submits a task                          |
| PENDING → **ACTIVE**     | `Scheduler.processNewPendingTask`  | `NEW_TASK` poke (or maintenance sweep)      |
| ACTIVE → **COMPLETE**    | `Scheduler.processTaskExecutionComplete` | an execution reached PROCESSED        |
| ACTIVE → **FAILED**      | `processTaskExecutionFailed` / `…EngineFailed` | retries exhausted, or an engine failure |
| ACTIVE → **TIMED_OUT**   | `processTaskTimeout` / `…ExecutionTimedOut` | task/execution deadline passed         |
| PENDING/ACTIVE → **CANCELLING** → **CANCELLED** | `Scheduler.processCancelTask` | `CANCEL_TASK` poke (or maintenance) |

Notes:

- **CANCELLING is a staging state.** A cancel request can arrive while the task is still
  PENDING or ACTIVE — nothing naturally drives it through CANCELLING first — so
  `processCancelTask` marks it CANCELLING then immediately CANCELLED within one transaction,
  and cancels any live executions.
- Entering ACTIVE / COMPLETE / FAILED / CANCELLED / TIMED_OUT writes a matching
  `SystemEventAudit` row in the same transaction (`db.updateTaskState`). These are the events
  `notify` broadcasts.

## 5. TaskExecution lifecycle

State machine (`models.taskExecutionStateTransitions`):

```
  DEFINED ────┐                                     ┌─▶ PROCESSED ─┐
  (immediate) ├─▶ ENQUEUED ─▶ ACQUIRED ─▶ PROCESSING┤              ├─▶ FINALIZED
  SCHEDULED ──┘                    │        │       └─▶ FAILED ────┘
  (future/retry)                   ▼        ▼
                              (any live state) ─────▶ CANCELLED
```

`CANCELLED` is reachable from every *live* state (DEFINED, SCHEDULED, ENQUEUED, ACQUIRED,
PROCESSING). All three outcomes — PROCESSED, FAILED, CANCELLED — are separate branches; that
is why `models.HasEnded()` is the *union* of "at or past" any of them, distinct from
`IsStateAtOrPast` which walks a single downstream chain.

| Transition                     | Owner     | Method                                   |
| ------------------------------ | --------- | ---------------------------------------- |
| create → **DEFINED**/**SCHEDULED** | Scheduler | `db.DefineNewTaskExecInstance` (immediate ⇒ DEFINED, scheduled/retry ⇒ SCHEDULED) |
| → **ENQUEUED**                 | Scheduler | `MarkTaskExecQueued` + send `PENDING_INSTANCE` |
| → **ACQUIRED**                 | Receiver  | `MarkTaskExecAcquired` (records `worker_name`) |
| → **PROCESSING**               | Executor  | `MarkTaskExecProcessing` (pre-processing) |
| → **PROCESSED** / **FAILED**   | Executor  | post-processing `defer` (captures `TerminalState`; on FAILED, also `FailureDisposition` — see §8.1) |
| → **FINALIZED**                | Scheduler | `MarkTaskExecFinalized` (after deciding task's next step) |
| → **CANCELLED**                | Scheduler | `MarkTaskExecCancelled` (parent cancelled / timed out) |

**Ownership handoff** — no two components touch the same transition:

- **Scheduler** owns creation, DEFINED/SCHEDULED → ENQUEUED, and the finalization tail.
- **Receiver** owns ENQUEUED → ACQUIRED and the failure/engine-failure reporting paths.
- **Executor** owns ACQUIRED → PROCESSING → PROCESSED/FAILED.

The deadline is honored at execution time: the executor derives a `context.WithDeadline`
from the instance's `Deadline`, so a processor that ignores its context is still cut off.

## 6. The Scheduler in detail

The scheduler is the only component that mutates state. It is a **single serial consumer**: one
support goroutine (`processQueue`) dequeues an event off its dedicated reliable IPC queue, parses
it, and runs the associated `process*` handler **inline** on that goroutine. There is no separate
worker — every event, including maintenance, is processed one at a time on this single path, so
request buffering lives in the IPC queue rather than a second in-process channel.

### 6.1 Two producers, one serial handler path

- **The IPC path** (`processQueue` → `processOneIPCRequest`): reads the scheduler queue, parses each
  message, and calls the matching `process*` handler **directly**. A handler that returns nil means
  the message is done and is deleted from the buffer; a fatal error leaves it buffered for startup
  replay; a poison message is audited and dropped.
- **The maintenance path** (`performMaintenance`): re-scans the DB and calls the same `process*`
  handlers directly. It does **not** run on a separate thread — the maintenance interval timer
  self-enqueues an `IPC_TASK_ENG_MAINTENANCE` message onto the scheduler's own queue, so the sweep
  is dequeued and run by `processQueue` in the exact same serial path as every other event.

Because everything runs on the one `processQueue` goroutine, no two handlers ever execute
concurrently and there is no in-process ordering to reconcile.

### 6.2 Handlers are idempotent

Every handler guards against duplicate and racing deliveries (an IPC poke racing the
maintenance sweep, or a redundant re-delivery from buffer recovery) using
`IsStateAtOrPast` / `HasEnded` / explicit `TaskState` checks. If the work is already done,
the delivery is a **safe no-op**; a genuinely out-of-order delivery is *not* masked and still
surfaces as a consistency error via the subsequent `ValidNextState` check.

### 6.3 The maintenance timer is the universal backstop

The maintenance interval timer does not invoke maintenance directly — it enqueues an
`IPC_TASK_ENG_MAINTENANCE` message onto the scheduler queue, so the sweep rides the same serial
`processQueue` path as every other event. `performMaintenance` re-scans the DB for every state a
lost poke could strand:

1. **PENDING / CANCELLING tasks** — schedule or cancel them.
2. **ACTIVE tasks past their deadline** — time them out.
3. **PROCESSED / FAILED executions** awaiting finalization — finalize them.
4. **SCHEDULED executions due to start** — enqueue them. *(This is also the only path that
   fires scheduled and retry executions — there is no poke for a future time.)*
5. **Live executions past their deadline** — time them out.

This is why the engine is self-healing: **a dropped IPC message only delays work; the next
maintenance pass catches it.** It is also structurally required for scheduled/retry
executions, which have no triggering message.

### 6.4 Retry decision

On an execution FAILED (and the task still ACTIVE), `processTaskExecutionFailed` counts prior
FAILED executions, asks `RetryParams.NextDelay(count-1)` for the backoff, and either defines a
new SCHEDULED retry execution (at `now + delay`) or, if retries are exhausted
(`delay <= 0`), marks the task FAILED. Backoff is exponential, clamped to a hard
`MaxRetryDelay` ceiling.

## 7. IPC: reliable Redis queues

`common` implements a **reliable queue** on Redis (`IPCMessageReceive`/`IPCMessageSend`):

- `DequeueMessage` atomically moves a message from the shared **main queue** into a
  **per-reader buffer queue** (`PopLeftAndMove`). The message lives in the buffer until the
  reader is *done* with it (`DeleteBufferedMessage`) — so a crash mid-processing leaves the
  message safely staged, not lost.
- Each reader owns an exclusive buffer queue, so multiple readers can share one main queue.

### 7.1 Startup buffer recovery

A crash after dequeue-but-before-delete strands messages in the buffer. Both consumers
recover at startup, **before** normal consumption begins:

- **Scheduler** — `recoverBufferedMessages` drains the buffer and re-enqueues valid messages
  onto the main queue (single processing path); poison messages are audited and dropped.
- **Receiver** — `Initialize` drains its buffers and *reconciles against the DB*: ENQUEUED
  instances are re-queued for retry; instances this worker was processing are marked FAILED;
  already-terminal or other-worker instances are ignored. It also fails any instance the DB
  still shows this worker as processing (it must have crashed mid-run).

### 7.2 Poison messages never crash-loop

Unreadable, unparsable, or unsupported-type messages are recorded as an
`INVALID_TASK_IPC_MESSAGE` audit event (best-effort — logged if the write fails) and dropped
from the buffer. A message that can never be processed must not become an infinite
crash/replay loop.

### 7.3 Fatal vs. recoverable errors

Only a `models.SQLError` (the database or its connection is broken) is **fatal** — no per-request
recovery is meaningful when the DB is down and every message would fail identically (so this is not a
per-message loop). The scheduler consumer, the workflow-scheduler consumer, and each receiver
per-queue consumer all use the same `isFatalDBError` (`errors.As` for `SQLError`, found through the
wrapped error chain) to make this call.

**Handing a fatal fault to the parent — `OnFatal`.** `tasking` is a library with a fire-and-forget
`Start`, so a worker goroutine must not decide the host process's lifetime by calling `os.Exit`
directly. Each of the three worker types accepts an optional `OnFatal(reporter, err, timestamp)`
callback on its constructor params (`NewSchedulerParams`, `NewWorkflowSchedulerParams`,
`NewReceiverParams`). When a consumer goroutine hits a fatal `SQLError` it invokes the callback and
then **exits that goroutine** (it does not loop re-firing on a permanently-broken DB), leaving the
decision of what to do — typically a graceful shutdown — to the parent application. The callback is
guarded by a per-component `sync.Once` (`reportFatal`), so even when several receiver queue threads
trip the same outage at once the parent is notified exactly once. When no callback is supplied, the
default preserves the prior behavior: log the fault and `log.Fatal`.

For the **scheduler** (and the **workflow scheduler**, which mirrors it) the split is expressed as
two independent decisions in `processOneIPCRequest` and `processQueue`:

- **Delete keys off *completion*, not success.** The reliable-queue buffer guards against exactly
  one thing — an application crash/panic mid-handling, where the handler never returns. So once a
  handler *returns* (nil **or** error), the message is deleted from the buffer. Only a crash leaves
  a message for `recoverBufferedMessages` to replay. A returned error never strands a message, so a
  deterministically-failing handler cannot become a replay crash-loop.
- **The error class decides only stop-vs-continue.** After the delete, `processOneIPCRequest`
  returns the handler error and `processQueue` classifies it: a `models.SQLError` reports via
  `OnFatal` and stops the consumer; anything else is logged and processing continues. The
  maintenance sweep re-drives the stranded work from the DB (the source of truth), so a single
  failing message never wedges the scheduler.

The **receiver** shares the same `SQLError`-only fatal split. Most handler failures never surface as
a returned error at all — a bad request is dropped, the failure is reported to the scheduler, and the
queue thread continues. Only an error that propagates out of `processOneIPCRequest` reaches the
fatal-vs-continue decision in `processOneQueue`: a `models.SQLError` reports via `OnFatal` and stops
that queue thread, anything else is logged and the thread continues.

## 8. Engine failure vs. execution failure

Two distinct failure kinds with different semantics:

- **Execution failure** (`EXECUTE_FAILED`, `SystemEventTypeFailedTask`) — the task's own
  processor returned an error. **Retryable** per the task's retry policy — *unless* the failure
  carries a non-retryable disposition (see below), in which case the task is failed outright with
  no retry, but it is still an *execution* failure (same message type, same `FailedTask` audit
  event).
- **Engine failure** (`ENGINE_FAILED`, `SystemEventTypeEngineFailedTask`) — the framework
  itself couldn't operate: the receiver couldn't claim the instance, couldn't submit it to the
  executor, or the executor's queue has **no registered processor** for the task name (a misrouted
  `PENDING_INSTANCE`). **Not retried** — `processTaskExecutionEngineFailed` finalizes the instance,
  fails the task, and writes an audit event atomically. The parent app is expected to review these.

### 8.1 Retry disposition — opting an execution failure out of retry

An execution failure carries a **`FailureDisposition`** (`RETRYABLE` / `NON_RETRYABLE`; nil is
treated as retryable). A `TaskExecutionProcessor` opts a failure out of retry by wrapping its
returned error in a `models.NonRecoverableError` (e.g. malformed input, a resource that will never
exist). The executor detects it with `errors.As` and, when marking the instance FAILED, persists
`NON_RETRYABLE` on the `TaskExecution` row **and** stamps it on the `EXECUTE_FAILED` IPC message.

The persisted column is the source of truth: `processTaskExecutionFailed` reads it (not the
message) via the shared `decideExecutionRetry` helper, so the maintenance backstop — which has no
message — reaches the same decision. A lost `EXECUTE_FAILED` poke therefore cannot resurrect a
non-retryable failure as a retry. `decideExecutionRetry` is the single point both the IPC handler
and the maintenance sweep call, so the "skip retry on NON_RETRYABLE" rule cannot drift between
them (see §6.2/§6.3).

The classification "which error → which report" lives in `receiver.onTaskComplete`: a
`TaskExecutorError` (the executor's marker for a missing processor) → `ENGINE_FAILED`; anything
else → `EXECUTE_FAILED`, with `NON_RETRYABLE` iff a `NonRecoverableError` is in the chain.

## 9. The seam into `notify`

Every state change writes a `SystemEventAudit` row in the same transaction, carrying the
task's opaque `Creator`. The `notify` package's producer polls those rows and broadcasts them
over Redis pub/sub. `task` is responsible only for **producing durable, creator-tagged audit
events**; routing, channels, and delivery are `notify`'s concern. See
[`notify/DESIGN.md`](../notify/DESIGN.md).

The forthcoming **workflow engine** is a first-class consumer of these notifications (fast
path) with a DB-reconciliation backstop, mirroring the task engine's own poke-plus-maintenance
reliability model.

## 10. Configuration surface

- **`TaskClientConfig`** — scheduler queue name; optional per-task-name retry overrides.
- **`TaskSchedulerConfig`** — maintenance interval (≥ 10s); scheduler queue name; task-name →
  execution-queue mappings (so the scheduler knows which queue to poke per task name).
- **`TaskReceiverConfig`** — receiver name; the queues it serves (each with worker count and
  buffer length); scheduler queue name (to report outcomes). The **processors** each queue runs
  are *not* config — they are behavior, supplied to `NewReceiver` via `NewReceiverParams.Processors`
  (§3.1), so the declarative config stays free of Go interface values.

Task-name → queue mapping is the routing fabric: the scheduler sends `PENDING_INSTANCE` to the
queue configured for that task's name, and the receiver serving that queue picks it up.

## 11. Non-goals / explicit deferrals

- **Periodic / recurring tasks** — the models hint at them (`TaskExecution` doc comment,
  `RETRY_EXECUTION` class), but only `IMMEDIATE_ONE_SHOT` and `SCHEDULED_ONE_SHOT` schedule
  classes are implemented. Recurring scheduling is future work.
- **Multi-tenancy / creator isolation** — the embedding app's responsibility; `Creator` is an
  opaque, uninterpreted string.
- **Scheduler leader election** — the scheduler is single-active *by contract*. Running more
  than one against the same DB is unsafe (they would both mutate state); electing a single
  active scheduler is the app's job. (Receivers, by contrast, scale out freely — they
  coordinate through the reliable queue and DB claim.)
- **Migrations** — schema is stated by the models; schema evolution is handled near release.
- **Workflow orchestration** — a separate engine and design (§9).
