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
| → **PROCESSED** / **FAILED**   | Executor  | post-processing `defer` (captures `TerminalState`) |
| → **FINALIZED**                | Scheduler | `MarkTaskExecFinalized` (after deciding task's next step) |
| → **CANCELLED**                | Scheduler | `MarkTaskExecCancelled` (parent cancelled / timed out) |

**Ownership handoff** — no two components touch the same transition:

- **Scheduler** owns creation, DEFINED/SCHEDULED → ENQUEUED, and the finalization tail.
- **Receiver** owns ENQUEUED → ACQUIRED and the failure/engine-failure reporting paths.
- **Executor** owns ACQUIRED → PROCESSING → PROCESSED/FAILED.

The deadline is honored at execution time: the executor derives a `context.WithDeadline`
from the instance's `Deadline`, so a processor that ignores its context is still cut off.

## 6. The Scheduler in detail

The scheduler is the only component that mutates state. It is built on a
`goutils.TaskProcessor` — a single-consumer worker with a **type-keyed dispatch map**. Nine
`schedulerWorkReq*` types each map to one `process*` handler.

### 6.1 Two entry paths into the handlers, deliberately not unified

- **The IPC path** (`processQueue` → `processOneIPCRequest`): runs on its own goroutine,
  reads the scheduler queue, and **`Submit`s** typed work-requests to the worker. It never
  calls a handler inline — the worker's event loop is a separate consumer.
- **The maintenance path** (`performMaintenance`): runs **on** the worker (it was itself
  submitted as a `schedulerWorkReqRunMaintenance`), so it calls the `process*` handlers
  **directly**.

These must stay separate: submitting from within the worker would deadlock or reorder; the
code comments call this out explicitly.

### 6.2 Handlers are idempotent

Every handler guards against duplicate and racing deliveries (an IPC poke racing the
maintenance sweep, or a redundant re-delivery from buffer recovery) using
`IsStateAtOrPast` / `HasEnded` / explicit `TaskState` checks. If the work is already done,
the delivery is a **safe no-op**; a genuinely out-of-order delivery is *not* masked and still
surfaces as a consistency error via the subsequent `ValidNextState` check.

### 6.3 The maintenance timer is the universal backstop

`performMaintenance` re-scans the DB for every state a lost poke could strand:

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

Only a `models.SQLError` (the database or its connection is broken) is **fatal** — it stops
the worker via `log.Fatal`, because no per-request recovery is meaningful when the DB is down.
Everything else is handled per request: drop the message, report the failure to the scheduler,
and continue.

## 8. Engine failure vs. execution failure

Two distinct failure kinds with different semantics:

- **Execution failure** (`EXECUTE_FAILED`, `SystemEventTypeFailedTask`) — the task's own
  processor returned an error. **Retryable** per the task's retry policy.
- **Engine failure** (`ENGINE_FAILED`, `SystemEventTypeEngineFailedTask`) — the framework
  itself couldn't operate: the receiver couldn't claim the instance, or couldn't submit it to
  the executor. **Not retried** — `processTaskExecutionEngineFailed` finalizes the instance,
  fails the task, and writes an audit event atomically. The parent app is expected to review
  these.

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
  buffer length); scheduler queue name (to report outcomes).

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
