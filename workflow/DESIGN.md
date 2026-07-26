# System Support Workflow

The workflow engine executes long-running, multi-step **DAG workflows**, using the
[Task engine](../task/README.md) as the underlying executor. Each workflow step is run as
a system task; the workflow engine owns the DAG orchestration, state management, and
lifecycle, while the task engine owns the actual execution, per-attempt retry, and
per-attempt timeout of individual steps.

This document describes the operational design of the workflow engine. It supersedes any
contradicting details in the current implementation — existing code will be adjusted to
match this design.

## Contents

- [Concepts and Data Model](#concepts-and-data-model)
- [Components](#components)
- [Task Engine Interface](#task-engine-interface)
- [Scheduler Events](#scheduler-events)
- [State Semantics](#state-semantics)
- [Deadlines and Timeouts](#deadlines-and-timeouts)
- [Cancellation](#cancellation)
- [Crash Recovery](#crash-recovery)
- [Design Invariants](#design-invariants)
- [Deferred to Later Phases](#deferred-to-later-phases)

---

## Concepts and Data Model

A **Workflow** is a DAG of **Workflow Steps**. Steps declare their parent steps (the steps
they depend on); the edges form the DAG. Both the workflow and its steps have explicit
state machines enforced by `ValidNextState`.

Each step carries a `Type` that selects which processor runs it, its `Parameters`, its
`RetryParams`, and a `Deadline`. The step `Deadline` is **derived, not user-authored** — it
mirrors the workflow deadline (see [Deadlines and Timeouts](#deadlines-and-timeouts)) and is
re-synced when the workflow deadline changes (see
[Revive Failed Workflow](#revive-failed-workflow)); it exists only to hand the
task engine a per-attempt timeout. A step is executed by the task engine as a single task of
type `ExecuteWorkflowStep`, whose primary parameter (`TaskParameterExecuteWorkflowStep`) is the
**workflow step ID**.

### Workflow States

```
PENDING ──▶ RUNNING ──▶ COMPLETE                (terminal)
                └──▶ FAILED / TIMED_OUT
                          │
                          └──▶ RUNNING           (user revive)
Any non-terminal state ──▶ CANCELLING ──▶ CANCELLED  (terminal)
```

Transitions (each with its sole producing handler):

- `PENDING → RUNNING` — [Process Workflow](#process-workflow), on first receipt.
- `RUNNING → {COMPLETE, FAILED, TIMED_OUT}` — [Execution Update](#workflow-step-execution-update)
  (and the deadline enforcers for `TIMED_OUT`).
- `{FAILED, TIMED_OUT} → RUNNING` — [Revive Failed Workflow](#revive-failed-workflow), the
  single user recovery action. It reverts the failed/timed-out steps to `DEFINED`; for a
  `TIMED_OUT` workflow it also requires a new deadline (the old one has passed).
- `Any non-terminal state → CANCELLING → CANCELLED` — [Cancellation](#cancellation);
  reachable from every non-terminal state (`PENDING`, `RUNNING`, `FAILED`, `TIMED_OUT`).

`{FAILED, TIMED_OUT} → RUNNING` and cancellation are the only ways out of `FAILED` /
`TIMED_OUT`. Only `COMPLETE` and `CANCELLED` are terminal; they admit no outgoing transition.

A workflow is **born `PENDING`**: the user creates the workflow row directly in `PENDING`
(creating it *is* the request to run it — there is no separate "defined but not-yet-requested"
workflow state), then submits a **Process Workflow** event. When the scheduler processes that
event it moves the workflow `PENDING → RUNNING` and begins fanning out startable steps. This
mirrors the task engine, where a `Task` is born `PENDING` and the scheduler drives it to
`ACTIVE`. (There is a step-level `DEFINED` state — the DAG needs it to mark a step not yet
dispatched — but no workflow-level `DEFINED`.)

**Only `COMPLETE` and `CANCELLED` are terminal.** `FAILED` and `TIMED_OUT` are not
terminal — a `FAILED` workflow may be brought back to `RUNNING` by a user-initiated revive;
a `TIMED_OUT` workflow is a hard stop but is still, formally, non-terminal (it awaits a
possible deadline change + revive, or cancellation).

### Workflow Step States

```
DEFINED ──▶ PENDING ──▶ RUNNING ──▶ COMPLETE
   ▲                            └──▶ FAILED / TIMED_OUT
   │                                       │
   └── (user revive, via workflow: sets user_restarted, reverts to DEFINED)
RUNNING ──▶ CANCELLING ──▶ CANCELLED       (a RUNNING step has a task to drain)
DEFINED / PENDING / FAILED / TIMED_OUT ──▶ CANCELLED   (nothing in-flight; skips CANCELLING)
```

There is **one** pending state (`PENDING`) and **one** executing state (`RUNNING`), on both
the first run and a revived re-run — a revived step re-enters at `DEFINED` and flows through
the *same* `DEFINED → PENDING → RUNNING` path as a first run. Whether a step is on its first
run or a user-requested re-run is carried by a **separate boolean attribute,
`user_restarted`**, on the step — *not* by a distinct state:

- **First run** of a step: `DEFINED → PENDING → RUNNING`, `user_restarted = false`.
- **Revived run** of a step (**only ever user-initiated** — the workflow engine has *no*
  automatic retry, unlike the task engine; a `FAILED`/`TIMED_OUT` step advances only when the
  user asks, via [Revive Failed Workflow](#revive-failed-workflow)): the step is reverted
  `{FAILED, TIMED_OUT} → DEFINED` with `user_restarted = true`, then re-runs the ordinary path.
- `user_restarted` records **"the user revived this step,"** *not* "a second execution
  occurred." (A step that was `TIMED_OUT` by a global workflow timeout without ever running,
  then revived, truthfully carries `user_restarted = true` — the user did revive it, even
  though it never previously executed.) It gives the same **visibility** — "has the user
  intervened on this step?" — that separate `PENDING_RETRY`/`RETRYING` states would have,
  without doubling the pending/executing states or forcing every handler to branch on which of
  two equivalent states a step is in. The state machine stays minimal; the flag is pure
  metadata.

---

## Components

The workflow engine has three components: the **Workflow Client** (submission / user-mutation
API), the **Workflow Scheduler** (the state machine), and the **Workflow Step Runner** (the
per-step executor). This mirrors the task engine's `Client` / `Scheduler` / `Receiver`+`Executor`
split.

### Workflow Client

The user-facing API, mirroring the task engine's `Client`. It is how an embedding app defines
and acts on workflows. Every Client operation is **"write DB rows, then poke the scheduler"** —
the Client itself never performs a *state transition*; it writes *definition* data and emits a
scheduler event, and the scheduler owns every state change (preserving the single-writer
invariant below). Operations:

- **Define workflow** — write the workflow row (born `PENDING`, with its mandatory deadline)
  and its step rows (born `DEFINED`), then emit **Process Workflow** to start it.
- **Revive workflow** — emit **Revive Failed Workflow** (optional `newDeadline`; required when
  the workflow is `TIMED_OUT`) to reattempt a `FAILED`/`TIMED_OUT` workflow.
- **Cancel workflow** — emit **Cancel Workflow**.

The Client only ever *writes the initial `DEFINED`/`PENDING` definition rows and enqueues
events*; it does not mutate the state of a workflow already in flight. That is the scheduler's
exclusive job (next), so there is a single writer of live state.

### Workflow Scheduler

The single point of truth and the only mutator of workflow **state**. Modeled on the task
scheduler: a **single-threaded** worker draining a dedicated IPC queue, dispatching typed
work requests to handlers, each wrapped in a database session (`ActiveSessionWrapper`). Because
it is single-threaded and the sole writer of workflow state, there are no intra-scheduler races
to guard against. (The Client writes only *definition* rows, never live-state transitions, so
it does not violate this single-writer property — see above.)

Responsibilities:

- Receive new-workflow start requests.
- Receive step execution feedback (as a `notify` subscriber; best-effort fast path).
- Manage workflow and step lifecycle / state transitions.
- Dispatch ready steps to the task engine (via the task client).
- Enforce workflow deadlines.
- Drive cancellation.
- Periodic maintenance sweep for recovery / liveness.

### Workflow Step Runner

A **single** `models.TaskExecutionProcessor` (implements `ProcessTaskExecution`) registered
with the task engine for the one task type `ExecuteWorkflowStep`. The task engine invokes it to
run one workflow step. It receives the workflow step ID (via
`TaskParameterExecuteWorkflowStep`), and its result becomes the task's terminal state.

**How the step's `Type` selects the work.** All step types funnel through this *one* task type
and *one* registered runner (the task engine routes by task *type*, and every workflow step is
type `ExecuteWorkflowStep` — see [Concepts](#concepts-and-data-model)). The per-`Type` dispatch
is therefore **internal to the Step Runner**: on invocation it loads the step row by ID, reads
the step's `Type` and `Parameters`, and looks up the handler registered for that `Type` in a
runner-owned registry, then runs it. The embedding app registers one handler per step `Type`
with the runner; the task engine never sees these types. (This keeps the task engine's
type→queue routing trivial — a single workflow queue — while still letting a workflow contain
heterogeneous step kinds.)

The step runner does **not** talk to the workflow scheduler directly about results. Result
feedback flows through the `notify` framework (see [Task Engine Interface](#task-engine-interface)):
the task engine records a terminal `SystemEventAudit` for the step's task in the normal
course of finalizing it, and `notify` broadcasts that event to the workflow scheduler. The
runner and the task engine remain unaware of workflow semantics.

---

## Task Engine Interface

The two directions of traffic use **two different transports**, deliberately:

- **Commands** (scheduler → task engine) go over the shared Redis reliable-queue transport
  via the task client.
- **Feedback** (task engine → scheduler) rides the [`notify`](../notify/DESIGN.md)
  framework's Redis **pub/sub**, not a queue. The workflow engine adds **one** new queue —
  the workflow scheduler queue, for the scheduler's own IPC (`Process Workflow`, `Schedule
  Workflow Step`, user requests, maintenance) — and **one** long-lived `notify`
  subscription for step feedback.

### Workflow Scheduler → Task Engine (commands)

Via the **task client**, which enqueues onto the task scheduler queue:

- **Submit step task** — define + submit a `ExecuteWorkflowStep` task for a
  step. The step's `RetryParams` and `Deadline` are handed to the task at definition time
  so the task engine owns per-attempt retry and timeout. **The task's `Creator` is set to
  the workflow engine's fixed creator identity** (see feedback below); that is the entire
  mechanism by which its terminal events later reach the scheduler. No per-task feedback
  target is stamped on the task.
- **Cancel step task** — request the task engine cancel a step's task. Reuses the task
  scheduler's existing `IPCMsgTypeCancelTask`. *(The task client does not yet expose a
  cancel-task method; it will be added during implementation.)*

### Task Engine → Workflow Scheduler (feedback) — via `notify`

Feedback is **not** a bespoke mechanism on the task model. The task engine already writes a
terminal `SystemEventAudit` row in the same transaction that finalizes a task
(`COMPLETE_TASK`, `FAILED_TASK`, `TIMED_OUT_TASK`, `CANCELLED_TASK`, and `ENGINE_FAILED_TASK`),
carrying the task's opaque `Creator`. The [`notify`](../notify/DESIGN.md) producer polls
those rows and broadcasts each as a `models.NotificationEvent` over Redis pub/sub. The
workflow scheduler is simply a **`notify` subscriber**. The task engine learns nothing about
workflows — it only does what it already does.

**Subscription: one static channel, the creator channel.** Because every step task is
submitted with the workflow engine's fixed `Creator`, every step task's events land on the
single channel `notify:creator:<workflow-engine-creator>`
(`models.BuildNotifyCreatorChannelName`). The scheduler holds **one** long-lived
`Subscriber` on that channel for its whole lifetime. This sidesteps `notify`'s constraint
that a `Subscriber`'s topic set is fixed at `Subscribe` time (see
[`notify/DESIGN.md` §6](../notify/DESIGN.md)): there is nothing to add or tear down as
steps go live and settle, and — unlike a `notify:type:*`/`notify:all` firehose — the
channel is already scoped to exactly "tasks this engine created," so no cross-tenant
filtering is needed.

**The handler is a thin adapter into the scheduler queue.** The `notify` callback runs on
the subscriber's single reader goroutine and **must return promptly**
([`notify/DESIGN.md` §6](../notify/DESIGN.md)); it does **no** DAG work. Per event it:

1. Filters by `EventType`. The creator channel carries *all* of the engine's task events;
   only the five terminal types above are relevant. `ACTIVATE_TASK` and any non-terminal
   type are dropped. `ENGINE_FAILED_TASK` is mapped to the **FAILED** step outcome (the
   engine could not run the attempt — non-retryable at the task layer, so the step failed).
2. Reads the task ID off the event (`SubjectID`, which for task events is the task ID) and
   forwards it — with the derived outcome — onto the workflow scheduler queue as the feedback
   that drives a **Workflow Step Execution Update**. All heavy work — the step↔task lookup
   that resolves the task to its step, the DB writes, and DAG advancement — happens later, on
   the single-threaded scheduler worker: when the worker handles this feedback it resolves
   task → step, derives the new step state, and applies `Execution Update(stepID, newStepState)`.

Key properties:

- **Task-engine agnostic.** Feedback is a side effect of the audit log the engine already
  writes; nothing workflow-shaped is added to the `Task` model. (The former `OnTermination`
  field is **removed** from this design.)
- **Delivery is best-effort.** `notify` is pub/sub: an event broadcast while the scheduler
  is offline is **missed** — Redis does not replay it. This is acceptable *only* because of
  the DB-reconciliation backstop: correctness never rests on the notification. See
  [State Semantics](#state-semantics) and [Crash Recovery](#crash-recovery) — the
  maintenance sweep reconciles each live step against its task's persisted terminal state,
  so a dropped "step done" **delays, never stalls,** a workflow. The notification is the
  fast path; the DB is the source of truth.
- **The outcome is carried by the event type,** not a separate state field: the scheduler
  maps `COMPLETE_TASK → COMPLETE`, `FAILED_TASK`/`ENGINE_FAILED_TASK → FAILED`,
  `TIMED_OUT_TASK → TIMED_OUT`, `CANCELLED_TASK → CANCELLED`.
- **Correctness rests on the audit *row*, not the broadcast.** The task engine writes the
  terminal `SystemEventAudit` row **in the same transaction** as the task's state change
  ([`task/DESIGN.md` §9](../task/DESIGN.md)), so the durable outcome and the event to
  broadcast commit atomically. The `notify` broadcast happens *later*, out of band, from the
  producer's poll — it is best-effort and may be lost. The workflow scheduler therefore never
  assumes atomic finalize-and-notify: it treats the broadcast purely as a fast-path poke and,
  when reconciling, reads the task's **committed** terminal state from the DB. A lost or
  never-sent broadcast cannot cause an incorrect outcome, only a delayed one.

### Step ↔ Task Linkage

A table links workflow steps and the tasks that execute them, keyed so a step resolves to
its tasks and a task resolves to its step. A step maps to **1..N tasks over its lifetime**
(first run + each user-initiated revive); a task executes exactly one step — so the relation
is **one-to-many (step → tasks)**, not many-to-many. (A plain `step_id` column on the link /
task-side row suffices; a join table is only warranted if a future feature lets one task
execute multiple steps, which this design does not.)

This linkage is load-bearing in three places: the feedback handler resolves the event's task
ID back to a step; idempotent dispatch checks *"is there already a **non-terminal** task for
this step?"*; and both recovery layers reconcile step state against the linked task's
persisted state. Throughout this document, *"a live task exists for a step"* means **a linked
task in a non-terminal state** — a terminal task from a prior run (e.g. the FAILED task of a
since-revived step) does **not** count as live and must not block a fresh dispatch.

**Reconciling a step with multiple terminal tasks.** Because revive does not delete the prior
attempt's task or its link — it only reverts the step to `DEFINED`, and the next dispatch
*appends* a new link — a re-run step accumulates **multiple** linked tasks, and a later
lost-feedback reconciliation can find **all of them terminal** (the stale prior-attempt task
plus the current attempt's). When a reconciler must derive a single outcome from a step's
tasks (the maintenance sweep's `RUNNING` row synthesizing an Execution Update from the task's
persisted state), it must key on the **most-recent** task — the current attempt — not an
arbitrary one; the step↔task lookup therefore returns tasks **most-recent-first** so the
current attempt is unambiguous. (The stale links are otherwise harmless: they are never
`live`, so they never block dispatch. Garbage-collecting a step's prior-attempt links on
revive is a possible future simplification, not a correctness requirement — ordering the
lookup is sufficient.)

### Step Execution: the Step Runner and its two registrations

Every workflow step, regardless of its `Type`, runs as a task of the **one** task type
`WorkflowExecutionTaskName` (`__EXECUTE_WORKFLOW_STEP__`), executed by the **one** task
processor the workflow engine registers with the task engine — the **Step Runner**. The
task engine therefore sees a single task type and a single processor; the heterogeneity of a
workflow's steps is entirely internal to the Step Runner. There are **two distinct
registration acts**, aimed at two different engines, and they must not be conflated:

1. **Runner ↔ Task Engine** (engine-internal, once): the workflow engine registers its Step
   Runner (a `models.TaskExecutionProcessor`) with the task engine under
   `WorkflowExecutionTaskName`. The embedding application never performs or sees this — it is
   the workflow engine wiring itself to its executor.
2. **Step handlers ↔ Runner** (application-facing, per `Type`): the embedding application
   supplies one `models.WorkflowStepProcessor` per step `Type`. The Runner holds these in a
   registry (`map[string]models.WorkflowStepProcessor`) and dispatches to them by `step.Type`.

**Registration is construct-time and immutable.** The `Type → WorkflowStepProcessor` map is
provided once, when the Runner is constructed, and never mutated afterward. A deployment's set
of step types is a static property of the binary; there is no runtime `Register` method and no
lock, which removes an entire class of races (a write to the registry concurrent with a task
consuming it) and the ordering hole of "a task for type X arrives before X is registered." This
mirrors the intended construct-time wiring of the task engine's own processors.

**The same registration is shared with the Workflow Client.** Because the handler set is known
at construction, the **Workflow Client** is given the *same* registration (the set of known step
`Type`s), so **Define Workflow can reject, up front, a workflow containing a step whose `Type`
has no registered handler** — a fail-fast validation far friendlier than a mid-run failure. In
the common single-process deployment the Client and Runner share this registration directly. The
runtime `MissingHandler` guard below remains the authoritative backstop (the Client and Runner
*could* be separate processes with divergent registration), but the definition-time check catches
the overwhelmingly common misconfiguration at the moment it is introduced.

#### What the Runner does on invocation

The task the scheduler submits carries, as its task `Parameters`, only the **workflow step
ID** (`TaskParameterExecuteWorkflowStep`). On invocation the Runner:

1. Parses the task `Parameters` to recover the step ID. **Malformed → `NonRecoverableError`**
   (see below): the task `Parameters` are Runner-owned plumbing the scheduler wrote, so a blob
   that won't parse is a wiring/code bug retrying can never fix.
2. Loads the `WorkflowStep` by ID, and its parent `Workflow` (from `step.WorkflowID` — the
   scheduler does not pass the workflow ID; the Runner derives it, so the two can never
   disagree).
3. Looks up `handlers[step.Type]`. **Missing → `NonRecoverableError`** (see below).
4. Invokes `handler.ProcessWorkflowStep(ctx, workflow, step)` and returns its result as the
   task's outcome.

**Two separate `Parameters` blobs — never conflated.** The *task* `Parameters` is
Runner-owned plumbing (the step ID) and the application never authors or reads it. The *step*
`Parameters` (`WorkflowStep.Parameters`, set by the application at Define Workflow) is opaque
to the Runner: it is passed through, unparsed, inside the `WorkflowStep` handed to
`ProcessWorkflowStep`. Only the per-`Type` handler knows how to interpret it. The Runner never
inspects `step.Parameters`.

**The Runner never reports to the scheduler.** Its return value becomes the *task's* terminal
state; feedback to the workflow scheduler flows entirely through the `notify` path above
(Invariant 6). The Runner is unaware of the scheduler.

#### Error handling and the two error namespaces

An error out of `ProcessTaskExecution` reaches the task engine as a plain `error`; the task
engine cannot (and by [Invariant 6](#design-invariants) must not) tell *why* the step failed.
Two error namespaces converge here, and their retry treatment differs. In the implementation the
Runner tags each failure with a concrete type: **retryable** failures use `StepExecutionError`
(handler failed) or `StepPreprocessError` (DB read failed); **non-retryable** failures wrap in
`models.NonRecoverableError`.

- **Retryable** — **Workflow step processor error** (the registered `WorkflowStepProcessor`
  failed — e.g. a remote server timed out; wrapped in `StepExecutionError`) and
  **Runner-internal transient error** (e.g. the DB read of the step/workflow failed; wrapped in
  `StepPreprocessError`): both are potentially **transient**, so both are returned and are
  **subject to the task engine's normal per-attempt retry** using the step's `RetryParams`. This
  is precisely the per-attempt retry the design wants the task engine to own; the Runner does not
  suppress it. (The wrappers carry the underlying error as `Core`, so `errors.As` still sees
  through them.)
- **Non-retryable** — **Malformed task `Parameters`** (the step-ID blob won't parse) and **no
  handler for `step.Type`** (no `WorkflowStepProcessor` registered for the step's `Type`): both
  are **configuration/wiring errors, never transient** — retrying cannot make a malformed blob
  parse or a missing handler appear.
  The Runner returns its error wrapped in a **`models.NonRecoverableError`**, the task engine's
  standard signal that a processor failure must not be retried. The executor detects it (via
  `errors.As`, even wrapped in the executor's own `TaskExecutionError`) and persists a
  `NON_RETRYABLE` failure disposition on the execution instance; the scheduler's
  `decideExecutionRetry` then short-circuits to "no retry" regardless of remaining budget,
  marking the task `FAILED` immediately (see [`task/DESIGN.md` §8](../task/DESIGN.md)). The step
  then goes `FAILED` promptly (its reason captured in `WorkflowStep.ErrorMessage`), rather than
  burning the full retry budget on a hopeless attempt.

  This needs **no workflow-specific task-engine machinery** — `NonRecoverableError` and the
  `NON_RETRYABLE` disposition are a generic task-engine facility any processor may use, and the
  Step Runner is simply one such processor. It keeps [Invariant 6](#design-invariants) intact:
  the task engine never learns that "malformed params" or "no handler" are *workflow* concepts,
  only that this processor returned a non-retryable failure.

#### Failure history and its retention

The **latest** failure reason for a step is cached on `WorkflowStep.ErrorMessage` at the
`FAILED`/`TIMED_OUT` transition, as a convenience snapshot (single read, no join). The full
**history of attempts** is *not* duplicated into a workflow-side table: it already exists as
the step's linked tasks (one task per run: first run + each revive) and each task's
`TaskExecution` rows (one per retry, each carrying its own `error_msg`), reachable through the
step↔task linkage. The audit event log is deliberately **not** used as this history store — it
is a persisted *log* subject to pruning/archival, with the wrong lifecycle for durable history.

Because that history lives in the task rows, it must not be deletable out from under a step:

- A task linked in `workflow_step_runner_tasks` **cannot be deleted directly** by the user
  (`DeleteTask` refuses it). It leaves only with its workflow.
- **Deleting a workflow reaps its step tasks** (and their `TaskExecution` history, via FK
  cascade) as part of tearing the workflow down. This is a privileged, unguarded delete path,
  distinct from the user `DeleteTask` — it must be, or the linkage guard would block the
  workflow's own cleanup. The ordering is **capture-then-cascade**: list the workflow's steps,
  list their linked task IDs, bulk-delete those tasks, then delete the workflow (which cascades
  the steps). The task IDs are captured *before* any delete, because deleting the workflow (or
  its steps) cascades away the `workflow_step_runner_tasks` link rows that are the only pointer
  from steps to tasks — read them first or the tasks orphan. The whole sequence runs in one
  transaction.

The intent is deliberate: **a workflow-owned task never outlives its workflow.** History is
coextensive with the workflow; when the workflow is legitimately deleted, its steps' tasks and
their execution attempts go with it, cleanly and atomically.

---

## Scheduler Events

The scheduler processes typed work requests, mirroring the task scheduler's
`schedulerWorkReq*` pattern. Events may arrive from the queue (external or from the task
engine) or be emitted by the scheduler onto its own queue.

| Event | Trigger | Purpose |
|---|---|---|
| **Process Workflow** | New-workflow start (user, at creation); emitted after a step completes | Start the workflow (`PENDING → RUNNING`) on first receipt; fan out startable steps |
| **Schedule Workflow Step** | Emitted by Process Workflow | Dispatch one step to the task engine |
| **Workflow Step Execution Update** | `notify` terminal event for a step's task | Apply a step's terminal outcome |
| **Revive Failed Workflow** | User | Revive a `FAILED`/`TIMED_OUT` workflow: revert its failed/timed-out steps to `DEFINED` (+ new deadline if `TIMED_OUT`) |
| **Cancel Workflow** | User | Cancel a workflow |
| **Workflow State Maintenance** | Periodic timer | Recovery / liveness sweep |

### Process Workflow

Fan-out reducer. A step is **startable** when it is `DEFINED` and every parent step is
`COMPLETE` (or it has no parents). This is true both on a first run and after a
[revive](#revive-failed-workflow) — revive returns steps to `DEFINED`, so Process Workflow
needs only this one case.

```
Process Workflow(workflow):
    if workflow.State in {TIMED_OUT, CANCELLING, CANCELLED, COMPLETE}:
        NOOP                          # hard stop — see State Semantics
    else:                             # PENDING (first processing), RUNNING, or FAILED
        if workflow.State == PENDING:
            mark workflow RUNNING      # the only PENDING → RUNNING transition
        for each startable step (DEFINED + parents all COMPLETE):
            mark step PENDING          # the only step DEFINED → PENDING transition
            emit Schedule Workflow Step
```

Notes:

- This is the **only** producer of the workflow `PENDING → RUNNING` transition — the very
  first Process Workflow event a workflow receives (submitted by the user at creation) starts
  it. Subsequent Process Workflow events find it already `RUNNING` (or `FAILED`) and skip the
  transition.
- This is the **only** producer of the *step* `DEFINED → PENDING` transition. It only ever
  advances `DEFINED` steps — a set that includes both first-run steps and steps returned to
  `DEFINED` by [Revive Failed Workflow](#revive-failed-workflow); Process Workflow does not
  distinguish the two (the `user_restarted` flag, set by Revive, is the only trace, and it is
  pure metadata).
- It does **not** gate on `FAILED`: healthy parallel tracks keep advancing past a failure
  on another track (soft stop). It **does** NOOP on `TIMED_OUT`/`CANCELLING`/`CANCELLED`
  (hard stop).
- Because the gate re-checks workflow state at handling time, a stale Process Workflow
  event sitting in the queue when the workflow flips to a hard-stop state harmlessly
  no-ops. No need to hunt down and cancel queued events.

### Schedule Workflow Step

Dispatches one specific step. Sole setter of `RUNNING`.

```
Schedule Workflow Step(step):
    if workflow deadline passed:
        time out workflow (see "Timing out" below); do not dispatch
    if workflow/step is cancelling/terminal:
        do not dispatch
    if a live (non-terminal) task already exists for this step (step↔task lookup):
        do not dispatch a duplicate; leave the live task alone
        # its terminal event (or the maintenance sweep) will drive the step forward
    else:
        submit step task via task client (Creator = workflow-engine creator)
        mark step RUNNING           # from PENDING; step.user_restarted already set (or default false)
```

`step.user_restarted` (see [Workflow Step States](#workflow-step-states)) records whether the
user has revived this step; it is set by Revive and read only for visibility, so Schedule
dispatches `PENDING → RUNNING` uniformly regardless of attempt.

**Timing out.** When the deadline has passed, the scheduler flips the workflow and its
non-terminal steps to `TIMED_OUT` — and for any step whose task is still *running*, it
**requests the task engine cancel that task** (via the task client, same as cancellation), so
a timeout does not leave compute burning against a dead deadline. The cancelled task will
still reach a terminal state and broadcast it; the already-`TIMED_OUT` step makes that
Execution Update a benign no-op (`ValidNextState` rejects the redundant terminal transition).

### Workflow Step Execution Update

Inbound reducer keyed by **`[step ID, new step state]`** — the two producers supply the step
directly, so the reducer never needs a task ID of its own:

- The **`notify` fast path** (below) carries a terminal *task* event; the worker resolves its
  task → step (step↔task lookup) and derives the new step state *before* invoking this reducer,
  so what reaches the reducer is already `[step ID, new step state]`.
- The **maintenance sweep** (backstop) is already iterating per step, so it passes its current
  step directly. This is what lets the sweep time out a **never-dispatched** `PENDING` step —
  one that has *no task at all* to key on (see [Layer 2](#layer-2--maintenance-sweep)).

Both paths land on the same handler. All writes occur in one database session; the current
step's state is written before the aggregate check so it is included.

The `new step state` is one of `COMPLETE`/`FAILED`/`TIMED_OUT`/`CANCELLED`. For task-driven
outcomes it is mapped from the source task event type (`ENGINE_FAILED_TASK` maps to `FAILED`);
the reducer itself never sees raw event types.

```
Execution Update(stepID, newStepState):
    resolve step and its workflow from stepID

    if workflow.State in {CANCELLING, CANCELLED}:
        mark step CANCELLED            # cancellation wins over the reported outcome
        if workflow now settled: mark workflow CANCELLED
        return

    switch newStepState:
      COMPLETE:
        mark step COMPLETE
        if every step is COMPLETE:
            mark workflow COMPLETE
        else:
            emit Process Workflow      # fan out newly-unblocked steps
      FAILED:
        mark step FAILED; mark workflow FAILED     # (notify user — later phase)
      TIMED_OUT:
        # all steps share the workflow deadline, so one timeout means all have timed out
        for each non-terminal step (this one included):
            if step is RUNNING: request task engine cancel its task   # don't burn compute
            mark step TIMED_OUT
        mark workflow TIMED_OUT                         # (notify user — later phase)
      CANCELLED:
        mark step CANCELLED
        if workflow now settled: mark workflow CANCELLED
```

Notes:

- Fan-out is **delegated** to Process Workflow (a single implementation of "fan out
  startable steps"), not inlined.
- The completion predicate is literally *"every step == `COMPLETE`"* — self-guarding
  against a `FAILED`/`TIMED_OUT` step, which can never satisfy it.
- **`TIMED_OUT` is whole-workflow, not per-step.** Unlike `FAILED` (a single-track failure —
  healthy tracks keep running under soft stop), a timeout is a blown *workflow* deadline, and
  every step's derived deadline is that same workflow deadline. So one timed-out step means
  **all** non-terminal steps have timed out: the branch flips the whole set at once. This makes
  the `notify`/sweep `TIMED_OUT` path identical in effect to the [Schedule Workflow Step
  timeout](#schedule-workflow-step) (which flips the workflow and its non-terminal steps
  together) — the two enforcers converge on the same whole-workflow result. Any step whose task
  is still *running* also has that task cancelled, exactly as in the Schedule-path timeout.

### Revive Failed Workflow

The **single** user recovery action for a `FAILED` *or* `TIMED_OUT` workflow. (A `FAILED`
workflow is one whose failure was an execution failure; a `TIMED_OUT` workflow is one whose
failure was a blown deadline — the recovery is otherwise identical, which is why the two are
one event.) If the user chooses to reattempt rather than [cancel](#cancellation), they request
a revive; if the workflow is `TIMED_OUT`, the request **must** also carry a `newDeadline`
(a `FAILED` workflow's deadline has not necessarily passed, so `newDeadline` is optional there —
and required for `TIMED_OUT` because otherwise the revived steps would immediately re-time-out).

The user invokes this through the [Workflow Client](#workflow-client) (one API call, optional
`newDeadline`); the Client emits the `Revive Failed Workflow` event and the **scheduler
handler** performs the transaction below, so the single-writer-of-state invariant holds.

```
Revive Failed Workflow(workflow, newDeadline?):        # newDeadline required iff TIMED_OUT
    require workflow.State in {FAILED, TIMED_OUT}
    if workflow.State == TIMED_OUT:
        require newDeadline is present and newDeadline > now
    in ONE DB transaction:
        mark workflow RUNNING                          # {FAILED, TIMED_OUT} → RUNNING
        if newDeadline: set workflow.deadline = newDeadline
        for each step in state {FAILED, TIMED_OUT}:
            set step.user_restarted = true
            mark step DEFINED                          # {FAILED, TIMED_OUT} → DEFINED
            if newDeadline: set step.deadline = newDeadline   # re-sync derived step deadline
    emit Process Workflow                              # re-run via the normal DEFINED flow
```

Key points:

- **Revive is whole-workflow, not per-step.** It reverts *every* `FAILED`/`TIMED_OUT` step at
  once — symmetric with [Cancellation](#cancellation), also a workflow-level action. (A global
  timeout flips *every* non-terminal step, including deep ones that never ran, to `TIMED_OUT`;
  revive reverts the whole set.) There is intentionally no way to revive just one failed step
  and leave another failed.
- **Reverted steps go to `DEFINED`, not `PENDING`.** This is the key simplification: revive
  returns steps to the *first-run* pending state, so the follow-on `Process Workflow` handles
  them through its **normal `DEFINED` startability path** — no special "re-dispatch a `PENDING`
  step" case. Steps whose parents are not yet `COMPLETE` simply wait as `DEFINED` until Process
  Workflow advances them, so the subtree **re-runs in dependency order** from one request.
- **The whole revert is one transaction**, deadline included, so there is no interval in which
  a step exists under an already-passed deadline that would re-time it out.
- **Recovery is a single `Process Workflow` poke** over already-committed state — consistent
  with [State-before-poke](#state-before-poke-why-a-failed-enqueue-is-safe): if the emit is
  lost, the maintenance sweep re-drives the now-`DEFINED` steps anyway.
- `user_restarted = true` marks every reverted step (the user *did* restart it), even a step
  that never previously executed (timed out before running) — consistent with the flag's
  meaning (see [Workflow Step States](#workflow-step-states)).
- The reverted step's prior terminal task stays linked (step↔task is one-to-many); it is
  terminal, so it does not block the fresh dispatch (non-terminal-task guard).

### Workflow State Maintenance

Periodic recovery / liveness sweep. See [Crash Recovery](#crash-recovery).

---

## State Semantics

### Terminal vs. non-terminal

- **Workflow terminal states:** `COMPLETE`, `CANCELLED`.
- Everything else is non-terminal. A `FAILED` or `TIMED_OUT` workflow can be acted upon
  ([revive](#revive-failed-workflow) — which also carries the deadline change for a
  `TIMED_OUT` workflow — or [cancel](#cancellation)).

**A `FAILED`/`TIMED_OUT` *step* is deliberately dual-natured** — the single most confusing
point in the model, so stated once here explicitly:

- It is **terminal for settle and dispatch purposes**: it counts as "done draining"
  (it does not keep a workflow from settling — see ["Settled"](#settled-and-completion)),
  and it never blocks or is blocked by new dispatch. For **cancel** it has nothing in-flight to
  drain, so Cancellation moves it straight to `CANCELLED` (skipping `CANCELLING`), alongside
  `DEFINED`/`PENDING` steps.
- It is **non-terminal for completion and revive purposes**: it prevents the workflow from
  ever reaching `COMPLETE` (completion requires *every* step `COMPLETE`), and it is the *only*
  kind of step a [Revive](#revive-failed-workflow) reverts to `DEFINED`.

**A `TIMED_OUT` step is a failure**, including a step that was timed out by a global workflow
deadline **without ever running**. The contract is "complete before the deadline"; a step that
did not — whether it ran and overran, or never got the chance — has failed that contract
(a configuration or execution error, in this step or an upstream one). It is treated exactly
like a `FAILED` step: dual-natured as above, and recoverable only by user
[Revive](#revive-failed-workflow).

### Soft stop vs. hard stop

| Workflow state | New step dispatch? | Meaning |
|---|---|---|
| `RUNNING` | yes | Normal operation |
| `FAILED` | **yes (soft stop)** | A step failed somewhere; healthy parallel tracks keep advancing until they stall on the failed subtree |
| `TIMED_OUT` | **no (hard stop)** | Deadline is absolute; nothing new starts |
| `CANCELLING` | **no (hard stop)** | Cancellation is absolute; nothing new starts |
| `COMPLETE` / `CANCELLED` | no | Terminal |

This `FAILED`-soft / `TIMED_OUT`-hard asymmetry is intentional: a failure is recoverable
per-track, a blown deadline is not.

#### Soft-stop: how a `FAILED` workflow quiesces and recovers

When a step on one path of the DAG fails, the workflow is marked `FAILED`, but the DAG does
**not** stop wholesale. Paths **not dependent** on the failed step keep advancing normally —
Process Workflow keeps dispatching their startable steps (soft stop). Those healthy paths run
until they reach a step that **converges** with the failed path: a step whose parents include
one on the failed subtree. That convergence step can never become startable, because
startability requires *every* parent to be `COMPLETE` and a `FAILED` step never satisfies that
predicate. So the healthy paths advance right up to the failed subtree and then stall there.

At that point the workflow has **quiesced**: no step is startable and no step is in-flight.
Quiesced is **not** the formal [settled](#settled-and-completion) predicate, though — the
convergence steps downstream of the failed subtree remain `DEFINED` (they can never become
startable behind a `FAILED` parent), so a step *is* still in a non-terminal state and the
settled predicate does not hold. "Quiesced" means only that the workflow has stopped making
progress: nothing is in-flight and nothing is startable, so the scheduler has no work to emit.
It is certainly **not** `COMPLETE` — the `FAILED` step blocks completion — and it can still be
revived. The scheduler simply stops emitting work for it — there is nothing to dispatch — and
it **remains `FAILED`** indefinitely. A quiesced `FAILED` workflow does **not** move on its own:

- It never becomes `COMPLETE`: the completion predicate is *"every step `COMPLETE`"*, which the
  `FAILED` step defeats.
- It stays `FAILED` even after all its healthy paths finish; only a user action changes it.

**Recovery is user-initiated revive.** The user revives the workflow via
[Revive Failed Workflow](#revive-failed-workflow): *all* its failed (and timed-out) steps are
reverted to `DEFINED` (with `user_restarted = true`) and the workflow moves from `FAILED` back
to `RUNNING`, whereupon the emitted `Process Workflow` reschedules them through the normal
first-run path. As each reverted step completes, its convergence steps finally become startable
and the previously-stalled paths resume. Revive is whole-workflow — every failed subtree is
reattempted together; the workflow returns to `FAILED` if any step fails again, and the user may
revive once more — the cycle repeats until every step is `COMPLETE` (→ `COMPLETE`) or the user
cancels.

**Maintenance-sweep note.** A quiesced `FAILED` workflow is still a non-terminal workflow, so
the maintenance sweep continues to scan it each interval (it must, to catch a `FAILED` workflow
that still has an in-flight step draining on a healthy path). Once fully quiesced it will find
nothing to do on each pass — an intentionally cheap no-op reconcile, not a bug — until the user
revives or cancels. It is not excluded from the sweep, because "fully quiesced" is not a stored
state the sweep could cheaply filter on; the redundant scan is accepted as the cost of a
single, uniform recovery path.

### "Settled" and completion

- A workflow is **settled** when no step is in a non-terminal state
  (`DEFINED`, `PENDING`, `RUNNING`, `CANCELLING`).
- **`COMPLETE`** is reached when *every* step is `COMPLETE`.
- There is no "wait for user" state. If a non-terminal workflow has no more startable
  steps, the scheduler simply stops emitting work for it (until the next feedback, user
  action, or maintenance sweep). A quiesced `FAILED` workflow is the canonical case — see
  [Soft-stop: how a `FAILED` workflow quiesces and recovers](#soft-stop-how-a-failed-workflow-quiesces-and-recovers).
  User notification of state changes is deferred to a later phase.

---

## Deadlines and Timeouts

- **Every workflow must have a deadline.** This is a hard invariant (enforced at workflow
  definition), because deadlines are the ultimate liveness backstop for crash recovery.
- **Step deadlines are derived, not user-authored.** A step's deadline mirrors the
  workflow deadline and exists only to be handed to the task engine, so the *task* enforces
  the per-attempt timeout. Users do not set step deadlines directly.
- **Enforcement is event-driven where it can be, timer-driven where it must be.** The
  scheduler checks the wall clock against the workflow deadline when it would otherwise act —
  specifically in **Schedule Workflow Step** (before dispatching) and on **Execution
  Update**. But a step that is `PENDING` and simply *sitting* (dispatched to Schedule but not
  yet acted on, or its Schedule event lost) has no event to hang a check on, so for that
  window the **periodic maintenance sweep is the sole enforcer**, not a secondary one.

Three enforcers cover the cases with no *unbounded* gap — the sweep bounds the one window
the event-driven checks cannot:

- The **task engine** times out a step execution already *running* when the deadline
  passes → records `TIMED_OUT_TASK`, which `notify` broadcasts back as a step
  `TIMED_OUT` outcome.
- The **workflow scheduler** refuses to start a step *not yet running* past the deadline
  (at Schedule Workflow Step / Execution Update time), flipping the workflow (and its
  non-terminal steps) to `TIMED_OUT`.
- The **maintenance sweep** catches a step that timed out while `PENDING` with no
  intervening event — the window the two event-driven checks structurally cannot see. The
  gap is therefore bounded by the maintenance interval, not zero.

[Revive Failed Workflow](#revive-failed-workflow) handles the deadline conditionally on *why*
the workflow needs reviving. A `FAILED` workflow has not (necessarily) blown its deadline, so
`newDeadline` is optional — revive just reverts its failed steps under the existing deadline. A
`TIMED_OUT` workflow *has* blown its deadline, so `newDeadline` is **required**: revive extends
the deadline and reverts the timed-out steps to `DEFINED` **in one transaction**, so no step is
ever `DEFINED`/`PENDING`/`RUNNING` under an already-passed deadline.

A `FAILED` workflow *can* sit past its deadline (its deadline lapsed while it was quiesced). A
revive there with no `newDeadline` would revert steps that the very next Schedule Workflow Step
immediately re-times-out — harmless but wasted. This self-heals: the maintenance sweep flips any
past-deadline non-terminal workflow to `TIMED_OUT` (see [Layer 2](#layer-2--maintenance-sweep)),
after which `newDeadline` becomes required. The wasted round only occurs in the narrow window
between the deadline lapsing and the next sweep.

A deadline can be **extended but never removed** — every workflow carries a deadline for its
whole lifetime, as it is the liveness backstop for crash recovery.

---

## Cancellation

```
Cancel Workflow(workflow):
    mark workflow CANCELLING
    for each step:
        if state == RUNNING:
            request task engine cancel the step's task (via task client)
            mark step CANCELLING
        else if state in {DEFINED, PENDING, FAILED, TIMED_OUT}:
            mark step CANCELLED           # nothing in-flight to wait on
        # COMPLETE / CANCELLED: leave as-is
    if no step is in {RUNNING, CANCELLING}:
        mark workflow CANCELLED           # settled immediately; nothing was in-flight
```

The final `{RUNNING, CANCELLING}` check is the general
[**settled** predicate](#settled-and-completion) specialized to this exact point: the loop
above has just moved every `DEFINED`/`PENDING`/`FAILED`/`TIMED_OUT` step to `CANCELLED`, so those
can no longer be present, and a step is settled unless it is `RUNNING` or `CANCELLING`. It is the
same predicate, not a different one.

Because the task engine cannot stop a task that is *actively executing*, cancelling a
step's task mainly **prevents a failed task from being retried**. The cancelled task will
still eventually reach a terminal state, which `notify` broadcasts as an Execution Update
(or, if that notification is dropped, which the maintenance sweep synthesizes).

**How a workflow becomes fully `CANCELLED`:** once the workflow is `CANCELLING`, any
subsequent Execution Update for one of its steps marks that step `CANCELLED`, regardless
of the task's actual reported outcome (**cancellation wins over a late `COMPLETE`** — a
task cancelled by the workflow also emits its own terminal event, so a step may draw both a
local cancel and a later broadcast for the same task; the CANCELLING/CANCELLED guard makes
the second a benign no-op). The last in-flight step to drain settles the workflow, flipping
it to `CANCELLED`.

Detection uses **both** a fast event-driven path (the `notify` broadcast) and a periodic
backstop (see below). Because feedback delivery is best-effort, the backstop — not the
notification — is what *guarantees* a `CANCELLING` workflow eventually settles.

---

## Crash Recovery

The workflow **scheduler queue** is assumed non-volatile (a Redis configuration concern), so
its buffered messages survive a crash and are recoverable (Layer 1). The **feedback path is
not** recoverable this way: it is `notify` pub/sub, which is best-effort and has no
per-subscriber buffer — any step-terminal event broadcast while the scheduler is down, or
dropped for a momentarily-disconnected subscriber, is simply gone. Recovery of *lost
feedback* is therefore **entirely** Layer 2's job: the maintenance sweep re-derives a step's
outcome from its linked task's persisted state. This is the crux of why the DB-reconciliation
backstop is not optional.

### State-before-poke: why a *failed enqueue* is safe

Every scheduler handler follows one ordering rule: **it commits the driving state change to
the DB first, and only then emits the follow-on event.** The emit is a *poke* — it merely lets
the next handler act sooner than the maintenance sweep would; it is never the sole record that
work is pending. Concretely:

- **Process Workflow** marks a step `PENDING` (committed), *then* emits Schedule Workflow Step.
- **Revive Failed Workflow** reverts the failed/timed-out steps to `DEFINED` and the workflow
  to `RUNNING` (committed, one transaction), *then* emits Process Workflow.
- **Execution Update** marks the step's terminal outcome (committed), *then* emits Process
  Workflow to fan out.

Because of this ordering, an **emit that fails** — the enqueue onto the scheduler queue
returns an error, or the process dies between the commit and the enqueue — loses only the
*poke*, never the *work*. The DB already reflects the intended state (`PENDING` step, `RUNNING`
workflow, terminal step), and the **maintenance sweep re-derives the missing follow-on** from
exactly that persisted state (a `PENDING` step → re-emit Schedule; a `DEFINED` step whose
parents are now `COMPLETE` → re-drive fan-out). So a failed enqueue needs **no special
handling at the call site** beyond logging: it degrades to the same "lost poke" the sweep
already covers. This is the same guarantee the task engine relies on — *"losing a poke delays
work; it never loses it"* ([`task/DESIGN.md` §1](../task/DESIGN.md)).

The one thing this rule forbids: a handler must **never** treat an emitted event as the only
evidence that work exists. If some future handler needs to enqueue work with no backing DB
state, that state must be persisted first (or the sweep given another way to re-derive it), or
a failed enqueue would lose it irrecoverably.

Recovery has **two layers**, mirroring the task engine.

### Layer 1 — Buffer Replay (`Initialize`)

This layer replays the scheduler's own **queue** — its `Process Workflow` / `Schedule
Workflow Step` / user-request messages, plus any Execution Update the `notify` callback had
already **forwarded onto the queue** and the sweep's synthesized Execution Updates. It cannot
recover feedback that never reached the queue (a broadcast lost while the scheduler was down,
or dropped before the callback could enqueue it) — that pub/sub delivery has no buffer (above)
and is Layer 2's job.

Modeled on the task receiver's `Initialize`. The queue transport stages each
claimed message on a per-reader **buffer queue** and only removes it when the reader
explicitly deletes it. The buffer therefore holds exactly the messages this reader
*claimed but did not finish processing* at crash time.

On startup, **before** the queue processor starts, the scheduler drains its buffer and, for
each buffered message, **re-derives what to do from persisted state** — it does not blindly
re-execute, because a handler may have partially committed. Per message type:

- **Schedule Workflow Step** — is the step still `PENDING`? Does a *non-terminal* task
  already exist (step↔task lookup)? If already advanced or a live task exists → drop; else
  → re-emit.
- **Execution Update** — is the step already in the target terminal state? If yes → drop;
  else → re-apply. (Dropping is safe for the *step's* state, but the handler's follow-on
  fan-out — `emit Process Workflow` on `COMPLETE` — is **not** re-driven by this drop. If the
  crash fell between "mark step COMPLETE" and "emit Process Workflow," Layer 1 drops the
  message and the fan-out is recovered by Layer 2's `DEFINED`-step re-evaluation, not here.
  Layer 1 guarantees step state; Layer 2 guarantees DAG progress.)
- **Process Workflow** — always safe to re-emit (idempotent; gated on workflow state).
- **Cancel / Revive** — re-derive from workflow/step state (e.g. a Revive whose transaction
  committed but whose Process Workflow emit was lost leaves `DEFINED` steps the sweep will
  re-drive anyway).

Unlike the task receiver (one entity type, one domain), the workflow scheduler's replay
spans multiple event types touching **two** domains (workflow steps *and* tasks), so its
replay logic switches on message type. The step↔task table is what makes the Schedule
replay case decidable.

### Layer 2 — Maintenance Sweep

A periodic **Workflow State Maintenance** event scans persisted state for **all
non-terminal workflows** — i.e. every workflow except `COMPLETE`/`CANCELLED`, including
`PENDING` (a workflow whose initial Process Workflow poke was lost), `FAILED`, and
`TIMED_OUT` (which can still have in-flight steps to drain) — and reconciles each. It trusts
no queue message; it is the backstop for work that has *no message to replay* — most
importantly **dropped `notify` feedback** (best-effort delivery, no buffer), a **failed
enqueue** of a scheduler event (see [State-before-poke](#state-before-poke-why-a-failed-enqueue-is-safe)),
and a crash between two DB writes or a follow-on event never emitted. It lets the engine
recover after a crash and self-heal logical errors. Because feedback is best-effort, this
sweep is **load-bearing for correctness**, not merely a liveness safety net.

**Workflow-level reconciliation** (before the per-step table below): a `PENDING` workflow is
driven `PENDING → RUNNING` (re-driving Process Workflow — its start poke was lost); a
`CANCELLING` workflow re-drives cancellation for any still-live step.

Per non-terminal step, reconcile against its linked task (step↔task table) and the
deadline:

| Step state | Reconciliation |
|---|---|
| `DEFINED` | Re-evaluate startability (effectively re-drive Process Workflow) |
| `PENDING` | Re-emit Schedule Workflow Step (dispatch is idempotent via the non-terminal-task-exists guard); **past deadline** → apply a synthesized `TIMED_OUT` Execution Update |
| `RUNNING` | Look up the step's task: **terminal but step still running** → feedback was lost → **apply a synthesized Execution Update from the task's persisted terminal state**; **task live** → leave alone (feedback will come); **task missing** → zombie → **apply a synthesized `FAILED` Execution Update**; **past deadline** → cancel the live task, then apply a synthesized `TIMED_OUT` Execution Update |
| `CANCELLING` | Task terminal/missing → mark step `CANCELLED` (settle workflow if applicable); task live → re-issue cancel and wait |

**All terminal-outcome reconciliation goes through the shared
[Execution Update](#workflow-step-execution-update) handler** — the sweep *synthesizes* an
Execution Update (`[step ID, new step state]`) rather than marking the step directly. Because
the reducer is keyed by **step**, the sweep can drive an outcome even for a step with **no task
to key on** — a never-dispatched `PENDING` step past its deadline — not only for steps with a
resolvable task. This is
what guarantees the workflow aggregate is updated consistently with the step: a synthesized
`FAILED` marks the workflow `FAILED` (just as a live `FAILED` would), a synthesized `TIMED_OUT`
marks it `TIMED_OUT`, and a synthesized `COMPLETE` re-drives fan-out. A zombie step therefore
does **not** leave the workflow stuck `RUNNING` behind a hidden `FAILED` step — it flows through
the same `FAILED` path as any other failure.

After per-step reconciliation, re-evaluate the workflow aggregate as a backstop for anything the
per-step updates did not already settle: all `COMPLETE` → `COMPLETE`; settled `CANCELLING` →
`CANCELLED`; past deadline → `TIMED_OUT`.

The **deadline is the ultimate backstop**: it guarantees no in-flight step can remain
non-terminal forever, so even a zombie step with no discoverable task is eventually
resolved.

### Startup ordering

```
Initialize (drain + replay buffer)  →  start worker  →  start maintenance timer
    →  start queue processor  →  Subscribe to notify:creator:<engine> (start feedback handler)
```

`Initialize` must complete before the queue processor starts, so buffered (recovered)
messages and fresh messages do not interleave. The `notify` subscription starts **last**:
any step-terminal events broadcast before it is live are missed (best-effort delivery), but
that is exactly the loss the maintenance sweep reconciles, so no ordering constraint binds it
to Initialize. Starting it last simply avoids feeding the worker before recovery has settled.

---

## Design Invariants

1. **Single-threaded scheduler.** Only the scheduler mutates workflow/step state; no
   intra-scheduler races.
2. **Every handler is idempotent / re-entrant** against persisted state. Both recovery
   layers may replay or synthesize a message whose side effects were partially applied.
   The `ValidNextState` guards provide much of this for free (a redundant terminal
   transition is rejected and treated as benign).
3. **Dispatch is idempotent.** Schedule Workflow Step must not submit a duplicate task; it
   checks the step↔task linkage first, where "duplicate" means *a linked non-terminal task
   already exists* (a terminal task from a prior attempt does not block a fresh dispatch).
4. **Every workflow has a deadline** — the liveness backstop for recovery.
5. **Step→task (one-to-many) linkage is persisted** — required by the feedback handler
   (task ID → step) and by both recovery layers (reconcile step state against task state).
6. **The task engine stays workflow-agnostic.** Feedback is a side effect of the audit
   events the engine already writes; it is broadcast by `notify` and consumed by the
   scheduler as a plain subscriber on `notify:creator:<engine>`. Nothing workflow-specific
   is added to the `Task` model. Commands flow through the task client.
7. **Feedback delivery is best-effort; the DB is the source of truth.** Correctness never
   depends on a `notify` event arriving — the maintenance sweep reconciles every live step
   against its task's persisted state. A dropped notification delays, never stalls, a
   workflow. (Same posture as the task engine's poke-plus-maintenance model.)
8. **State-before-poke: every emit is a poke over already-committed state.** A handler
   commits the driving state change to the DB *before* emitting any follow-on scheduler event
   or issuing any task-client command. No emitted event is ever the sole record that work
   exists. Therefore a **failed enqueue** (or a crash between the commit and the enqueue)
   loses only the poke, never the work — the maintenance sweep re-derives the missing
   follow-on from persisted state. Failed enqueues need no special call-site handling beyond
   logging. (This is what makes Invariant 7 hold for scheduler-internal events too, not just
   `notify` feedback.)
9. **Component IPC uses two transports:** the shared Redis reliable-**queue** transport
   for the scheduler's own queue and for commands to the task engine (exactly one new queue);
   and `notify` Redis **pub/sub** for the one long-lived step-feedback subscription.

---

## Deferred to Later Phases

- **User notification** of workflow/step state changes (`FAILED`, `TIMED_OUT`, `COMPLETE`,
  `CANCELLED`). Note these are the workflow engine's *own* audit events; they flow through
  the **same** `notify` producer with `subject:workflow:<id>` / `creator:<user>` routing
  ([`notify/DESIGN.md` §7](../notify/DESIGN.md)), symmetric to how the engine *consumes*
  task events here.
- **The workflow engine's creator identity** — the fixed `Creator` string stamped on every
  step task and used as the feedback channel (`notify:creator:<engine>`). It must be
  distinct from any user-facing creator so the engine's subscription sees only its own step
  tasks. Concrete value/derivation is an implementation detail.
