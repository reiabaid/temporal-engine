# Temporal Context Protocol — Spec (v0.2)

Status legend: **STABLE** = build against this without expecting it to move; **DRAFT** = the current best answer, expected to change once a real integration tests it; **DEFERRED** = named on purpose, not designed yet.

This document is the contract between the deterministic temporal engine and any LLM reasoning layer sitting on top of it. It describes what the code does *now*; the last section records what is not done. See `PLAN.md` for the build sequence.

v0.2 supersedes v0.1 after an audit of the running system (see "Change log"). The main structural change: **the event log is the only source of truth, and every write is validated against the log's current state inside a write transaction** (§7). Most of the bugs the audit found were the same bug — a process acting on a stale in-memory picture of the world.

---

## 1. State machine — STABLE

### States

| State | Meaning |
|---|---|
| `CREATED` | Exists but has no scheduled window: an inbox item, possibly with only a `deadline`. |
| `SCHEDULED` | Has a `scheduled_start` (and normally a `scheduled_end`) in the future. |
| `ACTIVE` | Now is inside the window. Derived from the clock; it means the window opened, **not** that anyone began work (see "Actual start"). |
| `WINDOW_ENDED` | `scheduled_end` passed and the task is not complete. Soft miss. |
| `OVERDUE` | `deadline` passed and the task is not complete. Hard miss. |
| `COMPLETED` | Done, on time. |
| `COMPLETED_LATE` | Done after `WINDOW_ENDED` or `OVERDUE`. |
| `RESCHEDULED` | Terminal — superseded by a new task at a new time. |
| `CARRIED_FORWARD` | Terminal — superseded by a new task, typically on a later day. |
| `DROPPED` | Terminal — abandoned. |
| `CANCELLED` | Terminal — removed as no longer relevant. |

`WINDOW_ENDED` and `OVERDUE` are separate because a planned slot ending is not a failure, while a missed deadline is. A task with no `deadline` only ever passes through `WINDOW_ENDED`.

### Transition table

| From | To | Trigger | Who |
|---|---|---|---|
| `SCHEDULED` | `ACTIVE` | `now >= scheduled_start` | engine |
| `ACTIVE` | `WINDOW_ENDED` | `now >= scheduled_end` | engine |
| `CREATED` / `SCHEDULED` / `ACTIVE` / `WINDOW_ENDED` | `OVERDUE` | `now >= deadline` (only if set) | engine |
| `CREATED` / `SCHEDULED` / `ACTIVE` | `COMPLETED` | marked done | `complete_task` |
| `WINDOW_ENDED` / `OVERDUE` | `COMPLETED_LATE` | marked done after the miss | `complete_task` |
| any non-terminal | `RESCHEDULED` | new time, same intent | `reschedule_task` |
| any non-terminal | `CARRIED_FORWARD` | moved to a later day | `carry_forward_task` |
| any non-terminal | `DROPPED` | abandoned | `cancel_task(mode=drop)` |
| any non-terminal | `CANCELLED` | no longer relevant | `cancel_task(mode=cancel)` |

The initial state is `SCHEDULED` if a `scheduled_start` is given, else `CREATED`. Every arrow above is the *only* legal path; anything else is rejected by `Task.apply_transition` whoever asks. A `CREATED` task never becomes `ACTIVE`/`WINDOW_ENDED` (it has no window) and is scheduled by being superseded via `reschedule_task`.

### Validation at the boundary (times come from untrusted callers)

Enforced in `validate_schedule`, used by task creation and by every move:

- every datetime is timezone-aware (a naive one is rejected — comparing it with aware times raises `TypeError` deep in the engine);
- `scheduled_end` requires `scheduled_start`, and must be **after** it;
- `scheduled_start` must be **before** the `deadline`. A window may *run past* the deadline (a plan that will breach it is still a plan); a task may not *start* after it;
- a reschedule inherits the task's deadline, so moving a task past its own deadline is rejected — extending the deadline must be said out loud via `new_deadline`;
- `title` is non-empty and `timezone` is a real IANA name.

A rejection is a logged `rejected` decision with a readable reason (§4), never an exception into the scheduler.

### Actual start (a fact, not a state)

`ACTIVE` is clock-derived. To answer "how long have I been working on this?" the engine needs to be told when work really began: `start_task` records `actual_start` (event `TASK_WORK_STARTED`) without changing status. Task summaries then include `worked_seconds` — elapsed time is arithmetic, so the server computes it rather than a model.

---

## 2. Event schema — STABLE

```
TemporalEvent {
    schema_version: int
    id: str                      # unique
    seq: int | None              # position in the log; assigned by storage; the cursor
    event_type: EventType
    occurred_at: datetime (UTC)  # valid time -- when it happened in the world
    recorded_at: datetime (UTC)  # transaction time -- when the engine noticed
    task_id: str | None
    payload: dict
}
```

`EventType`: `TASK_CREATED`, `TASK_STARTED`, `TASK_WINDOW_ENDED`, `TASK_DEADLINE_BREACHED`, `TASK_COMPLETED`, `TASK_RESCHEDULED`, `TASK_CARRIED_FORWARD`, `TASK_DROPPED`, `TASK_CANCELLED`, `TASK_WORK_STARTED`, `NEW_DAY`, `ACTION_PROPOSED`.

- `TASK_CREATED` carries a full snapshot of the task (needed so replay can rebuild it from the log alone) plus the `idempotency_key` of the action that created it, if any.
- **Valid vs transaction time.** `TASK_STARTED` etc. are stamped with the boundary they represent (`occurred_at = scheduled_start`), and `recorded_at` is when the engine noticed. `NEW_DAY.occurred_at` is the local midnight of the new day, *not* the moment of detection — after downtime those differ by hours or days, and "when did the day change" must not depend on when the app was reopened.
- A task's `last_transition_at` is the event's `recorded_at`, in both the live path and replay, so a live task and its replayed copy agree (tested).
- `schema_version` exists because the log is append-only and cannot be edited in place.

---

## 3. Action schema — DRAFT

```
ActionCall {
    idempotency_key: str
    action: "create_task" | "start_task" | "complete_task" | "reschedule_task"
          | "carry_forward_task" | "cancel_task"
    task_id: str | None
    args: dict                    # e.g. new_start, new_end, new_deadline, mode, title...
    reason: str                   # the caller's stated justification: logged, never executed
    requires_confirmation: bool   # true => held for a human (see §5)
}
```

**Idempotency is durable.** Whether an action was already applied is derived from the log (`View.seen_keys`), so it survives restarts and is shared between processes. Only *applied* keys are remembered: a rejected action may be retried later — the state it was refused against can change — so remembering it would turn a transient refusal into a permanent one. Every mutating MCP tool takes an optional `idempotency_key`; a retried `create_task` is answered with the *original* task rather than a duplicate. If the caller supplies no key, a random one is generated (no deduplication possible).

### Revisions from writing the second provider (Phase 4)

**Evidence level: written against each vendor's documented response shape and tested with fake clients shaped like them; no live call to either vendor has been made.** Design findings, not benchmark results.

- **Model output is untrusted.** Tool-call arguments can be malformed (OpenAI delivers them as a JSON *string*, which can be truncated), datetimes can lack an offset, fields can be missing. Parsing never raises: anything unusable becomes an `ActionCall` with `action: "unparseable_tool_call"`, which the deterministic layer rejects and logs.
- **`idempotency_key` from a vendor's tool-call id** protects re-*applying* a call whose acknowledgment was lost. It does **not** protect against re-*deciding*: asking the model again yields new ids and possibly a different decision.
- **Vendor differences are confined to wire format** (tool wrapping, `input_schema` vs `parameters`, system prompt as argument vs message, `tool_calls` `None` vs empty, arguments dict vs JSON string). Prompt, schema and parsing live in `provider_common.py`; each vendor module is ~50 lines. `base_url` lets the OpenAI provider drive local servers (e.g. Ollama).
- The action schema itself did not need to change. It stays DRAFT until a live model has been run against the scenarios.

### Prompt-injection hygiene

Task titles are text written by people (and, later, by calendar invites) that gets embedded in a model's prompt. Mitigations, layered: titles are flattened (no newlines/control characters, so they cannot forge lines of structure) and length-capped; the system prompt says titles are untrusted data and instructions inside them are never to be followed; the deterministic layer bounds what any model can do regardless (§1 validation, the move rate limit); and a model-proposed `cancel_task` is **held for a human** rather than applied (§5). This reduces the risk; it does not remove it — a model can still be *persuaded* to propose a bad reschedule, which the validator will accept if it is legal.

### Decision fixtures (`temporal_engine/scenarios.py`)

Each scenario encodes its correct decision as an assertion: an allowed-action set (empty = "correct answer is to do nothing"), enforced through the real deterministic layer (a proposal must be applied, or held, not merely well-formed), plus a per-scenario constraint. Restraint counts as correct. The same fixtures score the stub in CI (which also proves the scoring discriminates, by running deliberately wrong providers) and any live model by hand (`scripts/live_scenarios.py`). Four scenarios exist; the fuller list (deadlines, recurrence, dependencies, multi-day) is Phase 6.

---

## 4. Decision log — STABLE

Every `ActionCall` is logged as an `ACTION_PROPOSED` event whatever its fate:

```
payload: {
    action_call: ActionCall,
    outcome: "applied" | "rejected" | "superseded" | "pending_confirmation",
    rejection_reason: str | None
}
```

`applied` — ran, followed by its events. `rejected` — validation or the state machine refused it. `superseded` — that key was already applied. `pending_confirmation` — held for a human; nothing ran. A held action is resolved by a later `ACTION_PROPOSED` with the same key (`applied` on confirm, `rejected` on decline). The log of what was *considered* — including rejections — is what debugging and the benchmark need.

---

## 5. Primitive surface (MCP tools)

All tools are `async` (§7, Threading). Mutating tools take an optional `idempotency_key` and return `{outcome, rejection_reason, idempotency_key, task, new_task, overlaps_with}`.

| Tool | Notes |
|---|---|
| `create_task(title, timezone, scheduled_start?, scheduled_end?, deadline?)` | Validated (§1). Returns `new_task`. Reports `overlaps_with`. |
| `start_task(task_id)` | Records `actual_start`. |
| `complete_task(task_id)` | Late if the window/deadline had passed. |
| `reschedule_task(task_id, new_start, new_end, reason, new_deadline?)` | Supersedes; use the returned `new_task.id`. |
| `carry_forward_task(...)` | Same, to a later day. |
| `cancel_task(task_id, mode, reason)` | `drop` or `cancel`. |
| `list_pending_actions()`, `confirm_action(key)`, `reject_action(key)` | The confirmation hold. |
| `get_temporal_context(since_seq?, since?, event_limit?, include_finished?)` | See below. |

**`get_temporal_context`** returns: `now`, `day_boundary_timezone`, `is_scheduler_process`, `scheduler` health (`last_pass_at`, `last_tick_at`, `last_error`, `consecutive_errors`), unfinished `tasks` (with `worked_seconds` where started), `conflicts` (overlapping pairs), `pending_actions`, `events`, `events_truncated`, `cursor`, and `quarantined_events` (a count). To learn what happened while away, pass the previous `cursor` as `since_seq` (or an ISO time as `since`): newer events only, oldest first, never skipping. `include_finished` adds recently finished tasks (last 50), so "what did I finish?" is answerable; without it, finished tasks are omitted. It refreshes from the log first, so it reflects other processes' writes.

**Overlaps are reported, never refused.** People double-book on purpose; refusing would be the engine overreaching. Windows are half-open `[start, end)`.

### Human-in-the-loop — DECIDED and built: a hold, not an `ask_user` tool

v0.1 listed `ask_user(question)`. It is dropped: the agent already talks to the human in the conversation; a tool that blocks hangs the MCP request; elicitation depends on client support that can't be assumed. What the *server* owns is the **hold**. An `ActionCall` with `requires_confirmation` is logged as `pending_confirmation` and applied only by `confirm_action(key)` (which re-validates against the *current* state, so a task that finished while the hold waited is rejected, not forced) or discarded by `reject_action(key)`. It is non-blocking, works in every MCP client, and survives restarts because pending state is derived from the log. Confirm/decline check "is it still pending?" inside the write transaction, so two processes racing to resolve one hold cannot both win.

**Which actions are held:** those a *model proposes through the provider path* and that discard work — currently `cancel_task`. **Not held:** calls a client makes directly through the MCP tools. There the model is the client, in a conversation with the human, and the host's own tool-permission prompt is the gate. This is a deliberate boundary, and a known limit (see "Not done").

---

## 6. Event delivery — split by direction

- **Engine → itself (scheduler wake-up): STABLE.** Boundaries the process already knows about are ticked at the exact instant. Sleeping is bounded by a refresh interval (default 5 s, `TEMPORAL_ENGINE_REFRESH_SECONDS`) so a process notices boundaries created by *another* process and so a machine suspended mid-sleep (asyncio timers do not count suspended time) is at most that late. This is a local database check, not polling an LLM.
- **Engine → LLM, pull: STABLE.** `get_temporal_context` with a cursor. Works in every MCP client; it is what "what happened while I was away" relies on.
- **Engine → LLM after downtime: STABLE, verified.** If the host app (and the server with it) was closed while boundaries passed, the next start ticks immediately and catches up every missed transition before serving; `NEW_DAY` is recovered too (one event, with previous/new dates, however many days elapsed). **Nothing is generated while the app is closed** — there is no background daemon; state is corrected on the next start.
- **Engine → human, push: DRAFT, and the only real-client observation is negative.** The server sends no MCP notification. In Claude Code a task was created and the session left idle across its whole window; nothing surfaced until `get_temporal_context` was called, at which point the state was already correct. **Treat pull as the only reliable channel.** Whether any client renders a server-initiated notification is untested.

---

## 7. Storage, concurrency, timezones — STABLE

### The log is the truth; every write is validated against it

- A `View` (tasks, applied idempotency keys, pending holds, created-by-key index, last seq) is derived from the log and refreshed *incrementally* (only rows after its last seq).
- `Store.transaction()` takes the write lock (`BEGIN IMMEDIATE`), refreshes the view, lets the caller validate and mutate against exactly the state that will be committed, and commits. Any failure rolls back **and rebuilds the view**, so memory can never end up ahead of the log.
- One decision is several events (e.g. `TASK_RESCHEDULED` + `TASK_CREATED`); `append_events` writes them atomically — all or none. Separate commits would let a crash between them lose the task while the decision log said "applied".
- **Quarantine.** A bad event (illegal transition, unknown task, an event type from a newer version) is skipped, recorded (`quarantined_events` count) and logged rather than making the database unopenable. Strict `replay()` without a quarantine list still raises, which is what tests want. The log is never edited to "fix" it.

### Several processes, one database

The realistic deployment is two MCP clients (e.g. Claude Desktop and Claude Code) on one config. Every process runs the same loop (`runtime.py`): try to become the scheduler (an exclusive lock file, retried every pass), refresh its view, and if it is the scheduler tick inside a write transaction. So:

- exactly one process ticks; a stale process cannot corrupt the log because its writes are validated after it has seen everyone else's;
- if the scheduler exits while another client stays open, that client takes over on its next pass (previously the lock was tried only at startup, leaving nobody ticking);
- the day tracker is seeded from the log (last `NEW_DAY`, else the day the log began), so a handover neither repeats nor drops a `NEW_DAY`; it is reset after any failed pass;
- **Stale-lock recovery.** The lock file records the holder's PID. A graceful close releases it; a force-kill leaves it behind, so a lock whose PID is dead (or whose file is empty/corrupt) is reclaimed. Liveness on Windows uses `OpenProcess`/`GetExitCodeProcess` — `os.kill(pid, 0)` must not be used there, as Windows treats any signal but CTRL_C/CTRL_BREAK as "terminate the process". A reused PID makes a dead holder look alive; this fails safe (no duplicate scheduler).

### Failure is visible, never silent

A failed scheduling pass is logged to stderr (stdout is the MCP channel), recorded in `health` and reported by `get_temporal_context`, and retried with exponential backoff. The loop never dies. (Previously one uncaught `TypeError` killed it while `is_scheduler_process` kept saying true.)

### Threading

The SDK runs a *sync* tool on a worker thread. Engine state (task dict, SQLite connection, wake event) is touched by **one thread only** — the event loop the scheduler runs on — so every tool is `async def` and there is nothing to lock. A test asserts it. The blocking SQLite calls are short, which is fine for a local single-user tool.

### Timezones

- All instants are stored in UTC.
- A task's `display_timezone` (validated IANA name) is **for display only**. (v0.1 said it also drove the day boundary; that stopped being true in Phase 1 and the spec was not updated.)
- The **day boundary** — what `NEW_DAY` means — is one engine-level setting, `TEMPORAL_ENGINE_TZ` (validated at startup; default `UTC`, so **set it**: left at UTC, a user at +05:30 gets a "new day" at 05:30 local). It is reported in `get_temporal_context`. One timezone means one user; shift workers and multi-timezone teams are not modelled.
- DST is handled by not landing on ambiguous/nonexistent local hours where avoidable (see `exercises/timezone_practice.py`: compare midnight to midnight). Recurring tasks colliding with a DST transition are DEFERRED with recurrence itself.

### Move rate limit

At most 3 moves (reschedule/carry-forward) of one task lineage within 24 hours. It is a *rate* limit, not a lifetime total: a runaway loop makes many moves in seconds and is stopped, while a chore legitimately carried forward daily is never stuck. It cannot tell a person from a model — both count — which is the cost of not trusting the caller.

---

## Not done (explicit)

- **Recurrence.** The `recurrence` field exists and nothing uses it. Needs an RRULE implementation and the DST-collision design. Phase 5.
- **External calendars, multiple editing agents beyond the write-lock protocol, multi-user / multi-timezone.**
- **No automatic agent loop.** The server does not itself call an `LLMProvider` when events fire: the model is the MCP *client*, and acts when someone talks to it. The provider layer (`decide` -> `apply_action`, including the confirmation hold) is library code exercised by tests and scripts; wiring it to `Runtime` so events trigger decisions unattended is unbuilt. Until then, held actions only arise if something routes provider output through `Runtime.execute`.
- **Live model behaviour.** No live call to any LLM has been made; provider code is tested against fakes and the scenarios against a stub.
- **Claude Desktop** (the separate app) was never tested; real-client testing was Claude Code only.
- **Notifications** to a human: none sent; whether any client renders one is untested.
- **Suspend/resume.** Detection lag after the machine sleeps is bounded by the refresh interval by design, but was not measured on real hardware.
- **Directly-called `cancel_task` is not held** (§5), only model-proposed ones via the provider path.
- **Idempotency does not cover re-deciding** (§3).
- **Quarantined events are surfaced only as a count**; there is no tool to inspect or repair them.
- **The move limit is actor-blind.**
- **Prompt injection is mitigated, not solved** (§3).
- **PID reuse** can make a dead lock holder look alive (fails safe).

---

## Change log

### v0.1 → v0.2 — the audit

Found by testing the running system, not by reading code. Each has a permanent regression test; the worst two were each confirmed by reproducing the failure first, and the fixes were confirmed by re-introducing the bug and watching the tests fail.

1. **One bad input silently killed the scheduler.** `create_task` accepted a datetime with no UTC offset; the next scheduler pass raised `TypeError`, nothing caught it, and the loop died while `is_scheduler_process` stayed true. Fixed by validation at the boundary (§1) and by making the loop unkillable and its health visible (§7).
2. **Two clients could corrupt the log so no server could start.** B held a stale view, completed a task A had rescheduled, the log gained an illegal `RESCHEDULED → COMPLETED`, and replay raised on every start. Fixed by validating writes under the write lock against the refreshed log (§7) and by quarantining bad events. A first version of the real-process regression test passed even without the fix, because client B refreshed by reading before it acted; the test was corrected until it failed against the mutant.
3. **Bad values were accepted:** a deadline-only task never became overdue; a window ending before it started; a reschedule past the task's own deadline (overdue the instant it existed). Fixed (§1). Also found on the way: an unscheduled task could not even be completed.
4. **Threads.** Sync tools ran on worker threads sharing state with the scheduler; `asyncio.Event.set()` from another thread is not thread-safe (the live wake test had passed by luck). Fixed: async tools (§7).
5. **Non-atomic multi-event writes** could lose a task mid-reschedule. Fixed: atomic `append_events`.
6. **Idempotency was not end to end** (random key per MCP call; keys only in memory). Fixed: client-supplied keys, durable, shared.
7. **`get_temporal_context` didn't match this spec** (last 20 events, unfinished tasks only, full-log read per call). Fixed: cursor, `since`, `include_finished`, indexed queries.
8. **Timezone:** spec drift (above); `NEW_DAY.occurred_at` was detection time; `timezone` unvalidated.
9. **Move cap** was a lifetime total and could strand a chore; now a rate limit.
10. **No record of actual work start; overlaps unreported; injection unmitigated; confirmation hold unenforced.** Built (§1, §5, §3).
11. **Scheduler handover:** the lock was tried once at startup, so a surviving client never took over. Fixed.
12. `last_transition_at` differed between live and replayed tasks. Fixed and tested.

### Phase 3 (earlier)

Startup catch-up (the loop slept without ticking, so a window elapsing while the app was closed read `SCHEDULED` after a restart — initially asserted to work without having been tested through the server); `NEW_DAY` lost across downtime; stale-lock recovery; `reschedule_task` not returning the new task's id; stale sleep (a task created while the loop slept toward midnight was not ticked until then — fixed with the wake event, which is now safe because tools share the loop's thread).
