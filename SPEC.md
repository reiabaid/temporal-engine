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

`EventType`: `TASK_CREATED`, `TASK_STARTED`, `TASK_WINDOW_ENDED`, `TASK_DEADLINE_BREACHED`, `TASK_COMPLETED`, `TASK_RESCHEDULED`, `TASK_CARRIED_FORWARD`, `TASK_DROPPED`, `TASK_CANCELLED`, `TASK_WORK_STARTED`, `NEW_DAY`, `ACTION_PROPOSED`, `AGENT_CHECKPOINT` (§8), `RECURRENCE_CREATED`, `RECURRENCE_ENDED`, `RECURRENCE_GAP` (§9).

- `TASK_CREATED` carries a full snapshot of the task (needed so replay can rebuild it from the log alone) plus the `idempotency_key` of the action that created it, if any.
- **Valid vs transaction time.** `TASK_STARTED` etc. are stamped with the boundary they represent (`occurred_at = scheduled_start`), and `recorded_at` is when the engine noticed. `NEW_DAY.occurred_at` is the local midnight of the new day, *not* the moment of detection — after downtime those differ by hours or days, and "when did the day change" must not depend on when the app was reopened.
- **`NEW_DAY` is forward-only.** Its payload is `{previous_date, new_date, timezone}`. If the configured timezone moves west or the system clock steps back so the local date is earlier than the last announced one, nothing is emitted until the date passes it.
- **Causality.** An `ACTION_PROPOSED` decision carries `caused_by` (ids of the events that triggered it, for agent decisions), and every event a mutator writes for an action carries `action_key` (the action's idempotency key). Trigger -> decision -> effect can be walked from the log alone.
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

Each scenario encodes its correct decision as an assertion: an allowed-action set (empty = "correct answer is to do nothing"), enforced through the real deterministic layer (a proposal must be applied, or held, not merely well-formed), plus a per-scenario constraint. Restraint counts as correct. The same fixtures score the stub in CI (which also proves the scoring discriminates, by running deliberately wrong providers) and any live model by hand (`scripts/live_scenarios.py`). Four scenarios exist. A second, separate benchmark — **State-QA** (`temporal_engine/state_qa.py`, `BENCHMARK.md`) — tests the project's premise directly: can a model *derive* task statuses from a plain history? Its ground truth is produced by running the real engine, cross-checked by an independent oracle and, for daylight saving, by answers derived by hand. Harness and integrity checks are built; no real model has been scored.

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
| `create_recurring_task(title, timezone, frequency, start_time, ...)`, `end_recurring_task(recurrence_id)` | Recurring tasks (§9). |
| `list_pending_actions()`, `confirm_action(key)`, `reject_action(key)` | The confirmation hold. |
| `get_temporal_context(since_seq?, since?, event_limit?, include_finished?)` | See below. |
| `get_state_as_of(at, include_finished?)` | Status of every task as of `at` (needs a UTC offset). Built from what had been recorded by then, with time-derived states brought forward; a future `at` is a projection (`is_projection`). |

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

### Where the database lives

`TEMPORAL_ENGINE_DB` if set, otherwise `~/.temporal-engine/temporal_engine.db` (the folder is created if missing). The default was originally a *relative* path, which meant Claude Desktop, Claude Code and a terminal — each starting the server in a different working directory — would each silently get a separate database, defeating the multi-client design. Clients that should share state must resolve to the same file; an explicit setting is still the clearest way. If `TEMPORAL_ENGINE_TZ` is unset the server warns on stderr at startup, because the default (`UTC`) puts "a new day" at the wrong local time for almost everyone.

### Bounds: nothing unbounded goes in, out, or into a paid API

Found by trying to break the newer code (see the change log). Input and output are bounded, and every limit is applied to *new* input only — replaying the log never re-checks it, so a limit introduced later can never make recorded data unreadable.

- **In.** Titles ≤ 500 characters; a new recurrence's `first_date` within ~5 years of today; `idempotency_key` ≤ 200 characters at the MCP boundary. The **decision log clips** what it records: any argument string over 1,000 characters is cut and marked, and so are error messages that echo a value (an append-only log can never be trimmed afterwards, so a rejected 5 MB argument must not be written whole). An action held for confirmation is stored whole or not at all — oversized ones are rejected rather than clipped, since a clipped hold would corrupt what a later `confirm_action` applies.
- **Out.** `get_temporal_context` lists at most 500 tasks (nearest to now first, with `tasks_total` / `tasks_truncated`), 50 conflicts (`conflicts_truncated`; unbounded, n mutually overlapping tasks make n(n−1)/2 pairs — over a million for 1,500 — that the call would then try to serialize), and 100 pending actions and recurrences.
- **Into a model.** The agent's prompt carries at most `max_context_tasks` (default 100) unfinished tasks — always those the triggering events are about, then nearest to now — and says how many were left out. Every task in the context is sent to a paid API.
- **Creation.** Every caller shares one limit: 1000 tasks made by `create_task` (including calendar imports) per rolling 24 hours (`Runtime(max_creations_per_day=...)`), refused as a logged rejection with the reason. A retried call with an already-applied key is recognised first, so it is `superseded`, not refused. Moves and recurrence occurrences are bounded by their own rules and not counted. The agent has its own, tighter daily budget (§8).
- **Recurrence expansion** that would exceed its safety cap now *raises* instead of silently truncating: a truncated expansion stops short of the present and quietly generates nothing, on every pass, forever.

### One failure must not spread

A broken recurring template (or anything that makes one template's generation raise) is isolated: each template is generated atomically — nothing reaches memory unless the whole template succeeded — the failure is recorded in `scheduler.recurrence_errors` (logged once per distinct message, cleared when it recovers), and every other template *and every task's tick* carries on. Previously a single failure aborted the whole pass, so an unrelated task could stay `SCHEDULED` past its window. Creating a template that cannot be generated is refused and rolled back; a *different* healthy template can still be created while another is broken.

### Failure is visible, never silent

A failed scheduling pass is logged to stderr (stdout is the MCP channel), recorded in `health` and reported by `get_temporal_context`, and retried with exponential backoff. The loop never dies. (Previously one uncaught `TypeError` killed it while `is_scheduler_process` kept saying true.)

### Threading

The SDK runs a *sync* tool on a worker thread. Engine state (task dict, SQLite connection, wake event) is touched by **one thread only** — the event loop the scheduler runs on — so every tool is `async def` and there is nothing to lock. A test asserts it. The blocking SQLite calls are short, which is fine for a local single-user tool.

### Timezones

- All instants are stored in UTC.
- A task's `display_timezone` (validated IANA name) is **for display only**. (v0.1 said it also drove the day boundary; that stopped being true in Phase 1 and the spec was not updated.)
- The **day boundary** — what `NEW_DAY` means — is one engine-level setting, `TEMPORAL_ENGINE_TZ` (validated at startup; default `UTC`, so **set it**: left at UTC, a user at +05:30 gets a "new day" at 05:30 local). It is reported in `get_temporal_context`. One timezone means one user; shift workers and multi-timezone teams are not modelled.
- **The clock reports UTC, and engine arithmetic is done on unambiguous datetimes.** Python compares and subtracts two aware datetimes that share one `ZoneInfo` object by their *wall-clock fields*, ignoring DST: `02:30 - 01:30` is "1 hour" even when two real hours passed, and during a repeated hour the two passes compare equal. A clock returning local time would share its `tzinfo` with any boundary built in the same zone and inherit this. So `RealClock.now()` is UTC, occurrence times are rendered with a *fixed offset* (what a datetime parsed from the log has anyway, so live and replayed values behave identically), local midnights are converted to UTC, and `TemporalContext` carries the user's timezone explicitly. Found by a failing test on a 60-minute occurrence crossing the fall-back hour (it would have failed validation as "ends before it starts" and poisoned every scheduler pass); guarded by tests that fail against the old behaviour.
- DST rules for recurrence are in §9. `exercises/timezone_practice.py` (compare midnight to midnight) is the pattern for the day-boundary logic.

### Move rate limit

At most 3 moves (reschedule/carry-forward) of one task lineage within 24 hours. It is a *rate* limit, not a lifetime total: a runaway loop makes many moves in seconds and is stopped, while a chore legitimately carried forward daily is never stuck. It cannot tell a person from a model — both count — which is the cost of not trusting the caller.

---

## 8. The agent loop — built, OFF by default, never run against a real model

The design's original spine — scheduler event -> agent invocation -> LLM reasoning -> action — with no human in the loop. Without it the engine only *detects*; the model acts when someone talks to it. It is enabled by `TEMPORAL_ENGINE_PROVIDER` (`stub` | `anthropic` | `openai`; plus `TEMPORAL_ENGINE_MODEL`, `TEMPORAL_ENGINE_BASE_URL`, `TEMPORAL_ENGINE_AGENT_MIN_INTERVAL`) because it makes paid API calls unattended. `get_temporal_context` reports its status (`agent`), so it is never invisible.

**What it reacts to.** Only `TASK_WINDOW_ENDED`, `TASK_DEADLINE_BREACHED` and `NEW_DAY`. A task starting, or someone creating or completing something, needs no reasoning.

**Not waking the model for nothing** (deterministic, before any API call): a trigger whose task has since finished is dropped (the user completed it after the window ended); `NEW_DAY` only counts if something is unfinished; a batch with nothing left never calls the provider.

**At-most-once decisions, durably.**
- `AGENT_CHECKPOINT {through_seq, batch_id, proposed, outcomes, note}` is a log event marking how far triggers have been handled. A restart resumes there; a handover to another process agrees on it; **enabling the agent on an existing log starts from now** rather than acting on history.
- Idempotency keys are `agent:<batch_id>:<slot>`, derived from the triggering event ids, not from the vendor's tool-call ids. A crash *after applying but before checkpointing* re-decides the same batch — possibly differently — and lands on the same keys, so nothing applies twice (tested by mutating the derivation and watching the test fail). This closes the re-deciding gap noted in §3.

**Bounded.** At most 10 actions per batch, at most one batch per 30 s (configurable), exponential backoff on provider errors **without** advancing the checkpoint (nothing is lost, and a failing API is not hammered), and every action still passes the validator, the move rate limit (§7) and the confirmation hold (§5) — a model-proposed cancel is held for a human. End-to-end in real processes: a one-second task that keeps ending is rescheduled three times and the fourth attempt is refused, after which the agent stops.

**One runner.** Only the scheduler holder runs it; other processes return `not_scheduler`.

**Threading.** SDK calls block for seconds, so the provider runs on a worker thread — but on a **private deep copy** of the tasks and events. Engine state is still touched only by the event-loop thread; a test hands the provider a context it vandalises and checks live state is unchanged.

**The agent can build a plan.** `create_task` is in the provider's action enum (`new_start`/`new_end` map to the window). The agent fills in `display_timezone` from the engine's timezone when omitted, and creations are capped per day (`max_creations_per_day`, default 30), counted from the decision log so the budget survives restarts; over-budget calls are logged as `rejected`. The reference policy also handles `NEW_DAY` by carrying yesterday's unfinished work into today from 09:00 local, stacked without overlap.

**Known limits.** Untested against any real model. The trigger set is fixed. Two batches within one interval are coalesced by waiting, not merged. Confirmation of held actions needs a human to call `confirm_action`/`reject_action`; nothing notifies them (§6).

---

## 7a. Operating the database

- **Layout versioning.** `PRAGMA user_version` holds the database layout version (`DB_SCHEMA_VERSION`, now 1); `MIGRATIONS` maps `n -> function` taking a database from `n` to `n+1`, each run in its own transaction (a failing step leaves no trace). A database from a NEWER version is refused with `SchemaTooNew` rather than opened and possibly corrupted. Databases created before versioning report 0 and are adopted as 1 (identical layout). Separately, each *event* carries its payload `schema_version`; an event newer than the software understands is quarantined, not misread.
- **Backup and export.** `backup` uses SQLite's online backup API (consistent while in use) and never overwrites; `export` writes the log as JSON lines including `seq`; `import` rebuilds an EMPTY database all-or-nothing and preserves `seq`, because agent checkpoints refer to it. The round trip is tested to reproduce every event exactly.
- **Clock trust.** The engine cannot know whether the clock or the world is right, so it does not guess: a backward step of more than 5 seconds is counted in `scheduler.clock_regressions` with a `last_clock_warning`, and `NEW_DAY` is forward-only regardless (§2), so a step back cannot re-announce a day.
- **Headless and notifications.** `temporal-engine run` runs the scheduler (and agent) with no AI client; `temporal-engine watch` prints events that need a human (window ended, deadline, new day, task start, action held for confirmation) and can run a user-supplied command per event, passing the event through environment variables (task titles are untrusted and never reach the command line).

## 9. Recurring tasks

A recurring task is a **template** that generates ordinary tasks. The template (`RECURRENCE_CREATED`, in the log) holds title, timezone, rule, local start time and optional duration; each **occurrence** is a normal task with its own full lifecycle and carries `recurrence_id` plus `occurrence` (the local date, ISO). Completing, cancelling ("skip") or rescheduling one occurrence never affects another.

**Rules.** `daily` every N days; `weekly` every N weeks on given weekdays (default: the anchor's weekday); `monthly` every N months on given days of month (default: the anchor's day); optionally bounded by `until` (inclusive local date) or `count`, never both. A day of month a month lacks (the 31st in April) is skipped, not clamped. Weeks start on Monday; for `interval > 1` the anchor's week is week 0. All input is validated with readable reasons (§1's rule: untrusted input becomes a logged rejection).

**Time and DST.** `start_time` is local wall-clock time in the template's own timezone, so the UTC instant shifts with DST while the wall clock does not. A local time that does not exist (02:30 on a spring-forward day) resolves *forward* (to 03:30); one that occurs twice (01:30 on a fall-back day) resolves to its *first* occurrence. Duration is absolute: a 2-hour occurrence lasts two real hours across a transition. (See §7 for the datetime hazard this required guarding against.)

**Generation is deterministic and idempotent.** An occurrence's identity is `(template id, local date)`. `materialize` creates every occurrence within the next 7 days that does not already exist, inside the scheduler's write transaction, *before* ticking (so an occurrence created already-over is ended by the same pass). Running it twice, from two processes, or after a crash creates each occurrence exactly once. A cancelled or rescheduled occurrence still counts as existing and is never quietly recreated; a moved occurrence keeps its identity. Creating a template generates its first occurrences in the *same transaction* and returns them, so a crash cannot leave a template with none.

**After downtime.** Only occurrences from the last 24 hours are created (and read as missed, so an agent can respond to them); older ones are *not* — a month of downtime must not spawn a month of missed tasks. The omission is recorded once as `RECURRENCE_GAP {recurrence_id, through_date, skipped}`, so it is auditable rather than silent. A `first_date` in the past behaves the same way.

**Tools.** `create_recurring_task` returns the template and its `instances`; `end_recurring_task` stops generation and leaves existing occurrences alone (cancel any you do not want). `get_temporal_context` lists `recurrences` (rule in words, next three occurrences); ended ones appear with `include_finished`.

---

## 10. Planning facts and calendar import

**Estimates and dependencies.** `Task.estimated_minutes` (1 to 10080) and `Task.depends_on` (up to 20 ids of tasks that ALREADY exist, which also makes cycles impossible) are supplied facts, carried by `TASK_CREATED`, kept across reschedules, and validated at the boundary. `planning.py` derives from them, never raising events or refusing anything: `estimate_exceeds_window`, `remaining_estimate_minutes`, `over_estimate`, `projected_finish` and `at_risk_of_missing_deadline` (arithmetic, not prediction); `is_blocked` / `blocked_by` / `abandoned` / `scheduled_before_dependency`. A dependency is judged by the head of its lineage (a moved dependency is followed to its replacement); completed (even late) satisfies it; dropped or cancelled is reported as `abandoned` and does not block. A blocked task can still be completed: out-of-order work is normal, and refusing it would be the engine overreaching (the same stance as overlaps).

**Calendar import** (`calendar_import.py`, tool `import_calendar`). Pull-only and text-only: the caller passes the text of an `.ics` export, so there are no credentials and no network surface. Identity is `(source, UID)`. Same file again: nothing. Changed time: `reschedule_task` keyed on the head being moved (so a calendar that flaps A, B, A is followed, while a crash-retry is idempotent). `STATUS:CANCELLED`: `cancel_task`. A task finished or removed locally is never reopened: the local decision wins. Skipped with a reason, never guessed: all-day events, no end time, recurring events (RRULE, RECURRENCE-ID), unknown TZIDs. Times are normalised to UTC on the way in. Two calendars overlapping are reported by the existing conflict detection, which does not pick a winner. Bounds: 1,000,000 characters, 500 events, source names `[A-Za-z0-9_.-]{1,40}`. Not handled: title changes on an existing event, live sync, attendees, alarms.

---

## Not done (explicit)

- **Recurrence is a deliberate subset** (§9): daily/weekly/monthly with interval, weekdays, days of month, `until`/`count`. No iCalendar RRULE strings, no yearly, no "nth weekday of the month", no exception dates (skip an occurrence by cancelling it), no per-series deadlines, and a template cannot be edited (end it and create a new one). The 7-day generation horizon and 24-hour catch-up lookback are fixed constants.
- **Live calendar sync, multiple editing agents beyond the write-lock protocol, multi-user / multi-timezone.** Calendar import (§10) is one-way, manual and text-only.
- **The agent loop is built but only ever run against the stub.** It is off by default, and no real model has driven it (§8).
- **Live model behaviour.** No live call to any LLM has been made; provider code is tested against fakes and the scenarios against a stub.
- **Claude Desktop** (the separate app) was never tested; real-client testing was Claude Code only.
- **Push notifications from the engine itself.** `temporal-engine watch --exec` (§7a) is the hook; whether any AI client renders a server push is untested.
- **Suspend/resume.** Detection lag after the machine sleeps is bounded by the refresh interval by design, but was not measured on real hardware.
- **Directly-called `cancel_task` is not held** (§5), only model-proposed ones via the provider path.
- **Idempotency does not cover re-deciding for a human/client caller** (§3): the agent loop closes this gap for itself by deriving keys from events (§8), but a client that simply asks a model again gets new ids.
- **Quarantined events are surfaced only as a count**; there is no tool to inspect or repair them.
- **The move limit is actor-blind.**
- **Durations are a single estimate**, not a distribution or a learned model; nothing predicts overruns beyond the arithmetic in §10.
- **No schema migration has ever been needed**, so the migration machinery (§7a) is tested with synthetic migrations only.
- **The creation limit is a rate limit, not a size limit.** Nothing caps total log size or task count.
- **Prompt injection is mitigated, not solved** (§3).
- **PID reuse** can make a dead lock holder look alive (fails safe).

---

## Change log

### After v0.2 — finishing pass (closing the audit's gaps)

Built: estimates and dependencies (§10); calendar import (§10); database layout versioning and newer-event quarantine, backup/export/import (§7a); a creation limit shared by every caller (§7); clock-regression visibility (§7a); the `temporal-engine` CLI with headless `run` and notification-hook `watch` (§7a); two more State-QA categories, `historical` and `dependencies`, each with a flawed solver that must fail exactly that category. Every new protection was mutation-checked (removing it fails a test). Deliberately NOT done, and stated in "Not done": any run against a real model, Claude Desktop, live calendar sync, multi-user.

### After v0.2 — third audit (the whole system against the original brief)

See [AUDIT.md](AUDIT.md) for the traceability matrix. Fixes: `NEW_DAY` forward-only with the timezone in its payload; event causality (`caused_by`, `action_key`); the agent can `create_task` with a per-day cap; `get_state_as_of` / `Runtime.state_as_of` answer "what was true at time T" for the past and project the future from the clock alone (property-tested against live snapshots); the reference policy handles `NEW_DAY`. New acceptance test replays the brief's day story end to end.

### After v0.2 — second audit (of the newer code)

Method as before: try to break it, believe only what reproduces. Four suspicions, four confirmed: (1) one broken recurring template made the whole scheduler pass fail, freezing every task's tick; (2) an absurd `first_date` was accepted, produced *zero* occurrences (the expansion cap silently truncated before the present) and burned ~0.5 s of CPU on every 5-second pass; (3) a 5 MB title was accepted; (4) 1,500 overlapping tasks make 1.1 million conflict pairs that `get_temporal_context` would serialize. The same unbounded-output problem existed in the agent's prompt. The regression tests then found a hole the first fix missed: a *rejected* oversized call still wrote its entire argument into the log (mutation-checked: without the clipping, its test fails; likewise for isolation and the cap). Two of my own tests asserted nothing (`... or True`) and were replaced — a habit worth naming.

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

### After v0.2 — recurrence

Built §9. Writing its tests exposed a hazard that predated recurrence and would have bitten anything on a DST day: two aware datetimes sharing one `ZoneInfo` object are compared and subtracted by wall-clock time. A 60-minute occurrence across the fall-back hour failed validation as "ends before it starts"; the same trap sat latent in `RealClock` (local-zone `now` vs same-zone boundaries misjudges a repeated hour), the stub's slack arithmetic, and `worked_seconds`. Fixed as described in §7. Two mutations confirmed the guards: reverting the fixed offset fails the DST tests; removing the "already exists" check fails five recurrence tests.

### After v0.2 — the agent loop

Verified the rewritten server in the real client first (validation, overlap reporting, work-start tracking, scheduler health and the `Asia/Kolkata` boundary all behaved as specified). Then built §8. A first version of the crash-recovery test passed even with the key derivation removed, because the test provider returned the same key every time; it now returns a fresh id per call, and fails against the mutant.

### Phase 3 (earlier)

Startup catch-up (the loop slept without ticking, so a window elapsing while the app was closed read `SCHEDULED` after a restart — initially asserted to work without having been tested through the server); `NEW_DAY` lost across downtime; stale-lock recovery; `reschedule_task` not returning the new task's id; stale sleep (a task created while the loop slept toward midnight was not ticked until then — fixed with the wake event, which is now safe because tools share the loop's thread).
