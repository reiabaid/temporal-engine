# Temporal Context Protocol — Spec (v0.1)

Status legend: **STABLE** = build against this without expecting it to move; **DRAFT** = the current best answer, expected to change once a real integration (a second LLM provider, or a real MCP client) tests it; **DEFERRED** = named on purpose, not designed yet.

This document is the contract between the deterministic temporal engine and any LLM reasoning layer sitting on top of it. See `PLAN.md` for the phase-by-phase build sequence this spec supports.

---

## 1. State machine — STABLE

### States

| State | Meaning |
|---|---|
| `CREATED` | Task exists, not yet scheduled (no `scheduled_start` set). |
| `SCHEDULED` | Has a `scheduled_start`/`scheduled_end` window in the future. |
| `ACTIVE` | Current time is inside `[scheduled_start, scheduled_end)`. |
| `WINDOW_ENDED` | `scheduled_end` has passed; task not marked complete. Soft miss. |
| `OVERDUE` | `deadline` has passed; task not marked complete. Hard miss. |
| `COMPLETED` | Marked done, on time. |
| `COMPLETED_LATE` | Marked done, after `WINDOW_ENDED` or `OVERDUE`. |
| `RESCHEDULED` | Terminal — superseded by a new `Task` instance at a new time. |
| `CARRIED_FORWARD` | Terminal — superseded by a new `Task` instance, typically on a later day. |
| `DROPPED` | Terminal — explicitly abandoned. |
| `CANCELLED` | Terminal — removed before it became relevant. |

### Why `WINDOW_ENDED` and `OVERDUE` are separate states

A scheduled block ending is not a failure — plenty of tasks have no hard deadline, just a planned slot. A missed deadline is qualitatively different: it's the state a task reaches when a `deadline` field (not just `scheduled_end`) has passed. Conflating them was flagged early in design as the most common modeling mistake for this kind of system, so the two are kept distinct even though a task with no `deadline` will only ever pass through `WINDOW_ENDED`.

### Transition table

| From | To | Trigger | Who may trigger |
|---|---|---|---|
| `CREATED` | `SCHEDULED` | `scheduled_start`/`scheduled_end` assigned | engine (on task creation with times set) |
| `SCHEDULED` | `ACTIVE` | `now >= scheduled_start` | engine (deterministic) |
| `ACTIVE` | `WINDOW_ENDED` | `now >= scheduled_end` | engine (deterministic) |
| `SCHEDULED` / `ACTIVE` / `WINDOW_ENDED` | `OVERDUE` | `now >= deadline` (only if `deadline` is set) | engine (deterministic) |
| any non-terminal | `COMPLETED` | user/LLM marks done, before `scheduled_end`/`deadline` | LLM or user, via `complete_task` |
| `WINDOW_ENDED` / `OVERDUE` | `COMPLETED_LATE` | user/LLM marks done, after the miss | LLM or user, via `complete_task` |
| any non-terminal | `RESCHEDULED` | new time chosen for the same intent | LLM or user, via `reschedule_task` |
| any non-terminal | `CARRIED_FORWARD` | moved to a later day, typically at a `NEW_DAY` boundary | LLM or user, via `carry_forward_task` |
| any non-terminal | `DROPPED` | explicitly abandoned | LLM or user, via `cancel_task` (drop path) |
| any non-terminal | `CANCELLED` | removed before relevance | user, via `cancel_task` (cancel path) |

**Rule that must never be violated:** every arrow above is the *only* legal path between two states. The state-machine validator (Phase 1/2) rejects any transition not in this table, regardless of what an LLM requests — this is the deterministic/probabilistic boundary made concrete.

---

## 2. Event schema — STABLE

```
TemporalEvent {
    schema_version: int          # bump on any shape change to this struct
    id: str                      # stable unique id, for idempotent replay
    event_type: EventType
    occurred_at: datetime (UTC)  # valid time -- when it happened in the world
    recorded_at: datetime (UTC)  # transaction time -- when the engine noticed
    task_id: str | None
    payload: dict                # event_type-specific extra fields
}
```

`EventType` values (Phase 1 set): `TASK_CREATED`, `TASK_STARTED`, `TASK_WINDOW_ENDED`, `TASK_DEADLINE_BREACHED`, `TASK_COMPLETED`, `NEW_DAY`, `ACTION_PROPOSED` (see §4).

`TASK_CREATED` was added during the storage-layer build (not in the original v0.1 draft): replay needs to reconstruct a task from nothing but its event log, and a log of only status *changes* has no event describing the task's original fields (title, times, timezone). `TASK_CREATED`'s payload carries a full snapshot of those fields at creation time.

The `occurred_at` / `recorded_at` split (bitemporal: valid time vs. transaction time) is what makes "what happened while I was away" and "what did we know at 4pm yesterday" both answerable from the same log, without special-casing either query.

`schema_version` exists because this log is append-only and can never be edited in place — a future field addition to any event type needs a version to branch on, decided now rather than discovered as a migration crisis later.

---

## 3. Action schema — DRAFT

```
ActionCall {
    idempotency_key: str          # caller-generated; same key = same action, applied once
    action: str                   # "complete_task" | "reschedule_task" |
                                   # "carry_forward_task" | "cancel_task" | "ask_user"
    task_id: str | None
    args: dict
    reason: str                   # LLM's stated justification, logged, not executed
    requires_confirmation: bool   # true => must be surfaced to a human before applying
}
```

Marked DRAFT because a second LLM provider's native tool-calling shape (Phase 4) is expected to reveal an assumption baked in here that doesn't hold generally — this schema should be revisited, not treated as final, the first time that happens.

`idempotency_key` is required, not optional: if an action is applied to the event log but the acknowledgment is lost before the caller sees it, a retried call with the same key must be a no-op rather than a double-apply.

---

## 4. Decision log — STABLE

Every `ActionCall` an LLM proposes is logged as an `ACTION_PROPOSED` event, whether or not it was ultimately applied:

```
payload: {
    action_call: ActionCall,
    outcome: "applied" | "rejected" | "superseded",
    rejection_reason: str | None   # set when outcome == "rejected"
}
```

This exists even for rejected proposals — it's the record used to debug a bad decision after the fact, and it's what a benchmark (see `PLAN.md` Phase 6) needs to check "did the model even consider the right action," not just "did the final state end up correct."

---

## 5. Primitive surface

### STABLE

- `create_task(title, timezone, scheduled_start?, scheduled_end?, deadline?)` — missing from the original v0.1 draft entirely (the primitive list covered reading state and mutating an *existing* task, but never creating one — found only once Phase 3's MCP server needed a real tool an agent could call for "plan my day").
- `get_temporal_context()` — returns current time, all non-terminal tasks, and events since the caller's last-seen checkpoint. This subsumes what might otherwise be separate `get_upcoming_events`/`get_overdue_events` calls — one call for "everything relevant right now."
- `complete_task(task_id, idempotency_key)`
- `reschedule_task(task_id, new_start, new_end, idempotency_key)`
- `carry_forward_task(task_id, new_start, new_end, idempotency_key)`
- `cancel_task(task_id, mode: "drop" | "cancel", idempotency_key)`

Each mutator is a thin wrapper that calls the state-machine validator (§1) before writing an event — none of them write state directly.

### DRAFT

- `ask_user(question, task_id | None)` — the primitive itself is uncontroversial, but its **transport** is not standardized: does the call block until a human answers, return a `PENDING` status the client polls, or map to MCP's elicitation feature (unsupported in most clients as of this writing)? This is intentionally left open until Phase 3's real-client test settles it — see §6.

---

## 6. Event delivery — split by direction, STABLE vs DRAFT

- **Engine → itself (scheduler wake-up): STABLE.** `next_wake_time()` is a pure computation over task boundaries and the next local-midnight; the process holding the scheduler lock (see storage model, §7) sleeps exactly until that instant. No polling.
- **Engine → LLM, pull (`get_temporal_context` on the next call): STABLE.** Works regardless of what any given MCP client supports, since it's a normal request/response call. This is the mechanism "what happened while I was away" actually relies on.
- **Engine → human, push (server-initiated MCP notifications / sampling): DRAFT.** MCP notifications are not reliably surfaced by clients today, and the "sampling" callback is a server→model mechanism, not server→human. Do not build a feature that assumes a human is proactively notified the instant an event fires until Phase 3's real-client test confirms what the attached client actually does with one.

---

## 7. Storage & timezone model — STABLE

- All instants stored in UTC.
- Every `Task` carries a `display_timezone` (IANA name, e.g. `Asia/Kolkata`), used only for rendering and for computing the local-midnight boundary that drives `NEW_DAY`.
- DST is handled by never comparing or constructing datetimes in a way that lands on an ambiguous or nonexistent local hour when it can be avoided — see `timezone_practice.py`'s `dst_status()` for the pattern (compare midnight-to-midnight rather than probing the transition hour directly). Recurring tasks whose rule collides with a DST transition (e.g., a daily 2:30am task on a spring-forward day) are DEFERRED — genuinely gnarly, low-frequency, not worth designing before Phase 5.
- Exactly one process may run the scheduler loop against a given database at a time, enforced by an exclusive lock (`BEGIN IMMEDIATE` or an OS-level file lock) acquired at startup. Every other process attached to the same database is a read-only request handler. This exists because the realistic deployment is two MCP client processes (e.g. Claude Desktop and Claude Code) pointed at the same server config — WAL mode alone permits concurrent readers but does not prevent two processes each independently ticking the scheduler and firing duplicate events.

---

## Open questions log

Carried forward explicitly rather than resolved by assumption:

1. `ask_user` transport (§5) — **still open.** No `ask_user` tool exists in `mcp_server.py` yet; the six other primitives (including the new `create_task`) were built and verified first. Needs a decision before it's implemented, not after.
2. Action schema shape (§3) — expect revision in Phase 4, once a second LLM provider's native tool-calling shape is tested against it.
3. Push notification support (§6) — **partially resolved.** Pull-via-`get_temporal_context` is now STABLE and verified for real: a scripted MCP `ClientSession` over stdio created a task, and the scheduler loop (running in the same process, per the Phase 1 architecture decision) correctly woke early and ticked it through `ACTIVE` → `WINDOW_ENDED` in real time — proving the wake-event mechanism, not just the pull-query. What's *not* yet verified: whether an actual interactive client (Claude Desktop or Claude Code, not a scripted session) does anything useful with a server-initiated push, since none was attempted. That verification needs a human at the actual app, not something a script can stand in for.
4. Recurrence × DST collision (§7) — deferred to Phase 5.

## Bug found and fixed during Phase 3's real-client test

The scheduler loop originally computed its sleep duration once per iteration (`asyncio.sleep(delay)`). A task created with a near-term boundary while the loop was already asleep toward a distant target (e.g. tonight's midnight, computed against an empty task list at startup) would not be ticked until that stale target arrived — a real violation of "event-driven, no stale waits" that only surfaced by actually running the server and creating a task against it, not from reading the code. Fixed with a `wake_event` (`asyncio.Event`) that every task-mutating tool sets, and the scheduler now `asyncio.wait_for`s on it with the computed delay as a timeout — woken early, it recomputes; timed out naturally, it ticks. Verified live: a task scheduled 3 seconds out, created while the loop was asleep toward midnight, correctly transitioned `SCHEDULED` → `ACTIVE` → `WINDOW_ENDED` within the expected few seconds.
