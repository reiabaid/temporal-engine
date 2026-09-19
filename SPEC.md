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
                                   # "carry_forward_task" | "cancel_task"
    task_id: str | None
    args: dict
    reason: str                   # LLM's stated justification, logged, not executed
    requires_confirmation: bool   # INTENT: true => hold for a human before applying.
                                   # NOT ENFORCED YET -- see §5 "Human-in-the-loop".
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

`reschedule_task` and `carry_forward_task` return both the superseded `task` and a `new_task` summary. The new id is required to act on the task again (it supersedes rather than mutates), and was originally omitted — found by reasoning about what a model calling these tools actually needs, and fixed.

### Human-in-the-loop — DECIDED: no `ask_user` tool (design decision, not yet implemented)

The v0.1 draft listed `ask_user(question)` as a primitive. **Decision: it is removed as a server-side tool.** Reasons:

- The agent already has a channel to the human: the conversation it is running in. A server tool that "asks" adds nothing the model can't do by asking in chat.
- A tool that blocks until a human answers hangs the MCP request; MCP elicitation depends on client support that can't be assumed; a "pending, please poll" tool is just a worse version of the next option.
- What the *server* legitimately owns is not the asking but the **hold**: an action a model must not take unilaterally should be recorded but not applied until a human confirms. That is `requires_confirmation` on `ActionCall`, resolved by a `confirm_action(idempotency_key)` / `reject_action(idempotency_key)` pair. It is non-blocking, works in every MCP client (pull-based, like §6's stable path), and survives restarts because pending state lives in the event log (`ACTION_PROPOSED` with `outcome: "pending_confirmation"`).

**Status: not built.** `requires_confirmation` is currently stored in the decision log but `apply_action` does not act on it — a flag that does nothing is a hazard, so this stays listed as a known gap until a real consumer needs it (Phase 4/5).

---

## 6. Event delivery — split by direction, STABLE vs DRAFT

- **Engine → itself (scheduler wake-up): STABLE.** `next_wake_time()` is a pure computation over task boundaries and the next local-midnight; the process holding the scheduler lock (see storage model, §7) sleeps exactly until that instant. No polling.
- **Engine → LLM, pull (`get_temporal_context` on the next call): STABLE.** Works regardless of what any given MCP client supports, since it's a normal request/response call. This is the mechanism "what happened while I was away" actually relies on.
- **Engine → human, push: DRAFT — and the only real-client observation so far is negative.** The server does not currently send any MCP notification, so this tested "does the session learn of a boundary event without asking," not "does the client render a notification." In Claude Code, a task was created and the session left idle across the task's entire window; nothing surfaced until `get_temporal_context` was called, at which point the state was already correct (`WINDOW_ENDED`, with `occurred_at` timestamps matching the scheduled boundaries to the second). Conclusion for design: **treat pull as the only reliable channel.** Whether MCP server-initiated notifications render in any client remains untested; do not build features that assume a human is proactively notified. The "sampling" callback is a server→model mechanism, not server→human, and doesn't change this.
- **Engine → LLM after downtime: STABLE, and verified.** If the host app (and with it the server process) is closed while boundaries pass, the next start ticks immediately and catches up every missed transition before serving requests; the next `get_temporal_context` reflects real current state. `NEW_DAY` is likewise recovered across downtime (the day tracker is seeded from the last recorded event's date; if several days elapsed, one `NEW_DAY` is emitted with previous/new dates in its payload rather than one per day). **No events are generated *while* the app is closed** — there is no background daemon in this architecture; the state is corrected on the next start, not in real time.

---

## 7. Storage & timezone model — STABLE

- All instants stored in UTC.
- Every `Task` carries a `display_timezone` (IANA name, e.g. `Asia/Kolkata`), used only for rendering and for computing the local-midnight boundary that drives `NEW_DAY`.
- DST is handled by never comparing or constructing datetimes in a way that lands on an ambiguous or nonexistent local hour when it can be avoided — see `timezone_practice.py`'s `dst_status()` for the pattern (compare midnight-to-midnight rather than probing the transition hour directly). Recurring tasks whose rule collides with a DST transition (e.g., a daily 2:30am task on a spring-forward day) are DEFERRED — genuinely gnarly, low-frequency, not worth designing before Phase 5.
- Exactly one process may run the scheduler loop against a given database at a time, enforced by an exclusive lock (`BEGIN IMMEDIATE` or an OS-level file lock) acquired at startup. Every other process attached to the same database is a read-only request handler. This exists because the realistic deployment is two MCP client processes (e.g. Claude Desktop and Claude Code) pointed at the same server config — WAL mode alone permits concurrent readers but does not prevent two processes each independently ticking the scheduler and firing duplicate events.
- **Stale-lock recovery.** The lock file records the holder's PID. A graceful client close releases the lock (verified); a force-kill leaves it behind. On acquire, a lock whose PID is no longer alive (or whose file is empty/corrupt) is reclaimed. Liveness uses `OpenProcess`/`GetExitCodeProcess` on Windows — `os.kill(pid, 0)` must not be used there, since Windows treats any signal other than CTRL_C/CTRL_BREAK as an instruction to terminate the process. Known limitation: a reused PID makes a dead holder look alive; this fails safe (no duplicate scheduler). A process that cannot acquire the lock serves tools but does not tick, and reports `is_scheduler_process: false` in `get_temporal_context` so the condition is visible rather than silent.

---

## Open questions log

Carried forward explicitly rather than resolved by assumption:

1. ~~`ask_user` transport (§5)~~ — **resolved by decision:** the tool is dropped; human-in-the-loop becomes a `requires_confirmation` hold with `confirm_action`/`reject_action`. The hold itself is **not yet built** (tracked as a known gap in §5).
2. Action schema shape (§3) — expect revision in Phase 4, once a second LLM provider's native tool-calling shape is tested against it.
3. Push notification support (§6) — **resolved for design purposes, untested in the strict sense.** Pull is the only channel treated as reliable. Verified in the real client (Claude Code, live session): task created, session idle across its whole window, nothing surfaced unprompted, and a later pull returned correct state with exact boundary timestamps. Not tested: whether any client renders a server-initiated MCP notification, because the server sends none.
4. Recurrence × DST collision (§7) — deferred to Phase 5.
5. `requires_confirmation` is recorded but unenforced (§3/§5) — deferred to Phase 4/5.
6. Real-client testing was done in Claude Code only. Claude Desktop (the separate app) was not tested; its config is a different file and its behavior on server lifecycle is unverified.

## Bugs found and fixed during Phase 3

Each was found by running the real server, not by reading the code, and each has a permanent regression test in `tests/test_mcp_integration.py` or `tests/test_lock.py`.

**1. No catch-up after downtime (the most serious).** The scheduler loop computed `next_wake_time()` and slept, never ticking on startup. Because `next_wake_time` only considers boundaries still in the future, a task whose whole window elapsed while the host app was closed had no future boundary left, so the loop slept until midnight and the task read `SCHEDULED` long after it should have read `WINDOW_ENDED`. `tick()` itself was correct (Phase 1 tested catch-up directly); the loop that calls it was not. This was initially asserted to work without having tested it through the server — a test written afterwards reproduced the failure (`SCHEDULED` where `WINDOW_ENDED` was expected). Fixed by ticking at the top of every loop pass, before computing the sleep.

**2. `NEW_DAY` lost across downtime.** `DayTracker` held its last-seen date only in memory, so after a restart its first call recorded a baseline and reported nothing. Fixed by seeding it from the most recent event's date.

**3. Stale lock demoted the server silently.** See §7, "Stale-lock recovery."

**4. `reschedule_task` / `carry_forward_task` didn't return the new task's id.** See §5.

**5. Stale sleep.** (original entry, below)

The scheduler loop originally computed its sleep duration once per iteration (`asyncio.sleep(delay)`). A task created with a near-term boundary while the loop was already asleep toward a distant target (e.g. tonight's midnight, computed against an empty task list at startup) would not be ticked until that stale target arrived — a real violation of "event-driven, no stale waits" that only surfaced by actually running the server and creating a task against it, not from reading the code. Fixed with a `wake_event` (`asyncio.Event`) that every task-mutating tool sets, and the scheduler now `asyncio.wait_for`s on it with the computed delay as a timeout — woken early, it recomputes; timed out naturally, it ticks. Verified live: a task scheduled 3 seconds out, created while the loop was asleep toward midnight, correctly transitioned `SCHEDULED` → `ACTIVE` → `WINDOW_ENDED` within the expected few seconds.
