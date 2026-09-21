# Design decisions

The choices that shaped this project, why they were made, and what each one costs. [SPEC.md](../SPEC.md) is the contract; this is the reasoning. Every claim here is backed by a test unless it says otherwise.

## 1. The model does not own time

**Decision.** A deterministic engine derives every temporal fact (a window started, a deadline passed, a day changed). A model only proposes what to *do* about those facts, and its proposals pass through the same validator as any client.

**Why.** Handing a model the current time solves the easy part. What it cannot do is notice, unprompted, that 4:00 passed while nobody was talking to it. Deriving `now >= deadline` in a prompt is also exactly where models slip (offsets, midnight in the wrong zone, DST). A pure function cannot.

**Cost.** More machinery than "put the time in the system prompt". The State-QA benchmark exists to measure whether that machinery earns its keep; **no real model has been scored yet**, so the premise is a hypothesis with a working harness.

## 2. An append-only event log is the only source of truth

**Decision.** State is never stored; it is folded from events. Each event has two times: when it *happened* (valid time) and when the engine *noticed* (recorded time).

**Why.** It gives replay, an audit trail, and `state_as_of(t)` ("what was the plan at 4pm yesterday?") for free. Two times matter because after downtime they differ by hours: a new day starts at local midnight even if the app is reopened at 9am.

**Property test.** Asking for the state as of any moment during a simulated run reproduces exactly the snapshot taken live at that moment.

**Cost.** The log never shrinks. A bad event cannot be edited out, so bad events are *quarantined* (skipped and counted) instead of making the database unopenable.

## 3. Wake on boundaries, not on a timer

**Decision.** The scheduler computes the next moment anything can change (`next_wake_time`) and sleeps until then, bounded by a short refresh interval. The model is woken only for a window ending, a deadline breached, or a new day.

**Why / evidence.** The acceptance test and `scripts/demo.py` show a 23-hour day handled in 9 wake-ups instead of ~1,380 once-a-minute polls, with no model call to ask the time.

**Why the refresh bound exists.** Another process may add a task while this one sleeps. Waiting until midnight to notice would miss it, so sleeps are capped and any write wakes the loop.

## 4. Several processes, one database, no corruption

**Decision.** Every write takes SQLite's write lock, *then* refreshes its view from the log, *then* validates, *then* commits. One process at a time holds a lock file and acts as the scheduler; if it dies, another takes over.

**Why.** Claude Desktop and Claude Code can share a database. Validating against a stale in-memory view would let one process complete a task another had just moved. Found by a test that failed against the buggy version.

**Cost.** Writes serialise. Fine for one person's task list, wrong for a multi-tenant service.

## 5. Idempotency that survives crashes

**Decision.** Every mutating call carries an idempotency key, and *applied* keys are remembered by rebuilding them from the log, so they survive restarts and are shared across processes. The agent derives its keys from the ids of the events that triggered it, not from the model vendor's tool-call ids.

**Why.** If the process crashes after applying a decision but before recording that it did, the agent re-decides on restart, possibly differently. Keys derived from the *triggers* land on the same keys, so nothing applies twice. A test that mutates the derivation fails.

## 6. Time correctness is a set of explicit rules

- **Instants, not wall clocks.** Everything stored is a UTC instant. Local wall-clock time appears only where the meaning is genuinely local (a recurring "09:30").
- **A Python trap this project hit.** Subtracting or comparing two aware datetimes that share the *same* `ZoneInfo` object uses wall-clock arithmetic, ignoring the offset. Across a fall-back hour a real 2-hour span reads as 1 hour. The fix: a UTC clock and fixed-offset datetimes for computed occurrences. Write-up: [zoneinfo-trap.md](zoneinfo-trap.md).
- **DST rules are stated, not implied.** A skipped local time resolves forward; a repeated one takes its first occurrence; durations are absolute elapsed time. Verified against hand-derived answers for US, UK and Sydney transitions.
- **`NEW_DAY` never goes backwards.** If the timezone setting moves west or the clock steps back, nothing is announced until the local date passes the last announced one.

## 7. Untrusted input, bounded everything

Times and fields can come from a model, so they are validated at the boundary with a readable rejection, and the decision log records what was proposed and refused. Titles, arguments, listings, prompts and imported calendars are all length- or count-bounded. Model-proposed destructive actions (cancelling a task) are held for a human. Prompt injection through task titles is *mitigated* (titles are flattened, length-capped and labelled as data), not solved.

## 8. Things it does not do, on purpose

- **Blocked tasks are reported, never refused,** and overlaps likewise. People work out of order and double-book; refusing would be the engine overreaching.
- **Calendar import is pull-only and text-only.** No credentials, no network. Events it cannot represent faithfully are skipped with a reason rather than guessed.
- **Estimates are a single number** with arithmetic flags, not a prediction.

## 9. What I would change with more time

- Run the benchmark and the agent loop against real models. This is the largest unknown.
- The reference policy reschedules an unfinished window to start immediately, which burns the per-lineage move limit; a better heuristic would find the next free slot.
- Recurrence is a subset (no yearly, no "second Tuesday", no iCalendar RRULE strings).
- SQLite single-writer is right for one user; a multi-user service would need a different store and real tenancy.
- A push channel (the engine only pulls today; `temporal-engine watch --exec` is the hook).
