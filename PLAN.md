# Temporal Context Protocol — Build Plan (v0.2)

Status legend: **STABLE** = build against this; **DRAFT** = expect it to change after Phase 3/4; **DEFERRED** = named but not built yet.

---

## Phase 0 — Spec (versioned from day one)

Single file, `SPEC.md`, sections individually marked stable/draft. Nothing here is "finished" on day one — it's a living contract that gets revised when real integration work (Phase 3, a second LLM provider) surfaces a wrong assumption. Expect the action schema specifically to change once a second provider is wired in.

- **STABLE:**  state machine (states, transitions, who may trigger each).
- **STABLE:** event schema — `TemporalEvent{schema_version, id, event_type, occurred_at, recorded_at, task_id, payload}`. `schema_version` from the start: an append-only log you can never edit needs a migration story before it needs anything else.
- **DRAFT:** action schema — `ActionCall{idempotency_key, action, task_id, args, reason, requires_confirmation}`. `idempotency_key` is load-bearing, not optional: if an action is applied to the log but the ack is lost before the LLM layer sees it, a retry must be a no-op, not a double-apply.
- **STABLE:** primitive surface — `get_temporal_context`, `complete_task`, `reschedule_task`, `carry_forward_task`, `cancel_task`, `ask_user`. Resist adding more until Phase 2/3 demands it.
- **STABLE:** storage timezone model (see Phase 1 — this was wrongly deferred in v0.1 of this plan and is fixed here).

---

## Phase 1 — Deterministic core (3-5 days solo w/ event-sourcing experience; budget 6-10 if this is your first event-sourced system — replay/idempotency bugs are subtle and won't show up until you specifically test for them)

**Timezone/storage decision is made here, not deferred.** All instants stored in UTC; every `Task` carries a `display_timezone` (IANA name, e.g. `Asia/Kolkata`) used only for rendering and for computing the local midnight boundary. This is foundational — DST correctness in this phase's own tests depends on the storage model already being right, so it can't be pushed to a later hardening phase. What *does* stay deferred to hardening: recurring events whose rule collides with a DST transition (e.g., a daily 2:30am task on the day clocks spring forward) — genuinely gnarly, low-frequency, fine to defer.

**The wake-up mechanism is decided here, not left implicit.** V1 architecture: the process that owns the scheduler *is* the same long-running process that will later speak MCP (Phase 4) — a single asyncio event loop running both the request handler and the `next_wake_time()`-driven sleep, with SQLite (WAL mode) as the single source of truth both read from. This avoids a separate daemon/IPC layer for v1. It means: "the server" is not stateless — it must be a persistent process, not a per-request CGI-style handler, and "survives restart" means *this process* restarts and replays the event log to reconstruct in-memory task state, not that some other component reconstructs it for the process. Documented tradeoff: this doesn't horizontally scale past one process; a future version could split scheduler-daemon from stateless API replicas over a shared DB, using the MCP *sampling* callback pattern (server pushes to client when a scheduled event fires) the way existing MCP cron servers already do — but that split is explicitly out of scope until there's a concrete multi-instance need.

Build:
- `engine.py`: `Clock` (real + simulated), `Task`, `TaskStatus`, pure transition function, `next_wake_time()` (heap over task boundaries + next local midnight per task's `display_timezone`), `tick()`.
- Storage: SQLite, append-only `events` table (source of truth) + a materialized `tasks` table rebuilt by replaying events. `events.schema_version` on every row from the first migration onward.
- Tests are the deliverable, not the demo:
  - Hand-picked cases: DST spring-forward/fall-back around a scheduled boundary, midnight crossing, overlapping tasks, `WINDOW_ENDED` vs `OVERDUE` distinction, crash-and-replay produces identical state.
  - **Property-based tests** (Hypothesis): for any generated sequence of events, replaying the log twice yields identical materialized state; `tick()` is idempotent when called twice at the same `now`; state transitions never skip an intermediate state.
  - **These same scenarios are written once here and reused verbatim as Phase 6 benchmark fixtures** — the acceptance tests and the benchmark must test the same thing, not drift into two different suites.

**Exit criterion:** correct events from a fast-forwarded simulated clock, zero polling, full replay-after-crash test passing, property tests green.

---

## Phase 2 — Wire in one real LLM reasoner (2-3 days)

- Implement `LLMProvider` interface generically even though only one provider exists yet.
- Reschedule caps, minimum-slot-duration, and monotonic-time invariants are enforced **in the state-machine validator**, not requested-of or hoped-for from the model. This is the fix for the runaway-reschedule bug from the earlier prototype.
- Because the cap lives in the validator: **the regression test for it needs no live model call.** Feed the validator a stubbed/recorded `ActionCall` sequence that would runaway if unchecked, assert it's rejected after N — this is a deterministic unit test, runs in CI, costs nothing, never flakes.
- A live-model end-to-end run of the full day-planning scenario is a **manual/smoke check**, explicitly not part of CI — non-deterministic and paid, doesn't belong in an automated suite.

**Exit criterion:** validator-level regression test passes with zero API calls; one manual smoke run against a real model confirms the wiring works end to end.

---

## Phase 3 — Expose as MCP server, one provider (3-4 days)

(Reordered ahead of multi-provider support — a working MCP server with real usage is more valuable right now than a second provider with no users yet.)

- Thin MCP adapter over the Phase 1 library — the engine must stay importable/testable standalone; MCP is a wrapper, not where logic lives.
- Test against an actual MCP client (Claude Desktop/Code), not just unit tests of the adapter — protocol-level surprises (schema validation, tool annotation requirements) show up only here.
- This is where the single-process architecture decision from Phase 1 gets validated for real: does a long-running local MCP server process match how the client actually manages server lifecycle? Find out now, before building anything on top of an assumption that turns out wrong.

**Exit criterion:** a real MCP client can create a plan, and the engine correctly fires events without the client polling.

---

## Phase 4 — Model-agnosticism, for real (1-2 days)

- Add a second foundation-model provider (whichever of Claude/GPT wasn't used in Phase 2) — same `LLMProvider` interface, zero engine changes.
- Fold local-model support (Ollama) into this phase *or* defer it to Phase 5 hardening — it only matters once someone wants to self-host, so treat it as optional here, not a hard exit criterion.
- This is expected to be where the Phase 0 action schema gets revised — a second provider's tool-calling quirks are the first real pressure test of the "stable" primitive surface.

**Exit criterion:** the Phase 1 fixture scenarios pass against both providers with only a config swap.

---

## Phase 5 — Hardening (ongoing; pick 2-3, don't chase all)

Do:
- Optimistic concurrency (`version` column) for multi-writer safety.
- Recurrence via a real RRULE library, including the DST-collision case deferred from Phase 1.
- Ollama/local-model provider, if not already done in Phase 4.

Explicitly defer until a concrete user need appears (avoid speculative complexity):
- External calendar sync (Google Calendar, etc.)
- Multi-agent conflict resolution beyond basic optimistic concurrency
- Horizontal scaling / daemon-and-API split noted in Phase 1

---

## Phase 6 — Benchmark (start in parallel with Phase 2, not after)

Reuses the Phase 1 fixture scenarios verbatim, scored on two axes:
1. Bare LLM + clock tool, asked to reason over the same scenario from scratch.
2. Engine + LLM stack.

Scoring is on *decision correctness* (did it recognize the overdue state and choose a sane action), not on whether it can do date arithmetic — that's the distinction that makes this benchmark different from TRAM/Test-of-Time-style benchmarks, which mostly test in-context date math.

---

## Phase 7 — Open source + go-to-market

- Publish repo + `SPEC.md` (now mark previously-DRAFT sections STABLE where they held up) + benchmark results together.
- Submit to the MCP Connectors Directory.
- One integration PR against an agent framework (Letta or LangGraph) rather than a cold pitch to a foundation lab.

---

## Changes from v0.1 (redline summary)

1. Timezone/storage model moved from Phase 5 into Phase 1 as a foundational decision; only genuinely rare DST-collision cases (recurrence × DST) remain deferred.
2. Added an explicit wake-up mechanism decision in Phase 1: single long-running process owns both scheduler and MCP handling for v1; daemon/API split documented as an explicit non-goal for now.
3. Split Phase 2's exit criterion into a deterministic validator-level regression test (CI, no API calls) and a manual smoke test (live model, not CI).
4. Phase 0 spec sections now individually marked stable/draft instead of treated as finished on day one.
5. Added: idempotency key on `ActionCall`, `schema_version` on events, property-based tests, and explicit reuse of Phase 1 fixtures as Phase 6 benchmark scenarios.
6. Swapped order of MCP exposure and multi-provider support; local-model (Ollama) support demoted to optional/hardening.
