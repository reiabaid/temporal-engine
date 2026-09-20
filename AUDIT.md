# Audit: the whole system against the original brief

Method: every requirement in the original 14-point brief was traced to code and to a test, or marked partial/missing. "Verified" means a test exists that fails when the behaviour is removed (mutation-checked where noted), not that the code looks right. The acceptance test [tests/test_acceptance_day_plan.py](tests/test_acceptance_day_plan.py) replays the brief's day-planning story end to end.

Status: **done** = built and tested. **partial** = built for a subset, limit stated. **missing** = not built. **doc-only** = answered in design, no code.

## Brief items

| # | Requirement | Status | Evidence / limit |
|---|---|---|---|
| 1 | Prior-art survey, skeptical, cited | doc-only | Done in the ideation phase (conversation), not a repo artifact. |
| 2 | Formalize "temporal awareness" | done | SPEC.md §1–§4: bitemporal events, state machine, derived state. |
| 3 | Architecture | done | Event-sourced SQLite log, deterministic `tick`, scheduler loop, MCP surface. |
| 4 | Deterministic vs LLM boundary | done | Engine owns time and transitions; model only proposes actions (SPEC §3, §5, §8). A model-proposed cancel is held for a human. |
| 5 | Task state machine | done | 11 states, explicit transition table, `apply_transition` the only way to change status (test_task). |
| 6 | Event-driven wake, no per-minute LLM polling | done | `next_wake_time` boundary jumps; agent runs only on 3 trigger types. Acceptance test: a full day takes ≤12 wake-ups and ≤6 model calls, none triggered by a task start. |
| 7 | API / protocol | done | MCP over stdio, 14 tools. No REST/event bus (**partial**: pull via `since_seq` cursor and `temporal-engine watch`). |
| 8 | Python + SQLite + MCP MVP, one provider | done | Plus a second provider (OpenAI) and a stub. **No real model has run** against any of it. |
| 9 | Day walkthrough (2pm DSA, 4pm window end, decision) | done | Acceptance test, first test. |
| 10 | Midnight NEW_DAY, 7 steps | partial | See below. |
| 11 | Hard problems | see table below | |
| 12 | Use as general infrastructure | doc-only | Design discussion; MCP surface is client-agnostic. |
| 13 | Research novelty | doc-only | Discussion; State-QA benchmark is the empirical hook. |
| 14 | Benchmark | partial | State-QA built (engine ground truth, independent oracle, DST cases, flawed solvers, Wilson CIs). **Not scored on a real model.** Eight categories including `historical` and `dependencies`; no future-planning category. |

### The seven midnight steps

| Step | Status | How |
|---|---|---|
| 1 Archive yesterday | done | Log is never rewritten; yesterday is reproducible via `state_as_of` / `get_state_as_of`. |
| 2 Find unfinished | done | Status derived by fold; NEW_DAY trigger is skipped if nothing is unfinished. |
| 3 Carry forward | done | `carry_forward_task` (lineage kept); the stub's NEW_DAY policy stacks unfinished work from 09:00 local. |
| 4 Load calendar | partial | `import_calendar` (pull, text-only, re-importable). No live sync. |
| 5 Evaluate deadlines | done | `TASK_DEADLINE_BREACHED` / OVERDUE, including deadline-only tasks. |
| 6 Process recurring | done | Idempotent materialisation, 7-day horizon, 24h catch-up, explicit DST rules. |
| 7 Generate updated plan | partial | The agent can now `create_task` (daily cap, timezone defaulted). Quality with a real model is unmeasured. |

## Hard problems

| Problem | Status | Evidence |
|---|---|---|
| Timezone change | done | NEW_DAY is forward-only and carries its timezone; moving west does not re-announce an earlier day (test_audit3, mutation-checked). |
| DST | done | Fixed-offset occurrences, UTC clock, absolute duration; US/UK/Sydney cases in State-QA. |
| Device offline / scheduler crash / missed events | done | Tick-first catch-up chains through every missed boundary; DayTracker seeded from the log. |
| Late completion | done | COMPLETED_LATE. |
| Overlaps | done (detect) | Reported and bounded; not auto-resolved. |
| Recurrence | partial | Subset of RRULE (see SPEC §9). |
| Deadline vs scheduled time | done | Separate fields, both drive transitions. |
| Uncertain duration | partial | `estimated_minutes` plus derived arithmetic flags (`estimate_exceeds_window`, `at_risk_of_missing_deadline`, `over_estimate`). A single estimate, not a distribution. |
| Task dependencies | done | `depends_on`; blocked / abandoned / ordering reports; lineage-aware; never refuses. |
| Manual plan changes | done | Reschedule/carry/cancel with lineage and a per-lineage move limit. |
| External events / conflicting calendars | partial | `.ics` import identified by (source, UID); overlaps between calendars are reported, no winner picked. No live sync. |
| Multiple agents / races | done | `BEGIN IMMEDIATE` transactions, single scheduler lock, real-process tests. |
| Duplicates | done | Durable idempotency keys derived from the log; agent keys derived from trigger events. |
| Long-running agents | done | Checkpoint events, bounded context, backoff. |
| "What happened while I was away" | done | `get_temporal_context(since_seq=…)`. |
| "What changed since yesterday" | done | Same cursor, plus `since_time` on the event query. |
| "How long have I been working" | done | `TASK_WORK_STARTED`, `worked_seconds`. |
| "What should I have done by now" | done | `get_state_as_of` for any instant; a future instant is a projection driven only by the clock (flagged `is_projection`). |
| Why did the agent do that? | done (new) | `ACTION_PROPOSED.caused_by` lists trigger event ids; resulting events carry `action_key`. |

## Found in this audit, and fixed

1. **NEW_DAY could go backwards.** After a timezone change (Kolkata to New York) or an NTP step-back, the local date was earlier than the last announced one and a second, earlier NEW_DAY was emitted, waking the agent for nothing. Now forward-only. Two tests fail if the guard is loosened.
2. **The agent could not build a plan.** `create_task` was not in the provider's action enum, so step 7 of the midnight routine was impossible for a model. Added, with the user's timezone filled in and a per-day creation cap counted from the log (so it survives restarts).
3. **No causal trail.** Decisions were logged but could not be tied to the event that provoked them. Added `caused_by` / `action_key`.
4. **Yesterday was not queryable.** Added `state_as_of`. Its key property, tested: replaying as of any moment during a run equals the snapshot taken live at that moment. It also shows what was *true* while the app was closed, not just what had been recorded.
5. **The reference policy ignored NEW_DAY.** It now carries unfinished work from yesterday into today.

Two findings while writing the acceptance test, both about behaviour rather than defects: the reference policy reschedules an unfinished window to start *now*, so a never-touched task burns the per-lineage move limit (3 per 24h) and then stops; and a rescheduled recurring occurrence keeps its `(recurrence_id, date)` identity, so it is never generated twice.

## Closed in the finishing pass

Each item below has tests, and each protection was mutation-checked (the protection removed, a test fails).

| Gap | What was built |
|---|---|
| Dependencies, uncertain duration | `depends_on`, `estimated_minutes`, and the derived reports (SPEC §10). |
| Calendar | `import_calendar` for `.ics` text (SPEC §10). |
| No migration path | `PRAGMA user_version` versioning, atomic migrations, refusal of newer databases, quarantine of newer events (SPEC §7a). |
| No backup or export | `backup`, `export`, `import` (round trip reproduces every event, including `seq`). |
| Creation limit only for the agent | One limit for every caller, 1000 per rolling 24 hours (SPEC §7). |
| Clock trust | Backward steps are counted and reported in `get_temporal_context` (SPEC §7a). |
| Nothing runs with the client closed | `temporal-engine run` (headless scheduler and agent), verified with a real process and the real clock. |
| No notifications | `temporal-engine watch --exec`, with the event passed by environment and never through the command line. |
| Benchmark categories | `historical` and `dependencies`, each with a flawed solver that must fail exactly that category. |

## Known gaps (not built, and why)

- **No real LLM has been run** on the benchmark, the scenarios, or the agent loop. This needs an API key and spend, and it is the largest remaining unknown: everything about *decision quality* is unmeasured.
- **Claude Desktop is untested** (Claude Code only).
- **No REST API or push event bus.** MCP plus the `since_seq` cursor and `watch` are the delivery paths.
- **Calendar import is one-way and manual.** No live sync, no title-change tracking, no attendees.
- **A duration is one number**, not a distribution, and nothing learns from history.
- **Single user, single day-boundary timezone.** Tasks carry their own display timezone; nothing is multi-tenant.
- **No future-planning benchmark category.**
- **The migration machinery has only run synthetic migrations**, because no real layout change has happened yet.
- **Prompt injection is mitigated, not solved.**
