# Temporal Engine

A deterministic, model-agnostic layer that gives an AI agent a *persistent, event-driven sense of time* — served over [MCP](https://modelcontextprotocol.io) so any MCP client can use it.

> **Status: alpha.** Python 3.11+. Developed and tested on Windows; CI runs Linux and Windows (see `.github/workflows`). Driven end to end through Claude Code. **No real LLM has been run against the agent loop or the benchmark yet**, and it is untested in Claude Desktop. Not on PyPI. Read [Known limitations](#known-limitations) before relying on it.

## The problem

Give an LLM the current time and you have solved the easy 1%. What it still cannot do is *remember* that a task's window ended at 4:00 and it is now 4:12, notice that midnight passed while nobody was talking to it, or know that a deadline was missed while the app was closed. Every agent framework re-derives this per app, per model, inside the context window.

The idea here: **the LLM should not track time. A deterministic engine tracks time and state transitions; the LLM only decides what those facts mean.**

```
   clock ──► deterministic engine ──► append-only event log ──► MCP tools
             (state machine, scheduler,   (the single source        (any client)
              recurrence, validation)      of truth)                    │
                        │                                                ▼
                        └── events ──► [optional] agent loop ──► any LLM provider
                                                                  (decides; cannot
                                                                   bypass validation)
```

`current_time > deadline` is never a model's job. *"What should we do because this is now overdue?"* is.

## See it work (no API key)

```bash
pip install -e ".[dev]"
python scripts/demo.py
```

One simulated day, real engine. A scripted policy stands in for the model, so the demo shows what the *engine* does: it notices each boundary itself, and the decision-maker is woken only when a decision is needed.

```text
Plan for Sat 12 Sep (Asia/Kolkata):
  DSA practice  14:00-16:00   (the person never marks it done)
  Send report   deadline 23:00
  Standup       daily 09:30

Sat 12 Sep 09:30
   engine  started        Standup

Sat 12 Sep 09:45
   engine  WINDOW ENDED   Standup   (nobody marked it done)

Sat 12 Sep 14:00
   engine  started        DSA practice

Sat 12 Sep 16:00
   engine  WINDOW ENDED   DSA practice   (nobody marked it done)
   agent   reschedule_task    DSA practice: try again this evening  [applied]

Sat 12 Sep 19:00
   engine  started        DSA practice

Sat 12 Sep 21:00
   engine  WINDOW ENDED   DSA practice   (nobody marked it done)
   agent   carry_forward_task DSA practice: no time left today; carry to tomorrow morning  [applied]

Sat 12 Sep 23:00
   engine  DEADLINE MISSED Send report

Sun 13 Sep 00:00
   engine  NEW DAY        2026-09-13 (Asia/Kolkata)

Sun 13 Sep 08:00

What did the plan look like at 5pm on the 12th? (asked the next morning, answered from the log)
   DSA practice  RESCHEDULED Sat 12 Sep 14:00 -> Sat 12 Sep 16:00
   DSA practice  SCHEDULED   Sat 12 Sep 19:00 -> Sat 12 Sep 21:00
   Send report   CREATED     no window

Open now:
   Send report   OVERDUE     no window
   DSA practice  SCHEDULED   Sun 13 Sep 10:00 -> Sun 13 Sep 12:00

The engine woke 9 times in 23 hours (a once-a-minute poll: 1380).
The decision-maker was woken 5 times, each for a window ending, a missed deadline or a new day, never to ask the time.
```

Note what is *not* happening: no polling, no model call to ask the time, no lost event. The 5pm reconstruction is answered from the log, not from memory.

## Quick start

From a checkout (not published to PyPI):

```bash
pip install -e ".[dev]"
pytest                      # ~1 minute: integration tests spawn real server processes
```

Register the server with an MCP client (a portable template is in `.mcp.example.json`; copy it to `.mcp.json`). For **Claude Code**, add a project-level `.mcp.json`:

```json
{
  "mcpServers": {
    "temporal-engine": {
      "command": "temporal-engine-mcp",
      "env": {
        "TEMPORAL_ENGINE_TZ": "Asia/Kolkata"
      }
    }
  }
}
```

Use your own IANA timezone. If the command isn't on your `PATH`, give the full path to the script in your environment's `Scripts`/`bin` folder. For **Claude Desktop** the entry has the same shape under `mcpServers` in its config file — untested here, and **merge into the existing file rather than replacing it**.

Restart the client, then try: *"Create a task 'DSA' from 2pm to 4pm today, then check the temporal context."* Later, ask *"what happened while I was away?"*

## Tools

| Tool | What it does |
|---|---|
| `get_temporal_context` | Now (local), unfinished tasks, overlaps, held actions, scheduler health, recurrences, and events. Pass the returned `cursor` as `since_seq` next time to get only what's new. |
| `create_task` | A task with an optional window, deadline, `estimated_minutes` and `depends_on` (ids of existing tasks). |
| `start_task` | Record that work actually began (gives `worked_seconds`). |
| `complete_task` | Done; recorded as late if the window or deadline had passed. |
| `reschedule_task`, `carry_forward_task` | Supersede a task with a new one at a new time. |
| `cancel_task` | `drop` or `cancel`. |
| `create_recurring_task`, `end_recurring_task` | Daily / weekly / monthly series (see below). |
| `list_pending_actions`, `confirm_action`, `reject_action` | The human-confirmation hold. |
| `import_calendar` | Import the text of an `.ics` export as tasks; re-import is safe and follows changes and cancellations (see below). |
| `get_state_as_of` | What every task's status was (or, for a future time, will be if only the clock acts) at a given instant. Answers "what was I doing at 3pm yesterday?". |

Every mutating tool accepts an optional `idempotency_key`: retrying with the same key never does the thing twice, even across restarts.

## Configuration

Environment variables, set in the client's server entry:

| Variable | Default | Meaning |
|---|---|---|
| `TEMPORAL_ENGINE_TZ` | `UTC` (with a warning) | Your IANA timezone. Decides when "a new day" starts. **Set it.** |
| `TEMPORAL_ENGINE_DB` | `~/.temporal-engine/temporal_engine.db` | The database. Every client that should share state must point at the same file. |
| `TEMPORAL_ENGINE_REFRESH_SECONDS` | `5` | How often a process checks for changes made by another. |
| `TEMPORAL_ENGINE_PROVIDER` | *(off)* | Enables the [agent loop](#the-optional-agent-loop): `stub`, `anthropic`, or `openai`. |
| `TEMPORAL_ENGINE_MODEL` | | Model id for the provider (required for `openai`; nothing is guessed). |
| `TEMPORAL_ENGINE_BASE_URL` | | For `openai`-compatible servers such as a local Ollama. |
| `TEMPORAL_ENGINE_AGENT_MIN_INTERVAL` | `30` | Minimum seconds between agent batches. |

## How it stays correct

- **One source of truth.** State is an append-only event log; everything else is derived by replaying it. Every write is validated against the log's current state inside a write transaction, so two clients (say Claude Desktop and Claude Code) sharing one database cannot corrupt it. One process at a time is the scheduler; if it exits, another takes over.
- **Catch-up.** Close the app while a task's window elapses and the next start ticks through every missed boundary. Nothing is generated *while* the app is closed unless you run `temporal-engine run` (below), but the state is right when you return.
- **Untrusted input.** Times and fields may come from an LLM, so they are validated at the boundary (timezone-aware, well-ordered, real IANA zones). A bad value is a logged rejection with a reason, never a crash.
- **Bounded actions.** A task can be moved at most 3 times per 24 hours; a destructive action proposed by a model is held for human confirmation.
- **DST done deliberately.** Times are unambiguous instants; recurrence follows local wall-clock time, with an explicit rule for skipped and repeated hours.

Rationale, edge cases and the bugs found along the way are in [SPEC.md](SPEC.md).

## Planning facts: estimates and dependencies

`estimated_minutes` and `depends_on` are facts you supply; what follows from them is arithmetic the server does for you: `estimate_exceeds_window`, `at_risk_of_missing_deadline`, `remaining_estimate_minutes`, `is_blocked` / `blocked_by`, `scheduled_before_dependency`. A blocked task is *reported*, never refused (people work out of order). A dependency that was rescheduled is followed to its replacement; one that was cancelled is listed as `abandoned` instead of blocking forever.

## Calendar import

`import_calendar` takes the *text* of an iCalendar export (nothing is fetched, so no credentials). An event is identified by its source name and UID: re-importing does nothing, a changed time reschedules, `STATUS:CANCELLED` cancels, and a task you already finished here is left alone. All-day, no-end, recurring (RRULE) and unknown-time-zone events are skipped **with a reason** rather than guessed. Two calendars that overlap appear as ordinary conflicts.

## Running without a client: `temporal-engine`

```bash
temporal-engine run                      # scheduler (and agent, if configured) with no AI client open
temporal-engine watch --exec "notify.cmd"  # prints windows ending / deadlines / new days / held actions; runs your command per event
temporal-engine status
temporal-engine backup backup.db         # consistent copy; never overwrites
temporal-engine export log.jsonl         # the log as JSON lines
temporal-engine import log.jsonl --db new.db   # rebuild an EMPTY database
```

`watch` is the notification path: the engine does not push, but anything that can run a command can turn it into a toast or a message. The event reaches your command through environment variables (`TE_EVENT`, `TE_TEXT`, `TE_TASK_TITLE`), never spliced into the command line, because titles are untrusted.

## Recurring tasks

`create_recurring_task` makes a *template* that generates ordinary tasks about a week ahead. Skip one occurrence by cancelling it, change one by rescheduling it; none affects the others. After a long absence only the last 24 hours of missed occurrences are created, and the omission is recorded rather than silent. Rules: daily, weekly (chosen weekdays), monthly (chosen days), each with an interval, bounded by `until` or `count`.

## The optional agent loop

With `TEMPORAL_ENGINE_PROVIDER` set, the server reacts to *window ended*, *deadline breached* and *new day* events by asking a model what to do, with no one in the chat. **Off by default because it makes paid API calls unattended.** It skips events that need no decision, resumes where it left off after a restart, never applies a decision twice, is rate limited, and backs off on errors. `stub` runs a deterministic heuristic (no API, no cost) so you can watch it work. Details in [SPEC.md](SPEC.md) §8.

## Benchmark

[BENCHMARK.md](BENCHMARK.md) describes **State-QA**: given a plain-text history and the current time, can a model derive each task's status? Ground truth comes from running the real engine; scoring is exact match with confidence intervals; deliberately flawed reference solvers prove the benchmark tells error classes apart. **No real model has been scored yet**, and the document is explicit about what a score would and would not show.

## Results so far

**Reference solvers only, not models.** State-QA scores programs with one deliberate flaw each, to show the benchmark tells error classes apart (full table in [BENCHMARK.md](BENCHMARK.md)); for example, a solver that ignores dependencies scores 30/60 on `dependencies` and 100% everywhere else, and one that reads clock digits instead of instants fails only the daylight-saving cases.

**Real-model scores: none yet.** That is the open question this project exists to answer (does a model mis-derive temporal state from a history?), and it needs a run with an API key: `python scripts/live_state_qa.py --solver anthropic --model <id> ...`. Until then, treat the thesis as a hypothesis with a working test harness, not a finding.

## Known limitations

- **No real model has been scored on the benchmark**, and none has driven the agent loop. Providers are tested against fakes; decision quality is unmeasured. `scripts/live_scenarios.py` scores a real model on four fixtures once you supply a key.
- **Claude Desktop is untested.** Only Claude Code has been driven for real.
- **No push from the engine itself.** `temporal-engine watch --exec` is the hook; whether a given AI client would render a server push is unknown.
- **Nothing runs while the client is closed** unless you start `temporal-engine run`; without it, state is corrected on next start.
- **One user, one timezone** for the day boundary.
- **Recurrence is a subset** — no yearly rules, no "second Tuesday", no iCalendar strings, templates aren't editable (end and recreate).
- **Prompt injection is mitigated, not solved.**
- Durations are a single estimate, not a distribution; calendar import is one-way and manual (no live sync); creation is rate limited to 1000 tasks per 24 hours for everyone.
- Full list: [SPEC.md](SPEC.md), "Not done"; requirement-by-requirement status: [AUDIT.md](AUDIT.md).

## Project layout

```
temporal_engine/    the library and the MCP server (mcp_server.py)
tests/              unit, deterministic multi-process, and real-server integration tests
scripts/            demo.py (keyless day replay) and manual tools that need a real API key
docs/               design-decisions.md (the reasoning), zoneinfo-trap.md (a Python pitfall), demo-output.txt
AUDIT.md            requirement-by-requirement status against the original brief
exercises/          early learning exercises, kept for history
SPEC.md             the contract, design decisions, and change log
PLAN.md             the original build plan (see the status note at its top)
```

## License

MIT. See [LICENSE](LICENSE).
