"""
State-QA: does a model that must DERIVE temporal state from a plain history
get it right? The premise of this project is that it does not, reliably --
so this measures it.

A case is a scripted history (tasks created, completed, moved, recurring...)
plus a query instant. The ground truth is not typed in: the script is run
through the real engine (SimulatedClock, real Runtime) and the resulting
statuses are read back. Scoring is exact match, per task -- no judge model.

What this does and does NOT measure, stated up front:

* It measures whether a solver can compute task statuses from a history,
  including the cases where naive reasoning fails (deadline vs window, local
  midnight vs UTC date, and daylight saving time). The "engine arm" is the
  reference (100% by construction), so the interesting number is the
  *baseline model's* score.
* It does not measure decision quality (see scenarios.py) or whether a model
  is good at planning.
* The status definitions are given to the model in the prompt, so it is not
  being asked to guess our conventions -- only to apply them.

Integrity checks (see tests/test_state_qa.py): an independent oracle
(`derive_status`, the rules as a pure function) must agree with the engine on
every generated case; and deliberately flawed solvers must fail exactly the
categories their flaw predicts.
"""
from __future__ import annotations

import json
import math
import random
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional, Protocol
from zoneinfo import ZoneInfo

from temporal_engine.actions import ActionCall
from temporal_engine.events import EventType
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import SimulatedClock
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc

CATEGORIES = ["window_states", "deadlines", "completions", "moves", "local_midnight", "dst_recurrence"]

DEFINITIONS = """\
Status definitions. Apply the first rule that fits:
- CANCELLED: the user cancelled it.
- COMPLETED_LATE: the user marked it done at or after the end of its window, or at or after its deadline.
- COMPLETED: the user marked it done, and neither of the above applies.
- OVERDUE: not done, and the current time is at or after its deadline. This takes precedence over WINDOW_ENDED and ACTIVE.
- WINDOW_ENDED: not done, deadline not missed, and the current time is at or after the end of its window.
- ACTIVE: not done, and the current time is at or after the start of its window and before its end (or it has no end).
- SCHEDULED: not done, and its window has not started yet.
- CREATED: it has no window (at most a deadline) and that deadline has not passed.
If a task was rescheduled or carried forward, its status is that of its newest version (the latest window given); older versions are not asked about.
All times are instants. Compare them as instants, converting UTC offsets, never as clock digits."""

RECURRENCE_NOTE = """\
Recurring tasks: every date the rule produces is its own task, named "Title@YYYY-MM-DD" (the local date). Its window starts at the rule's start time on that date in the rule's timezone and lasts the stated duration in real elapsed time. Start times are local wall-clock times in that timezone, so the UTC instant depends on daylight saving time on that date. A local time that does not exist that day (skipped by a clock change) means the same instant read with the UTC offset in force just before the change; a local time that occurs twice means its first occurrence."""


# ------------------------------------------------------------------- model

@dataclass
class Step:
    at: datetime
    kind: str          # create | complete | cancel | reschedule | carry | recur
    key: str
    args: dict = field(default_factory=dict)


@dataclass
class Fact:
    """What the engine ended up holding for one asked-about task."""
    start: Optional[datetime]
    end: Optional[datetime]
    deadline: Optional[datetime]
    done_at: Optional[datetime]
    cancelled: bool


@dataclass
class QACase:
    id: str
    category: str
    timezone: str
    steps: list[Step]
    now: datetime
    now_in_utc_only: bool
    asked: list[str]
    expected: dict[str, str] = field(default_factory=dict)
    facts: dict[str, Fact] = field(default_factory=dict)


def _local(dt: datetime, tz: str) -> str:
    return dt.astimezone(ZoneInfo(tz)).isoformat(timespec="minutes")


# ------------------------------------------------------------ the rules, twice

def derive_status(fact: Fact, now: datetime) -> str:
    """The status rules as a pure function -- an independent statement of the
    semantics, used as an oracle against the engine. Works on any consistent
    kind of datetime (all aware, or all naive), which is what the deliberately
    flawed solvers below rely on."""
    if fact.cancelled:
        return "CANCELLED"
    if fact.done_at is not None:
        late = (fact.end is not None and fact.done_at >= fact.end) or \
               (fact.deadline is not None and fact.done_at >= fact.deadline)
        return "COMPLETED_LATE" if late else "COMPLETED"
    if fact.deadline is not None and now >= fact.deadline:
        return "OVERDUE"
    if fact.start is None:
        return "CREATED"
    if fact.end is not None and now >= fact.end:
        return "WINDOW_ENDED"
    return "ACTIVE" if now >= fact.start else "SCHEDULED"


# ----------------------------------------------------- running a case for real

def execute_case(case: QACase) -> tuple[dict[str, str], dict[str, Fact]]:
    """Run the script through the real engine; return statuses and facts for
    the asked-about tasks."""
    steps = sorted(case.steps, key=lambda s: s.at)
    tz = case.timezone
    with tempfile.TemporaryDirectory() as folder:
        clock = SimulatedClock(steps[0].at)
        runtime = Runtime(Path(folder) / "qa.db", day_boundary_tz=tz, clock=clock)
        try:
            runtime.run_pass()
            current: dict[str, str] = {}

            def act(action: str, task_id, args: dict, key: str):
                events = runtime.execute(ActionCall(f"qa-{next(_counter)}", action, task_id, args))
                assert events[0].payload["outcome"] == "applied", (case.id, key, events[0].payload)
                return events

            for step in steps:
                clock.set(step.at)
                runtime.run_pass()
                a = step.args
                if step.kind == "create":
                    events = act("create_task", None, {
                        "title": step.key, "display_timezone": tz, "scheduled_start": a.get("start"),
                        "scheduled_end": a.get("end"), "deadline": a.get("deadline"),
                    }, step.key)
                    current[step.key] = next(e.task_id for e in events if e.event_type == EventType.TASK_CREATED)
                elif step.kind in ("reschedule", "carry"):
                    events = act("reschedule_task" if step.kind == "reschedule" else "carry_forward_task",
                                 current[step.key], {"new_start": a["start"], "new_end": a["end"]}, step.key)
                    current[step.key] = next(e.task_id for e in events if e.event_type == EventType.TASK_CREATED)
                elif step.kind == "complete":
                    act("complete_task", current[step.key], {}, step.key)
                elif step.kind == "cancel":
                    act("cancel_task", current[step.key], {"mode": "cancel"}, step.key)
                elif step.kind == "recur":
                    act("create_recurrence", None, {"title": step.key, "display_timezone": tz, **a}, step.key)
                else:
                    raise ValueError(step.kind)

            clock.set(case.now)
            runtime.run_pass()
            tasks = runtime.store.refresh().tasks

            def latest(matching) -> Optional[Task]:
                found = [t for t in tasks.values() if matching(t)]
                return max(found, key=lambda t: (t.scheduled_start or datetime.min.replace(tzinfo=UTC))) if found else None

            statuses, facts = {}, {}
            for key in case.asked:
                if "@" in key:
                    title, occurrence = key.split("@", 1)
                    task = latest(lambda t: t.title == title and t.occurrence == occurrence)
                else:
                    task = tasks[current[key]]
                assert task is not None, (case.id, key)
                statuses[key] = task.status.value
                completed = task.status in (TaskStatus.COMPLETED, TaskStatus.COMPLETED_LATE)
                facts[key] = Fact(
                    start=task.scheduled_start, end=task.scheduled_end, deadline=task.deadline,
                    done_at=task.last_transition_at if completed else None,
                    cancelled=task.status == TaskStatus.CANCELLED,
                )
            return statuses, facts
        finally:
            runtime.close()


_counter = iter(range(1_000_000))


def _finish(case: QACase) -> QACase:
    case.expected, case.facts = execute_case(case)
    return case


# ------------------------------------------------------------------ generators

NAMES = ["Write report", "Gym", "Call mom", "Review PR", "Groceries", "Read paper", "Budget", "Laundry",
         "Dentist", "Tax forms", "Practice piano", "Water plants", "Email Sam", "Plan trip"]
ZONES = ["UTC", "Asia/Kolkata", "America/New_York", "Asia/Tokyo"]   # mid-year dates: no transition in play


def _m(minutes: float) -> timedelta:
    return timedelta(minutes=minutes)


def _base(rng: random.Random) -> datetime:
    return datetime(2026, 9, rng.randint(2, 25), rng.randint(5, 20), rng.choice([0, 10, 20, 35, 50]), tzinfo=UTC)


def gen_window_states(seed: int, n: int) -> list[QACase]:
    cases = []
    for i in range(n):
        rng = random.Random(f"ws-{seed}-{i}")
        tz, now = rng.choice(ZONES), _base(rng)
        names, made = rng.sample(NAMES, 4), now - _m(rng.randint(600, 900))
        plan = [
            (names[0], now + _m(rng.randint(30, 300)), rng.randint(30, 90)),      # future
            (names[1], now - _m(rng.randint(10, 50)), None),                       # active
            (names[2], now - _m(rng.randint(200, 400)), rng.randint(30, 90)),      # ended long ago
            (names[3], now - _m(rng.randint(70, 110)), rng.randint(30, 50)),       # ended recently
        ]
        steps = []
        for name, start, length in plan:
            if length is None:                                  # active: ends after now
                length = int((now - start).total_seconds() // 60) + rng.randint(10, 60)
            steps.append(Step(made, "create", name, {"start": start, "end": start + _m(length)}))
        cases.append(_finish(QACase(f"window_states-{seed}-{i}", "window_states", tz, steps, now,
                                    rng.random() < 0.5, names)))
    return cases


def gen_deadlines(seed: int, n: int) -> list[QACase]:
    cases = []
    for i in range(n):
        rng = random.Random(f"dl-{seed}-{i}")
        tz, now = rng.choice(ZONES), _base(rng)
        a, b, c, d, e, f = rng.sample(NAMES, 6)
        made = now - _m(rng.randint(700, 1000))
        steps = [
            # window ended, deadline still ahead -> WINDOW_ENDED
            Step(made, "create", a, {"start": now - _m(200), "end": now - _m(120), "deadline": now + _m(rng.randint(60, 600))}),
            # window ended AND deadline passed -> OVERDUE
            Step(made, "create", b, {"start": now - _m(300), "end": now - _m(200), "deadline": now - _m(rng.randint(20, 150))}),
            # still in its window but the deadline is already gone -> OVERDUE
            Step(made, "create", c, {"start": now - _m(60), "end": now + _m(60), "deadline": now - _m(rng.randint(10, 50))}),
            # window not started, deadline later -> SCHEDULED
            Step(made, "create", d, {"start": now + _m(60), "end": now + _m(120), "deadline": now + _m(400)}),
            # only a deadline, and it passed -> OVERDUE
            Step(made, "create", e, {"deadline": now - _m(rng.randint(20, 300))}),
            # only a deadline, still ahead -> CREATED
            Step(made, "create", f, {"deadline": now + _m(rng.randint(60, 900))}),
        ]
        cases.append(_finish(QACase(f"deadlines-{seed}-{i}", "deadlines", tz, steps, now,
                                    rng.random() < 0.5, [a, b, c, d, e, f])))
    return cases


def gen_completions(seed: int, n: int) -> list[QACase]:
    cases = []
    for i in range(n):
        rng = random.Random(f"cp-{seed}-{i}")
        tz, now = rng.choice(ZONES), _base(rng)
        a, b, c, d, e, f = rng.sample(NAMES, 6)
        made = now - _m(900)
        steps = [
            Step(made, "create", a, {"start": now + _m(120), "end": now + _m(180)}),
            Step(now - _m(rng.randint(400, 800)), "complete", a),                              # done before it started -> COMPLETED
            Step(made, "create", b, {"start": now - _m(200), "end": now - _m(100)}),
            Step(now - _m(150), "complete", b),                                                 # done inside its window -> COMPLETED
            Step(made, "create", c, {"start": now - _m(300), "end": now - _m(200)}),
            Step(now - _m(rng.randint(20, 150)), "complete", c),                                # done after it ended -> COMPLETED_LATE
            Step(made, "create", d, {"start": now - _m(300), "end": now - _m(100), "deadline": now - _m(220)}),
            Step(now - _m(160), "complete", d),                                                 # inside window but past deadline -> COMPLETED_LATE
            Step(made, "create", e, {"start": now - _m(90), "end": now + _m(90)}),
            Step(now - _m(30), "cancel", e),                                                    # -> CANCELLED
            Step(made, "create", f, {"start": now - _m(400), "end": now - _m(330)}),            # ignored -> WINDOW_ENDED
        ]
        steps.sort(key=lambda s: (s.at, s.kind != "create"))
        cases.append(_finish(QACase(f"completions-{seed}-{i}", "completions", tz, steps, now,
                                    rng.random() < 0.5, [a, b, c, d, e, f])))
    return cases


def gen_moves(seed: int, n: int) -> list[QACase]:
    cases = []
    for i in range(n):
        rng = random.Random(f"mv-{seed}-{i}")
        tz, now = rng.choice(ZONES), _base(rng)
        a, b, c, d = rng.sample(NAMES, 4)
        made = now - _m(1000)
        steps = [
            Step(made, "create", a, {"start": now - _m(300), "end": now - _m(240)}),
            Step(now - _m(200), "reschedule", a, {"start": now + _m(rng.randint(60, 300)), "end": now + _m(400)}),  # -> SCHEDULED
            Step(made, "create", b, {"start": now - _m(300), "end": now - _m(240)}),
            Step(now - _m(100), "reschedule", b, {"start": now - _m(rng.randint(20, 60)), "end": now + _m(rng.randint(20, 90))}),  # -> ACTIVE
            Step(made, "create", c, {"start": now - _m(500), "end": now - _m(440)}),
            Step(now - _m(400), "reschedule", c, {"start": now - _m(350), "end": now - _m(300)}),
            Step(now - _m(250), "reschedule", c, {"start": now - _m(200), "end": now - _m(150)}),                   # -> WINDOW_ENDED
            Step(made, "create", d, {"start": now - _m(200), "end": now - _m(140)}),
            Step(now - _m(120), "carry", d, {"start": now + _m(1300), "end": now + _m(1360)}),                     # tomorrow -> SCHEDULED
        ]
        steps.sort(key=lambda s: (s.at, s.kind != "create"))
        cases.append(_finish(QACase(f"moves-{seed}-{i}", "moves", tz, steps, now,
                                    rng.random() < 0.5, [a, b, c, d])))
    return cases


def gen_local_midnight(seed: int, n: int) -> list[QACase]:
    """The query time is given in UTC only, a few minutes after LOCAL midnight,
    where the UTC calendar date and the local one disagree."""
    cases = []
    for i in range(n):
        rng = random.Random(f"lm-{seed}-{i}")
        tz = rng.choice(["Asia/Kolkata", "Asia/Tokyo", "America/New_York"])
        zone = ZoneInfo(tz)
        day = datetime(2026, 9, rng.randint(3, 24), tzinfo=zone)
        midnight = (day + timedelta(days=1)).replace(hour=0, minute=0)          # local midnight after `day`
        now = (midnight + _m(rng.randint(5, 40))).astimezone(UTC)
        a, b, c = rng.sample(NAMES, 3)
        made = (day.replace(hour=8)).astimezone(UTC)
        steps = [
            Step(made, "create", a, {"start": midnight.astimezone(UTC) - _m(120), "end": midnight.astimezone(UTC) - _m(rng.randint(15, 60))}),   # -> WINDOW_ENDED
            Step(made, "create", b, {"start": midnight.astimezone(UTC) - _m(30), "end": midnight.astimezone(UTC) + _m(rng.randint(60, 120))}),    # spans midnight -> ACTIVE
            Step(made, "create", c, {"start": midnight.astimezone(UTC) + _m(rng.randint(60, 200)), "end": midnight.astimezone(UTC) + _m(300)}),   # -> SCHEDULED
        ]
        cases.append(_finish(QACase(f"local_midnight-{seed}-{i}", "local_midnight", tz, steps, now, True, [a, b, c])))
    return cases


def gen_dst_recurrence(seed: int, n: int) -> list[QACase]:
    """Fixed, hand-built cases on real 2026 transition dates in three regions.
    They do not vary with `seed`; `n` only switches the category on or off.
    Each is chosen so that assuming a fixed UTC offset, or reading clock
    digits, gives a visibly wrong answer for at least one task."""
    if n <= 0:
        return []

    def recurring(cid, tz, first_date, start_time, minutes, created_local, now_utc, asked):
        created = datetime.fromisoformat(created_local).replace(tzinfo=ZoneInfo(tz)).astimezone(UTC)
        steps = [Step(created, "recur", "Standup", {
            "frequency": "daily", "start_time": start_time, "duration_minutes": minutes, "first_date": first_date})]
        return _finish(QACase(cid, "dst_recurrence", tz, steps, datetime.fromisoformat(now_utc), True, asked))

    ny = "America/New_York"
    return [
        # US spring forward: 09:00 is EST (14:00Z) on the 7th, EDT (13:00Z) on the 8th
        recurring("dst_recurrence-spring", ny, "2026-03-05", "09:00", 30, "2026-03-05T08:00", "2026-03-08T13:15:00+00:00",
                  ["Standup@2026-03-07", "Standup@2026-03-08", "Standup@2026-03-09"]),
        # US fall back: 01:30 happens twice on 2026-11-01; the first (EDT, 05:30Z) is meant
        recurring("dst_recurrence-fall", ny, "2026-10-30", "01:30", 60, "2026-10-30T00:30", "2026-11-01T06:15:00+00:00",
                  ["Standup@2026-10-31", "Standup@2026-11-01", "Standup@2026-11-02"]),
        # a skipped local time: 02:30 does not exist on 2026-03-08, so it resolves forward to 03:30 EDT (07:30Z)
        recurring("dst_recurrence-gap", ny, "2026-03-06", "02:30", 60, "2026-03-06T01:00", "2026-03-08T07:45:00+00:00",
                  ["Standup@2026-03-07", "Standup@2026-03-08", "Standup@2026-03-09"]),
        # the morning AFTER the US fall-back: 09:00 is EDT (13:00Z) before it, EST (14:00Z) from Nov 1
        recurring("dst_recurrence-us-morning-after", ny, "2026-10-30", "09:00", 30, "2026-10-30T08:00", "2026-11-01T14:10:00+00:00",
                  ["Standup@2026-10-31", "Standup@2026-11-01", "Standup@2026-11-02"]),
        # UK spring forward (2026-03-29): 07:00 is GMT (07:00Z) on the 28th, BST (06:00Z) on the 29th
        recurring("dst_recurrence-uk-spring", "Europe/London", "2026-03-26", "07:00", 30, "2026-03-26T06:00", "2026-03-29T06:10:00+00:00",
                  ["Standup@2026-03-28", "Standup@2026-03-29", "Standup@2026-03-30"]),
        # Southern hemisphere: Sydney's DST ENDS on 2026-04-05, so its offset drops from +11 to +10
        recurring("dst_recurrence-sydney-autumn", "Australia/Sydney", "2026-04-02", "09:00", 30, "2026-04-02T08:00", "2026-04-04T23:10:00+00:00",
                  ["Standup@2026-04-04", "Standup@2026-04-05", "Standup@2026-04-06"]),
    ]


GENERATORS: dict[str, Callable[[int, int], list[QACase]]] = {
    "window_states": gen_window_states, "deadlines": gen_deadlines, "completions": gen_completions,
    "moves": gen_moves, "local_midnight": gen_local_midnight, "dst_recurrence": gen_dst_recurrence,
}


def generate(seed: int = 0, per_category: int = 5) -> list[QACase]:
    cases = []
    for category in CATEGORIES:
        cases.extend(GENERATORS[category](seed, per_category))
    return cases


# ---------------------------------------------------------------------- prompt

def render_history(case: QACase) -> list[str]:
    tz, lines = case.timezone, []
    for step in sorted(case.steps, key=lambda s: s.at):
        when, a = _local(step.at, tz), step.args
        if step.kind == "create":
            parts = []
            if a.get("start"):
                parts.append(f"window {_local(a['start'], tz)} to {_local(a['end'], tz)}" if a.get("end")
                             else f"starting {_local(a['start'], tz)} (no end)")
            if a.get("deadline"):
                parts.append(f"deadline {_local(a['deadline'], tz)}")
            lines.append(f'{when}  created task "{step.key}": {", ".join(parts) or "no window"}')
        elif step.kind == "reschedule":
            lines.append(f'{when}  rescheduled "{step.key}" to window {_local(a["start"], tz)} to {_local(a["end"], tz)}')
        elif step.kind == "carry":
            lines.append(f'{when}  carried "{step.key}" forward to window {_local(a["start"], tz)} to {_local(a["end"], tz)}')
        elif step.kind == "complete":
            lines.append(f'{when}  marked "{step.key}" done')
        elif step.kind == "cancel":
            lines.append(f'{when}  cancelled "{step.key}"')
        elif step.kind == "recur":
            lines.append(
                f'{when}  created recurring task "{step.key}": every day at {a["start_time"]} {tz} for '
                f'{a["duration_minutes"]} minutes, starting {a["first_date"]}'
            )
    return lines


def render_prompt(case: QACase) -> str:
    now_text = case.now.astimezone(UTC).strftime("%Y-%m-%dT%H:%MZ") if case.now_in_utc_only else _local(case.now, case.timezone)
    parts = [
        "You keep no state between conversations. Below is the complete history of a user's task list, oldest first, "
        "followed by the current time. Work out the CURRENT status of each task listed at the end.",
        f"The user's timezone is {case.timezone}. History timestamps are in that timezone and carry their UTC offset.",
        DEFINITIONS,
    ]
    if any(s.kind == "recur" for s in case.steps):
        parts.append(RECURRENCE_NOTE)
    parts += [
        "History:\n" + "\n".join(render_history(case)),
        f"Current time: {now_text}",
        "Answer with a single JSON object mapping each task name to its status, and nothing else. Tasks: "
        + ", ".join(f'"{k}"' for k in case.asked),
    ]
    return "\n\n".join(parts)


def parse_answer(text: str, asked: list[str]) -> dict[str, str]:
    """Pull the JSON object out of a reply. Anything unusable scores as wrong."""
    decoder = json.JSONDecoder()
    for start, char in enumerate(text):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[start:])
        except ValueError:
            continue
        if isinstance(obj, dict):
            return {str(k): str(v).strip().upper() for k, v in obj.items() if str(k) in asked}
    return {}


# ---------------------------------------------------------------------- solvers

class Solver(Protocol):
    name: str
    def solve(self, case: QACase) -> dict[str, str]: ...


class EngineSolver:
    """The reference arm: re-runs the history through the real engine."""
    name = "engine"

    def solve(self, case: QACase) -> dict[str, str]:
        return execute_case(case)[0]


class LLMSolver:
    """The baseline arm: a model that must derive the answer from the text.
    `complete` maps a prompt to the model's reply text."""

    def __init__(self, complete: Callable[[str], str], name: str = "llm"):
        self.complete, self.name = complete, name

    def solve(self, case: QACase) -> dict[str, str]:
        return parse_answer(self.complete(render_prompt(case)), case.asked)


class _FlawedSolver:
    """Base for solvers with ONE deliberate flaw, used to prove the benchmark
    can tell error classes apart. They are not models and their scores are not
    results about any model."""
    name = "flawed"

    def _wall(self, case: QACase, dt: Optional[datetime]) -> Optional[datetime]:
        return dt.astimezone(ZoneInfo(case.timezone)).replace(tzinfo=None) if dt else None

    def solve(self, case: QACase) -> dict[str, str]:
        raise NotImplementedError


class IgnoresDeadlines(_FlawedSolver):
    name = "flaw:ignores-deadlines"

    def solve(self, case):
        return {k: derive_status(Fact(f.start, f.end, None, f.done_at, f.cancelled), case.now)
                for k, f in case.facts.items()}


class ReadsClockDigits(_FlawedSolver):
    """Compares wall-clock digits instead of instants: right until daylight
    saving time makes two different instants share digits (or none do)."""
    name = "flaw:clock-digits"

    def solve(self, case):
        now = self._wall(case, case.now)
        return {k: derive_status(Fact(self._wall(case, f.start), self._wall(case, f.end), self._wall(case, f.deadline),
                                      self._wall(case, f.done_at), f.cancelled), now)
                for k, f in case.facts.items()}


class MixesTimezones(_FlawedSolver):
    """Compares the task's local digits to the current time's UTC digits --
    the classic 'forgot to convert the offset' error."""
    name = "flaw:mixes-timezones"

    def solve(self, case):
        now = case.now.astimezone(UTC).replace(tzinfo=None)
        return {k: derive_status(Fact(self._wall(case, f.start), self._wall(case, f.end), self._wall(case, f.deadline),
                                      self._wall(case, f.done_at), f.cancelled), now)
                for k, f in case.facts.items()}


class AssumesFixedOffset(_FlawedSolver):
    """The natural daylight-saving mistake: read the zone's UTC offset ONCE
    (on the first date) and apply it to every occurrence. Right in a zone
    without DST and on any date before the next clock change; wrong after it.
    Only recurring tasks are affected -- explicit-offset timestamps carry
    their own offset -- so it must fail dst_recurrence and nothing else."""
    name = "flaw:fixed-offset"

    def solve(self, case):
        recur = next((s for s in case.steps if s.kind == "recur"), None)
        if recur is None:
            return {k: derive_status(f, case.now) for k, f in case.facts.items()}
        a, zone = recur.args, ZoneInfo(case.timezone)
        hours, minutes = (int(x) for x in a["start_time"].split(":"))
        offset = datetime.combine(date.fromisoformat(a["first_date"]), time(hours, minutes), tzinfo=zone).utcoffset()
        out = {}
        for key, fact in case.facts.items():
            day = date.fromisoformat(key.split("@")[1])
            start = (datetime.combine(day, time(hours, minutes)) - offset).replace(tzinfo=UTC)
            out[key] = derive_status(
                Fact(start, start + timedelta(minutes=a["duration_minutes"]), None, None, False), case.now)
        return out


# ---------------------------------------------------------------------- scoring

def wilson_interval(successes: int, trials: int, z: float = 1.96) -> tuple[float, float]:
    """95% interval for a proportion. With a few dozen items the interval is
    wide -- which is the honest thing to show."""
    if trials == 0:
        return 0.0, 1.0
    p = successes / trials
    denom = 1 + z * z / trials
    centre = (p + z * z / (2 * trials)) / denom
    half = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials)) / denom
    return max(0.0, centre - half), min(1.0, centre + half)


@dataclass
class CategoryScore:
    category: str
    cases: int = 0
    perfect_cases: int = 0
    tasks: int = 0
    tasks_correct: int = 0
    unanswered: int = 0     # nothing usable came back (unparseable, refused, crashed)

    @property
    def task_accuracy(self) -> float:
        return self.tasks_correct / self.tasks if self.tasks else 0.0


@dataclass
class Report:
    solver: str
    by_category: dict[str, CategoryScore]

    def total(self) -> CategoryScore:
        total = CategoryScore("ALL")
        for s in self.by_category.values():
            total.cases += s.cases; total.perfect_cases += s.perfect_cases
            total.tasks += s.tasks; total.tasks_correct += s.tasks_correct; total.unanswered += s.unanswered
        return total

    def table(self) -> str:
        rows = [f"solver: {self.solver}",
                f"{'category':16s} {'tasks correct':>16s} {'95% CI':>16s} {'cases perfect':>15s} {'no answer':>10s}"]
        for s in [*self.by_category.values(), self.total()]:
            lo, hi = wilson_interval(s.tasks_correct, s.tasks)
            rows.append(f"{s.category:16s} {f'{s.tasks_correct}/{s.tasks}':>16s} {f'[{lo:.2f},{hi:.2f}]':>16s} "
                        f"{f'{s.perfect_cases}/{s.cases}':>15s} {s.unanswered:>10d}")
        return "\n".join(rows)


def evaluate(solver: Solver, cases: list[QACase]) -> Report:
    by_category: dict[str, CategoryScore] = {}
    for case in cases:
        score = by_category.setdefault(case.category, CategoryScore(case.category))
        try:
            answer = solver.solve(case)
        except Exception:      # a solver that crashes scores zero on that case, it does not abort the run
            answer = {}
        score.unanswered += int(not answer)
        correct = sum(answer.get(k) == v for k, v in case.expected.items())
        score.cases += 1
        score.tasks += len(case.expected)
        score.tasks_correct += correct
        score.perfect_cases += int(correct == len(case.expected))
    return Report(solver.name, by_category)
