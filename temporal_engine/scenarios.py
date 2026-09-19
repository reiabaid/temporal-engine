"""
Decision fixtures: scenario -> context handed to a provider -> the decision
that is correct. This is the hard rubric from PLAN.md (Phase 1 note /
Phase 6): pass/fail against an encoded expectation, never a judge model's
opinion. The same fixtures score the deterministic stub in CI and any live
provider by hand (scripts/live_scenarios.py).

A scenario passes only if EVERY action the provider proposes is
  1. one of the scenario's allowed actions (an empty allowed set means
     "the correct decision is to do nothing", and any action fails),
  2. accepted by the real deterministic layer (apply_action outcome
     "applied") -- so a proposal that is illegal, malformed, or over the
     reschedule cap fails even if its action name was right, and
  3. satisfying the scenario's own constraint (e.g. must not overlap the
     next task).
Restraint counts: proposing nothing when nothing is needed is a correct
answer, and a model that acts on every event is wrong.

More scenarios (deadlines, recurrence, dependencies, multi-day) belong
here as the benchmark grows; these four cover the decision the engine
currently hands to a model.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from temporal_engine.actions import ActionCall, apply_action
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.providers import LLMProvider, TemporalContext
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc

Constraint = Callable[[ActionCall, TemporalContext], Optional[str]]


@dataclass
class Scenario:
    name: str
    description: str
    build: Callable[[], TemporalContext]
    allowed_actions: set[str]
    constraint: Optional[Constraint] = None


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    proposed: list[str]
    failures: list[str] = field(default_factory=list)


def _window_ended_event(task: Task, at: datetime) -> TemporalEvent:
    return TemporalEvent(EventType.TASK_WINDOW_ENDED, at, at, task.id)


def _slack_available() -> TemporalContext:
    now = datetime(2026, 9, 12, 16, 0, tzinfo=UTC)
    dsa = Task.new("DSA", "UTC", scheduled_start=now.replace(hour=14), scheduled_end=now)
    dsa.status = TaskStatus.WINDOW_ENDED
    project = Task.new(
        "Project work", "UTC",
        scheduled_start=now.replace(hour=18), scheduled_end=now.replace(hour=20),
    )
    return TemporalContext(now=now, events=[_window_ended_event(dsa, now)], tasks=[dsa, project])


def _fits_before_next_task(call: ActionCall, ctx: TemporalContext) -> Optional[str]:
    # Malformed values (strings, naive datetimes) are already failed by the
    # deterministic layer; a constraint must never crash on them.
    new_start, new_end = call.args.get("new_start"), call.args.get("new_end")
    next_start = datetime(2026, 9, 12, 18, 0, tzinfo=UTC)
    if _is_aware(new_end) and new_end > next_start:
        return f"new_end {new_end.isoformat()} overlaps the next task starting {next_start.isoformat()}"
    if _is_aware(new_start) and new_start < ctx.now:
        return "new_start is in the past"
    return None


def _is_aware(value: object) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None


def _no_time_left() -> TemporalContext:
    now = datetime(2026, 9, 12, 23, 50, tzinfo=UTC)
    dsa = Task.new("DSA", "UTC", scheduled_start=now.replace(hour=21), scheduled_end=now)
    dsa.status = TaskStatus.WINDOW_ENDED
    return TemporalContext(now=now, events=[_window_ended_event(dsa, now)], tasks=[dsa])


def _lands_on_a_later_day(call: ActionCall, ctx: TemporalContext) -> Optional[str]:
    new_start = call.args.get("new_start")
    if _is_aware(new_start) and new_start.date() <= ctx.now.date():
        return f"new_start {new_start.isoformat()} is not on a later day than {ctx.now.date()}"
    return None


def _nothing_to_do() -> TemporalContext:
    now = datetime(2026, 9, 13, 0, 0, tzinfo=UTC)
    later = Task.new(
        "Standup", "UTC",
        scheduled_start=now.replace(hour=10), scheduled_end=now.replace(hour=10, minute=30),
    )
    return TemporalContext(
        now=now, events=[TemporalEvent(EventType.NEW_DAY, now, now)], tasks=[later],
    )


def _stale_event_for_finished_task() -> TemporalContext:
    now = datetime(2026, 9, 12, 16, 30, tzinfo=UTC)
    dsa = Task.new("DSA", "UTC", scheduled_start=now.replace(hour=14), scheduled_end=now.replace(hour=16))
    ended_at = now.replace(hour=16, minute=0)
    dsa.status = TaskStatus.COMPLETED_LATE  # user finished it after the window ended
    return TemporalContext(now=now, events=[_window_ended_event(dsa, ended_at)], tasks=[dsa])


SCENARIOS: list[Scenario] = [
    Scenario(
        name="slack_available",
        description="A window ended with two free hours before the next task: move it into the gap.",
        build=_slack_available,
        allowed_actions={"reschedule_task"},
        constraint=_fits_before_next_task,
    ),
    Scenario(
        name="no_time_left",
        description="A window ended ten minutes before midnight: it cannot fit today, so it must move to a later day.",
        build=_no_time_left,
        allowed_actions={"carry_forward_task"},
        constraint=_lands_on_a_later_day,
    ),
    Scenario(
        name="nothing_to_do",
        description="Only a new day began and nothing is unfinished: the right decision is to act on nothing.",
        build=_nothing_to_do,
        allowed_actions=set(),
    ),
    Scenario(
        name="stale_event_for_finished_task",
        description="A window-ended event whose task the user has since completed: no action is warranted.",
        build=_stale_event_for_finished_task,
        allowed_actions=set(),
    ),
]


def run_scenario(provider: LLMProvider, scenario: Scenario) -> ScenarioResult:
    ctx = scenario.build()
    tasks = {t.id: t for t in ctx.tasks}

    try:
        calls = provider.decide(ctx)
    except Exception as exc:  # a provider crash is a failed scenario, not a crashed run
        return ScenarioResult(scenario.name, False, [], [f"provider raised {type(exc).__name__}: {exc}"])

    proposed = [c.action for c in calls]
    failures: list[str] = []

    if not scenario.allowed_actions and calls:
        failures.append(f"expected no action, but proposed {proposed}")
    if scenario.allowed_actions and not calls:
        failures.append(f"expected one of {sorted(scenario.allowed_actions)}, but proposed nothing")

    seen: set[str] = set()
    for call in calls:
        if scenario.allowed_actions and call.action not in scenario.allowed_actions:
            failures.append(f"{call.action} is not an allowed action here ({sorted(scenario.allowed_actions)})")
        decision = apply_action(tasks, call, ctx.now, seen)[0]
        # A held action is a correct outcome: the deterministic layer did
        # its job by not acting without a human.
        if decision.payload["outcome"] not in ("applied", "pending_confirmation"):
            failures.append(
                f"{call.action} was {decision.payload['outcome']} by the deterministic layer: "
                f"{decision.payload.get('rejection_reason')}"
            )
        if scenario.constraint is not None:
            problem = scenario.constraint(call, ctx)
            if problem:
                failures.append(problem)

    return ScenarioResult(scenario.name, not failures, proposed, failures)


def run_all(provider: LLMProvider) -> list[ScenarioResult]:
    return [run_scenario(provider, s) for s in SCENARIOS]
