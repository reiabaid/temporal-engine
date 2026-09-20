"""
LLMProvider: the model-agnostic reasoning boundary from SPEC.md. Any
provider implementing `decide(ctx) -> list[ActionCall]` can sit behind
the engine interchangeably -- that substitutability is the actual point
of the whole project, made concrete as code instead of an aspiration.

TemporalContext is deliberately just data: current time, the events since
the caller last looked, and the current state of every task. A provider
is never allowed to reach around this and query storage directly -- if it
needs a fact, that fact belongs in this struct, so every provider
(Claude, GPT, a local model, this stub) reasons over exactly the same
information.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

from temporal_engine.actions import ActionCall
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.task import Task, TaskStatus


@dataclass
class TemporalContext:
    now: datetime
    events: list[TemporalEvent]
    tasks: list[Task]
    # The user's timezone (IANA). `now` is an unambiguous instant, usually
    # UTC, and says nothing about what "today", "midnight" or "9am" mean to
    # the person -- this does.
    timezone: str = "UTC"
    # Unfinished tasks left out of `tasks` to bound the prompt (they cost money and dilute attention).
    omitted_tasks: int = 0


class LLMProvider(Protocol):
    def decide(self, ctx: TemporalContext) -> list[ActionCall]: ...


class StubHeuristicLLM:
    """A deterministic stand-in for a real model call -- no API key, no
    network, no cost. Exists so the engine can be exercised end-to-end in
    automated tests. A real provider (see anthropic_provider.py)
    implements the identical `decide` signature and drops in with zero
    changes to engine.py, actions.py, or mutators.py.

    This also fixes, deliberately, the exact bug the very first prototype
    of this project hit: that version set a rescheduled task's new_end
    equal to its new_start (zero duration), which immediately re-fired
    WINDOW_ENDED and rescheduled again, forever. Here, new_end always
    preserves the task's original duration, capped by however much slack
    actually exists -- and even if this logic were wrong again, the
    reschedule cap in mutators.py would now stop it regardless.
    """

    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        actions: list[ActionCall] = []
        tasks_by_id = {t.id: t for t in ctx.tasks}
        handled: set[str] = set()
        for event in ctx.events:
            if event.event_type != EventType.TASK_WINDOW_ENDED:
                continue
            # Events can be stale by the time we look at them: the user may
            # have completed the task since the window ended. Acting on a
            # finished task would just be a rejected proposal.
            task = tasks_by_id.get(event.task_id)
            if task is None or task.is_terminal():
                continue
            handled.add(task.id)
            actions.append(self._handle_window_ended(ctx, event))
        if any(e.event_type == EventType.NEW_DAY for e in ctx.events):
            actions.extend(self._handle_new_day(ctx, handled))
        return actions

    def _handle_window_ended(self, ctx: TemporalContext, event: TemporalEvent) -> ActionCall:
        task = next(t for t in ctx.tasks if t.id == event.task_id)
        utc = timezone.utc
        local = ZoneInfo(ctx.timezone)
        now = ctx.now.astimezone(utc)          # all arithmetic on UTC instants: exact across DST
        local_today = now.astimezone(local).date()

        duration = (
            task.scheduled_end.astimezone(utc) - task.scheduled_start.astimezone(utc)
            if task.scheduled_start and task.scheduled_end
            else timedelta(hours=1)
        )

        # Slack is bounded by two things, not just "the next task": the
        # next same-day task's start, AND the day boundary itself. The
        # first version of this heuristic only checked the former, which
        # meant "nothing else scheduled today" was treated as infinite
        # slack even at 11:50pm -- happily proposing a reschedule that
        # would run past midnight. Fixed by always capping slack at
        # whichever comes first.
        next_midnight = datetime.combine(
            local_today + timedelta(days=1), time.min, tzinfo=local
        ).astimezone(utc)
        remaining_today = [
            t for t in ctx.tasks
            if t.status == TaskStatus.SCHEDULED
            and t.scheduled_start
            and t.scheduled_start.astimezone(local).date() == local_today
            and t.scheduled_start > now
        ]
        boundaries = [next_midnight] + [t.scheduled_start.astimezone(utc) for t in remaining_today]
        slack = min(boundaries) - now

        if slack > timedelta(minutes=30):
            new_end = now + min(duration, slack)
            return ActionCall(
                idempotency_key=str(uuid.uuid4()),
                action="reschedule_task",
                task_id=task.id,
                args={"new_start": now, "new_end": new_end},
                reason=(
                    f"{int(slack.total_seconds() // 60)} min of slack before the "
                    f"next task; squeezing '{task.title}' in now."
                ),
            )
        else:
            tomorrow_9am = datetime.combine(
                local_today + timedelta(days=1), time(9, 0), tzinfo=local
            ).astimezone(utc)
            return ActionCall(
                idempotency_key=str(uuid.uuid4()),
                action="carry_forward_task",
                task_id=task.id,
                args={"new_start": tomorrow_9am, "new_end": tomorrow_9am + duration},
                reason="No slack left today; carrying forward to tomorrow's plan.",
            )

    def _handle_new_day(self, ctx: TemporalContext, handled: set[str]) -> list[ActionCall]:
        """Yesterday's unfinished work is carried forward to today, stacked from 09:00 (or now, if it is later)
        in the order it was originally due. Tasks whose deadline would already have passed are left alone:
        extending a deadline is a human's decision, not a heuristic's."""
        utc, local = timezone.utc, ZoneInfo(ctx.timezone)
        now = ctx.now.astimezone(utc)
        today = now.astimezone(local).date()
        cursor = max(now, datetime.combine(today, time(9, 0), tzinfo=local).astimezone(utc))

        stale = sorted(
            (t for t in ctx.tasks
             if t.id not in handled and t.status == TaskStatus.WINDOW_ENDED
             and t.scheduled_start and t.scheduled_end and t.scheduled_end.astimezone(local).date() < today),
            key=lambda t: t.scheduled_end,
        )
        actions = []
        for task in stale:
            duration = task.scheduled_end.astimezone(utc) - task.scheduled_start.astimezone(utc)
            if task.deadline and cursor >= task.deadline.astimezone(utc):
                continue
            actions.append(ActionCall(
                idempotency_key=str(uuid.uuid4()), action="carry_forward_task", task_id=task.id,
                args={"new_start": cursor, "new_end": cursor + duration},
                reason=f"'{task.title}' was not finished yesterday; carrying it into today's plan.",
            ))
            cursor += duration
        return actions
