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
from datetime import datetime, time, timedelta
from typing import Protocol

from temporal_engine.actions import ActionCall
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.task import Task, TaskStatus


@dataclass
class TemporalContext:
    now: datetime
    events: list[TemporalEvent]
    tasks: list[Task]


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
        for event in ctx.events:
            if event.event_type != EventType.TASK_WINDOW_ENDED:
                continue
            # Events can be stale by the time we look at them: the user may
            # have completed the task since the window ended. Acting on a
            # finished task would just be a rejected proposal.
            task = tasks_by_id.get(event.task_id)
            if task is None or task.is_terminal():
                continue
            actions.append(self._handle_window_ended(ctx, event))
        return actions

    def _handle_window_ended(self, ctx: TemporalContext, event: TemporalEvent) -> ActionCall:
        task = next(t for t in ctx.tasks if t.id == event.task_id)
        duration = (
            task.scheduled_end - task.scheduled_start
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
            ctx.now.date() + timedelta(days=1), time.min, tzinfo=ctx.now.tzinfo
        )
        remaining_today = [
            t for t in ctx.tasks
            if t.status == TaskStatus.SCHEDULED
            and t.scheduled_start
            and t.scheduled_start.date() == ctx.now.date()
            and t.scheduled_start > ctx.now
        ]
        boundaries = [next_midnight] + [t.scheduled_start for t in remaining_today]
        slack = min(boundaries) - ctx.now

        if slack > timedelta(minutes=30):
            new_end = ctx.now + min(duration, slack)
            return ActionCall(
                idempotency_key=str(uuid.uuid4()),
                action="reschedule_task",
                task_id=task.id,
                args={"new_start": ctx.now, "new_end": new_end},
                reason=(
                    f"{int(slack.total_seconds() // 60)} min of slack before the "
                    f"next task; squeezing '{task.title}' in now."
                ),
            )
        else:
            tomorrow = ctx.now.date() + timedelta(days=1)
            new_start = datetime.combine(tomorrow, time(9, 0), tzinfo=ctx.now.tzinfo)
            return ActionCall(
                idempotency_key=str(uuid.uuid4()),
                action="carry_forward_task",
                task_id=task.id,
                args={"new_start": new_start, "new_end": new_start + duration},
                reason="No slack left today; carrying forward to tomorrow's plan.",
            )
