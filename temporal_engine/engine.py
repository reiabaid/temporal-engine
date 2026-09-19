"""
tick(): the one function where task.py's transition table, events.py's
event types, and scheduler.py's day-boundary logic all meet.

tick() is deliberately pure with respect to storage: it mutates the Task
objects it's given (via apply_transition) and returns the TemporalEvents
that resulted, but it never touches the database itself. The caller
decides whether/how to persist those events. Keeping "what changed"
separate from "persist it" is what makes this function trivial to test
without a database in the loop at all.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.task import Task, TaskStatus


class DayTracker:
    """Remembers the last local date `tick` has seen, so NEW_DAY fires
    exactly once per crossing -- not on every tick call made during that
    same day. This is small, mutable, per-engine-instance state the
    caller owns and passes in, the same way SimulatedClock's internal
    `_now` is owned by the clock instance, not global."""

    def __init__(self, last_seen_date: Optional[date] = None) -> None:
        # Seed this from persisted state on restart. Starting empty means
        # the first check() only records a baseline and reports nothing,
        # so a day boundary crossed while the process was down would be
        # silently lost. If several days elapsed, one NEW_DAY is emitted
        # whose payload carries the previous and new dates.
        self._last_seen_date: Optional[date] = last_seen_date

    def check(self, now: datetime, day_boundary_tz: str) -> list[TemporalEvent]:
        local_date = now.astimezone(ZoneInfo(day_boundary_tz)).date()

        if self._last_seen_date is None:
            self._last_seen_date = local_date
            return []

        if local_date == self._last_seen_date:
            return []

        event = TemporalEvent(
            event_type=EventType.NEW_DAY,
            occurred_at=now,
            recorded_at=now,
            payload={
                "previous_date": self._last_seen_date.isoformat(),
                "new_date": local_date.isoformat(),
            },
        )
        self._last_seen_date = local_date
        return [event]


def tick(
    tasks: Iterable[Task],
    now: datetime,
    day_tracker: DayTracker,
    day_boundary_tz: str,
) -> list[TemporalEvent]:
    """Advance every task's state to what it deterministically should be
    at `now`, returning the events raised.

    Idempotent by construction, not by special-casing: each `if` checks
    the task's *current* status before transitioning, so calling tick()
    twice at the same `now` fires nothing the second time -- the first
    call already moved every task past the condition that would trigger
    it again. This is also what makes catching up after downtime safe:
    if `now` has jumped far ahead because the process was offline, a
    single tick() call chains through every boundary that was missed,
    in order, in one pass.
    """
    events: list[TemporalEvent] = []
    events.extend(day_tracker.check(now, day_boundary_tz))

    for task in tasks:
        if task.is_terminal():
            continue

        if task.status == TaskStatus.SCHEDULED and task.scheduled_start and now >= task.scheduled_start:
            task.apply_transition(TaskStatus.ACTIVE, at=now)
            events.append(TemporalEvent(EventType.TASK_STARTED, task.scheduled_start, now, task.id))

        if task.status == TaskStatus.ACTIVE and task.scheduled_end and now >= task.scheduled_end:
            task.apply_transition(TaskStatus.WINDOW_ENDED, at=now)
            events.append(TemporalEvent(EventType.TASK_WINDOW_ENDED, task.scheduled_end, now, task.id))

        if (
            task.status in (TaskStatus.SCHEDULED, TaskStatus.ACTIVE, TaskStatus.WINDOW_ENDED)
            and task.deadline
            and now >= task.deadline
        ):
            task.apply_transition(TaskStatus.OVERDUE, at=now)
            events.append(TemporalEvent(EventType.TASK_DEADLINE_BREACHED, task.deadline, now, task.id))

    return events
