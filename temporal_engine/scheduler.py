"""
Clock + next_wake_time: the event-driven scheduling core.

The whole point of this file is that nothing outside it ever asks "is it
time yet?" in a loop. Instead, next_wake_time computes the single next
instant at which *anything* observable could change, and the caller
(real code: asyncio.sleep; tests: SimulatedClock.set) jumps straight
there. See PLAN.md's "wake-up mechanism" decision for why this matters.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Iterable
from zoneinfo import ZoneInfo

from temporal_engine.task import Task


class Clock:
    def now(self) -> datetime:
        raise NotImplementedError


class RealClock(Clock):
    """Production clock. Wraps datetime.now() so the rest of the codebase
    never calls it directly -- that's what makes SimulatedClock a drop-in
    replacement for tests, instead of every function needing a `now`
    parameter threaded through it by hand."""

    def __init__(self, tz_name: str = "UTC"):
        self.tz = ZoneInfo(tz_name)

    def now(self) -> datetime:
        return datetime.now(self.tz)


class SimulatedClock(Clock):
    """Test/demo clock. Lets you jump straight to the next event boundary
    instead of sleeping in real time -- this is how a whole simulated day
    can run in a fraction of a second."""

    def __init__(self, start: datetime):
        self._now = start

    def now(self) -> datetime:
        return self._now

    def set(self, dt: datetime) -> None:
        if dt < self._now:
            raise ValueError(f"clock must not move backwards: {dt} < {self._now}")
        self._now = dt


def _next_local_midnight(now: datetime, tz_name: str) -> datetime:
    """The next NEW_DAY boundary, in the engine's configured day-boundary
    timezone -- not necessarily the same timezone any individual task is
    displayed in."""
    tz = ZoneInfo(tz_name)
    local_now = now.astimezone(tz)
    next_day = local_now.date() + timedelta(days=1)
    return datetime.combine(next_day, time.min, tzinfo=tz)


def next_wake_time(tasks: Iterable[Task], now: datetime, day_boundary_tz: str) -> datetime:
    """The earliest future instant at which some task's state could change,
    or the next local midnight -- whichever comes first. Always returns a
    real datetime, never None: even with zero tasks, tomorrow's midnight
    is always a candidate, so there is always something to wait for.
    """
    candidates = [_next_local_midnight(now, day_boundary_tz)]

    for task in tasks:
        if task.is_terminal():
            continue
        for boundary in (task.scheduled_start, task.scheduled_end, task.deadline):
            if boundary is not None and boundary > now:
                candidates.append(boundary)

    return min(candidates)
