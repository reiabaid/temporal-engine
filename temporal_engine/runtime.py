"""
Runtime: everything a running engine process does, minus the MCP protocol.
Kept separate so it can be tested deterministically -- two Runtime objects
on one database file stand in for two client processes, with a
SimulatedClock, and no subprocesses or real waiting.

Every process runs the same loop. Each pass it:
  1. tries to become THE scheduler if it isn't (the lock is retried every
     pass, so if the holder exits -- e.g. Claude Code is closed while Claude
     Desktop stays open -- another process takes over instead of nobody
     ever ticking again);
  2. refreshes its view of the log, so it sees what other processes did;
  3. if it is the scheduler, ticks inside a write transaction.
Sleeping is bounded by `refresh_interval`. That is not polling an LLM: it
is a local check that lets a process notice boundaries created by another
process, and bounds how late a boundary can be noticed after the machine
was suspended (asyncio timers do not count suspended time). Boundaries this
process already knows about are still ticked at the exact instant.

Errors never kill the loop. A failed pass is logged, recorded in `health`
(which the MCP layer reports, so a stalled scheduler is visible rather
than silent), and retried with backoff.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from temporal_engine.actions import ActionCall, apply_action, decline_event, action_call_from_payload
from temporal_engine.engine import DayTracker, tick
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.lock import SchedulerLock, SchedulerLockHeld
from temporal_engine.mutators import materialize
from temporal_engine.scheduler import Clock, RealClock, next_wake_time
from temporal_engine.storage import Store, view_as_of
from temporal_engine.task import Task

log = logging.getLogger("temporal_engine")

BACKOFF_BASE_SECONDS = 1.0
# Tasks created through create_task (any caller) per rolling 24 hours. A runaway client would otherwise grow the
# append-only log without bound; the log can never be trimmed, so the limit has to be at the door.
DEFAULT_MAX_CREATIONS_PER_DAY = 1000
# A clock that moves backwards by more than this between passes is reported (NTP steps, manual changes, VM restores).
CLOCK_REGRESSION_TOLERANCE = timedelta(seconds=5)
MAX_BACKOFF_SECONDS = 60.0


@dataclass
class Health:
    is_scheduler: bool = False
    last_pass_at: Optional[datetime] = None
    last_tick_at: Optional[datetime] = None
    last_error: Optional[str] = None
    consecutive_errors: int = 0
    # Templates that could not be generated on the latest pass: {recurrence id: reason}. The scheduler keeps
    # ticking everything else, so this is where a stuck template becomes visible.
    recurrence_errors: dict[str, str] = field(default_factory=dict)
    # The engine cannot know whether the clock or the world is right, so it does not guess: it counts backward
    # steps and says so. (NEW_DAY is forward-only regardless, so a step back cannot re-announce a day.)
    clock_regressions: int = 0
    last_clock_warning: Optional[str] = None


class Runtime:
    def __init__(
        self,
        db_path: str | Path,
        day_boundary_tz: str = "UTC",
        clock: Optional[Clock] = None,
        refresh_interval: float = 5.0,
        max_creations_per_day: int = DEFAULT_MAX_CREATIONS_PER_DAY,
    ):
        try:
            ZoneInfo(day_boundary_tz)
        except (KeyError, ValueError) as exc:
            raise ValueError(
                f"unknown day-boundary timezone {day_boundary_tz!r} (expected an IANA name, "
                f"e.g. 'Asia/Kolkata')"
            ) from exc

        self.day_boundary_tz = day_boundary_tz
        self.clock = clock or RealClock(day_boundary_tz)
        self.refresh_interval = refresh_interval
        self.max_creations_per_day = max_creations_per_day
        self.health = Health()
        self.wake_event = asyncio.Event()
        # Set after a scheduler pass records events, so the agent loop reacts
        # when something happens instead of only on its poll interval.
        self.new_events = asyncio.Event()
        self.agent = None  # set by the host if an Agent is enabled

        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.store = Store(str(db_path))
        self.lock = SchedulerLock(db_path.with_suffix(".lock"))
        self._tracker: Optional[DayTracker] = None
        self._logged_recurrence_errors: dict[str, str] = {}

    # ------------------------------------------------------------- scheduling

    def run_pass(self) -> None:
        """One scheduling pass. Synchronous and deterministic given the
        clock; run_forever wraps it with sleeping and error handling."""
        now = self.clock.now()
        self._note_clock(now)
        try:
            if not self.health.is_scheduler:
                try:
                    self.lock.acquire()
                    self.health.is_scheduler = True
                    self._tracker = None
                except SchedulerLockHeld:
                    pass

            if not self.health.is_scheduler:
                self.store.refresh()
            else:
                if self._tracker is None:
                    self.store.refresh()
                    self._tracker = DayTracker(last_seen_date=self.store.day_seed(self.day_boundary_tz))
                with self.store.transaction() as view:
                    # Read inside the transaction: waiting for the write
                    # lock can take a moment, and boundaries must be judged
                    # against the time at which we actually hold it.
                    now = self.clock.now()
                    # Materialize BEFORE ticking, so an occurrence created
                    # already-over (a window missed within the lookback) is
                    # ended by this same pass.
                    self.health.recurrence_errors = {}
                    events = materialize(view, now, on_error=self._recurrence_failed)
                    self.store.append(events)
                    ticked = tick(view.tasks.values(), now, self._tracker, self.day_boundary_tz)
                    self.store.append(ticked)
                    events = events + ticked
                if events:
                    self.health.last_tick_at = now
                    self.new_events.set()
        except BaseException:
            # The tracker may have advanced past a NEW_DAY that was never
            # persisted; forget it so it is re-seeded from the log.
            self._tracker = None
            raise

        self.health.last_pass_at = now
        self.health.last_error = None
        self.health.consecutive_errors = 0

    def _note_clock(self, now: datetime) -> None:
        last = self.health.last_pass_at
        if last is not None and now < last - CLOCK_REGRESSION_TOLERANCE:
            self.health.clock_regressions += 1
            self.health.last_clock_warning = (
                f"the clock moved backwards by {int((last - now).total_seconds())}s "
                f"({last.isoformat()} -> {now.isoformat()}); day changes never go backwards, but check the system time"
            )
            log.warning(self.health.last_clock_warning)

    def _recurrence_failed(self, recurrence, exc: Exception) -> None:
        message = f"{type(exc).__name__}: {exc}"
        self.health.recurrence_errors[recurrence.id] = message
        if self._logged_recurrence_errors.get(recurrence.id) != message:      # once per distinct failure, not every pass
            self._logged_recurrence_errors[recurrence.id] = message
            log.error("recurring task %r (%s) cannot be generated: %s", recurrence.title, recurrence.id, message)

    def next_delay(self) -> float:
        now = self.clock.now()
        wake = next_wake_time(self.store.view.tasks.values(), now, self.day_boundary_tz)
        return max(0.0, min((wake - now).total_seconds(), self.refresh_interval))

    async def run_forever(self) -> None:
        while True:
            # Cleared before the pass so a signal raised during it isn't lost.
            self.wake_event.clear()
            try:
                self.run_pass()
                delay = self.next_delay()
            except Exception as exc:
                self.health.consecutive_errors += 1
                self.health.last_error = f"{type(exc).__name__}: {exc}"
                log.exception("scheduler pass failed (%d in a row)", self.health.consecutive_errors)
                delay = min(MAX_BACKOFF_SECONDS, BACKOFF_BASE_SECONDS * 2.0 ** self.health.consecutive_errors)
            try:
                await asyncio.wait_for(self.wake_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    # -------------------------------------------------------------- mutations

    def execute(self, call: ActionCall, confirmed: bool = False) -> list[TemporalEvent]:
        """Validate and apply one action against the log's current state and
        persist the result atomically. Safe against other processes: the
        write lock is held from refresh to commit."""
        now = self.clock.now()
        with self.store.transaction() as view:
            events = self._apply(view, call, now, confirmed)
        self.wake_event.set()
        return events

    def _apply(self, view, call: ActionCall, now, confirmed: bool) -> list[TemporalEvent]:
        """Apply one action and persist it -- and, if it created a recurring
        template, generate its occurrences in the SAME transaction, so the
        caller gets them back immediately and a crash cannot leave a template
        with no occurrences."""
        if (call.action == "create_task" and call.idempotency_key not in view.seen_keys
                and (confirmed or not call.requires_confirmation)
                and self.store.count_creations_since(now - timedelta(hours=24)) >= self.max_creations_per_day):
            events = [decline_event(call, now, f"creation limit reached ({self.max_creations_per_day} tasks per 24 hours)")]
            self.store.append(events)
            return events
        events = apply_action(
            view.tasks, call, now, view.seen_keys, confirmed=confirmed, recurrences=view.recurrences,
        )
        self.store.append(events)
        new_ids = {e.payload["id"] for e in events if e.event_type == EventType.RECURRENCE_CREATED}
        if new_ids:
            def strict(recurrence, exc):
                if recurrence.id in new_ids:
                    raise exc      # the template being created cannot be generated: refuse it (the transaction rolls back)
                self._recurrence_failed(recurrence, exc)

            extra = materialize(view, now, on_error=strict)
            self.store.append(extra)
            events = events + extra
        return events

    def confirm(self, idempotency_key: str) -> list[TemporalEvent]:
        """Apply an action that was held for confirmation. The 'is it still
        pending?' check happens inside the write transaction: checked
        outside, another process could decline it in the gap and this would
        still apply it."""
        now = self.clock.now()
        with self.store.transaction() as view:
            pending = view.pending.get(idempotency_key)
            if pending is None:
                raise ValueError(f"no pending action with key {idempotency_key!r}")
            events = self._apply(view, action_call_from_payload(pending), now, confirmed=True)
        self.wake_event.set()
        return events

    def decline(self, idempotency_key: str) -> list[TemporalEvent]:
        now = self.clock.now()
        with self.store.transaction() as view:
            pending = view.pending.get(idempotency_key)
            if pending is None:
                raise ValueError(f"no pending action with key {idempotency_key!r}")
            events = [decline_event(action_call_from_payload(pending), now)]
            self.store.append(events)
        return events

    def state_as_of(self, at: datetime) -> dict[str, Task]:
        """The task list as it stood at `at`: the events recorded by then, with the clock-derived statuses brought
        to `at` (so a window that had ended by then reads as ended even if the app was closed and only noticed
        later). For an instant in the FUTURE this is a projection assuming nothing else happens -- only the
        clock moves tasks on."""
        view = view_as_of(self.store.conn, at)
        tick(view.tasks.values(), at, DayTracker(), self.day_boundary_tz)     # a throwaway view: nothing is persisted
        return view.tasks

    def reject(self, call: ActionCall, reason: str) -> list[TemporalEvent]:
        """Record a call as rejected without trying it (a limit the caller enforces, e.g. the agent's
        creation budget), so what was proposed and refused is still in the decision log."""
        now = self.clock.now()
        with self.store.transaction():
            events = [decline_event(call, now, reason)]
            self.store.append(events)
        return events

    def close(self) -> None:
        if self.health.is_scheduler:
            self.lock.release()
            self.health.is_scheduler = False
        self.store.close()
