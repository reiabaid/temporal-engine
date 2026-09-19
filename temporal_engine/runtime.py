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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from temporal_engine.actions import ActionCall, apply_action, decline_event, action_call_from_payload
from temporal_engine.engine import DayTracker, tick
from temporal_engine.events import TemporalEvent
from temporal_engine.lock import SchedulerLock, SchedulerLockHeld
from temporal_engine.scheduler import Clock, RealClock, next_wake_time
from temporal_engine.storage import Store

log = logging.getLogger("temporal_engine")

BACKOFF_BASE_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0


@dataclass
class Health:
    is_scheduler: bool = False
    last_pass_at: Optional[datetime] = None
    last_tick_at: Optional[datetime] = None
    last_error: Optional[str] = None
    consecutive_errors: int = 0


class Runtime:
    def __init__(
        self,
        db_path: str | Path,
        day_boundary_tz: str = "UTC",
        clock: Optional[Clock] = None,
        refresh_interval: float = 5.0,
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
        self.health = Health()
        self.wake_event = asyncio.Event()
        # Set after a scheduler pass records events, so the agent loop reacts
        # when something happens instead of only on its poll interval.
        self.new_events = asyncio.Event()
        self.agent = None  # set by the host if an Agent is enabled

        db_path = Path(db_path)
        self.store = Store(str(db_path))
        self.lock = SchedulerLock(db_path.with_suffix(".lock"))
        self._tracker: Optional[DayTracker] = None

    # ------------------------------------------------------------- scheduling

    def run_pass(self) -> None:
        """One scheduling pass. Synchronous and deterministic given the
        clock; run_forever wraps it with sleeping and error handling."""
        now = self.clock.now()
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
                    events = tick(view.tasks.values(), now, self._tracker, self.day_boundary_tz)
                    self.store.append(events)
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
            events = apply_action(view.tasks, call, now, view.seen_keys, confirmed=confirmed)
            self.store.append(events)
        self.wake_event.set()
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
            events = apply_action(
                view.tasks, action_call_from_payload(pending), now, view.seen_keys, confirmed=True,
            )
            self.store.append(events)
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

    def close(self) -> None:
        if self.health.is_scheduler:
            self.lock.release()
            self.health.is_scheduler = False
        self.store.close()
