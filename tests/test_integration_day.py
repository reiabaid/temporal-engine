"""
Phase 1's exit criterion, as an actual test: a fast-forwarded simulated
day, zero polling, and a genuine crash-recovery check (the connection is
closed and a fresh one reopened against the same file, so recovery is
proven from nothing but the on-disk event log -- not from in-memory Task
objects that a real crash would have destroyed).

This is deliberately the scenario from the very first design discussion:
a DSA block from 14:00-16:00 that the user never marks complete.
"""
import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from temporal_engine.engine import DayTracker, tick
from temporal_engine.events import EventType
from temporal_engine.scheduler import SimulatedClock, next_wake_time
from temporal_engine.storage import append_event, init_db, replay, task_created_event
from temporal_engine.task import Task, TaskStatus

TZ_NAME = "Asia/Kolkata"
TZ = ZoneInfo(TZ_NAME)


def test_full_day_fires_correct_events_with_no_polling_then_survives_a_crash(tmp_path):
    db_path = tmp_path / "day.db"
    conn = sqlite3.connect(str(db_path))
    init_db(conn)

    day1 = datetime(2026, 9, 12, tzinfo=TZ)
    dsa = Task.new(
        "DSA", TZ_NAME,
        scheduled_start=day1.replace(hour=14),
        scheduled_end=day1.replace(hour=16),
    )
    clock = SimulatedClock(day1.replace(hour=13, minute=55))
    append_event(conn, task_created_event(dsa, at=clock.now()))

    day_tracker = DayTracker()
    fired_event_types = []

    # The loop itself is the "no polling" proof: each iteration jumps
    # straight to the next meaningful instant via next_wake_time, never
    # to "one minute later." A day with one task takes exactly 3 jumps
    # to reach NEW_DAY: start, window-end, midnight.
    for _ in range(10):
        wake = next_wake_time([dsa], clock.now(), day_boundary_tz=TZ_NAME)
        clock.set(wake)
        events = tick([dsa], clock.now(), day_tracker, day_boundary_tz=TZ_NAME)
        for event in events:
            append_event(conn, event)
            fired_event_types.append(event.event_type)
        if EventType.NEW_DAY in [e.event_type for e in events]:
            break

    assert fired_event_types == [
        EventType.TASK_STARTED,
        EventType.TASK_WINDOW_ENDED,
        EventType.NEW_DAY,
    ]
    # Never marked complete -- deciding what to do about that is Phase 2's
    # job (an LLM/mutator layer), not something the deterministic engine
    # should ever guess at on its own.
    assert dsa.status == TaskStatus.WINDOW_ENDED

    live_conn = conn
    live_conn.close()  # simulate a crash: connection AND the in-memory
                        # `dsa` object are both gone from here on --
                        # everything below rebuilds from disk alone.

    recovered_conn = sqlite3.connect(str(db_path))
    recovered_tasks = replay(recovered_conn)

    assert recovered_tasks[dsa.id].title == "DSA"
    assert recovered_tasks[dsa.id].status == TaskStatus.WINDOW_ENDED
    assert recovered_tasks[dsa.id].scheduled_start == day1.replace(hour=14)
