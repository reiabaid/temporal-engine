from datetime import datetime, timedelta, timezone

from temporal_engine.engine import DayTracker, tick
from temporal_engine.events import EventType
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc


def test_tick_starts_a_task_once_now_reaches_scheduled_start():
    now = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)
    t = Task.new("DSA", "UTC", scheduled_start=now, scheduled_end=now + timedelta(hours=2))

    events = tick([t], now, DayTracker(), day_boundary_tz="UTC")

    assert t.status == TaskStatus.ACTIVE
    assert [e.event_type for e in events] == [EventType.TASK_STARTED]


def test_tick_ends_the_window_once_now_reaches_scheduled_end():
    start = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)
    end = start + timedelta(hours=2)
    t = Task.new("DSA", "UTC", scheduled_start=start, scheduled_end=end)
    t.status = TaskStatus.ACTIVE  # already started, as if a prior tick did it

    events = tick([t], end, DayTracker(), day_boundary_tz="UTC")

    assert t.status == TaskStatus.WINDOW_ENDED
    assert [e.event_type for e in events] == [EventType.TASK_WINDOW_ENDED]


def test_tick_marks_overdue_once_deadline_passes():
    start = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)
    t = Task.new("Report", "UTC", scheduled_start=start, deadline=start + timedelta(hours=1))
    t.status = TaskStatus.WINDOW_ENDED

    events = tick([t], start + timedelta(hours=1), DayTracker(), day_boundary_tz="UTC")

    assert t.status == TaskStatus.OVERDUE
    assert [e.event_type for e in events] == [EventType.TASK_DEADLINE_BREACHED]


def test_tick_chains_through_every_missed_boundary_after_downtime():
    """The 'device was offline' hard problem from our design discussion:
    if `now` jumps far past several boundaries at once, one tick() call
    must catch every one of them, in order, not just the first."""
    start = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)
    end = start + timedelta(hours=2)
    deadline = end + timedelta(hours=1)
    t = Task.new("DSA", "UTC", scheduled_start=start, scheduled_end=end, deadline=deadline)

    # simulate coming back online long after all three boundaries passed
    events = tick([t], deadline + timedelta(hours=5), DayTracker(), day_boundary_tz="UTC")

    assert t.status == TaskStatus.OVERDUE
    assert [e.event_type for e in events] == [
        EventType.TASK_STARTED,
        EventType.TASK_WINDOW_ENDED,
        EventType.TASK_DEADLINE_BREACHED,
    ]


def test_tick_is_idempotent_when_called_twice_at_the_same_instant():
    now = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)
    t = Task.new("DSA", "UTC", scheduled_start=now, scheduled_end=now + timedelta(hours=2))
    tracker = DayTracker()

    first = tick([t], now, tracker, day_boundary_tz="UTC")
    second = tick([t], now, tracker, day_boundary_tz="UTC")

    assert len(first) == 1
    assert second == []


def test_tick_ignores_terminal_tasks_entirely():
    now = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)
    t = Task.new("Old", "UTC", scheduled_start=now)
    t.status = TaskStatus.COMPLETED

    events = tick([t], now + timedelta(days=1), DayTracker(), day_boundary_tz="UTC")

    assert t.status == TaskStatus.COMPLETED  # untouched
    assert events == []


def test_day_tracker_fires_new_day_exactly_once_per_crossing():
    tracker = DayTracker()
    day1_evening = datetime(2026, 9, 12, 23, 0, tzinfo=UTC)
    day2_morning = datetime(2026, 9, 13, 0, 5, tzinfo=UTC)
    day2_noon = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)

    first = tracker.check(day1_evening, "UTC")
    second = tracker.check(day2_morning, "UTC")
    third = tracker.check(day2_noon, "UTC")

    assert first == []
    assert len(second) == 1 and second[0].event_type.value == "NEW_DAY"
    assert third == []  # same day as `second` -- must not fire again
