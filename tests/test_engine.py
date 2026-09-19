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


def test_day_tracker_seeded_from_persisted_date_reports_a_boundary_crossed_while_down():
    from datetime import date

    tracker = DayTracker(last_seen_date=date(2026, 9, 12))  # last activity before shutdown
    after_restart = datetime(2026, 9, 15, 8, 0, tzinfo=UTC)  # three days later

    events = tracker.check(after_restart, "UTC")

    assert len(events) == 1
    assert events[0].payload == {"previous_date": "2026-09-12", "new_date": "2026-09-15"}


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


def test_a_deadline_only_task_becomes_overdue():
    """Regression: with no scheduled window the task sat at CREATED forever,
    because tick only considered deadlines for SCHEDULED/ACTIVE/WINDOW_ENDED."""
    now = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)
    t = Task.new("report", "UTC", deadline=now - timedelta(hours=1))
    assert t.status == TaskStatus.CREATED

    events = tick([t], now, DayTracker(), day_boundary_tz="UTC")

    assert t.status == TaskStatus.OVERDUE
    assert [e.event_type for e in events] == [EventType.TASK_DEADLINE_BREACHED]


def test_new_day_records_the_local_midnight_as_valid_time_and_the_notice_as_recorded_time():
    from zoneinfo import ZoneInfo
    kolkata = ZoneInfo("Asia/Kolkata")
    tracker = DayTracker()
    tracker.check(datetime(2026, 9, 12, 23, 0, tzinfo=kolkata), "Asia/Kolkata")

    noticed = datetime(2026, 9, 14, 9, 30, tzinfo=kolkata)      # noticed 33 hours after the boundary
    (event,) = tracker.check(noticed, "Asia/Kolkata")

    assert event.occurred_at == datetime(2026, 9, 14, 0, 0, tzinfo=kolkata)
    assert event.recorded_at == noticed


def test_the_day_boundary_follows_the_configured_timezone_not_utc():
    """23:00 UTC on the 12th is already the 13th at +05:30 -- the reason a
    server left on UTC would announce a new day at 05:30 local time."""
    from zoneinfo import ZoneInfo
    tracker = DayTracker()
    assert tracker.check(datetime(2026, 9, 12, 17, 0, tzinfo=UTC), "Asia/Kolkata") == []   # 22:30 IST, the 12th
    events = tracker.check(datetime(2026, 9, 12, 19, 0, tzinfo=UTC), "Asia/Kolkata")       # 00:30 IST, the 13th
    assert len(events) == 1 and events[0].payload["new_date"] == "2026-09-13"
