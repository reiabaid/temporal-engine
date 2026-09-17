from datetime import datetime, timedelta, timezone

import pytest
from hypothesis import given, strategies as st

from temporal_engine.scheduler import SimulatedClock, next_wake_time
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc


def test_next_wake_time_with_no_tasks_is_next_midnight_in_day_boundary_tz():
    now = datetime(2026, 9, 12, 13, 55, tzinfo=UTC)
    wake = next_wake_time([], now, day_boundary_tz="UTC")
    assert wake == datetime(2026, 9, 13, 0, 0, tzinfo=UTC)


def test_next_wake_time_picks_earliest_task_boundary_before_midnight():
    now = datetime(2026, 9, 12, 13, 55, tzinfo=UTC)
    dsa = Task.new(
        "DSA", "UTC",
        scheduled_start=now.replace(hour=14),
        scheduled_end=now.replace(hour=16),
    )
    wake = next_wake_time([dsa], now, day_boundary_tz="UTC")
    assert wake == now.replace(hour=14)  # scheduled_start, not midnight


def test_next_wake_time_ignores_boundaries_already_in_the_past():
    now = datetime(2026, 9, 12, 15, 0, tzinfo=UTC)
    dsa = Task.new(
        "DSA", "UTC",
        scheduled_start=now.replace(hour=14),  # already passed
        scheduled_end=now.replace(hour=16),
    )
    wake = next_wake_time([dsa], now, day_boundary_tz="UTC")
    assert wake == now.replace(hour=16)  # scheduled_end, not the passed start


def test_next_wake_time_ignores_terminal_tasks():
    now = datetime(2026, 9, 12, 13, 0, tzinfo=UTC)
    done = Task.new("Old task", "UTC", scheduled_start=now.replace(hour=14))
    done.status = TaskStatus.COMPLETED
    wake = next_wake_time([done], now, day_boundary_tz="UTC")
    # nothing left but the day boundary, since the only task is terminal
    assert wake == datetime(2026, 9, 13, 0, 0, tzinfo=UTC)


def test_next_wake_time_considers_deadline_too():
    now = datetime(2026, 9, 12, 10, 0, tzinfo=UTC)
    t = Task.new(
        "Report", "UTC",
        scheduled_start=now.replace(hour=11),
        scheduled_end=now.replace(hour=12),
        deadline=now.replace(hour=11, minute=30),  # deadline inside the window
    )
    wake = next_wake_time([t], now, day_boundary_tz="UTC")
    assert wake == now.replace(hour=11)  # still the earliest of the three


def test_simulated_clock_rejects_moving_backwards():
    clock = SimulatedClock(datetime(2026, 1, 1, tzinfo=UTC))
    clock.set(datetime(2026, 1, 2, tzinfo=UTC))
    with pytest.raises(ValueError):
        clock.set(datetime(2026, 1, 1, tzinfo=UTC))


# --- property test: next_wake_time is always the minimum of the day
# boundary and every future task boundary, for any combination of offsets ---

@given(
    offsets_minutes=st.lists(
        st.integers(min_value=1, max_value=60 * 20),  # up to ~20h ahead
        min_size=0, max_size=5,
    )
)
def test_next_wake_time_is_always_the_true_minimum(offsets_minutes):
    now = datetime(2026, 9, 12, 0, 0, tzinfo=UTC)
    tasks = [
        Task.new(f"t{i}", "UTC", scheduled_start=now + timedelta(minutes=m))
        for i, m in enumerate(offsets_minutes)
    ]
    midnight = datetime(2026, 9, 13, 0, 0, tzinfo=UTC)
    expected = min([midnight] + [now + timedelta(minutes=m) for m in offsets_minutes])

    assert next_wake_time(tasks, now, day_boundary_tz="UTC") == expected
