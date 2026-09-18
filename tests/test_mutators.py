from datetime import datetime, timedelta, timezone

import pytest

from temporal_engine.mutators import (
    DEFAULT_MAX_LINEAGE_LENGTH,
    RescheduleCapExceeded,
    carry_forward_task,
    cancel_task,
    complete_task,
    lineage_length,
    reschedule_task,
)
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc


def test_complete_task_on_time_is_not_late():
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, tzinfo=UTC))
    event = complete_task(t, now=datetime(2026, 1, 1, tzinfo=UTC))
    assert t.status == TaskStatus.COMPLETED
    assert event.payload["late"] is False


def test_complete_task_after_window_ended_is_late():
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, tzinfo=UTC))
    t.status = TaskStatus.WINDOW_ENDED
    event = complete_task(t, now=datetime(2026, 1, 1, 18, tzinfo=UTC))
    assert t.status == TaskStatus.COMPLETED_LATE
    assert event.payload["late"] is True


def test_reschedule_task_creates_a_new_task_linked_by_carried_from():
    tasks = {}
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    tasks[t.id] = t
    now = datetime(2026, 1, 1, 16, tzinfo=UTC)

    new_task, events = reschedule_task(
        tasks, t.id,
        new_start=now, new_end=now + timedelta(hours=1),
        now=now,
    )

    assert t.status == TaskStatus.RESCHEDULED
    assert new_task.carried_from == t.id
    assert new_task.status == TaskStatus.SCHEDULED
    assert [e.event_type.value for e in events] == ["TASK_RESCHEDULED", "TASK_CREATED"]


def test_reschedule_rejects_zero_or_negative_duration():
    """This is the exact shape of bug that made the original prototype's
    reschedule loop runaway: a new window whose end isn't after its
    start. Rejected here, deterministically, regardless of who asked."""
    tasks = {}
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    tasks[t.id] = t
    now = datetime(2026, 1, 1, 16, tzinfo=UTC)

    with pytest.raises(ValueError):
        reschedule_task(tasks, t.id, new_start=now, new_end=now, now=now)


def test_reschedule_cap_is_enforced_after_max_lineage_length():
    """The regression test PLAN.md's Phase 2 specifically asks for: a
    stubbed sequence of reschedules that would run away if unchecked,
    proven to be rejected after the cap -- with zero LLM/API calls."""
    tasks = {}
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    tasks[t.id] = t

    current_id = t.id
    now = datetime(2026, 1, 1, 14, tzinfo=UTC)
    for _ in range(DEFAULT_MAX_LINEAGE_LENGTH):
        now += timedelta(hours=1)
        new_task, _ = reschedule_task(
            tasks, current_id, new_start=now, new_end=now + timedelta(hours=1), now=now,
        )
        current_id = new_task.id

    # one more reschedule attempt, on a lineage that has now hit the cap
    now += timedelta(hours=1)
    with pytest.raises(RescheduleCapExceeded):
        reschedule_task(
            tasks, current_id, new_start=now, new_end=now + timedelta(hours=1), now=now,
        )


def test_lineage_length_of_a_fresh_task_is_zero():
    tasks = {}
    t = Task.new("DSA", "UTC")
    tasks[t.id] = t
    assert lineage_length(tasks, t.id) == 0


def test_carry_forward_uses_its_own_event_type():
    tasks = {}
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    tasks[t.id] = t
    tomorrow = datetime(2026, 1, 2, 9, tzinfo=UTC)

    new_task, events = carry_forward_task(
        tasks, t.id, new_start=tomorrow, new_end=tomorrow + timedelta(hours=2), now=tomorrow,
    )

    assert t.status == TaskStatus.CARRIED_FORWARD
    assert [e.event_type.value for e in events] == ["TASK_CARRIED_FORWARD", "TASK_CREATED"]


def test_cancel_task_drop_mode():
    # CREATED can only reach SCHEDULED or CANCELLED directly (task.py's
    # transition table) -- DROPPED requires starting from SCHEDULED.
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, tzinfo=UTC))
    event = cancel_task(t, "drop", now=datetime(2026, 1, 1, tzinfo=UTC))
    assert t.status == TaskStatus.DROPPED
    assert event.event_type.value == "TASK_DROPPED"


def test_cancel_task_cancel_mode():
    # CANCELLED *is* reachable straight from CREATED, so this one didn't
    # need a scheduled_start -- included for contrast with the test above.
    t = Task.new("DSA", "UTC")
    event = cancel_task(t, "cancel", now=datetime(2026, 1, 1, tzinfo=UTC))
    assert t.status == TaskStatus.CANCELLED
    assert event.event_type.value == "TASK_CANCELLED"


def test_cancel_task_rejects_unknown_mode():
    t = Task.new("DSA", "UTC")
    with pytest.raises(ValueError):
        cancel_task(t, "delete", now=datetime(2026, 1, 1, tzinfo=UTC))


def test_completing_an_already_terminal_task_is_rejected():
    """A completed task can't be completed again -- apply_transition's
    'terminal states have no exits' invariant, exercised through the
    mutator layer this time instead of directly."""
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, tzinfo=UTC))
    complete_task(t, now=datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(ValueError):
        complete_task(t, now=datetime(2026, 1, 2, tzinfo=UTC))
