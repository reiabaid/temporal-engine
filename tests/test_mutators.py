from datetime import datetime, timedelta, timezone

import pytest

from temporal_engine.mutators import (
    DEFAULT_MAX_MOVES,
    RescheduleCapExceeded,
    carry_forward_task,
    cancel_task,
    complete_task,
    create_task,
    lineage_length,
    reschedule_task,
    start_task,
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
    for _ in range(DEFAULT_MAX_MOVES):
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


# ---- rate-limit semantics of the cap ----

def test_moves_spread_beyond_the_window_are_not_capped():
    """A chore carried forward one day at a time must never get stuck; only a
    burst of moves (the runaway shape) is stopped."""
    tasks = {}
    t = Task.new("laundry", "UTC", scheduled_start=datetime(2026, 1, 1, 9, tzinfo=UTC))
    tasks[t.id] = t
    current_id = t.id
    now = datetime(2026, 1, 1, 9, tzinfo=UTC)
    for day in range(1, 8):  # seven carry-forwards, one per day
        now = datetime(2026, 1, 1, 9, tzinfo=UTC) + timedelta(days=day)
        new_task, _ = carry_forward_task(
            tasks, current_id, new_start=now, new_end=now + timedelta(hours=1), now=now,
        )
        current_id = new_task.id
    assert lineage_length(tasks, current_id) == 7


# ---- validation of times ----

def test_reschedule_past_the_tasks_own_deadline_is_rejected():
    tasks = {}
    now = datetime(2026, 1, 1, 9, tzinfo=UTC)
    t = Task.new("report", "UTC", scheduled_start=now + timedelta(hours=1),
                 scheduled_end=now + timedelta(hours=2), deadline=now + timedelta(hours=5))
    tasks[t.id] = t
    with pytest.raises(ValueError, match="before the deadline"):
        reschedule_task(tasks, t.id, now + timedelta(hours=10), now + timedelta(hours=11), now)
    assert t.status == TaskStatus.SCHEDULED  # nothing was mutated


def test_extending_the_deadline_explicitly_allows_the_move():
    tasks = {}
    now = datetime(2026, 1, 1, 9, tzinfo=UTC)
    t = Task.new("report", "UTC", scheduled_start=now + timedelta(hours=1),
                 scheduled_end=now + timedelta(hours=2), deadline=now + timedelta(hours=5))
    tasks[t.id] = t
    new_task, _ = reschedule_task(
        tasks, t.id, now + timedelta(hours=10), now + timedelta(hours=11), now,
        new_deadline=now + timedelta(hours=20),
    )
    assert new_task.deadline == now + timedelta(hours=20)


# ---- create / start ----

def test_create_task_registers_the_task_and_emits_a_created_event_carrying_the_key():
    tasks = {}
    now = datetime(2026, 1, 1, tzinfo=UTC)
    task, events = create_task(tasks, now, "DSA", "UTC", idempotency_key="k1")
    assert tasks[task.id] is task
    assert events[0].event_type.value == "TASK_CREATED"
    assert events[0].payload["idempotency_key"] == "k1"


def test_create_task_rejects_invalid_input_without_registering_anything():
    tasks = {}
    now = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        create_task(tasks, now, "x", "UTC", scheduled_start=datetime(2026, 1, 1, 9))  # naive
    assert tasks == {}


def test_start_task_records_actual_start_once_and_does_not_change_status():
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    now = datetime(2026, 1, 1, 14, 5, tzinfo=UTC)
    event = start_task(t, now)
    assert t.actual_start == now
    assert t.status == TaskStatus.SCHEDULED  # ACTIVE stays clock-derived
    assert event.event_type.value == "TASK_WORK_STARTED"
    with pytest.raises(ValueError, match="already started"):
        start_task(t, now + timedelta(minutes=1))


def test_start_task_refuses_a_finished_task():
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    complete_task(t, datetime(2026, 1, 1, 15, tzinfo=UTC))
    with pytest.raises(ValueError, match="finished"):
        start_task(t, datetime(2026, 1, 1, 16, tzinfo=UTC))


def test_unknown_task_id_is_a_readable_value_error():
    with pytest.raises(ValueError, match="unknown task_id"):
        reschedule_task({}, "nope", datetime(2026, 1, 1, tzinfo=UTC),
                        datetime(2026, 1, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC))
