from datetime import datetime, timezone

import pytest
from hypothesis import given, strategies as st

from temporal_engine.task import Task, TaskStatus, TERMINAL_STATUSES, can_transition

UTC = timezone.utc


def test_new_task_without_schedule_starts_created():
    t = Task.new("Read a book", "Asia/Kolkata")
    assert t.status == TaskStatus.CREATED


def test_new_task_with_schedule_starts_scheduled():
    t = Task.new(
        "DSA",
        "Asia/Kolkata",
        scheduled_start=datetime(2026, 9, 12, 14, 0, tzinfo=UTC),
        scheduled_end=datetime(2026, 9, 12, 16, 0, tzinfo=UTC),
    )
    assert t.status == TaskStatus.SCHEDULED


def test_legal_transition_updates_status_and_timestamp():
    t = Task.new("DSA", "Asia/Kolkata", scheduled_start=datetime(2026, 9, 12, 14, tzinfo=UTC))
    now = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)
    t.apply_transition(TaskStatus.ACTIVE, at=now)
    assert t.status == TaskStatus.ACTIVE
    assert t.last_transition_at == now


def test_illegal_transition_raises():
    t = Task.new("DSA", "Asia/Kolkata")
    # CREATED can only go to SCHEDULED or CANCELLED -- not straight to ACTIVE.
    with pytest.raises(ValueError):
        t.apply_transition(TaskStatus.ACTIVE, at=datetime(2026, 9, 12, tzinfo=UTC))


@pytest.mark.parametrize("terminal_status", sorted(TERMINAL_STATUSES, key=lambda s: s.value))
def test_terminal_states_have_no_outgoing_transitions(terminal_status):
    for candidate in TaskStatus:
        assert not can_transition(terminal_status, candidate), (
            f"{terminal_status} should not be able to move to {candidate}"
        )


def test_every_non_terminal_state_has_at_least_one_exit():
    """Design invariant: a non-terminal state that can never transition
    anywhere would be a dead end a task could get permanently stuck in."""
    for status in TaskStatus:
        if status in TERMINAL_STATUSES:
            continue
        has_exit = any(can_transition(status, other) for other in TaskStatus)
        assert has_exit, f"{status} is a non-terminal dead end"


@given(
    from_status=st.sampled_from(list(TaskStatus)),
    to_status=st.sampled_from(list(TaskStatus)),
)
def test_can_transition_agrees_with_apply_transition(from_status, to_status):
    """Property: apply_transition succeeds exactly when can_transition says
    it should, for every possible pair of statuses -- not just the ones we
    thought to hand-pick."""
    t = Task.new("x", "UTC")
    t.status = from_status
    now = datetime(2026, 1, 1, tzinfo=UTC)

    if can_transition(from_status, to_status):
        t.apply_transition(to_status, at=now)
        assert t.status == to_status
    else:
        with pytest.raises(ValueError):
            t.apply_transition(to_status, at=now)
