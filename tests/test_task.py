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


# ---- validation at the boundary ----

from datetime import timedelta  # noqa: E402

from temporal_engine.task import validate_schedule  # noqa: E402

_A = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)


@pytest.mark.parametrize("start, end, deadline, message", [
    (datetime(2026, 9, 12, 14), None, None, "UTC offset"),                       # naive start
    (_A, datetime(2026, 9, 12, 15), None, "UTC offset"),                         # naive end
    (_A, _A + timedelta(hours=1), datetime(2026, 9, 12, 20), "UTC offset"),      # naive deadline
    ("tomorrow", None, None, "ISO 8601"),                                        # not a datetime
    (None, _A, None, "requires"),                                                # end without start
    (_A, _A, None, "must be after"),                                             # zero-length window
    (_A, _A - timedelta(hours=1), None, "must be after"),                        # backwards window
    (_A, None, _A, "before the deadline"),                                       # starts at its deadline
    (_A + timedelta(hours=2), None, _A, "before the deadline"),                  # starts after its deadline
])
def test_validate_schedule_rejects_bad_input_with_a_readable_reason(start, end, deadline, message):
    with pytest.raises(ValueError, match=message):
        validate_schedule(start, end, deadline)


@pytest.mark.parametrize("start, end, deadline", [
    (None, None, None),                                   # an inbox item
    (None, None, _A),                                     # deadline only
    (_A, None, None),                                     # start only
    (_A, _A + timedelta(hours=1), None),
    (_A, _A + timedelta(hours=1), _A + timedelta(hours=3)),
    (_A, _A + timedelta(hours=5), _A + timedelta(hours=3)),   # window running past the deadline is allowed
])
def test_validate_schedule_accepts_legitimate_shapes(start, end, deadline):
    validate_schedule(start, end, deadline)


def test_task_new_refuses_to_construct_an_invalid_task():
    with pytest.raises(ValueError, match="UTC offset"):
        Task.new("x", "UTC", scheduled_start=datetime(2026, 9, 12, 14))
    with pytest.raises(ValueError, match="timezone"):
        Task.new("x", "Mars/Olympus")
    with pytest.raises(ValueError, match="title"):
        Task.new("   ", "UTC")


def test_validate_schedule_names_fields_the_way_the_caller_does():
    with pytest.raises(ValueError, match="new_end"):
        validate_schedule(_A, _A, None, labels=("new_start", "new_end", "deadline"))


# ---- an unscheduled (CREATED) task is a first-class inbox item ----

@pytest.mark.parametrize("target", [
    TaskStatus.COMPLETED, TaskStatus.DROPPED, TaskStatus.CANCELLED,
    TaskStatus.OVERDUE, TaskStatus.RESCHEDULED,
])
def test_a_created_task_can_be_finished_dropped_or_moved(target):
    """It used to be stuck: CREATED could only become SCHEDULED or CANCELLED,
    so an unscheduled task could not even be completed."""
    assert can_transition(TaskStatus.CREATED, target)


def test_a_created_task_still_cannot_skip_to_a_window_state():
    assert not can_transition(TaskStatus.CREATED, TaskStatus.ACTIVE)
    assert not can_transition(TaskStatus.CREATED, TaskStatus.WINDOW_ENDED)
