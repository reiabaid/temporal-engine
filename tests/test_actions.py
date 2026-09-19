from datetime import datetime, timedelta, timezone

from temporal_engine.actions import ActionCall, apply_action
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc


def test_apply_action_applies_a_valid_complete_task_call():
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, tzinfo=UTC))
    tasks = {t.id: t}
    call = ActionCall(idempotency_key="k1", action="complete_task", task_id=t.id)

    events = apply_action(tasks, call, now=datetime(2026, 1, 1, tzinfo=UTC), seen_idempotency_keys=set())

    assert t.status == TaskStatus.COMPLETED
    assert events[0].event_type.value == "ACTION_PROPOSED"
    assert events[0].payload["outcome"] == "applied"
    assert events[1].event_type.value == "TASK_COMPLETED"


def test_apply_action_logs_and_rejects_an_invalid_call():
    """The decision log records rejections too -- what was *considered*
    matters as much as what happened, per SPEC.md section 4."""
    t = Task.new("DSA", "UTC")
    t.status = TaskStatus.COMPLETED  # already terminal
    tasks = {t.id: t}
    call = ActionCall(idempotency_key="k1", action="complete_task", task_id=t.id)

    events = apply_action(tasks, call, now=datetime(2026, 1, 1, tzinfo=UTC), seen_idempotency_keys=set())

    assert len(events) == 1  # no downstream events -- nothing was applied
    assert events[0].payload["outcome"] == "rejected"
    assert events[0].payload["rejection_reason"]  # non-empty explanation


def test_apply_action_rejects_unknown_action_type():
    t = Task.new("DSA", "UTC")
    tasks = {t.id: t}
    call = ActionCall(idempotency_key="k1", action="delete_everything", task_id=t.id)

    events = apply_action(tasks, call, now=datetime(2026, 1, 1, tzinfo=UTC), seen_idempotency_keys=set())

    assert events[0].payload["outcome"] == "rejected"


def test_apply_action_deduplicates_by_idempotency_key():
    """A retried call with the same idempotency_key must be a no-op, not
    a double-apply -- SPEC.md section 3's requirement, exercised here."""
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, tzinfo=UTC))
    tasks = {t.id: t}
    call = ActionCall(idempotency_key="same-key", action="complete_task", task_id=t.id)
    seen = set()
    now = datetime(2026, 1, 1, tzinfo=UTC)

    first = apply_action(tasks, call, now, seen)
    second = apply_action(tasks, call, now, seen)  # simulated retry

    assert first[0].payload["outcome"] == "applied"
    assert second[0].payload["outcome"] == "superseded"
    assert len(second) == 1  # no mutator ran a second time


def test_apply_action_reschedule_produces_decision_plus_mutator_events():
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    tasks = {t.id: t}
    now = datetime(2026, 1, 1, 16, tzinfo=UTC)
    call = ActionCall(
        idempotency_key="k1", action="reschedule_task", task_id=t.id,
        args={"new_start": now, "new_end": now + timedelta(hours=1)},
        reason="slack available before the next task",
    )

    events = apply_action(tasks, call, now, seen_idempotency_keys=set())

    assert [e.event_type.value for e in events] == [
        "ACTION_PROPOSED", "TASK_RESCHEDULED", "TASK_CREATED",
    ]
    assert events[0].payload["action_call"]["reason"] == "slack available before the next task"


def test_naive_datetime_from_a_model_is_rejected_with_a_readable_reason_not_a_crash():
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    tasks = {t.id: t}
    naive = datetime(2026, 1, 1, 16, 0)  # no tzinfo
    call = ActionCall(
        idempotency_key="k", action="reschedule_task", task_id=t.id,
        args={"new_start": naive, "new_end": naive + timedelta(hours=1)},
    )

    events = apply_action(tasks, call, now=datetime(2026, 1, 1, 16, tzinfo=UTC), seen_idempotency_keys=set())

    assert events[0].payload["outcome"] == "rejected"
    assert "UTC offset" in events[0].payload["rejection_reason"]
    assert t.status == TaskStatus.SCHEDULED  # untouched


def test_missing_or_unparseable_times_are_rejected_not_raised():
    t = Task.new("DSA", "UTC", scheduled_start=datetime(2026, 1, 1, 14, tzinfo=UTC))
    tasks = {t.id: t}
    now = datetime(2026, 1, 1, 16, tzinfo=UTC)

    for args in ({}, {"new_start": "sometime soon", "new_end": now + timedelta(hours=1)}):
        call = ActionCall(idempotency_key=str(args), action="reschedule_task", task_id=t.id, args=args)
        events = apply_action(tasks, call, now, seen_idempotency_keys=set())
        assert events[0].payload["outcome"] == "rejected"
        assert "ISO 8601" in events[0].payload["rejection_reason"]
