from datetime import datetime, timedelta, timezone

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.providers import StubHeuristicLLM, TemporalContext
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc


def test_stub_reschedules_when_slack_exists_before_the_next_task():
    now = datetime(2026, 9, 12, 16, 0, tzinfo=UTC)
    dsa = Task.new("DSA", "UTC", scheduled_start=now.replace(hour=14), scheduled_end=now)
    dsa.status = TaskStatus.WINDOW_ENDED
    project = Task.new("Project", "UTC", scheduled_start=now.replace(hour=18))  # 2h of slack

    ctx = TemporalContext(
        now=now,
        events=[TemporalEvent(EventType.TASK_WINDOW_ENDED, now, now, dsa.id)],
        tasks=[dsa, project],
    )

    actions = StubHeuristicLLM().decide(ctx)

    assert len(actions) == 1
    assert actions[0].action == "reschedule_task"
    assert actions[0].task_id == dsa.id
    # duration preserved (2h original), capped by slack -- and specifically
    # NOT zero, which was the exact bug in the original prototype
    assert actions[0].args["new_end"] > actions[0].args["new_start"]
    assert actions[0].args["new_end"] - actions[0].args["new_start"] == timedelta(hours=2)


def test_stub_caps_the_rescheduled_slot_to_available_slack():
    now = datetime(2026, 9, 12, 16, 0, tzinfo=UTC)
    dsa = Task.new("DSA", "UTC", scheduled_start=now.replace(hour=14), scheduled_end=now)  # 2h task
    dsa.status = TaskStatus.WINDOW_ENDED
    project = Task.new("Project", "UTC", scheduled_start=now + timedelta(minutes=45))  # only 45m slack

    ctx = TemporalContext(
        now=now,
        events=[TemporalEvent(EventType.TASK_WINDOW_ENDED, now, now, dsa.id)],
        tasks=[dsa, project],
    )

    actions = StubHeuristicLLM().decide(ctx)

    assert actions[0].action == "reschedule_task"
    assert actions[0].args["new_end"] - actions[0].args["new_start"] == timedelta(minutes=45)


def test_stub_carries_forward_when_too_close_to_midnight():
    # 23:50 -- no other tasks today, but also less than 30 minutes until
    # the day boundary itself. This is the exact case the original
    # version of the heuristic got wrong: it only checked "is there
    # another task today," so an empty remaining_today list looked like
    # infinite slack even ten minutes before midnight.
    now = datetime(2026, 9, 12, 23, 50, tzinfo=UTC)
    dsa = Task.new("DSA", "UTC", scheduled_start=now.replace(hour=21), scheduled_end=now)
    dsa.status = TaskStatus.WINDOW_ENDED

    ctx = TemporalContext(
        now=now,
        events=[TemporalEvent(EventType.TASK_WINDOW_ENDED, now, now, dsa.id)],
        tasks=[dsa],
    )

    actions = StubHeuristicLLM().decide(ctx)

    assert actions[0].action == "carry_forward_task"
    assert actions[0].args["new_start"].date() == (now.date() + timedelta(days=1))


def test_stub_reschedules_late_in_the_day_if_enough_room_before_midnight():
    # 19:00, no other tasks today, but still 5 hours until midnight --
    # this must reschedule, not carry forward, now that the fix correctly
    # measures slack against the day boundary instead of treating "no
    # more tasks" as unconditionally meaning "carry forward."
    now = datetime(2026, 9, 12, 19, 0, tzinfo=UTC)
    dsa = Task.new("DSA", "UTC", scheduled_start=now.replace(hour=17), scheduled_end=now)
    dsa.status = TaskStatus.WINDOW_ENDED

    ctx = TemporalContext(
        now=now,
        events=[TemporalEvent(EventType.TASK_WINDOW_ENDED, now, now, dsa.id)],
        tasks=[dsa],
    )

    actions = StubHeuristicLLM().decide(ctx)

    assert actions[0].action == "reschedule_task"
    assert actions[0].args["new_end"] <= datetime(2026, 9, 13, 0, 0, tzinfo=UTC)


def test_stub_proposes_nothing_for_irrelevant_events():
    now = datetime(2026, 9, 12, tzinfo=UTC)
    ctx = TemporalContext(
        now=now,
        events=[TemporalEvent(EventType.NEW_DAY, now, now)],
        tasks=[],
    )
    assert StubHeuristicLLM().decide(ctx) == []
