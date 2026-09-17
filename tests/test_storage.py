import sqlite3
from datetime import datetime, timezone

from hypothesis import given, strategies as st

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.storage import append_event, init_db, replay, task_created_event
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    return conn


def test_replay_reconstructs_a_freshly_created_task():
    conn = _fresh_db()
    t = Task.new("DSA", "Asia/Kolkata")
    append_event(conn, task_created_event(t, at=datetime(2026, 1, 1, tzinfo=UTC)))

    tasks = replay(conn)

    assert tasks[t.id].title == "DSA"
    assert tasks[t.id].status == TaskStatus.CREATED


def test_replay_applies_a_sequence_of_events_in_order():
    conn = _fresh_db()
    start = datetime(2026, 9, 12, 14, tzinfo=UTC)
    end = start.replace(hour=16)
    t = Task.new("DSA", "Asia/Kolkata", scheduled_start=start, scheduled_end=end)

    append_event(conn, task_created_event(t, at=start))
    append_event(conn, TemporalEvent(EventType.TASK_STARTED, start, start, t.id))
    append_event(conn, TemporalEvent(EventType.TASK_WINDOW_ENDED, end, end, t.id))
    append_event(conn, TemporalEvent(
        EventType.TASK_COMPLETED, end.replace(hour=17), end.replace(hour=17), t.id,
        payload={"late": True},
    ))

    tasks = replay(conn)

    assert tasks[t.id].status == TaskStatus.COMPLETED_LATE


def test_replaying_the_same_log_twice_gives_identical_state():
    """This is the exact invariant PLAN.md's Phase 1 exit criterion asks
    for: replay must be a pure function of the log, not something that
    drifts if you happen to call it more than once."""
    conn = _fresh_db()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    t = Task.new("DSA", "Asia/Kolkata", scheduled_start=now)
    append_event(conn, task_created_event(t, at=now))
    append_event(conn, TemporalEvent(EventType.TASK_STARTED, now, now, t.id))

    first = replay(conn)
    second = replay(conn)

    assert first[t.id].status == second[t.id].status == TaskStatus.ACTIVE
    assert first[t.id].title == second[t.id].title


# --- property test: replay is idempotent across every legal path through
# the state machine that this file currently knows how to represent as
# events (RESCHEDULED/CARRIED_FORWARD/DROPPED/CANCELLED have no event
# mapping yet -- those arrive with the Phase 2 mutators). ---

_EVENT_FOR_STATUS = {
    TaskStatus.ACTIVE: EventType.TASK_STARTED,
    TaskStatus.WINDOW_ENDED: EventType.TASK_WINDOW_ENDED,
    TaskStatus.OVERDUE: EventType.TASK_DEADLINE_BREACHED,
}

_VALID_WALKS = [
    [TaskStatus.SCHEDULED],
    [TaskStatus.SCHEDULED, TaskStatus.ACTIVE],
    [TaskStatus.SCHEDULED, TaskStatus.ACTIVE, TaskStatus.WINDOW_ENDED],
    [TaskStatus.SCHEDULED, TaskStatus.ACTIVE, TaskStatus.WINDOW_ENDED, TaskStatus.OVERDUE],
    [TaskStatus.SCHEDULED, TaskStatus.COMPLETED],
    [TaskStatus.SCHEDULED, TaskStatus.OVERDUE],
    [TaskStatus.SCHEDULED, TaskStatus.ACTIVE, TaskStatus.WINDOW_ENDED, TaskStatus.COMPLETED_LATE],
]


def _events_for_walk(task_id: str, walk: list[TaskStatus], now: datetime) -> list[TemporalEvent]:
    events = []
    for status in walk[1:]:
        if status in _EVENT_FOR_STATUS:
            events.append(TemporalEvent(_EVENT_FOR_STATUS[status], now, now, task_id))
        else:  # COMPLETED / COMPLETED_LATE
            late = status == TaskStatus.COMPLETED_LATE
            events.append(TemporalEvent(EventType.TASK_COMPLETED, now, now, task_id, {"late": late}))
    return events


@given(st.sampled_from(_VALID_WALKS))
def test_replay_is_idempotent_across_every_valid_walk(walk):
    conn = _fresh_db()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    t = Task.new("x", "UTC", scheduled_start=now)
    append_event(conn, task_created_event(t, at=now))
    for event in _events_for_walk(t.id, walk, now):
        append_event(conn, event)

    first = replay(conn)
    second = replay(conn)

    assert first[t.id].status == second[t.id].status == walk[-1]
