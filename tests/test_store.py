import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.storage import (
    Store, append_event, append_events, init_db, refresh_view, replay, task_created_event, View,
)
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc
T0 = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)


def _created(title="T", start_hour=14) -> tuple[Task, TemporalEvent]:
    task = Task.new(title, "UTC", scheduled_start=T0.replace(hour=start_hour),
                    scheduled_end=T0.replace(hour=start_hour + 1))
    return task, task_created_event(task, at=T0)


@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "s.db")


# ---------------------------------------------------------------- atomicity

def test_a_failing_batch_writes_nothing():
    """One decision is several events; a failure partway must not leave the
    first ones behind (a rescheduled task with no replacement)."""
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    task, created = _created()
    duplicate_id = TemporalEvent(EventType.NEW_DAY, T0, T0, id=created.id)  # violates UNIQUE(id)

    with pytest.raises(sqlite3.IntegrityError):
        append_events(conn, [created, duplicate_id])

    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


def test_append_assigns_increasing_seq_numbers():
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    a = TemporalEvent(EventType.NEW_DAY, T0, T0, payload={"previous_date": "2026-09-11", "new_date": "2026-09-12"})
    b = TemporalEvent(EventType.NEW_DAY, T0, T0, payload={"previous_date": "2026-09-12", "new_date": "2026-09-13"})
    append_events(conn, [a, b])
    assert (a.seq, b.seq) == (1, 2)


# ------------------------------------------------------ views and refreshing

def test_a_second_store_sees_the_first_stores_writes_after_refresh(db):
    a, b = Store(db), Store(db)
    task, created = _created()
    with a.transaction():
        a.view.tasks[task.id] = task  # the mutation the caller would have made
        a.append([created])

    assert task.id not in b.view.tasks       # b has not looked yet
    assert task.id in b.refresh().tasks      # now it has
    a.close(); b.close()


def test_refresh_is_incremental_and_never_reapplies_events(db):
    """Re-applying an event to a task that already reflects it would raise an
    illegal transition; refreshing repeatedly must be a no-op."""
    store = Store(db)
    task, created = _created()
    with store.transaction():
        store.view.tasks[task.id] = task
        store.append([created])
    for _ in range(3):
        store.refresh()
    assert store.view.tasks[task.id].status == TaskStatus.SCHEDULED
    assert store.view.last_seq == 1
    store.close()


def test_a_failed_transaction_rolls_back_the_log_and_rebuilds_the_view(db):
    store = Store(db)
    task, created = _created()
    with store.transaction():
        store.view.tasks[task.id] = task
        store.append([created])

    with pytest.raises(RuntimeError):
        with store.transaction() as view:
            view.tasks[task.id].status = TaskStatus.COMPLETED     # mutate the live view...
            store.append([TemporalEvent(EventType.TASK_COMPLETED, T0, T0, task.id, {"late": False})])
            raise RuntimeError("something failed before commit")

    assert store.view.tasks[task.id].status == TaskStatus.SCHEDULED   # memory did not stay ahead of the log
    assert store.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    store.close()


def test_append_outside_a_transaction_is_refused(db):
    store = Store(db)
    with pytest.raises(RuntimeError):
        store.append([TemporalEvent(EventType.NEW_DAY, T0, T0, payload={"new_date": "2026-09-12"})])
    store.close()


# --------------------------------------------------------------- quarantine

def _write_illegal_log(db):
    """created -> rescheduled -> completed: the last is illegal (terminal)."""
    conn = sqlite3.connect(db)
    init_db(conn)
    task, created = _created()
    append_event(conn, created)
    append_event(conn, TemporalEvent(EventType.TASK_RESCHEDULED, T0, T0, task.id))
    append_event(conn, TemporalEvent(EventType.TASK_COMPLETED, T0, T0, task.id, {"late": False}))
    conn.close()
    return task


def test_strict_replay_raises_on_an_illegal_event(db):
    _write_illegal_log(db)
    conn = sqlite3.connect(db)
    with pytest.raises(ValueError, match="illegal transition"):
        replay(conn)


def test_a_store_quarantines_an_illegal_event_instead_of_refusing_to_open(db):
    """The regression at the heart of the multi-client bug: one bad event
    used to make the whole database unopenable."""
    task = _write_illegal_log(db)
    store = Store(db)                                   # must not raise
    assert store.view.tasks[task.id].status == TaskStatus.RESCHEDULED   # state from the legal prefix
    assert [seq for seq, _ in store.view.quarantined] == [3]
    assert "illegal transition" in store.view.quarantined[0][1]
    store.close()


def test_replay_can_collect_quarantined_events_instead_of_raising(db):
    _write_illegal_log(db)
    quarantine = []
    replay(sqlite3.connect(db), quarantine=quarantine)
    assert len(quarantine) == 1


def test_an_event_type_from_a_newer_version_is_quarantined_not_fatal(db):
    conn = sqlite3.connect(db)
    init_db(conn)
    conn.execute(
        "INSERT INTO events (id, schema_version, event_type, occurred_at, recorded_at, task_id, payload) "
        "VALUES ('x', 99, 'SOMETHING_FROM_THE_FUTURE', ?, ?, NULL, '{}')", (T0.isoformat(), T0.isoformat()),
    )
    conn.commit(); conn.close()

    store = Store(db)
    assert len(store.view.quarantined) == 1
    store.close()


def test_an_event_for_an_unknown_task_is_quarantined(db):
    conn = sqlite3.connect(db)
    init_db(conn)
    append_event(conn, TemporalEvent(EventType.TASK_STARTED, T0, T0, "no-such-task"))
    conn.close()
    store = Store(db)
    assert len(store.view.quarantined) == 1
    store.close()


# ----------------------------------------------------------------- queries

def _log_of_numbered_events(db, n):
    store = Store(db)
    with store.transaction():
        store.append([
            TemporalEvent(EventType.NEW_DAY, T0 + timedelta(days=i), T0 + timedelta(days=i),
                          payload={"previous_date": "x", "new_date": (T0 + timedelta(days=i)).date().isoformat()})
            for i in range(n)
        ])
    return store


def test_query_without_filters_returns_the_most_recent_events_oldest_first(db):
    store = _log_of_numbered_events(db, 10)
    events, truncated = store.events(limit=3)
    assert [e.seq for e in events] == [8, 9, 10]
    assert truncated is True
    store.close()


def test_query_since_a_cursor_pages_forward_without_skipping(db):
    store = _log_of_numbered_events(db, 10)
    first, more = store.events(since_seq=0, limit=4)
    second, more2 = store.events(since_seq=first[-1].seq, limit=4)
    third, more3 = store.events(since_seq=second[-1].seq, limit=4)
    assert [e.seq for e in first + second + third] == list(range(1, 11))
    assert (more, more2, more3) == (True, True, False)
    store.close()


def test_query_since_a_time_and_by_task(db):
    store = Store(db)
    a, ca = _created("A"); b, cb = _created("B")
    cb.recorded_at = T0 + timedelta(days=2)
    with store.transaction():
        store.view.tasks.update({a.id: a, b.id: b})
        store.append([ca, cb])
    late, _ = store.events(since_time=T0 + timedelta(days=1))
    assert [e.task_id for e in late] == [b.id]
    only_a, _ = store.events(task_ids=[a.id])
    assert [e.task_id for e in only_a] == [a.id]
    store.close()


# ------------------------------------------------------------- derived data

def test_day_seed_prefers_the_last_announced_day_over_the_first_event(db):
    store = Store(db)
    assert store.day_seed("UTC") is None                       # empty log
    task, created = _created()
    with store.transaction():
        store.view.tasks[task.id] = task
        store.append([created])
    assert store.day_seed("UTC") == T0.date()                  # falls back to when the log began
    with store.transaction():
        store.append([TemporalEvent(EventType.NEW_DAY, T0, T0, payload={"previous_date": "2026-09-12", "new_date": "2026-09-15"})])
    assert store.day_seed("UTC").isoformat() == "2026-09-15"   # then to the last announced day
    store.close()


def test_action_indexes_are_rebuilt_from_the_log_by_a_fresh_store(db):
    """Idempotency and pending confirmations must survive a restart, so they
    are derived from the log rather than held only in memory."""
    store = Store(db)
    def decision(key, outcome):
        return TemporalEvent(EventType.ACTION_PROPOSED, T0, T0, payload={
            "action_call": {"idempotency_key": key, "action": "cancel_task", "task_id": None, "args": {}},
            "outcome": outcome, "rejection_reason": None,
        })
    with store.transaction():
        store.append([decision("done", "applied"), decision("held", "pending_confirmation"),
                      decision("failed", "rejected"), decision("was-held", "pending_confirmation"),
                      decision("was-held", "applied")])
    store.close()

    fresh = Store(db)
    assert fresh.view.seen_keys == {"done", "was-held"}     # rejected keys stay retryable
    assert set(fresh.view.pending) == {"held"}              # resolved holds are cleared
    fresh.close()
