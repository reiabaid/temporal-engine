"""
Storage layer: an append-only `events` table is the single source of
truth. Task state is never stored directly -- it is a materialized view,
derived by folding events forward (SPEC.md section 7).

Two ways to use it:

  * `replay(conn)` -- fold the whole log into a dict of tasks. Simple, used
    by tests and one-shot tools.
  * `Store` -- a long-lived handle for a running process. It keeps a `View`
    and *refreshes it incrementally* from the log (only rows after the last
    seq it saw), and performs every write inside one `BEGIN IMMEDIATE`
    transaction that first refreshes the view. Because the write lock is
    held from refresh to commit, a mutation is always validated against
    exactly the state it will be applied to. That is what makes two
    processes sharing one database safe: a stale process cannot complete a
    task another process has already rescheduled, because by the time it
    holds the write lock it has already seen that reschedule.

A bad event (an illegal transition, an event for an unknown task, an event
type from a newer version) must never make the whole database unopenable,
so the Store *quarantines* it -- skips it, records it, logs it -- rather
than raising. `replay()` without a quarantine list stays strict, which is
what tests want.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Iterable, Iterator, Optional
from zoneinfo import ZoneInfo

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.task import Task, TaskStatus

log = logging.getLogger("temporal_engine")

_CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    task_id TEXT,
    payload TEXT NOT NULL
)
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_EVENTS_TABLE)
    conn.commit()


# ---------------------------------------------------------------- writing

def append_events(conn: sqlite3.Connection, events: list[TemporalEvent]) -> Optional[int]:
    """Append events atomically: all of them or none. Returns the seq of
    the last event written (and sets `.seq` on each). If the connection is
    already inside a transaction the caller owns commit/rollback;
    otherwise this wraps the batch in its own.

    Atomicity matters because one decision is several events (e.g.
    TASK_RESCHEDULED then TASK_CREATED for its replacement). Separate
    commits would let a crash between them lose the task while the decision
    log said "applied".
    """
    if not events:
        return None
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN")
    try:
        last = None
        for event in events:
            cursor = conn.execute(
                "INSERT INTO events "
                "(id, schema_version, event_type, occurred_at, recorded_at, task_id, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event.id,
                    event.schema_version,
                    event.event_type.value,
                    event.occurred_at.isoformat(),
                    event.recorded_at.isoformat(),
                    event.task_id,
                    json.dumps(event.payload),
                ),
            )
            event.seq = last = cursor.lastrowid
        if owns_transaction:
            conn.execute("COMMIT")
        return last
    except BaseException:
        if owns_transaction:
            conn.execute("ROLLBACK")
        raise


def append_event(conn: sqlite3.Connection, event: TemporalEvent) -> None:
    """The only way a single event enters the log. There is no update or
    delete counterpart -- correcting a mistake means appending a new event
    that supersedes the old one, never rewriting history."""
    append_events(conn, [event])


# ---------------------------------------------------------------- reading

def _row_to_event(row: sqlite3.Row) -> TemporalEvent:
    return TemporalEvent(
        id=row["id"],
        schema_version=row["schema_version"],
        event_type=EventType(row["event_type"]),
        occurred_at=datetime.fromisoformat(row["occurred_at"]),
        recorded_at=datetime.fromisoformat(row["recorded_at"]),
        task_id=row["task_id"],
        payload=json.loads(row["payload"]),
        seq=row["seq"],
    )


def _select(conn: sqlite3.Connection, sql: str, params: Iterable = ()) -> list[sqlite3.Row]:
    cursor = conn.execute(sql, tuple(params))
    cursor.row_factory = sqlite3.Row  # per-cursor: never mutate the shared connection
    return cursor.fetchall()


def all_events(conn: sqlite3.Connection) -> list[TemporalEvent]:
    """Every event, in append order. Ordered by `seq`, deliberately not by
    `recorded_at`: two events can share a timestamp, never an insertion
    order."""
    return [_row_to_event(r) for r in _select(conn, "SELECT * FROM events ORDER BY seq ASC")]


def query_events(
    conn: sqlite3.Connection,
    since_seq: Optional[int] = None,
    since_time: Optional[datetime] = None,
    task_ids: Optional[list[str]] = None,
    limit: int = 20,
) -> tuple[list[TemporalEvent], bool]:
    """Events in ascending order plus whether more exist beyond `limit`.

    With no filters this returns the most recent `limit` events. With
    `since_seq`/`since_time` it returns the oldest matching events first,
    so a caller paging forward with the last seq it saw never skips any.
    """
    where, params = [], []
    if since_seq is not None:
        where.append("seq > ?")
        params.append(since_seq)
    if since_time is not None:
        where.append("recorded_at >= ?")
        params.append(since_time.isoformat())
    if task_ids:
        where.append(f"task_id IN ({','.join('?' * len(task_ids))})")
        params.extend(task_ids)
    clause = f"WHERE {' AND '.join(where)}" if where else ""

    if since_seq is None and since_time is None:
        rows = _select(conn, f"SELECT * FROM events {clause} ORDER BY seq DESC LIMIT ?", params + [limit + 1])
        rows.reverse()
        truncated = len(rows) > limit
        return [_row_to_event(r) for r in rows[-limit:]], truncated

    rows = _select(conn, f"SELECT * FROM events {clause} ORDER BY seq ASC LIMIT ?", params + [limit + 1])
    return [_row_to_event(r) for r in rows[:limit]], len(rows) > limit


# ------------------------------------------------------------------ view

@dataclass
class View:
    """Everything derived from the log. Never written directly."""
    tasks: dict[str, Task] = field(default_factory=dict)
    # Idempotency keys of actions that were APPLIED (rejected ones may be
    # retried -- state may have changed since). Durable because it is
    # rebuilt from the log, so it survives restarts and is shared between
    # processes.
    seen_keys: set[str] = field(default_factory=set)
    # Actions held for human confirmation: key -> the logged action_call.
    pending: dict[str, dict] = field(default_factory=dict)
    # key -> id of the task that key created, so a retried create can be
    # answered with the original task instead of a duplicate.
    created_by_key: dict[str, str] = field(default_factory=dict)
    last_seq: int = 0
    last_new_day: Optional[date] = None
    first_event_at: Optional[datetime] = None
    quarantined: list[tuple[int, str]] = field(default_factory=list)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(value) if value else None


def _apply_to_tasks(tasks: dict[str, Task], event: TemporalEvent) -> None:
    """Fold one event into the task dict. Transition times use the event's
    recorded_at -- the moment the engine noticed -- which is exactly what
    the live code path used, so a replayed task and the live task it
    reconstructs agree on last_transition_at."""
    kind = event.event_type

    if kind == EventType.TASK_CREATED:
        p = event.payload
        tasks[event.task_id] = Task(
            id=event.task_id,
            title=p["title"],
            display_timezone=p["display_timezone"],
            scheduled_start=_parse_dt(p.get("scheduled_start")),
            scheduled_end=_parse_dt(p.get("scheduled_end")),
            deadline=_parse_dt(p.get("deadline")),
            status=TaskStatus(p["status"]),
            recurrence=p.get("recurrence"),
            carried_from=p.get("carried_from"),
        )
        return

    if kind in (EventType.NEW_DAY, EventType.ACTION_PROPOSED):
        return  # not about one task's status

    task = tasks[event.task_id]
    at = event.recorded_at
    if kind == EventType.TASK_STARTED:
        task.apply_transition(TaskStatus.ACTIVE, at=at)
    elif kind == EventType.TASK_WINDOW_ENDED:
        task.apply_transition(TaskStatus.WINDOW_ENDED, at=at)
    elif kind == EventType.TASK_DEADLINE_BREACHED:
        task.apply_transition(TaskStatus.OVERDUE, at=at)
    elif kind == EventType.TASK_COMPLETED:
        late = event.payload.get("late", False)
        task.apply_transition(TaskStatus.COMPLETED_LATE if late else TaskStatus.COMPLETED, at=at)
    elif kind == EventType.TASK_RESCHEDULED:
        task.apply_transition(TaskStatus.RESCHEDULED, at=at)
    elif kind == EventType.TASK_CARRIED_FORWARD:
        task.apply_transition(TaskStatus.CARRIED_FORWARD, at=at)
    elif kind == EventType.TASK_DROPPED:
        task.apply_transition(TaskStatus.DROPPED, at=at)
    elif kind == EventType.TASK_CANCELLED:
        task.apply_transition(TaskStatus.CANCELLED, at=at)
    elif kind == EventType.TASK_WORK_STARTED:
        task.actual_start = event.occurred_at


def _index(view: View, event: TemporalEvent) -> None:
    """Update the derived indexes (not the tasks). Also called for a
    process's own just-appended events, whose task effects were already
    applied live by the mutator."""
    if view.first_event_at is None:
        view.first_event_at = event.recorded_at

    if event.event_type == EventType.NEW_DAY:
        view.last_new_day = date.fromisoformat(event.payload["new_date"])

    elif event.event_type == EventType.TASK_CREATED:
        key = event.payload.get("idempotency_key")
        if key:
            view.created_by_key[key] = event.task_id

    elif event.event_type == EventType.ACTION_PROPOSED:
        call = event.payload.get("action_call", {})
        key = call.get("idempotency_key")
        outcome = event.payload.get("outcome")
        if key is None:
            return
        if outcome == "pending_confirmation":
            view.pending[key] = call
        else:
            view.pending.pop(key, None)
            if outcome == "applied":
                view.seen_keys.add(key)


def apply_event(view: View, event: TemporalEvent) -> None:
    _apply_to_tasks(view.tasks, event)
    _index(view, event)


def refresh_view(conn: sqlite3.Connection, view: View, strict: bool = False) -> None:
    """Bring `view` up to date by applying only events after view.last_seq.
    With strict=False a bad event is quarantined instead of raised."""
    for row in _select(conn, "SELECT * FROM events WHERE seq > ? ORDER BY seq ASC", (view.last_seq,)):
        seq = row["seq"]
        try:
            apply_event(view, _row_to_event(row))
        except (ValueError, KeyError, TypeError) as exc:
            if strict:
                raise
            reason = f"{type(exc).__name__}: {exc}"
            view.quarantined.append((seq, reason))
            log.warning("quarantined event seq=%s (%s)", seq, reason)
        view.last_seq = seq


def replay(conn: sqlite3.Connection, quarantine: Optional[list] = None) -> dict[str, Task]:
    """Rebuild every task's current state from nothing but the event log.
    Strict by default (raises on a bad event); pass a list to instead skip
    bad events and receive (seq, reason) entries in it."""
    view = View()
    refresh_view(conn, view, strict=quarantine is None)
    if quarantine is not None:
        quarantine.extend(view.quarantined)
    return view.tasks


class Store:
    """A running process's handle on the database. Single-threaded by
    design (the connection is not shared across threads)."""

    def __init__(self, path: str, busy_timeout_seconds: float = 5.0):
        # isolation_level=None: we issue BEGIN/COMMIT ourselves, so what is
        # inside a transaction is explicit rather than implied by the driver.
        self.conn = sqlite3.connect(str(path), isolation_level=None, timeout=busy_timeout_seconds)
        self.conn.execute("PRAGMA journal_mode=WAL")
        init_db(self.conn)
        self.view = View()
        self.refresh()

    def refresh(self) -> View:
        refresh_view(self.conn, self.view)
        return self.view

    def reload(self) -> View:
        """Discard the view and rebuild it from the log. Used after a
        failed transaction, since the mutator may have changed the view
        before the write failed."""
        self.view = View()
        return self.refresh()

    @contextmanager
    def transaction(self) -> Iterator[View]:
        """Take the write lock, catch up to the log, then let the caller
        validate and mutate against exactly the state that will be
        committed. Any failure rolls back and rebuilds the view, so memory
        can never end up ahead of the log."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            refresh_view(self.conn, self.view)
            yield self.view
            self.conn.execute("COMMIT")
        except BaseException:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self.reload()
            raise

    def append(self, events: list[TemporalEvent]) -> None:
        """Persist events. Must be called inside `transaction()`; the
        mutation that produced them has already been applied to the view,
        so only the indexes are updated here."""
        if not events:
            return
        if not self.conn.in_transaction:
            raise RuntimeError("Store.append must be called inside Store.transaction()")
        last = append_events(self.conn, events)
        for event in events:
            _index(self.view, event)
        self.view.last_seq = last

    def day_seed(self, day_boundary_tz: str) -> Optional[date]:
        """The date a new scheduler should assume was last announced: the
        newest NEW_DAY's date, else the day the log began. Derived from the
        log so that whichever process becomes the scheduler agrees with the
        last one about which days have already been reported."""
        if self.view.last_new_day is not None:
            return self.view.last_new_day
        if self.view.first_event_at is not None:
            return self.view.first_event_at.astimezone(ZoneInfo(day_boundary_tz)).date()
        return None

    def events(self, **kwargs) -> tuple[list[TemporalEvent], bool]:
        return query_events(self.conn, **kwargs)

    def close(self) -> None:
        self.conn.close()


def task_created_event(task: Task, at: datetime, idempotency_key: Optional[str] = None) -> TemporalEvent:
    """Builds the TASK_CREATED event for a freshly constructed Task,
    capturing every field replay needs to reconstruct it later."""
    payload = {
        "title": task.title,
        "display_timezone": task.display_timezone,
        "scheduled_start": task.scheduled_start.isoformat() if task.scheduled_start else None,
        "scheduled_end": task.scheduled_end.isoformat() if task.scheduled_end else None,
        "deadline": task.deadline.isoformat() if task.deadline else None,
        "status": task.status.value,
        "recurrence": task.recurrence,
        "carried_from": task.carried_from,
    }
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key
    return TemporalEvent(
        event_type=EventType.TASK_CREATED,
        occurred_at=at,
        recorded_at=at,
        task_id=task.id,
        payload=payload,
    )
