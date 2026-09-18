"""
Storage layer, per SPEC.md section 7: an append-only `events` table is the
single source of truth. Task state is never stored directly -- it's a
materialized view, rebuilt on demand by folding every event forward from
the beginning of the log (see `replay`). This is deliberate, not
incidental: it's what lets "what happened while I was away" and "recover
after a crash" fall out of the same mechanism instead of needing two
separate code paths.

No multi-writer lock yet -- that's a separate, later piece (SPEC.md
section 7's scheduler lock). This file only proves the log-and-replay
idea works at all.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Optional

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.task import Task, TaskStatus

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


def append_event(conn: sqlite3.Connection, event: TemporalEvent) -> None:
    """The only way anything ever enters the log. There is no update or
    delete counterpart -- correcting a mistake means appending a new event
    that supersedes the old one, never rewriting history."""
    conn.execute(
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
    conn.commit()


def _row_to_event(row: sqlite3.Row) -> TemporalEvent:
    return TemporalEvent(
        id=row["id"],
        schema_version=row["schema_version"],
        event_type=EventType(row["event_type"]),
        occurred_at=datetime.fromisoformat(row["occurred_at"]),
        recorded_at=datetime.fromisoformat(row["recorded_at"]),
        task_id=row["task_id"],
        payload=json.loads(row["payload"]),
    )


def all_events(conn: sqlite3.Connection) -> list[TemporalEvent]:
    """Every event, in the order they actually happened to be appended.
    Ordered by `seq` (an autoincrement column) -- deliberately not by
    `recorded_at`. Two events can share a timestamp (same millisecond);
    they can never share an insertion order, so `seq` is the only
    ordering guaranteed to be unambiguous."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM events ORDER BY seq ASC").fetchall()
    return [_row_to_event(r) for r in rows]


def replay(conn: sqlite3.Connection) -> dict[str, Task]:
    """Rebuild every task's current state from nothing but the event log.
    This is the whole idea of event sourcing made concrete: call this
    after a crash, after opening the app fresh, or just to answer "what's
    true right now" -- there is no other place task state lives."""
    tasks: dict[str, Task] = {}
    for event in all_events(conn):
        _apply(tasks, event)
    return tasks


def _apply(tasks: dict[str, Task], event: TemporalEvent) -> None:
    if event.event_type == EventType.TASK_CREATED:
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

    if event.event_type in (EventType.NEW_DAY, EventType.ACTION_PROPOSED):
        return  # these don't mutate a specific task's status

    task = tasks[event.task_id]
    if event.event_type == EventType.TASK_STARTED:
        task.apply_transition(TaskStatus.ACTIVE, at=event.occurred_at)
    elif event.event_type == EventType.TASK_WINDOW_ENDED:
        task.apply_transition(TaskStatus.WINDOW_ENDED, at=event.occurred_at)
    elif event.event_type == EventType.TASK_DEADLINE_BREACHED:
        task.apply_transition(TaskStatus.OVERDUE, at=event.occurred_at)
    elif event.event_type == EventType.TASK_COMPLETED:
        late = event.payload.get("late", False)
        task.apply_transition(
            TaskStatus.COMPLETED_LATE if late else TaskStatus.COMPLETED,
            at=event.occurred_at,
        )
    elif event.event_type == EventType.TASK_RESCHEDULED:
        task.apply_transition(TaskStatus.RESCHEDULED, at=event.occurred_at)
    elif event.event_type == EventType.TASK_CARRIED_FORWARD:
        task.apply_transition(TaskStatus.CARRIED_FORWARD, at=event.occurred_at)
    elif event.event_type == EventType.TASK_DROPPED:
        task.apply_transition(TaskStatus.DROPPED, at=event.occurred_at)
    elif event.event_type == EventType.TASK_CANCELLED:
        task.apply_transition(TaskStatus.CANCELLED, at=event.occurred_at)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(value) if value else None


def task_created_event(task: Task, at: datetime) -> TemporalEvent:
    """Builds the TASK_CREATED event for a freshly constructed Task,
    capturing every field `replay` needs to reconstruct it later. Call
    this once, right after `Task.new(...)`, and append it before any
    other event referencing that task's id."""
    return TemporalEvent(
        event_type=EventType.TASK_CREATED,
        occurred_at=at,
        recorded_at=at,
        task_id=task.id,
        payload={
            "title": task.title,
            "display_timezone": task.display_timezone,
            "scheduled_start": task.scheduled_start.isoformat() if task.scheduled_start else None,
            "scheduled_end": task.scheduled_end.isoformat() if task.scheduled_end else None,
            "deadline": task.deadline.isoformat() if task.deadline else None,
            "status": task.status.value,
            "recurrence": task.recurrence,
            "carried_from": task.carried_from,
        },
    )
