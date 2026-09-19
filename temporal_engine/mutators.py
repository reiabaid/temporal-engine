"""
Mutators: the only way task state may change as a *decision* (as opposed
to tick()'s deterministic clock-driven transitions). Every mutator here
routes through Task.apply_transition, so illegal state changes are
rejected the same way regardless of who's asking.

This file also enforces the invariant that must never depend on model
behavior: a cap on how many times a single task lineage may be
rescheduled or carried forward. This is the fix for the exact bug hit in
the very first prototype of this project -- a reschedule heuristic that
set a task's new window to zero duration, which immediately re-triggered
WINDOW_ENDED, which rescheduled again, forever. The cap lives here, in
the deterministic layer, specifically so no LLM's behavior (buggy,
adversarial, or just a bad decision) can reproduce that failure mode.

Nothing here imports an LLM or knows one exists -- these functions are
called *by* whatever executes an ActionCall (see actions.py).
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.storage import task_created_event
from temporal_engine.task import Task, TaskStatus

DEFAULT_MAX_LINEAGE_LENGTH = 3


class RescheduleCapExceeded(RuntimeError):
    """Raised when a task lineage has already been rescheduled/carried
    forward the maximum allowed number of times."""


def lineage_length(tasks: dict[str, Task], task_id: str) -> int:
    """How many times has this task's lineage already been rescheduled or
    carried forward? Walks backward through `carried_from` links. A brand
    new task (never superseded anything) has length 0."""
    count = 0
    current = tasks.get(task_id)
    while current is not None and current.carried_from is not None:
        count += 1
        current = tasks.get(current.carried_from)
    return count


def complete_task(task: Task, now: datetime) -> TemporalEvent:
    late = task.status in (TaskStatus.WINDOW_ENDED, TaskStatus.OVERDUE)
    target = TaskStatus.COMPLETED_LATE if late else TaskStatus.COMPLETED
    task.apply_transition(target, at=now)  # raises ValueError if already terminal
    return TemporalEvent(EventType.TASK_COMPLETED, now, now, task.id, {"late": late})


def _relocate(
    tasks: dict[str, Task],
    task_id: str,
    new_start: datetime,
    new_end: datetime,
    now: datetime,
    to_status: TaskStatus,
    event_type: EventType,
    max_lineage_length: int,
) -> tuple[Task, list[TemporalEvent]]:
    """Shared logic behind reschedule_task and carry_forward_task -- both
    are 'supersede this task with a new one at a new time,' differing
    only in the target status and event type."""
    task = tasks[task_id]

    # Times may originate from an LLM, i.e. untrusted input. A naive
    # datetime (no UTC offset) compared against our aware ones raises
    # TypeError deep inside the engine, which apply_action would not
    # treat as a rejection -- so reject it here with a readable reason.
    for name, value in (("new_start", new_start), ("new_end", new_end)):
        if not isinstance(value, datetime):
            raise ValueError(f"{name} must be an ISO 8601 datetime, got {value!r}")
        if value.tzinfo is None:
            raise ValueError(f"{name} must include a UTC offset, got {value.isoformat()}")

    if new_end <= new_start:
        raise ValueError(f"new_end ({new_end}) must be after new_start ({new_start})")

    if lineage_length(tasks, task_id) >= max_lineage_length:
        raise RescheduleCapExceeded(
            f"task {task_id!r}'s lineage has already been moved "
            f"{max_lineage_length} times; refusing another"
        )

    task.apply_transition(to_status, at=now)

    new_task = Task.new(
        task.title, task.display_timezone,
        scheduled_start=new_start, scheduled_end=new_end,
        deadline=task.deadline, recurrence=task.recurrence,
    )
    new_task.carried_from = task.id
    tasks[new_task.id] = new_task

    events = [
        TemporalEvent(event_type, now, now, task.id),
        task_created_event(new_task, at=now),
    ]
    return new_task, events


def reschedule_task(
    tasks: dict[str, Task], task_id: str, new_start: datetime, new_end: datetime,
    now: datetime, max_lineage_length: int = DEFAULT_MAX_LINEAGE_LENGTH,
) -> tuple[Task, list[TemporalEvent]]:
    return _relocate(
        tasks, task_id, new_start, new_end, now,
        to_status=TaskStatus.RESCHEDULED,
        event_type=EventType.TASK_RESCHEDULED,
        max_lineage_length=max_lineage_length,
    )


def carry_forward_task(
    tasks: dict[str, Task], task_id: str, new_start: datetime, new_end: datetime,
    now: datetime, max_lineage_length: int = DEFAULT_MAX_LINEAGE_LENGTH,
) -> tuple[Task, list[TemporalEvent]]:
    return _relocate(
        tasks, task_id, new_start, new_end, now,
        to_status=TaskStatus.CARRIED_FORWARD,
        event_type=EventType.TASK_CARRIED_FORWARD,
        max_lineage_length=max_lineage_length,
    )


def cancel_task(task: Task, mode: Literal["drop", "cancel"], now: datetime) -> TemporalEvent:
    if mode == "drop":
        task.apply_transition(TaskStatus.DROPPED, at=now)
        return TemporalEvent(EventType.TASK_DROPPED, now, now, task.id)
    elif mode == "cancel":
        task.apply_transition(TaskStatus.CANCELLED, at=now)
        return TemporalEvent(EventType.TASK_CANCELLED, now, now, task.id)
    else:
        raise ValueError(f"unknown cancel mode: {mode!r} (expected 'drop' or 'cancel')")
