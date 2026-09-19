"""
Mutators: the only way task state may change as a *decision* (as opposed
to tick()'s deterministic clock-driven transitions). Every mutator routes
through Task.apply_transition, so illegal state changes are rejected the
same way regardless of who's asking, and every one validates its inputs
first, because inputs may come from an LLM.

The reschedule cap lives here on purpose. It is the fix for the bug in the
very first prototype of this project -- a reschedule heuristic that gave a
task a zero-length window, which immediately re-fired WINDOW_ENDED, which
rescheduled again, forever. The invariant is enforced in the deterministic
layer so no model's behaviour (buggy, adversarial, or just a bad decision)
can reproduce that failure mode.

The cap is a RATE limit, not a lifetime total: at most DEFAULT_MAX_MOVES
moves of one task lineage within DEFAULT_MOVE_WINDOW. A runaway loop makes
many moves in seconds and is stopped; a chore legitimately carried forward
one day at a time is never stuck. It cannot tell a person from a model --
both are counted -- which is the price of not trusting the caller.

Nothing here imports an LLM or knows one exists.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal, Optional

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.storage import task_created_event
from temporal_engine.task import Task, TaskStatus, validate_schedule

DEFAULT_MAX_MOVES = 3
DEFAULT_MOVE_WINDOW = timedelta(hours=24)


class RescheduleCapExceeded(RuntimeError):
    """Raised when a task lineage has been moved too often, too recently."""


def get_task(tasks: dict[str, Task], task_id: Optional[str]) -> Task:
    task = tasks.get(task_id) if task_id is not None else None
    if task is None:
        raise ValueError(f"unknown task_id {task_id!r}")
    return task


def lineage_length(tasks: dict[str, Task], task_id: str) -> int:
    """Total number of times this task's lineage has ever been moved,
    walking `carried_from` links back to the original. Informational; the
    cap itself uses recent_moves."""
    count = 0
    current = tasks.get(task_id)
    while current is not None and current.carried_from is not None:
        count += 1
        current = tasks.get(current.carried_from)
    return count


def recent_moves(
    tasks: dict[str, Task], task_id: str, now: datetime, window: timedelta,
) -> int:
    """How many times this lineage was moved within `window` before `now`.
    An ancestor's last_transition_at is the moment it was superseded."""
    count = 0
    current = tasks.get(task_id)
    while current is not None and current.carried_from is not None:
        parent = tasks.get(current.carried_from)
        if parent is None:
            break
        if parent.last_transition_at is not None and now - parent.last_transition_at <= window:
            count += 1
        current = parent
    return count


def create_task(
    tasks: dict[str, Task],
    now: datetime,
    title: str,
    display_timezone: str,
    scheduled_start: Optional[datetime] = None,
    scheduled_end: Optional[datetime] = None,
    deadline: Optional[datetime] = None,
    idempotency_key: Optional[str] = None,
) -> tuple[Task, list[TemporalEvent]]:
    task = Task.new(  # validates everything; raises ValueError on bad input
        title, display_timezone,
        scheduled_start=scheduled_start, scheduled_end=scheduled_end, deadline=deadline,
    )
    tasks[task.id] = task
    return task, [task_created_event(task, at=now, idempotency_key=idempotency_key)]


def start_task(task: Task, now: datetime) -> TemporalEvent:
    """Record that work actually began. Does not change status: ACTIVE is
    derived from the clock and means only that the window opened."""
    if task.is_terminal():
        raise ValueError(f"cannot start a finished task (status {task.status.value})")
    if task.actual_start is not None:
        raise ValueError(f"task was already started at {task.actual_start.isoformat()}")
    task.actual_start = now
    return TemporalEvent(EventType.TASK_WORK_STARTED, now, now, task.id)


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
    max_moves: int,
    window: timedelta,
    new_deadline: Optional[datetime],
) -> tuple[Task, list[TemporalEvent]]:
    """Shared logic behind reschedule_task and carry_forward_task -- both
    supersede a task with a new one at a new time, differing only in target
    status and event type."""
    task = get_task(tasks, task_id)

    for label, value in (("new_start", new_start), ("new_end", new_end)):
        if not isinstance(value, datetime):
            raise ValueError(f"{label} must be an ISO 8601 datetime, got {value!r}")

    # Moving a task past its own deadline is a contradiction, not a plan:
    # the new task would be OVERDUE the instant it exists. Extending the
    # deadline must be said out loud via new_deadline.
    deadline = new_deadline if new_deadline is not None else task.deadline
    validate_schedule(new_start, new_end, deadline, labels=("new_start", "new_end", "deadline"))

    if recent_moves(tasks, task_id, now, window) >= max_moves:
        raise RescheduleCapExceeded(
            f"task {task_id!r} was already moved {max_moves} times within "
            f"{int(window.total_seconds() // 3600)}h; refusing another"
        )

    task.apply_transition(to_status, at=now)

    new_task = Task.new(
        task.title, task.display_timezone,
        scheduled_start=new_start, scheduled_end=new_end,
        deadline=deadline, recurrence=task.recurrence,
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
    now: datetime, max_moves: int = DEFAULT_MAX_MOVES, window: timedelta = DEFAULT_MOVE_WINDOW,
    new_deadline: Optional[datetime] = None,
) -> tuple[Task, list[TemporalEvent]]:
    return _relocate(
        tasks, task_id, new_start, new_end, now,
        to_status=TaskStatus.RESCHEDULED, event_type=EventType.TASK_RESCHEDULED,
        max_moves=max_moves, window=window, new_deadline=new_deadline,
    )


def carry_forward_task(
    tasks: dict[str, Task], task_id: str, new_start: datetime, new_end: datetime,
    now: datetime, max_moves: int = DEFAULT_MAX_MOVES, window: timedelta = DEFAULT_MOVE_WINDOW,
    new_deadline: Optional[datetime] = None,
) -> tuple[Task, list[TemporalEvent]]:
    return _relocate(
        tasks, task_id, new_start, new_end, now,
        to_status=TaskStatus.CARRIED_FORWARD, event_type=EventType.TASK_CARRIED_FORWARD,
        max_moves=max_moves, window=window, new_deadline=new_deadline,
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
