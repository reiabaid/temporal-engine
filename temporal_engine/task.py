"""
Task and TaskStatus: the direct translation of SPEC.md section 1 into code.

Nothing in this file knows about wall-clock time, SQLite, or LLMs. It only
knows: what states exist, and which transitions between them are legal.
That narrowness is deliberate -- it's what makes this file testable in
isolation and safe to build first.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo


class TaskStatus(str, Enum):
    CREATED = "CREATED"
    SCHEDULED = "SCHEDULED"
    ACTIVE = "ACTIVE"
    WINDOW_ENDED = "WINDOW_ENDED"
    OVERDUE = "OVERDUE"
    COMPLETED = "COMPLETED"
    COMPLETED_LATE = "COMPLETED_LATE"
    RESCHEDULED = "RESCHEDULED"
    CARRIED_FORWARD = "CARRIED_FORWARD"
    DROPPED = "DROPPED"
    CANCELLED = "CANCELLED"


# Terminal states: once a task reaches one of these, it never transitions
# again. A completed/rescheduled/dropped/cancelled task is done, full stop.
TERMINAL_STATUSES = {
    TaskStatus.COMPLETED,
    TaskStatus.COMPLETED_LATE,
    TaskStatus.RESCHEDULED,
    TaskStatus.CARRIED_FORWARD,
    TaskStatus.DROPPED,
    TaskStatus.CANCELLED,
}

# The transition table from SPEC.md section 1, as data rather than as a
# chain of if/elif. Keys are "from" states; values are the set of "to"
# states legal from that state. Every arrow in the spec's markdown table
# must appear here exactly once, and nothing else is allowed to appear.
_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    # CREATED = exists but has no scheduled window (an inbox item, possibly
    # with only a deadline). It can be finished, dropped or cancelled like
    # any other task, can breach its deadline, and is scheduled by being
    # superseded via reschedule_task. It never becomes ACTIVE/WINDOW_ENDED
    # because it has no window.
    TaskStatus.CREATED: {
        TaskStatus.SCHEDULED,
        TaskStatus.OVERDUE,
        TaskStatus.COMPLETED,
        TaskStatus.RESCHEDULED,
        TaskStatus.CARRIED_FORWARD,
        TaskStatus.DROPPED,
        TaskStatus.CANCELLED,
    },
    TaskStatus.SCHEDULED: {
        TaskStatus.ACTIVE,
        TaskStatus.OVERDUE,
        TaskStatus.COMPLETED,
        TaskStatus.RESCHEDULED,
        TaskStatus.CARRIED_FORWARD,
        TaskStatus.DROPPED,
        TaskStatus.CANCELLED,
    },
    TaskStatus.ACTIVE: {
        TaskStatus.WINDOW_ENDED,
        TaskStatus.OVERDUE,
        TaskStatus.COMPLETED,
        TaskStatus.RESCHEDULED,
        TaskStatus.CARRIED_FORWARD,
        TaskStatus.DROPPED,
        TaskStatus.CANCELLED,
    },
    TaskStatus.WINDOW_ENDED: {
        TaskStatus.OVERDUE,
        TaskStatus.COMPLETED_LATE,
        TaskStatus.RESCHEDULED,
        TaskStatus.CARRIED_FORWARD,
        TaskStatus.DROPPED,
        TaskStatus.CANCELLED,
    },
    TaskStatus.OVERDUE: {
        TaskStatus.COMPLETED_LATE,
        TaskStatus.RESCHEDULED,
        TaskStatus.CARRIED_FORWARD,
        TaskStatus.DROPPED,
        TaskStatus.CANCELLED,
    },
    # Terminal states: no outgoing transitions at all.
    TaskStatus.COMPLETED: set(),
    TaskStatus.COMPLETED_LATE: set(),
    TaskStatus.RESCHEDULED: set(),
    TaskStatus.CARRIED_FORWARD: set(),
    TaskStatus.DROPPED: set(),
    TaskStatus.CANCELLED: set(),
}


def can_transition(from_status: TaskStatus, to_status: TaskStatus) -> bool:
    """The single source of truth for 'is this state change legal.'
    Every mutator in the engine must call this before writing a new
    status -- an LLM (or a bug) asking for an illegal transition gets
    rejected here, deterministically, regardless of why it was requested.
    """
    return to_status in _TRANSITIONS[from_status]


def validate_schedule(
    start: object,
    end: object,
    deadline: object,
    labels: tuple[str, str, str] = ("scheduled_start", "scheduled_end", "deadline"),
) -> None:
    """Reject schedules that are malformed or self-contradictory.

    Times can originate from an LLM or a client, i.e. untrusted input. A
    naive datetime (no UTC offset) compared against an aware one raises
    TypeError deep inside the engine -- and an uncaught TypeError inside
    the scheduler loop used to kill it silently. So every path that can
    create or move a task validates here, at the boundary, with a readable
    reason. `labels` lets callers name the fields the way their own API
    does (e.g. new_start for a reschedule).
    """
    label_start, label_end, label_deadline = labels
    for label, value in ((label_start, start), (label_end, end), (label_deadline, deadline)):
        if value is None:
            continue
        if not isinstance(value, datetime):
            raise ValueError(f"{label} must be an ISO 8601 datetime, got {value!r}")
        if value.tzinfo is None:
            raise ValueError(f"{label} must include a UTC offset, got {value.isoformat()}")

    if end is not None and start is None:
        raise ValueError(f"{label_end} requires {label_start}")
    if start is not None and end is not None and end <= start:
        raise ValueError(
            f"{label_end} ({end.isoformat()}) must be after {label_start} ({start.isoformat()})"
        )
    if start is not None and deadline is not None and start >= deadline:
        raise ValueError(
            f"{label_start} ({start.isoformat()}) must be before the {label_deadline} "
            f"({deadline.isoformat()})"
        )


@dataclass
class Task:
    id: str
    title: str
    display_timezone: str
    scheduled_start: Optional[datetime] = None
    scheduled_end: Optional[datetime] = None
    deadline: Optional[datetime] = None
    status: TaskStatus = TaskStatus.CREATED
    recurrence: Optional[str] = None
    carried_from: Optional[str] = None
    last_transition_at: Optional[datetime] = None
    # When work actually began, as told to us -- distinct from the
    # clock-derived ACTIVE status, which only means the window opened.
    actual_start: Optional[datetime] = None

    @staticmethod
    def new(
        title: str,
        display_timezone: str,
        scheduled_start: Optional[datetime] = None,
        scheduled_end: Optional[datetime] = None,
        deadline: Optional[datetime] = None,
        recurrence: Optional[str] = None,
    ) -> "Task":
        if not isinstance(title, str) or not title.strip():
            raise ValueError("title must be a non-empty string")
        try:
            ZoneInfo(display_timezone)
        except (KeyError, ValueError, TypeError) as exc:
            raise ValueError(f"unknown timezone {display_timezone!r} (expected an IANA name)") from exc
        validate_schedule(scheduled_start, scheduled_end, deadline)

        status = TaskStatus.SCHEDULED if scheduled_start else TaskStatus.CREATED
        return Task(
            id=str(uuid.uuid4()),
            title=title,
            display_timezone=display_timezone,
            scheduled_start=scheduled_start,
            scheduled_end=scheduled_end,
            deadline=deadline,
            status=status,
            recurrence=recurrence,
        )

    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def apply_transition(self, to_status: TaskStatus, at: datetime) -> None:
        """The only way this task's status is allowed to change.
        Raises if the transition isn't in the table -- this is the
        deterministic/probabilistic boundary from SPEC.md made concrete
        in code: nothing upstream (LLM, user, scheduler) can force an
        illegal state by construction.
        """
        if not can_transition(self.status, to_status):
            raise ValueError(
                f"illegal transition: {self.status} -> {to_status} "
                f"(task {self.id!r})"
            )
        self.status = to_status
        self.last_transition_at = at
