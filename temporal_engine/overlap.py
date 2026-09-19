"""
Overlap detection: pure functions over tasks, no clock and no storage.

Overlaps are REPORTED, never rejected. People double-book on purpose all
the time, so refusing to schedule one would be the engine overreaching;
what the engine owes the caller is knowing about it. Callers (the MCP
tools, the model prompt) surface conflicts so a human or model can decide.

Windows are half-open [start, end): a task ending at 15:00 and another
starting at 15:00 do not overlap. Tasks without both a start and an end
have no duration, so they cannot overlap anything.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

from temporal_engine.task import Task


def windows_overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


def _window(task: Task) -> Optional[tuple[datetime, datetime]]:
    if task.scheduled_start is None or task.scheduled_end is None:
        return None
    return task.scheduled_start, task.scheduled_end


def find_overlapping(
    tasks: Iterable[Task], start: datetime, end: datetime, exclude_id: Optional[str] = None,
) -> list[Task]:
    """Unfinished tasks whose window overlaps [start, end)."""
    found = []
    for task in tasks:
        if task.id == exclude_id or task.is_terminal():
            continue
        window = _window(task)
        if window and windows_overlap(start, end, *window):
            found.append(task)
    return found


def find_conflicts(tasks: Iterable[Task]) -> list[tuple[Task, Task]]:
    """Every pair of unfinished tasks whose windows overlap."""
    windowed = sorted(
        ((t, w) for t in tasks if not t.is_terminal() and (w := _window(t)) is not None),
        key=lambda item: item[1][0],
    )
    pairs = []
    for i, (first, (_, first_end)) in enumerate(windowed):
        for second, (second_start, _) in windowed[i + 1:]:
            if second_start >= first_end:
                break  # sorted by start: nothing later can overlap `first`
            pairs.append((first, second))
    return pairs
