"""
What follows, deterministically, from the planning facts a caller supplied:
estimated duration and dependencies.

Nothing here changes state or raises events. A task with an unfinished dependency is *reported* as blocked, never
refused: people finish things out of order, and the engine overreaching would be worse than the report. The same
stance as overlaps.

Dependencies follow a task through moves. When a dependency is rescheduled it is superseded by a successor with a
new id, so "is it done?" is asked of the head of its lineage, not of the retired original.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable

from temporal_engine.task import Task, TaskStatus

_DONE = (TaskStatus.COMPLETED, TaskStatus.COMPLETED_LATE)
_ABANDONED = (TaskStatus.DROPPED, TaskStatus.CANCELLED)
_MOVED = (TaskStatus.RESCHEDULED, TaskStatus.CARRIED_FORWARD)


def _successors(tasks: dict[str, Task]) -> dict[str, list[str]]:
    children: dict[str, list[str]] = {}
    for t in tasks.values():
        if t.carried_from is not None:
            children.setdefault(t.carried_from, []).append(t.id)
    return children


def lineage_heads(tasks: dict[str, Task], task_id: str, children: dict[str, list[str]] | None = None) -> list[Task]:
    """The live end(s) of a task's lineage: the task itself if it was not moved, else whatever replaced it."""
    children = children if children is not None else _successors(tasks)
    task = tasks.get(task_id)
    if task is None:
        return []
    if task.status not in _MOVED:
        return [task]
    heads: list[Task] = []
    for child in children.get(task_id, []):
        heads.extend(lineage_heads(tasks, child, children))
    return heads or [task]


def dependency_state(tasks: dict[str, Task], task: Task, children: dict[str, list[str]] | None = None) -> dict[str, list[str]]:
    """{'blocked_by': [...], 'abandoned': [...]} for one task, as lists of dependency ids as the caller named them.
    A dependency blocks until its lineage is completed. One that was dropped or cancelled is listed as abandoned
    rather than blocking forever: whether the dependent work still makes sense is a judgement for the model or the
    person, not something to guess."""
    children = children if children is not None else _successors(tasks)
    blocked, abandoned = [], []
    for dep_id in task.depends_on:
        heads = lineage_heads(tasks, dep_id, children)
        if not heads:
            continue    # cannot happen for ids that were validated, but a missing one must never crash a listing
        if all(h.status in _DONE for h in heads):
            continue
        if all(h.status in _ABANDONED for h in heads):
            abandoned.append(dep_id)
        else:
            blocked.append(dep_id)
    return {"blocked_by": blocked, "abandoned": abandoned}


def schedule_order_problems(tasks: dict[str, Task], task: Task, children: dict[str, list[str]] | None = None) -> list[str]:
    """Dependencies whose scheduled window ends AFTER this task is due to start: the plan contradicts the ordering."""
    if task.scheduled_start is None:
        return []
    children = children if children is not None else _successors(tasks)
    late = []
    for dep_id in task.depends_on:
        for head in lineage_heads(tasks, dep_id, children):
            if head.status in _DONE + _ABANDONED:
                continue
            finish = head.scheduled_end or head.deadline
            if finish is not None and finish > task.scheduled_start:
                late.append(dep_id)
                break
    return late


def estimate_report(task: Task, now: datetime) -> dict[str, Any]:
    """Deterministic consequences of an estimate. Empty when there is none. Flags are conservative statements of
    arithmetic ("the window is shorter than the estimate"), never predictions."""
    if task.estimated_minutes is None:
        return {}
    estimate = timedelta(minutes=task.estimated_minutes)
    out: dict[str, Any] = {"estimated_minutes": task.estimated_minutes}
    if task.is_terminal():
        return out

    if task.scheduled_start and task.scheduled_end and task.scheduled_end - task.scheduled_start < estimate:
        out["estimate_exceeds_window"] = True

    if task.actual_start is not None:
        worked = max(timedelta(0), now - task.actual_start)
        out["remaining_estimate_minutes"] = max(0, int((estimate - worked).total_seconds() // 60))
        out["over_estimate"] = worked > estimate
        finish = task.actual_start + estimate
    else:
        # Not started: the earliest it could finish is an estimate's length after the later of now and its start.
        begin = max(now, task.scheduled_start) if task.scheduled_start else now
        finish = begin + estimate
    limit = task.deadline
    if limit is not None:
        out["projected_finish"] = finish.isoformat()
        out["at_risk_of_missing_deadline"] = finish > limit
    return out


def planning_report(tasks: dict[str, Task], task: Task, now: datetime, children: dict[str, list[str]] | None = None) -> dict[str, Any]:
    """Everything derived from the planning fields, for one task, in the shape the tool responses use."""
    out = estimate_report(task, now)
    if task.depends_on:
        out["depends_on"] = list(task.depends_on)
        state = dependency_state(tasks, task, children)
        out.update(state)
        out["is_blocked"] = bool(state["blocked_by"])
        problems = schedule_order_problems(tasks, task, children)
        if problems:
            out["scheduled_before_dependency"] = problems
    return out


def blocked_tasks(tasks: Iterable[Task], all_tasks: dict[str, Task]) -> list[Task]:
    children = _successors(all_tasks)
    return [t for t in tasks if not t.is_terminal() and dependency_state(all_tasks, t, children)["blocked_by"]]
