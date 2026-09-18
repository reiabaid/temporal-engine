"""
MCP server exposing the temporal engine, per SPEC.md section 5's
primitive surface.

Architecture note (see PLAN.md, Phase 1's "wake-up mechanism" decision):
this is one long-running asyncio process that both answers MCP tool
calls AND runs the scheduler loop, in the same event loop, via the
lifespan background-task pattern. There is no separate daemon process
for v1.

Per SPEC.md section 7's multi-writer lock: only one process may hold
SchedulerLock against a given database at a time. If this process fails
to acquire it (because another instance already holds it -- e.g. Claude
Desktop and Claude Code both configured against the same DB file), it
still answers every tool call, it just never runs tick() itself. That's
the "read-only-for-scheduling, read/write-for-everything-else" role
SPEC.md describes.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from mcp.server.mcpserver import Context, MCPServer

from temporal_engine.actions import ActionCall, apply_action
from temporal_engine.engine import DayTracker, tick
from temporal_engine.lock import SchedulerLock, SchedulerLockHeld
from temporal_engine.scheduler import RealClock, next_wake_time
from temporal_engine.storage import all_events, append_event, init_db, replay, task_created_event
from temporal_engine.task import Task

DB_PATH = Path(os.environ.get("TEMPORAL_ENGINE_DB", "temporal_engine.db"))
LOCK_PATH = DB_PATH.with_suffix(".lock")
DAY_BOUNDARY_TZ = os.environ.get("TEMPORAL_ENGINE_TZ", "UTC")


@dataclass
class AppState:
    conn: sqlite3.Connection
    tasks: dict[str, Task]
    clock: RealClock
    seen_idempotency_keys: set = field(default_factory=set)
    is_scheduler: bool = False
    # Set by any tool that changes `tasks`, so a sleeping scheduler loop
    # wakes up and recomputes next_wake_time immediately instead of
    # sitting through a sleep duration that was computed before the
    # change happened. Found by actually running the server end to end:
    # without this, a task created with a near-term boundary while the
    # loop was mid-sleep toward a distant one (e.g. tonight's midnight)
    # would not be ticked until that stale target arrived.
    wake_event: asyncio.Event = field(default_factory=asyncio.Event)


@asynccontextmanager
async def lifespan(server: "MCPServer[AppState]") -> AsyncIterator[AppState]:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    init_db(conn)

    state = AppState(conn=conn, tasks=replay(conn), clock=RealClock(DAY_BOUNDARY_TZ))

    lock = SchedulerLock(LOCK_PATH)
    scheduler_task: Optional[asyncio.Task] = None
    try:
        lock.acquire()
        state.is_scheduler = True
        scheduler_task = asyncio.create_task(_scheduler_loop(state))
    except SchedulerLockHeld:
        # Another process already owns the scheduler for this DB. We
        # still serve tool calls below -- we just never tick().
        pass

    try:
        yield state
    finally:
        if scheduler_task is not None:
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass
            lock.release()
        conn.close()


async def _scheduler_loop(state: AppState) -> None:
    """The event-driven core, unchanged in spirit from PLAN.md's Phase 1:
    compute the next meaningful instant, sleep exactly until then, tick,
    repeat. No fixed-interval polling anywhere."""
    day_tracker = DayTracker()
    while True:
        now = state.clock.now()
        wake = next_wake_time(state.tasks.values(), now, day_boundary_tz=DAY_BOUNDARY_TZ)
        delay = max((wake - now).total_seconds(), 0)

        state.wake_event.clear()
        try:
            await asyncio.wait_for(state.wake_event.wait(), timeout=delay)
            # Woken early by a tool that changed `tasks` -- loop back and
            # recompute next_wake_time against the new state rather than
            # ticking on a boundary that's no longer the right one.
            continue
        except asyncio.TimeoutError:
            pass  # the wake boundary was reached naturally; proceed to tick

        now = state.clock.now()
        events = tick(state.tasks.values(), now, day_tracker, day_boundary_tz=DAY_BOUNDARY_TZ)
        for event in events:
            append_event(state.conn, event)


mcp = MCPServer("temporal-engine", lifespan=lifespan)


def _task_summary(task: Task) -> dict:
    return {
        "id": task.id,
        "title": task.title,
        "status": task.status.value,
        "scheduled_start": task.scheduled_start.isoformat() if task.scheduled_start else None,
        "scheduled_end": task.scheduled_end.isoformat() if task.scheduled_end else None,
        "deadline": task.deadline.isoformat() if task.deadline else None,
        "carried_from": task.carried_from,
    }


@mcp.tool()
def get_temporal_context(ctx: Context) -> dict:
    """Everything relevant right now: current time, every non-terminal
    task, and the most recent events. Per SPEC.md section 6, this
    pull-based call is how an agent answers "what happened while I was
    away" -- it works regardless of whether this MCP client supports
    push notifications (most don't, as of this writing)."""
    state: AppState = ctx.request_context.lifespan_context
    recent = all_events(state.conn)[-20:]
    return {
        "now": state.clock.now().isoformat(),
        "is_scheduler_process": state.is_scheduler,
        "tasks": [_task_summary(t) for t in state.tasks.values() if not t.is_terminal()],
        "recent_events": [
            {"event_type": e.event_type.value, "task_id": e.task_id, "occurred_at": e.occurred_at.isoformat()}
            for e in recent
        ],
    }


@mcp.tool()
def create_task(
    ctx: Context,
    title: str,
    timezone: str,
    scheduled_start: Optional[str] = None,
    scheduled_end: Optional[str] = None,
    deadline: Optional[str] = None,
) -> dict:
    """Create a new task. Times are ISO 8601, e.g. '2026-09-12T14:00:00+05:30'."""
    state: AppState = ctx.request_context.lifespan_context
    task = Task.new(
        title, timezone,
        scheduled_start=datetime.fromisoformat(scheduled_start) if scheduled_start else None,
        scheduled_end=datetime.fromisoformat(scheduled_end) if scheduled_end else None,
        deadline=datetime.fromisoformat(deadline) if deadline else None,
    )
    state.tasks[task.id] = task
    append_event(state.conn, task_created_event(task, at=state.clock.now()))
    state.wake_event.set()  # a sleeping scheduler loop must recheck against this new task
    return _task_summary(task)


def _run_action(state: AppState, action: str, task_id: str, args: dict, reason: str) -> dict:
    call = ActionCall(
        idempotency_key=str(uuid.uuid4()),
        action=action, task_id=task_id, args=args, reason=reason,
    )
    events = apply_action(state.tasks, call, state.clock.now(), state.seen_idempotency_keys)
    for event in events:
        append_event(state.conn, event)
    state.wake_event.set()  # tasks may have changed shape (new task, new status)

    decision = events[0]
    return {
        "outcome": decision.payload["outcome"],
        "rejection_reason": decision.payload.get("rejection_reason"),
        "task": _task_summary(state.tasks[task_id]) if task_id in state.tasks else None,
    }


@mcp.tool()
def complete_task(ctx: Context, task_id: str) -> dict:
    """Mark a task complete. Automatically recorded as late if the task's
    window had already ended or its deadline had already passed."""
    state: AppState = ctx.request_context.lifespan_context
    return _run_action(state, "complete_task", task_id, {}, reason="")


@mcp.tool()
def reschedule_task(ctx: Context, task_id: str, new_start: str, new_end: str, reason: str) -> dict:
    """Move a task to a new time. Capped at 3 moves per task lineage
    (mutators.py) -- further attempts are rejected outright, never
    silently looped."""
    state: AppState = ctx.request_context.lifespan_context
    return _run_action(
        state, "reschedule_task", task_id,
        {"new_start": datetime.fromisoformat(new_start), "new_end": datetime.fromisoformat(new_end)},
        reason=reason,
    )


@mcp.tool()
def carry_forward_task(ctx: Context, task_id: str, new_start: str, new_end: str, reason: str) -> dict:
    """Move a task to a future day (typically tomorrow). Same lineage
    cap as reschedule_task."""
    state: AppState = ctx.request_context.lifespan_context
    return _run_action(
        state, "carry_forward_task", task_id,
        {"new_start": datetime.fromisoformat(new_start), "new_end": datetime.fromisoformat(new_end)},
        reason=reason,
    )


@mcp.tool()
def cancel_task(ctx: Context, task_id: str, mode: str, reason: str) -> dict:
    """mode='drop' abandons the task without finishing it; mode='cancel'
    removes it as no longer relevant (see SPEC.md section 1)."""
    state: AppState = ctx.request_context.lifespan_context
    return _run_action(state, "cancel_task", task_id, {"mode": mode}, reason=reason)


if __name__ == "__main__":
    mcp.run(transport="stdio")
