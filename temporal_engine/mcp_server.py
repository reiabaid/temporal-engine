"""
MCP server exposing the temporal engine (SPEC.md section 5).

All the engine logic lives in runtime.py; this file only translates between
MCP tool calls and it.

Every tool is `async def` on purpose. The SDK runs a *sync* tool function on
a worker thread, which would put tool code and the scheduler loop on
different threads sharing one task dict and one SQLite connection. Async
tools run on the event-loop thread with the scheduler, so there is exactly
one thread touching engine state and no locking to get wrong. The database
calls inside are short and blocking, which is fine for a local single-user
tool; moving them to an executor would reintroduce the threads.

Logging goes to stderr only: stdout is the MCP protocol channel.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional

from mcp.server.mcpserver import Context, MCPServer

from temporal_engine.actions import ActionCall, lenient_datetime
from temporal_engine.agent import Agent, AgentConfig, provider_from_env
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.overlap import find_conflicts, find_overlapping
from temporal_engine.runtime import Runtime
from temporal_engine.task import Task

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

DB_PATH = os.environ.get("TEMPORAL_ENGINE_DB", "temporal_engine.db")
DAY_BOUNDARY_TZ = os.environ.get("TEMPORAL_ENGINE_TZ", "UTC")
REFRESH_SECONDS = float(os.environ.get("TEMPORAL_ENGINE_REFRESH_SECONDS", "5"))

MAX_FINISHED_TASKS = 50

# The unattended agent loop is OFF unless TEMPORAL_ENGINE_PROVIDER is set
# (stub | anthropic | openai): it makes paid API calls without anyone asking.
AGENT_MIN_INTERVAL = float(os.environ.get("TEMPORAL_ENGINE_AGENT_MIN_INTERVAL", "30"))


@asynccontextmanager
async def lifespan(server: "MCPServer[Runtime]") -> AsyncIterator[Runtime]:
    runtime = Runtime(DB_PATH, day_boundary_tz=DAY_BOUNDARY_TZ, refresh_interval=REFRESH_SECONDS)
    provider = provider_from_env(os.environ)  # raises with a clear message on misconfiguration
    if provider is not None:
        runtime.agent = Agent(runtime, provider, AgentConfig(min_interval_seconds=AGENT_MIN_INTERVAL))

    loops = [asyncio.create_task(runtime.run_forever())]
    if runtime.agent is not None:
        loops.append(asyncio.create_task(runtime.agent.run_forever()))
    try:
        yield runtime
    finally:
        for loop_task in loops:
            loop_task.cancel()
        for loop_task in loops:
            try:
                await loop_task
            except asyncio.CancelledError:
                pass
        runtime.close()


mcp = MCPServer("temporal-engine", lifespan=lifespan)


# --------------------------------------------------------------- formatting

def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def _task_summary(task: Task, now: datetime) -> dict[str, Any]:
    summary = {
        "id": task.id,
        "title": task.title,
        "status": task.status.value,
        "scheduled_start": _iso(task.scheduled_start),
        "scheduled_end": _iso(task.scheduled_end),
        "deadline": _iso(task.deadline),
        "carried_from": task.carried_from,
        "actual_start": _iso(task.actual_start),
    }
    if task.actual_start is not None:
        # Elapsed work is deterministic arithmetic, so the server does it
        # rather than leaving it to the model.
        end = task.last_transition_at if task.is_terminal() and task.last_transition_at else now
        summary["worked_seconds"] = max(0, int((end - task.actual_start).total_seconds()))
    return summary


def _event_summary(event: TemporalEvent) -> dict[str, Any]:
    return {
        "seq": event.seq,
        "event_type": event.event_type.value,
        "task_id": event.task_id,
        "occurred_at": event.occurred_at.isoformat(),
        "recorded_at": event.recorded_at.isoformat(),
        "detail": event.payload if event.event_type != EventType.TASK_CREATED else None,
    }


def _result(runtime: Runtime, call: ActionCall, events: list[TemporalEvent]) -> dict[str, Any]:
    """Shape an action's outcome for the caller. Includes the *new* task for
    reschedule/carry_forward/create: those supersede or create rather than
    mutate, and the caller needs the new id to act on the task again."""
    view, now = runtime.store.view, runtime.clock.now()
    decision = events[0]
    created = [e.task_id for e in events if e.event_type == EventType.TASK_CREATED]
    new_task_id = created[0] if created else None
    if new_task_id is None and decision.payload["outcome"] == "superseded":
        new_task_id = view.created_by_key.get(call.idempotency_key)  # a retried create

    new_task = view.tasks.get(new_task_id) if new_task_id else None
    task = view.tasks.get(call.task_id) if call.task_id else None

    result: dict[str, Any] = {
        "outcome": decision.payload["outcome"],
        "rejection_reason": decision.payload.get("rejection_reason"),
        "idempotency_key": call.idempotency_key,
        "task": _task_summary(task, now) if task else None,
        "new_task": _task_summary(new_task, now) if new_task else None,
    }
    if new_task is not None and new_task.scheduled_start and new_task.scheduled_end:
        result["overlaps_with"] = [
            {"id": t.id, "title": t.title, "scheduled_start": _iso(t.scheduled_start),
             "scheduled_end": _iso(t.scheduled_end)}
            for t in find_overlapping(
                view.tasks.values(), new_task.scheduled_start, new_task.scheduled_end,
                exclude_id=new_task.id,
            )
        ]
    return result


def _execute(runtime: Runtime, action: str, task_id: Optional[str], args: dict, reason: str,
             idempotency_key: Optional[str]) -> dict[str, Any]:
    call = ActionCall(
        idempotency_key=idempotency_key or str(uuid.uuid4()),
        action=action, task_id=task_id, args=args, reason=reason,
    )
    return _result(runtime, call, runtime.execute(call))


# -------------------------------------------------------------------- reads

@mcp.tool()
async def get_temporal_context(
    ctx: Context,
    since_seq: Optional[int] = None,
    since: Optional[str] = None,
    event_limit: int = 20,
    include_finished: bool = False,
) -> dict[str, Any]:
    """Everything relevant right now: current time, unfinished tasks,
    overlapping tasks, actions awaiting confirmation, scheduler health, and
    recent events.

    To learn what happened while away, pass the `cursor` from your previous
    call as `since_seq` (or an ISO 8601 time as `since`): you get only newer
    events, oldest first, with `events_truncated` set if there are more than
    `event_limit`. Set include_finished to also list recently finished tasks
    (completed, moved, dropped, cancelled) -- otherwise they are omitted."""
    runtime: Runtime = ctx.request_context.lifespan_context
    view = runtime.store.refresh()
    now = runtime.clock.now()

    since_time = lenient_datetime(since) if since else None
    if since is not None and not isinstance(since_time, datetime):
        raise ValueError(f"since must be an ISO 8601 datetime, got {since!r}")
    events, truncated = runtime.store.events(
        since_seq=since_seq, since_time=since_time, limit=max(1, min(event_limit, 200)),
    )

    active = [t for t in view.tasks.values() if not t.is_terminal()]
    health = runtime.health
    out: dict[str, Any] = {
        "now": now.isoformat(),
        "day_boundary_timezone": runtime.day_boundary_tz,
        "is_scheduler_process": health.is_scheduler,
        "scheduler": {
            "last_pass_at": _iso(health.last_pass_at),
            "last_tick_at": _iso(health.last_tick_at),
            "last_error": health.last_error,
            "consecutive_errors": health.consecutive_errors,
        },
        "quarantined_events": len(view.quarantined),
        "agent": runtime.agent.status() if runtime.agent else {"enabled": False},
        "tasks": [_task_summary(t, now) for t in active],
        "conflicts": [
            {"a": {"id": a.id, "title": a.title}, "b": {"id": b.id, "title": b.title}}
            for a, b in find_conflicts(active)
        ],
        "pending_actions": [
            {"idempotency_key": key, "action": c["action"], "task_id": c.get("task_id"),
             "reason": c.get("reason", ""), "args": c.get("args", {})}
            for key, c in view.pending.items()
        ],
        "events": [_event_summary(e) for e in events],
        "events_truncated": truncated,
        "cursor": view.last_seq,
    }
    if include_finished:
        finished = sorted(
            (t for t in view.tasks.values() if t.is_terminal()),
            key=lambda t: t.last_transition_at or datetime.min.replace(tzinfo=now.tzinfo),
            reverse=True,
        )[:MAX_FINISHED_TASKS]
        out["finished_tasks"] = [_task_summary(t, now) for t in finished]
    return out


@mcp.tool()
async def list_pending_actions(ctx: Context) -> list[dict[str, Any]]:
    """Actions held for a human's confirmation (e.g. cancelling a task).
    Resolve each with confirm_action or reject_action."""
    runtime: Runtime = ctx.request_context.lifespan_context
    view = runtime.store.refresh()
    return [
        {"idempotency_key": key, "action": c["action"], "task_id": c.get("task_id"),
         "reason": c.get("reason", ""), "args": c.get("args", {})}
        for key, c in view.pending.items()
    ]


# ------------------------------------------------------------------ actions
# Every mutating tool takes an optional idempotency_key. Supply the same key
# when retrying a call whose response you did not receive: the second call is
# recognised (durably -- across restarts and processes) and does nothing.

@mcp.tool()
async def create_task(
    ctx: Context,
    title: str,
    timezone: str,
    scheduled_start: Optional[str] = None,
    scheduled_end: Optional[str] = None,
    deadline: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> dict[str, Any]:
    """Create a task. Times are ISO 8601 WITH a UTC offset, e.g.
    '2026-09-12T14:00:00+05:30'. `timezone` is an IANA name used for display.
    A window needs both scheduled_start and scheduled_end (end after start);
    a task may have only a deadline. The response lists any overlapping
    tasks; overlaps are reported, not refused."""
    runtime: Runtime = ctx.request_context.lifespan_context
    return _execute(runtime, "create_task", None, {
        "title": title, "display_timezone": timezone,
        "scheduled_start": lenient_datetime(scheduled_start),
        "scheduled_end": lenient_datetime(scheduled_end),
        "deadline": lenient_datetime(deadline),
    }, "", idempotency_key)


@mcp.tool()
async def start_task(ctx: Context, task_id: str, idempotency_key: Optional[str] = None) -> dict[str, Any]:
    """Record that work on a task actually began now. Enables the
    `worked_seconds` figure in task summaries ("how long have I been on
    this?"). Does not change the task's status."""
    runtime: Runtime = ctx.request_context.lifespan_context
    return _execute(runtime, "start_task", task_id, {}, "", idempotency_key)


@mcp.tool()
async def complete_task(ctx: Context, task_id: str, idempotency_key: Optional[str] = None) -> dict[str, Any]:
    """Mark a task complete. Recorded as late if its window had already
    ended or its deadline had already passed."""
    runtime: Runtime = ctx.request_context.lifespan_context
    return _execute(runtime, "complete_task", task_id, {}, "", idempotency_key)


@mcp.tool()
async def reschedule_task(
    ctx: Context, task_id: str, new_start: str, new_end: str, reason: str,
    new_deadline: Optional[str] = None, idempotency_key: Optional[str] = None,
) -> dict[str, Any]:
    """Move a task to a new time. It is superseded by a new task -- use the
    returned `new_task.id` from then on. Rejected if the new start is not
    before the task's deadline (pass new_deadline to extend it on purpose).
    A task lineage can be moved at most 3 times per 24 hours; more is
    rejected, never looped."""
    runtime: Runtime = ctx.request_context.lifespan_context
    args = {"new_start": lenient_datetime(new_start), "new_end": lenient_datetime(new_end)}
    if new_deadline:
        args["new_deadline"] = lenient_datetime(new_deadline)
    return _execute(runtime, "reschedule_task", task_id, args, reason, idempotency_key)


@mcp.tool()
async def carry_forward_task(
    ctx: Context, task_id: str, new_start: str, new_end: str, reason: str,
    new_deadline: Optional[str] = None, idempotency_key: Optional[str] = None,
) -> dict[str, Any]:
    """Move a task to a later day. Same rules, superseding and rate limit
    as reschedule_task."""
    runtime: Runtime = ctx.request_context.lifespan_context
    args = {"new_start": lenient_datetime(new_start), "new_end": lenient_datetime(new_end)}
    if new_deadline:
        args["new_deadline"] = lenient_datetime(new_deadline)
    return _execute(runtime, "carry_forward_task", task_id, args, reason, idempotency_key)


@mcp.tool()
async def cancel_task(
    ctx: Context, task_id: str, mode: str, reason: str, idempotency_key: Optional[str] = None,
) -> dict[str, Any]:
    """mode='drop' abandons the task without finishing it; mode='cancel'
    removes it as no longer relevant (SPEC.md section 1)."""
    runtime: Runtime = ctx.request_context.lifespan_context
    return _execute(runtime, "cancel_task", task_id, {"mode": mode}, reason, idempotency_key)


@mcp.tool()
async def confirm_action(ctx: Context, idempotency_key: str) -> dict[str, Any]:
    """Approve an action that is awaiting confirmation (see
    list_pending_actions) and apply it."""
    runtime: Runtime = ctx.request_context.lifespan_context
    events = runtime.confirm(idempotency_key)
    call = ActionCall(idempotency_key=idempotency_key, action="", task_id=events[0].task_id)
    return _result(runtime, call, events)


@mcp.tool()
async def reject_action(ctx: Context, idempotency_key: str) -> dict[str, Any]:
    """Decline an action that is awaiting confirmation. Nothing is applied."""
    runtime: Runtime = ctx.request_context.lifespan_context
    events = runtime.decline(idempotency_key)
    return {"outcome": events[0].payload["outcome"], "idempotency_key": idempotency_key}


if __name__ == "__main__":
    mcp.run(transport="stdio")
