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
from pathlib import Path
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

from mcp.server.mcpserver import Context, MCPServer

from temporal_engine.actions import ActionCall, lenient_datetime
from temporal_engine.agent import Agent, AgentConfig, provider_from_env
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.overlap import find_conflicts, find_overlapping
from temporal_engine.recurrence import Recurrence, next_occurrences
from temporal_engine.runtime import Runtime
from temporal_engine.task import Task

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

def default_db_path(env: Mapping[str, str]) -> str:
    """TEMPORAL_ENGINE_DB if set, else a fixed per-user location. A relative
    default was a footgun: Claude Desktop, Claude Code and a terminal each
    start the server in a different working directory, and would each
    silently get their own separate database."""
    return env.get("TEMPORAL_ENGINE_DB") or str(Path.home() / ".temporal-engine" / "temporal_engine.db")


DB_PATH = default_db_path(os.environ)
DAY_BOUNDARY_TZ = os.environ.get("TEMPORAL_ENGINE_TZ", "UTC")
REFRESH_SECONDS = float(os.environ.get("TEMPORAL_ENGINE_REFRESH_SECONDS", "5"))

MAX_FINISHED_TASKS = 50

# The unattended agent loop is OFF unless TEMPORAL_ENGINE_PROVIDER is set
# (stub | anthropic | openai): it makes paid API calls without anyone asking.
AGENT_MIN_INTERVAL = float(os.environ.get("TEMPORAL_ENGINE_AGENT_MIN_INTERVAL", "30"))


@asynccontextmanager
async def lifespan(server: "MCPServer[Runtime]") -> AsyncIterator[Runtime]:
    if "TEMPORAL_ENGINE_TZ" not in os.environ:
        logging.getLogger("temporal_engine").warning(
            "TEMPORAL_ENGINE_TZ is not set; day boundaries (NEW_DAY) use UTC. Set it to your IANA "
            "timezone, e.g. Asia/Kolkata, or 'a new day' will start at the wrong local time."
        )
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
    if task.recurrence_id:
        summary["recurrence_id"] = task.recurrence_id
        summary["occurrence"] = task.occurrence
    if task.actual_start is not None:
        # Elapsed work is deterministic arithmetic, so the server does it
        # rather than leaving it to the model.
        end = task.last_transition_at if task.is_terminal() and task.last_transition_at else now
        summary["worked_seconds"] = max(0, int((end - task.actual_start).total_seconds()))
    return summary


def _recurrence_summary(rec: Recurrence, now: datetime) -> dict[str, Any]:
    return {
        "id": rec.id,
        "title": rec.title,
        "rule": rec.describe(),
        "timezone": rec.display_timezone,
        "ended": rec.ended,
        "next_occurrences": [dt.isoformat() for dt in next_occurrences(rec, now, 3)],
    }


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
        "now": now.astimezone(ZoneInfo(runtime.day_boundary_tz)).isoformat(),   # display only
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
        "recurrences": [
            _recurrence_summary(r, now) for r in view.recurrences.values()
            if include_finished or not r.ended
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


def _recurrence_result(runtime: Runtime, call: ActionCall, events: list[TemporalEvent]) -> dict[str, Any]:
    view, now = runtime.store.view, runtime.clock.now()
    decision = events[0]
    rec_id = next((e.payload["id"] for e in events if e.event_type == EventType.RECURRENCE_CREATED), None)
    if rec_id is None:
        rec_id = (call.args or {}).get("recurrence_id") or view.created_by_key.get(call.idempotency_key)
    rec = view.recurrences.get(rec_id) if rec_id else None
    return {
        "outcome": decision.payload["outcome"],
        "rejection_reason": decision.payload.get("rejection_reason"),
        "idempotency_key": call.idempotency_key,
        "recurrence": _recurrence_summary(rec, now) if rec else None,
        "instances": [
            _task_summary(view.tasks[e.task_id], now)
            for e in events if e.event_type == EventType.TASK_CREATED and e.task_id in view.tasks
        ],
        "occurrences_skipped": sum(
            e.payload["skipped"] for e in events if e.event_type == EventType.RECURRENCE_GAP
        ),
    }


@mcp.tool()
async def create_recurring_task(
    ctx: Context,
    title: str,
    timezone: str,
    frequency: str,
    start_time: str,
    duration_minutes: Optional[int] = None,
    interval: int = 1,
    weekdays: Optional[list[str]] = None,
    month_days: Optional[list[int]] = None,
    first_date: Optional[str] = None,
    until: Optional[str] = None,
    count: Optional[int] = None,
    idempotency_key: Optional[str] = None,
) -> dict[str, Any]:
    """Create a task that repeats. `frequency` is daily, weekly or monthly;
    `interval` is every N of them. `start_time` is HH:MM LOCAL time in
    `timezone` (an IANA name) and stays that wall-clock time across daylight
    saving changes. weekly takes `weekdays` (MO TU WE TH FR SA SU; default:
    the first date's weekday); monthly takes `month_days` (1-31; a day a
    month lacks is skipped, not clamped). `first_date` (YYYY-MM-DD, default
    today) anchors it; bound it with `until` (inclusive) or `count`, not both.

    Each occurrence becomes an ordinary task, created about a week ahead and
    returned in `instances`. To skip one occurrence, cancel that task; to
    change one, reschedule it; complete one as usual -- none affects the
    others. Use end_recurring_task to stop the series."""
    runtime: Runtime = ctx.request_context.lifespan_context
    call = ActionCall(
        idempotency_key=idempotency_key or str(uuid.uuid4()), action="create_recurrence", task_id=None,
        args={
            "title": title, "display_timezone": timezone, "frequency": frequency, "start_time": start_time,
            "duration_minutes": duration_minutes, "interval": interval, "weekdays": weekdays,
            "month_days": month_days, "first_date": first_date, "until": until, "count": count,
        },
    )
    return _recurrence_result(runtime, call, runtime.execute(call))


@mcp.tool()
async def end_recurring_task(
    ctx: Context, recurrence_id: str, idempotency_key: Optional[str] = None,
) -> dict[str, Any]:
    """Stop generating further occurrences of a recurring task. Occurrences
    that already exist are ordinary tasks and are left alone; cancel any you
    do not want."""
    runtime: Runtime = ctx.request_context.lifespan_context
    call = ActionCall(
        idempotency_key=idempotency_key or str(uuid.uuid4()), action="end_recurrence", task_id=None,
        args={"recurrence_id": recurrence_id},
    )
    return _recurrence_result(runtime, call, runtime.execute(call))


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


def main() -> None:
    """Console-script entry point: `temporal-engine-mcp`."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
