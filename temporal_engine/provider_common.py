"""
Everything about talking to an LLM that is NOT specific to one vendor: the
instructions, the tool schema, the prompt built from a TemporalContext, and
the conversion of a model's raw tool-call output into an ActionCall.

Vendor modules (anthropic_provider.py, openai_provider.py) only translate
between this and their SDK's wire format. That split is what "model
agnostic" means in practice: if a third vendor needs 30 lines of glue
instead of a rewrite, the factoring is right.

Model output is untrusted input. Nothing here raises on malformed output;
anything unusable becomes an ActionCall that the deterministic layer
rejects and logs (see apply_action), so a bad tool call shows up in the
decision log instead of crashing the caller.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from temporal_engine.actions import ActionCall
from temporal_engine.providers import TemporalContext

PROPOSE_ACTION_NAME = "propose_action"

PROPOSE_ACTION_DESCRIPTION = (
    "Propose exactly one action in response to the temporal events that just "
    "occurred. Call this once per action you want to take; call it zero times "
    "if no action is warranted right now."
)

PROPOSE_ACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["complete_task", "reschedule_task", "carry_forward_task", "cancel_task"],
        },
        "task_id": {"type": "string"},
        "new_start": {
            "type": "string",
            "description": "ISO 8601 datetime WITH a UTC offset; required for reschedule_task/carry_forward_task",
        },
        "new_end": {
            "type": "string",
            "description": "ISO 8601 datetime WITH a UTC offset, after new_start; required for reschedule_task/carry_forward_task",
        },
        "mode": {
            "type": "string",
            "enum": ["drop", "cancel"],
            "description": "required for cancel_task",
        },
        "reason": {"type": "string", "description": "one sentence explaining the decision"},
    },
    "required": ["action", "task_id", "reason"],
}

SYSTEM_PROMPT = (
    "You are the reasoning layer of a scheduling engine. A deterministic clock "
    "has already worked out what happened; you decide what to do about it.\n"
    "Rules:\n"
    "- Trust the times and statuses you are given; never recompute them.\n"
    "- Only act on tasks that are not already finished (completed, rescheduled, "
    "carried forward, dropped, cancelled). If nothing needs doing, propose nothing.\n"
    "- A rescheduled task must fit before the next scheduled task and before the "
    "end of the current day; if it cannot, carry it forward to a later day instead.\n"
    "- Every datetime you produce must be ISO 8601 with a UTC offset, and new_end "
    "must be after new_start.\n"
    "- The same task can only be moved a few times; do not keep moving it."
)


def build_prompt(ctx: TemporalContext) -> str:
    lines = [f"Current time: {ctx.now.isoformat()}", "", "Events since the last check:"]
    if not ctx.events:
        lines.append("- (none)")
    for e in ctx.events:
        lines.append(f"- {e.event_type.value} (task_id={e.task_id}) at {e.occurred_at.isoformat()}")
    lines += ["", "Current tasks:"]
    if not ctx.tasks:
        lines.append("- (none)")
    for t in ctx.tasks:
        start = t.scheduled_start.isoformat() if t.scheduled_start else "none"
        end = t.scheduled_end.isoformat() if t.scheduled_end else "none"
        lines.append(
            f"- id={t.id} title={t.title!r} status={t.status.value} start={start} end={end}"
        )
    lines += ["", f"Decide what, if anything, should happen using the {PROPOSE_ACTION_NAME} tool."]
    return "\n".join(lines)


def unparseable_call(call_id: str, detail: str) -> ActionCall:
    """A tool call we could not make sense of. Represented as an ActionCall
    with an action no dispatcher knows, so apply_action rejects it and the
    decision log records exactly what went wrong."""
    return ActionCall(
        idempotency_key=call_id,
        action="unparseable_tool_call",
        task_id=None,
        args={"detail": detail},
        reason=detail,
    )


def action_call_from_tool_input(call_id: str, raw: Any) -> ActionCall:
    if not isinstance(raw, dict):
        return unparseable_call(call_id, f"tool input was not an object: {raw!r}")

    action = raw.get("action")
    if not isinstance(action, str) or not action:
        return unparseable_call(call_id, f"tool input had no usable 'action': {raw!r}")

    args: dict[str, Any] = {}
    for key in ("new_start", "new_end"):
        value = raw.get(key)
        if not value:
            continue
        try:
            args[key] = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            # Keep the raw value; the validator rejects it with a clear
            # reason rather than us guessing what the model meant.
            args[key] = value
    if raw.get("mode"):
        args["mode"] = raw["mode"]

    return ActionCall(
        idempotency_key=call_id,
        action=action,
        task_id=raw.get("task_id"),
        args=args,
        reason=str(raw.get("reason", "")),
    )
