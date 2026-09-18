"""
ActionCall / apply_action: the boundary between "what an LLM (or a
heuristic stub, or a human) proposed" and "what the deterministic layer
actually did." Every ActionCall is logged as an ACTION_PROPOSED event
(SPEC.md section 4) regardless of outcome -- applied, rejected, or a
duplicate -- because the record of what was *considered* matters as much
as what happened, both for debugging and for the benchmark planned in
PLAN.md's Phase 6.

apply_action never talks to an LLM. It receives an ActionCall that
already exists -- who produced it (a real model, a stub, a human clicking
a button) is none of this file's business.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.mutators import (
    RescheduleCapExceeded,
    carry_forward_task,
    cancel_task,
    complete_task,
    reschedule_task,
)
from temporal_engine.task import Task


@dataclass
class ActionCall:
    idempotency_key: str
    action: str  # "complete_task" | "reschedule_task" | "carry_forward_task" | "cancel_task"
    task_id: Optional[str]
    args: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    requires_confirmation: bool = False


def apply_action(
    tasks: dict[str, Task],
    call: ActionCall,
    now: datetime,
    seen_idempotency_keys: set[str],
) -> list[TemporalEvent]:
    """Validate and apply one ActionCall. Always returns at least the
    ACTION_PROPOSED decision-log event; on success it's followed by
    whatever events the underlying mutator produced."""
    if call.idempotency_key in seen_idempotency_keys:
        return [_decision_event(call, now, outcome="superseded",
                                 rejection_reason="duplicate idempotency_key")]
    seen_idempotency_keys.add(call.idempotency_key)

    try:
        events = _dispatch(tasks, call, now)
        return [_decision_event(call, now, outcome="applied")] + events
    except (KeyError, ValueError, RescheduleCapExceeded) as exc:
        return [_decision_event(call, now, outcome="rejected", rejection_reason=str(exc))]


def _dispatch(tasks: dict[str, Task], call: ActionCall, now: datetime) -> list[TemporalEvent]:
    if call.action == "complete_task":
        task = tasks[call.task_id]
        return [complete_task(task, now)]

    elif call.action == "reschedule_task":
        _, events = reschedule_task(
            tasks, call.task_id, call.args["new_start"], call.args["new_end"], now,
        )
        return events

    elif call.action == "carry_forward_task":
        _, events = carry_forward_task(
            tasks, call.task_id, call.args["new_start"], call.args["new_end"], now,
        )
        return events

    elif call.action == "cancel_task":
        task = tasks[call.task_id]
        return [cancel_task(task, call.args.get("mode", "cancel"), now)]

    else:
        raise ValueError(f"unknown action: {call.action!r}")


def _decision_event(
    call: ActionCall, now: datetime, outcome: str, rejection_reason: Optional[str] = None,
) -> TemporalEvent:
    return TemporalEvent(
        event_type=EventType.ACTION_PROPOSED,
        occurred_at=now,
        recorded_at=now,
        task_id=call.task_id,
        payload={
            "action_call": {
                "idempotency_key": call.idempotency_key,
                "action": call.action,
                "task_id": call.task_id,
                "args": {
                    k: (v.isoformat() if hasattr(v, "isoformat") else v)
                    for k, v in call.args.items()
                },
                "reason": call.reason,
                "requires_confirmation": call.requires_confirmation,
            },
            "outcome": outcome,
            "rejection_reason": rejection_reason,
        },
    )
