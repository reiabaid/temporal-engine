"""
ActionCall / apply_action: the boundary between "what an LLM (or a stub, or
a human) proposed" and "what the deterministic layer actually did." Every
ActionCall is logged as an ACTION_PROPOSED event (SPEC.md section 4)
whatever the outcome, because the record of what was *considered* matters
as much as what happened.

Outcomes:
  applied               the action ran; its events follow the decision event
  rejected              validation or the state machine refused it (reason logged)
  superseded            the same idempotency_key was already applied; nothing ran
  pending_confirmation  requires_confirmation was set and no human has
                        confirmed yet; nothing ran, and it is held in the log
                        until confirm/decline resolves it

apply_action never talks to an LLM. Who produced the ActionCall is none of
this file's business.
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
    create_task,
    get_task,
    reschedule_task,
    start_task,
)
from temporal_engine.task import Task

# Argument names that hold datetimes, so a logged call can be turned back
# into a live one when a held action is confirmed.
_DATETIME_ARGS = ("new_start", "new_end", "new_deadline", "scheduled_start", "scheduled_end", "deadline")


@dataclass
class ActionCall:
    idempotency_key: str
    action: str  # create_task | start_task | complete_task | reschedule_task | carry_forward_task | cancel_task
    task_id: Optional[str]
    args: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    requires_confirmation: bool = False


def lenient_datetime(value: Any) -> Any:
    """Parse an ISO 8601 string if it is one; otherwise hand the raw value
    on so the validator rejects it with a readable reason (and the decision
    log records it) instead of us guessing what the caller meant."""
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def action_call_from_payload(call: dict) -> ActionCall:
    """Rebuild an ActionCall from its logged form (ISO strings back to datetimes)."""
    args = dict(call.get("args", {}))
    for key in _DATETIME_ARGS:
        if key in args:
            args[key] = lenient_datetime(args[key])
    return ActionCall(
        idempotency_key=call["idempotency_key"],
        action=call["action"],
        task_id=call.get("task_id"),
        args=args,
        reason=call.get("reason", ""),
        requires_confirmation=bool(call.get("requires_confirmation", False)),
    )


def apply_action(
    tasks: dict[str, Task],
    call: ActionCall,
    now: datetime,
    seen_idempotency_keys: set[str],
    confirmed: bool = False,
) -> list[TemporalEvent]:
    """Validate and apply one ActionCall. Always returns at least the
    ACTION_PROPOSED decision event; on success it is followed by whatever
    events the underlying mutator produced.

    Only *applied* keys are remembered: a rejected action may legitimately
    be retried later (the state it was rejected against can change), so
    remembering it would turn a transient refusal into a permanent one.
    """
    if call.idempotency_key in seen_idempotency_keys:
        return [_decision_event(call, now, "superseded", "duplicate idempotency_key")]

    if call.requires_confirmation and not confirmed:
        return [_decision_event(call, now, "pending_confirmation")]

    try:
        events = _dispatch(tasks, call, now)
    except (ValueError, RescheduleCapExceeded) as exc:
        return [_decision_event(call, now, "rejected", str(exc))]

    seen_idempotency_keys.add(call.idempotency_key)
    return [_decision_event(call, now, "applied")] + events


def decline_event(call: ActionCall, now: datetime, reason: str = "declined by a human") -> TemporalEvent:
    """Resolve a held action as rejected. Logged with the same key, which is
    what clears it from the pending set."""
    return _decision_event(call, now, "rejected", reason)


def _dispatch(tasks: dict[str, Task], call: ActionCall, now: datetime) -> list[TemporalEvent]:
    a = call.args

    if call.action == "create_task":
        _, events = create_task(
            tasks, now,
            title=a.get("title"),
            display_timezone=a.get("display_timezone"),
            scheduled_start=a.get("scheduled_start"),
            scheduled_end=a.get("scheduled_end"),
            deadline=a.get("deadline"),
            idempotency_key=call.idempotency_key,
        )
        return events

    if call.action == "start_task":
        return [start_task(get_task(tasks, call.task_id), now)]

    if call.action == "complete_task":
        return [complete_task(get_task(tasks, call.task_id), now)]

    if call.action in ("reschedule_task", "carry_forward_task"):
        mutate = reschedule_task if call.action == "reschedule_task" else carry_forward_task
        _, events = mutate(
            tasks, call.task_id, a.get("new_start"), a.get("new_end"), now,
            new_deadline=a.get("new_deadline"),
        )
        return events

    if call.action == "cancel_task":
        return [cancel_task(get_task(tasks, call.task_id), a.get("mode", "cancel"), now)]

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
