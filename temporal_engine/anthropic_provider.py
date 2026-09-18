"""
AnthropicProvider: a real LLMProvider implementation, wrapping the Claude
API behind the exact same `decide(ctx) -> list[ActionCall]` interface as
StubHeuristicLLM. This file is what actually proves model-agnosticism --
everything in engine.py, actions.py, and mutators.py is completely
unaware this class exists.

Kept in its own module, separate from providers.py, on purpose: importing
providers.py (which engine tests depend on) must never require the
`anthropic` package or an API key. Only code that explicitly wants a live
model imports this file.

Not exercised by the automated test suite -- see scripts/live_smoke_test.py
for how to run this against a real API key by hand. A live model call is
non-deterministic and costs money; it has no business in a suite that
runs on every commit.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

import anthropic

from temporal_engine.actions import ActionCall
from temporal_engine.providers import TemporalContext

_TOOLS = [
    {
        "name": "propose_action",
        "description": (
            "Propose exactly one action in response to the temporal events "
            "that just occurred. Call this once per action you want to take; "
            "call it zero times if no action is warranted right now."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["complete_task", "reschedule_task", "carry_forward_task", "cancel_task"],
                },
                "task_id": {"type": "string"},
                "new_start": {
                    "type": "string",
                    "description": "ISO 8601 datetime; required for reschedule_task/carry_forward_task",
                },
                "new_end": {
                    "type": "string",
                    "description": "ISO 8601 datetime; required for reschedule_task/carry_forward_task",
                },
                "mode": {
                    "type": "string",
                    "enum": ["drop", "cancel"],
                    "description": "required for cancel_task",
                },
                "reason": {"type": "string", "description": "one sentence explaining the decision"},
            },
            "required": ["action", "task_id", "reason"],
        },
    }
]


class AnthropicProvider:
    def __init__(self, model: str = "claude-sonnet-5", api_key: Optional[str] = None):
        self._client = anthropic.Anthropic(api_key=api_key or os.environ["ANTHROPIC_API_KEY"])
        self._model = model

    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        response = self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            tools=_TOOLS,
            messages=[{"role": "user", "content": self._build_prompt(ctx)}],
        )
        return [
            self._to_action_call(block)
            for block in response.content
            if block.type == "tool_use"
        ]

    def _build_prompt(self, ctx: TemporalContext) -> str:
        lines = [f"Current time: {ctx.now.isoformat()}", "", "Events since the last check:"]
        for e in ctx.events:
            lines.append(f"- {e.event_type.value} (task_id={e.task_id}) at {e.occurred_at.isoformat()}")
        lines.append("")
        lines.append("Current tasks:")
        for t in ctx.tasks:
            lines.append(
                f"- id={t.id} title={t.title!r} status={t.status.value} "
                f"start={t.scheduled_start} end={t.scheduled_end}"
            )
        lines.append("")
        lines.append("Decide what, if anything, should happen using the propose_action tool.")
        return "\n".join(lines)

    def _to_action_call(self, block) -> ActionCall:
        raw = dict(block.input)
        action = raw.pop("action")
        task_id = raw.pop("task_id")
        reason = raw.pop("reason", "")

        args = {}
        for key, value in raw.items():
            if not value:
                continue
            if key in ("new_start", "new_end"):
                args[key] = datetime.fromisoformat(value)
            else:
                args[key] = value

        return ActionCall(
            idempotency_key=block.id,  # Anthropic's tool_use block id is unique per call
            action=action,
            task_id=task_id,
            args=args,
            reason=reason,
        )
