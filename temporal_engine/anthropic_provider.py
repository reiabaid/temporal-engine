"""
AnthropicProvider: an LLMProvider backed by the Claude API. Only wire
format lives here; prompt, schema, and output parsing are shared with
every other provider via provider_common.py.

The `anthropic` package is imported lazily, and a client can be injected,
so tests can exercise the translation with a fake client -- no SDK, no
API key, no network. Live use is manual: scripts/live_scenarios.py.
"""
from __future__ import annotations

import os
from typing import Any, Optional

from temporal_engine.actions import ActionCall
from temporal_engine.provider_common import (
    PROPOSE_ACTION_DESCRIPTION,
    PROPOSE_ACTION_NAME,
    PROPOSE_ACTION_SCHEMA,
    SYSTEM_PROMPT,
    action_call_from_tool_input,
    build_prompt,
)
from temporal_engine.providers import TemporalContext

_TOOLS = [
    {
        "name": PROPOSE_ACTION_NAME,
        "description": PROPOSE_ACTION_DESCRIPTION,
        "input_schema": PROPOSE_ACTION_SCHEMA,
    }
]


class AnthropicProvider:
    def __init__(
        self,
        model: str = "claude-sonnet-5",
        api_key: Optional[str] = None,
        client: Any = None,
    ):
        if client is None:
            import anthropic

            client = anthropic.Anthropic(api_key=api_key or os.environ["ANTHROPIC_API_KEY"])
        self._client = client
        self._model = model

    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        response = self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=_TOOLS,
            messages=[{"role": "user", "content": build_prompt(ctx)}],
        )
        return [
            action_call_from_tool_input(block.id, block.input)
            for block in response.content
            if block.type == "tool_use" and block.name == PROPOSE_ACTION_NAME
        ]
