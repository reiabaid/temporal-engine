"""
OpenAIProvider: an LLMProvider backed by the OpenAI Chat Completions API,
or by anything that speaks the same protocol via `base_url` -- which
includes local servers such as Ollama (http://localhost:11434/v1), giving
local-model support without a third provider class.

Where this differs from the Anthropic wire format (the actual "hidden
assumptions" a second provider was expected to expose):
  * tools are wrapped as {"type": "function", "function": {...}}, with the
    schema under "parameters" rather than "input_schema";
  * a tool call's arguments arrive as a JSON *string*, not a dict, and can
    be malformed -- so parsing can fail and must not crash the caller;
  * the system prompt is a message, not a separate argument;
  * `tool_calls` is None (not an empty list) when the model calls nothing.

No default model is supplied on purpose: a hardcoded model id goes stale,
and a wrong default fails confusingly. Pass the one you mean.

Lazy SDK import + injectable client: see anthropic_provider.py.
"""
from __future__ import annotations

import json
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
    unparseable_call,
)
from temporal_engine.providers import TemporalContext

_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": PROPOSE_ACTION_NAME,
            "description": PROPOSE_ACTION_DESCRIPTION,
            "parameters": PROPOSE_ACTION_SCHEMA,
        },
    }
]


class OpenAIProvider:
    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        client: Any = None,
    ):
        if client is None:
            import openai

            client = openai.OpenAI(
                api_key=api_key or os.environ.get("OPENAI_API_KEY", "not-needed-for-local"),
                base_url=base_url,
            )
        self._client = client
        self._model = model

    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_prompt(ctx)},
            ],
            tools=_TOOLS,
        )
        message = response.choices[0].message

        calls: list[ActionCall] = []
        for tool_call in message.tool_calls or []:
            if tool_call.function.name != PROPOSE_ACTION_NAME:
                continue
            try:
                arguments = json.loads(tool_call.function.arguments)
            except (TypeError, json.JSONDecodeError) as exc:
                calls.append(unparseable_call(tool_call.id, f"arguments were not valid JSON: {exc}"))
                continue
            calls.append(action_call_from_tool_input(tool_call.id, arguments))
        return calls
