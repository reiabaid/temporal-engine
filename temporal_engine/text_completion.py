"""
Plain text completion for the State-QA baseline: prompt in, reply text out.
No tools, no system prompt -- the baseline gets the same information the
engine arm's ground truth was built from (the history, the definitions, the
current time) and nothing more. Giving it the current time in the prompt is
deliberately generous to the baseline: it saves it a clock-tool call it could
get wrong.

Same shape as the decision providers: the SDK is imported lazily and a client
can be injected, so this is tested with fakes and needs no key in CI.
"""
from __future__ import annotations

import os
from typing import Any, Optional


class AnthropicText:
    def __init__(self, model: str, api_key: Optional[str] = None, client: Any = None,
                 max_tokens: int = 1024, temperature: Optional[float] = None):
        if client is None:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key or os.environ["ANTHROPIC_API_KEY"])
        self._client, self._model, self._max_tokens, self._temperature = client, model, max_tokens, temperature
        self.name = f"anthropic:{model}"

    def complete(self, prompt: str) -> str:
        kwargs: dict[str, Any] = {}
        if self._temperature is not None:        # only sent if asked: not every model accepts it
            kwargs["temperature"] = self._temperature
        response = self._client.messages.create(
            model=self._model, max_tokens=self._max_tokens,
            messages=[{"role": "user", "content": prompt}], **kwargs,
        )
        return "".join(block.text for block in response.content if block.type == "text")


class OpenAIText:
    def __init__(self, model: str, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 client: Any = None, temperature: Optional[float] = None):
        if client is None:
            import openai
            client = openai.OpenAI(api_key=api_key or os.environ.get("OPENAI_API_KEY", "not-needed-for-local"),
                                   base_url=base_url)
        self._client, self._model, self._temperature = client, model, temperature
        self.name = f"openai:{model}"

    def complete(self, prompt: str) -> str:
        kwargs: dict[str, Any] = {}
        if self._temperature is not None:
            kwargs["temperature"] = self._temperature
        response = self._client.chat.completions.create(
            model=self._model, messages=[{"role": "user", "content": prompt}], **kwargs,
        )
        return response.choices[0].message.content or ""
