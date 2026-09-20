from types import SimpleNamespace as NS

from temporal_engine.text_completion import AnthropicText, OpenAIText


class _FakeAnthropic:
    def __init__(self, blocks):
        self.blocks, self.requests = blocks, []
        self.messages = NS(create=self._create)

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        return NS(content=self.blocks)


class _FakeOpenAI:
    def __init__(self, content):
        self.content, self.requests = content, []
        self.chat = NS(completions=NS(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        return NS(choices=[NS(message=NS(content=self.content))])


def test_anthropic_text_sends_only_the_prompt_and_joins_text_blocks():
    fake = _FakeAnthropic([NS(type="text", text='{"A": '), NS(type="thinking", text="ignored"), NS(type="text", text='"ACTIVE"}')])
    reply = AnthropicText("m", client=fake).complete("PROMPT")
    assert reply == '{"A": "ACTIVE"}'
    request = fake.requests[0]
    assert request["messages"] == [{"role": "user", "content": "PROMPT"}]
    assert "tools" not in request and "system" not in request and "temperature" not in request


def test_temperature_is_sent_only_when_asked_for():
    fake = _FakeAnthropic([NS(type="text", text="x")])
    AnthropicText("m", client=fake, temperature=0.0).complete("p")
    assert fake.requests[0]["temperature"] == 0.0


def test_openai_text_handles_a_missing_content_field():
    assert OpenAIText("m", client=_FakeOpenAI(None)).complete("p") == ""
    fake = _FakeOpenAI("hello")
    assert OpenAIText("m", client=fake).complete("PROMPT") == "hello"
    assert fake.requests[0]["messages"] == [{"role": "user", "content": "PROMPT"}]
    assert "tools" not in fake.requests[0]
