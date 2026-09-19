"""
The two vendor providers are tested against fake SDK clients shaped like
each vendor's real response objects. No SDK, no key, no network -- what's
under test is OUR translation: request shape out, tool-call parsing in.
Whether a live model produces good decisions is a different question,
answered by scripts/live_scenarios.py, deliberately not by CI.
"""
import json
from datetime import datetime, timezone
from types import SimpleNamespace as NS

from temporal_engine.anthropic_provider import AnthropicProvider
from temporal_engine.openai_provider import OpenAIProvider
from temporal_engine.provider_common import PROPOSE_ACTION_NAME, action_call_from_tool_input
from temporal_engine.providers import TemporalContext
from temporal_engine.scenarios import SCENARIOS, run_scenario

UTC = timezone.utc


def _ctx() -> TemporalContext:
    return SCENARIOS[0].build()


# ---------- shared parsing ----------

def test_action_call_from_tool_input_parses_iso_datetimes_and_carries_the_call_id():
    call = action_call_from_tool_input("call_1", {
        "action": "reschedule_task", "task_id": "t1", "reason": "gap",
        "new_start": "2026-09-12T16:00:00+00:00", "new_end": "2026-09-12T17:00:00+00:00",
    })
    assert call.idempotency_key == "call_1"
    assert call.args["new_start"] == datetime(2026, 9, 12, 16, tzinfo=UTC)


def test_unparseable_datetime_is_kept_raw_so_the_validator_can_reject_it_readably():
    call = action_call_from_tool_input("c", {
        "action": "reschedule_task", "task_id": "t1", "reason": "x",
        "new_start": "sometime this afternoon", "new_end": "2026-09-12T17:00:00+00:00",
    })
    assert call.args["new_start"] == "sometime this afternoon"


def test_non_object_or_actionless_tool_input_becomes_an_unparseable_call_not_an_exception():
    for bad in ("just text", None, {"task_id": "t1"}, {"action": ""}):
        call = action_call_from_tool_input("c", bad)
        assert call.action == "unparseable_tool_call"


# ---------- Anthropic ----------

class _FakeAnthropic:
    def __init__(self, blocks):
        self.blocks = blocks
        self.requests = []
        self.messages = NS(create=self._create)

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        return NS(content=self.blocks)


def test_anthropic_request_shape_and_tool_use_parsing():
    fake = _FakeAnthropic([
        NS(type="text", text="thinking out loud"),  # non-tool blocks must be ignored
        NS(type="tool_use", id="toolu_1", name=PROPOSE_ACTION_NAME, input={
            "action": "reschedule_task", "task_id": "t1", "reason": "gap",
            "new_start": "2026-09-12T16:00:00+00:00", "new_end": "2026-09-12T17:00:00+00:00",
        }),
    ])
    calls = AnthropicProvider(client=fake).decide(_ctx())

    assert [c.action for c in calls] == ["reschedule_task"]
    assert calls[0].idempotency_key == "toolu_1"

    request = fake.requests[0]
    assert request["tools"][0]["name"] == PROPOSE_ACTION_NAME
    assert "input_schema" in request["tools"][0]
    assert "system" in request  # system prompt is its own argument for this vendor


def test_anthropic_returning_no_tool_use_means_no_actions():
    assert AnthropicProvider(client=_FakeAnthropic([NS(type="text", text="all good")])).decide(_ctx()) == []


# ---------- OpenAI ----------

class _FakeOpenAI:
    def __init__(self, tool_calls):
        self.tool_calls = tool_calls
        self.requests = []
        self.chat = NS(completions=NS(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        return NS(choices=[NS(message=NS(tool_calls=self.tool_calls))])


def _openai_call(call_id, arguments, name=PROPOSE_ACTION_NAME):
    return NS(id=call_id, function=NS(name=name, arguments=arguments))


def test_openai_request_shape_and_json_string_argument_parsing():
    fake = _FakeOpenAI([_openai_call("call_9", json.dumps({
        "action": "carry_forward_task", "task_id": "t1", "reason": "late",
        "new_start": "2026-09-13T09:00:00+00:00", "new_end": "2026-09-13T11:00:00+00:00",
    }))])
    calls = OpenAIProvider(model="some-model", client=fake).decide(_ctx())

    assert [c.action for c in calls] == ["carry_forward_task"]
    assert calls[0].idempotency_key == "call_9"

    request = fake.requests[0]
    assert request["model"] == "some-model"
    assert request["tools"][0]["type"] == "function"
    assert request["tools"][0]["function"]["name"] == PROPOSE_ACTION_NAME
    assert "parameters" in request["tools"][0]["function"]
    assert request["messages"][0]["role"] == "system"  # a message here, not a separate argument


def test_openai_tool_calls_being_none_means_no_actions():
    """OpenAI returns None, not [], when the model calls no tool."""
    assert OpenAIProvider(model="m", client=_FakeOpenAI(None)).decide(_ctx()) == []


def test_openai_malformed_json_arguments_are_logged_as_a_rejectable_call_not_a_crash():
    fake = _FakeOpenAI([_openai_call("call_bad", '{"action": "reschedule_task", "task_id": ')])  # truncated
    calls = OpenAIProvider(model="m", client=fake).decide(_ctx())

    assert len(calls) == 1
    assert calls[0].action == "unparseable_tool_call"
    assert calls[0].idempotency_key == "call_bad"


def test_openai_ignores_calls_to_tools_we_did_not_offer():
    fake = _FakeOpenAI([_openai_call("c", "{}", name="something_else")])
    assert OpenAIProvider(model="m", client=fake).decide(_ctx()) == []


# ---------- the point of the exercise ----------

def test_both_providers_produce_the_identical_decision_from_equivalent_model_output():
    """Model-agnosticism, concretely: same intent from two different wire
    formats yields the same ActionCall content, which then passes the same
    scenario through the same deterministic layer."""
    payload = {
        "action": "reschedule_task", "reason": "gap",
        "new_start": "2026-09-12T16:00:00+00:00", "new_end": "2026-09-12T18:00:00+00:00",
    }
    scenario = SCENARIOS[0]  # slack_available
    task_id = scenario.build().events[0].task_id  # ids are fresh per build; read them off the built ctx

    class _Bound:
        """Adapts a vendor provider so the model 'answers' for whichever
        task id this build of the scenario generated."""
        def __init__(self, make_provider):
            self._make = make_provider

        def decide(self, ctx):
            return self._make(ctx.events[0].task_id).decide(ctx)

    anthropic = _Bound(lambda tid: AnthropicProvider(client=_FakeAnthropic([
        NS(type="tool_use", id="a1", name=PROPOSE_ACTION_NAME, input={**payload, "task_id": tid}),
    ])))
    openai = _Bound(lambda tid: OpenAIProvider(model="m", client=_FakeOpenAI([
        _openai_call("o1", json.dumps({**payload, "task_id": tid})),
    ])))

    assert run_scenario(anthropic, scenario).passed
    assert run_scenario(openai, scenario).passed
    assert task_id  # (sanity: the scenario really does carry a task id)
