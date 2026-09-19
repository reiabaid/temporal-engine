from datetime import datetime, timedelta, timezone

from temporal_engine.actions import ActionCall
from temporal_engine.providers import StubHeuristicLLM, TemporalContext
from temporal_engine.scenarios import SCENARIOS, run_all, run_scenario

UTC = timezone.utc


def _by_name(results):
    return {r.name: r for r in results}


def test_the_stub_passes_every_scenario():
    results = run_all(StubHeuristicLLM())
    failing = {r.name: r.failures for r in results if not r.passed}
    assert failing == {}


def test_scenario_names_are_unique():
    names = [s.name for s in SCENARIOS]
    assert len(names) == len(set(names))


class _DoesNothing:
    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        return []


def test_a_provider_that_never_acts_fails_exactly_the_scenarios_that_need_action():
    results = _by_name(run_all(_DoesNothing()))
    assert not results["slack_available"].passed
    assert not results["no_time_left"].passed
    assert results["nothing_to_do"].passed  # restraint is a correct answer here
    assert results["stale_event_for_finished_task"].passed


class _CompletesEverything:
    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        return [
            ActionCall(idempotency_key=f"k{i}", action="complete_task", task_id=t.id)
            for i, t in enumerate(ctx.tasks)
        ]


def test_a_provider_that_acts_on_everything_fails_the_restraint_scenarios():
    results = _by_name(run_all(_CompletesEverything()))
    assert not results["nothing_to_do"].passed
    assert not results["stale_event_for_finished_task"].passed
    assert not results["slack_available"].passed  # complete_task is not an allowed action


class _ReschedulesPastMidnight:
    """The classic wrong answer to 'no time left': squeeze it in anyway."""

    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        if not ctx.events or ctx.events[0].event_type.value != "TASK_WINDOW_ENDED":
            return []
        task_id = ctx.events[0].task_id
        return [ActionCall(
            idempotency_key="k", action="reschedule_task", task_id=task_id,
            args={"new_start": ctx.now, "new_end": ctx.now + timedelta(hours=2)},
        )]


def test_rescheduling_across_midnight_is_caught_as_the_wrong_decision():
    result = _by_name(run_all(_ReschedulesPastMidnight()))["no_time_left"]
    assert not result.passed
    assert any("not an allowed action" in f for f in result.failures)


class _OverlapsTheNextTask:
    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        if not ctx.events or ctx.events[0].event_type.value != "TASK_WINDOW_ENDED":
            return []
        return [ActionCall(
            idempotency_key="k", action="reschedule_task", task_id=ctx.events[0].task_id,
            args={"new_start": ctx.now, "new_end": ctx.now + timedelta(hours=4)},  # runs into 18:00
        )]


def test_a_reschedule_that_overlaps_the_next_task_fails_the_constraint():
    result = _by_name(run_all(_OverlapsTheNextTask()))["slack_available"]
    assert not result.passed
    assert any("overlaps the next task" in f for f in result.failures)


class _NaiveDatetimes:
    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        if not ctx.events or ctx.events[0].event_type.value != "TASK_WINDOW_ENDED":
            return []
        naive = datetime(2026, 9, 12, 16, 0)  # no UTC offset -- a realistic model mistake
        return [ActionCall(
            idempotency_key="k", action="reschedule_task", task_id=ctx.events[0].task_id,
            args={"new_start": naive, "new_end": naive + timedelta(hours=1)},
        )]


def test_naive_datetimes_from_a_model_fail_cleanly_instead_of_crashing():
    result = _by_name(run_all(_NaiveDatetimes()))["slack_available"]
    assert not result.passed
    assert any("must include a UTC offset" in f for f in result.failures)


class _Explodes:
    def decide(self, ctx: TemporalContext) -> list[ActionCall]:
        raise RuntimeError("API timed out")


def test_a_provider_that_raises_fails_its_scenarios_without_aborting_the_run():
    results = run_all(_Explodes())
    assert len(results) == len(SCENARIOS)
    assert all(not r.passed for r in results)
    assert "API timed out" in results[0].failures[0]
