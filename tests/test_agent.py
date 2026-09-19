import asyncio
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import pytest

from temporal_engine.actions import ActionCall
from temporal_engine.agent import Agent, AgentConfig, provider_from_env
from temporal_engine.events import EventType
from temporal_engine.providers import StubHeuristicLLM
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import SimulatedClock
from temporal_engine.storage import replay
from temporal_engine.task import TaskStatus

UTC = timezone.utc
T0 = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)
CFG = AgentConfig(min_interval_seconds=30.0, backoff_base_seconds=5.0)


@pytest.fixture
def env(tmp_path):
    clock = SimulatedClock(T0)
    made = []

    def make():
        rt = Runtime(tmp_path / "a.db", day_boundary_tz="UTC", clock=clock)
        made.append(rt)
        return rt

    class Env:
        pass

    e = Env()
    e.db, e.clock, e.make = tmp_path / "a.db", clock, make
    e.rt = make()
    e.rt.run_pass()          # becomes the scheduler
    yield e
    for rt in made:
        rt.close()


class Spy:
    """A provider that records how it was called and answers with `fn(ctx)`."""

    def __init__(self, fn=None):
        self.fn, self.contexts, self.threads = fn, [], []

    def decide(self, ctx):
        self.contexts.append(ctx)
        self.threads.append(threading.get_ident())
        return self.fn(ctx) if self.fn else []


_n = iter(range(10_000))


def run(coro):
    return asyncio.run(coro)


def create(rt, title="T", start=None, end=None):
    events = rt.execute(ActionCall(f"c{next(_n)}", "create_task", None, {
        "title": title, "display_timezone": "UTC", "scheduled_start": start, "scheduled_end": end,
    }))
    assert events[0].payload["outcome"] == "applied"
    return events[1].task_id


def window_ends(env, rt, hours=3, title="T"):
    """Create a task whose window has ended by the time this returns."""
    tid = create(rt, title, start=env.clock.now() + timedelta(hours=1), end=env.clock.now() + timedelta(hours=2))
    env.clock.set(env.clock.now() + timedelta(hours=hours))
    rt.run_pass()
    return tid


def agent_decisions(rt):
    events, _ = rt.store.events(limit=500)
    return [e.payload for e in events
            if e.event_type == EventType.ACTION_PROPOSED
            and e.payload["action_call"]["idempotency_key"].startswith("agent:")]


# -------------------------------------------------------------- the happy path

def test_a_window_ending_makes_the_agent_reschedule_and_checkpoint(env):
    agent = Agent(env.rt, StubHeuristicLLM(), CFG)
    assert run(agent.step()) == "idle"                        # empty log: nothing to do

    tid = window_ends(env, env.rt)
    assert run(agent.step()) == "decided"

    tasks = env.rt.store.refresh().tasks
    assert tasks[tid].status == TaskStatus.RESCHEDULED
    assert [t for t in tasks.values() if t.carried_from == tid and not t.is_terminal()]
    assert env.rt.store.view.agent_cursor is not None
    assert run(agent.step()) in ("idle", "waiting")            # and it does not act twice
    replay(sqlite3.connect(env.db))                            # checkpoints don't break strict replay


def test_enabling_the_agent_on_an_existing_log_does_not_act_on_history(env):
    """Turning it on must not go and 'fix' everything that ever happened."""
    window_ends(env, env.rt)                                   # a trigger exists BEFORE the agent has ever run
    spy = Spy()
    agent = Agent(env.rt, spy, CFG)

    assert run(agent.step()) == "idle"
    assert spy.contexts == []
    assert env.rt.store.view.agent_cursor == env.rt.store.view.last_seq - 1   # bookmark = "now" (the checkpoint itself follows)


# ------------------------------------------------------- not waking the model

def test_a_trigger_for_a_task_the_user_already_finished_never_reaches_the_model(env):
    spy = Spy()
    agent = Agent(env.rt, spy, CFG)
    run(agent.step())                                          # initial bookmark

    tid = window_ends(env, env.rt)
    env.rt.execute(ActionCall("done", "complete_task", tid))    # user finishes it before the agent looks
    assert run(agent.step()) == "idle"
    assert spy.contexts == []                                   # no paid call for a non-decision
    assert env.rt.store.view.agent_cursor >= 1                  # but the trigger was consumed


@pytest.mark.parametrize("has_unfinished_task, expect_calls", [(False, 0), (True, 1)])
def test_a_new_day_only_wakes_the_model_if_something_is_unfinished(env, has_unfinished_task, expect_calls):
    spy = Spy()
    agent = Agent(env.rt, spy, CFG)
    if has_unfinished_task:
        create(env.rt, "later", start=T0 + timedelta(days=5), end=T0 + timedelta(days=5, hours=1))
    run(agent.step())
    env.clock.set(T0 + timedelta(days=1, hours=1))
    env.rt.run_pass()                                           # NEW_DAY
    run(agent.step())
    assert len(spy.contexts) == expect_calls


# --------------------------------------------------------------- at most once

def test_re_deciding_after_a_crash_cannot_apply_anything_twice(env):
    """The gap noted in SPEC.md: idempotency covered re-*applying*, not
    re-*deciding*. A crash after applying but before the checkpoint means the
    same batch is decided again, possibly by a different-minded model; the
    keys are derived from the triggering events so the second attempt lands
    on the same slots and is recognised."""
    def start_it(ctx):
        # a real vendor issues a fresh tool-call id every time it is asked
        return [ActionCall(f"vendor-call-{next(_n)}", "start_task", ctx.events[0].task_id)]

    agent = Agent(env.rt, Spy(start_it), CFG)
    run(agent.step())
    tid = window_ends(env, env.rt)

    real_checkpoint, crashed = agent._checkpoint, {"done": False}

    def crash_once(*a, **k):
        if not crashed["done"]:
            crashed["done"] = True
            raise RuntimeError("process died after applying, before checkpointing")
        return real_checkpoint(*a, **k)

    agent._checkpoint = crash_once
    with pytest.raises(RuntimeError):
        run(agent.step())
    assert env.rt.store.refresh().tasks[tid].actual_start is not None   # the action DID apply

    assert run(agent.step()) == "decided"                                # re-decides the same batch
    outcomes = [d["outcome"] for d in agent_decisions(env.rt)]
    assert outcomes == ["applied", "superseded"]                         # ...and nothing ran twice


# ---------------------------------------------------------------- provider errors

def test_a_failing_provider_backs_off_without_losing_the_trigger(env):
    attempts = {"n": 0}

    def flaky(ctx):
        attempts["n"] += 1
        if attempts["n"] <= 2:
            raise ConnectionError("API unreachable")
        return []

    agent = Agent(env.rt, Spy(flaky), CFG)
    run(agent.step())
    window_ends(env, env.rt)
    cursor_before = env.rt.store.refresh().agent_cursor

    assert run(agent.step()) == "error"
    assert "API unreachable" in agent.status()["last_error"]
    assert env.rt.store.view.agent_cursor == cursor_before        # trigger NOT consumed
    assert run(agent.step()) == "waiting"                          # and no hammering of a failing API

    env.clock.set(env.clock.now() + timedelta(seconds=11))         # past the first backoff (5 * 2)
    assert run(agent.step()) == "error"
    env.clock.set(env.clock.now() + timedelta(seconds=21))         # past the longer second backoff
    assert run(agent.step()) == "decided"
    assert agent.status()["last_error"] is None
    assert env.rt.store.view.agent_cursor > cursor_before          # now it is consumed


# ------------------------------------------------------------------ who runs it

def test_only_the_scheduler_process_runs_the_agent(env):
    other = env.make()
    other.run_pass()                                               # cannot get the lock
    assert other.health.is_scheduler is False
    spy = Spy()
    assert run(Agent(other, spy, CFG).step()) == "not_scheduler"
    assert spy.contexts == []


# ------------------------------------------------------------ threads and copies

def test_the_provider_runs_off_the_loop_thread_on_a_private_copy(env):
    """API calls block for seconds, so the provider runs on a worker thread --
    but it must not be able to touch live engine state from there."""
    def vandal(ctx):
        for task in ctx.tasks:
            task.status = TaskStatus.COMPLETED     # mutate what it was handed
        ctx.events.clear()
        return []

    spy = Spy(vandal)
    agent = Agent(env.rt, spy, CFG)
    run(agent.step())
    tid = window_ends(env, env.rt)

    async def go():
        loop_thread = threading.get_ident()
        await agent.step()
        return loop_thread

    loop_thread = run(go())
    assert spy.threads and spy.threads[0] != loop_thread
    assert env.rt.store.refresh().tasks[tid].status == TaskStatus.WINDOW_ENDED   # untouched


# --------------------------------------------------------- bounds and the hold

def test_a_model_proposed_cancel_is_held_until_a_human_confirms(env):
    def cancel_it(ctx):
        return [ActionCall("x", "cancel_task", ctx.events[0].task_id, {"mode": "drop"},
                           reason="looks abandoned", requires_confirmation=True)]

    agent = Agent(env.rt, Spy(cancel_it), CFG)
    run(agent.step())
    tid = window_ends(env, env.rt)
    assert run(agent.step()) == "decided"

    view = env.rt.store.refresh()
    assert view.tasks[tid].status == TaskStatus.WINDOW_ENDED       # nothing was cancelled
    (key,) = view.pending
    assert key.startswith("agent:")
    env.rt.confirm(key)
    assert env.rt.store.view.tasks[tid].status == TaskStatus.DROPPED


def test_a_runaway_model_is_capped_per_batch(env):
    def flood(ctx):
        return [ActionCall("x", "start_task", f"ghost-{i}") for i in range(25)]

    agent = Agent(env.rt, Spy(flood), AgentConfig(max_calls_per_batch=10))
    run(agent.step())
    window_ends(env, env.rt)
    run(agent.step())
    assert len(agent_decisions(env.rt)) == 10


def test_batches_are_rate_limited(env):
    agent = Agent(env.rt, Spy(), CFG)
    run(agent.step())
    window_ends(env, env.rt, title="one")
    assert run(agent.step()) == "decided"

    now = env.clock.now()                                           # a second trigger, seconds later
    create(env.rt, "two", start=now + timedelta(seconds=1), end=now + timedelta(seconds=2))
    env.clock.set(now + timedelta(seconds=3))
    env.rt.run_pass()
    assert run(agent.step()) == "waiting"
    env.clock.set(env.clock.now() + timedelta(seconds=31))
    assert run(agent.step()) == "decided"


# ---------------------------------------------------------------- configuration

def test_provider_from_env():
    assert provider_from_env({}) is None
    assert provider_from_env({"TEMPORAL_ENGINE_PROVIDER": ""}) is None
    assert isinstance(provider_from_env({"TEMPORAL_ENGINE_PROVIDER": "stub"}), StubHeuristicLLM)
    with pytest.raises(ValueError, match="unknown"):
        provider_from_env({"TEMPORAL_ENGINE_PROVIDER": "skynet"})
    with pytest.raises(ValueError, match="MODEL is required"):
        provider_from_env({"TEMPORAL_ENGINE_PROVIDER": "openai"})
