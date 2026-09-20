"""
Third audit: the whole system against the original brief. Timezone changes,
the agent creating tasks, causality, and historical/future state.
"""
import asyncio
import sqlite3
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace as NS
from zoneinfo import ZoneInfo

import pytest

from temporal_engine.actions import ActionCall
from temporal_engine.agent import Agent, AgentConfig
from temporal_engine.engine import DayTracker
from temporal_engine.events import EventType
from temporal_engine.mcp_server import get_state_as_of
from temporal_engine.provider_common import action_call_from_tool_input
from temporal_engine.providers import StubHeuristicLLM, TemporalContext
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import SimulatedClock
from temporal_engine.storage import replay, view_as_of
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc
T0 = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)
_n = iter(range(10**6))


@pytest.fixture
def env(tmp_path):
    clock = SimulatedClock(T0)
    rt = Runtime(tmp_path / "a.db", day_boundary_tz="UTC", clock=clock)
    rt.run_pass()
    e = NS(rt=rt, clock=clock, db=tmp_path / "a.db")
    yield e
    rt.close()


def do(rt, action, task_id=None, **args):
    return rt.execute(ActionCall(f"k{next(_n)}", action, task_id, args))


def run(coro):
    return asyncio.run(coro)


# ------------------------------------------- NEW_DAY never goes backwards

def test_a_new_day_never_goes_backwards_when_the_timezone_setting_moves_west():
    """The user travels Kolkata -> New York between runs: the local date is
    now EARLIER than the last one announced."""
    tracker = DayTracker()
    tracker.check(datetime(2026, 9, 20, 18, 0, tzinfo=UTC), "Asia/Kolkata")          # 23:30 on the 20th
    (announced,) = tracker.check(datetime(2026, 9, 20, 19, 0, tzinfo=UTC), "Asia/Kolkata")
    assert announced.payload["new_date"] == "2026-09-21"

    assert tracker.check(datetime(2026, 9, 20, 22, 0, tzinfo=UTC), "America/New_York") == []      # 18:00 on the 20th there
    assert tracker.check(datetime(2026, 9, 21, 20, 0, tzinfo=UTC), "America/New_York") == []      # still the 21st there
    (next_day,) = tracker.check(datetime(2026, 9, 22, 5, 0, tzinfo=UTC), "America/New_York")      # the 22nd at last
    assert (next_day.payload["previous_date"], next_day.payload["new_date"]) == ("2026-09-21", "2026-09-22")
    assert next_day.payload["timezone"] == "America/New_York"


def test_a_backwards_system_clock_does_not_announce_an_earlier_day(env):
    do(env.rt, "create_task", title="x", display_timezone="UTC")
    env.clock.set(datetime(2026, 9, 13, 9, 0, tzinfo=UTC))
    env.rt.run_pass()                                                # the 13th is announced
    env.clock._now = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)         # NTP steps the clock back a day (bypassing the sim's guard)
    env.rt.run_pass()
    days = [(e.payload["previous_date"], e.payload["new_date"]) for e in env.rt.store.events(limit=100)[0]
            if e.event_type == EventType.NEW_DAY]
    assert days == [("2026-09-12", "2026-09-13")]


# ----------------------------------------------------------------- causality

def test_a_decision_can_be_traced_from_its_trigger_to_its_effect(env):
    """trigger event -> decision (ACTION_PROPOSED.caused_by) -> resulting events (payload.action_key)."""
    class Mover:
        def decide(self, ctx):
            e = ctx.events[0]
            return [ActionCall("x", "reschedule_task", e.task_id, {
                "new_start": ctx.now + timedelta(hours=1), "new_end": ctx.now + timedelta(hours=2)})]

    agent = Agent(env.rt, Mover(), AgentConfig())
    run(agent.step())
    tid = do(env.rt, "create_task", title="t", display_timezone="UTC",
             scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))[1].task_id
    env.clock.set(T0 + timedelta(hours=3))
    env.rt.run_pass()
    assert run(agent.step()) == "decided"

    events, _ = env.rt.store.events(limit=100)
    trigger = next(e for e in events if e.event_type == EventType.TASK_WINDOW_ENDED and e.task_id == tid)
    decision = next(e for e in events if e.event_type == EventType.ACTION_PROPOSED
                    and e.payload["action_call"]["idempotency_key"].startswith("agent:"))
    assert trigger.id in decision.payload["action_call"]["caused_by"]
    key = decision.payload["action_call"]["idempotency_key"]
    effects = [e for e in events if e.payload.get("action_key") == key]
    assert {e.event_type for e in effects} == {EventType.TASK_RESCHEDULED, EventType.TASK_CREATED}


# ------------------------------------------------- the agent can build a plan

def test_the_tool_call_shape_for_creating_a_task_maps_onto_the_action(env):
    call = action_call_from_tool_input("c1", {
        "action": "create_task", "title": "Review yesterday", "reason": "start of day",
        "new_start": "2026-09-13T09:30:00+00:00", "new_end": "2026-09-13T10:00:00+00:00",
    })
    assert call.action == "create_task" and call.task_id is None
    assert call.args["title"] == "Review yesterday"
    assert call.args["scheduled_start"] == datetime(2026, 9, 13, 9, 30, tzinfo=UTC)
    assert call.requires_confirmation is False                     # creating is not destructive


def _creator(n):
    class Creator:
        def decide(self, ctx):
            return [ActionCall("x", "create_task", None, {
                "title": f"plan item {i}", "scheduled_start": ctx.now + timedelta(hours=1 + i),
                "scheduled_end": ctx.now + timedelta(hours=1 + i, minutes=30)}) for i in range(n)]
    return Creator()


def _trigger(env):
    do(env.rt, "create_task", title="t", display_timezone="UTC",
       scheduled_start=env.clock.now() + timedelta(hours=1), scheduled_end=env.clock.now() + timedelta(hours=2))
    env.clock.set(env.clock.now() + timedelta(hours=3))
    env.rt.run_pass()


def test_the_agent_can_create_tasks_in_the_users_timezone_by_default(env):
    agent = Agent(env.rt, _creator(2), AgentConfig())
    run(agent.step())
    _trigger(env)
    assert run(agent.step()) == "decided"
    plan = [t for t in env.rt.store.refresh().tasks.values() if t.title.startswith("plan item")]
    assert len(plan) == 2 and all(t.display_timezone == "UTC" for t in plan)     # filled from the engine's timezone


def test_the_agents_creations_are_capped_per_day_and_the_refusals_are_logged(env):
    agent = Agent(env.rt, _creator(4), AgentConfig(max_creations_per_day=2))
    run(agent.step())
    _trigger(env)
    run(agent.step())
    decisions = [e.payload for e in env.rt.store.events(limit=200)[0]
                 if e.event_type == EventType.ACTION_PROPOSED and e.payload["action_call"]["idempotency_key"].startswith("agent:")]
    assert [d["outcome"] for d in decisions] == ["applied", "applied", "rejected", "rejected"]
    assert "creation limit" in decisions[2]["rejection_reason"]
    assert len([t for t in env.rt.store.refresh().tasks.values() if t.title.startswith("plan item")]) == 2


def test_the_creation_budget_survives_a_restart_because_it_is_counted_from_the_log(env, tmp_path):
    agent = Agent(env.rt, _creator(2), AgentConfig(max_creations_per_day=2))
    run(agent.step())
    _trigger(env)
    run(agent.step())
    env.rt.close()

    fresh = Runtime(tmp_path / "a.db", day_boundary_tz="UTC", clock=env.clock)
    try:
        fresh.run_pass()
        assert Agent(fresh, _creator(1), AgentConfig(max_creations_per_day=2))._creation_budget(env.clock.now()) == 0
    finally:
        fresh.close()


def test_a_creation_with_no_title_is_a_readable_rejection(env):
    class Untitled:
        def decide(self, ctx):
            return [action_call_from_tool_input("v", {"action": "create_task", "reason": "x"})]
    agent = Agent(env.rt, Untitled(), AgentConfig())
    run(agent.step())
    _trigger(env)
    run(agent.step())
    decision = [e.payload for e in env.rt.store.events(limit=200)[0]
                if e.event_type == EventType.ACTION_PROPOSED and e.payload["action_call"]["idempotency_key"].startswith("agent:")][-1]
    assert decision["outcome"] == "rejected" and "title" in decision["rejection_reason"]


# -------------------------------------------- the reference policy at midnight

KOLKATA = ZoneInfo("Asia/Kolkata")


def _ctx(now_local, tasks):
    return TemporalContext(
        now=now_local.astimezone(UTC),
        events=[NS(event_type=EventType.NEW_DAY, task_id=None, id="nd", occurred_at=now_local)],
        tasks=tasks, timezone="Asia/Kolkata",
    )


def _ended(title, start_h, end_h, day=12, **kw):
    t = Task.new(title, "Asia/Kolkata",
                 scheduled_start=datetime(2026, 9, day, start_h, 0, tzinfo=KOLKATA),
                 scheduled_end=datetime(2026, 9, day, end_h, 0, tzinfo=KOLKATA), **kw)
    t.status = TaskStatus.WINDOW_ENDED
    return t


def test_the_reference_policy_carries_yesterdays_unfinished_work_into_today_from_nine(env):
    a, b = _ended("A", 14, 16), _ended("B", 17, 19)
    today_task = _ended("C", 1, 2, day=13)                     # ended TODAY: not yesterday's business
    actions = StubHeuristicLLM().decide(_ctx(datetime(2026, 9, 13, 0, 5, tzinfo=KOLKATA), [a, b, today_task]))
    assert [x.task_id for x in actions] == [a.id, b.id]        # in the order they were due
    assert all(x.action == "carry_forward_task" for x in actions)
    start_a, start_b = (x.args["new_start"].astimezone(KOLKATA) for x in actions)
    assert (start_a.hour, start_b.hour) == (9, 11)             # stacked from 09:00 local, not overlapping
    assert start_a.date() == date(2026, 9, 13)


def test_the_reference_policy_leaves_alone_what_cannot_move_without_a_new_deadline():
    t = _ended("late", 14, 16, deadline=datetime(2026, 9, 13, 8, 0, tzinfo=KOLKATA))
    assert StubHeuristicLLM().decide(_ctx(datetime(2026, 9, 13, 0, 5, tzinfo=KOLKATA), [t])) == []


def test_a_task_already_handled_by_a_window_ended_action_is_not_moved_twice():
    t = _ended("A", 22, 23, day=12)
    ctx = TemporalContext(
        now=datetime(2026, 9, 13, 0, 5, tzinfo=KOLKATA).astimezone(UTC),
        events=[NS(event_type=EventType.TASK_WINDOW_ENDED, task_id=t.id, id="w", occurred_at=None),
                NS(event_type=EventType.NEW_DAY, task_id=None, id="nd", occurred_at=None)],
        tasks=[t], timezone="Asia/Kolkata",
    )
    assert len(StubHeuristicLLM().decide(ctx)) == 1


# ------------------------------------------------------ historical & future state

def _snapshot(rt):
    return {t.id: t.status for t in rt.store.refresh().tasks.values()}


def test_historical_state_matches_the_live_snapshot_taken_at_that_moment(env):
    """The property that makes state_as_of trustworthy: at any time during a
    run, asking later for the state as of that time must reproduce exactly
    what was true then -- across completions, moves, cancels, recurrence and
    the passage of time."""
    rt, clock = env.rt, env.clock
    snapshots = {}

    def step(hours, action=None):
        clock.set(T0 + timedelta(hours=hours))
        rt.run_pass()
        if action:
            action()
        snapshots[clock.now()] = _snapshot(rt)

    a = do(rt, "create_task", title="A", display_timezone="UTC",
           scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))[1].task_id
    b = do(rt, "create_task", title="B", display_timezone="UTC",
           scheduled_start=T0 + timedelta(hours=3), scheduled_end=T0 + timedelta(hours=4),
           deadline=T0 + timedelta(hours=5))[1].task_id
    do(rt, "create_recurrence", title="R", display_timezone="UTC", frequency="daily", start_time="10:00", duration_minutes=30)
    snapshots[clock.now()] = _snapshot(rt)

    step(1.5)
    step(2.5, lambda: do(rt, "complete_task", a))                       # completed late (window ended at +2h? no: at +2.5h)
    step(3.5, lambda: do(rt, "reschedule_task", b, new_start=T0 + timedelta(hours=4.5), new_end=T0 + timedelta(hours=4.75)))
    step(6)
    step(30, lambda: None)                                              # a day later: recurrence has moved on, B overdue
    step(50)

    for at, expected in snapshots.items():
        replayed = {tid: t.status for tid, t in rt.state_as_of(at).items()}
        assert replayed == expected, f"state as of {at.isoformat()} differs"


def test_as_of_excludes_what_did_not_exist_yet_and_shows_what_was_still_open(env):
    rt, clock = env.rt, env.clock
    before = clock.now()
    tid = do(rt, "create_task", title="later", display_timezone="UTC",
             scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))[1].task_id
    clock.set(T0 + timedelta(hours=3)); rt.run_pass()
    do(rt, "complete_task", tid)

    assert tid not in rt.state_as_of(before - timedelta(minutes=1))                       # not created yet
    assert rt.state_as_of(T0 + timedelta(hours=1, minutes=30))[tid].status == TaskStatus.ACTIVE
    assert rt.state_as_of(T0 + timedelta(hours=2, minutes=30))[tid].status == TaskStatus.WINDOW_ENDED
    assert rt.state_as_of(clock.now())[tid].status == TaskStatus.COMPLETED_LATE


def test_as_of_brings_time_derived_state_forward_even_when_nothing_was_running(env, tmp_path):
    """The app was closed while the window elapsed, so the log has no tick for it. The
    log only says what the system KNEW; state_as_of says what was TRUE."""
    rt, clock = env.rt, env.clock
    tid = do(rt, "create_task", title="t", display_timezone="UTC",
             scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))[1].task_id
    rt.close()
    later = T0 + timedelta(hours=5)
    clock.set(later)
    fresh = Runtime(tmp_path / "a.db", day_boundary_tz="UTC", clock=clock)     # has NOT ticked yet
    try:
        assert view_as_of(fresh.store.conn, later).tasks[tid].status == TaskStatus.SCHEDULED    # what was recorded
        assert fresh.state_as_of(later)[tid].status == TaskStatus.WINDOW_ENDED                  # what was true
    finally:
        fresh.close()


def test_as_of_a_future_instant_is_a_projection_driven_only_by_the_clock(env):
    rt = env.rt
    tid = do(rt, "create_task", title="t", display_timezone="UTC",
             scheduled_start=T0 + timedelta(hours=5), scheduled_end=T0 + timedelta(hours=6), deadline=T0 + timedelta(hours=8))[1].task_id
    assert rt.state_as_of(T0 + timedelta(hours=5, minutes=30))[tid].status == TaskStatus.ACTIVE
    assert rt.state_as_of(T0 + timedelta(hours=7))[tid].status == TaskStatus.WINDOW_ENDED
    assert rt.state_as_of(T0 + timedelta(hours=9))[tid].status == TaskStatus.OVERDUE
    assert rt.store.refresh().tasks[tid].status == TaskStatus.SCHEDULED       # and nothing was persisted or changed


def test_the_tool_validates_its_input_and_flags_projections(env):
    rt = env.rt
    ctx = NS(request_context=NS(lifespan_context=rt))
    tid = do(rt, "create_task", title="t", display_timezone="UTC",
             scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))[1].task_id
    with pytest.raises(ValueError, match="UTC offset"):
        run(get_state_as_of(ctx, "2026-09-12T10:00:00"))
    with pytest.raises(ValueError, match="ISO 8601"):
        run(get_state_as_of(ctx, "yesterday afternoon"))
    env.clock.set(T0 + timedelta(hours=1))
    past = run(get_state_as_of(ctx, (T0 + timedelta(minutes=1)).isoformat()))
    assert past["is_projection"] is False and [t["id"] for t in past["tasks"]] == [tid]
    assert run(get_state_as_of(ctx, (T0 + timedelta(days=3)).isoformat()))["is_projection"] is True
