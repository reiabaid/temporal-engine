"""
Regression tests from the second audit: unbounded input, unbounded output,
and a single failure spreading further than it should.
"""
import asyncio
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace as NS

import pytest

import temporal_engine.mutators as mutators
from temporal_engine.actions import ActionCall
from temporal_engine.agent import Agent, AgentConfig
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.mcp_server import MAX_KEY_LENGTH, _execute, get_temporal_context
from temporal_engine.overlap import find_conflicts
from temporal_engine.providers import TemporalContext
from temporal_engine.provider_common import build_prompt
from temporal_engine.recurrence import Recurrence, build_recurrence, occurrence_dates
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import SimulatedClock
from temporal_engine.storage import Store, append_event, replay
from temporal_engine.task import MAX_TITLE_LENGTH, Task, TaskStatus

UTC = timezone.utc
T0 = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)
_n = iter(range(10**6))


@pytest.fixture
def env(tmp_path):
    clock = SimulatedClock(T0)
    rt = Runtime(tmp_path / "h.db", day_boundary_tz="UTC", clock=clock)
    rt.run_pass()
    e = NS(rt=rt, clock=clock, db=tmp_path / "h.db")
    yield e
    rt.close()


def do(rt, action, task_id=None, **args):
    return rt.execute(ActionCall(f"k{next(_n)}", action, task_id, args))


def make_recurrence(rt, title, **kw):
    args = dict(title=title, display_timezone="UTC", frequency="daily", start_time="10:00", duration_minutes=30)
    args.update(kw)
    events = do(rt, "create_recurrence", **args)
    assert events[0].payload["outcome"] == "applied", events[0].payload
    return next(e.payload["id"] for e in events if e.event_type == EventType.RECURRENCE_CREATED)


def ctx_for(rt):
    return NS(request_context=NS(lifespan_context=rt))


# ------------------------------------------------- one failure must not spread

def test_a_broken_template_cannot_stop_the_scheduler_from_ticking_everything_else(env, monkeypatch):
    rt = env.rt
    innocent = do(rt, "create_task", title="innocent", display_timezone="UTC",
                  scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))[1].task_id
    good = make_recurrence(rt, "good")
    poison = make_recurrence(rt, "poison")
    before = {t.id for t in rt.store.refresh().tasks.values() if t.recurrence_id == poison}

    real, seen = mutators.occurrence_window, {"poison_calls": 0}

    def flaky(recurrence, on):
        if recurrence.title == "poison":
            seen["poison_calls"] += 1
            if seen["poison_calls"] >= 3:            # fail PARTWAY through the template's dates
                raise RuntimeError("template is broken")
        return real(recurrence, on)

    monkeypatch.setattr(mutators, "occurrence_window", flaky)
    env.clock.set(T0 + timedelta(days=9))
    rt.run_pass()                                    # must not raise

    view = rt.store.refresh()
    assert view.tasks[innocent].status == TaskStatus.WINDOW_ENDED       # the unrelated task still ticked
    assert list(rt.health.recurrence_errors) == [poison]                # and the failure is visible
    assert "template is broken" in rt.health.recurrence_errors[poison]
    assert any(t.recurrence_id == good and t.occurrence == "2026-09-21" for t in view.tasks.values())   # other templates carried on
    # atomic: the poison template's half-computed occurrences never reached memory
    assert {t.id for t in view.tasks.values() if t.recurrence_id == poison} == before
    assert {t.id: t.status for t in view.tasks.values()} == {t.id: t.status for t in replay(sqlite3.connect(env.db)).values()}

    monkeypatch.setattr(mutators, "occurrence_window", real)             # repaired: it recovers by itself
    env.clock.set(env.clock.now() + timedelta(minutes=1))
    rt.run_pass()
    assert rt.health.recurrence_errors == {}
    assert any(t.recurrence_id == poison and t.occurrence == "2026-09-21" for t in rt.store.refresh().tasks.values())


def test_a_template_that_cannot_be_generated_is_refused_and_leaves_nothing_behind(env, monkeypatch):
    rt = env.rt
    real = mutators.occurrence_window
    monkeypatch.setattr(mutators, "occurrence_window",
                        lambda rec, on: (_ for _ in ()).throw(RuntimeError("cannot generate")) if rec.title == "doomed" else real(rec, on))
    with pytest.raises(RuntimeError, match="cannot generate"):
        make_recurrence(rt, "doomed")
    assert not rt.store.refresh().recurrences                            # rolled back: not half-created
    replay(sqlite3.connect(env.db))                                      # and the log is still strictly replayable


def test_creating_a_healthy_template_is_not_blocked_by_someone_elses_broken_one(env, monkeypatch):
    rt = env.rt
    make_recurrence(rt, "already-there")
    real = mutators.occurrence_window
    monkeypatch.setattr(mutators, "occurrence_window",
                        lambda rec, on: (_ for _ in ()).throw(RuntimeError("old one broke")) if rec.title == "already-there" else real(rec, on))
    make_recurrence(rt, "fresh")                                         # applies fine
    assert len(rt.store.refresh().recurrences) == 2


# ---------------------------------------------------------- bounds on new input

@pytest.mark.parametrize("first_date", ["0001-01-01", "2010-01-01", "2040-01-01"])
def test_an_absurd_first_date_is_rejected_with_a_reason(env, first_date):
    events = do(env.rt, "create_recurrence", title="x", display_timezone="UTC", frequency="daily",
                start_time="10:00", first_date=first_date)
    assert events[0].payload["outcome"] == "rejected"
    assert "within 5 years" in events[0].payload["rejection_reason"]


def test_a_recent_backfill_is_still_fine(env):
    events = do(env.rt, "create_recurrence", title="x", display_timezone="UTC", frequency="daily",
                start_time="10:00", first_date="2023-01-01")
    assert events[0].payload["outcome"] == "applied"


def test_an_expansion_that_hits_the_safety_cap_raises_instead_of_silently_truncating():
    ancient = build_recurrence(rec_id="r", title="t", display_timezone="UTC", frequency="daily",
                               start_time="10:00", first_date="0001-01-01")
    with pytest.raises(ValueError, match="too many occurrences"):
        occurrence_dates(ancient, date(2026, 9, 12))


def test_limits_added_later_never_make_a_recorded_template_unreadable(env):
    """Replay must accept whatever was legal when it was written: a 600-char
    title and a 2000-era first_date would fail today's creation checks."""
    conn = sqlite3.connect(env.db)
    old = build_recurrence(rec_id="old", title="t" * 600, display_timezone="UTC", frequency="daily",
                           start_time="10:00", first_date="2000-01-01", check_bounds=False)
    append_event(conn, TemporalEvent(EventType.RECURRENCE_CREATED, T0, T0, payload=old.to_payload()))
    conn.close()
    store = Store(str(env.db))
    assert "old" in store.view.recurrences and store.view.quarantined == []
    assert Recurrence.from_payload(old.to_payload()).title == "t" * 600
    store.close()


def test_titles_are_bounded_everywhere_a_task_can_be_made():
    assert Task.new("t" * MAX_TITLE_LENGTH, "UTC").title
    with pytest.raises(ValueError, match="too long"):
        Task.new("t" * (MAX_TITLE_LENGTH + 1), "UTC")
    with pytest.raises(ValueError, match="too long"):
        build_recurrence(rec_id="r", title="t" * (MAX_TITLE_LENGTH + 1), display_timezone="UTC",
                         frequency="daily", start_time="10:00", first_date="2026-09-12")


def test_an_oversized_title_is_rejected_with_a_readable_reason(env):
    events = do(env.rt, "create_task", title="x" * 5_000_000, display_timezone="UTC")
    assert events[0].payload["outcome"] == "rejected"
    assert "too long" in events[0].payload["rejection_reason"]


def test_a_rejected_call_does_not_write_its_oversized_arguments_into_the_log(env):
    """The decision log records what was proposed -- but not without limit."""
    do(env.rt, "create_task", title="x" * 5_000_000, display_timezone="UTC")
    events, _ = env.rt.store.events(limit=10)
    stored = max(len(e.payload.__repr__()) for e in events)
    assert stored < 20_000


# ------------------------------------------------ bounds at the MCP boundary

def test_an_overlong_idempotency_key_is_refused_before_anything_is_recorded(env):
    before = env.rt.store.refresh().last_seq
    with pytest.raises(ValueError, match="idempotency_key is too long"):
        _execute(env.rt, "create_task", None, {"title": "x", "display_timezone": "UTC"}, "", "k" * (MAX_KEY_LENGTH + 1))
    assert env.rt.store.refresh().last_seq == before


def test_an_overlong_reason_is_clipped(env):
    tid = do(env.rt, "create_task", title="x", display_timezone="UTC")[1].task_id
    _execute(env.rt, "start_task", tid, {}, "r" * 50_000, None)
    events, _ = env.rt.store.events(limit=5)
    reason = [e.payload["action_call"]["reason"] for e in events if e.event_type == EventType.ACTION_PROPOSED][-1]
    assert len(reason) < 1100 and "truncated in the log" in reason      # bounded, and says it was cut


# ---------------------------------------------------------- bounded responses

def test_the_context_lists_at_most_500_tasks_nearest_to_now_and_says_it_truncated(env):
    rt = env.rt
    for i in range(520):
        do(rt, "create_task", title=f"far-{i}", display_timezone="UTC",
           scheduled_start=T0 + timedelta(days=30 + i), scheduled_end=T0 + timedelta(days=30 + i, hours=1))
    do(rt, "create_task", title="SOON", display_timezone="UTC",
       scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))

    out = asyncio.run(get_temporal_context(ctx_for(rt)))
    assert len(out["tasks"]) == 500 and out["tasks_total"] == 521 and out["tasks_truncated"] is True
    assert out["tasks"][0]["title"] == "SOON"                  # nearest first, so the important ones survive the cut


def test_a_pile_of_overlapping_tasks_cannot_make_one_response_enormous(env):
    rt = env.rt
    for i in range(300):
        do(rt, "create_task", title=f"same-{i}", display_timezone="UTC",
           scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))
    start = time.perf_counter()
    out = asyncio.run(get_temporal_context(ctx_for(rt)))
    assert len(out["conflicts"]) == 50 and out["conflicts_truncated"] is True     # 44,850 pairs exist
    assert time.perf_counter() - start < 3


def test_find_conflicts_stops_at_its_limit_without_building_every_pair():
    tasks = [Task.new(f"t{i}", "UTC", scheduled_start=T0, scheduled_end=T0 + timedelta(hours=1)) for i in range(2000)]
    started = time.perf_counter()
    assert len(find_conflicts(tasks, limit=10)) == 10
    assert time.perf_counter() - started < 1
    assert len(find_conflicts(tasks[:30])) == 30 * 29 // 2                        # unlimited still complete for small inputs


def test_the_agents_prompt_is_bounded_however_many_tasks_exist(env):
    """Every task in the context is sent to a paid API."""
    rt = env.rt

    class Spy:
        contexts = []
        def decide(self, ctx):
            Spy.contexts.append(ctx)
            return []

    for i in range(250):
        do(rt, "create_task", title=f"bulk-{i}", display_timezone="UTC",
           scheduled_start=T0 + timedelta(days=20 + i), scheduled_end=T0 + timedelta(days=20 + i, hours=1))
    agent = Agent(rt, Spy(), AgentConfig(max_context_tasks=40))
    asyncio.run(agent.step())                                                     # initial bookmark
    trigger = do(rt, "create_task", title="trigger", display_timezone="UTC",
                 scheduled_start=env.clock.now() + timedelta(hours=1), scheduled_end=env.clock.now() + timedelta(hours=2))[1].task_id
    env.clock.set(env.clock.now() + timedelta(hours=3))
    rt.run_pass()
    assert asyncio.run(agent.step()) == "decided"

    ctx = Spy.contexts[0]
    assert len(ctx.tasks) == 40 and any(t.id == trigger for t in ctx.tasks)        # the task the event is about is always there
    assert ctx.omitted_tasks == 251 - 40
    assert "211 more unfinished tasks not shown" in build_prompt(ctx)


def test_an_error_message_that_echoes_a_huge_value_is_clipped_too(env):
    tid = do(env.rt, "create_task", title="x", display_timezone="UTC",
             scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))[1].task_id
    events = do(env.rt, "reschedule_task", tid, new_start="z" * 3_000_000, new_end="z")   # "must be an ISO 8601 datetime, got '<3MB>'"
    assert events[0].payload["outcome"] == "rejected"
    assert len(events[0].payload["rejection_reason"]) < 2_000


def test_a_held_action_is_stored_whole_or_not_held_at_all(env):
    """Clipping a hold would corrupt what a later confirm applies."""
    tid = do(env.rt, "create_task", title="x", display_timezone="UTC")[1].task_id
    small = env.rt.execute(ActionCall("small", "cancel_task", tid, {"mode": "drop"}, requires_confirmation=True))
    assert small[0].payload["outcome"] == "pending_confirmation"
    huge = env.rt.execute(ActionCall("huge", "cancel_task", tid, {"mode": "d" * 100_000}, requires_confirmation=True))
    assert huge[0].payload["outcome"] == "rejected" and "too large to hold" in huge[0].payload["rejection_reason"]
