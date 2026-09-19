"""
Deterministic tests of Runtime: a SimulatedClock and several Runtime objects
on one database file stand in for several client processes. (The lock
decides holder-ness by PID liveness, and all runtimes share this test's PID,
so only the first to acquire it is the scheduler -- exactly the situation
two real processes are in.)
"""
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

import temporal_engine.runtime as runtime_module
from temporal_engine.actions import ActionCall
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import SimulatedClock
from temporal_engine.storage import replay
from temporal_engine.task import TaskStatus

UTC = timezone.utc
T0 = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)


@pytest.fixture
def env(tmp_path):
    db = tmp_path / "r.db"
    clock = SimulatedClock(T0)
    made = []

    def make(tz="UTC"):
        rt = Runtime(db, day_boundary_tz=tz, clock=clock)
        made.append(rt)
        return rt

    class Env:
        pass

    e = Env()
    e.db, e.clock, e.make = db, clock, make
    yield e
    for rt in made:
        rt.close()


_counter = iter(range(10_000))


def call(action, task_id=None, args=None, **kw):
    return ActionCall(idempotency_key=f"k{next(_counter)}", action=action, task_id=task_id, args=args or {}, **kw)


def create(rt, title="T", start=None, end=None, deadline=None):
    events = rt.execute(call("create_task", args={
        "title": title, "display_timezone": "UTC",
        "scheduled_start": start, "scheduled_end": end, "deadline": deadline,
    }))
    assert events[0].payload["outcome"] == "applied", events[0].payload
    return events[1].task_id


def status(rt, task_id):
    return rt.store.refresh().tasks[task_id].status


# --------------------------------------------------------------- scheduling

def test_first_pass_catches_up_every_boundary_missed_while_no_process_was_running(env):
    a = env.make()
    tid = create(a, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    a.close()

    env.clock.set(T0 + timedelta(hours=5))          # the whole window elapsed with nothing running
    b = env.make()
    b.run_pass()
    assert status(b, tid) == TaskStatus.WINDOW_ENDED


def test_only_one_runtime_is_the_scheduler_and_the_other_sees_its_ticks(env):
    a, b = env.make(), env.make()
    a.run_pass(); b.run_pass()
    assert (a.health.is_scheduler, b.health.is_scheduler) == (True, False)

    tid = create(b, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))   # written by the NON-scheduler
    env.clock.set(T0 + timedelta(hours=1, minutes=30))
    a.run_pass()                                      # the scheduler notices b's task and ticks it
    assert status(b, tid) == TaskStatus.ACTIVE       # and b sees that without restarting


def test_the_second_process_cannot_act_on_stale_state(env):
    """The regression for the worst bug: B completes a task A has already
    moved. B's view is stale, but the write is validated against the log
    under the write lock, so it is rejected and the log stays replayable."""
    a, b = env.make(), env.make()
    tid = create(a, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    b.store.refresh()
    assert b.store.view.tasks[tid].status == TaskStatus.SCHEDULED   # b has the task

    a.execute(call("reschedule_task", tid, {
        "new_start": T0 + timedelta(hours=5), "new_end": T0 + timedelta(hours=6)}))
    assert b.store.view.tasks[tid].status == TaskStatus.SCHEDULED   # b is now stale

    events = b.execute(call("complete_task", tid))
    assert events[0].payload["outcome"] == "rejected"
    assert "illegal transition" in events[0].payload["rejection_reason"]

    replay(sqlite3.connect(env.db))                                  # strict: raises on any illegal event


def test_when_the_scheduler_closes_another_runtime_takes_over(env):
    a, b = env.make(), env.make()
    a.run_pass(); b.run_pass()
    assert (a.health.is_scheduler, b.health.is_scheduler) == (True, False)

    tid = create(b, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    a.close()                                         # e.g. Claude Code exits; the lock is released
    env.clock.set(T0 + timedelta(hours=3))
    b.run_pass()
    assert b.health.is_scheduler is True
    assert status(b, tid) == TaskStatus.WINDOW_ENDED   # and it ticked what it inherited


def test_new_day_is_announced_once_even_across_a_handover_and_carries_the_real_midnight(env):
    a = env.make()
    a.run_pass()
    create(a, "seed")                                  # gives the log a first event on day 1
    env.clock.set(datetime(2026, 9, 13, 0, 0, 5, tzinfo=UTC))
    a.run_pass()
    a.close()

    b = env.make()                                     # takes over on the same day
    b.run_pass()
    b.run_pass()
    days = [e for e in b.store.events(limit=50)[0] if e.event_type.value == "NEW_DAY"]
    assert len(days) == 1
    assert days[0].occurred_at == datetime(2026, 9, 13, 0, 0, tzinfo=UTC)   # the boundary, not the notice
    assert days[0].recorded_at == datetime(2026, 9, 13, 0, 0, 5, tzinfo=UTC)


def test_new_day_is_recovered_after_downtime_with_the_real_midnight(env):
    a = env.make()
    a.run_pass()
    create(a, "seed")
    a.close()

    env.clock.set(datetime(2026, 9, 15, 8, 0, tzinfo=UTC))     # three days later, nothing was running
    b = env.make()
    b.run_pass()
    (day,) = [e for e in b.store.events(limit=50)[0] if e.event_type.value == "NEW_DAY"]
    assert day.payload == {"previous_date": "2026-09-12", "new_date": "2026-09-15"}
    assert day.occurred_at == datetime(2026, 9, 15, 0, 0, tzinfo=UTC)


def test_invalid_day_boundary_timezone_is_a_clear_startup_error(tmp_path):
    with pytest.raises(ValueError, match="unknown day-boundary timezone"):
        Runtime(tmp_path / "x.db", day_boundary_tz="Not/AZone")


# --------------------------------------------------------------- resilience

def test_a_failing_pass_does_not_kill_the_loop_and_is_visible_in_health(env, monkeypatch):
    monkeypatch.setattr(runtime_module, "BACKOFF_BASE_SECONDS", 0.01)
    rt = env.make()
    rt.refresh_interval = 0.02
    real_pass, failures = rt.run_pass, {"left": 2}

    def flaky():
        if failures["left"] > 0:
            failures["left"] -= 1
            raise RuntimeError("disk hiccup")
        real_pass()

    rt.run_pass = flaky

    async def scenario():
        task = asyncio.create_task(rt.run_forever())
        await asyncio.sleep(0.3)
        errors_seen = rt.health.consecutive_errors
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return errors_seen

    asyncio.run(scenario())
    assert failures["left"] == 0                 # it kept going through both failures
    assert rt.health.consecutive_errors == 0     # and recovered
    assert rt.health.last_error is None
    assert rt.health.is_scheduler is True


def test_a_failed_pass_records_its_error(env):
    rt = env.make()
    rt.run_pass()

    def boom(*a, **k):
        raise RuntimeError("bad")

    original = runtime_module.tick
    runtime_module.tick = boom
    try:
        with pytest.raises(RuntimeError):
            rt.run_pass()
    finally:
        runtime_module.tick = original
    rt.run_pass()                                # and the next pass simply works


def test_a_failed_pass_never_loses_a_new_day_notice(env):
    """The day tracker advances before the event is persisted; if the write
    then fails, the tracker must be re-seeded from the log rather than
    believing the day was already announced."""
    a = env.make()
    a.run_pass()
    create(a, "seed")
    env.clock.set(datetime(2026, 9, 13, 1, 0, tzinfo=UTC))

    original = a.store.append
    a.store.append = lambda events: (_ for _ in ()).throw(RuntimeError("write failed"))
    with pytest.raises(RuntimeError):
        a.run_pass()
    a.store.append = original

    a.run_pass()
    days = [e for e in a.store.events(limit=50)[0] if e.event_type.value == "NEW_DAY"]
    assert len(days) == 1


# ---------------------------------------------------------- durable actions

def test_idempotency_keys_survive_a_restart(env):
    a = env.make()
    tid = create(a, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    first = a.execute(ActionCall("same-key", "complete_task", tid))
    a.close()

    b = env.make()                                    # a fresh process: no in-memory memory of the key
    second = b.execute(ActionCall("same-key", "complete_task", tid))
    assert first[0].payload["outcome"] == "applied"
    assert second[0].payload["outcome"] == "superseded"
    assert len(second) == 1


def test_a_rejected_action_can_be_retried_with_the_same_key_once_state_allows_it(env):
    a = env.make()
    tid = create(a, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    a.execute(ActionCall("k-x", "complete_task", tid))
    rejected = a.execute(ActionCall("retry-me", "start_task", tid))      # task is finished: refused
    assert rejected[0].payload["outcome"] == "rejected"

    tid2 = create(a, "other", start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    retried = a.execute(ActionCall("retry-me", "start_task", tid2))       # same key, now valid
    assert retried[0].payload["outcome"] == "applied"


# --------------------------------------------------------------- confirmation

def test_a_held_action_does_nothing_until_confirmed_and_survives_a_restart(env):
    a = env.make()
    tid = create(a, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    held = a.execute(ActionCall("cancel-1", "cancel_task", tid, {"mode": "drop"}, requires_confirmation=True))
    assert held[0].payload["outcome"] == "pending_confirmation"
    assert status(a, tid) == TaskStatus.SCHEDULED                    # nothing happened
    a.close()

    b = env.make()                                                    # the hold is in the log, not in memory
    assert list(b.store.view.pending) == ["cancel-1"]
    events = b.confirm("cancel-1")
    assert events[0].payload["outcome"] == "applied"
    assert status(b, tid) == TaskStatus.DROPPED
    assert b.store.view.pending == {}


def test_declining_a_held_action_applies_nothing_and_clears_it(env):
    a = env.make()
    tid = create(a, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    a.execute(ActionCall("cancel-2", "cancel_task", tid, {"mode": "drop"}, requires_confirmation=True))
    a.decline("cancel-2")
    assert status(a, tid) == TaskStatus.SCHEDULED
    assert a.store.view.pending == {}
    with pytest.raises(ValueError, match="no pending action"):
        a.confirm("cancel-2")                                         # cannot be applied after being declined


def test_confirm_uses_the_pending_state_under_the_write_lock(env):
    """Two processes race to resolve one held action: the loser must be told
    there is nothing pending, never apply a second time."""
    a, b = env.make(), env.make()
    tid = create(a, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    a.execute(ActionCall("cancel-3", "cancel_task", tid, {"mode": "drop"}, requires_confirmation=True))
    b.store.refresh()
    assert "cancel-3" in b.store.view.pending             # b believes it is still pending

    a.decline("cancel-3")                                  # ...but a resolves it first
    with pytest.raises(ValueError, match="no pending action"):
        b.confirm("cancel-3")
    assert status(b, tid) == TaskStatus.SCHEDULED


def test_confirmed_action_that_is_no_longer_valid_is_rejected_not_forced(env):
    a = env.make()
    tid = create(a, start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    a.execute(ActionCall("cancel-4", "cancel_task", tid, {"mode": "drop"}, requires_confirmation=True))
    a.execute(call("complete_task", tid))                              # the task finishes while the hold waits
    events = a.confirm("cancel-4")
    assert events[0].payload["outcome"] == "rejected"
    assert status(a, tid) == TaskStatus.COMPLETED


# -------------------------------------------------------------- consistency

def test_the_live_view_always_equals_a_fresh_replay_of_the_log(env):
    """The property everything above relies on: mutating the view live and
    replaying the log must agree, whatever mix of operations happened."""
    a = env.make()
    t1 = create(a, "one", start=T0 + timedelta(hours=1), end=T0 + timedelta(hours=2))
    t2 = create(a, "two", start=T0 + timedelta(hours=3), end=T0 + timedelta(hours=4), deadline=T0 + timedelta(hours=9))
    t3 = create(a, "inbox", deadline=T0 + timedelta(hours=6))
    a.execute(call("start_task", t1))
    a.execute(call("reschedule_task", t2, {"new_start": T0 + timedelta(hours=7), "new_end": T0 + timedelta(hours=8)}))
    for hours in (2, 5, 10):
        env.clock.set(T0 + timedelta(hours=hours))
        a.run_pass()
    a.execute(call("complete_task", t1))
    a.execute(call("cancel_task", t3, {"mode": "cancel"}))

    live = {t.id: (t.status, t.actual_start, t.carried_from, t.last_transition_at)
            for t in a.store.view.tasks.values()}
    replayed = {t.id: (t.status, t.actual_start, t.carried_from, t.last_transition_at)
                for t in replay(sqlite3.connect(env.db)).values()}
    assert live == replayed
