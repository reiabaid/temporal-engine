"""
Acceptance test: the day-planning story from the original brief, end to end,
with real components and only the clock simulated.

  * a 2pm-4pm DSA session; the user never marks it done
  * at 4pm the engine (not a model) notices, raises TASK_WINDOW_ENDED, and the
    agent is woken once to decide what it means
  * at midnight NEW_DAY: yesterday is archived as history, unfinished work is
    carried forward, a recurring standup is materialised, a deadline is breached
  * the whole day costs a handful of model calls -- nothing polls
"""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS
from zoneinfo import ZoneInfo

import pytest

from temporal_engine.actions import ActionCall
from temporal_engine.agent import Agent, AgentConfig
from temporal_engine.events import EventType
from temporal_engine.providers import StubHeuristicLLM
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import SimulatedClock, next_wake_time
from temporal_engine.task import TaskStatus

IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc


def ist(day, hour, minute=0):
    return datetime(2026, 9, day, hour, minute, tzinfo=IST).astimezone(UTC)


class CountingProvider:
    """The reference policy, with a meter on it."""

    def __init__(self):
        self.inner = StubHeuristicLLM()
        self.calls = []

    def decide(self, ctx):
        self.calls.append([e.event_type for e in ctx.events])
        return self.inner.decide(ctx)


def ok(rt, action, task_id=None, n=iter(range(10**6)), **args):
    return rt.execute(ActionCall(f"acc{next(n)}", action, task_id, args))


@pytest.fixture
def day(tmp_path):
    clock = SimulatedClock(ist(12, 9))
    rt = Runtime(tmp_path / "day.db", day_boundary_tz="Asia/Kolkata", clock=clock)
    provider = CountingProvider()
    agent = Agent(rt, provider, AgentConfig(min_interval_seconds=0))
    passes = NS(n=0)

    def advance_to(target):
        """What the scheduler does: jump straight to the next boundary, never in steps."""
        clock.set(target)
        rt.run_pass()
        passes.n += 1
        asyncio.run(agent.step())

    rt.run_pass()
    asyncio.run(agent.step())
    yield NS(rt=rt, clock=clock, provider=provider, advance_to=advance_to, passes=passes)
    rt.close()


def events_of(rt, *types):
    return [e for e in rt.store.events(limit=1000)[0] if e.event_type in types]


def test_the_days_story_from_2pm_to_the_next_morning(day):
    rt = day.rt
    dsa = ok(rt, "create_task", title="DSA practice", display_timezone="Asia/Kolkata",
             scheduled_start=ist(12, 14), scheduled_end=ist(12, 16))[1].task_id
    report = ok(rt, "create_task", title="Send report", display_timezone="Asia/Kolkata",
                deadline=ist(12, 23))[1].task_id
    ok(rt, "create_recurrence", title="Standup", display_timezone="Asia/Kolkata",
       frequency="daily", start_time="09:30", duration_minutes=15)

    # ---- 2pm: the engine starts the task by itself
    day.advance_to(ist(12, 14))
    assert rt.store.refresh().tasks[dsa].status == TaskStatus.ACTIVE

    # ---- 4pm: the window ends with no completion; a fact, then one decision
    day.advance_to(ist(12, 16))
    ended = [e for e in events_of(rt, EventType.TASK_WINDOW_ENDED) if e.task_id == dsa]
    assert len(ended) == 1 and ended[0].occurred_at == ist(12, 16)
    decisions = [e.payload for e in events_of(rt, EventType.ACTION_PROPOSED)
                 if e.payload["action_call"]["idempotency_key"].startswith("agent:")]
    assert decisions and all(d["outcome"] in ("applied", "pending_confirmation") for d in decisions)
    assert any(ended[0].id in d["action_call"]["caused_by"] for d in decisions)          # traceable to its trigger

    # ---- 11pm: the deadline passes with the report unsent
    day.advance_to(ist(12, 23))
    assert rt.store.refresh().tasks[report].status == TaskStatus.OVERDUE

    # ---- midnight: NEW_DAY is a fact stamped at local midnight, in the user's timezone
    day.advance_to(ist(13, 0))
    (new_day,) = events_of(rt, EventType.NEW_DAY)
    assert new_day.occurred_at == ist(13, 0)
    assert new_day.payload["new_date"] == "2026-09-13" and new_day.payload["timezone"] == "Asia/Kolkata"

    # ---- step 1-2: yesterday is history, reproducible exactly as it was
    yesterday_5pm = rt.state_as_of(ist(12, 17))
    assert yesterday_5pm[report].status == TaskStatus.CREATED                            # deadline not yet reached
    assert yesterday_5pm[dsa].status == TaskStatus.RESCHEDULED                           # the decision made at 4pm
    assert rt.state_as_of(ist(12, 15))[dsa].status == TaskStatus.ACTIVE                 # "what was I doing at 3pm?"
    assert rt.state_as_of(ist(12, 23, 30))[report].status == TaskStatus.OVERDUE

    # ---- step 3: unfinished work continues today (a lineage, never a silent loss)
    view = rt.store.refresh()
    lineage = [t for t in view.tasks.values() if t.title == "DSA practice"]
    live = [t for t in lineage if not t.is_terminal()]
    assert len(live) == 1                                                                # exactly one open continuation
    assert live[0].id != dsa and live[0].scheduled_start >= ist(12, 16)                  # later than the missed window
    assert all(t.status == TaskStatus.RESCHEDULED for t in lineage if t is not live[0])  # the rest is history, not deleted

    # ---- step 6: every day has exactly one live standup, even though the agent moved some
    # (a rescheduled occurrence keeps its identity, so it can never be generated twice)
    live_standups = sorted(t.occurrence for t in view.tasks.values()
                           if t.title == "Standup" and not t.is_terminal())
    assert live_standups[:3] == ["2026-09-12", "2026-09-13", "2026-09-14"]
    assert len(live_standups) == len(set(live_standups))

    # ---- the morning: standup starts on its own, nobody asked a model
    day.advance_to(ist(13, 9, 30))
    today_standup = next(t for t in rt.store.refresh().tasks.values()
                         if t.title == "Standup" and t.occurrence == "2026-09-13" and not t.is_terminal())
    assert today_standup.status == TaskStatus.ACTIVE
    assert not any(EventType.TASK_STARTED in call for call in day.provider.calls)        # a start is not a reason to think

    # ---- the cost of the whole day: model calls only where a decision was needed
    assert 1 <= len(day.provider.calls) <= 6   # bounded by the per-lineage move limit
    assert all(any(t in (EventType.TASK_WINDOW_ENDED, EventType.TASK_DEADLINE_BREACHED, EventType.NEW_DAY) for t in call)
              for call in day.provider.calls)


def test_the_wake_schedule_is_boundary_driven_not_a_poll(day):
    """From 09:00 to the next morning the scheduler needs only as many wake-ups as
    there are boundaries -- not one per minute."""
    rt = day.rt
    ok(rt, "create_task", title="DSA", display_timezone="Asia/Kolkata",
       scheduled_start=ist(12, 14), scheduled_end=ist(12, 16))
    now, wakes = day.clock.now(), 0
    end = ist(13, 8)
    while now < end and wakes < 200:
        wake = next_wake_time(rt.store.refresh().tasks.values(), now, "Asia/Kolkata")
        assert wake > now
        wake = min(wake, end)
        day.advance_to(wake)
        now, wakes = wake, wakes + 1
    assert now == end
    assert wakes <= 12                                    # ~23 hours: minutes-level polling would be ~1400
    assert len(day.provider.calls) <= 6
