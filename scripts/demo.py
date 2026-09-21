"""
A keyless, deterministic replay of one day, so you can see the engine work in about a minute.

    python scripts/demo.py

Nothing here calls a model or the network. The clock is simulated and the engine is the real one. The "agent" is a
tiny scripted policy standing in for an LLM (clearly not one): the demo is about what the ENGINE does -- it notices
every boundary by itself, and the decision-maker is woken only when a decision is needed, never to ask what time
it is. The wake-up times below come from the engine's own `next_wake_time`, not from a hand-written list.
"""
import asyncio
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from temporal_engine.actions import ActionCall
from temporal_engine.agent import Agent, AgentConfig
from temporal_engine.events import EventType
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import SimulatedClock, next_wake_time

TZ = "Asia/Kolkata"
IST = ZoneInfo(TZ)


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, day, hour, minute, tzinfo=IST).astimezone(timezone.utc)


def local(dt: datetime) -> str:
    return dt.astimezone(IST).strftime("%a %d %b %H:%M")


class ScriptedPolicy:
    """Stands in for an LLM. When a one-off task's window ends: try again tonight at 19:00 if there is still time,
    otherwise carry it to 10:00 tomorrow. Missed recurring occurrences are left alone. Counts how often it is woken."""

    def __init__(self):
        self.calls = 0

    def decide(self, ctx):
        self.calls += 1
        by_id = {t.id: t for t in ctx.tasks}
        local_now = ctx.now.astimezone(IST)
        actions = []
        for event in ctx.events:
            task = by_id.get(event.task_id)
            if event.event_type != EventType.TASK_WINDOW_ENDED or task is None or task.recurrence_id:
                continue
            tonight = local_now.replace(hour=19, minute=0, second=0, microsecond=0)
            if local_now < tonight:
                start, name, why = tonight, "reschedule_task", "try again this evening"
            else:
                start, name, why = (local_now + timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0), \
                    "carry_forward_task", "no time left today; carry to tomorrow morning"
            actions.append(ActionCall("scripted", name, task.id, {
                "new_start": start.astimezone(timezone.utc),
                "new_end": (start + timedelta(hours=2)).astimezone(timezone.utc),
            }, reason=why))
        return actions


def main() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as folder:
        clock = SimulatedClock(at(12, 9))
        rt = Runtime(Path(folder) / "demo.db", day_boundary_tz=TZ, clock=clock)
        policy = ScriptedPolicy()
        agent = Agent(rt, policy, AgentConfig(min_interval_seconds=0))
        seen = 0
        try:
            def title_of(task_id):
                task = rt.store.view.tasks.get(task_id)
                return task.title if task else ""

            def show_new_events():
                nonlocal seen
                events, _ = rt.store.events(since_seq=seen, limit=500)
                for e in events:
                    seen = e.seq
                    title = title_of(e.task_id)
                    if e.event_type == EventType.TASK_STARTED:
                        print(f"   engine  started        {title}")
                    elif e.event_type == EventType.TASK_WINDOW_ENDED:
                        print(f"   engine  WINDOW ENDED   {title}   (nobody marked it done)")
                    elif e.event_type == EventType.TASK_DEADLINE_BREACHED:
                        print(f"   engine  DEADLINE MISSED {title}")
                    elif e.event_type == EventType.NEW_DAY:
                        print(f"   engine  NEW DAY        {e.payload['new_date']} ({e.payload['timezone']})")
                    elif (e.event_type == EventType.ACTION_PROPOSED
                          and e.payload["action_call"]["idempotency_key"].startswith("agent:")):
                        call = e.payload["action_call"]
                        print(f"   agent   {call['action']:<19}{title_of(call['task_id'])}: {call['reason']}  [{e.payload['outcome']}]")

            def act(action, **args):
                rt.execute(ActionCall(f"demo-{action}-{args.get('title')}", action, None, args))

            rt.run_pass()
            print("Plan for Sat 12 Sep (Asia/Kolkata):")
            print("  DSA practice  14:00-16:00   (the person never marks it done)")
            print("  Send report   deadline 23:00")
            print("  Standup       daily 09:30")
            act("create_task", title="DSA practice", display_timezone=TZ, scheduled_start=at(12, 14), scheduled_end=at(12, 16))
            act("create_task", title="Send report", display_timezone=TZ, deadline=at(12, 23))
            act("create_recurrence", title="Standup", display_timezone=TZ, frequency="daily", start_time="09:30", duration_minutes=15)

            end, wakes = at(13, 8), 0
            while clock.now() < end:
                target = min(next_wake_time(rt.store.refresh().tasks.values(), clock.now(), TZ), end)
                clock.set(target)
                rt.run_pass()
                asyncio.run(agent.step())
                wakes += 1
                print(f"\n{local(target)}")
                show_new_events()

            print("\nWhat did the plan look like at 5pm on the 12th? (asked the next morning, answered from the log)")
            for t in sorted(rt.state_as_of(at(12, 17)).values(), key=lambda t: t.title):
                if t.title != "Standup":
                    window = f"{local(t.scheduled_start)} -> {local(t.scheduled_end)}" if t.scheduled_end else "no window"
                    print(f"   {t.title:<14}{t.status.value:<12}{window}")

            print("\nOpen now:")
            for t in sorted(rt.store.refresh().tasks.values(), key=lambda t: t.scheduled_start or at(12, 0)):
                if not t.is_terminal() and t.title != "Standup":
                    window = f"{local(t.scheduled_start)} -> {local(t.scheduled_end)}" if t.scheduled_end else "no window"
                    print(f"   {t.title:<14}{t.status.value:<12}{window}")

            span_minutes = int((end - at(12, 9)).total_seconds() // 60)
            print(f"\nThe engine woke {wakes} times in {span_minutes // 60} hours (a once-a-minute poll: {span_minutes}).")
            print(f"The decision-maker was woken {policy.calls} times, each for a window ending, a missed deadline or a new day, never to ask the time.")
        finally:
            rt.close()


if __name__ == "__main__":
    main()
