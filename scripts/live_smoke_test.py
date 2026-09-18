"""
Manual smoke test -- NOT part of the automated test suite on purpose
(it lives outside tests/, so pytest never collects it). Runs the
day-planning scenario against a real Claude API call instead of the
deterministic StubHeuristicLLM, to confirm the AnthropicProvider wiring
actually works end to end.

This costs a small amount of real money and its output is not
deterministic -- that's exactly why PLAN.md's Phase 2 keeps it separate
from CI rather than trying to assert on it automatically.

Requires:
    pip install anthropic
    set ANTHROPIC_API_KEY=sk-...   (or export, on non-Windows shells)

Run:
    python scripts/live_smoke_test.py
"""
from datetime import datetime, timedelta, timezone

from temporal_engine.actions import apply_action
from temporal_engine.anthropic_provider import AnthropicProvider
from temporal_engine.engine import DayTracker, tick
from temporal_engine.providers import TemporalContext
from temporal_engine.scheduler import SimulatedClock, next_wake_time
from temporal_engine.task import Task

UTC = timezone.utc


def main() -> None:
    provider = AnthropicProvider()

    day1 = datetime(2026, 9, 12, tzinfo=UTC)
    dsa = Task.new(
        "DSA", "UTC",
        scheduled_start=day1.replace(hour=14),
        scheduled_end=day1.replace(hour=16),
    )
    project = Task.new(
        "Project work", "UTC",
        scheduled_start=day1.replace(hour=17),
        scheduled_end=day1.replace(hour=19),
    )
    tasks = {dsa.id: dsa, project.id: project}

    clock = SimulatedClock(day1.replace(hour=13, minute=55))
    day_tracker = DayTracker()
    seen_ids: set[str] = set()

    for step in range(10):
        wake = next_wake_time(tasks.values(), clock.now(), day_boundary_tz="UTC")
        clock.set(wake)
        temporal_events = tick(tasks.values(), clock.now(), day_tracker, day_boundary_tz="UTC")

        if not temporal_events:
            continue

        print(f"\n[t={clock.now().isoformat()}] events: {[e.event_type.value for e in temporal_events]}")

        ctx = TemporalContext(now=clock.now(), events=temporal_events, tasks=list(tasks.values()))
        action_calls = provider.decide(ctx)

        for call in action_calls:
            print(f"  Claude proposes: {call.action} on {call.task_id} -- {call.reason!r}")
            # `tasks` is a dict, passed by reference -- mutators.py inserts
            # any newly created task (from reschedule/carry_forward)
            # straight into it, so nothing extra is needed here to track
            # new tasks across iterations.
            outcome_events = apply_action(tasks, call, clock.now(), seen_ids)
            for e in outcome_events:
                if e.event_type.value == "ACTION_PROPOSED":
                    detail = f" ({e.payload['rejection_reason']})" if e.payload.get("rejection_reason") else ""
                    print(f"    -> {e.payload['outcome']}{detail}")
                else:
                    print(f"    -> {e.event_type.value}")

        if any(e.event_type.value == "NEW_DAY" for e in temporal_events):
            break

    print("\nFinal task states:")
    for t in tasks.values():
        print(f"  [{t.status.value:16s}] {t.title}")


if __name__ == "__main__":
    main()
