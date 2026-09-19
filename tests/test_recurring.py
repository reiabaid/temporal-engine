import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from temporal_engine.actions import ActionCall
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import RealClock, SimulatedClock
from temporal_engine.storage import Store, append_event, replay
from temporal_engine.task import TaskStatus

UTC = timezone.utc
T0 = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)          # a Saturday


@pytest.fixture
def env(tmp_path):
    clock = SimulatedClock(T0)
    made = []

    def make():
        rt = Runtime(tmp_path / "r.db", day_boundary_tz="UTC", clock=clock)
        made.append(rt)
        return rt

    class Env:
        pass

    e = Env()
    e.db, e.clock, e.make = tmp_path / "r.db", clock, make
    e.rt = make()
    e.rt.run_pass()
    yield e
    for rt in made:
        rt.close()


_n = iter(range(10_000))


def create_rec(rt, key=None, **overrides):
    args = {"title": "standup", "display_timezone": "UTC", "frequency": "daily",
            "start_time": "10:00", "duration_minutes": 30}
    args.update(overrides)
    return rt.execute(ActionCall(key or f"r{next(_n)}", "create_recurrence", None, args))


def instances(rt, rec_id=None):
    tasks = rt.store.refresh().tasks.values()
    return sorted((t for t in tasks if t.recurrence_id and (rec_id is None or t.recurrence_id == rec_id)),
                  key=lambda t: (t.occurrence, t.scheduled_start))


def live(rt, rec_id=None):
    return [t for t in instances(rt, rec_id) if not t.is_terminal()]


def rec_id_of(events):
    return next(e.payload["id"] for e in events if e.event_type == EventType.RECURRENCE_CREATED)


# ------------------------------------------------------------- generation

def test_creating_a_recurrence_generates_the_next_week_immediately_and_atomically(env):
    events = create_rec(env.rt)
    assert events[0].payload["outcome"] == "applied"
    assert events[1].event_type == EventType.RECURRENCE_CREATED       # the template first...
    made = [e for e in events if e.event_type == EventType.TASK_CREATED]
    assert len(made) == 8                                             # ...then 12th..19th inclusive
    tasks = instances(env.rt)
    assert [t.occurrence for t in tasks] == [f"2026-09-{d}" for d in range(12, 20)]
    assert all(t.status == TaskStatus.SCHEDULED for t in tasks)
    assert tasks[0].scheduled_start == datetime(2026, 9, 12, 10, 0, tzinfo=UTC)
    assert tasks[0].scheduled_end - tasks[0].scheduled_start == timedelta(minutes=30)


def test_weekly_on_weekdays_only_produces_those_days(env):
    events = create_rec(env.rt, frequency="weekly", weekdays=["MO", "FR"])
    assert [t.occurrence for t in instances(env.rt, rec_id_of(events))] == ["2026-09-14", "2026-09-18"]


def test_generation_is_idempotent_across_passes_and_processes(env):
    events = create_rec(env.rt)
    rid = rec_id_of(events)
    other = env.make()
    for rt in (env.rt, other, env.rt, other):
        env.clock.set(env.clock.now() + timedelta(seconds=1))
        rt.run_pass()
    assert len(instances(env.rt, rid)) == 8


def test_the_window_slides_as_time_passes(env):
    rid = rec_id_of(create_rec(env.rt))
    env.clock.set(T0 + timedelta(days=1))                              # 13th, 09:00
    env.rt.run_pass()
    assert instances(env.rt, rid)[-1].occurrence == "2026-09-20"       # the 20th has come into range
    assert len(instances(env.rt, rid)) == 9
    assert instances(env.rt, rid)[0].status == TaskStatus.WINDOW_ENDED  # yesterday's is over


def test_a_retried_create_with_the_same_key_makes_one_series(env):
    first = create_rec(env.rt, key="same")
    second = create_rec(env.rt, key="same")
    assert first[0].payload["outcome"] == "applied" and second[0].payload["outcome"] == "superseded"
    assert len(env.rt.store.refresh().recurrences) == 1
    assert len(instances(env.rt)) == 8


def test_an_invalid_rule_is_rejected_and_creates_nothing(env):
    events = create_rec(env.rt, frequency="hourly")
    assert events[0].payload["outcome"] == "rejected"
    assert "frequency must be one of" in events[0].payload["rejection_reason"]
    assert len(events) == 1 and not env.rt.store.refresh().recurrences and not instances(env.rt)


# ------------------------------------------- occurrences are independent tasks

def test_a_skipped_occurrence_is_never_quietly_recreated(env):
    rid = rec_id_of(create_rec(env.rt))
    victim = next(t for t in instances(env.rt, rid) if t.occurrence == "2026-09-14")
    env.rt.execute(ActionCall("skip", "cancel_task", victim.id, {"mode": "cancel"}))
    for step in range(3):
        env.clock.set(env.clock.now() + timedelta(minutes=step + 1))
        env.rt.run_pass()
    on_14th = [t for t in instances(env.rt, rid) if t.occurrence == "2026-09-14"]
    assert len(on_14th) == 1 and on_14th[0].status == TaskStatus.CANCELLED
    assert len(instances(env.rt, rid)) == 8


def test_a_moved_occurrence_stays_that_occurrence_and_is_not_duplicated(env):
    rid = rec_id_of(create_rec(env.rt))
    target = next(t for t in instances(env.rt, rid) if t.occurrence == "2026-09-15")
    env.rt.execute(ActionCall("move", "reschedule_task", target.id, {
        "new_start": datetime(2026, 9, 15, 16, 0, tzinfo=UTC), "new_end": datetime(2026, 9, 15, 16, 30, tzinfo=UTC)}))
    env.clock.set(env.clock.now() + timedelta(minutes=5))
    env.rt.run_pass()
    on_15th = [t for t in instances(env.rt, rid) if t.occurrence == "2026-09-15"]
    assert len(on_15th) == 2                                               # the superseded one and its replacement
    (moved,) = [t for t in on_15th if not t.is_terminal()]
    assert moved.scheduled_start == datetime(2026, 9, 15, 16, 0, tzinfo=UTC)
    assert moved.recurrence_id == rid


def test_completing_one_occurrence_does_not_touch_the_others(env):
    rid = rec_id_of(create_rec(env.rt))
    first = instances(env.rt, rid)[0]
    env.rt.execute(ActionCall("done", "complete_task", first.id))
    statuses = [t.status for t in instances(env.rt, rid)]
    assert statuses[0] == TaskStatus.COMPLETED and set(statuses[1:]) == {TaskStatus.SCHEDULED}


def test_ending_a_series_stops_generation_but_leaves_existing_occurrences(env):
    rid = rec_id_of(create_rec(env.rt))
    ended = env.rt.execute(ActionCall("end", "end_recurrence", None, {"recurrence_id": rid}))
    assert ended[0].payload["outcome"] == "applied"
    env.clock.set(T0 + timedelta(days=10))
    env.rt.run_pass()
    assert len(instances(env.rt, rid)) == 8                                 # nothing new
    assert all(t.status == TaskStatus.WINDOW_ENDED for t in instances(env.rt, rid))   # existing ones ran their course
    again = env.rt.execute(ActionCall("end2", "end_recurrence", None, {"recurrence_id": rid}))
    assert again[0].payload["outcome"] == "rejected"
    missing = env.rt.execute(ActionCall("end3", "end_recurrence", None, {"recurrence_id": "nope"}))
    assert "unknown recurrence_id" in missing[0].payload["rejection_reason"]


# ------------------------------------------------------------------ downtime

def test_a_long_gap_does_not_flood_and_the_omission_is_recorded_once(env):
    rid = rec_id_of(create_rec(env.rt))
    env.rt.close()

    env.clock.set(datetime(2026, 10, 2, 9, 0, tzinfo=UTC))                   # 20 days later, nothing was running
    back = env.make()
    back.run_pass()

    dates = [t.occurrence for t in instances(back, rid)]
    assert "2026-09-25" not in dates and "2026-09-30" not in dates            # too old to matter: not created
    assert "2026-10-01" in dates                                              # within 24h: created...
    assert next(t for t in instances(back, rid) if t.occurrence == "2026-10-01").status == TaskStatus.WINDOW_ENDED   # ...and already missed
    gaps = [e for e in back.store.events(limit=500)[0] if e.event_type == EventType.RECURRENCE_GAP]
    assert len(gaps) == 1 and gaps[0].payload["skipped"] == 11 and gaps[0].payload["through_date"] == "2026-09-30"

    env.clock.set(env.clock.now() + timedelta(seconds=30))
    back.run_pass()
    assert len([e for e in back.store.events(limit=500)[0] if e.event_type == EventType.RECURRENCE_GAP]) == 1


def test_a_first_date_in_the_past_records_the_omitted_history(env):
    events = create_rec(env.rt, first_date="2026-09-01")
    assert [e.payload["skipped"] for e in events if e.event_type == EventType.RECURRENCE_GAP] == [10]   # 1st..10th
    assert instances(env.rt)[0].occurrence == "2026-09-11"                    # 11th 10:00 is within 24h of now


# --------------------------------------------------------------- persistence

def test_a_fresh_process_rebuilds_templates_from_the_log(env):
    rid = rec_id_of(create_rec(env.rt))
    env.rt.execute(ActionCall("end", "end_recurrence", None, {"recurrence_id": rid}))
    fresh = env.make()
    assert fresh.store.view.recurrences[rid].ended is True
    assert len(instances(fresh, rid)) == 8


def test_the_live_view_equals_a_replay_including_occurrence_identity(env):
    rid = rec_id_of(create_rec(env.rt, frequency="weekly", weekdays=["MO", "WE", "FR"]))
    victim = instances(env.rt, rid)[0]
    env.rt.execute(ActionCall("move", "reschedule_task", victim.id, {
        "new_start": T0 + timedelta(days=3), "new_end": T0 + timedelta(days=3, hours=1)}))
    for hours in (30, 80):
        env.clock.set(T0 + timedelta(hours=hours))
        env.rt.run_pass()

    def shape(tasks):
        return {t.id: (t.status, t.recurrence_id, t.occurrence, t.carried_from, t.scheduled_start, t.scheduled_end)
                for t in tasks}

    assert shape(env.rt.store.view.tasks.values()) == shape(replay(sqlite3.connect(env.db)).values())


def test_an_end_event_for_an_unknown_template_is_quarantined_not_fatal(env):
    conn = sqlite3.connect(env.db)
    append_event(conn, TemporalEvent(EventType.RECURRENCE_ENDED, T0, T0, payload={"recurrence_id": "ghost"}))
    conn.close()
    store = Store(str(env.db))
    assert len(store.view.quarantined) == 1
    store.close()


# ------------------------------------------------------------------ DST

def test_occurrences_keep_their_wall_clock_time_across_a_dst_change(tmp_path):
    clock = SimulatedClock(datetime(2026, 3, 5, 12, 0, tzinfo=UTC))
    rt = Runtime(tmp_path / "d.db", day_boundary_tz="America/New_York", clock=clock)
    try:
        rt.run_pass()
        events = create_rec(rt, display_timezone="America/New_York", start_time="09:00", first_date="2026-03-05")
        by_day = {t.occurrence: t.scheduled_start.astimezone(UTC) for t in instances(rt, rec_id_of(events))}
        assert by_day["2026-03-07"] == datetime(2026, 3, 7, 14, 0, tzinfo=UTC)     # 09:00 EST
        assert by_day["2026-03-09"] == datetime(2026, 3, 9, 13, 0, tzinfo=UTC)     # 09:00 EDT: the instant moved, the wall clock did not
    finally:
        rt.close()


def test_a_sixty_minute_occurrence_across_the_fall_back_hour_is_valid_and_ticks_exactly(tmp_path):
    """01:30 EDT + 60 real minutes = 01:30 EST. Two datetimes sharing one
    ZoneInfo would compare EQUAL by wall clock, fail 'end must be after
    start', and poison every scheduler pass. And the engine must judge the
    repeated hour by real instants, not wall-clock digits."""
    clock = SimulatedClock(datetime(2026, 10, 31, 12, 0, tzinfo=UTC))
    rt = Runtime(tmp_path / "f.db", day_boundary_tz="America/New_York", clock=clock)
    try:
        rt.run_pass()
        events = create_rec(rt, display_timezone="America/New_York", start_time="01:30",
                            duration_minutes=60, first_date="2026-11-01")
        assert events[0].payload["outcome"] == "applied", events[0].payload
        (task,) = [t for t in instances(rt, rec_id_of(events)) if t.occurrence == "2026-11-01"]
        assert task.scheduled_end - task.scheduled_start == timedelta(hours=1)
        assert task.scheduled_start.astimezone(UTC) == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)

        for at, expected in [
            (datetime(2026, 11, 1, 5, 29, tzinfo=UTC), TaskStatus.SCHEDULED),
            (datetime(2026, 11, 1, 6, 15, tzinfo=UTC), TaskStatus.ACTIVE),          # 01:15 EST, second pass through 01:xx
            (datetime(2026, 11, 1, 6, 31, tzinfo=UTC), TaskStatus.WINDOW_ENDED),
        ]:
            clock.set(at)
            rt.run_pass()
            assert rt.store.view.tasks[task.id].status == expected, at
    finally:
        rt.close()


def test_the_real_clock_reports_an_unambiguous_utc_instant():
    now = RealClock("America/New_York").now()
    assert now.tzinfo is timezone.utc and now.utcoffset() == timedelta(0)
