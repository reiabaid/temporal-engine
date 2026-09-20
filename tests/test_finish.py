"""
The remaining gaps from the audit, closed: estimates and dependencies, database versioning, backup/export/import,
a creation limit for every caller, clock-regression visibility, calendar import, and the command line.
"""
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

import temporal_engine.storage as storage
from temporal_engine import cli
from temporal_engine.actions import ActionCall
from temporal_engine.calendar_import import import_calendar, parse_ics
from temporal_engine.events import EventType, TemporalEvent
from temporal_engine.mcp_server import _task_summary
from temporal_engine.planning import estimate_report, planning_report
from temporal_engine.provider_common import build_prompt
from temporal_engine.providers import TemporalContext
from temporal_engine.runtime import Runtime
from temporal_engine.scheduler import SimulatedClock
from temporal_engine.storage import Store, SchemaTooNew, backup_database, export_jsonl, import_jsonl, init_db, replay
from temporal_engine.task import Task, TaskStatus

UTC = timezone.utc
T0 = datetime(2026, 9, 12, 9, 0, tzinfo=UTC)
_n = iter(range(10**6))


def do(rt, action, task_id=None, **args):
    return rt.execute(ActionCall(f"f{next(_n)}", action, task_id, args))


def created_id(events):
    return next(e.task_id for e in events if e.event_type == EventType.TASK_CREATED)


@pytest.fixture
def env(tmp_path):
    clock = SimulatedClock(T0)
    rt = Runtime(tmp_path / "f.db", day_boundary_tz="UTC", clock=clock)
    rt.run_pass()
    yield NS(rt=rt, clock=clock, path=tmp_path / "f.db", tmp=tmp_path)
    rt.close()


def mk(rt, title="t", **kw):
    return created_id(do(rt, "create_task", title=title, display_timezone="UTC", **kw))


def summary(rt, task_id):
    view = rt.store.refresh()
    return _task_summary(view.tasks[task_id], rt.clock.now(), view.tasks)


# ============================================================ estimates

def test_estimates_are_validated_like_every_other_input(env):
    for bad in (0, -5, 10**9, 1.5, "30", True):
        (decision, *_) = do(env.rt, "create_task", title="x", display_timezone="UTC", estimated_minutes=bad)
        assert decision.payload["outcome"] == "rejected", bad
        assert "estimated_minutes" in decision.payload["rejection_reason"]


def test_an_estimate_survives_replay_and_a_move(env):
    tid = mk(env.rt, estimated_minutes=90, scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=4))
    (_, *_ ) = do(env.rt, "reschedule_task", tid, new_start=T0 + timedelta(hours=5), new_end=T0 + timedelta(hours=8), reason="x")
    tasks = replay(env.rt.store.conn)
    assert sorted(t.estimated_minutes for t in tasks.values()) == [90, 90]        # the successor is the same piece of work


def test_the_estimate_report_states_only_arithmetic():
    now = T0
    short_window = Task.new("a", "UTC", scheduled_start=now + timedelta(hours=1), scheduled_end=now + timedelta(hours=1, minutes=30),
                            estimated_minutes=60)
    assert estimate_report(short_window, now)["estimate_exceeds_window"] is True

    tight = Task.new("b", "UTC", scheduled_start=now + timedelta(hours=1), deadline=now + timedelta(hours=1, minutes=30),
                     estimated_minutes=60)
    report = estimate_report(tight, now)
    assert report["at_risk_of_missing_deadline"] is True and report["projected_finish"].startswith("2026-09-12T11:00")

    fine = Task.new("c", "UTC", scheduled_start=now + timedelta(hours=1), deadline=now + timedelta(hours=5), estimated_minutes=60)
    assert estimate_report(fine, now)["at_risk_of_missing_deadline"] is False

    started = Task.new("d", "UTC", estimated_minutes=60)
    started.actual_start = now - timedelta(minutes=45)
    assert estimate_report(started, now)["remaining_estimate_minutes"] == 15
    assert estimate_report(started, now + timedelta(minutes=30))["over_estimate"] is True

    assert estimate_report(Task.new("e", "UTC"), now) == {}                        # no estimate, nothing invented


def test_the_tool_summary_carries_the_estimate_report(env):
    tid = mk(env.rt, estimated_minutes=120, scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))
    s = summary(env.rt, tid)
    assert s["estimated_minutes"] == 120 and s["estimate_exceeds_window"] is True


# ========================================================== dependencies

def test_a_dependency_must_exist_and_is_bounded(env):
    (d, *_) = do(env.rt, "create_task", title="x", display_timezone="UTC", depends_on=["nope"])
    assert d.payload["outcome"] == "rejected" and "unknown task ids" in d.payload["rejection_reason"]
    (d, *_) = do(env.rt, "create_task", title="x", display_timezone="UTC", depends_on="not-a-list")
    assert d.payload["outcome"] == "rejected"
    (d, *_) = do(env.rt, "create_task", title="x", display_timezone="UTC", depends_on=[f"id{i}" for i in range(21)])
    assert d.payload["outcome"] == "rejected" and "limit" in d.payload["rejection_reason"]


def test_a_task_is_blocked_until_its_dependency_completes_and_never_refused(env):
    a = mk(env.rt, "write")
    b = mk(env.rt, "publish", depends_on=[a])
    s = summary(env.rt, b)
    assert s["is_blocked"] is True and s["blocked_by"] == [a]

    (decision, *_) = do(env.rt, "complete_task", b)                                # out of order is allowed
    assert decision.payload["outcome"] == "applied"


def test_completing_the_dependency_unblocks(env):
    a = mk(env.rt, "write")
    b = mk(env.rt, "publish", depends_on=[a])
    do(env.rt, "complete_task", a)
    assert summary(env.rt, b)["is_blocked"] is False


def test_a_moved_dependency_is_followed_to_its_replacement(env):
    a = mk(env.rt, "write", scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))
    b = mk(env.rt, "publish", depends_on=[a])
    new_a = created_id(do(env.rt, "reschedule_task", a, new_start=T0 + timedelta(hours=3), new_end=T0 + timedelta(hours=4), reason="x"))
    assert summary(env.rt, b)["blocked_by"] == [a]                                 # still blocked by the SAME work, moved
    do(env.rt, "complete_task", new_a)
    assert summary(env.rt, b)["is_blocked"] is False                               # finishing the replacement unblocks it


def test_an_abandoned_dependency_is_reported_not_blocking_forever(env):
    a = mk(env.rt, "write")
    b = mk(env.rt, "publish", depends_on=[a])
    do(env.rt, "cancel_task", a, mode="cancel")
    s = summary(env.rt, b)
    assert s["is_blocked"] is False and s["abandoned"] == [a]


def test_scheduling_a_task_before_its_dependency_ends_is_flagged(env):
    a = mk(env.rt, "write", scheduled_start=T0 + timedelta(hours=3), scheduled_end=T0 + timedelta(hours=5))
    b = mk(env.rt, "publish", depends_on=[a], scheduled_start=T0 + timedelta(hours=4), scheduled_end=T0 + timedelta(hours=5))
    assert summary(env.rt, b)["scheduled_before_dependency"] == [a]


def test_dependencies_persist_and_reach_the_agents_prompt(env):
    a = mk(env.rt, "write")
    b = mk(env.rt, "publish", depends_on=[a], estimated_minutes=30)
    tasks = replay(env.rt.store.conn)
    assert tasks[b].depends_on == (a,) and tasks[b].estimated_minutes == 30
    prompt = build_prompt(TemporalContext(now=T0, events=[], tasks=list(tasks.values()), timezone="UTC"))
    assert f"blocked_by=['{a}']" in prompt and "estimate_minutes=30" in prompt


# ===================================================== database versioning

def test_a_new_database_is_stamped_and_a_legacy_one_is_adopted(tmp_path):
    conn = sqlite3.connect(tmp_path / "x.db", isolation_level=None)
    init_db(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == storage.DB_SCHEMA_VERSION
    conn.execute("PRAGMA user_version = 0")                                        # what every pre-versioning database looks like
    init_db(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
    conn.close()


def test_a_database_from_a_newer_version_is_refused_not_corrupted(tmp_path):
    conn = sqlite3.connect(tmp_path / "x.db", isolation_level=None)
    init_db(conn)
    conn.execute("PRAGMA user_version = 99")
    with pytest.raises(SchemaTooNew, match="upgrade"):
        init_db(conn)
    with pytest.raises(SchemaTooNew):
        Store(str(tmp_path / "x.db"))
    conn.close()


def test_migrations_run_in_order_and_atomically(tmp_path, monkeypatch):
    conn = sqlite3.connect(tmp_path / "x.db", isolation_level=None)
    init_db(conn)
    ran = []

    def to_2(c):
        ran.append(2)
        c.execute("ALTER TABLE events ADD COLUMN note TEXT")

    def to_3(c):
        ran.append(3)
        raise RuntimeError("boom")

    monkeypatch.setattr(storage, "MIGRATIONS", {1: to_2, 2: to_3})
    monkeypatch.setattr(storage, "DB_SCHEMA_VERSION", 3)
    with pytest.raises(RuntimeError, match="boom"):
        init_db(conn)
    assert ran == [2, 3]
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 2                  # step 1 stuck; the failed step left no trace
    monkeypatch.setattr(storage, "MIGRATIONS", {1: to_2, 2: lambda c: None})
    init_db(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
    conn.close()


def test_a_missing_migration_is_an_error_not_a_silent_skip(tmp_path, monkeypatch):
    conn = sqlite3.connect(tmp_path / "x.db", isolation_level=None)
    init_db(conn)
    monkeypatch.setattr(storage, "DB_SCHEMA_VERSION", 2)
    with pytest.raises(RuntimeError, match="no migration"):
        init_db(conn)
    conn.close()


def test_an_event_written_by_a_newer_version_is_quarantined_not_misread(env):
    tid = mk(env.rt)
    env.rt.store.conn.execute(
        "INSERT INTO events (id, schema_version, event_type, occurred_at, recorded_at, task_id, payload) VALUES (?,?,?,?,?,?,?)",
        ("future", 2, "TASK_DROPPED", T0.isoformat(), T0.isoformat(), tid, "{}"))
    view = env.rt.store.refresh()
    assert view.tasks[tid].status == TaskStatus.CREATED                            # the unknown event did not act
    assert view.quarantined and "schema_version 2" in view.quarantined[-1][1]


# ============================================================== backup

def test_backup_is_a_working_copy_and_will_not_overwrite(env):
    mk(env.rt, "keep me")
    dest = env.tmp / "b.db"
    env.rt.store.backup(dest)
    copy = Store(str(dest))
    assert [t.title for t in copy.view.tasks.values()] == ["keep me"]
    copy.close()
    with pytest.raises(FileExistsError):
        env.rt.store.backup(dest)


def test_export_and_import_reproduce_the_log_exactly(env):
    a = mk(env.rt, "a", scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))
    do(env.rt, "reschedule_task", a, new_start=T0 + timedelta(hours=3), new_end=T0 + timedelta(hours=4), reason="x")
    do(env.rt, "create_recurrence", title="R", display_timezone="UTC", frequency="daily", start_time="10:00", duration_minutes=15)
    dump = env.tmp / "log.jsonl"
    count = export_jsonl(env.rt.store.conn, dump)
    assert count == env.rt.store.view.last_seq

    conn = sqlite3.connect(env.tmp / "new.db", isolation_level=None)
    init_db(conn)
    assert import_jsonl(conn, dump) == count
    original = [(e.seq, e.id, e.event_type, e.payload) for e in storage.all_events(env.rt.store.conn)]
    restored = [(e.seq, e.id, e.event_type, e.payload) for e in storage.all_events(conn)]
    assert restored == original                                                    # including seq, which checkpoints refer to
    assert {t.id: t.status for t in replay(conn).values()} == {t.id: t.status for t in env.rt.store.view.tasks.values()}
    conn.close()


def test_import_refuses_a_used_database_and_a_bad_file_changes_nothing(env):
    dump = env.tmp / "log.jsonl"
    mk(env.rt)
    export_jsonl(env.rt.store.conn, dump)
    with pytest.raises(ValueError, match="already has events"):
        import_jsonl(env.rt.store.conn, dump)

    bad = env.tmp / "bad.jsonl"
    lines = dump.read_text(encoding="utf-8").splitlines()
    bad.write_text("\n".join(lines + ['{"seq": 99, "id": "x"}']) + "\n", encoding="utf-8")
    conn = sqlite3.connect(env.tmp / "empty.db", isolation_level=None)
    init_db(conn)
    with pytest.raises(ValueError, match="bad.jsonl:"):
        import_jsonl(conn, bad)
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0          # all or nothing
    conn.close()


# ===================================================== creation limit

def test_every_caller_shares_a_daily_creation_limit(env):
    env.rt.max_creations_per_day = 3
    for i in range(3):
        assert do(env.rt, "create_task", title=f"t{i}", display_timezone="UTC")[0].payload["outcome"] == "applied"
    (d, *_) = do(env.rt, "create_task", title="one too many", display_timezone="UTC")
    assert d.payload["outcome"] == "rejected" and "creation limit" in d.payload["rejection_reason"]
    assert len(env.rt.store.view.tasks) == 3

    env.clock.set(T0 + timedelta(hours=25))                                        # the window is rolling, not per-calendar-day
    assert do(env.rt, "create_task", title="tomorrow", display_timezone="UTC")[0].payload["outcome"] == "applied"


def test_moves_and_completions_do_not_use_up_the_creation_limit(env):
    env.rt.max_creations_per_day = 2
    a = mk(env.rt, "a", scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))
    new = created_id(do(env.rt, "reschedule_task", a, new_start=T0 + timedelta(hours=3), new_end=T0 + timedelta(hours=4), reason="x"))
    do(env.rt, "complete_task", new)
    assert do(env.rt, "create_task", title="b", display_timezone="UTC")[0].payload["outcome"] == "applied"


def test_a_retried_creation_is_recognised_before_the_limit_applies(env):
    env.rt.max_creations_per_day = 1
    call = ActionCall("same-key", "create_task", None, {"title": "a", "display_timezone": "UTC"})
    assert env.rt.execute(call)[0].payload["outcome"] == "applied"
    assert env.rt.execute(call)[0].payload["outcome"] == "superseded"              # not "rejected: limit"


# ================================================ clock regression visible

def test_a_backwards_clock_is_counted_and_reported(env):
    env.clock.set(T0 + timedelta(hours=2)); env.rt.run_pass()
    env.clock._now = T0 + timedelta(hours=1)
    env.rt.run_pass()
    assert env.rt.health.clock_regressions == 1
    assert "moved backwards by 3600s" in env.rt.health.last_clock_warning
    env.clock._now = T0 + timedelta(hours=1, seconds=1)                            # jitter under the tolerance is not a regression
    env.rt.run_pass()
    assert env.rt.health.clock_regressions == 1


# ====================================================== calendar import

ICS = """BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:standup-1
SUMMARY:Team sync
DTSTART:20260913T090000Z
DTEND:20260913T093000Z
END:VEVENT
BEGIN:VEVENT
UID:dentist
SUMMARY:Dentist\\, Dr. Rao
DTSTART;TZID=Asia/Kolkata:20260914T160000
DTEND;TZID=Asia/Kolkata:20260914T170000
END:VEVENT
BEGIN:VEVENT
UID:long-line
SUMMARY:Planning
  offsite
DTSTART:20260915T100000Z
DURATION:PT1H30M
END:VEVENT
BEGIN:VEVENT
UID:holiday
SUMMARY:Public holiday
DTSTART;VALUE=DATE:20260916
END:VEVENT
BEGIN:VEVENT
UID:weekly
SUMMARY:Weekly review
DTSTART:20260917T100000Z
DTEND:20260917T110000Z
RRULE:FREQ=WEEKLY
END:VEVENT
BEGIN:VEVENT
UID:moon
SUMMARY:Lunar base
DTSTART;TZID=Mars/Olympus:20260918T100000
DTEND;TZID=Mars/Olympus:20260918T110000
END:VEVENT
BEGIN:VEVENT
UID:no-end
SUMMARY:Reminder
DTSTART:20260919T100000Z
END:VEVENT
END:VCALENDAR
""".replace("\n", "\r\n")


def test_ics_parsing_handles_zones_folding_durations_and_escapes():
    events, skipped = parse_ics(ICS, "UTC")
    by_uid = {e.uid: e for e in events}
    assert by_uid["standup-1"].start == datetime(2026, 9, 13, 9, 0, tzinfo=UTC)
    assert by_uid["dentist"].title == "Dentist, Dr. Rao"
    assert by_uid["dentist"].start == datetime(2026, 9, 14, 10, 30, tzinfo=UTC)    # 16:00 IST
    assert by_uid["long-line"].title == "Planning offsite"                          # a folded line is one line
    assert by_uid["long-line"].end - by_uid["long-line"].start == timedelta(minutes=90)
    assert {s["uid"]: s["reason"] for s in skipped} == {
        "holiday": "all-day event",
        "weekly": "recurring event: not imported (use create_recurring_task)",
        "moon": "unknown time zone 'Mars/Olympus'",
        "no-end": "no end time",
    }


def test_floating_times_use_the_given_zone_across_dst():
    text = "BEGIN:VEVENT\nUID:a\nSUMMARY:x\nDTSTART:20261101T013000\nDTEND:20261101T023000\nEND:VEVENT\n"
    (event,), _ = parse_ics(text, "America/New_York")
    assert event.start.tzinfo == timezone.utc and event.start == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)   # first 01:30 (EDT)


def test_bad_calendars_are_bounded():
    with pytest.raises(ValueError, match="too large"):
        parse_ics("x" * 1_000_001)
    many = "".join(f"BEGIN:VEVENT\nUID:{i}\nEND:VEVENT\n" for i in range(501))
    with pytest.raises(ValueError, match="more than 500"):
        parse_ics(many)
    with pytest.raises(ValueError, match="unknown timezone"):
        parse_ics(ICS, "Nowhere/Land")


def test_import_creates_tasks_and_reports_what_it_skipped(env):
    report = import_calendar(env.rt, ICS, source="work")
    assert (report["created"], report["skipped"]) == (3, 4)
    titles = sorted(t.title for t in env.rt.store.view.tasks.values())
    assert titles == ["Dentist, Dr. Rao", "Planning offsite", "Team sync"]
    dentist = next(t for t in env.rt.store.view.tasks.values() if t.title.startswith("Dentist"))
    assert dentist.scheduled_start == datetime(2026, 9, 14, 10, 30, tzinfo=UTC) and dentist.status == TaskStatus.SCHEDULED


def test_reimporting_the_same_file_changes_nothing(env):
    import_calendar(env.rt, ICS, source="work")
    seq = env.rt.store.refresh().last_seq
    report = import_calendar(env.rt, ICS, source="work")
    assert report["created"] == 0 and report["unchanged"] == 3
    creations = [e for e in storage.all_events(env.rt.store.conn) if e.event_type == EventType.TASK_CREATED]
    assert len(creations) == 3
    assert env.rt.store.refresh().last_seq >= seq                                  # only decision-log lines were added


def test_a_time_change_in_the_calendar_reschedules_and_follows_flapping(env):
    one = ICS.replace("DTSTART:20260913T090000Z", "DTSTART:20260913T100000Z").replace("DTEND:20260913T093000Z", "DTEND:20260913T103000Z")
    import_calendar(env.rt, ICS, source="work")
    assert import_calendar(env.rt, one, source="work")["updated"] == 1
    live = [t for t in env.rt.store.view.tasks.values() if t.title == "Team sync" and not t.is_terminal()]
    assert len(live) == 1 and live[0].scheduled_start == datetime(2026, 9, 13, 10, 0, tzinfo=UTC)
    assert import_calendar(env.rt, ICS, source="work")["updated"] == 1             # and back again
    live = [t for t in env.rt.store.view.tasks.values() if t.title == "Team sync" and not t.is_terminal()]
    assert live[0].scheduled_start == datetime(2026, 9, 13, 9, 0, tzinfo=UTC)


def test_a_cancelled_event_cancels_its_task(env):
    import_calendar(env.rt, ICS, source="work")
    cancelled = ICS.replace("UID:dentist\r\n", "UID:dentist\r\nSTATUS:CANCELLED\r\n")
    assert import_calendar(env.rt, cancelled, source="work")["cancelled"] == 1
    dentist = next(t for t in env.rt.store.view.tasks.values() if t.title.startswith("Dentist"))
    assert dentist.status == TaskStatus.CANCELLED


def test_what_the_person_did_locally_wins_over_the_calendar(env):
    import_calendar(env.rt, ICS, source="work")
    sync = next(t for t in env.rt.store.view.tasks.values() if t.title == "Team sync")
    do(env.rt, "complete_task", sync.id)
    moved = ICS.replace("DTSTART:20260913T090000Z", "DTSTART:20260913T120000Z").replace("DTEND:20260913T093000Z", "DTEND:20260913T123000Z")
    report = import_calendar(env.rt, moved, source="work")
    row = next(r for r in report["events"] if r["uid"] == "standup-1")
    assert row["result"] == "unchanged" and "local decision wins" in row["detail"]
    assert env.rt.store.view.tasks[sync.id].status == TaskStatus.COMPLETED


def test_two_calendars_that_overlap_are_reported_as_conflicts_not_merged(env):
    from temporal_engine.overlap import find_conflicts
    a = "BEGIN:VEVENT\nUID:1\nSUMMARY:Work call\nDTSTART:20260913T090000Z\nDTEND:20260913T100000Z\nEND:VEVENT\n"
    b = "BEGIN:VEVENT\nUID:1\nSUMMARY:School play\nDTSTART:20260913T093000Z\nDTEND:20260913T103000Z\nEND:VEVENT\n"
    import_calendar(env.rt, a, source="work")
    import_calendar(env.rt, b, source="family")                                    # same UID, different source: different event
    tasks = [t for t in env.rt.store.view.tasks.values()]
    assert len(tasks) == 2 and len(find_conflicts(tasks)) == 1


def test_the_import_goes_through_the_creation_limit(env):
    env.rt.max_creations_per_day = 2
    report = import_calendar(env.rt, ICS, source="work")
    assert report["created"] == 2 and report["rejected"] == 1
    assert "creation limit" in next(r["detail"] for r in report["events"] if r["result"] == "rejected")


def test_source_names_are_restricted(env):
    with pytest.raises(ValueError, match="source"):
        import_calendar(env.rt, ICS, source="bad source; drop")


# ============================================================== the CLI

def test_status_backup_export_import_from_the_command_line(env, capsys):
    mk(env.rt, "cli task")
    env.rt.store.conn.execute("PRAGMA wal_checkpoint(FULL)")
    db = str(env.path)
    assert cli.main(["--db", db, "status"]) == 0
    assert "1 total" in capsys.readouterr().out
    assert cli.main(["--db", db, "backup", str(env.tmp / "cli-backup.db")]) == 0
    assert cli.main(["--db", db, "backup", str(env.tmp / "cli-backup.db")]) == 1       # never overwrites
    assert "already exists" in capsys.readouterr().err
    assert cli.main(["--db", db, "export", str(env.tmp / "cli.jsonl")]) == 0
    assert cli.main(["--db", str(env.tmp / "restored.db"), "import", str(env.tmp / "cli.jsonl")]) == 0
    assert cli.main(["--db", str(env.tmp / "restored.db"), "import", str(env.tmp / "cli.jsonl")]) == 1
    restored = Store(str(env.tmp / "restored.db"))
    assert [t.title for t in restored.view.tasks.values()] == ["cli task"]
    restored.close()


def test_watch_reports_what_needs_a_human_and_can_run_a_command(env, capsys, monkeypatch):
    monkeypatch.setenv("TEMPORAL_ENGINE_TZ", "UTC")
    hold = ActionCall("held", "cancel_task", mk(env.rt, "Water plants", scheduled_start=T0 + timedelta(hours=1),
                                                 scheduled_end=T0 + timedelta(hours=2)), {"mode": "cancel"}, requires_confirmation=True)
    env.rt.execute(hold)
    env.clock.set(T0 + timedelta(hours=3)); env.rt.run_pass()

    out_file = env.tmp / "notified.txt"
    command = f'"{sys.executable}" -c "import os; open(r\'{out_file}\', \'a\').write(os.environ[\'TE_EVENT\'] + \'|\' + os.environ[\'TE_TASK_TITLE\'] + \'\\n\')"'
    code = cli.main(["--db", str(env.path), "watch", "--once", "--from-start", "--exec", command])
    assert code == 0
    out = capsys.readouterr().out
    assert "needs your confirmation: cancel_task on Water plants" in out
    assert "time is up: Water plants" in out and "starting now: Water plants" in out
    lines = out_file.read_text().splitlines()
    assert "TASK_WINDOW_ENDED|Water plants" in lines and "CONFIRMATION_NEEDED|Water plants" in lines


def test_a_hostile_title_cannot_inject_into_the_notification_command(env, capsys, monkeypatch):
    """The event reaches the command through the environment, never through the command line."""
    marker = env.tmp / "pwned"
    title = f'x"; echo pwned > "{marker}"; echo "'
    mk(env.rt, title, scheduled_start=T0 + timedelta(hours=1), scheduled_end=T0 + timedelta(hours=2))
    env.clock.set(T0 + timedelta(hours=3)); env.rt.run_pass()
    cli.main(["--db", str(env.path), "watch", "--once", "--from-start", "--exec", f'"{sys.executable}" -c "pass"'])
    assert not marker.exists()


def test_headless_run_records_boundaries_with_no_client_open(tmp_path):
    """The gap 'nothing runs while the client is closed', shown with real processes and the real clock."""
    db = tmp_path / "headless.db"
    seed = Runtime(db, day_boundary_tz="UTC", refresh_interval=0.3)
    now = datetime.now(UTC)
    do(seed, "create_task", title="soon", display_timezone="UTC",
       scheduled_start=now + timedelta(seconds=1.5), scheduled_end=now + timedelta(seconds=3))
    seed.close()

    env = dict(os.environ, TEMPORAL_ENGINE_DB=str(db), TEMPORAL_ENGINE_TZ="UTC")
    env.pop("TEMPORAL_ENGINE_PROVIDER", None)
    proc = subprocess.Popen([sys.executable, "-m", "temporal_engine.cli", "run", "--refresh", "0.3"],
                            env=env, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        deadline = time.time() + 25
        kinds = set()
        while time.time() < deadline and EventType.TASK_WINDOW_ENDED not in kinds:
            time.sleep(0.4)
            reader = Store(str(db))
            kinds = {e.event_type for e in reader.events(limit=200)[0]}
            reader.close()
        assert {EventType.TASK_STARTED, EventType.TASK_WINDOW_ENDED} <= kinds, proc.stderr.read1(4000) if proc.poll() is not None else kinds
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
