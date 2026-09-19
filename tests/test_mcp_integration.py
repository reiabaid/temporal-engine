"""
End-to-end tests of mcp_server.py: each test spawns the real server as a
subprocess and drives it over stdio with a genuine MCP ClientSession -- the
same transport Claude Code/Desktop use. No LLM and no API key are involved,
so these are deterministic and cost nothing; they exist to catch wiring
bugs (lifespan, lock, tool serialization, scheduler wake) that unit tests
of the pieces cannot. Logic is tested deterministically in test_runtime.py;
these confirm it survives contact with real processes.
"""
import asyncio
import json
import os
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from temporal_engine.storage import replay
import sqlite3

PROJECT_DIR = str(Path(__file__).resolve().parent.parent)


@asynccontextmanager
async def _session(db_path: Path, refresh_seconds: float = 5.0):
    env = os.environ.copy()
    env["TEMPORAL_ENGINE_DB"] = str(db_path)
    env["TEMPORAL_ENGINE_REFRESH_SECONDS"] = str(refresh_seconds)
    env["PYTHONPATH"] = PROJECT_DIR
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "temporal_engine.mcp_server"],
        cwd=PROJECT_DIR,
        env=env,
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


async def _call(session: ClientSession, tool: str, args: dict | None = None) -> dict:
    result = await session.call_tool(tool, args or {})
    assert not result.is_error, result.content[0].text
    return json.loads(result.content[0].text)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _future(days: float = 1.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=days)


# --------------------------------------------------------------- scheduling

def test_scheduler_wakes_early_for_a_task_created_while_it_sleeps(tmp_path):
    """The loop starts against an empty task list (sleeping until midnight),
    then a task with a near-term boundary appears -- it must still be
    ticked on time."""

    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            await asyncio.sleep(0.5)
            now = datetime.now(timezone.utc)
            await _call(s, "create_task", {
                "title": "soon", "timezone": "UTC",
                "scheduled_start": _iso(now + timedelta(seconds=1)),
                "scheduled_end": _iso(now + timedelta(seconds=2)),
            })
            for _ in range(10):
                await asyncio.sleep(0.5)
                ctx = await _call(s, "get_temporal_context")
                if ctx["tasks"][0]["status"] == "WINDOW_ENDED":
                    return True
            return False

    assert asyncio.run(scenario())


def test_boundaries_crossed_while_the_server_was_down_are_caught_up_on_restart(tmp_path):
    """The 'I closed Claude in between' scenario: the window elapses with no
    server running; on the next start the task must already read as
    WINDOW_ENDED."""
    db_path = tmp_path / "t.db"

    async def first_run():
        async with _session(db_path) as s:
            now = datetime.now(timezone.utc)
            await _call(s, "create_task", {
                "title": "elapses while away", "timezone": "UTC",
                "scheduled_start": _iso(now + timedelta(seconds=1)),
                "scheduled_end": _iso(now + timedelta(seconds=2)),
            })

    async def second_run():
        async with _session(db_path) as s:
            await asyncio.sleep(0.5)
            return (await _call(s, "get_temporal_context"))["tasks"][0]["status"]

    asyncio.run(first_run())
    time.sleep(3)
    assert asyncio.run(second_run()) == "WINDOW_ENDED"


# ---------------------------------------------------------------- robustness

def test_bad_input_is_rejected_at_the_boundary_and_the_scheduler_stays_alive(tmp_path):
    """Regression: a datetime with no UTC offset used to be accepted, then
    kill the scheduler loop on the next pass while is_scheduler_process
    kept saying true. Now it is refused, and a later valid task is still
    ticked on time."""

    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            await asyncio.sleep(0.5)
            bad = await _call(s, "create_task", {
                "title": "naive", "timezone": "UTC",
                "scheduled_start": "2026-09-19T10:00:00", "scheduled_end": "2026-09-19T11:00:00",
            })
            backwards = await _call(s, "create_task", {
                "title": "backwards", "timezone": "UTC",
                "scheduled_start": _iso(_future()), "scheduled_end": _iso(_future(0.5)),
            })
            bad_zone = await _call(s, "create_task", {"title": "x", "timezone": "Mars/Olympus"})

            now = datetime.now(timezone.utc)
            await _call(s, "create_task", {
                "title": "good", "timezone": "UTC",
                "scheduled_start": _iso(now + timedelta(seconds=1)),
                "scheduled_end": _iso(now + timedelta(seconds=2)),
            })
            await asyncio.sleep(4)
            ctx = await _call(s, "get_temporal_context")
            return bad, backwards, bad_zone, ctx

    bad, backwards, bad_zone, ctx = asyncio.run(scenario())
    assert bad["outcome"] == "rejected" and "UTC offset" in bad["rejection_reason"]
    assert backwards["outcome"] == "rejected" and "must be after" in backwards["rejection_reason"]
    assert bad_zone["outcome"] == "rejected" and "timezone" in bad_zone["rejection_reason"]
    assert [t["title"] for t in ctx["tasks"]] == ["good"]  # rejected tasks were never created
    assert ctx["tasks"][0]["status"] == "WINDOW_ENDED"      # and the scheduler is alive
    assert ctx["scheduler"]["last_error"] is None


def test_stale_lock_from_a_dead_process_does_not_demote_the_server(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    db_path = tmp_path / "t.db"
    db_path.with_suffix(".lock").write_text(str(proc.pid))  # dead holder's PID

    async def scenario():
        async with _session(db_path) as s:
            return await _call(s, "get_temporal_context")

    assert asyncio.run(scenario())["is_scheduler_process"] is True


# ------------------------------------------------------------- two clients

def test_two_clients_cannot_corrupt_the_log_and_the_second_sees_the_first(tmp_path):
    """Regression for the worst bug found in review: client B held a stale
    view, completed a task client A had already rescheduled, the log gained
    an illegal RESCHEDULED -> COMPLETED transition, and no server could
    start again. Writes are now validated against the log under the write
    lock, so B's stale request is simply rejected."""
    db_path = tmp_path / "t.db"
    base = _future()

    async def scenario():
        async with _session(db_path) as a:
            created = await _call(a, "create_task", {
                "title": "T", "timezone": "UTC",
                "scheduled_start": _iso(base), "scheduled_end": _iso(base + timedelta(hours=1)),
            })
            t_id = created["new_task"]["id"]
            async with _session(db_path) as b:
                b_first = await _call(b, "get_temporal_context")
                moved = await _call(a, "reschedule_task", {
                    "task_id": t_id, "new_start": _iso(base + timedelta(hours=5)),
                    "new_end": _iso(base + timedelta(hours=6)), "reason": "move",
                })
                # B acts on its stale view FIRST (it has not read since A's
                # move) -- reading context first would refresh it and hide
                # the very bug this test exists to catch.
                b_complete = await _call(b, "complete_task", {"task_id": t_id})
                b_second = await _call(b, "get_temporal_context")
                return created, moved, b_first, b_second, b_complete

    created, moved, b_first, b_second, b_complete = asyncio.run(scenario())

    assert b_first["is_scheduler_process"] is False
    assert moved["outcome"] == "applied"
    # B saw A's reschedule without restarting: the superseded task is gone
    # from its unfinished list and the replacement is present.
    ids = [t["id"] for t in b_second["tasks"]]
    assert moved["new_task"]["id"] in ids and created["new_task"]["id"] not in ids
    assert b_complete["outcome"] == "rejected"

    # and the log is still replayable strictly, so a fresh server can start
    conn = sqlite3.connect(str(db_path))
    replay(conn)  # raises if any illegal transition was ever recorded
    conn.close()


def test_when_the_scheduler_process_exits_another_client_takes_over(tmp_path):
    """Regression: the lock used to be tried once at startup, so if the
    holder (say Claude Code) was closed while another client stayed open,
    nobody ticked again until that one restarted."""
    db_path = tmp_path / "t.db"

    async def scenario():
        async with _session(db_path, refresh_seconds=1) as b:
            async with _session(db_path, refresh_seconds=1) as a:
                await asyncio.sleep(1.5)
                holders_before = [
                    (await _call(x, "get_temporal_context"))["is_scheduler_process"] for x in (a, b)
                ]
            # a (started second) exits; b must become the scheduler
            for _ in range(15):
                await asyncio.sleep(0.5)
                if (await _call(b, "get_temporal_context"))["is_scheduler_process"]:
                    return holders_before, True
            return holders_before, False

    holders_before, took_over = asyncio.run(scenario())
    assert sorted(holders_before) == [False, True]  # exactly one scheduler at a time
    assert took_over


# ------------------------------------------------------ idempotency / reads

def test_a_retried_create_with_the_same_key_returns_the_original_task(tmp_path):
    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            args = {"title": "once", "timezone": "UTC", "idempotency_key": "abc-123"}
            first = await _call(s, "create_task", args)
            retry = await _call(s, "create_task", args)
            ctx = await _call(s, "get_temporal_context")
            return first, retry, ctx

    first, retry, ctx = asyncio.run(scenario())
    assert first["outcome"] == "applied" and retry["outcome"] == "superseded"
    assert retry["new_task"]["id"] == first["new_task"]["id"]
    assert len(ctx["tasks"]) == 1


def test_idempotency_survives_a_server_restart(tmp_path):
    db_path = tmp_path / "t.db"
    args = {"title": "once", "timezone": "UTC", "idempotency_key": "durable-key"}

    async def run():
        async with _session(db_path) as s:
            return await _call(s, "create_task", args)

    first = asyncio.run(run())
    second = asyncio.run(run())  # a brand-new server process, same database
    assert first["outcome"] == "applied" and second["outcome"] == "superseded"
    assert second["new_task"]["id"] == first["new_task"]["id"]


def test_cursor_returns_only_newer_events_and_finished_tasks_can_be_listed(tmp_path):
    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            a = await _call(s, "create_task", {"title": "A", "timezone": "UTC", "deadline": _iso(_future())})
            first = await _call(s, "get_temporal_context")
            b = await _call(s, "create_task", {"title": "B", "timezone": "UTC"})
            await _call(s, "complete_task", {"task_id": a["new_task"]["id"]})
            newer = await _call(s, "get_temporal_context", {"since_seq": first["cursor"], "include_finished": True})
            default = await _call(s, "get_temporal_context")
            return first, b, newer, default

    first, b, newer, default = asyncio.run(scenario())
    assert all(e["seq"] > first["cursor"] for e in newer["events"])
    assert {e["event_type"] for e in newer["events"]} >= {"TASK_CREATED", "TASK_COMPLETED"}
    assert newer["cursor"] > first["cursor"]
    assert [t["title"] for t in newer["finished_tasks"]] == ["A"]
    assert "finished_tasks" not in default          # opt-in
    assert [t["title"] for t in default["tasks"]] == ["B"]


def test_a_deadline_only_task_becomes_overdue(tmp_path):
    """Regression: a task with a deadline but no window used to sit at
    CREATED forever."""

    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            await asyncio.sleep(0.5)
            await _call(s, "create_task", {
                "title": "report", "timezone": "UTC",
                "deadline": _iso(datetime.now(timezone.utc) + timedelta(seconds=2)),
            })
            await asyncio.sleep(4)
            return (await _call(s, "get_temporal_context"))["tasks"][0]["status"]

    assert asyncio.run(scenario()) == "OVERDUE"


def test_start_task_reports_worked_time_and_overlaps_are_reported_not_refused(tmp_path):
    base = _future()

    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            first = await _call(s, "create_task", {
                "title": "first", "timezone": "UTC",
                "scheduled_start": _iso(base), "scheduled_end": _iso(base + timedelta(hours=2)),
            })
            second = await _call(s, "create_task", {
                "title": "second", "timezone": "UTC",
                "scheduled_start": _iso(base + timedelta(hours=1)),
                "scheduled_end": _iso(base + timedelta(hours=3)),
            })
            started = await _call(s, "start_task", {"task_id": first["new_task"]["id"]})
            ctx = await _call(s, "get_temporal_context")
            return first, second, started, ctx

    first, second, started, ctx = asyncio.run(scenario())
    assert second["outcome"] == "applied"                         # not refused
    assert [o["title"] for o in second["overlaps_with"]] == ["first"]  # but reported
    assert len(ctx["conflicts"]) == 1
    assert started["outcome"] == "applied"
    mine = next(t for t in ctx["tasks"] if t["title"] == "first")
    assert mine["actual_start"] is not None and mine["worked_seconds"] >= 0


def test_reschedule_returns_new_task_id_and_cap_rejects_the_fourth_move(tmp_path):
    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            base = _future()
            created = await _call(s, "create_task", {
                "title": "movable", "timezone": "UTC",
                "scheduled_start": _iso(base), "scheduled_end": _iso(base + timedelta(hours=1)),
            })
            current_id = created["new_task"]["id"]

            outcomes = []
            for i in range(4):
                start = base + timedelta(hours=2 * (i + 1))
                result = await _call(s, "reschedule_task", {
                    "task_id": current_id,
                    "new_start": _iso(start),
                    "new_end": _iso(start + timedelta(hours=1)),
                    "reason": f"move {i + 1}",
                })
                outcomes.append(result["outcome"])
                if result["new_task"]:
                    current_id = result["new_task"]["id"]
            return outcomes

    # 3 moves allowed per 24h (DEFAULT_MAX_MOVES), the 4th refused
    assert asyncio.run(scenario()) == ["applied", "applied", "applied", "rejected"]
