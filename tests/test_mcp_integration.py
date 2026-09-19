"""
End-to-end tests of mcp_server.py: each test spawns the real server as a
subprocess and drives it over stdio with a genuine MCP ClientSession --
the same transport Claude Code/Desktop use. No LLM and no API key are
involved, so these are deterministic and cost nothing; they exist to
catch wiring bugs (lifespan, lock, tool serialization, scheduler wake)
that unit tests of the pieces cannot.
"""
import asyncio
import json
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

PROJECT_DIR = str(Path(__file__).resolve().parent.parent)


@asynccontextmanager
async def _session(db_path: Path):
    env = os.environ.copy()
    env["TEMPORAL_ENGINE_DB"] = str(db_path)
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
    return json.loads(result.content[0].text)


def test_scheduler_wakes_early_for_a_task_created_while_it_sleeps(tmp_path):
    """Regression test for the stale-sleep bug: the loop starts against an
    empty task list (sleeping until midnight), then a task with a
    near-term boundary appears -- it must still be ticked on time."""

    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            await asyncio.sleep(0.5)  # let the loop compute its first, distant target
            now = datetime.now(timezone.utc)
            await _call(s, "create_task", {
                "title": "soon", "timezone": "UTC",
                "scheduled_start": (now + timedelta(seconds=1)).isoformat(),
                "scheduled_end": (now + timedelta(seconds=2)).isoformat(),
            })
            for _ in range(10):
                await asyncio.sleep(0.5)
                ctx = await _call(s, "get_temporal_context")
                if ctx["tasks"][0]["status"] == "WINDOW_ENDED":
                    return True
            return False

    assert asyncio.run(scenario())


def test_boundaries_crossed_while_the_server_was_down_are_caught_up_on_restart(tmp_path):
    """The 'I closed Claude in between' scenario. The host process (and
    with it the server, and its scheduler) is gone while a task's whole
    window elapses; on the next start the task must already read as
    WINDOW_ENDED, not sit at SCHEDULED until some later wake-up."""
    db_path = tmp_path / "t.db"

    async def first_run():
        async with _session(db_path) as s:
            now = datetime.now(timezone.utc)
            await _call(s, "create_task", {
                "title": "elapses while away", "timezone": "UTC",
                "scheduled_start": (now + timedelta(seconds=1)).isoformat(),
                "scheduled_end": (now + timedelta(seconds=2)).isoformat(),
            })
        # session closed here -- server process terminated before the window began

    async def second_run():
        async with _session(db_path) as s:
            await asyncio.sleep(0.5)
            return (await _call(s, "get_temporal_context"))["tasks"][0]["status"]

    asyncio.run(first_run())
    import time
    time.sleep(3)  # the task's entire window elapses with no server running
    assert asyncio.run(second_run()) == "WINDOW_ENDED"


def test_reschedule_returns_new_task_id_and_cap_rejects_the_fourth_move(tmp_path):
    async def scenario():
        async with _session(tmp_path / "t.db") as s:
            base = datetime.now(timezone.utc) + timedelta(days=1)
            created = await _call(s, "create_task", {
                "title": "movable", "timezone": "UTC",
                "scheduled_start": base.isoformat(),
                "scheduled_end": (base + timedelta(hours=1)).isoformat(),
            })
            current_id = created["id"]

            outcomes = []
            for i in range(4):
                start = base + timedelta(hours=2 * (i + 1))
                result = await _call(s, "reschedule_task", {
                    "task_id": current_id,
                    "new_start": start.isoformat(),
                    "new_end": (start + timedelta(hours=1)).isoformat(),
                    "reason": f"move {i + 1}",
                })
                outcomes.append(result["outcome"])
                if result["new_task"]:
                    current_id = result["new_task"]["id"]
            return outcomes

    # 3 moves allowed (DEFAULT_MAX_LINEAGE_LENGTH), the 4th refused
    assert asyncio.run(scenario()) == ["applied", "applied", "applied", "rejected"]


def test_stale_lock_from_a_dead_process_does_not_demote_the_server(tmp_path):
    """The failure mode flagged before it was fixed: an MCP host force-kills
    the server, leaving its lock file behind, and the next start silently
    runs without a scheduler forever."""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    db_path = tmp_path / "t.db"
    db_path.with_suffix(".lock").write_text(str(proc.pid))  # dead holder's PID

    async def scenario():
        async with _session(db_path) as s:
            return await _call(s, "get_temporal_context")

    assert asyncio.run(scenario())["is_scheduler_process"] is True
