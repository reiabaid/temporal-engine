"""
Things a stranger who clones the repo will touch first: the demo, the example client config, and how times read.
"""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from temporal_engine.mcp_server import _task_summary
from temporal_engine.task import Task

ROOT = Path(__file__).resolve().parent.parent


def test_the_demo_runs_without_a_key_and_tells_the_story():
    result = subprocess.run([sys.executable, str(ROOT / "scripts" / "demo.py")],
                            capture_output=True, text=True, timeout=120, cwd=ROOT)
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "WINDOW ENDED   DSA practice" in out
    assert "reschedule_task" in out and "carry_forward_task" in out
    assert "NEW DAY        2026-09-13 (Asia/Kolkata)" in out
    assert "DEADLINE MISSED Send report" in out
    assert "never to ask the time" in out
    assert "Traceback" not in result.stderr


def test_the_committed_demo_transcript_matches_what_the_demo_prints():
    """The README quotes docs/demo-output.txt; if the demo changes, that file must be regenerated."""
    result = subprocess.run([sys.executable, str(ROOT / "scripts" / "demo.py")],
                            capture_output=True, text=True, timeout=120, cwd=ROOT)
    committed = (ROOT / "docs" / "demo-output.txt").read_text(encoding="utf-8")
    assert result.stdout.strip() == committed.strip()


def test_the_example_client_config_is_portable():
    config = json.loads((ROOT / ".mcp.example.json").read_text(encoding="utf-8"))
    server = config["mcpServers"]["temporal-engine"]
    text = json.dumps(server)
    assert server["command"] == "temporal-engine-mcp"
    assert ":\\\\" not in text and "/Users/" not in text and "/home/" not in text     # no one's machine baked in


def test_a_task_summary_writes_times_in_the_tasks_own_timezone():
    start = datetime(2026, 9, 21, 5, 30, tzinfo=timezone.utc)
    task = Task.new("imported", "Asia/Kolkata", scheduled_start=start, scheduled_end=datetime(2026, 9, 21, 6, 30, tzinfo=timezone.utc))
    summary = _task_summary(task, start)
    assert summary["scheduled_start"] == "2026-09-21T11:00:00+05:30"
    assert datetime.fromisoformat(summary["scheduled_start"]) == start          # the same instant, only written locally
