"""
`temporal-engine`: the command-line side of the engine, for things that should not need an AI client open.

  run      Run the scheduler (and the agent, if TEMPORAL_ENGINE_PROVIDER is set) headless, so boundaries are noticed
           and recorded while no client is running. MCP servers started later share the database and simply
           are not the scheduler.
  watch    Print (and optionally run a command for) events that need a human: windows ending, deadlines missed,
           new days, tasks starting, actions held for confirmation. This is the notification path: the engine
           does not push, but anything that can run a command can turn this into a toast, a chat message or a sound.
  backup   A consistent copy of the database file.
  export   The log as JSON lines.
  import   Rebuild an empty database from an export.
  status   A one-screen summary.

The MCP server (`temporal-engine-mcp`) is unchanged.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from temporal_engine.agent import Agent, AgentConfig, provider_from_env
from temporal_engine.events import EventType
from temporal_engine.mcp_server import default_db_path
from temporal_engine.runtime import Runtime
from temporal_engine.storage import SchemaTooNew, Store, import_jsonl, init_db

import sqlite3

WATCH_DEFAULT = (
    EventType.TASK_STARTED, EventType.TASK_WINDOW_ENDED, EventType.TASK_DEADLINE_BREACHED, EventType.NEW_DAY,
)


def _db(args) -> str:
    return args.db or default_db_path(os.environ)


def _tz() -> str:
    return os.environ.get("TEMPORAL_ENGINE_TZ", "UTC")


# ---------------------------------------------------------------------- run

async def _run_forever(db: str, tz: str, refresh: float) -> None:
    runtime = Runtime(db, day_boundary_tz=tz, refresh_interval=refresh)
    provider = provider_from_env(os.environ)
    if provider is not None:
        interval = float(os.environ.get("TEMPORAL_ENGINE_AGENT_MIN_INTERVAL", "30"))
        runtime.agent = Agent(runtime, provider, AgentConfig(min_interval_seconds=interval))
    loops = [asyncio.create_task(runtime.run_forever())]
    if runtime.agent is not None:
        loops.append(asyncio.create_task(runtime.agent.run_forever()))
    print(f"temporal-engine running: db={db} timezone={tz} agent={'on' if runtime.agent else 'off'} (Ctrl-C to stop)",
          file=sys.stderr)
    try:
        await asyncio.gather(*loops)
    finally:
        for t in loops:
            t.cancel()
        for t in loops:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        runtime.close()


def cmd_run(args) -> int:
    try:
        asyncio.run(_run_forever(_db(args), _tz(), args.refresh))
    except KeyboardInterrupt:
        pass
    return 0


# -------------------------------------------------------------------- watch

def describe(event, view, tz: ZoneInfo) -> Optional[tuple[str, str]]:
    """(kind, one-line text) for an event worth telling a human about, else None."""
    task = view.tasks.get(event.task_id) if event.task_id else None
    title = task.title if task else ""
    when = event.occurred_at.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    kind = event.event_type.value
    if event.event_type == EventType.ACTION_PROPOSED:
        if event.payload.get("outcome") != "pending_confirmation":
            return None
        call = event.payload.get("action_call", {})
        return "CONFIRMATION_NEEDED", f"{when}  needs your confirmation: {call.get('action')} on {title or call.get('task_id')}"
    if event.event_type == EventType.NEW_DAY:
        return kind, f"{when}  new day: {event.payload.get('new_date')}"
    if event.event_type == EventType.TASK_WINDOW_ENDED:
        return kind, f"{when}  time is up: {title}"
    if event.event_type == EventType.TASK_DEADLINE_BREACHED:
        return kind, f"{when}  deadline passed: {title}"
    if event.event_type == EventType.TASK_STARTED:
        return kind, f"{when}  starting now: {title}"
    return None


def cmd_watch(args) -> int:
    tz = ZoneInfo(_tz())
    store = Store(_db(args))
    seen = 0 if args.from_start else store.view.last_seq
    interval = max(0.2, args.interval)
    try:
        while True:
            view = store.refresh()
            events, _ = store.events(since_seq=seen, limit=200)
            for event in events:
                seen = event.seq
                item = describe(event, view, tz)
                if item is None:
                    continue
                kind, text = item
                print(text, flush=True)
                if args.exec:
                    # The command is the user's; the event is passed in the environment, never spliced into the command
                    # line, because task titles are untrusted text.
                    env = dict(os.environ, TE_EVENT=kind, TE_TEXT=text,
                               TE_TASK_TITLE=(view.tasks[event.task_id].title if event.task_id in view.tasks else ""))
                    try:
                        subprocess.run(args.exec, shell=True, env=env, timeout=10, check=False)
                    except (subprocess.TimeoutExpired, OSError) as exc:
                        print(f"(notification command failed: {exc})", file=sys.stderr)
            if args.once:
                return 0
            time.sleep(interval)
    except KeyboardInterrupt:
        return 0
    finally:
        store.close()


# ------------------------------------------------------- backup and friends

def cmd_backup(args) -> int:
    store = Store(_db(args))
    try:
        store.backup(args.destination)
    finally:
        store.close()
    print(f"backed up to {args.destination}")
    return 0


def cmd_export(args) -> int:
    store = Store(_db(args))
    try:
        n = store.export_jsonl(args.destination)
    finally:
        store.close()
    print(f"exported {n} events to {args.destination}")
    return 0


def cmd_import(args) -> int:
    path = Path(_db(args))
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)
    try:
        init_db(conn)
        n = import_jsonl(conn, args.source)
    finally:
        conn.close()
    print(f"imported {n} events into {path}")
    return 0


def cmd_status(args) -> int:
    store = Store(_db(args))
    try:
        view = store.view
        live = [t for t in view.tasks.values() if not t.is_terminal()]
        tz = ZoneInfo(_tz())
        print(f"database:   {_db(args)}")
        print(f"events:     {view.last_seq}   quarantined: {len(view.quarantined)}")
        print(f"tasks:      {len(view.tasks)} total, {len(live)} unfinished, {len(view.pending)} awaiting confirmation")
        print(f"recurring:  {sum(1 for r in view.recurrences.values() if not r.ended)} active")
        last = store.events(limit=1)[0]
        print(f"last event: {last[0].recorded_at.astimezone(tz).isoformat() if last else 'none'}")
    finally:
        store.close()
    return 0


# --------------------------------------------------------------------- main

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="temporal-engine", description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", help="database file (default: TEMPORAL_ENGINE_DB or ~/.temporal-engine/temporal_engine.db)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("run", help="run the scheduler headless")
    p.add_argument("--refresh", type=float, default=float(os.environ.get("TEMPORAL_ENGINE_REFRESH_SECONDS", "5")))
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("watch", help="print events that need a human; optionally run a command for each")
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--from-start", action="store_true", help="also report events already in the log")
    p.add_argument("--once", action="store_true", help="report what is there now, then exit")
    p.add_argument("--exec", help="shell command run per event, with TE_EVENT / TE_TEXT / TE_TASK_TITLE in its environment")
    p.set_defaults(func=cmd_watch)

    for name, func, help_text in (("backup", cmd_backup, "copy the database"), ("export", cmd_export, "write the log as JSON lines")):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("destination")
        p.set_defaults(func=func)

    p = sub.add_parser("import", help="rebuild an EMPTY database from an export")
    p.add_argument("source")
    p.set_defaults(func=cmd_import)

    p = sub.add_parser("status", help="summary")
    p.set_defaults(func=cmd_status)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (FileExistsError, ValueError, SchemaTooNew) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
