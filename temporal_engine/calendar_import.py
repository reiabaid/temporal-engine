"""
Import events from an iCalendar (.ics) file as tasks, and keep them in step on re-import.

This is a deliberate, read-only, pull-style bridge -- not a live sync. You hand it the text of a calendar export;
it does not fetch anything, so there is no credential or network surface. Everything it does goes through the same
actions as any client (validation, idempotency, the creation limit, the decision log).

Rules, stated so they can be tested:
  * An event's identity is (source, UID). Re-importing the same file does nothing.
  * If an event's time changed since the last import, the task it created is rescheduled (subject to the usual
    move limit). If it is now STATUS:CANCELLED, the task is cancelled.
  * A task the user already finished, dropped or cancelled locally is never reopened or moved: what the person
    did wins over the calendar.
  * Events that cannot be represented faithfully are SKIPPED WITH A REASON, never guessed at: all-day events,
    events with no end, recurring events (RRULE), unknown time zones. The report says which and why.
  * Every time is normalised to UTC on the way in. Two calendars that overlap show up as ordinary overlaps
    (conflicts) -- the engine reports them and does not pick a winner.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from temporal_engine.actions import ActionCall
from temporal_engine.planning import lineage_heads
from temporal_engine.task import MAX_TITLE_LENGTH

MAX_ICS_CHARS = 1_000_000
MAX_EVENTS = 500
MAX_SOURCE_LENGTH = 40


@dataclass
class CalendarEvent:
    uid: str
    title: str
    start: datetime          # always UTC
    end: datetime            # always UTC
    cancelled: bool = False


def _unfold(text: str) -> list[str]:
    lines: list[str] = []
    for raw in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if raw[:1] in (" ", "\t") and lines:
            lines[-1] += raw[1:]
        else:
            lines.append(raw)
    return lines


def _unescape(value: str) -> str:
    out, i = [], 0
    while i < len(value):
        if value[i] == "\\" and i + 1 < len(value):
            nxt = value[i + 1]
            out.append("\n" if nxt in "nN" else nxt)
            i += 2
        else:
            out.append(value[i])
            i += 1
    return "".join(out)


def _split_property(line: str) -> tuple[str, dict[str, str], str]:
    head, _, value = line.partition(":")
    name, *params = head.split(";")
    parsed = {}
    for p in params:
        k, _, v = p.partition("=")
        parsed[k.upper()] = v.strip('"')
    return name.upper(), parsed, value


_DURATION = re.compile(r"^([+-])?P(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def _parse_duration(value: str) -> Optional[timedelta]:
    m = _DURATION.match(value.strip())
    if not m or not any(m.groups()[1:]):
        return None
    sign, w, d, h, mi, s = m.groups()
    delta = timedelta(weeks=int(w or 0), days=int(d or 0), hours=int(h or 0), minutes=int(mi or 0), seconds=int(s or 0))
    return -delta if sign == "-" else delta


def _parse_time(value: str, params: dict[str, str], default_tz: str) -> datetime:
    """A DATE-TIME as an aware UTC datetime. Raises ValueError with a reason we can show."""
    if params.get("VALUE", "").upper() == "DATE" or re.fullmatch(r"\d{8}", value.strip()):
        raise ValueError("all-day event")
    text = value.strip()
    m = re.fullmatch(r"(\d{8})T(\d{6})(Z?)", text)
    if not m:
        raise ValueError(f"unrecognised time {value!r}")
    naive = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
    if m.group(3):
        return naive.replace(tzinfo=timezone.utc)
    name = params.get("TZID") or default_tz
    try:
        zone = ZoneInfo(name)
    except (KeyError, ValueError, OSError) as exc:
        raise ValueError(f"unknown time zone {name!r}") from exc
    return naive.replace(tzinfo=zone).astimezone(timezone.utc)    # normalise now: never compare two same-zone datetimes


def parse_ics(text: str, default_tz: str = "UTC") -> tuple[list[CalendarEvent], list[dict[str, str]]]:
    """(events, skipped). `skipped` entries are {uid, title, reason}."""
    if len(text) > MAX_ICS_CHARS:
        raise ValueError(f"calendar text is too large ({len(text)} characters; the limit is {MAX_ICS_CHARS})")
    try:
        ZoneInfo(default_tz)
    except (KeyError, ValueError, OSError) as exc:
        raise ValueError(f"unknown timezone {default_tz!r}") from exc

    events: list[CalendarEvent] = []
    skipped: list[dict[str, str]] = []
    block: Optional[list[tuple[str, dict[str, str], str]]] = None

    for line in _unfold(text):
        upper = line.strip().upper()
        if upper == "BEGIN:VEVENT":
            block = []
        elif upper == "END:VEVENT" and block is not None:
            _finish_event(block, default_tz, events, skipped)
            block = None
        elif block is not None and line.strip():
            block.append(_split_property(line))
        if len(events) + len(skipped) > MAX_EVENTS:
            raise ValueError(f"calendar has more than {MAX_EVENTS} events; split it into smaller files")
    return events, skipped


def _finish_event(props, default_tz: str, events: list[CalendarEvent], skipped: list[dict[str, str]]) -> None:
    first: dict[str, tuple[dict[str, str], str]] = {}
    for name, params, value in props:
        first.setdefault(name, (params, value))
    uid = first.get("UID", ({}, ""))[1].strip()
    title = _unescape(first.get("SUMMARY", ({}, "(no title)"))[1]).strip() or "(no title)"
    title = title if len(title) <= MAX_TITLE_LENGTH else title[: MAX_TITLE_LENGTH - 1] + "…"

    def skip(reason: str) -> None:
        skipped.append({"uid": uid or "(none)", "title": title[:120], "reason": reason})

    if not uid:
        return skip("no UID")
    if "RRULE" in first or "RECURRENCE-ID" in first:
        return skip("recurring event: not imported (use create_recurring_task)")
    if "DTSTART" not in first:
        return skip("no start time")
    try:
        start = _parse_time(first["DTSTART"][1], first["DTSTART"][0], default_tz)
        if "DTEND" in first:
            end = _parse_time(first["DTEND"][1], first["DTEND"][0], default_tz)
        elif "DURATION" in first:
            delta = _parse_duration(first["DURATION"][1])
            if delta is None:
                return skip(f"unrecognised duration {first['DURATION'][1]!r}")
            end = start + delta
        else:
            return skip("no end time")
    except ValueError as exc:
        return skip(str(exc))
    if end <= start:
        return skip("end is not after start")
    cancelled = first.get("STATUS", ({}, ""))[1].strip().upper() == "CANCELLED"
    events.append(CalendarEvent(uid, title, start, end, cancelled))


def _base_key(source: str, uid: str) -> str:
    key = f"ics:{source}:{uid}"
    if len(key) > 150:      # keys are bounded; a very long UID is hashed (stable, so identity is unchanged)
        key = f"ics:{source}:{hashlib.sha256(uid.encode()).hexdigest()}"
    return key


def import_calendar(runtime, text: str, source: str = "calendar", timezone_name: Optional[str] = None) -> dict[str, Any]:
    """Apply an .ics export to the engine. Returns a report of what happened to every event."""
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,%d}" % MAX_SOURCE_LENGTH, source or ""):
        raise ValueError(f"source must be 1-{MAX_SOURCE_LENGTH} letters, digits, '.', '_' or '-'")
    tz = timezone_name or runtime.day_boundary_tz
    events, skipped = parse_ics(text, tz)

    results: list[dict[str, str]] = []
    counts = {"created": 0, "updated": 0, "cancelled": 0, "unchanged": 0, "rejected": 0}

    def run(key: str, action: str, task_id: Optional[str], args: dict, reason: str = "") -> tuple[str, Optional[str]]:
        call = ActionCall(idempotency_key=key, action=action, task_id=task_id, args=args, reason=reason)
        decision = runtime.execute(call)[0].payload
        return decision["outcome"], decision.get("rejection_reason")

    for ev in events:
        base = _base_key(source, ev.uid)
        view = runtime.store.refresh()
        existing = view.created_by_key.get(base)
        outcome = "unchanged"
        detail: Optional[str] = None

        if existing is None:
            if ev.cancelled:
                results.append({"uid": ev.uid, "title": ev.title[:120], "result": "unchanged", "detail": "cancelled, never imported"})
                counts["unchanged"] += 1
                continue
            status, why = run(base, "create_task", None, {
                "title": ev.title, "display_timezone": tz, "scheduled_start": ev.start, "scheduled_end": ev.end,
            })
            if status == "applied":
                outcome = "created"
            elif status == "superseded":
                outcome = "unchanged"        # created in an earlier import, and the log agrees
            else:
                outcome, detail = "rejected", why
        else:
            heads = lineage_heads(view.tasks, existing)
            head = heads[0] if len(heads) == 1 else None
            if head is None or head.is_terminal():
                outcome, detail = "unchanged", "already finished or removed locally; the local decision wins"
            elif ev.cancelled:
                status, why = run(f"{base}:cancelled", "cancel_task", head.id, {"mode": "cancel"}, "cancelled in the calendar")
                outcome, detail = ("cancelled", None) if status in ("applied", "superseded") else ("rejected", why)
            elif head.scheduled_start != ev.start or head.scheduled_end != ev.end:
                # Keyed on the head being moved, so a calendar that flaps A -> B -> A -> B is followed each time,
                # while a retry of the same move (a crash mid-import) is still recognised as done.
                key = f"{base}:{head.id}:{ev.start.strftime('%Y%m%dT%H%M%SZ')}-{ev.end.strftime('%Y%m%dT%H%M%SZ')}"
                status, why = run(key, "reschedule_task", head.id, {"new_start": ev.start, "new_end": ev.end},
                                  "time changed in the calendar")
                if status == "applied":
                    outcome = "updated"
                elif status == "superseded":
                    outcome = "unchanged"
                else:
                    outcome, detail = "rejected", why
        counts[outcome] += 1
        row = {"uid": ev.uid, "title": ev.title[:120], "result": outcome}
        if detail:
            row["detail"] = detail
        results.append(row)

    return {"source": source, **counts, "skipped": len(skipped), "events": results, "skipped_events": skipped}
