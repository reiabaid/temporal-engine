"""
Recurrence: the template model and the generation of its occurrences. Pure
-- no storage, no clock -- so every rule below is testable exactly.

A recurring task is a TEMPLATE (a `Recurrence`, recorded in the event log)
that generates ordinary tasks. Each occurrence is a full task with its own
lifecycle: completing, skipping (cancel) or moving one never affects
another. Occurrence identity is (recurrence id, local date); the engine
uses it to create each occurrence exactly once.

Supported rules (a deliberate, small subset; each maps onto iCalendar
RRULE for later interop):
  daily    every N days
  weekly   every N weeks on the given weekdays (default: the anchor's weekday)
  monthly  every N months on the given days of month (default: the anchor's day)
each optionally bounded by `until` (inclusive local date) or `count`.

Semantics worth stating because they are the easy things to get wrong:

* Wall-clock time. "09:00" means 09:00 in the recurrence's own timezone on
  every occurrence, so the UTC instant shifts with DST. A rule is anchored
  to a timezone, never to a UTC offset.
* DST gaps and overlaps, explicitly. A local time that does not exist
  (02:30 on a spring-forward day) resolves FORWARD to the same instant read
  with the pre-transition offset (03:30 local); a local time that occurs
  twice (01:30 on a fall-back day) resolves to the FIRST occurrence. The
  duration is absolute: a 2-hour occurrence lasts two real hours, even
  across a transition.
* A day-of-month that does not exist in a month (the 31st in April) is
  skipped for that month, as iCalendar does; it is not clamped to the 30th.
* Weeks start on Monday. For `interval` > 1 the anchor date's week is week 0.
* `count` and `until` are mutually exclusive.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterator, Optional
from zoneinfo import ZoneInfo

WEEKDAY_CODES = ("MO", "TU", "WE", "TH", "FR", "SA", "SU")
FREQUENCIES = ("daily", "weekly", "monthly")
MAX_CANDIDATES = 200_000  # safety bound on expansion; far beyond any real rule


@dataclass
class Recurrence:
    id: str
    title: str
    display_timezone: str
    frequency: str
    interval: int
    weekdays: tuple[int, ...]       # 0 = Monday; used by weekly
    month_days: tuple[int, ...]     # 1..31; used by monthly
    first_date: date                # anchor: no occurrence before this local date
    start_time: time                # local wall-clock start
    duration: Optional[timedelta]   # None => a start time only
    until: Optional[date] = None
    count: Optional[int] = None
    ended: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "display_timezone": self.display_timezone,
            "frequency": self.frequency,
            "interval": self.interval,
            "weekdays": [WEEKDAY_CODES[i] for i in self.weekdays],
            "month_days": list(self.month_days),
            "first_date": self.first_date.isoformat(),
            "start_time": self.start_time.strftime("%H:%M"),
            "duration_minutes": int(self.duration.total_seconds() // 60) if self.duration else None,
            "until": self.until.isoformat() if self.until else None,
            "count": self.count,
        }

    @staticmethod
    def from_payload(p: dict[str, Any]) -> "Recurrence":
        return build_recurrence(
            rec_id=p["id"], title=p["title"], display_timezone=p["display_timezone"],
            frequency=p["frequency"], start_time=p["start_time"], first_date=p["first_date"],
            interval=p.get("interval", 1), weekdays=p.get("weekdays"), month_days=p.get("month_days"),
            duration_minutes=p.get("duration_minutes"), until=p.get("until"), count=p.get("count"),
        )

    def describe(self) -> str:
        every = "" if self.interval == 1 else f"{self.interval} "
        unit = {"daily": "day", "weekly": "week", "monthly": "month"}[self.frequency]
        text = f"every {every}{unit}{'s' if self.interval != 1 else ''}"
        if self.frequency == "weekly":
            text += " on " + ",".join(WEEKDAY_CODES[i] for i in self.weekdays)
        if self.frequency == "monthly":
            text += " on day " + ",".join(str(d) for d in self.month_days)
        text += f" at {self.start_time.strftime('%H:%M')} {self.display_timezone}"
        if self.duration:
            text += f" for {int(self.duration.total_seconds() // 60)} min"
        if self.until:
            text += f" until {self.until.isoformat()}"
        if self.count:
            text += f", {self.count} times"
        return text


# ------------------------------------------------------------ construction

def _as_date(name: str, value: Any) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        raise ValueError(f"{name} must be a date (YYYY-MM-DD), got a datetime")
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{name} must be a date (YYYY-MM-DD), got {value!r}") from exc


def _as_time(value: Any) -> time:
    if isinstance(value, time):
        return value.replace(second=0, microsecond=0)
    try:
        hours, minutes = str(value).split(":")
        return time(int(hours), int(minutes))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"start_time must be HH:MM (24-hour, local time), got {value!r}") from exc


def build_recurrence(
    *,
    rec_id: str,
    title: Any,
    display_timezone: Any,
    frequency: Any,
    start_time: Any,
    first_date: Any,
    interval: Any = 1,
    weekdays: Any = None,
    month_days: Any = None,
    duration_minutes: Any = None,
    until: Any = None,
    count: Any = None,
) -> Recurrence:
    """Validate and build. Every input may come from an LLM, so every
    failure is a ValueError with a readable reason."""
    if not isinstance(title, str) or not title.strip():
        raise ValueError("title must be a non-empty string")
    try:
        ZoneInfo(display_timezone)
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError(f"unknown timezone {display_timezone!r} (expected an IANA name)") from exc

    frequency = str(frequency).lower()
    if frequency not in FREQUENCIES:
        raise ValueError(f"frequency must be one of {', '.join(FREQUENCIES)}, got {frequency!r}")
    if isinstance(interval, bool) or not isinstance(interval, int) or not 1 <= interval <= 366:
        raise ValueError(f"interval must be a whole number from 1 to 366, got {interval!r}")

    anchor = _as_date("first_date", first_date)
    if anchor is None:
        raise ValueError("first_date is required")

    if frequency != "weekly" and weekdays:
        raise ValueError("weekdays only applies to weekly recurrences")
    if frequency != "monthly" and month_days:
        raise ValueError("month_days only applies to monthly recurrences")

    day_numbers: tuple[int, ...] = ()
    if frequency == "weekly":
        if weekdays:
            try:
                day_numbers = tuple(sorted({WEEKDAY_CODES.index(str(w).upper()[:2]) for w in weekdays}))
            except ValueError as exc:
                raise ValueError(f"weekdays must be from {', '.join(WEEKDAY_CODES)}, got {weekdays!r}") from exc
        else:
            day_numbers = (anchor.weekday(),)

    days_of_month: tuple[int, ...] = ()
    if frequency == "monthly":
        if month_days:
            if not all(isinstance(d, int) and not isinstance(d, bool) and 1 <= d <= 31 for d in month_days):
                raise ValueError(f"month_days must be whole numbers from 1 to 31, got {month_days!r}")
            days_of_month = tuple(sorted(set(month_days)))
        else:
            days_of_month = (anchor.day,)

    duration = None
    if duration_minutes is not None:
        if isinstance(duration_minutes, bool) or not isinstance(duration_minutes, int) or not 1 <= duration_minutes <= 24 * 60:
            raise ValueError(f"duration_minutes must be a whole number from 1 to 1440, got {duration_minutes!r}")
        duration = timedelta(minutes=duration_minutes)

    end_date = _as_date("until", until)
    if end_date is not None and count is not None:
        raise ValueError("give either until or count, not both")
    if end_date is not None and end_date < anchor:
        raise ValueError(f"until ({end_date}) is before first_date ({anchor})")
    if count is not None and (isinstance(count, bool) or not isinstance(count, int) or count < 1):
        raise ValueError(f"count must be a whole number of at least 1, got {count!r}")

    return Recurrence(
        id=rec_id, title=title, display_timezone=display_timezone, frequency=frequency,
        interval=interval, weekdays=day_numbers, month_days=days_of_month, first_date=anchor,
        start_time=_as_time(start_time), duration=duration, until=end_date, count=count,
    )


# --------------------------------------------------------------- generation

def _add_months(year: int, month: int, months: int) -> tuple[int, int]:
    index = year * 12 + (month - 1) + months
    return index // 12, index % 12 + 1


def _candidates(rec: Recurrence) -> Iterator[date]:
    """Every date the rule produces, ascending, from the anchor, unbounded."""
    first = rec.first_date
    if rec.frequency == "daily":
        step, current = timedelta(days=rec.interval), first
        while True:
            yield current
            current += step

    elif rec.frequency == "weekly":
        week_zero = first - timedelta(days=first.weekday())
        week = 0
        while True:
            base = week_zero + timedelta(weeks=week * rec.interval)
            for weekday in rec.weekdays:
                current = base + timedelta(days=weekday)
                if current >= first:
                    yield current
            week += 1

    else:  # monthly
        k = 0
        while True:
            year, month = _add_months(first.year, first.month, k * rec.interval)
            last_day = calendar.monthrange(year, month)[1]
            for day in rec.month_days:
                if day <= last_day:          # the 31st is skipped in a 30-day month, not clamped
                    current = date(year, month, day)
                    if current >= first:
                        yield current
            k += 1


def occurrence_dates(rec: Recurrence, through: date) -> list[date]:
    """Local dates of every occurrence up to and including `through`,
    honouring the anchor, `until` and `count`."""
    limit = min(through, rec.until) if rec.until else through
    dates: list[date] = []
    for examined, current in enumerate(_candidates(rec), start=1):
        if current > limit or examined > MAX_CANDIDATES:
            break
        dates.append(current)
        if rec.count is not None and len(dates) >= rec.count:
            break
    return dates


def _fixed(moment: datetime) -> datetime:
    """The same instant with a fixed-offset tzinfo. Python compares and
    subtracts two aware datetimes that share one ZoneInfo object by their
    WALL-CLOCK fields, ignoring DST (02:30 - 01:30 is "1 hour" even when two
    real hours passed). A fixed offset has no such ambiguity -- and is what a
    datetime parsed back from the log has anyway, so live and replayed
    values behave identically."""
    return moment.replace(tzinfo=timezone(moment.utcoffset()))


def occurrence_window(rec: Recurrence, on: date) -> tuple[datetime, Optional[datetime]]:
    """Start and end of the occurrence on local date `on`, as timezone-aware
    datetimes rendered in the recurrence's timezone. Built through UTC so a
    DST gap resolves forward, an overlap resolves to its first occurrence,
    and the duration is absolute."""
    tz = ZoneInfo(rec.display_timezone)
    wall = datetime.combine(on, rec.start_time, tzinfo=tz)  # fold=0
    start_utc = wall.astimezone(timezone.utc)
    end = _fixed((start_utc + rec.duration).astimezone(tz)) if rec.duration else None
    return _fixed(start_utc.astimezone(tz)), end


def next_occurrences(rec: Recurrence, after: datetime, n: int = 3) -> list[datetime]:
    """The next `n` occurrence start times strictly after `after`."""
    if rec.ended:
        return []
    tz = ZoneInfo(rec.display_timezone)
    horizon = after.astimezone(tz).date() + timedelta(days=max(400, 40 * rec.interval))
    found = []
    for on in occurrence_dates(rec, horizon):
        start, _ = occurrence_window(rec, on)
        if start > after:
            found.append(start)
            if len(found) == n:
                break
    return found
