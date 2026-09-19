from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from temporal_engine.recurrence import (
    Recurrence, build_recurrence, next_occurrences, occurrence_dates, occurrence_window,
)

UTC = timezone.utc
NY = ZoneInfo("America/New_York")


def rec(frequency="daily", first="2026-09-14", start="09:00", tz="UTC", **kw):
    return build_recurrence(
        rec_id="r1", title="standup", display_timezone=tz, frequency=frequency,
        start_time=start, first_date=first, **kw,
    )


def days(r, through):
    return [d.isoformat() for d in occurrence_dates(r, date.fromisoformat(through))]


# ------------------------------------------------------------- generation

def test_daily_every_day_and_every_third_day():
    assert days(rec(), "2026-09-17") == ["2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17"]
    assert days(rec(interval=3), "2026-09-24") == ["2026-09-14", "2026-09-17", "2026-09-20", "2026-09-23"]


def test_weekly_on_several_weekdays_never_yields_before_the_anchor():
    # 2026-09-16 is a Wednesday; MO/WE/FR from that anchor must not produce Monday the 14th
    r = rec("weekly", first="2026-09-16", weekdays=["MO", "WE", "FR"])
    assert days(r, "2026-09-25") == ["2026-09-16", "2026-09-18", "2026-09-21", "2026-09-23", "2026-09-25"]


def test_weekly_defaults_to_the_anchors_weekday():
    assert days(rec("weekly", first="2026-09-16"), "2026-10-07") == [
        "2026-09-16", "2026-09-23", "2026-09-30", "2026-10-07"]


def test_biweekly_counts_weeks_from_the_anchors_week():
    r = rec("weekly", first="2026-09-16", weekdays=["MO", "WE"], interval=2)
    # week of the 14th is week 0 (Wed the 16th is its first hit); the week of the 21st is skipped
    assert days(r, "2026-10-14") == ["2026-09-16", "2026-09-28", "2026-09-30", "2026-10-12", "2026-10-14"]


def test_monthly_on_a_day_of_month():
    assert days(rec("monthly", first="2026-01-15"), "2026-04-30") == [
        "2026-01-15", "2026-02-15", "2026-03-15", "2026-04-15"]


def test_the_31st_is_skipped_in_short_months_not_clamped():
    got = days(rec("monthly", first="2026-01-31"), "2026-07-31")
    assert got == ["2026-01-31", "2026-03-31", "2026-05-31", "2026-07-31"]   # no Feb, Apr, Jun


def test_monthly_every_two_months_and_multiple_days():
    r = rec("monthly", first="2026-01-01", month_days=[1, 15], interval=2)
    assert days(r, "2026-05-31") == ["2026-01-01", "2026-01-15", "2026-03-01", "2026-03-15", "2026-05-01", "2026-05-15"]


def test_until_is_inclusive_and_count_stops_exactly():
    assert days(rec(until="2026-09-16"), "2026-12-31") == ["2026-09-14", "2026-09-15", "2026-09-16"]
    assert days(rec(count=2), "2026-12-31") == ["2026-09-14", "2026-09-15"]
    assert days(rec("weekly", first="2026-09-16", weekdays=["MO", "WE"], count=3), "2027-01-01") == [
        "2026-09-16", "2026-09-21", "2026-09-23"]


def test_nothing_is_generated_past_the_requested_horizon():
    assert days(rec(), "2026-09-13") == []       # before the anchor


# --------------------------------------------------------------- DST rules

def test_wall_clock_time_is_preserved_across_a_dst_change():
    r = rec(first="2026-03-07", start="09:00", tz="America/New_York")
    before, _ = occurrence_window(r, date(2026, 3, 7))     # EST
    after, _ = occurrence_window(r, date(2026, 3, 9))      # EDT
    assert (before.hour, after.hour) == (9, 9)                              # still 09:00 on the wall
    assert before.astimezone(UTC).hour == 14 and after.astimezone(UTC).hour == 13   # the instant moved


def test_a_nonexistent_local_time_resolves_forward():
    """02:30 does not exist on 2026-03-08 in New York (02:00 jumps to 03:00)."""
    start, _ = occurrence_window(rec(first="2026-03-08", start="02:30", tz="America/New_York"), date(2026, 3, 8))
    assert start.astimezone(NY).strftime("%H:%M") == "03:30"
    assert start.astimezone(UTC) == datetime(2026, 3, 8, 7, 30, tzinfo=UTC)


def test_an_ambiguous_local_time_resolves_to_its_first_occurrence():
    """01:30 happens twice on 2026-11-01 (EDT then EST); take the first."""
    start, _ = occurrence_window(rec(first="2026-11-01", start="01:30", tz="America/New_York"), date(2026, 11, 1))
    assert start.astimezone(UTC) == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)     # 01:30 EDT, not 01:30 EST (06:30Z)


def test_duration_is_absolute_across_a_transition():
    """A 2-hour occurrence starting 01:30 EDT on the fall-back day lasts two
    real hours, so it ends 02:30 EST -- not 03:30 by wall-clock arithmetic."""
    r = rec(first="2026-11-01", start="01:30", tz="America/New_York", duration_minutes=120)
    start, end = occurrence_window(r, date(2026, 11, 1))
    assert end - start == timedelta(hours=2)
    assert end.astimezone(NY).strftime("%H:%M") == "02:30"


def test_a_zone_without_dst_never_shifts():
    r = rec(first="2026-01-01", start="09:00", tz="Asia/Kolkata")
    offsets = {occurrence_window(r, date(2026, m, 1))[0].utcoffset() for m in range(1, 13)}
    assert offsets == {timedelta(hours=5, minutes=30)}


# --------------------------------------------------------------- validation

@pytest.mark.parametrize("kwargs, message", [
    (dict(frequency="hourly"), "frequency must be one of"),
    (dict(interval=0), "interval"),
    (dict(interval=True), "interval"),
    (dict(tz="Mars/Olympus"), "unknown timezone"),
    (dict(start="9am"), "HH:MM"),
    (dict(start="25:00"), "HH:MM"),
    (dict(first="tomorrow"), "first_date"),
    (dict(weekdays=["MO"]), "only applies to weekly"),
    (dict(month_days=[1]), "only applies to monthly"),
    (dict(frequency="weekly", weekdays=["XX"]), "weekdays must be from"),
    (dict(frequency="monthly", month_days=[0]), "month_days"),
    (dict(frequency="monthly", month_days=[32]), "month_days"),
    (dict(duration_minutes=0), "duration_minutes"),
    (dict(duration_minutes=2000), "duration_minutes"),
    (dict(until="2026-01-01"), "before first_date"),
    (dict(until="2027-01-01", count=3), "either until or count"),
    (dict(count=0), "count"),
])
def test_invalid_recurrences_are_rejected_with_a_readable_reason(kwargs, message):
    args = dict(frequency="daily", first="2026-09-14", start="09:00", tz="UTC")
    args.update(kwargs)
    with pytest.raises(ValueError, match=message):
        build_recurrence(
            rec_id="r", title="x", display_timezone=args.pop("tz"), frequency=args.pop("frequency"),
            start_time=args.pop("start"), first_date=args.pop("first"), **args,
        )


def test_an_empty_title_is_rejected():
    with pytest.raises(ValueError, match="title"):
        build_recurrence(rec_id="r", title="  ", display_timezone="UTC", frequency="daily",
                         start_time="09:00", first_date="2026-09-14")


# ----------------------------------------------------- persistence and helpers

def test_a_recurrence_survives_a_round_trip_through_its_logged_form():
    r = rec("weekly", first="2026-09-16", weekdays=["MO", "FR"], interval=2, duration_minutes=45,
            until="2027-01-31", tz="Asia/Kolkata", start="07:30")
    again = Recurrence.from_payload(r.to_payload())
    assert again == r
    assert occurrence_dates(again, date(2027, 2, 1)) == occurrence_dates(r, date(2027, 2, 1))


def test_next_occurrences_are_strictly_after_the_given_instant():
    r = rec(first="2026-09-14", start="09:00")
    after = datetime(2026, 9, 15, 9, 0, tzinfo=UTC)          # exactly at an occurrence
    nxt = next_occurrences(r, after, n=2)
    assert [d.date().isoformat() for d in nxt] == ["2026-09-16", "2026-09-17"]


def test_an_ended_recurrence_has_no_next_occurrences():
    r = rec()
    r.ended = True
    assert next_occurrences(r, datetime(2026, 9, 14, tzinfo=UTC)) == []


def test_describe_is_human_readable():
    text = rec("weekly", first="2026-09-16", weekdays=["MO", "FR"], duration_minutes=30, tz="Asia/Kolkata", start="07:30").describe()
    assert text == "every week on MO,FR at 07:30 Asia/Kolkata for 30 min"
