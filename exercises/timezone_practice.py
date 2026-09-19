from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def dst_status(local_date: date, tz_name: str) -> str:
    """Returns one of: "no_transition", "spring_forward", "fall_back"
    for the given calendar date in the given IANA timezone.

    Signal used: compare the UTC offset at midnight on local_date to the
    UTC offset at midnight the next day. If they differ, a transition
    happened sometime during local_date. Comparing midnight-to-midnight
    (rather than probing the transition hour itself) sidesteps the
    "2:30am doesn't exist" / "1:30am happens twice" edge cases entirely,
    since midnight is never the ambiguous or skipped hour.
    """
    tz = ZoneInfo(tz_name)

    midnight_today = datetime.combine(local_date, time.min, tzinfo=tz)
    next_day = local_date + timedelta(days=1)
    midnight_tomorrow = datetime.combine(next_day, time.min, tzinfo=tz)

    offset_today = midnight_today.utcoffset()
    offset_tomorrow = midnight_tomorrow.utcoffset()

    if offset_tomorrow == offset_today:
        return "no_transition"
    elif offset_tomorrow > offset_today:
        # offset moved toward zero / more positive -> local clocks
        # jumped forward -> this is what you measured Jan(-5) -> Jul(-4)
        return "spring_forward"
    else:
        return "fall_back"


if __name__ == "__main__":
    cases = [
        (date(2026, 3, 8), "America/New_York"),   # US spring-forward day
        (date(2026, 11, 1), "America/New_York"),  # US fall-back day
        (date(2026, 6, 15), "Asia/Kolkata"),       # no DST at all
    ]
    for d, tz in cases:
        print(d, tz, "->", dst_status(d, tz))
