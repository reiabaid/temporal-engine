# Two datetimes, one timezone, the wrong answer

While building a scheduler for AI agents I hit a Python behaviour that quietly gives the wrong duration across a daylight-saving change. Here it is with the numbers, and the fix.

## The trap

Python's documentation says that when two aware `datetime` objects share the **same `tzinfo` object**, subtraction and comparison ignore the offset and use wall-clock arithmetic. With `ZoneInfo` that means the same zone object is enough to trigger it.

```python
from datetime import datetime
from zoneinfo import ZoneInfo

ny = ZoneInfo("America/New_York")

start = datetime(2026, 11, 1, 0, 30, tzinfo=ny)            # 00:30 EDT
end   = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=1)    # the SECOND 01:30, after the clocks fell back (EST)

print(end - start)                                          # 1:00:00   <- wall-clock difference
utc = ZoneInfo("UTC")
print(end.astimezone(utc) - start.astimezone(utc))          # 2:00:00   <- the real elapsed time
```

On 1 November 2026 clocks in New York go from 01:59 back to 01:00. Between 00:30 and the *second* 01:30 two real hours pass, but the same-zone subtraction reports one. The same applies to `>` and `<`: the comparison is made on wall-clock digits.

## How it bit this project

A recurring 60-minute task at 01:30 on the fall-back day starts at 01:30 EDT and ends 60 real minutes later, which is 01:30 EST. Both are "01:30" on the wall clock:

```python
start = datetime(2026, 11, 1, 1, 30, tzinfo=ny)                       # 01:30-04:00
end   = (start.astimezone(utc) + timedelta(minutes=60)).astimezone(ny)  # 01:30-05:00, fold=1

end > start        # False  (same ZoneInfo: compared as wall-clock digits)
end - start        # 0:00:00
# real elapsed:      1:00:00
```

The validation "a window must end after it starts" rejected a perfectly good one-hour occurrence as zero-length. It was found by a test built on the real 2026 transition dates, not by reading code.

## The fix

Two rules, applied everywhere:

1. **Store and compare instants in UTC.** The clock returns UTC, and anything read from storage is converted before arithmetic.
2. **Compute local wall-clock times once, then freeze them as fixed-offset datetimes.** A recurring "01:30 New York" is resolved to a concrete instant (with an explicit rule for skipped and repeated local times), converted to a fixed-offset `datetime`, and only *that* is compared or subtracted.

After that, two datetimes never share a `ZoneInfo`, and arithmetic is on real elapsed time.

## Why this matters for LLM agents

A model asked "has this deadline passed?" has to do the same conversions in its head, and the failure modes are the same family: comparing local digits, forgetting an offset, assuming a zone never changes offset. That is why the accompanying benchmark (State-QA) includes reference solvers that make each of those mistakes on purpose, and a check that they fail exactly the categories the mistake predicts.
