"""Roll the hourly Silver table up to Europe/Zurich calendar days.

`rollup_rows` is the pure-Python core: it takes hourly rows and returns one dict per local day.
`rollup_daily` reads the same three Silver columns from a Spark frame and calls the core, so
the counting and the day length are tested without Spark.

For each day the result holds:

- `local_date`: the calendar date of `ts_local`.
- `hours`: how many hourly rows the day has.
- `expected_hours`: the length of that calendar day in Europe/Zurich, 23 on the day the clocks
  go forward, 25 on the day they go back, otherwise 24.
- `incomplete`: `hours < expected_hours`. A short day is reported as it is; it is never scaled
  up to a full day.
- `gwh_total`: the sum of `load_mw` over the day divided by 1000. One hourly row is one hour,
  so MW summed over hours is MWh, and 1000 MWh is 1 GWh.
- `gwh_enduser` (Swissgrid only): the same for `load_enduser_mw`. It is None when the day has
  no end-user value or when any hour of the day lacks one, so a partial sum is never shown.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Europe/Zurich")
SOURCES = ("entsoe", "swissgrid")
_UTC = timezone.utc


def expected_hours(day: date) -> int:
    """Length of the Europe/Zurich calendar day `day` in hours: 23, 24 or 25."""
    start = datetime(day.year, day.month, day.day, tzinfo=LOCAL_TZ)
    following = day + timedelta(days=1)
    end = datetime(following.year, following.month, following.day, tzinfo=LOCAL_TZ)
    return round((end.astimezone(_UTC) - start.astimezone(_UTC)).total_seconds() / 3600)


def rollup_rows(
    rows: Iterable[tuple[datetime, float | None, float | None]], source: str
) -> list[dict[str, Any]]:
    """Roll hourly `(ts_local, load_mw, load_enduser_mw)` rows up to local days, oldest first.

    A row with a NULL `load_mw` is not counted as an hour. `gwh_enduser` is present only when
    `source` is `swissgrid`.
    """
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}; expected one of {SOURCES}")
    days: dict[date, dict[str, float]] = {}
    for ts_local, mw, enduser_mw in rows:
        if mw is None:
            continue
        acc = days.setdefault(
            ts_local.date(), {"hours": 0, "mw": 0.0, "enduser_mw": 0.0, "enduser_hours": 0}
        )
        acc["hours"] += 1
        acc["mw"] += mw
        if enduser_mw is not None:
            acc["enduser_mw"] += enduser_mw
            acc["enduser_hours"] += 1
    result = []
    for day in sorted(days):
        acc = days[day]
        hours = int(acc["hours"])
        expected = expected_hours(day)
        out: dict[str, Any] = {
            "local_date": day,
            "hours": hours,
            "expected_hours": expected,
            "incomplete": hours < expected,
            "gwh_total": acc["mw"] / 1000,
        }
        if source == "swissgrid":
            complete = acc["enduser_hours"] == hours
            out["gwh_enduser"] = acc["enduser_mw"] / 1000 if complete else None
        result.append(out)
    return result


def rollup_daily(df: Any, source: str) -> list[dict[str, Any]]:
    """Roll the rows of `source` in the Silver frame `df` up to local days.

    Reads `source`, `ts_local`, `load_mw` and `load_enduser_mw`; the day comes from `ts_local`,
    the local wall clock, so the repeated hour of the clock change stays in its own day.
    """
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}; expected one of {SOURCES}")
    picked = df.filter(df["source"] == source).select("ts_local", "load_mw", "load_enduser_mw")
    return rollup_rows(((r[0], r[1], r[2]) for r in picked.collect()), source)
