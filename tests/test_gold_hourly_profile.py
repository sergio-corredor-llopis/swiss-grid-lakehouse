"""Gold mart g3: the typical load by weekday and local hour."""

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from swiss_grid_lakehouse.gold import hourly_profile
from swiss_grid_lakehouse.silver.ch_load_silver import _silver_schema
from swiss_grid_lakehouse.silver.daily_rollup import expected_hours

pytestmark = pytest.mark.spark

ZONE = ZoneInfo("Europe/Zurich")
STAMP = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


def _day_rows(source, day, mw_of_hour, count=None, batch="aaaaaaaaaaaa"):
    """Hourly Silver rows of one local day; `mw_of_hour(i)` is the load of the i-th hour."""
    start = datetime(day.year, day.month, day.day, tzinfo=ZONE).astimezone(UTC)
    length = count if count is not None else expected_hours(day)
    rows = []
    for i in range(length):
        ts_utc = start + timedelta(hours=i)
        local = ts_utc.astimezone(ZONE)
        rows.append(
            (
                source,
                "CH",
                ts_utc,
                local.replace(tzinfo=None),
                local.date(),
                60,
                float(mw_of_hour(i)),
                None,
                None,
                batch,
                STAMP,
                STAMP,
            )
        )
    return rows


def _frame(spark, rows):
    return spark.createDataFrame(rows, schema=_silver_schema())


def _by_key(df):
    return {(r["source"], r["weekday"], r["hour_local"]): r for r in df.collect()}


def test_two_days_and_two_sources_give_96_rows(spark):
    rows = []
    for source in ("entsoe", "swissgrid"):
        rows += _day_rows(source, date(2026, 8, 30), lambda i: 6000 + i)
        rows += _day_rows(source, date(2026, 8, 31), lambda i: 7000 + i)
    df = hourly_profile.build(spark, _frame(spark, rows))
    assert df.count() == 96
    assert df.columns == [*hourly_profile.KEY, *hourly_profile.CHANGE_COLS]
    assert {r["n_days"] for r in df.collect()} == {1}


def test_sunday_is_7_and_monday_is_1(spark):
    rows = _day_rows("entsoe", date(2026, 8, 30), lambda i: 100 + i)
    rows += _day_rows("entsoe", date(2026, 8, 31), lambda i: 200 + i)
    got = _by_key(hourly_profile.build(spark, _frame(spark, rows)))
    assert {k[1] for k in got} == {1, 7}
    assert got[("entsoe", 7, 5)]["avg_mw"] == 105.0
    assert got[("entsoe", 1, 5)]["avg_mw"] == 205.0
    assert {k[2] for k in got} == set(range(24))


def test_the_same_weekday_on_two_dates_is_one_row_with_two_days(spark):
    rows = _day_rows("entsoe", date(2026, 8, 31), lambda i: 100)
    rows += _day_rows("entsoe", date(2026, 9, 7), lambda i: 300)
    got = _by_key(hourly_profile.build(spark, _frame(spark, rows)))
    row = got[("entsoe", 1, 9)]
    assert (row["n_days"], row["avg_mw"], row["min_mw"], row["max_mw"]) == (2, 200.0, 100.0, 300.0)


def test_the_repeated_hour_of_the_clock_change_counts_in_its_hour_local(spark):
    # 2026-10-25 has 25 hours; local 02:00 occurs twice (first at +02:00, then at +01:00).
    rows = _day_rows("entsoe", date(2026, 10, 25), lambda i: 1000 + i)
    assert len(rows) == 25
    got = _by_key(hourly_profile.build(spark, _frame(spark, rows)))
    assert len(got) == 24
    twice = got[("entsoe", 7, 2)]
    # the 02:00 readings are the 3rd and 4th hour of the day: 1002 and 1003
    assert (twice["n_days"], twice["avg_mw"], twice["min_mw"], twice["max_mw"]) == (
        1,
        1002.5,
        1002.0,
        1003.0,
    )
    assert got[("entsoe", 7, 3)]["avg_mw"] == 1004.0


def test_the_short_day_has_no_hour_2(spark):
    # 2026-03-29 has 23 hours: local 02:00 does not exist.
    rows = _day_rows("entsoe", date(2026, 3, 29), lambda i: i)
    assert len(rows) == 23
    got = _by_key(hourly_profile.build(spark, _frame(spark, rows)))
    assert ("entsoe", 7, 2) not in got
    assert len(got) == 23
