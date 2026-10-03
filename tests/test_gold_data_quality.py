"""Gold mart g4: which days are short, re-pulled or hold rejected rows."""

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from swiss_grid_lakehouse.gold import data_quality
from swiss_grid_lakehouse.silver.ch_load_silver import _silver_schema

pytestmark = pytest.mark.spark

ZONE = ZoneInfo("Europe/Zurich")
STAMP = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


def _hours(source, day, count, batch="aaaaaaaaaaaa", first=0):
    """`count` hourly Silver rows of the local day from the `first`-th hour of the day."""
    start = datetime(day.year, day.month, day.day, tzinfo=ZONE).astimezone(UTC)
    rows = []
    for i in range(first, first + count):
        ts_utc = start + timedelta(hours=i)
        local = ts_utc.astimezone(ZONE)
        rows.append(
            (source, "CH", ts_utc, local.replace(tzinfo=None), local.date(), 60, 6000.0 + i)
            + (None, None, batch, STAMP, STAMP)
        )
    return rows


def _frame(spark, rows):
    return spark.createDataFrame(rows, schema=_silver_schema())


def _by_key(df):
    return {(r["source"], r["local_date"]): r for r in df.collect()}


def test_hours_equal_the_daily_load_hours(spark):
    from swiss_grid_lakehouse.gold import daily_load

    rows = []
    for source in ("entsoe", "swissgrid"):
        rows += _hours(source, date(2026, 8, 30), 24)
        rows += _hours(source, date(2026, 8, 31), 23)
        rows += _hours(source, date(2026, 10, 25), 25)
    silver = _frame(spark, rows)
    quality = _by_key(data_quality.build(spark, silver))
    load = _by_key(daily_load.build(spark, silver))
    assert quality.keys() == load.keys()
    assert all(quality[k]["hours"] == load[k]["hours"] for k in quality)


def test_a_23_hour_day_of_24_is_one_missing_hour(spark):
    silver = _frame(spark, _hours("entsoe", date(2026, 8, 30), 23))
    row = _by_key(data_quality.build(spark, silver))[("entsoe", date(2026, 8, 30))]
    assert (row["hours"], row["expected_hours"], row["missing_hours"]) == (23, 24, 1)
    assert row["incomplete"] is True


def test_complete_days_of_every_length_have_nothing_missing(spark):
    rows = (
        _hours("entsoe", date(2026, 3, 29), 23)
        + _hours("entsoe", date(2026, 8, 30), 24)
        + _hours("entsoe", date(2026, 10, 25), 25)
    )
    got = _by_key(data_quality.build(spark, _frame(spark, rows)))
    assert {k[1]: v["expected_hours"] for k, v in got.items()} == {
        date(2026, 3, 29): 23,
        date(2026, 8, 30): 24,
        date(2026, 10, 25): 25,
    }
    assert {v["missing_hours"] for v in got.values()} == {0}
    assert {v["incomplete"] for v in got.values()} == {False}


def test_a_day_with_two_batch_ids_is_repulled(spark):
    rows = _hours("entsoe", date(2026, 8, 30), 20, batch="aaaaaaaaaaaa")
    rows += _hours("entsoe", date(2026, 8, 30), 4, batch="bbbbbbbbbbbb", first=20)
    rows += _hours("entsoe", date(2026, 8, 31), 24, batch="aaaaaaaaaaaa")
    got = _by_key(data_quality.build(spark, _frame(spark, rows)))
    assert (
        got[("entsoe", date(2026, 8, 30))]["batches"],
        got[("entsoe", date(2026, 8, 30))]["repulled"],
    ) == (2, True)
    assert (
        got[("entsoe", date(2026, 8, 31))]["batches"],
        got[("entsoe", date(2026, 8, 31))]["repulled"],
    ) == (1, False)


def test_rejected_rows_are_counted_per_day_and_source(spark):
    rows = _hours("entsoe", date(2026, 8, 30), 24) + _hours("entsoe", date(2026, 8, 31), 24)
    rows += _hours("swissgrid", date(2026, 8, 30), 24)
    rejected = spark.createDataFrame(
        [
            ("entsoe", datetime(2026, 8, 30, 3, 0)),
            ("entsoe", datetime(2026, 8, 30, 4, 0)),
            ("entsoe", datetime(2026, 8, 31, 5, 0)),
        ],
        schema="source string, ts_local timestamp_ntz",
    )
    got = _by_key(data_quality.build(spark, _frame(spark, rows), rejected))
    assert got[("entsoe", date(2026, 8, 30))]["rejected_rows"] == 2
    assert got[("entsoe", date(2026, 8, 31))]["rejected_rows"] == 1
    assert got[("swissgrid", date(2026, 8, 30))]["rejected_rows"] == 0


def test_rejected_rows_with_a_local_date_column_are_counted(spark):
    silver = _frame(spark, _hours("entsoe", date(2026, 8, 30), 24))
    rejected = spark.createDataFrame(
        [("entsoe", date(2026, 8, 30))], schema="source string, local_date date"
    )
    got = _by_key(data_quality.build(spark, silver, rejected))
    assert got[("entsoe", date(2026, 8, 30))]["rejected_rows"] == 1


def test_without_a_rejected_table_the_count_is_zero(spark):
    silver = _frame(spark, _hours("entsoe", date(2026, 8, 30), 24))
    df = data_quality.build(spark, silver)
    assert df.columns == [*data_quality.KEY, *data_quality.CHANGE_COLS]
    assert [r["rejected_rows"] for r in df.collect()] == [0]
