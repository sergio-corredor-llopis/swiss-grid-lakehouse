from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import swiss_grid_lakehouse.reconcile as rc
from swiss_grid_lakehouse.gold.daily_control import CHANGE_COLS, KEY, build
from swiss_grid_lakehouse.silver.ch_load_silver import _silver_schema
from swiss_grid_lakehouse.silver.daily_rollup import rollup_rows

pytestmark = pytest.mark.spark

ZONE = ZoneInfo("Europe/Zurich")
STAMP = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
DAILY_SCHEMA = (
    "local_date string, national_gwh double, final_gwh double, "
    "registry_modified string, batch_id string"
)
OLD_STAMP = "2027-01-15T00:00:00"  # long after every day below: figures are final
YOUNG_STAMP = "2026-09-02T03:00:00"  # one day after the day below: still preliminary

PASS_DAY = date(2026, 6, 1)
PRELIMINARY_DAY = date(2026, 9, 1)
FAIL_DAY = date(2026, 6, 2)
INCOMPLETE_DAY = date(2026, 6, 3)


def _day(source, day, gwh, enduser_gwh=None, count=24):
    """`count` hourly Silver rows from local midnight adding up to `gwh` GWh."""
    start = datetime(day.year, day.month, day.day, tzinfo=ZONE).astimezone(UTC)
    rows = []
    for i in range(count):
        ts_utc = start + timedelta(hours=i)
        ts_local = ts_utc.astimezone(ZONE).replace(tzinfo=None)
        end_mw = None if enduser_gwh is None else enduser_gwh * 1000 / count
        rows.append(
            (
                source,
                "CH",
                ts_utc,
                ts_local,
                ts_local.date(),
                60,
                gwh * 1000 / count,
                end_mw,
                None,
                "batch_a",
                STAMP,
                STAMP,
            )
        )
    return rows


def _silver_rows():
    """Four days: pass, preliminary, fail and incomplete, for both sources."""
    rows = []
    for day, total, enduser, entsoe, count in (
        (PASS_DAY, 150.0, 110.0, 151.0, 24),
        (PRELIMINARY_DAY, 148.0, 109.0, 149.0, 24),
        (FAIL_DAY, 150.0, 110.0, 150.0, 24),
        (INCOMPLETE_DAY, 150.0, 110.0, 150.0, 22),
    ):
        rows += _day("swissgrid", day, total, enduser, count)
        rows += _day("entsoe", day, entsoe, None, count)
    return rows


def _daily_rows():
    return [
        (PASS_DAY.isoformat(), 150.5, 110.5, OLD_STAMP, "d1"),
        (PRELIMINARY_DAY.isoformat(), 148.5, 109.5, YOUNG_STAMP, "d1"),
        (FAIL_DAY.isoformat(), 120.0, 80.0, OLD_STAMP, "d1"),
        (INCOMPLETE_DAY.isoformat(), 150.0, 110.0, OLD_STAMP, "d1"),
    ]


def _frames(spark, silver_rows, daily_rows):
    silver = spark.createDataFrame(silver_rows, schema=_silver_schema())
    return silver, spark.createDataFrame(daily_rows, schema=DAILY_SCHEMA)


def _oracle(silver_rows, daily_rows):
    rollups = {
        source: rollup_rows([(r[3], r[6], r[7]) for r in silver_rows if r[0] == source], source)
        for source in ("swissgrid", "entsoe")
    }
    keys = ("local_date", "national_gwh", "final_gwh", "registry_modified", "batch_id")
    published = [dict(zip(keys, row, strict=True)) for row in daily_rows]
    found = {}
    for result in rc.reconcile(rollups, published):
        for d in result.days:
            found[(result.source, result.series, d.day)] = d
    return found


def _close(a, b):
    return (a is None and b is None) or (a is not None and b is not None and abs(a - b) <= 1e-9)


def test_module_constants():
    assert KEY == ["source", "series", "local_date"]
    assert CHANGE_COLS == [
        "pipeline_gwh",
        "published_gwh",
        "diff_pct",
        "hours",
        "status",
        "registry_modified",
    ]


def test_mart_equals_reconcile(spark):
    silver_rows, daily_rows = _silver_rows(), _daily_rows()
    silver, daily = _frames(spark, silver_rows, daily_rows)
    mart = {
        (r["source"], r["series"], r["local_date"]): r
        for r in build(spark, silver, daily).collect()
    }
    expected = _oracle(silver_rows, daily_rows)
    assert len(expected) == 12
    assert set(mart) == set(expected)
    for key, want in expected.items():
        got = mart[key]
        assert got["status"] == want.status, key
        assert _close(got["diff_pct"], want.diff_pct), key
        assert _close(got["pipeline_gwh"], want.pipeline_gwh), key
        assert _close(got["published_gwh"], want.published_gwh), key
        assert got["hours"] == want.hours, key


def test_each_status_is_reached(spark):
    silver, daily = _frames(spark, _silver_rows(), _daily_rows())
    got = {
        (r["source"], r["series"], r["local_date"]): r["status"]
        for r in build(spark, silver, daily).collect()
    }
    assert got[("swissgrid", "total", PASS_DAY)] == "PASS"
    assert got[("swissgrid", "enduser", PASS_DAY)] == "PASS"
    assert got[("entsoe", "total", PASS_DAY)] == "PASS"
    assert got[("swissgrid", "total", PRELIMINARY_DAY)] == "PRELIMINARY"
    assert got[("swissgrid", "total", FAIL_DAY)] == "FAIL"
    assert got[("swissgrid", "total", INCOMPLETE_DAY)] == "INCOMPLETE"


def test_incomplete_day_keeps_its_partial_figure(spark):
    silver, daily = _frames(spark, _silver_rows(), _daily_rows())
    rows = build(spark, silver, daily).filter(f"local_date = DATE'{INCOMPLETE_DAY}'").collect()
    assert len(rows) == 3
    for row in rows:
        assert row["status"] == "INCOMPLETE"
        assert row["hours"] == 22
        # The 22 rows add up to the figure given to `_day`; nothing is scaled to 24 hours.
        want = 110.0 if row["series"] == "enduser" else 150.0
        assert row["pipeline_gwh"] == pytest.approx(want)


def test_columns_and_types(spark):
    silver, daily = _frames(spark, _silver_rows(), _daily_rows())
    assert dict(build(spark, silver, daily).dtypes) == {
        "source": "string",
        "series": "string",
        "local_date": "date",
        "pipeline_gwh": "double",
        "published_gwh": "double",
        "diff_pct": "double",
        "hours": "int",
        "status": "string",
        "registry_modified": "string",
    }


def test_none_daily_gives_none(spark):
    silver = spark.createDataFrame(_silver_rows(), schema=_silver_schema())
    assert build(spark, silver, None) is None


def test_empty_daily_gives_none(spark):
    silver, daily = _frames(spark, _silver_rows(), [])
    assert build(spark, silver, daily) is None


def test_latest_batch_per_day_wins(spark):
    silver_rows = _day("swissgrid", PASS_DAY, 150.0, 110.0) + _day("entsoe", PASS_DAY, 150.0)
    daily_rows = [
        (PASS_DAY.isoformat(), 100.0, 70.0, "2027-01-10T00:00:00", "d1"),
        (PASS_DAY.isoformat(), 150.5, 110.5, "2027-01-20T00:00:00", "d2"),
        (PASS_DAY.isoformat(), 90.0, 60.0, "2027-01-15T00:00:00", "d3"),
    ]
    silver, daily = _frames(spark, silver_rows, daily_rows)
    mart = build(spark, silver, daily).collect()
    assert len(mart) == 3
    assert {r["registry_modified"] for r in mart} == {"2027-01-20T00:00:00"}
    assert {r["status"] for r in mart} == {"PASS"}
    expected = _oracle(silver_rows, daily_rows)
    for row in mart:
        want = expected[(row["source"], row["series"], row["local_date"])]
        assert _close(row["diff_pct"], want.diff_pct)
        assert _close(row["published_gwh"], want.published_gwh)


def test_equal_stamps_are_broken_by_batch_id(spark):
    silver_rows = _day("swissgrid", PASS_DAY, 150.0, 110.0) + _day("entsoe", PASS_DAY, 150.0)
    daily_rows = [
        (PASS_DAY.isoformat(), 150.5, 110.5, OLD_STAMP, "a"),
        (PASS_DAY.isoformat(), 100.0, 70.0, OLD_STAMP, "b"),
    ]
    silver, daily = _frames(spark, silver_rows, daily_rows)
    mart = build(spark, silver, daily).collect()
    expected = _oracle(silver_rows, daily_rows)
    for row in mart:
        want = expected[(row["source"], row["series"], row["local_date"])]
        assert row["status"] == want.status
        assert _close(row["published_gwh"], want.published_gwh)


def test_day_without_a_published_figure_is_not_compared(spark):
    silver_rows = _day("swissgrid", PASS_DAY, 150.0, 110.0) + _day(
        "swissgrid", FAIL_DAY, 150.0, 110.0
    )
    daily_rows = [(PASS_DAY.isoformat(), 150.5, 110.5, OLD_STAMP, "d1")]
    silver, daily = _frames(spark, silver_rows, daily_rows)
    days = {r["local_date"] for r in build(spark, silver, daily).collect()}
    assert days == {PASS_DAY}
