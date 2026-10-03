from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from swiss_grid_lakehouse.gold.daily_load import CHANGE_COLS, KEY, build
from swiss_grid_lakehouse.silver.ch_load_silver import _silver_schema
from swiss_grid_lakehouse.silver.daily_rollup import expected_hours, rollup_rows

pytestmark = pytest.mark.spark

ZONE = ZoneInfo("Europe/Zurich")
STAMP = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


def _day(source, day, count=None, enduser=True, difference=True, skip=(), mw=100.0):
    """Hourly Silver rows of one local day: `count` hours from local midnight, minus `skip`.

    Rows are told apart by `ts_utc`, so the repeated hour of the clock change is two rows.
    `ts_local` is the wall clock, which shows the same time twice on that day.
    """
    start = datetime(day.year, day.month, day.day, tzinfo=ZONE).astimezone(UTC)
    count = expected_hours(day) if count is None else count
    rows = []
    for i in range(count):
        if i in skip:
            continue
        ts_utc = start + timedelta(hours=i)
        ts_local = ts_utc.astimezone(ZONE).replace(tzinfo=None)
        rows.append(
            (
                source,
                "CH",
                ts_utc,
                ts_local,
                ts_local.date(),
                60,
                mw + i,
                (mw / 2 + i) if enduser and source == "swissgrid" else None,
                (mw / 10 + i) if difference else None,
                "batch_a",
                STAMP,
                STAMP,
            )
        )
    return rows


def _silver(spark, rows):
    return spark.createDataFrame(rows, schema=_silver_schema())


def _mart(spark, rows):
    return {(r["source"], r["local_date"]): r for r in build(spark, _silver(spark, rows)).collect()}


def _close(a, b):
    return (a is None and b is None) or (a is not None and b is not None and abs(a - b) <= 1e-9)


def test_module_constants():
    assert KEY == ["source", "local_date"]
    assert CHANGE_COLS == [
        "hours",
        "expected_hours",
        "incomplete",
        "day_kind",
        "gwh_total",
        "gwh_enduser",
        "gwh_difference",
    ]


def test_columns_and_types(spark):
    got = build(spark, _silver(spark, _day("swissgrid", date(2026, 8, 30))))
    assert dict(got.dtypes) == {
        "source": "string",
        "local_date": "date",
        "hours": "int",
        "expected_hours": "int",
        "incomplete": "boolean",
        "day_kind": "string",
        "gwh_total": "double",
        "gwh_enduser": "double",
        "gwh_difference": "double",
    }


@pytest.mark.parametrize("source", ["swissgrid", "entsoe"])
def test_mart_equals_rollup_rows(spark, source):
    rows = (
        _day(source, date(2026, 8, 30))
        + _day(source, date(2026, 8, 31), count=20)
        + _day(source, date(2026, 3, 29))
        + _day(source, date(2026, 10, 25))
    )
    mart = _mart(spark, rows)
    expected = rollup_rows([(r[3], r[6], r[7]) for r in rows], source)
    assert len(expected) == 4
    for want in expected:
        got = mart[(source, want["local_date"])]
        assert got["hours"] == want["hours"]
        assert got["expected_hours"] == want["expected_hours"]
        assert got["incomplete"] == want["incomplete"]
        assert _close(got["gwh_total"], want["gwh_total"])
        assert _close(got["gwh_enduser"], want.get("gwh_enduser"))
    assert len(mart) == 4


def test_two_sources_stay_apart(spark):
    day = date(2026, 8, 30)
    mart = _mart(spark, _day("swissgrid", day, mw=100.0) + _day("entsoe", day, mw=200.0))
    assert set(mart) == {("swissgrid", day), ("entsoe", day)}
    assert mart[("swissgrid", day)]["gwh_enduser"] is not None
    assert mart[("entsoe", day)]["gwh_enduser"] is None
    assert mart[("entsoe", day)]["gwh_total"] > mart[("swissgrid", day)]["gwh_total"]


def test_short_day_when_clocks_go_forward(spark):
    day = date(2026, 3, 29)
    rows = _day("swissgrid", day)
    assert len(rows) == 23
    got = _mart(spark, rows)[("swissgrid", day)]
    assert got["expected_hours"] == 23
    assert got["hours"] == 23
    assert got["day_kind"] == "short"
    assert got["incomplete"] is False


def test_long_day_keeps_the_repeated_hour(spark):
    day = date(2026, 10, 25)
    rows = _day("swissgrid", day)
    assert len(rows) == 25
    wall_clock = [r[3] for r in rows]
    assert len(wall_clock) - len(set(wall_clock)) == 1
    got = _mart(spark, rows)[("swissgrid", day)]
    assert got["expected_hours"] == 25
    assert got["hours"] == 25
    assert got["day_kind"] == "long"
    assert got["incomplete"] is False
    assert _close(got["gwh_total"], sum(r[6] for r in rows) / 1000)


def test_normal_day_kind(spark):
    got = _mart(spark, _day("entsoe", date(2026, 8, 30)))[("entsoe", date(2026, 8, 30))]
    assert (got["expected_hours"], got["day_kind"]) == (24, "normal")


def test_incomplete_day_is_reported_not_scaled(spark):
    day = date(2026, 8, 31)
    rows = _day("swissgrid", day, skip=(5,))
    assert len(rows) == 23
    got = _mart(spark, rows)[("swissgrid", day)]
    assert got["hours"] == 23
    assert got["expected_hours"] == 24
    assert got["incomplete"] is True
    assert got["day_kind"] == "normal"
    partial = sum(r[6] for r in rows) / 1000
    assert _close(got["gwh_total"], partial)
    assert got["gwh_total"] < partial * 24 / 23
    assert got["gwh_difference"] is None


def test_enduser_is_null_when_an_hour_lacks_it(spark):
    day = date(2026, 8, 30)
    rows = _day("swissgrid", day)
    rows[3] = rows[3][:7] + (None,) + rows[3][8:]
    got = _mart(spark, rows)[("swissgrid", day)]
    assert got["gwh_enduser"] is None
    assert got["incomplete"] is False


def test_difference_is_summed_for_a_complete_day(spark):
    day = date(2026, 8, 30)
    rows = _day("swissgrid", day)
    got = _mart(spark, rows)[("swissgrid", day)]
    assert _close(got["gwh_difference"], sum(r[8] for r in rows) / 1000)


def test_difference_is_null_when_the_column_is_null(spark):
    day = date(2026, 8, 30)
    got = _mart(spark, _day("swissgrid", day, difference=False))[("swissgrid", day)]
    assert got["gwh_difference"] is None
    assert got["gwh_total"] is not None


def test_empty_silver_gives_an_empty_mart(spark):
    assert build(spark, _silver(spark, [])).count() == 0
