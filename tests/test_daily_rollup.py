from datetime import date, datetime, timedelta

import pytest

from swiss_grid_lakehouse.silver.daily_rollup import expected_hours, rollup_daily, rollup_rows


def _hours(day, count, mw=1000.0, enduser=None, first=0):
    """`count` hourly rows of one local day, starting at local hour `first`."""
    start = datetime(day.year, day.month, day.day, first)
    return [(start + timedelta(hours=i), mw, enduser) for i in range(count)]


def test_expected_hours_on_the_clock_change_days():
    assert expected_hours(date(2026, 3, 29)) == 23
    assert expected_hours(date(2026, 10, 25)) == 25
    assert expected_hours(date(2026, 3, 28)) == 24
    assert expected_hours(date(2026, 10, 26)) == 24


def test_ordinary_day_has_24_hours():
    (day,) = rollup_rows(_hours(date(2026, 8, 30), 24, mw=6000.0), "entsoe")
    assert day["local_date"] == date(2026, 8, 30)
    assert day["hours"] == 24
    assert day["expected_hours"] == 24
    assert day["incomplete"] is False
    assert day["gwh_total"] == pytest.approx(144.0)
    assert "gwh_enduser" not in day


def test_spring_forward_day_is_complete_with_23_hours():
    (day,) = rollup_rows(_hours(date(2026, 3, 29), 23), "entsoe")
    assert (day["hours"], day["expected_hours"], day["incomplete"]) == (23, 23, False)
    assert day["gwh_total"] == pytest.approx(23.0)


def test_spring_forward_day_with_24_rows_is_not_short():
    (day,) = rollup_rows(_hours(date(2026, 3, 29), 24), "entsoe")
    assert day["hours"] == 24
    assert day["incomplete"] is False


def test_fall_back_day_is_complete_with_25_hours():
    # local 02:00 happens twice: the two rows carry the same ts_local and both count
    rows = _hours(date(2026, 10, 25), 25)
    rows[3:] = [(ts - timedelta(hours=1), mw, e) for ts, mw, e in rows[3:]]
    rows = sorted(rows, key=lambda r: r[0])
    assert [r[0].hour for r in rows].count(2) == 2
    (day,) = rollup_rows(rows, "entsoe")
    assert (day["hours"], day["expected_hours"], day["incomplete"]) == (25, 25, False)
    assert day["gwh_total"] == pytest.approx(25.0)


def test_incomplete_day_reports_its_real_hour_count_and_is_not_scaled():
    (day,) = rollup_rows(_hours(date(2026, 8, 31), 20, mw=5000.0), "entsoe")
    assert day["incomplete"] is True
    assert day["hours"] == 20
    assert day["expected_hours"] == 24
    assert day["gwh_total"] == pytest.approx(100.0)  # 20 h x 5000 MW; not 120


def test_incomplete_dst_days_are_measured_against_their_own_length():
    short_spring = rollup_rows(_hours(date(2026, 3, 29), 22), "entsoe")[0]
    short_autumn = rollup_rows(_hours(date(2026, 10, 25), 24), "entsoe")[0]
    assert short_spring["incomplete"] is True
    assert short_autumn["incomplete"] is True
    assert short_autumn["hours"] == 24


def test_days_are_sorted_and_split_at_local_midnight():
    rows = _hours(date(2026, 9, 2), 24) + _hours(date(2026, 9, 1), 24)
    days = rollup_rows(rows, "entsoe")
    assert [d["local_date"] for d in days] == [date(2026, 9, 1), date(2026, 9, 2)]
    assert [d["hours"] for d in days] == [24, 24]


def test_swissgrid_reports_total_and_enduser():
    rows = _hours(date(2026, 8, 30), 24, mw=6000.0, enduser=4500.0)
    (day,) = rollup_rows(rows, "swissgrid")
    assert day["gwh_total"] == pytest.approx(144.0)
    assert day["gwh_enduser"] == pytest.approx(108.0)


def test_entsoe_rows_with_null_enduser_have_no_enduser_key():
    (day,) = rollup_rows(_hours(date(2026, 8, 30), 24, mw=6000.0, enduser=None), "entsoe")
    assert set(day) == {"local_date", "hours", "expected_hours", "incomplete", "gwh_total"}


def test_swissgrid_partial_enduser_is_none_not_a_short_sum():
    rows = _hours(date(2026, 8, 30), 24, mw=6000.0, enduser=4500.0)
    rows[5] = (rows[5][0], 6000.0, None)
    (day,) = rollup_rows(rows, "swissgrid")
    assert day["gwh_total"] == pytest.approx(144.0)
    assert day["gwh_enduser"] is None


def test_swissgrid_without_any_enduser_value_is_none():
    (day,) = rollup_rows(_hours(date(2026, 8, 30), 24, mw=6000.0), "swissgrid")
    assert day["gwh_enduser"] is None


def test_unknown_source_is_rejected():
    with pytest.raises(ValueError):
        rollup_rows([], "other")


def test_empty_input_gives_no_days():
    assert rollup_rows([], "swissgrid") == []


@pytest.mark.spark
def test_rollup_daily_on_silver_frame_uses_source_and_ts_local(spark):
    from pyspark.sql import types as t

    schema = t.StructType(
        [
            t.StructField("source", t.StringType(), False),
            t.StructField("ts_local", t.TimestampNTZType(), False),
            t.StructField("load_mw", t.DoubleType(), False),
            t.StructField("load_enduser_mw", t.DoubleType(), True),
        ]
    )
    rows = [("swissgrid", ts, mw, 750.0) for ts, mw, _ in _hours(date(2026, 3, 29), 23)]
    rows += [("entsoe", ts, mw, None) for ts, mw, _ in _hours(date(2026, 3, 28), 24)]
    df = spark.createDataFrame(rows, schema=schema)
    (swiss,) = rollup_daily(df, "swissgrid")
    (entsoe,) = rollup_daily(df, "entsoe")
    assert (swiss["local_date"], swiss["hours"], swiss["incomplete"]) == (
        date(2026, 3, 29),
        23,
        False,
    )
    assert swiss["gwh_enduser"] == pytest.approx(23 * 0.75)
    assert (entsoe["local_date"], entsoe["hours"]) == (date(2026, 3, 28), 24)
    assert "gwh_enduser" not in entsoe


@pytest.mark.spark
def test_rollup_daily_on_recorded_entsoe_hours(spark, bronze_path):
    from swiss_grid_lakehouse.silver import to_silver

    silver = to_silver(spark.read.format("delta").load(bronze_path))
    days = rollup_daily(silver, "entsoe")
    for d in days:
        print(f"ROLLUP source=entsoe {d['local_date']} hours={d['hours']} gwh={d['gwh_total']:.2f}")
    assert [d["hours"] for d in days] == [24, 24]
    assert not any(d["incomplete"] for d in days)
