from datetime import date, datetime

import pytest

from swiss_grid_lakehouse.gold.gate import run_gold_gate
from swiss_grid_lakehouse.silver.ch_load_silver import _silver_schema

pytestmark = pytest.mark.spark

DAY = date(2026, 8, 30)


def _silver(spark):
    rows = []
    for source, mw in (("entsoe", 5000.0), ("swissgrid", 4800.0)):
        for hour in range(2):
            ts = datetime(2026, 8, 30, hour)
            rows.append(
                (source, "CH", ts, ts, DAY, 60, mw, None, None, "pull-1", ts, ts),
            )
    return spark.createDataFrame(rows, schema=_silver_schema())


def _g1(spark, gwh_entsoe=10.0, gwh_swissgrid=9.6):
    return spark.createDataFrame(
        [("entsoe", DAY, 2, gwh_entsoe), ("swissgrid", DAY, 2, gwh_swissgrid)],
        schema="source string, local_date date, hours int, gwh_total double",
    )


def _g2(spark):
    return spark.createDataFrame(
        [("swissgrid", "total", DAY, 9.6), ("entsoe", "total", DAY, 10.0)],
        schema="source string, series string, local_date date, pipeline_gwh double",
    )


def _g3(spark, n_days=1):
    return spark.createDataFrame(
        [("entsoe", 7, 0, n_days), ("swissgrid", 7, 0, n_days)],
        schema="source string, weekday int, hour_local int, n_days int",
    )


def _g4(spark, hours=2):
    return spark.createDataFrame(
        [("entsoe", DAY, hours), ("swissgrid", DAY, 2)],
        schema="source string, local_date date, hours int",
    )


def _marts(spark, **over):
    marts = {"g1": _g1(spark), "g2": _g2(spark), "g3": _g3(spark), "g4": _g4(spark)}
    marts.update(over)
    return marts


def test_good_set_passes_with_five_checks(spark):
    result = run_gold_gate(_silver(spark), _marts(spark))
    assert result.passed
    assert (result.checks, result.skipped) == (5, 0)
    assert result.summary() == "GOLD GATE: PASS checks=5"


def test_missing_g2_is_skipped(spark):
    result = run_gold_gate(_silver(spark), _marts(spark, g2=None))
    assert result.passed
    assert (result.checks, result.skipped) == (4, 1)
    assert result.summary() == "GOLD GATE: PASS checks=4 skipped=1"


def test_g1_off_by_one_gwh_fails(spark):
    result = run_gold_gate(_silver(spark), _marts(spark, g1=_g1(spark, gwh_entsoe=11.0)))
    assert not result.passed
    assert result.failures == {"gwh_total": 1}
    assert result.summary() == "GOLD GATE: FAIL gwh_total=1"


def test_null_key_fails(spark):
    g3 = spark.createDataFrame(
        [("entsoe", 7, 0, 1), (None, 7, 1, 1)],
        schema="source string, weekday int, hour_local int, n_days int",
    )
    result = run_gold_gate(_silver(spark), _marts(spark, g3=g3))
    assert not result.passed
    assert result.failures == {"null_key": 1}


def test_g4_hours_different_from_g1_fails(spark):
    result = run_gold_gate(_silver(spark), _marts(spark, g4=_g4(spark, hours=1)))
    assert not result.passed
    assert result.failures == {"hours_match": 1}
    assert result.summary() == "GOLD GATE: FAIL hours_match=1"


def test_g3_row_without_a_day_fails(spark):
    result = run_gold_gate(_silver(spark), _marts(spark, g3=_g3(spark, n_days=0)))
    assert result.failures == {"n_days": 2}
