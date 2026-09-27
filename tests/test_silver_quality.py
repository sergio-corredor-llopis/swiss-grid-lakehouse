from datetime import UTC, datetime, timedelta

import pytest

from swiss_grid_lakehouse.bronze.entsoe_load_bronze import COLUMNS, _schema
from swiss_grid_lakehouse.silver import to_silver
from swiss_grid_lakehouse.silver.quality import run_gate

pytestmark = pytest.mark.spark


def _candidate(spark, rows):
    bronze = spark.createDataFrame(rows, schema=_schema()).select(*COLUMNS)
    return to_silver(bronze)


def _rows(hours, load=5000.0, batch="a"):
    pulled = datetime(2026, 9, 3, tzinfo=UTC)
    base = datetime(2026, 9, 1, tzinfo=UTC)
    return [
        ("entsoe", "CH", pulled, batch, "x.xml", base + timedelta(hours=h), load) for h in hours
    ]


def test_gate_passes_on_fixture(spark, bronze_path):
    silver = to_silver(spark.read.format("delta").load(bronze_path))
    result = run_gate(silver, current_count=0)
    assert result.passed
    assert result.failures == {}
    assert result.rejected_df.count() == 0
    assert result.summary() == "GATE: PASS"


def test_gate_fails_on_negative_load(spark):
    rows = _rows(range(6))
    rows[3] = rows[3][:6] + (-50.0,)
    result = run_gate(_candidate(spark, rows), current_count=0)
    assert not result.passed
    assert result.failures == {"range_load_mw": 1}
    rejected = result.rejected_df.collect()
    assert len(rejected) == 1
    assert rejected[0]["check"] == "range_load_mw"
    assert rejected[0]["load_mw"] == -50.0
    assert result.summary() == "GATE: FAIL range_load_mw=1"


def test_gate_fails_on_duplicate_key(spark):
    df = _candidate(spark, _rows(range(6)))
    result = run_gate(df.union(df.limit(1)), current_count=0)
    assert not result.passed
    assert result.failures["key_unique"] == 2
    assert {r["check"] for r in result.rejected_df.collect()} == {"key_unique"}


def test_gate_fails_on_missing_hour(spark):
    result = run_gate(_candidate(spark, _rows([0, 1, 3, 4])), current_count=0)
    assert not result.passed
    assert result.failures == {"missing_hours": 1}
    assert result.rejected_df.count() == 0
    assert run_gate(_candidate(spark, _rows([0, 1, 3, 4])), 0, max_missing=1).passed


def test_gate_fails_when_count_drops_below_current(spark):
    result = run_gate(_candidate(spark, _rows(range(6))), current_count=10)
    assert result.failures == {"count_below_current": 4}


def test_gate_freshness_is_off_by_default(spark):
    df = _candidate(spark, _rows(range(6)))
    assert run_gate(df, 0).passed
    assert run_gate(df, 0, max_age=timedelta(hours=1)).failures == {"freshness": 1}
