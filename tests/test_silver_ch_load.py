from datetime import UTC, datetime

import pytest

from swiss_grid_lakehouse.bronze.entsoe_load_bronze import COLUMNS, _schema
from swiss_grid_lakehouse.silver import SILVER_COLUMNS, merge_silver, to_silver

pytestmark = pytest.mark.spark


def _bronze(spark, rows):
    return spark.createDataFrame(rows, schema=_schema()).select(*COLUMNS)


def _row(ts, load, pulled, batch):
    return ("entsoe", "CH", pulled, batch, "x.xml", ts, load)


def test_columns_and_row_count(spark, bronze_path):
    silver = to_silver(spark.read.format("delta").load(bronze_path))
    assert silver.columns == SILVER_COLUMNS
    assert silver.count() == 48
    assert silver.filter("resolution_min = 60").count() == 48


def test_dedupe_keeps_latest_pull(spark):
    ts = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    old = datetime(2026, 9, 2, tzinfo=UTC)
    new = datetime(2026, 9, 3, tzinfo=UTC)
    silver = to_silver(_bronze(spark, [_row(ts, 5000.0, old, "a"), _row(ts, 5100.0, new, "b")]))
    rows = silver.collect()
    assert len(rows) == 1
    assert rows[0]["load_mw"] == 5100.0
    assert rows[0]["batch_id"] == "b"


def test_dst_end_keeps_both_repeated_hours(spark):
    pulled = datetime(2026, 10, 26, tzinfo=UTC)
    # 2026-10-25: clocks go back at 01:00 UTC, so local 02:00 happens twice.
    hours = [22, 23]
    stamps = [datetime(2026, 10, 24, h, tzinfo=UTC) for h in hours]
    stamps += [datetime(2026, 10, 25, h, tzinfo=UTC) for h in (0, 1, 2)]
    silver = to_silver(_bronze(spark, [_row(t, 6000.0, pulled, "d") for t in stamps]))
    rows = silver.orderBy("ts_utc").collect()
    assert len(rows) == 5
    local = [r["ts_local"].strftime("%Y-%m-%d %H:%M") for r in rows]
    assert local == [
        "2026-10-25 00:00",
        "2026-10-25 01:00",
        "2026-10-25 02:00",
        "2026-10-25 02:00",
        "2026-10-25 03:00",
    ]
    assert str(rows[0]["local_date"]) == "2026-10-25"


def test_merge_twice_inserts_then_nothing(spark, bronze_path, tmp_path):
    target = str(tmp_path / "silver")
    bronze = spark.read.format("delta").load(bronze_path)
    assert merge_silver(spark, to_silver(bronze), target) == (48, 0)
    assert merge_silver(spark, to_silver(bronze), target) == (0, 0)
    table = spark.read.format("delta").load(target)
    assert table.count() == 48
    assert table.select("source", "area", "ts_utc").distinct().count() == 48


def test_merge_updates_only_newer_changed_rows(spark, tmp_path):
    target = str(tmp_path / "silver")
    ts = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    first = datetime(2026, 9, 2, tzinfo=UTC)
    later = datetime(2026, 9, 3, tzinfo=UTC)
    merge_silver(spark, to_silver(_bronze(spark, [_row(ts, 5000.0, first, "a")])), target)
    older = to_silver(_bronze(spark, [_row(ts, 9000.0, datetime(2026, 9, 1, tzinfo=UTC), "z")]))
    assert merge_silver(spark, older, target) == (0, 0)
    newer = to_silver(_bronze(spark, [_row(ts, 5200.0, later, "b")]))
    assert merge_silver(spark, newer, target) == (0, 1)
    assert spark.read.format("delta").load(target).first()["load_mw"] == 5200.0
