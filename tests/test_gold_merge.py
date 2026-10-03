from datetime import date

import pytest

from swiss_grid_lakehouse.gold.merge import mart_line, merge_mart

pytestmark = pytest.mark.spark

KEY = ["source", "local_date"]
CHANGE = ["hours", "gwh_total"]
SCHEMA = "source string, local_date date, hours int, gwh_total double, note string"


def _frame(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


def _rows(day=30):
    return [
        ("entsoe", date(2026, 8, day), 24, 150.0, "a"),
        ("swissgrid", date(2026, 8, day), 24, 149.0, "a"),
        ("entsoe", date(2026, 8, day + 1), 24, 160.0, "a"),
        ("swissgrid", date(2026, 8, day + 1), 24, 158.0, "a"),
    ]


def _history_versions(spark, path):
    return spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version").count()


def test_first_call_creates_version_zero(spark, tmp_path):
    path = str(tmp_path / "mart")
    assert merge_mart(spark, _frame(spark, _rows()), path, KEY, CHANGE) == (4, 0)
    assert _history_versions(spark, path) == 1
    assert spark.read.format("delta").option("versionAsOf", 0).load(path).count() == 4


def test_rerun_changes_nothing(spark, tmp_path):
    path = str(tmp_path / "mart")
    merge_mart(spark, _frame(spark, _rows()), path, KEY, CHANGE)
    assert merge_mart(spark, _frame(spark, _rows()), path, KEY, CHANGE) == (0, 0)


def test_changed_change_column_updates_one_row(spark, tmp_path):
    path = str(tmp_path / "mart")
    merge_mart(spark, _frame(spark, _rows()), path, KEY, CHANGE)
    rows = _rows()
    rows[0] = ("entsoe", date(2026, 8, 30), 24, 151.5, "a")
    assert merge_mart(spark, _frame(spark, rows), path, KEY, CHANGE) == (0, 1)
    got = spark.read.format("delta").load(path).filter("source = 'entsoe' AND hours = 24")
    assert got.filter("local_date = '2026-08-30'").first()["gwh_total"] == 151.5


def test_non_change_column_does_not_update(spark, tmp_path):
    path = str(tmp_path / "mart")
    merge_mart(spark, _frame(spark, _rows()), path, KEY, CHANGE)
    rows = [(s, d, h, g, "changed") for s, d, h, g, _ in _rows()]
    assert merge_mart(spark, _frame(spark, rows), path, KEY, CHANGE) == (0, 0)
    assert spark.read.format("delta").load(path).filter("note = 'a'").count() == 4


def test_new_key_is_inserted(spark, tmp_path):
    path = str(tmp_path / "mart")
    merge_mart(spark, _frame(spark, _rows()), path, KEY, CHANGE)
    rows = _rows() + [("entsoe", date(2026, 9, 1), 24, 170.0, "a")]
    assert merge_mart(spark, _frame(spark, rows), path, KEY, CHANGE) == (1, 0)
    assert spark.read.format("delta").load(path).count() == 5


def test_table_name_target(spark, tmp_path):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS gold_merge_test LOCATION '{tmp_path / 'db'}'")
    name = "gold_merge_test.mart"
    try:
        assert merge_mart(spark, _frame(spark, _rows()), name, KEY, CHANGE) == (4, 0)
        assert merge_mart(spark, _frame(spark, _rows()), name, KEY, CHANGE) == (0, 0)
        rows = _rows()
        rows[1] = ("swissgrid", date(2026, 8, 30), 23, 140.0, "a")
        assert merge_mart(spark, _frame(spark, rows), name, KEY, CHANGE) == (0, 1)
    finally:
        spark.sql("DROP TABLE IF EXISTS gold_merge_test.mart")
        spark.sql("DROP DATABASE IF EXISTS gold_merge_test")


def test_mart_line():
    assert mart_line("g1", 4, 4, 0) == "MART g1 rows=4 inserted=4 updated=0"
