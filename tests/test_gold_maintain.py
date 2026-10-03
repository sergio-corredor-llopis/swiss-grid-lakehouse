import pytest

from swiss_grid_lakehouse.gold.maintain import (
    optimize,
    optimize_line,
    version_line,
    version_query,
)

pytestmark = pytest.mark.spark

SCHEMA = "source string, weekday int, avg_mw double"


def _append(spark, path, rows):
    spark.createDataFrame(rows, schema=SCHEMA).coalesce(1).write.format("delta").mode(
        "append"
    ).save(path)


def _three_batches(spark, path):
    _append(spark, path, [("entsoe", 1, 5000.0), ("swissgrid", 1, 4900.0)])
    _append(spark, path, [("entsoe", 2, 5100.0), ("swissgrid", 2, 5000.0)])
    _append(spark, path, [("entsoe", 3, 5200.0), ("swissgrid", 3, 5100.0)])


def test_optimize_compacts_three_appends(spark, tmp_path):
    path = str(tmp_path / "mart")
    _three_batches(spark, path)
    result = optimize(spark, path, ["source", "weekday"])
    assert result["refused"] is None
    assert result["files_removed"] >= 2
    assert result["files_added"] == 1
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").filter("operation = 'OPTIMIZE'")
    metrics = history.first()["operationMetrics"]
    assert int(metrics["numRemovedFiles"]) == result["files_removed"]
    assert int(metrics["numAddedFiles"]) == 1
    assert spark.read.format("delta").load(path).count() == 6


def test_version_query_reads_the_old_rows_after_an_update(spark, tmp_path):
    path = str(tmp_path / "mart")
    _append(spark, path, [("entsoe", 1, 5000.0), ("swissgrid", 1, 4900.0)])
    spark.sql(f"UPDATE delta.`{path}` SET avg_mw = 1.0 WHERE source = 'entsoe'")
    then = version_query(spark, path, 0)
    assert sorted(r["avg_mw"] for r in then.collect()) == [4900.0, 5000.0]
    now = spark.read.format("delta").load(path)
    assert sorted(r["avg_mw"] for r in now.collect()) == [1.0, 4900.0]


def test_refused_optimize_is_reported_not_raised(spark, tmp_path, monkeypatch):
    path = str(tmp_path / "mart")
    _three_batches(spark, path)
    real_sql = spark.sql

    def refuse(statement, *args, **kwargs):
        if statement.startswith("OPTIMIZE"):
            raise RuntimeError("ZORDER is not supported on this compute")
        return real_sql(statement, *args, **kwargs)

    monkeypatch.setattr(spark, "sql", refuse)
    result = optimize(spark, path, ["source", "weekday"])
    assert result == {
        "files_removed": 0,
        "files_added": 0,
        "refused": "zorder_is_not_supported_on_this_compute",
    }
    assert optimize_line("g3", result) == (
        "OPTIMIZE table=g3 refused reason=zorder_is_not_supported_on_this_compute"
    )


def test_printed_forms():
    done = {"files_removed": 3, "files_added": 1, "refused": None}
    assert optimize_line("g3", done) == "OPTIMIZE table=g3 files_removed=3 files_added=1"
    assert version_line("g1", 0, 23, 24) == "VERSION table=g1 v=0 rows=23 now=24"
