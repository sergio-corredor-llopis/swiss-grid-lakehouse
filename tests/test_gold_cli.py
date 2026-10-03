"""The Gold command line: marts, gate, merge, OPTIMIZE and time travel on a Silver Delta table."""

import re
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.spark

ZONE = ZoneInfo("Europe/Zurich")
STAMP = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
REGISTRY_STAMP = "2026-09-30T02:56:07.812085"


def _hours(source, day, count=24, first=0, batch="aaaaaaaaaaaa"):
    """Hourly Silver rows of one local day, `count` hours from the `first`-th hour."""
    start = datetime(day.year, day.month, day.day, tzinfo=ZONE).astimezone(UTC)
    rows = []
    for i in range(first, first + count):
        ts_utc = start + timedelta(hours=i)
        local = ts_utc.astimezone(ZONE)
        mw = 6000.0 + i
        enduser = mw - 500.0 if source == "swissgrid" else None
        difference = 500.0 if source == "swissgrid" else None
        rows.append(
            (source, "CH", ts_utc, local.replace(tzinfo=None), local.date(), 60, mw)
            + (enduser, difference, batch, STAMP, STAMP)
        )
    return rows


def _recorded_days():
    rows = []
    for source in ("entsoe", "swissgrid"):
        for day in (date(2026, 8, 30), date(2026, 8, 31)):
            rows += _hours(source, day)
    return rows


def _write_silver(spark, rows, path, mode="overwrite", nullable=False):
    from pyspark.sql import types as t

    from swiss_grid_lakehouse.silver.ch_load_silver import _silver_schema

    schema = _silver_schema()
    if nullable:
        schema = t.StructType([t.StructField(f.name, f.dataType, True) for f in schema.fields])
    spark.createDataFrame(rows, schema=schema).write.format("delta").mode(mode).save(path)


def _run(argv, capsys):
    from swiss_grid_lakehouse.gold.__main__ import main

    code = main(argv)
    return code, capsys.readouterr().out.splitlines()


def _version(spark, path):
    return spark.sql(f"DESCRIBE HISTORY delta.`{path}`").first()["version"]


def _args(tmp_path, *extra):
    return ["--silver", str(tmp_path / "silver"), "--target", str(tmp_path / "gold"), *extra]


def test_first_run_prints_the_marts_optimize_and_version(spark, tmp_path, capsys):
    _write_silver(spark, _recorded_days(), str(tmp_path / "silver"))
    code, lines = _run(_args(tmp_path), capsys)
    assert code == 0
    assert lines[0] == "SKIPPED gold_daily_control reason=no_daily_bronze"
    assert lines[1] == "GOLD GATE: PASS checks=4 skipped=1"
    assert lines[2:5] == [
        "MART g1 rows=4 inserted=4 updated=0",
        "MART g3 rows=96 inserted=96 updated=0",
        "MART g4 rows=4 inserted=4 updated=0",
    ]
    assert re.fullmatch(
        r"OPTIMIZE table=g3 (files_removed=\d+ files_added=\d+|refused reason=\w+)", lines[5]
    )
    assert lines[6] == "VERSION table=g1 v=0 rows=4 now=4"
    assert len(lines) == 7
    assert not any(line.startswith("MART g2") for line in lines)


def test_second_run_changes_nothing(spark, tmp_path, capsys):
    _write_silver(spark, _recorded_days(), str(tmp_path / "silver"))
    assert _run(_args(tmp_path), capsys)[0] == 0
    code, lines = _run(_args(tmp_path), capsys)
    assert code == 0
    marts = [line for line in lines if line.startswith("MART")]
    assert len(marts) == 3
    assert all(line.endswith("inserted=0 updated=0") for line in marts)


def _daily_bronze(spark, path):
    from swiss_grid_lakehouse.daily.ogd_client import write_bronze

    rows = [
        ("ogd", "2026-08-30", 149.0, 110.0, STAMP, "dddddddddddd", REGISTRY_STAMP),
        ("ogd", "2026-08-31", 149.0, 125.0, STAMP, "dddddddddddd", REGISTRY_STAMP),
    ]
    assert write_bronze(spark, rows, path)


def test_a_daily_bronze_table_adds_the_control_mart(spark, tmp_path, capsys):
    _write_silver(spark, _recorded_days(), str(tmp_path / "silver"))
    _daily_bronze(spark, str(tmp_path / "daily"))
    code, lines = _run(_args(tmp_path, "--daily", str(tmp_path / "daily")), capsys)
    assert code == 0
    assert not any(line.startswith("SKIPPED") for line in lines)
    assert lines[0] == "GOLD GATE: PASS checks=5"
    assert "MART g2 rows=6 inserted=6 updated=0" in lines


def test_the_late_hour_updates_one_row_and_the_old_version_stays_incomplete(
    spark, tmp_path, capsys
):
    from swiss_grid_lakehouse.gold.maintain import version_query

    silver = str(tmp_path / "silver")
    day = date(2026, 8, 30)
    _write_silver(spark, _hours("entsoe", day, count=23), silver)
    code, lines = _run(_args(tmp_path), capsys)
    assert code == 0
    assert "MART g1 rows=1 inserted=1 updated=0" in lines
    g1 = str(tmp_path / "gold" / "gold_daily_load")
    assert spark.read.format("delta").load(g1).first()["incomplete"] is True

    _write_silver(
        spark, _hours("entsoe", day, count=1, first=23, batch="bbbbbbbbbbbb"), silver, "append"
    )
    code, lines = _run(_args(tmp_path), capsys)
    assert code == 0
    assert "MART g1 rows=1 inserted=0 updated=1" in lines
    assert version_query(spark, g1, 0).first()["incomplete"] is True
    now = spark.read.format("delta").load(g1).first()
    assert now["incomplete"] is False
    assert now["hours"] == 24


def test_a_planted_row_with_a_null_date_fails_the_gate_and_merges_nothing(spark, tmp_path, capsys):
    _write_silver(spark, _recorded_days(), str(tmp_path / "silver"))
    assert _run(_args(tmp_path), capsys)[0] == 0
    tables = ["gold_daily_load", "gold_hourly_profile", "gold_data_quality"]
    before = {t: _version(spark, str(tmp_path / "gold" / t)) for t in tables}

    planted = _hours("entsoe", date(2026, 9, 1), count=1)
    planted[0] = planted[0][:4] + (None,) + planted[0][5:]
    bad = str(tmp_path / "silver_bad")
    _write_silver(spark, _recorded_days() + planted, bad, nullable=True)
    code, lines = _run(["--silver", bad, "--target", str(tmp_path / "gold")], capsys)
    assert code == 2
    assert lines[-1].startswith("GOLD GATE: FAIL")
    assert not any(line.startswith(("MART", "OPTIMIZE", "VERSION")) for line in lines)
    assert {t: _version(spark, str(tmp_path / "gold" / t)) for t in tables} == before


def test_a_missing_silver_table_exits_1(spark, tmp_path, capsys):
    code, _ = _run(
        ["--silver", str(tmp_path / "nothing"), "--target", str(tmp_path / "gold")], capsys
    )
    assert code == 1
    assert not (tmp_path / "gold").exists()


def test_a_missing_argument_exits_1(capsys):
    from swiss_grid_lakehouse.gold.__main__ import main

    assert main(["--silver", "x"]) == 1
    capsys.readouterr()
