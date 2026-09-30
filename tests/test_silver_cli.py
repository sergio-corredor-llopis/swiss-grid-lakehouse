"""The Silver command line: merge on a clean batch, exit 2 on a bad one."""

import pytest

pytestmark = pytest.mark.spark


def test_cli_merges_and_is_idempotent(spark, bronze_path, tmp_path, capsys):
    from swiss_grid_lakehouse.silver.__main__ import main

    target = str(tmp_path / "silver")
    argv = ["--bronze", bronze_path, "--target", target]
    assert main(argv) == 0
    assert "GATE: PASS checks=6" in capsys.readouterr().out
    assert main(argv) == 0
    assert "merged inserted=0 updated=0 rows=48" in capsys.readouterr().out


def _swissgrid_bronze(spark, tmp_path):
    from datetime import UTC, datetime
    from pathlib import Path

    from swiss_grid_lakehouse.bronze.swissgrid_energy_bronze import to_bronze_rows, write_bronze

    csv = Path(__file__).parent / "fixtures" / "swissgrid_ch_energy_2026-08-30.csv"
    rows = to_bronze_rows(csv, datetime(2026, 9, 3, 12, 0, tzinfo=UTC))
    path = str(tmp_path / "bronze_swissgrid")
    write_bronze(spark, rows, path)
    return path, rows


def test_cli_swissgrid_merges_twice_with_the_same_count(spark, tmp_path, capsys):
    from swiss_grid_lakehouse.silver.__main__ import main

    bronze, _ = _swissgrid_bronze(spark, tmp_path)
    target = str(tmp_path / "silver")
    argv = ["--bronze", bronze, "--target", target, "--source", "swissgrid"]
    assert main(argv) == 0
    out = capsys.readouterr().out
    assert "GATE: PASS checks=6" in out
    assert "merged inserted=48 updated=0 rows=48" in out
    assert main(argv) == 0
    assert "merged inserted=0 updated=0 rows=48" in capsys.readouterr().out


def test_cli_swissgrid_planted_row_exits_2_and_keeps_silver(spark, bronze_path, tmp_path, capsys):
    from datetime import UTC, datetime

    from swiss_grid_lakehouse.bronze.swissgrid_energy_bronze import write_bronze
    from swiss_grid_lakehouse.silver.__main__ import main
    from swiss_grid_lakehouse.silver.swissgrid_hourly import SERIES

    bronze, rows = _swissgrid_bronze(spark, tmp_path)
    target = str(tmp_path / "silver")
    assert main(["--bronze", bronze_path, "--target", target]) == 0
    assert main(["--bronze", bronze, "--target", target, "--source", "swissgrid"]) == 0
    capsys.readouterr()
    version = spark.sql(f"DESCRIBE HISTORY delta.`{target}`").first()["version"]

    # Four slots of the hour that starts 2026-08-30T12:00Z (14:00 local) at -250 kWh each: the
    # hour sums to -1 MW, below the load range. The labels are slot starts.
    starts = {"30.08.2026 14:00", "30.08.2026 14:15", "30.08.2026 14:30", "30.08.2026 14:45"}
    later = datetime(2026, 9, 3, 13, 0, tzinfo=UTC)
    planted = [
        r[:5] + (-250.0, later, "planted", "planted.csv")
        for r in rows
        if r[1] == SERIES and r[3] in starts
    ]
    assert len(planted) == 4
    write_bronze(spark, planted, bronze)

    argv = ["--bronze", bronze, "--target", target, "--source", "swissgrid"]
    assert main(argv) == 2
    assert "GATE: FAIL range_load_mw=1" in capsys.readouterr().out
    assert spark.read.format("delta").load(target).count() == 96
    latest = spark.sql(f"DESCRIBE HISTORY delta.`{target}`").first()["version"]
    assert latest == version
