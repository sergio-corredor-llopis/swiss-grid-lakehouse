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
