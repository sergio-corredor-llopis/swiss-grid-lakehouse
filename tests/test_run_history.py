from datetime import UTC, datetime

import pytest

from swiss_grid_lakehouse.runs import COLUMNS, compute_status, gate_text, record_run, run_line
from swiss_grid_lakehouse.runs.history import FAILED, PARTIAL, SUCCESS

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


def _row(**over):
    row = {
        "run_id": "run-1",
        "started_at": NOW,
        "target": "ch-load",
        "source": "recorded",
        "bronze_rows": 48,
        "silver_inserted": 96,
        "silver_updated": 0,
        "gate": "PASS checks=5",
        "compare": "OK",
        "reconcile_diff": None,
        "gold_inserted": 4,
        "gold_updated": 0,
    }
    row.update(over)
    return row


def test_complete_figures_and_passed_gate_are_success():
    assert compute_status(_row()) == SUCCESS


def test_failed_gate_is_failed():
    assert compute_status(_row(gate="FAIL g1=1")) == FAILED


def test_failed_gate_wins_over_complete_figures():
    row = _row(gate="FAIL g1_gwh=1", gold_inserted=4, gold_updated=0)
    assert compute_status(row) != SUCCESS


def test_missing_gate_is_failed():
    assert compute_status(_row(gate=None)) == FAILED


def test_missing_figure_is_partial():
    assert compute_status(_row(gold_inserted=None)) == PARTIAL


def test_status_in_the_row_is_not_trusted():
    assert compute_status(_row(status="SUCCESS", gate="FAIL g1=1")) == FAILED


def test_azure_target_needs_a_reconcile_difference():
    assert compute_status(_row(target="ch-load-azure")) == PARTIAL
    assert compute_status(_row(target="ch-load-azure", reconcile_diff=1.8)) == SUCCESS


def test_reconcile_difference_beyond_tolerance_is_failed():
    row = _row(target="ch-load-azure", reconcile_diff=-9.0)
    assert compute_status(row) == FAILED
    assert compute_status(row, tolerance=10.0) == SUCCESS


def test_gate_text_drops_the_prefix():
    assert gate_text("GOLD GATE: PASS checks=5") == "PASS checks=5"
    assert gate_text("GOLD GATE: FAIL g1=1") == "FAIL g1=1"
    assert gate_text(None) is None


def test_run_line_shows_the_computed_status():
    assert run_line(_row()) == "RUN id=run-1 target=ch-load status=SUCCESS"
    assert run_line(_row(gate="FAIL g1=1")) == "RUN id=run-1 target=ch-load status=FAILED"


def test_columns_follow_the_ledger_definition():
    assert COLUMNS[0] == "run_id" and COLUMNS[-1] == "recorded_at"
    assert len(COLUMNS) == 14


def test_record_run_rejects_an_unknown_column_and_a_missing_run_id():
    with pytest.raises(ValueError, match="unknown ledger columns"):
        record_run(None, _row(speed=1))
    with pytest.raises(ValueError, match="run_id"):
        record_run(None, _row(run_id=""))


@pytest.mark.spark
def test_same_run_id_twice_leaves_one_row(spark, tmp_path):
    table = str(tmp_path / "pipeline_runs")
    assert record_run(spark, _row(), table, now=NOW) == (1, 0)
    assert record_run(spark, _row(), table, now=NOW) == (0, 0)
    frame = spark.read.format("delta").load(table)
    assert frame.count() == 1
    assert frame.first()["status"] == SUCCESS


@pytest.mark.spark
def test_changed_figures_for_one_run_id_update_the_row(spark, tmp_path):
    table = str(tmp_path / "pipeline_runs")
    record_run(spark, _row(gold_inserted=None), table, now=NOW)
    assert record_run(spark, _row(), table, now=NOW) == (0, 1)
    frame = spark.read.format("delta").load(table)
    assert frame.count() == 1
    assert frame.first()["status"] == SUCCESS


@pytest.mark.spark
def test_failed_gate_writes_a_failed_row(spark, tmp_path):
    table = str(tmp_path / "pipeline_runs")
    record_run(spark, _row(run_id="run-2", gate="FAIL g1=1", gold_inserted=None), table, now=NOW)
    frame = spark.read.format("delta").load(table)
    row = frame.filter("run_id = 'run-2'").first()
    assert row["gate"] == "FAIL g1=1"
    assert row["status"] == FAILED
    assert row["gold_inserted"] is None


@pytest.mark.spark
def test_distinct_run_ids_make_distinct_rows(spark, tmp_path):
    from swiss_grid_lakehouse.runs import history_line

    table = str(tmp_path / "pipeline_runs")
    record_run(spark, _row(run_id="a"), table, now=NOW)
    record_run(spark, _row(run_id="b"), table, now=NOW)
    assert history_line(spark, table) == "HISTORY runs=2 success=2 failed=0"


# --- local runner: python -m swiss_grid_lakehouse.runs (no Spark: stages and ledger are stubs) ---


def _stage(text, code=0, calls=None, name=""):
    def main(argv):
        if calls is not None:
            calls.append(name)
        print(text, end="")
        return code

    return main


def _stub_steps(calls, silver_code=0, silver_text="merged inserted=48 updated=0 rows=48\n"):
    def steps(args, lake):
        return [
            ("bronze_entsoe", _stage("wrote 48 rows batch_id=0a1b2c3d\n", 0, calls, "bronze"), []),
            ("silver_entsoe", _stage(silver_text, silver_code, calls, "silver_entsoe"), []),
            ("silver_swissgrid", _stage(silver_text, silver_code, calls, "silver_swissgrid"), []),
            (
                "reconcile",
                _stage("RECONCILE source=swissgrid series=total diff_pct=1.80 tol=2 PASS\n"),
                [],
            ),
            (
                "gold",
                _stage(
                    "GOLD GATE: PASS checks=5\nMART g1 rows=4 inserted=4 updated=0\n"
                    "MART g3 rows=9 inserted=9 updated=0\n",
                    0,
                    calls,
                    "gold",
                ),
                [],
            ),
        ]

    return steps


class _Ledger:
    """An in-memory stand-in for the pipeline_runs table: one row per run id."""

    def __init__(self):
        self.rows = {}

    def record(self, row):
        self.rows[row["run_id"]] = dict(row, status=compute_status(row))

    def history(self):
        status = [r["status"] for r in self.rows.values()]
        return (
            f"HISTORY runs={len(status)} success={status.count(SUCCESS)} "
            f"failed={status.count(FAILED)}"
        )


def _run(capsys, argv, steps):
    from swiss_grid_lakehouse.runs.__main__ import main

    ledger = _Ledger()
    code = main(argv, steps=steps, record=ledger.record, history=ledger.history)
    return code, capsys.readouterr().out, ledger


def test_runner_case_a_one_run_runs_the_stages_in_order_and_records_success(capsys):
    calls = []
    code, out, ledger = _run(capsys, [], _stub_steps(calls))
    assert code == 0
    assert calls == ["bronze", "silver_entsoe", "silver_swissgrid", "gold"]
    (row,) = ledger.rows.values()
    assert (row["bronze_rows"], row["silver_inserted"], row["silver_updated"]) == (48, 96, 0)
    assert (row["gold_inserted"], row["gold_updated"]) == (13, 0)
    assert row["gate"] == "PASS checks=5" and row["reconcile_diff"] == 1.8
    assert row["status"] == SUCCESS
    assert f"RUN n=1 id={row['run_id']} target=local status=SUCCESS" in out
    assert out.splitlines()[-1] == "HISTORY runs=1 success=1 failed=0"


def test_runner_case_b_times_two_records_two_runs_and_prints_the_history(capsys):
    zero = "merged inserted=0 updated=0 rows=48\n"
    calls = []
    first = _stub_steps(calls)
    second = _stub_steps(calls, silver_text=zero)
    order = iter([first, second])
    code, out, ledger = _run(capsys, ["--times", "2"], lambda a, lake: next(order)(a, lake))
    assert code == 0
    assert sum(x.startswith("RUN n=") for x in out.splitlines()) == 2
    assert "RUN n=1 " in out and "RUN n=2 " in out
    assert len(ledger.rows) == 2
    assert out.splitlines()[-1] == "HISTORY runs=2 success=2 failed=0"
    second_row = list(ledger.rows.values())[1]
    assert (second_row["silver_inserted"], second_row["silver_updated"]) == (0, 0)


def test_runner_case_c_a_failed_silver_gate_skips_gold_but_still_writes_the_row(capsys):
    calls = []
    steps = _stub_steps(calls, silver_code=2, silver_text="GATE: FAIL g1=1\n")
    code, out, ledger = _run(capsys, ["--times", "2"], steps)
    assert code == 2
    assert calls == ["bronze", "silver_entsoe"]
    (row,) = ledger.rows.values()
    assert row["gate"] == "FAIL silver_entsoe g1=1"
    assert row["status"] == FAILED and row.get("gold_inserted") is None
    assert "status=SUCCESS" not in out
    assert "RUN n=1 " in out and "RUN n=2 " not in out
    assert out.splitlines()[-1] == "HISTORY runs=1 success=0 failed=1"
