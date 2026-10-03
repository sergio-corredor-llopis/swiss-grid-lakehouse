"""The run ledger: one row per pipeline run, merged into the Delta table `pipeline_runs`.

`record_run` writes the row of one run with a MERGE on `run_id`, so recording the same run
twice leaves one row. `compute_status` derives the status of the run from its data outcomes
only: SUCCESS needs a passed Gold gate, every expected figure and a reconcile difference within
the tolerance. A task state of the scheduler is never an input, so a run that the scheduler
reports as finished but whose figures are missing is not SUCCESS. A missing figure stays NULL
in the table and is what turns the status into PARTIAL. Spark is imported lazily.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from swiss_grid_lakehouse.gold.merge import merge_mart
from swiss_grid_lakehouse.reconcile import TIGHT_PCT

TABLE = "pipeline_runs"
KEY = ["run_id"]
SUCCESS, PARTIAL, FAILED = "SUCCESS", "PARTIAL", "FAILED"

SCHEMA = (
    "run_id string, started_at timestamp, target string, source string, bronze_rows long, "
    "silver_inserted long, silver_updated long, gate string, compare string, "
    "reconcile_diff double, gold_inserted long, gold_updated long, status string, "
    "recorded_at timestamp"
)
COLUMNS = tuple(part.split()[0] for part in SCHEMA.split(", "))
# recorded_at moves on every call, so it is not a change column: a repeat changes nothing.
CHANGE_COLUMNS = [c for c in COLUMNS if c not in ("run_id", "recorded_at")]

# The figures every run must carry. The Azure target adds the reconcile difference.
EXPECTED = ("bronze_rows", "silver_inserted", "silver_updated", "gold_inserted", "gold_updated")
AZURE_TARGET = "ch-load-azure"


def gate_text(summary: str | None) -> str | None:
    """The stored form of a Gold gate summary: `GOLD GATE: PASS checks=5` -> `PASS checks=5`."""
    if summary is None:
        return None
    return summary.split("GOLD GATE:", 1)[-1].strip() or None


def compute_status(row: Mapping[str, Any], tolerance: float = TIGHT_PCT) -> str:
    """SUCCESS, PARTIAL or FAILED from the figures of one run, never from a task state.

    FAILED: the gate is missing or starts with FAIL, or `reconcile_diff` is beyond `tolerance`
    (an absolute percentage). SUCCESS: the gate starts with PASS, every figure in `EXPECTED` is
    present (the Azure target also needs `reconcile_diff`) and the difference is within
    `tolerance`. PARTIAL: the gate passed but a figure is missing.
    """
    gate = (row.get("gate") or "").strip().upper()
    diff = row.get("reconcile_diff")
    if not gate.startswith("PASS") or (diff is not None and abs(diff) > tolerance):
        return FAILED
    expected = EXPECTED + (("reconcile_diff",) if row.get("target") == AZURE_TARGET else ())
    if any(row.get(name) is None for name in expected):
        return PARTIAL
    return SUCCESS


def _complete(row: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    unknown = set(row) - set(COLUMNS)
    if unknown:
        raise ValueError(f"unknown ledger columns: {sorted(unknown)}")
    if not row.get("run_id"):
        raise ValueError("a ledger row needs a run_id")
    full = {name: row.get(name) for name in COLUMNS}
    full["status"] = compute_status(full)
    full["recorded_at"] = now
    return full


def record_run(
    spark: Any, row: Mapping[str, Any], table: str = TABLE, now: datetime | None = None
) -> tuple[int, int]:
    """Merge the ledger row of one run into `table`; return (inserted, updated).

    `row` holds any of `COLUMNS`; a missing column is NULL. `status` and `recorded_at` are set
    here, a `status` in `row` is replaced by `compute_status`. The first call for a run id
    returns (1, 0); the same row again returns (0, 0); changed figures for the same run id
    return (0, 1). The table has one row per run id in every case.
    """
    full = _complete(row, now or datetime.now(UTC))
    df = spark.createDataFrame([tuple(full[name] for name in COLUMNS)], schema=SCHEMA)
    return merge_mart(spark, df, table, KEY, CHANGE_COLUMNS)


def run_line(row: Mapping[str, Any]) -> str:
    """The printed form of one run, for example `RUN id=r1 target=ch-load status=SUCCESS`."""
    return f"RUN id={row.get('run_id')} target={row.get('target')} status={compute_status(row)}"


def history_line(spark: Any, table: str = TABLE) -> str:
    """The printed form of the ledger, for example `HISTORY runs=2 success=2 failed=0`."""
    df = spark.read.table(table) if "/" not in table else spark.read.format("delta").load(table)
    rows = df.groupBy("status").count().collect()
    counts = {r["status"]: r["count"] for r in rows}
    runs = sum(counts.values())
    return f"HISTORY runs={runs} success={counts.get(SUCCESS, 0)} failed={counts.get(FAILED, 0)}"
