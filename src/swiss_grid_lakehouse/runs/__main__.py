"""Local runner: the scheduled job on this machine, then one row in the run ledger.

    python -m swiss_grid_lakehouse.runs [--times N] [--lake DIR] [--source recorded]

Runs the same stages as the scheduled job, in the same order and fail-fast: ENTSO-E Bronze,
Swissgrid Bronze, Silver for both sources, the cross-source compare, the daily Bronze load, the
reconcile and the Gold marts. The first stage that exits with a code other than 0 stops the
run. Whatever happened, the figures of the run go to `record_run`, the same function the
scheduled job calls, so the ledger needs no workspace to be shown. After each run the runner
prints `RUN n=.. id=.. target=.. status=..`; after the last one it prints
`HISTORY runs=.. success=.. failed=..`. The status comes from the figures (`compute_status`),
never from an exit code.

`--times N` repeats the run N times over the same lakehouse; the second and later runs print
`inserted=0 updated=0` for Silver and Gold. Exit code: 0 when every run is SUCCESS, otherwise
the exit code of the stage that failed (1 when the stages passed but the figures are
incomplete). Spark starts on the first stage; `--help` and the unit tests do not need it.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import re
import sys
import tempfile
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from swiss_grid_lakehouse.runs.history import (
    SUCCESS,
    compute_status,
    gate_text,
    history_line,
    record_run,
    run_line,
)

TARGET = "local"
FIXTURES = "tests/fixtures"
Step = tuple[str, Callable[[list[str]], int], list[str]]


def _call(main: Callable[[list[str]], int], argv: list[str]) -> tuple[int, str]:
    """Run one stage in this process; return (exit code, what it printed)."""
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            code = main(argv)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        code = 1
    return int(code or 0), buffer.getvalue()


def default_steps(args: argparse.Namespace, lake: str) -> list[Step]:
    """The stages of one run, each as (name, main function, argv)."""
    from swiss_grid_lakehouse.bronze import __main__ as bronze
    from swiss_grid_lakehouse.compare import __main__ as compare
    from swiss_grid_lakehouse.daily import __main__ as daily
    from swiss_grid_lakehouse.gold import __main__ as gold
    from swiss_grid_lakehouse.reconcile import main as reconcile
    from swiss_grid_lakehouse.silver import __main__ as silver

    b_ent, b_swiss, silver_t = f"{lake}/bronze_entsoe", f"{lake}/bronze_swissgrid", f"{lake}/silver"
    daily_t = f"{lake}/daily"
    return [
        ("bronze_entsoe", bronze.main, ["--xml", args.entsoe, "--target", b_ent]),
        ("bronze_swissgrid", bronze.main, ["--swissgrid", args.swissgrid, "--target", b_swiss]),
        (
            "silver_entsoe",
            silver.main,
            ["--bronze", b_ent, "--target", silver_t, "--source", "entsoe"],
        ),
        (
            "silver_swissgrid",
            silver.main,
            ["--bronze", b_swiss, "--target", silver_t, "--source", "swissgrid"],
        ),
        ("compare", compare.main, ["--silver", silver_t]),
        (
            "daily",
            daily.main,
            [
                *["--package", args.package, "--csv", args.csv, "--target", daily_t],
                *["--state", f"{lake}/state.json"],
            ],
        ),
        ("reconcile", reconcile, ["--silver", silver_t, "--daily", daily_t]),
        ("gold", gold.main, ["--silver", silver_t, "--target", f"{lake}/gold", "--daily", daily_t]),
    ]


def _add(row: dict[str, Any], name: str, value: int) -> None:
    row[name] = (row.get(name) or 0) + value


def absorb(name: str, out: str, row: dict[str, Any]) -> None:
    """Copy the figures a stage printed into the ledger row. A figure not printed stays absent."""
    if name == "bronze_entsoe":
        found = re.search(r"(?:wrote|skipped) (\d+) rows", out)
        if found:
            row["bronze_rows"] = int(found.group(1))
    elif name.startswith("silver_"):
        found = re.search(r"merged inserted=(\d+) updated=(\d+)", out)
        if found:
            _add(row, "silver_inserted", int(found.group(1)))
            _add(row, "silver_updated", int(found.group(2)))
    elif name == "compare":
        line = next((x for x in out.splitlines() if x.startswith("COMPARE ")), None)
        if line:
            row["compare"] = line.removeprefix("COMPARE ").strip()
    elif name == "reconcile":
        # The like-for-like Swissgrid series carry the tight tolerance the status checks.
        diffs = []
        for line in out.splitlines():
            found = re.match(r"RECONCILE source=swissgrid .* diff_pct=(-?\d+(?:\.\d+)?) ", line)
            if found:
                diffs.append(float(found.group(1)))
        if diffs:
            row["reconcile_diff"] = max(diffs, key=abs)
    elif name == "gold":
        line = next((x for x in out.splitlines() if x.startswith("GOLD GATE:")), None)
        if line:
            row["gate"] = gate_text(line)
        for found in re.finditer(r"^MART \w+ rows=\d+ inserted=(\d+) updated=(\d+)", out, re.M):
            _add(row, "gold_inserted", int(found.group(1)))
            _add(row, "gold_updated", int(found.group(2)))


def _failed_gate(name: str, out: str) -> str:
    """The gate text of a run that stopped before the Gold gate was reached."""
    for line in out.splitlines():
        if line.startswith("GATE: FAIL"):
            return f"FAIL {name} {line.removeprefix('GATE: FAIL').strip()}".strip()
    return f"FAIL {name}"


def run_once(steps: Sequence[Step], row: dict[str, Any]) -> int:
    """Run the stages in order, stop at the first failure; return its exit code (0 = all passed)."""
    for name, main, argv in steps:
        code, out = _call(main, argv)
        print(out, end="" if out.endswith("\n") or not out else "\n")
        absorb(name, out, row)
        if code != 0:
            if row.get("gate") is None:
                row["gate"] = _failed_gate(name, out)
            print(f"STOPPED at {name} exit={code}")
            return code
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m swiss_grid_lakehouse.runs",
        description="Run the job stages locally in order, fail-fast, and record the run.",
    )
    parser.add_argument("--times", type=int, default=1, help="how many runs (default: 1)")
    parser.add_argument("--lake", help="directory for the Delta tables (default: a new temp dir)")
    parser.add_argument("--source", default="recorded", help="source label in the ledger")
    parser.add_argument("--entsoe", default=f"{FIXTURES}/entsoe_ch_load_2026-08-30.xml")
    parser.add_argument("--swissgrid", default=f"{FIXTURES}/swissgrid_ch_energy_2026-08-30.csv")
    parser.add_argument("--package", default=f"{FIXTURES}/ckan_package_show_2026-09-29.json")
    parser.add_argument("--csv", default=f"{FIXTURES}/ogd103_2026-08-30_31.csv")
    return parser


def _spark_ledger(lake: str) -> tuple[Callable[[dict[str, Any]], Any], Callable[[], str]]:
    from swiss_grid_lakehouse.spark import get_spark

    table = f"{lake}/pipeline_runs"
    return (
        lambda row: record_run(get_spark("local"), row, table),
        lambda: history_line(get_spark("local"), table),
    )


def main(
    argv: list[str] | None = None,
    *,
    steps: Callable[[argparse.Namespace, str], list[Step]] | None = None,
    record: Callable[[dict[str, Any]], Any] | None = None,
    history: Callable[[], str] | None = None,
) -> int:
    """Run `--times` runs; `steps`, `record` and `history` replace the real ones in tests."""
    args = _parser().parse_args(argv)
    if args.times < 1:
        print("RUN error: --times must be at least 1", file=sys.stderr)
        return 2
    lake = (args.lake or tempfile.mkdtemp(prefix="lakehouse-")).rstrip("/")
    if record is None or history is None:
        record, history = _spark_ledger(lake)
    exit_code = 0
    for n in range(1, args.times + 1):
        row: dict[str, Any] = {
            "run_id": str(uuid.uuid4()),
            "started_at": datetime.now(UTC),
            "target": TARGET,
            "source": args.source,
        }
        code = run_once((steps or default_steps)(args, lake), row)
        record(row)
        print(run_line(row).replace("RUN ", f"RUN n={n} ", 1))
        if code == 0 and compute_status(row) != SUCCESS:
            code = 1
        if code != 0:
            exit_code = code
            break
    print(history())
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
