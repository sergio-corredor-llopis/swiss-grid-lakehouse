"""Command line for the Gold layer.

    python -m swiss_grid_lakehouse.gold --silver S --target DIR [--daily D] [--rejected R]

Reads the Silver table (and, when given, the daily Bronze table and the rejected rows), builds
the four marts, runs the gate and then merges each mart into its Delta table, runs OPTIMIZE
with ZORDER BY on the largest mart (g3) and reads version 0 of g1 back with VERSION AS OF.
S, D and R are Delta paths or table names. When `--target` is a path each mart is the Delta
table `<target>/<table name>`; otherwise `--target` is `catalog.schema` and each mart is
`<target>.<table name>`. Without `--daily` the control mart (g2) is skipped.

Exit code 0 on a merge, 2 when the gate fails (nothing is merged, no MART line), 1 on a usage
or read error.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from swiss_grid_lakehouse.gold import daily_control, daily_load, data_quality, hourly_profile
from swiss_grid_lakehouse.gold.gate import run_gold_gate
from swiss_grid_lakehouse.gold.maintain import (
    optimize,
    optimize_line,
    version_line,
    version_query,
)
from swiss_grid_lakehouse.gold.merge import mart_line, merge_mart

TABLES = {
    "g1": "gold_daily_load",
    "g2": "gold_daily_control",
    "g3": "gold_hourly_profile",
    "g4": "gold_data_quality",
}
ZORDER_BY = ["source", "weekday"]


def _mart_target(target: str, table: str) -> str:
    """The Delta path or table name of one mart under `--target`."""
    from swiss_grid_lakehouse.silver import ch_load_silver as m

    if m._is_path(target):
        return target.rstrip("/\\") + "/" + table
    return f"{target}.{table}"


def _read(spark: Any, location: str) -> Any:
    from swiss_grid_lakehouse.silver import ch_load_silver as m

    if m._is_path(location):
        return spark.read.format("delta").load(location)
    return spark.table(location)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m swiss_grid_lakehouse.gold",
        description="Silver -> Gold: four marts, gate, Delta MERGE, OPTIMIZE and time travel.",
    )
    parser.add_argument("--silver", required=True, help="Silver Delta table path or name")
    parser.add_argument(
        "--target", required=True, help="directory for the mart tables, or catalog.schema"
    )
    parser.add_argument("--daily", help="daily Bronze Delta table path or name (enables g2)")
    parser.add_argument("--rejected", help="rejected Silver rows, Delta table path or name")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
    except SystemExit as exc:
        return 0 if exc.code in (0, None) else 1

    from swiss_grid_lakehouse.spark import get_spark

    try:
        spark = get_spark("local")
        silver = _read(spark, args.silver)
        daily = _read(spark, args.daily) if args.daily else None
        rejected = _read(spark, args.rejected) if args.rejected else None
    except Exception as exc:
        print(
            f"cannot read input: {str(exc).splitlines()[0] if str(exc) else exc!r}", file=sys.stderr
        )
        return 1

    marts = {
        "g1": daily_load.build(spark, silver),
        "g2": daily_control.build(spark, silver, daily),
        "g3": hourly_profile.build(spark, silver),
        "g4": data_quality.build(spark, silver, rejected),
    }
    if marts["g2"] is None:
        print("SKIPPED gold_daily_control reason=no_daily_bronze")
    result = run_gold_gate(silver, marts)
    print(result.summary())
    if not result.passed:
        return 2

    keys = {
        "g1": (daily_load.KEY, daily_load.CHANGE_COLS),
        "g2": (daily_control.KEY, daily_control.CHANGE_COLS),
        "g3": (hourly_profile.KEY, hourly_profile.CHANGE_COLS),
        "g4": (data_quality.KEY, data_quality.CHANGE_COLS),
    }
    for short, df in marts.items():
        if df is None:
            continue
        key, change_cols = keys[short]
        target = _mart_target(args.target, TABLES[short])
        inserted, updated = merge_mart(spark, df, target, key, change_cols)
        print(mart_line(short, _read(spark, target).count(), inserted, updated))

    g3 = _mart_target(args.target, TABLES["g3"])
    print(optimize_line("g3", optimize(spark, g3, ZORDER_BY)))
    g1 = _mart_target(args.target, TABLES["g1"])
    then_rows = version_query(spark, g1, 0).count()
    print(version_line("g1", 0, then_rows, _read(spark, g1).count()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
