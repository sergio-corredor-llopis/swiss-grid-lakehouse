"""Command line for the Silver layer.

    python -m swiss_grid_lakehouse.silver --bronze SRC --target DST

Reads the Bronze Delta table, builds Silver, runs the quality gate and then either merges
into the Silver Delta table or rejects the batch. Exit code 0 on a merge, 2 when the gate
fails (nothing is merged; the offending rows go to `<target>_rejected`).
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from swiss_grid_lakehouse.silver.ch_load_silver import merge_silver, to_silver
from swiss_grid_lakehouse.silver.quality import run_gate

CHECKS = 6  # row checks (4) + missing hours + row count


def _current_count(spark: Any, target: str) -> int:
    from swiss_grid_lakehouse.silver import ch_load_silver as m

    if not m._exists(spark, target):
        return 0
    if m._is_path(target):
        return spark.read.format("delta").load(target).count()
    return spark.table(target).count()


def _write_rejected(rejected: Any, target: str) -> None:
    from swiss_grid_lakehouse.silver import ch_load_silver as m

    writer = rejected.write.format("delta").mode("append")
    if m._is_path(target):
        writer.save(target.rstrip("/\\") + "_rejected")
    else:
        writer.saveAsTable(target + "_rejected")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m swiss_grid_lakehouse.silver",
        description="Bronze -> Silver: dedupe, quality gate, Delta MERGE.",
    )
    parser.add_argument("--bronze", required=True, help="Bronze Delta table path or name")
    parser.add_argument("--target", required=True, help="Silver Delta table path or name")
    args = parser.parse_args(argv)

    from swiss_grid_lakehouse.spark import get_spark

    spark = get_spark("local")
    from swiss_grid_lakehouse.silver import ch_load_silver as m

    if m._is_path(args.bronze):
        bronze = spark.read.format("delta").load(args.bronze)
    else:
        bronze = spark.table(args.bronze)
    silver = to_silver(bronze)
    result = run_gate(silver, _current_count(spark, args.target))
    if not result.passed:
        print(result.summary())
        _write_rejected(result.rejected_df, args.target)
        return 2
    print(f"{result.summary()} checks={CHECKS}")
    inserted, updated = merge_silver(spark, silver, args.target)
    rows = _current_count(spark, args.target)
    print(f"merged inserted={inserted} updated={updated} rows={rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
