"""Command line for the Silver layer.

    python -m swiss_grid_lakehouse.silver --bronze SRC --target DST [--source entsoe|swissgrid]

Reads the Bronze Delta table of one source, builds Silver, runs the quality gate and then
either merges into the Silver Delta table or rejects the batch. The source defaults to
`entsoe`. For `swissgrid` the 15-minute Bronze slots (label = slot start) are first summed to
hours; `load_mw` is the total consumed energy, and the end-user energy and the difference go to
two further nullable columns that stay NULL for `entsoe`. Both sources
share one Silver table; the gate runs once per source, against that source's current row
count. Exit code 0 on a merge, 2 when the gate fails (nothing is merged; the offending rows go
to `<target>_rejected`).
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from swiss_grid_lakehouse.silver.ch_load_silver import merge_silver, to_silver
from swiss_grid_lakehouse.silver.quality import run_gate

CHECKS = 6  # row checks (4) + missing hours + row count


def _current_count(spark: Any, target: str, source: str | None = None) -> int:
    """Rows in the Silver table, or only those of one `source` when it is given."""
    from swiss_grid_lakehouse.silver import ch_load_silver as m

    if not m._exists(spark, target):
        return 0
    if m._is_path(target):
        table = spark.read.format("delta").load(target)
    else:
        table = spark.table(target)
    if source is not None:
        table = table.filter(table["source"] == source)
    return table.count()


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
    parser.add_argument(
        "--source",
        choices=["entsoe", "swissgrid"],
        default="entsoe",
        help="which publisher the Bronze table holds (default: entsoe)",
    )
    args = parser.parse_args(argv)

    from swiss_grid_lakehouse.spark import get_spark

    spark = get_spark("local")
    from swiss_grid_lakehouse.silver import ch_load_silver as m

    if m._is_path(args.bronze):
        bronze = spark.read.format("delta").load(args.bronze)
    else:
        bronze = spark.table(args.bronze)
    if args.source == "swissgrid":
        from swiss_grid_lakehouse.silver.swissgrid_hourly import to_hourly

        silver = to_silver(to_hourly(bronze), value_col="energy_kwh")
    else:
        silver = to_silver(bronze)
    result = run_gate(silver, _current_count(spark, args.target, args.source))
    if not result.passed:
        print(result.summary())
        _write_rejected(result.rejected_df, args.target)
        return 2
    print(f"{result.summary()} checks={CHECKS}")
    inserted, updated = merge_silver(spark, silver, args.target)
    rows = _current_count(spark, args.target, args.source)
    print(f"merged inserted={inserted} updated={updated} rows={rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
