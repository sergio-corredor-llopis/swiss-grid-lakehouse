"""Merge a Gold mart into its Delta table.

`merge_mart` creates the table on the first call, so version 0 already holds the rows. Later
calls run a Delta MERGE on the mart key: a matched row is updated only when at least one change
column differs (compared null-safe), an unmatched row is inserted and nothing is ever deleted.
Running the same rows twice therefore changes nothing. Spark is imported lazily.
"""

from __future__ import annotations

from typing import Any

from swiss_grid_lakehouse.silver.ch_load_silver import _delta_table, _exists, _is_path


def _create(df: Any, target: str) -> None:
    writer = df.write.format("delta")
    if _is_path(target):
        writer.save(target)
    else:
        writer.saveAsTable(target)


def merge_mart(
    spark: Any, df: Any, target: str, key: list[str], change_cols: list[str]
) -> tuple[int, int]:
    """Merge `df` into the Delta table at `target`; return (inserted, updated).

    `target` is a path or a table name. The first call writes `df` as the table and returns
    (row count, 0). After that a row with the same `key` is updated only if a column in
    `change_cols` differs, so a rerun on equal rows returns (0, 0). The counts come from the
    MERGE metrics in the table history.
    """
    if not _exists(spark, target):
        _create(df, target)
        return df.count(), 0
    table = _delta_table(spark, target)
    cond = " AND ".join(f"t.{k} = s.{k}" for k in key)
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in change_cols)
    (
        table.alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .execute()
    )
    last = table.history(1).select("operation", "operationMetrics").first()
    if last["operation"] != "MERGE":
        return 0, 0
    metrics = last["operationMetrics"]
    return int(metrics["numTargetRowsInserted"]), int(metrics["numTargetRowsUpdated"])


def mart_line(short: str, rows: int, inserted: int, updated: int) -> str:
    """The printed form of one merged mart, for example `MART g1 rows=4 inserted=4 updated=0`."""
    return f"MART {short} rows={rows} inserted={inserted} updated={updated}"
