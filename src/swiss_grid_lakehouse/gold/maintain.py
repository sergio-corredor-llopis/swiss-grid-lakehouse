"""Table maintenance and time travel for the Gold marts.

`optimize` compacts the small files of a Delta table and clusters them with ZORDER BY; the file
counts come from the table history. A runtime that refuses the statement does not stop the run:
`optimize` returns the reason instead of raising. `version_query` reads an older version of a
table with `VERSION AS OF`. The `*_line` helpers give the printed forms. Spark is imported
lazily.
"""

from __future__ import annotations

import re
from typing import Any

from swiss_grid_lakehouse.silver.ch_load_silver import _delta_table, _is_path


def _ref(target: str) -> str:
    """The name Spark SQL uses for `target`: a path in `delta.` form, or the table name."""
    return f"delta.`{target}`" if _is_path(target) else target


def _reason(exc: Exception) -> str:
    """A short reason with underscores: the first words of the error, lower case."""
    first = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
    words = re.sub(r"[^a-z0-9]+", "_", first.lower()).strip("_")
    return words[:40].strip("_") or type(exc).__name__.lower()


def optimize(spark: Any, target: str, zorder_by: list[str]) -> dict[str, Any]:
    """Run OPTIMIZE ... ZORDER BY on `target`; return the file counts and any refusal.

    The result is {files_removed, files_added, refused}. `refused` is None on success and a
    short reason when the runtime refuses the statement; then both counts are 0. The counts
    are numRemovedFiles and numAddedFiles of the OPTIMIZE entry in the table history.
    """
    statement = f"OPTIMIZE {_ref(target)} ZORDER BY ({', '.join(zorder_by)})"
    try:
        spark.sql(statement)
    except Exception as exc:
        return {"files_removed": 0, "files_added": 0, "refused": _reason(exc)}
    last = _delta_table(spark, target).history(1).select("operation", "operationMetrics").first()
    if last["operation"] != "OPTIMIZE":
        return {"files_removed": 0, "files_added": 0, "refused": None}
    metrics = last["operationMetrics"]
    return {
        "files_removed": int(metrics["numRemovedFiles"]),
        "files_added": int(metrics["numAddedFiles"]),
        "refused": None,
    }


def optimize_line(short: str, metrics: dict[str, Any]) -> str:
    """The printed form of an OPTIMIZE result, `OPTIMIZE table=g3 files_removed=R files_added=A`."""
    if metrics.get("refused"):
        return f"OPTIMIZE table={short} refused reason={metrics['refused']}"
    return (
        f"OPTIMIZE table={short} files_removed={metrics['files_removed']} "
        f"files_added={metrics['files_added']}"
    )


def version_query(spark: Any, target: str, version: int) -> Any:
    """Return the rows of `target` as they were at `version`, read with Spark SQL."""
    return spark.sql(f"SELECT * FROM {_ref(target)} VERSION AS OF {int(version)}")


def version_line(short: str, version: int, then_rows: int, now_rows: int) -> str:
    """The printed form of a time-travel read, `VERSION table=g1 v=0 rows=N now=M`."""
    return f"VERSION table={short} v={version} rows={then_rows} now={now_rows}"
