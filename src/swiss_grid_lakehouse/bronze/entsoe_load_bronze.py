"""Write recorded ENTSO-E Swiss actual-load XML into an append-only Bronze Delta table.

Usage: python -m swiss_grid_lakehouse.bronze --xml FILE --target PATH_OR_TABLE

Each input file becomes one batch. The batch_id is a hash of the file content, so loading
the same file again is detected and skipped. Spark is imported lazily; `to_bronze_rows`
needs only pandas.
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from swiss_grid_lakehouse.ingest.entsoe_load import parse_ch_load_xml

SOURCE = "entsoe"
AREA = "CH"
COLUMNS = ["source", "area", "pulled_at", "batch_id", "source_file", "ts_utc", "actual_load_mw"]


def batch_id_for(xml_text: str) -> str:
    """Return a stable id for one raw response: the first 16 hex chars of its SHA-256."""
    return hashlib.sha256(xml_text.encode("utf-8")).hexdigest()[:16]


def to_bronze_rows(xml_text: str, source_file: str, pulled_at: datetime) -> list[tuple]:
    """Turn one ENTSO-E load response into Bronze rows.

    Returns one tuple per timestamp, ordered as COLUMNS. Raises ValueError on input that
    `parse_ch_load_xml` rejects.
    """
    frame = parse_ch_load_xml(xml_text)
    batch_id = batch_id_for(xml_text)
    return [
        (
            SOURCE,
            AREA,
            pulled_at,
            batch_id,
            source_file,
            ts.to_pydatetime(),
            float(value),
        )
        for ts, value in frame["Actual Load"].items()
    ]


def _schema() -> Any:
    from pyspark.sql import types as t

    return t.StructType(
        [
            t.StructField("source", t.StringType(), False),
            t.StructField("area", t.StringType(), False),
            t.StructField("pulled_at", t.TimestampType(), False),
            t.StructField("batch_id", t.StringType(), False),
            t.StructField("source_file", t.StringType(), False),
            t.StructField("ts_utc", t.TimestampType(), False),
            t.StructField("actual_load_mw", t.DoubleType(), False),
        ]
    )


def _is_path(target: str) -> bool:
    return "/" in target or "\\" in target


def _batch_exists(spark: Any, target: str, batch_id: str) -> bool:
    if _is_path(target):
        from delta.tables import DeltaTable

        if not DeltaTable.isDeltaTable(spark, target):
            return False
        frame = spark.read.format("delta").load(target)
    else:
        if not spark.catalog.tableExists(target):
            return False
        frame = spark.table(target)
    return frame.filter(frame["batch_id"] == batch_id).limit(1).count() > 0


def write_bronze(spark: Any, rows: list[tuple], target: str) -> bool:
    """Append rows to the Bronze Delta table at `target` (a path or a table name).

    Append-only: nothing is updated or deleted. If the batch_id of the rows is already in
    the table the write is skipped. Returns True if rows were written, False if skipped.
    """
    if not rows:
        return False
    batch_id = rows[0][COLUMNS.index("batch_id")]
    if _batch_exists(spark, target, batch_id):
        return False
    writer = spark.createDataFrame(rows, schema=_schema()).write.format("delta").mode("append")
    if _is_path(target):
        writer.save(target)
    else:
        writer.saveAsTable(target)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m swiss_grid_lakehouse.bronze",
        description="Load one recorded ENTSO-E CH load XML file into a Bronze Delta table.",
    )
    parser.add_argument("--xml", "--input", dest="xml", required=True, help="XML file to load")
    parser.add_argument(
        "--target", "--out", dest="target", required=True, help="Delta path or table name"
    )
    args = parser.parse_args(argv)

    from swiss_grid_lakehouse.spark import get_spark

    path = Path(args.xml)
    rows = to_bronze_rows(path.read_text(encoding="utf-8"), path.name, datetime.now(UTC))
    written = write_bronze(get_spark(), rows, args.target)
    print(f"{'wrote' if written else 'skipped'} {len(rows)} rows batch_id={rows[0][3]}")
    return 0
