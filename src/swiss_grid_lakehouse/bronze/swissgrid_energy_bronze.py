"""Write a recorded Swissgrid energy workbook into an append-only Bronze Delta table.

Usage: python -m swiss_grid_lakehouse.bronze --swissgrid FILE --target PATH_OR_TABLE

Source: Swissgrid AG, aggregated energy data of the Swiss control block. The workbook has
the sheet `Zeitreihen0h15` with 64 series: row 1 holds the series names (column B onwards;
a header cell is the German name, a line break and the English name), row 2 holds the unit
of each series, and every later row is one 15-minute slot with the interval START as a
local-time label in column A (a datetime cell or a `dd.mm.yyyy HH:MM` string). Two series
are selected by their German name (`Summe endverbrauchte Energie Regelblock Schweiz` and
`Summe verbrauchte Energie Regelblock Schweiz`); only their unit (`kWh`) is validated, the
other columns, including the price series in `Euro/MWh`, are never looked at. A `.csv`
extract with the same layout is read by the same code.

Bronze keeps the raw facts and does no time-zone logic: `ts_label` is the label as printed
and `row_index` is the 1-based row number in the sheet, so a later layer can resolve the
repeated hour of the October clock change by order. Each input file is one batch; the
batch_id is a hash of the file bytes, so loading the same file again is detected and skipped.
Spark is imported lazily; `read_workbook` and `to_bronze_rows` need only the standard library
(plus openpyxl for `.xlsx`).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from swiss_grid_lakehouse.bronze.entsoe_load_bronze import _batch_exists, _is_path

SOURCE = "swissgrid"
SHEET = "Zeitreihen0h15"
UNIT = "kWh"
SELECTED = (
    "Summe endverbrauchte Energie Regelblock Schweiz",
    "Summe verbrauchte Energie Regelblock Schweiz",
)
LABEL_FORMAT = "%d.%m.%Y %H:%M"
COLUMNS = [
    "source",
    "series",
    "unit",
    "ts_label",
    "row_index",
    "energy_kwh",
    "pulled_at",
    "batch_id",
    "source_file",
]


def batch_id_for(data: bytes) -> str:
    """Return a stable id for one raw file: the first 16 hex chars of its SHA-256."""
    return hashlib.sha256(data).hexdigest()[:16]


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime(LABEL_FORMAT)
    return str(value).strip()


def _first_line(value: Any) -> str:
    """The German first line of a header cell (the cell may add a line break and English)."""
    text = "" if value is None else str(value)
    return text.splitlines()[0].strip() if text.strip() else ""


def _sheet_rows(path: Path) -> Iterator[list[Any]]:
    """Yield the raw rows of the series sheet: `.csv` is taken to be that sheet already."""
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.reader(handle):
                yield list(row)
        return
    from openpyxl import load_workbook

    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        if SHEET not in workbook.sheetnames:
            raise ValueError(f"{path.name}: sheet {SHEET!r} not found, got {workbook.sheetnames}")
        for row in workbook[SHEET].iter_rows(values_only=True):
            yield list(row)
    finally:
        workbook.close()


def _select(path: Path) -> tuple[list[str], list[str], list[tuple[int, str, list[Any]]]]:
    """Return (series names, units, data) for the two selected series only.

    Series are found by the German first line of their header cell in row 1. The unit in
    row 2 is checked for the selected series only; every other column is never looked at.
    Data is a list of (sheet row number, label, raw values) in sheet order; rows that are
    empty in the label and in the selected columns are dropped.
    """
    rows = _sheet_rows(path)
    header = next(rows, None)
    unit_row = next(rows, None)
    if header is None or unit_row is None:
        raise ValueError(f"{path.name}: expected a header row and a unit row")
    seen = [_first_line(c) for c in header[1:]]
    cols: list[int] = []
    for name in SELECTED:
        hits = [i for i, text in enumerate(seen, start=1) if text == name]
        if not hits:
            shown = [t for t in seen if t][:8]
            raise ValueError(
                f"{path.name}: series {name!r} not found in row 1 "
                f"({len(seen)} columns, first names seen: {shown})"
            )
        if len(hits) > 1:
            raise ValueError(f"{path.name}: series {name!r} appears in {len(hits)} columns")
        cols.append(hits[0])
    units = [_cell_text(unit_row[c]) if c < len(unit_row) else "" for c in cols]
    for name, unit in zip(SELECTED, units, strict=True):
        if unit != UNIT:
            raise ValueError(f"{path.name}: series {name!r} has unit {unit!r}, expected {UNIT!r}")
    data: list[tuple[int, str, list[Any]]] = []
    for number, row in enumerate(rows, start=3):
        label = _cell_text(row[0]) if row else ""
        values = [row[c] if c < len(row) else None for c in cols]
        if not label and not any(_cell_text(v) for v in values):
            continue
        data.append((number, label, values))
    if not data:
        raise ValueError(f"{path.name}: no data rows below the unit row")
    return list(SELECTED), units, data


def read_workbook(path: str | Path) -> list[list[Any]]:
    """Read the two selected series of the Swissgrid sheet `Zeitreihen0h15`.

    Works on `.xlsx` (openpyxl, read-only mode) and on a `.csv` extract with the same code.
    Returns the rows of a reduced sheet: row 1 is an empty first cell plus the two German
    series names, row 2 is a label cell plus the units, then one row per 15-minute slot
    with the label and the two values. Labels are the interval start as printed (a datetime
    cell is formatted as `dd.mm.yyyy HH:MM`). Raises ValueError, naming what it saw, when the
    sheet or a series is missing or the unit of a selected series is not `kWh`.
    """
    path = Path(path)
    names, units, data = _select(path)
    return [[""] + names, ["Zeitstempel"] + units] + [[label] + vals for _, label, vals in data]


def to_bronze_rows(path: str | Path, pulled_at: datetime) -> list[tuple]:
    """Turn one Swissgrid workbook into long-form Bronze rows.

    Returns one tuple per series and slot, ordered as COLUMNS. Raises ValueError when the
    layout is wrong or a value is missing or not a number.
    """
    path = Path(path)
    names, _, data = _select(path)
    batch_id = batch_id_for(path.read_bytes())
    out: list[tuple] = []
    for number, label, values in data:
        if not label:
            raise ValueError(f"{path.name}: sheet row {number} has no time label")
        for name, raw in zip(names, values, strict=True):
            try:
                value = float(raw)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{path.name}: sheet row {number}, series {name!r}: not a number: {raw!r}"
                ) from None
            out.append((SOURCE, name, UNIT, label, number, value, pulled_at, batch_id, path.name))
    return out


def _schema() -> Any:
    from pyspark.sql import types as t

    return t.StructType(
        [
            t.StructField("source", t.StringType(), False),
            t.StructField("series", t.StringType(), False),
            t.StructField("unit", t.StringType(), False),
            t.StructField("ts_label", t.StringType(), False),
            t.StructField("row_index", t.IntegerType(), False),
            t.StructField("energy_kwh", t.DoubleType(), False),
            t.StructField("pulled_at", t.TimestampType(), False),
            t.StructField("batch_id", t.StringType(), False),
            t.StructField("source_file", t.StringType(), False),
        ]
    )


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


def _count_batch(spark: Any, target: str, batch_id: str) -> int:
    if _is_path(target):
        frame = spark.read.format("delta").load(target)
    else:
        frame = spark.table(target)
    return frame.filter(frame["batch_id"] == batch_id).count()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m swiss_grid_lakehouse.bronze",
        description="Load one Swissgrid energy workbook (.xlsx or .csv) into a Bronze Delta table.",
    )
    parser.add_argument("--swissgrid", required=True, help="workbook (.xlsx) or CSV to load")
    parser.add_argument(
        "--target", "--out", dest="target", required=True, help="Delta path or table name"
    )
    args = parser.parse_args(argv)

    from swiss_grid_lakehouse.spark import get_spark

    rows = to_bronze_rows(args.swissgrid, datetime.now(UTC))
    spark = get_spark()
    written = write_bronze(spark, rows, args.target)
    total = _count_batch(spark, args.target, rows[0][COLUMNS.index("batch_id")])
    if written:
        print(f"BRONZE rows={len(rows)} rerun=0")
    else:
        print(f"BRONZE rows={len(rows)} rerun={total}")
    return 0
