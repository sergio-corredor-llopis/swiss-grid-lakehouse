"""Client for the published Swiss national consumption (dataset ogd103), into a Bronze table.

Usage: python -m swiss_grid_lakehouse.daily --package PKG --csv CSV --target DIR --state FILE

The data is the daily national and final consumption in whole GWh published by the Swiss
Federal Office of Energy (SFOE) on opendata.swiss. The client first reads the registry
record (CKAN `package_show`) and its `metadata_modified` stamp. Only when that stamp differs
from the one stored in the state file does it fetch the CSV and append one batch to the
Bronze Delta table `ch_consumption_daily_bronze`. PKG and CSV are each a local path or an
https URL. Every HTTP request names itself in a User-Agent header, because the registry
answers 403 to a default client string. `Datum` stays text; rows are sorted by date.
Spark is imported lazily; everything before the write needs only the standard library.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SOURCE = "ogd103"
TABLE = "ch_consumption_daily_bronze"
COLUMNS = [
    "source",
    "local_date",
    "national_gwh",
    "final_gwh",
    "pulled_at",
    "batch_id",
    "registry_modified",
]
PACKAGE_URL = (
    "https://ckan.opendata.swiss/api/3/action/package_show"
    "?id=energiedashboard-ch-landesverbrauch-und-endverbrauch"
)
CSV_URL = "https://www.bfe-ogd.ch/ogd103_stromverbrauch_swissgrid_lv_und_endv.csv"
USER_AGENT = "swiss-grid-lakehouse/0.1 (daily consumption client; open-data reader)"
HEADER = ["Datum", "Landesverbrauch_GWh", "Endverbrauch_GWh"]

Get = Callable[[str, dict[str, str]], tuple[int, bytes]]


def http_get(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    """Fetch `url` with `headers`; return (status, body). HTTP errors return their status."""
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, b""


def fetch_bytes(location: str, get: Get = http_get) -> bytes:
    """Read a local path, or fetch an https URL with the named User-Agent.

    Raises RuntimeError on any status other than 200.
    """
    if not location.startswith(("http://", "https://")):
        return Path(location).read_bytes()
    status, body = get(location, {"User-Agent": USER_AGENT})
    if status != 200:
        raise RuntimeError(f"GET {location} answered {status}")
    return body


def read_registry_stamp(package_json: str | bytes) -> str:
    """Return the `metadata_modified` text of a CKAN package_show answer or a trimmed copy.

    Accepts the full answer (`{"result": {...}}`) or the bare package object.
    """
    data = json.loads(package_json)
    package = data.get("result", data) if isinstance(data, dict) else {}
    stamp = package.get("metadata_modified") if isinstance(package, dict) else None
    if not isinstance(stamp, str) or not stamp:
        raise ValueError("package record has no metadata_modified")
    return stamp


def batch_id_for(csv_bytes: bytes) -> str:
    """Return a stable id for one file: the first 12 hex chars of the SHA-256 of its bytes."""
    return hashlib.sha256(csv_bytes).hexdigest()[:12]


def parse_daily_csv(csv_bytes: bytes) -> list[tuple[str, float, float]]:
    """Parse the published CSV into (Datum, national_gwh, final_gwh), sorted by Datum.

    `Datum` is kept as the text of the file. A leading byte-order mark is ignored.
    Raises ValueError on a wrong header or an unreadable number.
    """
    text = csv_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if header is None or [h.strip() for h in header] != HEADER:
        raise ValueError(f"unexpected header {header!r}, want {HEADER!r}")
    rows = []
    for line in reader:
        if not line or not any(cell.strip() for cell in line):
            continue
        if len(line) != 3:
            raise ValueError(f"unexpected row {line!r}")
        rows.append((line[0].strip(), float(line[1]), float(line[2])))
    return sorted(rows, key=lambda row: row[0])


def to_bronze_rows(csv_bytes: bytes, registry_modified: str, pulled_at: datetime) -> list[tuple]:
    """Turn the CSV bytes into Bronze rows, ordered as COLUMNS and sorted by date."""
    batch_id = batch_id_for(csv_bytes)
    return [
        (SOURCE, day, national, final, pulled_at, batch_id, registry_modified)
        for day, national, final in parse_daily_csv(csv_bytes)
    ]


def _schema() -> Any:
    from pyspark.sql import types as t

    return t.StructType(
        [
            t.StructField("source", t.StringType(), False),
            t.StructField("local_date", t.StringType(), False),
            t.StructField("national_gwh", t.DoubleType(), False),
            t.StructField("final_gwh", t.DoubleType(), False),
            t.StructField("pulled_at", t.TimestampType(), False),
            t.StructField("batch_id", t.StringType(), False),
            t.StructField("registry_modified", t.StringType(), False),
        ]
    )


def write_bronze(spark: Any, rows: list[tuple], target: str) -> bool:
    """Append rows to the Bronze Delta table at `target` (a path or a table name).

    Append-only: nothing is updated or deleted. If the batch_id of the rows is already in
    the table the write is skipped. Returns True if rows were written, False if skipped.
    """
    from swiss_grid_lakehouse.bronze.entsoe_load_bronze import _batch_exists, _is_path

    if not rows:
        return False
    if _batch_exists(spark, target, rows[0][COLUMNS.index("batch_id")]):
        return False
    writer = spark.createDataFrame(rows, schema=_schema()).write.format("delta").mode("append")
    if _is_path(target):
        writer.save(target)
    else:
        writer.saveAsTable(target)
    return True


def _read_state(state: Path) -> str | None:
    if not state.exists():
        return None
    return json.loads(state.read_text(encoding="utf-8")).get("metadata_modified")


def _write_state(state: Path, stamp: str) -> None:
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text(json.dumps({"metadata_modified": stamp}, indent=2) + "\n", encoding="utf-8")


def run(
    package: str,
    csv_location: str,
    target: str,
    state: Path,
    get: Get = http_get,
    spark: Any = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> str:
    """Run one registry check; return the line to print.

    Returns `REGISTRY unchanged` without fetching the CSV if the stamp equals the stored one.
    Otherwise fetches the CSV, appends one batch, stores the new stamp (after the write) and
    returns `REGISTRY changed fetched=1 rows=<n> batch=<12 hex>`; rows=0 if the batch was
    already present.
    """
    stamp = read_registry_stamp(fetch_bytes(package, get))
    if stamp == _read_state(state):
        return "REGISTRY unchanged"
    csv_bytes = fetch_bytes(csv_location, get)
    rows = to_bronze_rows(csv_bytes, stamp, now())
    if spark is None:
        # Spark's console progress bar writes carriage returns onto the same line as the
        # result; switch it off so that the result line starts a line of its own.
        os.environ.setdefault(
            "PYSPARK_SUBMIT_ARGS", "--conf spark.ui.showConsoleProgress=false pyspark-shell"
        )
        from swiss_grid_lakehouse.spark import get_spark

        spark = get_spark()
    written = write_bronze(spark, rows, target)
    _write_state(state, stamp)
    return (
        f"REGISTRY changed fetched=1 rows={len(rows) if written else 0} "
        f"batch={batch_id_for(csv_bytes)}"
    )


def main(argv: list[str] | None = None, get: Get = http_get) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m swiss_grid_lakehouse.daily",
        description="Append the published daily national consumption to a Bronze Delta table.",
    )
    parser.add_argument("--package", default=PACKAGE_URL, help="package_show path or URL")
    parser.add_argument("--csv", default=CSV_URL, help="CSV path or URL")
    parser.add_argument("--target", required=True, help="Delta path or table name")
    parser.add_argument("--state", required=True, help="file holding the last metadata_modified")
    args = parser.parse_args(argv)
    line = run(args.package, args.csv, args.target, Path(args.state), get=get)
    print(line)
    return 0
