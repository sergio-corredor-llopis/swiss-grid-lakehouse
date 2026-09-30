#!/usr/bin/env python3
"""Cut a small CSV slice out of the Swissgrid "Energieuebersicht Schweiz" workbook.

Usage:
    python scripts/record_swissgrid_slice.py --workbook EnergieUebersichtCH-2026.xlsx \
        [--from 2026-08-30] [--to 2026-08-31] \
        [--out tests/fixtures/swissgrid_ch_energy_2026-08-30.csv]

The workbook is published by Swissgrid AG under Operation > Grid data on
swissgrid.ch (energy overview Switzerland) and is not stored in this repository.
The script reads sheet ``Zeitreihen0h15`` (quarter-hour series, 64 columns)
and keeps two of them, selected by the German first line of their row-1
header cell:

    Summe endverbrauchte Energie Regelblock Schweiz
    Summe verbrauchte Energie Regelblock Schweiz

Output CSV: row 1 holds the two German names, row 2 the unit (asserted to be
``kWh`` in the workbook for both columns), then one row per quarter hour of
the local days --from .. --to inclusive. Column A is printed as
``dd.mm.yyyy HH:MM`` exactly as the workbook labels it (interval START).
Values are written as the workbook stores them. Needs openpyxl (extra
``swissgrid``); it reads the workbook in read-only mode.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

SHEET = "Zeitreihen0h15"
SERIES = (
    "Summe endverbrauchte Energie Regelblock Schweiz",
    "Summe verbrauchte Energie Regelblock Schweiz",
)
UNIT = "kWh"
DEFAULT_WORKBOOK = "EnergieUebersichtCH-2026.xlsx"
DEFAULT_OUT = "tests/fixtures/swissgrid_ch_energy_2026-08-30.csv"
LABEL_FORMAT = "%d.%m.%Y %H:%M"


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write a two-series, two-day CSV slice of the Swissgrid workbook."
    )
    parser.add_argument("--workbook", default=DEFAULT_WORKBOOK, help="Path of the .xlsx file.")
    parser.add_argument("--from", dest="first", default="2026-08-30", help="First local day.")
    parser.add_argument(
        "--to", dest="last", default="2026-08-31", help="Last local day (inclusive)."
    )
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output CSV path.")
    return parser.parse_args(argv)


def _german_line(cell: object) -> str:
    return str(cell).split("\n", 1)[0].strip() if cell is not None else ""


def slice_workbook(workbook: Path, first: date, last: date) -> list[list[str]]:
    """Return the CSV rows (two header rows, then the quarter-hour rows)."""
    from openpyxl import load_workbook

    wb = load_workbook(workbook, read_only=True, data_only=True)
    try:
        ws = wb[SHEET]
        rows = ws.iter_rows(values_only=True)
        names = next(rows)
        units = next(rows)
        index = {}
        for name in SERIES:
            hits = [i for i, cell in enumerate(names) if _german_line(cell) == name]
            if len(hits) != 1:
                raise ValueError(f"series {name!r}: expected one column, found {len(hits)}")
            index[name] = hits[0]
        for name, i in index.items():
            if str(units[i]).strip() != UNIT:
                raise ValueError(f"series {name!r}: unit row is {units[i]!r}, expected {UNIT!r}")

        start = datetime.combine(first, datetime.min.time())
        stop = datetime.combine(last + timedelta(days=1), datetime.min.time())
        out = [["", *SERIES], ["Zeitstempel", UNIT, UNIT]]
        for row in rows:
            ts = row[0]
            if not isinstance(ts, datetime) or not start <= ts < stop:
                continue
            out.append([ts.strftime(LABEL_FORMAT), *(repr(row[index[n]]) for n in SERIES)])
        return out
    finally:
        wb.close()


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    first, last = date.fromisoformat(args.first), date.fromisoformat(args.last)
    if last < first:
        print("ERROR: --to is before --from", file=sys.stderr)
        return 2
    workbook = Path(args.workbook)
    if not workbook.is_file():
        print(f"ERROR: workbook not found: {workbook}", file=sys.stderr)
        return 2
    rows = slice_workbook(workbook, first, last)
    expected = ((last - first).days + 1) * 96
    if len(rows) - 2 != expected:
        print(f"ERROR: expected {expected} quarter hours, found {len(rows) - 2}", file=sys.stderr)
        return 3
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as fh:
        csv.writer(fh, lineterminator="\n").writerows(rows)
    print(f"WROTE {out} rows={len(rows) - 2} first={rows[2][0]} last={rows[-1][0]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
