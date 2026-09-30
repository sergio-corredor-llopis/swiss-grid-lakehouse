"""Reconcile the hourly Silver table with the published daily national consumption.

    python -m swiss_grid_lakehouse.reconcile --silver DIR --daily DIR

Each Silver source is rolled up to Europe/Zurich days and set beside the daily figure the
Swiss Federal Office of Energy (SFOE) publishes from Swissgrid data. The published figure is a
control total: it shows whether the hourly pipeline lost or doubled a day.

Two of the three comparisons are like for like, because the published series and the Swissgrid
workbook have the same origin: Swissgrid total consumed energy against Landesverbrauch, and
Swissgrid end-user consumption against Endverbrauch. They use the tight tolerance. ENTSO-E
actual total load against Landesverbrauch compares two different publishers, and uses the wide
tolerance.

The newest published days are revised later, so a day whose figure is still young is
`preliminary`: it is printed and counted, and never sets the exit code. Whether a figure is
young depends only on the registry stamp (`metadata_modified`) of the batch it came from,
never on today's date, so the same rows give the same result in any year.

All arithmetic is in pure functions over plain rows (`reconcile`); only the two loaders read
Delta tables.

Exit codes: 0 no final day beyond tolerance; 3 a final day beyond tolerance, no overlap between
the two sides, or an overlapping day with missing hours; 2 a table could not be read.
"""

from __future__ import annotations

import argparse
import math
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

# Set from the measured daily differences between the published figures and the Swissgrid
# workbook over the 243 local days 2026-01-01 to 2026-08-31 (published data as of 2026-09-30).
#
# PRELIM_DAYS = 30: the one day of at most 30 days of age differs by 7.55 % (total) and 7.81 %
#   (end-user); the 242 older days differ by at most 4.08 %, and their 95th percentile is
#   0.99 % (total) and 0.87 % (end-user).
# TIGHT_PCT = 6.5: the largest difference of any day older than PRELIM_DAYS is 4.08 %; the
#   tolerance leaves room above it and stays below the 7.55 % of the young day.
# WIDE_PCT = 5.0: ENTSO-E differs from the workbook total by 1.86 % and 2.41 % on the two
#   recorded days; twice the larger one, 4.82 %, rounded up. It rests on those two days only.
TIGHT_PCT = 6.5
WIDE_PCT = 5.0
PRELIM_DAYS = 30

EXIT_PASS = 0
EXIT_USAGE = 2
EXIT_FAIL = 3

# (source, series, published series, Silver roll-up key, Bronze column, tolerance kind)
SERIES = (
    ("swissgrid", "total", "landesverbrauch", "gwh_total", "national_gwh", "tight"),
    ("swissgrid", "enduser", "endverbrauch", "gwh_enduser", "final_gwh", "tight"),
    ("entsoe", "total", "landesverbrauch", "gwh_total", "national_gwh", "wide"),
)


def age_days(local_date: date, registry_modified: str) -> int:
    """Age in days of a figure for `local_date`, at the registry stamp of its batch.

    Counted from the end of the day, which is the earliest moment the figure can exist: a day
    that ended the day before the stamp is 0 days old. Only the date part of the stamp is used.
    """
    return (date.fromisoformat(registry_modified[:10]) - local_date).days - 1


def latest_per_day(daily_rows: Iterable[Mapping[str, Any]]) -> dict[date, Mapping[str, Any]]:
    """Keep, for every local day, the row of the batch with the latest registry stamp.

    Rows carry `local_date` (ISO text), `registry_modified` and `batch_id`; ties on the stamp
    are broken by `batch_id` so the choice never depends on row order.
    """
    best: dict[date, Mapping[str, Any]] = {}
    for row in daily_rows:
        day = date.fromisoformat(str(row["local_date"])[:10])
        key = (row["registry_modified"], row.get("batch_id") or "")
        held = best.get(day)
        if held is None or key > (held["registry_modified"], held.get("batch_id") or ""):
            best[day] = row
    return best


@dataclass(frozen=True)
class DayResult:
    day: date
    pipeline_gwh: float | None
    published_gwh: float
    diff_pct: float | None
    hours: int
    status: str  # PASS, PRELIMINARY, FAIL or INCOMPLETE


@dataclass
class SeriesResult:
    source: str
    series: str
    published: str
    tol: float
    days: list[DayResult] = field(default_factory=list)

    @property
    def preliminary(self) -> int:
        return sum(d.status == "PRELIMINARY" for d in self.days)

    @property
    def final(self) -> int:
        return sum(d.status in ("PASS", "FAIL") for d in self.days)

    @property
    def passed(self) -> bool:
        return bool(self.days) and all(d.status in ("PASS", "PRELIMINARY") for d in self.days)

    def lines(self) -> list[str]:
        label = f"source={self.source} series={self.series}"
        out = [
            f"DAY {d.day.isoformat()} {label} pipeline_gwh={_num(d.pipeline_gwh)} "
            f"published_gwh={_num(d.published_gwh)} diff_pct={_num(d.diff_pct)} "
            f"hours={d.hours} status={d.status}"
            for d in self.days
        ]
        counted = [d for d in self.days if d.pipeline_gwh is not None and d.status != "INCOMPLETE"]
        pipeline = sum(d.pipeline_gwh for d in counted if d.pipeline_gwh is not None)
        published = sum(d.published_gwh for d in counted)
        total_pct = _pct(pipeline, published) if counted else None
        out.append(
            f"RECONCILE {label} published={self.published} days={len(self.days)} "
            f"pipeline_gwh={_num(pipeline)} published_gwh={_num(published)} "
            f"diff_pct={_num(total_pct)} tol={self.tol:g} preliminary={self.preliminary} "
            f"final={self.final} {'PASS' if self.passed else 'FAIL'}"
        )
        return out


def _pct(pipeline: float, published: float) -> float:
    if published == 0:
        return 0.0 if pipeline == 0 else math.inf
    return 100.0 * (pipeline - published) / published


def _num(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def reconcile(
    rollups: Mapping[str, Sequence[Mapping[str, Any]]],
    daily_rows: Iterable[Mapping[str, Any]],
    tight_pct: float = TIGHT_PCT,
    wide_pct: float = WIDE_PCT,
    prelim_days: int = PRELIM_DAYS,
) -> list[SeriesResult]:
    """Compare every Silver roll-up with the published daily rows; return one result per series.

    `rollups` maps a source name to its daily roll-up (`silver.daily_rollup.rollup_rows`).
    A day is compared only when both sides have it. It is INCOMPLETE if its hours are fewer
    than the calendar day has, or an end-user hour is missing; the partial figure is shown and
    never scaled. Otherwise it is PRELIMINARY if `age_days <= prelim_days`, else PASS or FAIL
    by `abs(diff_pct) <= tolerance`. The tolerance is `wide_pct` for ENTSO-E and `tight_pct`
    for the two Swissgrid series.
    """
    published_by_day = latest_per_day(daily_rows)
    results = []
    for source, series, published_name, key, column, kind in SERIES:
        tol = tight_pct if kind == "tight" else wide_pct
        result = SeriesResult(source, series, published_name, tol)
        for row in rollups.get(source, ()):
            published_row = published_by_day.get(row["local_date"])
            if published_row is None:
                continue
            published = float(published_row[column])
            value = row.get(key)
            diff = None if value is None else _pct(value, published)
            if row["incomplete"] or value is None:
                status = "INCOMPLETE"
            elif age_days(row["local_date"], published_row["registry_modified"]) <= prelim_days:
                status = "PRELIMINARY"
            else:
                status = "PASS" if abs(diff or 0.0) <= tol else "FAIL"
            result.days.append(
                DayResult(row["local_date"], value, published, diff, row["hours"], status)
            )
        results.append(result)
    return results


def exit_code(results: Iterable[SeriesResult]) -> int:
    return EXIT_PASS if all(r.passed for r in results) else EXIT_FAIL


def _read_table(spark: Any, location: str) -> Any:
    from swiss_grid_lakehouse.silver import ch_load_silver as m

    if m._is_path(location):
        return spark.read.format("delta").load(location)
    return spark.table(location)


def load_rollups(silver: str) -> dict[str, list[dict[str, Any]]]:
    """Read the Silver table and roll each source up to local days."""
    from pyspark.sql import functions as F

    from swiss_grid_lakehouse.silver.daily_rollup import SOURCES, rollup_daily
    from swiss_grid_lakehouse.spark import get_spark

    table = _read_table(get_spark("local"), silver)
    if "load_enduser_mw" not in table.columns:
        table = table.withColumn("load_enduser_mw", F.lit(None).cast("double"))
    return {source: rollup_daily(table, source) for source in SOURCES}


def load_daily_rows(daily: str) -> list[dict[str, Any]]:
    """Read the Bronze daily table: local_date, national_gwh, final_gwh, stamp and batch id."""
    from swiss_grid_lakehouse.spark import get_spark

    table = _read_table(get_spark("local"), daily)
    cols = ["local_date", "national_gwh", "final_gwh", "registry_modified", "batch_id"]
    return [r.asDict() for r in table.select(*cols).collect()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m swiss_grid_lakehouse.reconcile",
        description=(
            "Roll the Silver table up to local days and compare it with the published "
            "daily national consumption."
        ),
        epilog=(
            f"Tolerances: {TIGHT_PCT:g} % for the two Swissgrid series, {WIDE_PCT:g} % for "
            f"ENTSO-E. A day at most {PRELIM_DAYS} days old at the registry stamp is "
            "preliminary. Exit 0 pass; 3 a final day beyond tolerance, no overlap or an "
            "incomplete day; 2 unreadable table."
        ),
    )
    parser.add_argument("--silver", required=True, help="Silver Delta table path or name")
    parser.add_argument("--daily", required=True, help="Bronze daily Delta table path or name")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        rollups = load_rollups(args.silver)
        daily_rows = load_daily_rows(args.daily)
    except Exception as exc:  # unreadable table, missing path, missing column
        print(f"RECONCILE error: cannot read the tables: {exc}", file=sys.stderr)
        return EXIT_USAGE
    results = reconcile(rollups, daily_rows)
    for result in results:
        print("\n".join(result.lines()))
    return exit_code(results)
