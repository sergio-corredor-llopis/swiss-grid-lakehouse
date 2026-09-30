"""Compare the ENTSO-E and Swissgrid load series in the Silver table.

    python -m swiss_grid_lakehouse.compare --silver DST [--tol-pct X]

Both publishers describe Swiss consumption, but hour by hour they do not agree closely: on the
two recorded days the hourly values differ by about 10 % on average and by up to 36 %, while
the daily totals differ by about 2 %. The definitions differ (Swissgrid's total consumed energy
includes grid losses, power plants' own requirements and the pumps of pumped storage plants;
ENTSO-E's total load is derived from transmission-system-operator metering), and the cause of
the hourly gap is not established. The pump-consumption series is not available, so no
pumped-storage adjustment is computed.

The command reports the hourly differences and gates on the daily total only.

Report (never decides the exit code). For every hour present in both sources, joined on
(area, ts_utc), with the ENTSO-E value above zero:

    pct = 100 * (swissgrid - entsoe) / entsoe

The minimum, median, maximum and count of `pct` are printed by hour of day (local time,
0..23, pooled over the days present), once against Swissgrid total consumption (`load_mw`) and
once against Swissgrid end-user consumption (`load_enduser_mw`), followed by the lines
`HOURLY total: ...` and `HOURLY end-user: ...`.

Gate. Per local day (Europe/Zurich) that is complete in both sources (24 hours; 23 or 25 on a
clock-change day), the sum of ENTSO-E MWh is compared with the sum of Swissgrid total
consumption MWh: `gap = 100 * (swissgrid - entsoe) / entsoe`. The check passes when every
day's absolute gap is at most the tolerance `--tol-pct`.

Default tolerance. On the two recorded local days (2026-08-30 and 2026-08-31) the daily gaps are
+1.89 % and +2.47 % (Swissgrid minus ENTSO-E, relative to ENTSO-E). The default is the larger
absolute gap, 2.47 %, rounded up to the next whole percent: 3 %.

Exit codes:

    0  PASS: at least one complete day and every daily gap within the tolerance
    3  FAIL: a daily gap above the tolerance, or no complete day overlaps
    2  usage or input error (bad flag, negative tolerance, Silver table unreadable)

The last line printed is `COMPARE days=n hours=n daily_max_abs_pct=.. tol=.. PASS|FAIL`.

All arithmetic is in pure functions over plain rows (`build_report`), which need no Spark;
only `_load_rows` reads the Delta table.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

DEFAULT_TOL_PCT = 3.0
EXIT_PASS = 0
EXIT_USAGE = 2
EXIT_FAIL = 3
LOCAL_TZ = ZoneInfo("Europe/Zurich")
HOUR = timedelta(hours=1)


def _pct(swissgrid: float, entsoe: float) -> float:
    return 100.0 * (swissgrid - entsoe) / entsoe


def _as_utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def _local_hour(key: tuple[Any, datetime]) -> int:
    return key[1].astimezone(LOCAL_TZ).hour


def hours_in_local_day(day: date) -> int:
    """Number of hours in a local calendar day: 24, or 23 / 25 on a clock-change day."""
    start = datetime(day.year, day.month, day.day, tzinfo=LOCAL_TZ).astimezone(UTC)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=LOCAL_TZ).astimezone(UTC)
    return round((end - start) / HOUR)


@dataclass(frozen=True)
class DayGap:
    """Daily energy of both sources for one complete local day."""

    day: date
    entsoe_mwh: float
    swissgrid_mwh: float

    @property
    def gap_pct(self) -> float:
        return _pct(self.swissgrid_mwh, self.entsoe_mwh)


@dataclass(frozen=True)
class HourStat:
    """min, median, max and count of the percent gap for one hour of day."""

    hour: int
    n: int
    minimum: float
    median: float
    maximum: float


@dataclass(frozen=True)
class Report:
    """Hourly percent-gap statistics plus the daily gate."""

    days: list[DayGap]
    hours: int
    tol_pct: float
    total_by_hour: list[HourStat] = field(default_factory=list)
    enduser_by_hour: list[HourStat] = field(default_factory=list)
    total_pooled: HourStat | None = None
    enduser_pooled: HourStat | None = None

    @property
    def daily_max_abs_pct(self) -> float:
        return max((abs(d.gap_pct) for d in self.days), default=0.0)

    @property
    def passed(self) -> bool:
        """True when a complete day exists and every daily gap is within the tolerance."""
        return bool(self.days) and all(abs(d.gap_pct) <= self.tol_pct for d in self.days)

    @property
    def exit_code(self) -> int:
        return EXIT_PASS if self.passed else EXIT_FAIL

    def line(self) -> str:
        """The one summary line the command ends with."""
        tol = f"{self.tol_pct:g}"
        if not self.days:
            return (
                f"COMPARE days=0 hours=0 daily_max_abs_pct=n/a tol={tol} FAIL "
                "no complete day overlaps"
            )
        verdict = "PASS" if self.passed else "FAIL"
        return (
            f"COMPARE days={len(self.days)} hours={self.hours} "
            f"daily_max_abs_pct={self.daily_max_abs_pct:.2f} tol={tol} {verdict}"
        )

    def lines(self) -> list[str]:
        """The whole report: hourly table, HOURLY lines, daily lines, summary line."""
        out = ["hour   n | total min  median     max | end-user min  median     max"]
        end = {s.hour: s for s in self.enduser_by_hour}
        for s in self.total_by_hour:
            e = end.get(s.hour)
            right = (
                f"{e.minimum:>12.2f} {e.median:>7.2f} {e.maximum:>7.2f}" if e else " " * 12 + "n/a"
            )
            out.append(
                f"{s.hour:>4} {s.n:>3} | {s.minimum:>9.2f} {s.median:>7.2f} {s.maximum:>7.2f} | "
                f"{right}"
            )
        out.append(_hourly_line("total", self.total_pooled))
        out.append(_hourly_line("end-user", self.enduser_pooled))
        for d in self.days:
            out.append(
                f"DAILY {d.day.isoformat()} entsoe_mwh={d.entsoe_mwh:.1f} "
                f"swissgrid_mwh={d.swissgrid_mwh:.1f} gap_pct={d.gap_pct:.2f}"
            )
        out.append(self.line())
        return out


def _hourly_line(name: str, s: HourStat | None) -> str:
    if s is None:
        return f"HOURLY {name}: n/a"
    return (
        f"HOURLY {name}: min {s.minimum:.2f} median {s.median:.2f} max {s.maximum:.2f} "
        f"(n={s.n}, percent)"
    )


def _stat(hour: int, values: list[float]) -> HourStat:
    return HourStat(hour, len(values), min(values), statistics.median(values), max(values))


def _by_hour(pairs: Iterable[tuple[int, float]]) -> tuple[list[HourStat], HourStat | None]:
    groups: dict[int, list[float]] = defaultdict(list)
    for hour, pct in pairs:
        groups[hour].append(pct)
    everything = [p for values in groups.values() for p in values]
    stats = [_stat(h, groups[h]) for h in sorted(groups)]
    return stats, (_stat(-1, everything) if everything else None)


def build_report(rows: Iterable[Mapping[str, Any]], tol_pct: float = DEFAULT_TOL_PCT) -> Report:
    """Join `entsoe` and `swissgrid` rows on (area, ts_utc) and build the report.

    Each row needs `source`, `area`, `ts_utc` and `load_mw`; a Swissgrid row may also carry
    `load_enduser_mw`. Rows of other sources are ignored, and an hour with an ENTSO-E value of
    zero or less is skipped because the percentage is undefined there. Only complete local days
    (every hour of the day present in both sources) enter the report.
    """
    entsoe: dict[tuple[Any, datetime], float] = {}
    total: dict[tuple[Any, datetime], float] = {}
    enduser: dict[tuple[Any, datetime], float] = {}
    for row in rows:
        key = (row["area"], _as_utc(row["ts_utc"]))
        if row["source"] == "entsoe":
            entsoe[key] = float(row["load_mw"])
        elif row["source"] == "swissgrid":
            total[key] = float(row["load_mw"])
            if row.get("load_enduser_mw") is not None:
                enduser[key] = float(row["load_enduser_mw"])
    joined = sorted(k for k in entsoe.keys() & total.keys() if entsoe[k] > 0)
    per_day: dict[tuple[Any, date], list[tuple[Any, datetime]]] = defaultdict(list)
    for key in joined:
        per_day[(key[0], key[1].astimezone(LOCAL_TZ).date())].append(key)
    days: list[DayGap] = []
    used: list[tuple[Any, datetime]] = []
    for (_area, day), keys in sorted(per_day.items(), key=lambda kv: (kv[0][1], str(kv[0][0]))):
        if len(keys) != hours_in_local_day(day):
            continue
        used.extend(keys)
        days.append(DayGap(day, sum(entsoe[k] for k in keys), sum(total[k] for k in keys)))
    total_stats, total_pool = _by_hour((_local_hour(k), _pct(total[k], entsoe[k])) for k in used)
    end_stats, end_pool = _by_hour(
        (_local_hour(k), _pct(enduser[k], entsoe[k])) for k in used if k in enduser
    )
    return Report(days, len(used), tol_pct, total_stats, end_stats, total_pool, end_pool)


def _load_rows(silver: str) -> list[Mapping[str, Any]]:
    """Read source, area, ts_utc, load_mw and load_enduser_mw from the Silver Delta table."""
    from swiss_grid_lakehouse.silver import ch_load_silver as m
    from swiss_grid_lakehouse.spark import get_spark

    spark = get_spark("local")
    if m._is_path(silver):
        table = spark.read.format("delta").load(silver)
    else:
        table = spark.table(silver)
    enduser = "load_enduser_mw" if "load_enduser_mw" in table.columns else None
    cols = ["source", "area", table["ts_utc"].cast("long").alias("epoch"), "load_mw"]
    if enduser:
        cols.append(enduser)
    rows = []
    for r in table.filter(table["source"].isin("entsoe", "swissgrid")).select(*cols).collect():
        d = r.asDict()
        d["ts_utc"] = datetime.fromtimestamp(d.pop("epoch"), UTC)
        rows.append(d)
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m swiss_grid_lakehouse.compare",
        description=(
            "Report the hourly difference between ENTSO-E and Swissgrid load (min, median, "
            "max by hour of day) and gate on the daily total."
        ),
        epilog=(
            "Exit codes: 0 PASS; 3 gate FAIL or no complete day overlaps; 2 usage or input "
            "error. On the two recorded days the daily gaps are +1.89 % (2026-08-30) and "
            "+2.47 % (2026-08-31); the default tolerance, 3 %, is the larger one rounded up "
            "to the next whole percent."
        ),
    )
    parser.add_argument("--silver", required=True, help="Silver Delta table path or name")
    parser.add_argument(
        "--tol-pct",
        type=float,
        default=DEFAULT_TOL_PCT,
        help="largest accepted absolute daily gap in percent (default: %(default)g)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.tol_pct < 0:
        print("COMPARE error: --tol-pct must not be negative", file=sys.stderr)
        return EXIT_USAGE
    try:
        rows = _load_rows(args.silver)
    except Exception as exc:  # unreadable table, missing path, missing column
        print(f"COMPARE error: cannot read Silver table {args.silver!r}: {exc}", file=sys.stderr)
        return EXIT_USAGE
    report = build_report(rows, args.tol_pct)
    print("\n".join(report.lines()))
    return report.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
