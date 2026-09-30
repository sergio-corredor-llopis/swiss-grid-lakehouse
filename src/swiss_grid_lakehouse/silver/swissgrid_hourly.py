"""Turn Swissgrid 15-minute Bronze rows into hourly rows that `to_silver` can take.

Swissgrid publishes energy per 15-minute slot in kWh. The label of a slot is its START, in
local time: the sheet begins at 00:00 and ends at 23:45. Silver keeps one row per hour, keyed
by the UTC start of the hour, in MW.

Formula, for hour h with its four slots E1..E4 (kWh):

    load_mw(h) = (E1 + E2 + E3 + E4) [kWh] / 1000 [kWh per MWh] / 1 [h]

That is the mean power in MW over the hour. This module only sums the four slots into
`energy_kwh`; the division by 1000 is the unit-map entry `kWh_per_h` in `ch_load_silver`, so
every unit conversion stays in one place. An hour with fewer than four slots is dropped, not
scaled; the quality gate then reports it as a missing hour.

Three values are carried per hour, all in kWh until `to_silver` converts them:

    energy_kwh             "Summe verbrauchte Energie Regelblock Schweiz" (total consumed),
                           the series that is compared to ENTSO-E and gated;
    energy_enduser_kwh     "Summe endverbrauchte Energie Regelblock Schweiz" (end-user);
    energy_difference_kwh  total minus end-user, NULL when either is missing for the hour.

What the difference contains is Swissgrid's own definition, from the sheet `Uebersicht` of the
workbook (English text). Total consumed: "The sum contains all the energy consumed in the
transmission and distribution grids. Included are grid losses, energy consumed for a power
plant's own requirements and to drive the pumps in pumped storage hydro power plant." End-user
consumption: "Not included are grid losses or energy consumed for power plant's own
requirements or to drive the pumps in pumped storage hydro power plant." So the difference is
what the total holds beyond end-user consumption: grid losses, power plants' own requirements
and the pumps of pumped storage plants. Swissgrid does not publish the three parts separately,
and this module does not split them.

Time: the label is the slot START, so `slot_start_utc = anchor_utc + 15 min * slot_number`,
where the anchor is the first label read as Europe/Zurich local time. Each label is also
converted on its own and must give the same instant. Inside the repeated hour of the October
clock change the same local label occurs twice; there the order in the sheet decides (first
pass = summer time, second pass = winter time), so that day has 25 hours. The data are the
Swissgrid AG aggregated energy data of the Swiss control block.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from swiss_grid_lakehouse.bronze.entsoe_load_bronze import AREA
from swiss_grid_lakehouse.bronze.swissgrid_energy_bronze import LABEL_FORMAT, SOURCE

SERIES = "Summe verbrauchte Energie Regelblock Schweiz"
SERIES_ENDUSER = "Summe endverbrauchte Energie Regelblock Schweiz"
LOCAL_TZ = ZoneInfo("Europe/Zurich")
SLOT = timedelta(minutes=15)
SLOTS_PER_HOUR = 4
HOURLY_COLUMNS = [
    "source",
    "area",
    "pulled_at",
    "batch_id",
    "source_file",
    "ts_utc",
    "energy_kwh",
    "energy_enduser_kwh",
    "energy_difference_kwh",
]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _local_to_utc(naive: datetime, fold: int = 0) -> datetime:
    return naive.replace(tzinfo=LOCAL_TZ, fold=fold).astimezone(UTC)


def _is_repeated(naive: datetime) -> bool:
    """True when this wall-clock time happens twice (the October clock change)."""
    return _local_to_utc(naive, 0) != _local_to_utc(naive, 1)


def _slot_starts(labels: list[str]) -> list[datetime]:
    """Return the UTC slot start for each label, in sheet order, and check every label.

    A label is the slot START in local time. The anchor is the first label localised to
    Europe/Zurich; every later slot is 15 minutes after the one before.
    """
    naive = [datetime.strptime(label, LABEL_FORMAT) for label in labels]
    anchor = _local_to_utc(naive[0])
    seen: dict[datetime, int] = defaultdict(int)
    starts: list[datetime] = []
    for number, (label, wall) in enumerate(zip(labels, naive, strict=True)):
        start = anchor + SLOT * number
        if _is_repeated(wall):
            expected = _local_to_utc(wall, seen[wall])
            seen[wall] += 1
            if seen[wall] > 2:
                raise ValueError(f"label {label!r} occurs more than twice")
        else:
            expected = _local_to_utc(wall)
            if expected.astimezone(LOCAL_TZ).replace(tzinfo=None) != wall:
                raise ValueError(f"label {label!r} is not a local time that exists")
        if expected != start:
            raise ValueError(
                f"label {label!r} at slot {number} gives {expected:%Y-%m-%d %H:%M} UTC "
                f"as slot start, expected {start:%Y-%m-%d %H:%M} UTC"
            )
        starts.append(start)
    return starts


def slots_to_hourly(rows: list[tuple]) -> list[tuple]:
    """Aggregate slot rows to hourly rows (slot start = the label).

    `rows` are tuples (batch_id, source_file, row_index, ts_label, energy_kwh, pulled_at) of
    the total-consumed series, optionally with a seventh element, the end-user kWh of the
    same slot (None when absent); `pulled_at` is a timezone-aware datetime. Each batch is one
    workbook, so the slot number is the order of its rows. Slots seen in several batches are
    reduced to the latest `pulled_at` (ties: the larger batch_id). Returns tuples ordered as
    HOURLY_COLUMNS, sorted by hour, for hours that have all four slots. The end-user value of
    an hour is None unless all four slots have one; the difference (total minus end-user, in
    kWh) is None whenever the end-user value is.
    """
    by_batch: dict[str, list[tuple]] = defaultdict(list)
    for row in rows:
        by_batch[row[0]].append(row)
    latest: dict[datetime, tuple] = {}
    for batch_rows in by_batch.values():
        batch_rows.sort(key=lambda r: r[2])
        starts = _slot_starts([r[3] for r in batch_rows])
        for start, row in zip(starts, batch_rows, strict=True):
            batch_id, source_file, _idx, _label, kwh, pulled_at = row[:6]
            enduser = row[6] if len(row) > 6 else None
            candidate = (pulled_at, batch_id, source_file, kwh, enduser)
            if start not in latest or candidate[:2] > latest[start][:2]:
                latest[start] = candidate
    hours: dict[datetime, list[tuple]] = defaultdict(list)
    for start, slot in latest.items():
        hours[start.replace(minute=0, second=0, microsecond=0)].append(slot)
    out: list[tuple] = []
    for hour in sorted(hours):
        slots = hours[hour]
        if len(slots) < SLOTS_PER_HOUR:
            continue
        pulled_at, batch_id, source_file = max(s[:3] for s in slots)
        total = sum(s[3] for s in slots)
        if any(s[4] is None for s in slots):
            enduser, difference = None, None
        else:
            enduser = sum(s[4] for s in slots)
            difference = total - enduser
        out.append(
            (SOURCE, AREA, pulled_at, batch_id, source_file, hour, total, enduser, difference)
        )
    return out


def _hourly_schema() -> Any:
    from pyspark.sql import types as t

    return t.StructType(
        [
            t.StructField("source", t.StringType(), False),
            t.StructField("area", t.StringType(), False),
            t.StructField("pulled_at", t.TimestampType(), False),
            t.StructField("batch_id", t.StringType(), False),
            t.StructField("source_file", t.StringType(), False),
            t.StructField("ts_utc", t.TimestampType(), False),
            t.StructField("energy_kwh", t.DoubleType(), False),
            t.StructField("energy_enduser_kwh", t.DoubleType(), True),
            t.StructField("energy_difference_kwh", t.DoubleType(), True),
        ]
    )


def to_hourly(bronze_df: Any) -> Any:
    """Return hourly rows (columns HOURLY_COLUMNS) from a Swissgrid Bronze frame.

    The total-consumed series drives the rows; the end-user series is paired with it by
    (batch_id, row_index) and may be absent for a slot. The result feeds
    `to_silver(df, value_col="energy_kwh")`, which converts the three kWh columns to MW.
    The Bronze table is small (one row per series and slot), so the slot arithmetic runs on
    the driver.
    """
    from pyspark.sql import functions as f

    picked = (
        bronze_df.filter(f.col("series").isin(SERIES, SERIES_ENDUSER))
        .select(
            "series",
            "batch_id",
            "source_file",
            "row_index",
            "ts_label",
            "energy_kwh",
            f.unix_micros("pulled_at").alias("pulled_us"),
        )
        .collect()
    )
    enduser = {
        (r["batch_id"], r["row_index"]): r["energy_kwh"]
        for r in picked
        if r["series"] == SERIES_ENDUSER
    }
    rows = [
        (
            r["batch_id"],
            r["source_file"],
            r["row_index"],
            r["ts_label"],
            r["energy_kwh"],
            _EPOCH + timedelta(microseconds=r["pulled_us"]),
            enduser.get((r["batch_id"], r["row_index"])),
        )
        for r in picked
        if r["series"] == SERIES
    ]
    return bronze_df.sparkSession.createDataFrame(slots_to_hourly(rows), schema=_hourly_schema())
