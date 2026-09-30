import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from swiss_grid_lakehouse.silver.swissgrid_hourly import (
    HOURLY_COLUMNS,
    SERIES,
    SERIES_ENDUSER,
    slots_to_hourly,
    to_hourly,
)

FIXTURE = Path(__file__).parent / "fixtures" / "swissgrid_ch_energy_2026-08-30.csv"
PULLED = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
ZURICH = ZoneInfo("Europe/Zurich")


def _label(start_utc):
    return start_utc.astimezone(ZURICH).strftime("%d.%m.%Y %H:%M")


def _slot_rows(first_start_utc, values, batch="batch1", pulled=PULLED, enduser=None):
    """Slot tuples as the parser lists them: the label is the LOCAL start of each slot."""
    rows = []
    first_start_utc = first_start_utc.astimezone(
        UTC
    )  # add durations to an instant, not a wall time
    for i, kwh in enumerate(values):
        start = first_start_utc + timedelta(minutes=15 * i)
        row = (batch, "x.csv", i + 3, _label(start), kwh, pulled)
        rows.append(row + (enduser[i],) if enduser is not None else row)
    return rows


def _fixture_hours():
    """Hourly sums of the fixture CSV, computed without the code under test (kWh)."""
    with FIXTURE.open(newline="", encoding="utf-8") as handle:
        table = list(csv.reader(handle))
    header, data = table[0], table[2:]
    enduser_col = header.index(SERIES_ENDUSER)
    total_col = header.index(SERIES)
    hours = []
    for i in range(0, len(data), 4):
        chunk = data[i : i + 4]
        hours.append(
            (
                sum(float(r[total_col]) for r in chunk),
                sum(float(r[enduser_col]) for r in chunk),
            )
        )
    return hours


def test_formula_on_four_known_slots():
    rows = _slot_rows(datetime(2026, 9, 1, 0, 0, tzinfo=ZURICH), [1.0e6, 1.1e6, 1.2e6, 1.3e6])
    hourly = slots_to_hourly(rows)
    assert len(hourly) == 1
    source, area, _pulled, _batch, _file, ts_utc, kwh, enduser, difference = hourly[0]
    assert (source, area) == ("swissgrid", "CH")
    # the label 00:00 is the slot START: the hour starts at 22:00 UTC the evening before
    assert ts_utc == datetime(2026, 8, 31, 22, 0, tzinfo=UTC)
    assert kwh == pytest.approx(4.6e6)
    # (E1 + E2 + E3 + E4) kWh / 1000 kWh per MWh / 1 h = 4600 MW
    assert kwh / 1000 / 1 == pytest.approx(4600.0)
    assert (enduser, difference) == (None, None)


def test_difference_is_total_minus_enduser_per_hour():
    total = [1.0e6, 1.1e6, 1.2e6, 1.3e6]
    enduser = [0.9e6, 1.0e6, 1.0e6, 1.1e6]
    rows = _slot_rows(datetime(2026, 9, 1, 0, 0, tzinfo=ZURICH), total, enduser=enduser)
    (hour,) = slots_to_hourly(rows)
    assert hour[6] == pytest.approx(4.6e6)
    assert hour[7] == pytest.approx(4.0e6)
    assert hour[8] == pytest.approx(0.6e6)


def test_partial_hour_is_dropped_not_scaled():
    values = [1.0e6] * 4 + [1.0e6] * 2
    hourly = slots_to_hourly(_slot_rows(datetime(2026, 9, 1, 0, 0, tzinfo=ZURICH), values))
    assert [h[5] for h in hourly] == [datetime(2026, 8, 31, 22, 0, tzinfo=UTC)]


def test_label_that_breaks_the_sequence_is_rejected():
    rows = _slot_rows(datetime(2026, 9, 1, 0, 0, tzinfo=ZURICH), [1.0e6] * 8)
    rows[5] = rows[5][:3] + ("01.09.2026 09:00",) + rows[5][4:]
    with pytest.raises(ValueError, match="slot 5"):
        slots_to_hourly(rows)


def test_latest_pull_wins_per_slot():
    start = datetime(2026, 9, 1, 0, 0, tzinfo=ZURICH)
    old = _slot_rows(start, [1.0e6] * 4, batch="first", pulled=PULLED)
    new = _slot_rows(start, [2.0e6] * 4, batch="second", pulled=PULLED + timedelta(days=1))
    hourly = slots_to_hourly(old + new)
    assert len(hourly) == 1
    assert hourly[0][6] == pytest.approx(8.0e6)
    assert hourly[0][3] == "second"


def test_october_clock_change_gives_25_hours_without_spark():
    start = datetime(2026, 10, 25, 0, 0, tzinfo=ZURICH)
    day_end = datetime(2026, 10, 26, 0, 0, tzinfo=ZURICH).astimezone(UTC)
    slots = int((day_end - start.astimezone(UTC)) / timedelta(minutes=15))
    assert slots == 100
    hourly = slots_to_hourly(_slot_rows(start, [1.0e6] * slots))
    assert len(hourly) == 25
    local = [h[5].astimezone(ZURICH).strftime("%H:%M") for h in hourly]
    assert local.count("02:00") == 2
    # hour starts follow one another by exactly one hour of real time
    starts = [h[5] for h in hourly]
    assert all(b - a == timedelta(hours=1) for a, b in zip(starts, starts[1:], strict=False))


@pytest.mark.spark
def test_october_clock_change_in_memory_frame(spark):
    from swiss_grid_lakehouse.bronze.swissgrid_energy_bronze import COLUMNS, _schema

    start = datetime(2026, 10, 25, 0, 0, tzinfo=ZURICH)
    rows = []
    for batch, _file, idx, label, kwh, pulled in _slot_rows(start, [1.0e6] * 100):
        rows.append(("swissgrid", SERIES, "kWh", label, idx, kwh, pulled, batch, "x.csv"))
        rows.append(("swissgrid", SERIES_ENDUSER, "kWh", label, idx, 9.0e5, pulled, batch, "x.csv"))
        rows.append(("swissgrid", "other series", "kWh", label, idx, 9.0e9, pulled, batch, "x.csv"))
    bronze = spark.createDataFrame(rows, schema=_schema()).select(*COLUMNS)
    hourly = to_hourly(bronze)
    assert hourly.columns == HOURLY_COLUMNS
    assert hourly.count() == 25
    assert hourly.filter("energy_kwh <> 4000000.0").count() == 0
    assert hourly.filter("abs(energy_difference_kwh - 400000.0) > 1e-6").count() == 0


@pytest.fixture
def swissgrid_bronze(spark, tmp_path):
    from swiss_grid_lakehouse.bronze.swissgrid_energy_bronze import to_bronze_rows, write_bronze

    path = str(tmp_path / "bronze_sg")
    write_bronze(spark, to_bronze_rows(FIXTURE, PULLED), path)
    return spark.read.format("delta").load(path)


@pytest.mark.spark
def test_to_silver_converts_kwh_to_mw(spark, swissgrid_bronze):
    from swiss_grid_lakehouse.silver import SILVER_COLUMNS, to_silver

    silver = to_silver(to_hourly(swissgrid_bronze), value_col="energy_kwh")
    assert silver.columns == SILVER_COLUMNS
    assert silver.count() == 48
    ours = silver.filter("source = 'swissgrid' AND area = 'CH' AND resolution_min = 60")
    assert ours.count() == 48
    first = silver.orderBy("ts_utc").first()
    assert str(first["local_date"]) == "2026-08-30"
    assert first["ts_local"].strftime("%H:%M") == "00:00"
    total_kwh, _ = _fixture_hours()[0]
    assert first["load_mw"] == pytest.approx(total_kwh / 1000)
    assert 1000.0 <= first["load_mw"] <= 15000.0


@pytest.mark.spark
def test_difference_column_equals_total_minus_enduser(spark, swissgrid_bronze):
    from swiss_grid_lakehouse.silver import to_silver

    silver = to_silver(to_hourly(swissgrid_bronze), value_col="energy_kwh")
    rows = silver.orderBy("ts_utc").collect()
    expected = _fixture_hours()
    assert len(rows) == len(expected) == 48
    for row, (total, enduser) in zip(rows, expected, strict=True):
        assert row["load_mw"] == pytest.approx(total / 1000)
        assert row["load_enduser_mw"] == pytest.approx(enduser / 1000)
        assert row["load_difference_mw"] == pytest.approx((total - enduser) / 1000)
        assert row["load_difference_mw"] == pytest.approx(row["load_mw"] - row["load_enduser_mw"])
    differences = [r["load_difference_mw"] for r in rows]
    assert all(d is not None for d in differences)
    print(f"difference MW min={min(differences):.1f} max={max(differences):.1f}")


@pytest.mark.spark
def test_merge_twice_inserts_then_nothing(spark, swissgrid_bronze, tmp_path):
    from swiss_grid_lakehouse.silver import merge_silver, to_silver

    target = str(tmp_path / "silver")
    silver = to_silver(to_hourly(swissgrid_bronze), value_col="energy_kwh")
    assert merge_silver(spark, silver, target) == (48, 0)
    assert merge_silver(spark, silver, target) == (0, 0)
    assert spark.read.format("delta").load(target).count() == 48


@pytest.mark.spark
def test_no_key_collision_with_entsoe(spark, bronze_path, swissgrid_bronze, tmp_path):
    from swiss_grid_lakehouse.silver import merge_silver, to_silver

    target = str(tmp_path / "silver")
    entsoe = to_silver(spark.read.format("delta").load(bronze_path))
    swissgrid = to_silver(to_hourly(swissgrid_bronze), value_col="energy_kwh")
    assert merge_silver(spark, entsoe, target) == (48, 0)
    assert merge_silver(spark, swissgrid, target) == (48, 0)
    assert merge_silver(spark, entsoe, target) == (0, 0)
    assert merge_silver(spark, swissgrid, target) == (0, 0)
    table = spark.read.format("delta").load(target)
    assert table.count() == 96
    assert table.select("source", "area", "ts_utc").distinct().count() == 96
    assert table.filter("source = 'swissgrid'").count() == 48
    # ENTSO-E rows carry NULL in the Swissgrid-only columns, Swissgrid rows do not
    assert table.filter("source = 'entsoe' AND load_enduser_mw IS NOT NULL").count() == 0
    assert table.filter("source = 'entsoe' AND load_difference_mw IS NOT NULL").count() == 0
    assert table.filter("source = 'swissgrid' AND load_difference_mw IS NULL").count() == 0


@pytest.mark.spark
def test_difference_column_comment_quotes_swissgrid(spark, swissgrid_bronze, tmp_path):
    from swiss_grid_lakehouse.silver import merge_silver, to_silver

    target = str(tmp_path / "silver")
    merge_silver(spark, to_silver(to_hourly(swissgrid_bronze), value_col="energy_kwh"), target)
    described = {
        r["col_name"]: r["comment"] for r in spark.sql(f"DESCRIBE delta.`{target}`").collect()
    }
    comment = described["load_difference_mw"]
    for phrase in ("grid losses", "power plant's own requirements", "pumps", "Uebersicht"):
        assert phrase in comment
