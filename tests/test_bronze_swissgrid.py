import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from swiss_grid_lakehouse.bronze.swissgrid_energy_bronze import (
    COLUMNS,
    read_workbook,
    to_bronze_rows,
    write_bronze,
)

FIXTURE = Path(__file__).parent / "fixtures" / "swissgrid_ch_energy_2026-08-30.csv"
PULLED = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
SERIES = (
    "Summe endverbrauchte Energie Regelblock Schweiz",
    "Summe verbrauchte Energie Regelblock Schweiz",
)
ENGLISH = (
    "Total energy consumed by end users in the Swiss controlblock",
    "Total energy consumption Swiss controlblock",
)
PRICE = "Preis Ausgleichsenergie\nBalancing energy price"


def _write_csv(path: Path, unit: str = "kWh") -> Path:
    lines = FIXTURE.read_text(encoding="utf-8").splitlines()
    lines[1] = f"Zeitstempel,{unit},{unit}"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _make_xlsx(path: Path, headers, units, rows, sheet="Zeitreihen0h15") -> Path:
    openpyxl = pytest.importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    ws = workbook.active
    ws.title = sheet
    ws.append([None, *headers])
    ws.append(["Zeitstempel", *units])
    for row in rows:
        ws.append(list(row))
    workbook.save(path)
    return path


def _bilingual(german: tuple[str, ...]) -> list[str]:
    return [f"{g}\n{e}" for g, e in zip(german, ENGLISH, strict=True)]


def test_read_workbook_header_unit_and_data_rows():
    rows = read_workbook(FIXTURE)
    assert tuple(rows[0][1:]) == SERIES
    assert rows[1][1:] == ["kWh", "kWh"]
    assert len(rows) == 2 + 192
    assert rows[2][0] == "30.08.2026 00:00"


def test_rows_are_long_form_two_series_by_192_slots():
    rows = to_bronze_rows(FIXTURE, PULLED)
    assert len(rows) == 384
    assert {r[0] for r in rows} == {"swissgrid"}
    assert {r[1] for r in rows} == set(SERIES)
    assert {r[2] for r in rows} == {"kWh"}
    assert len({r[7] for r in rows}) == 1
    assert {r[8] for r in rows} == {FIXTURE.name}
    assert all(len(r) == len(COLUMNS) for r in rows)
    assert rows[0][3:6] == ("30.08.2026 00:00", 3, 1161986.329)
    assert rows[1][3:6] == ("30.08.2026 00:00", 3, 1350876.179)
    assert rows[-1][3:6] == ("31.08.2026 23:45", 194, 1358648.775)


def test_bad_unit_on_a_selected_series_raises(tmp_path):
    with pytest.raises(ValueError, match="unit 'MWh'"):
        read_workbook(_write_csv(tmp_path / "bad_unit.csv", unit="MWh"))


def test_non_numeric_value_raises(tmp_path):
    path = tmp_path / "bad_value.csv"
    path.write_text(FIXTURE.read_text(encoding="utf-8").replace("1161986.329", "n/a"))
    with pytest.raises(ValueError, match="not a number"):
        to_bronze_rows(path, PULLED)


def test_missing_series_in_csv_names_what_it_saw(tmp_path):
    path = tmp_path / "one_series.csv"
    path.write_text(
        "x,Other series,Summe verbrauchte Energie Regelblock Schweiz\n"
        "Zeitstempel,kWh,kWh\n30.08.2026 00:00,1,2\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Summe endverbrauchte.*first names seen.*Other series"):
        read_workbook(path)


def test_xlsx_reads_like_the_csv(tmp_path):
    rows = []
    for line in FIXTURE.read_text(encoding="utf-8").splitlines()[2:]:
        label, a, b = line.split(",")
        rows.append([label, float(a), float(b)])
    path = _make_xlsx(tmp_path / "sample.xlsx", _bilingual(SERIES), ["kWh", "kWh"], rows)
    from_xlsx = to_bronze_rows(path, PULLED)
    from_csv = to_bronze_rows(FIXTURE, PULLED)
    assert len(from_xlsx) == 384
    assert [r[:6] for r in from_xlsx] == [r[:6] for r in from_csv]


def test_xlsx_selects_by_header_and_ignores_other_units(tmp_path):
    headers = [PRICE, *_bilingual(SERIES)[::-1], "Summe produzierte Energie Regelblock Schweiz"]
    rows = [
        ["30.08.2026 00:00", 45.5, 20.0, 10.0, 7.0],
        ["30.08.2026 00:15", "n/a", 21.0, 11.0, 8.0],
    ]
    path = _make_xlsx(tmp_path / "wide.xlsx", headers, ["Euro/MWh", "kWh", "kWh", "MWh"], rows)
    out = to_bronze_rows(path, PULLED)
    assert [(r[1], r[3], r[5]) for r in out] == [
        (SERIES[0], "30.08.2026 00:00", 10.0),
        (SERIES[1], "30.08.2026 00:00", 20.0),
        (SERIES[0], "30.08.2026 00:15", 11.0),
        (SERIES[1], "30.08.2026 00:15", 21.0),
    ]


def test_xlsx_wrong_unit_on_selected_series_raises(tmp_path):
    rows = [["30.08.2026 00:00", 1.0, 2.0]]
    path = _make_xlsx(tmp_path / "unit.xlsx", _bilingual(SERIES), ["kWh", "Euro/MWh"], rows)
    with pytest.raises(ValueError, match=r"Summe verbrauchte.*unit 'Euro/MWh'"):
        read_workbook(path)


def test_xlsx_datetime_cell_is_kept_as_printed_start_label(tmp_path):
    rows = [[datetime(2026, 8, 30, 0, 0), 1.0, 2.0], [datetime(2026, 8, 30, 0, 15), 3.0, 4.0]]
    path = _make_xlsx(tmp_path / "dt.xlsx", _bilingual(SERIES), ["kWh", "kWh"], rows)
    out = to_bronze_rows(path, PULLED)
    assert [r[3] for r in out] == ["30.08.2026 00:00"] * 2 + ["30.08.2026 00:15"] * 2
    assert [r[4] for r in out] == [3, 3, 4, 4]


def test_xlsx_english_only_header_is_rejected(tmp_path):
    rows = [["30.08.2026 00:00", 1.0, 2.0]]
    path = _make_xlsx(tmp_path / "en.xlsx", list(ENGLISH), ["kWh", "kWh"], rows)
    with pytest.raises(ValueError, match="not found in row 1"):
        read_workbook(path)


def test_xlsx_with_wrong_sheet_name_raises(tmp_path):
    path = _make_xlsx(tmp_path / "wrong_sheet.xlsx", SERIES, ["kWh", "kWh"], [], sheet="Other")
    with pytest.raises(ValueError, match="Zeitreihen0h15.*Other"):
        read_workbook(path)


@pytest.mark.spark
def test_write_is_append_only_and_idempotent(spark, tmp_path):
    target = str(tmp_path / "bronze_swissgrid")
    rows = to_bronze_rows(FIXTURE, PULLED)
    assert write_bronze(spark, rows, target) is True
    assert spark.read.format("delta").load(target).count() == 384
    assert write_bronze(spark, rows, target) is False
    frame = spark.read.format("delta").load(target)
    assert frame.count() == 384
    assert frame.select("batch_id").distinct().count() == 1


@pytest.mark.spark
def test_cli_rerun_appends_nothing(spark, tmp_path, capsys):
    from swiss_grid_lakehouse.bronze.__main__ import main

    target = str(tmp_path / "bronze_swissgrid")
    source = tmp_path / "copy.csv"
    shutil.copy(FIXTURE, source)
    assert main(["--swissgrid", str(source), "--target", target]) == 0
    assert main(["--swissgrid", str(source), "--target", target]) == 0
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("BRONZE")]
    assert lines == ["BRONZE rows=384 rerun=0", "BRONZE rows=384 rerun=384"]
