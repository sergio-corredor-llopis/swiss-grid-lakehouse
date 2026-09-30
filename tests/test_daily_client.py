import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from swiss_grid_lakehouse.daily import (
    COLUMNS,
    batch_id_for,
    parse_daily_csv,
    read_registry_stamp,
    run,
    to_bronze_rows,
)
from swiss_grid_lakehouse.daily.ogd_client import CSV_URL, PACKAGE_URL, USER_AGENT, fetch_bytes
from swiss_grid_lakehouse.daily.ogd_client import main as daily_main

FIXTURES = Path(__file__).parent / "fixtures"
CSV = FIXTURES / "ogd103_2026-08-30_31.csv"
PACKAGE = FIXTURES / "ckan_package_show_2026-09-29.json"
PULLED = datetime(2026, 9, 30, 6, 0, tzinfo=UTC)


class FakeRegistry:
    """An injected `get`: answers 403 unless a User-Agent header is present."""

    def __init__(self, package: bytes, csv_bytes: bytes):
        self.answers = {PACKAGE_URL: package, CSV_URL: csv_bytes}
        self.calls: list[str] = []

    def __call__(self, url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        self.calls.append(url)
        if not any(name.lower() == "user-agent" for name in headers):
            return 403, b""
        if url not in self.answers:
            return 404, b""
        return 200, self.answers[url]


def _registry() -> FakeRegistry:
    return FakeRegistry(PACKAGE.read_bytes(), CSV.read_bytes())


def _with_stamp(stamp: str) -> bytes:
    record = json.loads(PACKAGE.read_text(encoding="utf-8"))
    record["metadata_modified"] = stamp
    return json.dumps(record).encode("utf-8")


def test_stamp_read_from_recorded_answer():
    stamp = read_registry_stamp(PACKAGE.read_bytes())
    assert stamp == json.loads(PACKAGE.read_text(encoding="utf-8"))["metadata_modified"]
    assert stamp.startswith("2026-")


def test_stamp_read_from_full_package_show_answer():
    answer = json.dumps({"success": True, "result": {"metadata_modified": "2027-01-15T00:00:00"}})
    assert read_registry_stamp(answer) == "2027-01-15T00:00:00"


def test_stamp_missing_is_rejected():
    with pytest.raises(ValueError):
        read_registry_stamp(b'{"result": {}}')


def test_request_without_user_agent_is_refused():
    registry = _registry()
    assert registry(PACKAGE_URL, {}) == (403, b"")
    assert registry(PACKAGE_URL, {"Accept": "application/json"}) == (403, b"")
    assert registry(PACKAGE_URL, {"User-Agent": "x"})[0] == 200


def test_client_names_itself_and_fails_on_403():
    registry = _registry()
    assert fetch_bytes(PACKAGE_URL, registry) == PACKAGE.read_bytes()
    assert USER_AGENT

    def anonymous(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return registry(url, {})

    with pytest.raises(RuntimeError, match="403"):
        fetch_bytes(PACKAGE_URL, anonymous)


def test_rows_sorted_by_date_and_datum_kept_as_text():
    shuffled = (
        b"Datum,Landesverbrauch_GWh,Endverbrauch_GWh\n2026-08-31,149,125\n2026-08-30,149,110\n"
    )
    rows = parse_daily_csv(shuffled)
    assert rows == [("2026-08-30", 149.0, 110.0), ("2026-08-31", 149.0, 125.0)]
    assert all(isinstance(row[0], str) for row in rows)


def test_byte_order_mark_is_ignored():
    assert parse_daily_csv(b"\xef\xbb\xbf" + CSV.read_bytes()) == parse_daily_csv(CSV.read_bytes())


def test_wrong_header_is_rejected():
    with pytest.raises(ValueError):
        parse_daily_csv(b"day,a,b\n2026-08-30,1,2\n")


def test_bronze_rows_shape():
    rows = to_bronze_rows(CSV.read_bytes(), "2026-09-30T02:56:07.812085", PULLED)
    assert COLUMNS == [
        "source",
        "local_date",
        "national_gwh",
        "final_gwh",
        "pulled_at",
        "batch_id",
        "registry_modified",
    ]
    assert [r[1] for r in rows] == ["2026-08-30", "2026-08-31"]
    assert [(r[2], r[3]) for r in rows] == [(149.0, 110.0), (149.0, 125.0)]
    assert {r[5] for r in rows} == {batch_id_for(CSV.read_bytes())}
    assert len(rows[0][5]) == 12
    assert {r[6] for r in rows} == {"2026-09-30T02:56:07.812085"}


def test_batch_id_depends_on_the_bytes():
    assert batch_id_for(b"a") != batch_id_for(b"b")
    assert batch_id_for(b"a") == batch_id_for(b"a")


@pytest.mark.spark
def test_changed_then_unchanged(spark, tmp_path):
    registry = _registry()
    target, state = str(tmp_path / "daily"), tmp_path / "state.json"
    first = run(PACKAGE_URL, CSV_URL, target, state, get=registry, spark=spark, now=lambda: PULLED)
    assert first.startswith("REGISTRY changed fetched=1 rows=2 batch=")
    assert first.split("batch=")[1] == batch_id_for(CSV.read_bytes())
    calls_after_first = len(registry.calls)
    second = run(PACKAGE_URL, CSV_URL, target, state, get=registry, spark=spark, now=lambda: PULLED)
    assert second == "REGISTRY unchanged"
    assert registry.calls[calls_after_first:] == [PACKAGE_URL]
    frame = spark.read.format("delta").load(target)
    assert frame.count() == 2
    assert dict(frame.dtypes)["local_date"] == "string"


@pytest.mark.spark
def test_state_file_written(spark, tmp_path):
    state = tmp_path / "sub" / "state.json"
    stamp = read_registry_stamp(PACKAGE.read_bytes())
    run(
        str(PACKAGE),
        str(CSV),
        str(tmp_path / "daily"),
        state,
        spark=spark,
        now=lambda: PULLED,
    )
    assert json.loads(state.read_text(encoding="utf-8")) == {"metadata_modified": stamp}


@pytest.mark.spark
def test_new_stamp_with_same_file_skips_the_batch(spark, tmp_path):
    registry = _registry()
    target, state = str(tmp_path / "daily"), tmp_path / "state.json"
    run(PACKAGE_URL, CSV_URL, target, state, get=registry, spark=spark, now=lambda: PULLED)
    registry.answers[PACKAGE_URL] = _with_stamp("2026-10-01T00:00:00")
    again = run(PACKAGE_URL, CSV_URL, target, state, get=registry, spark=spark, now=lambda: PULLED)
    assert again.startswith("REGISTRY changed fetched=1 rows=0 batch=")
    frame = spark.read.format("delta").load(target)
    assert frame.count() == 2
    assert frame.select("batch_id").distinct().count() == 1
    assert json.loads(state.read_text(encoding="utf-8"))["metadata_modified"] == (
        "2026-10-01T00:00:00"
    )


@pytest.mark.spark
def test_changed_file_appends_a_second_batch(spark, tmp_path):
    registry = _registry()
    target, state = str(tmp_path / "daily"), tmp_path / "state.json"
    run(PACKAGE_URL, CSV_URL, target, state, get=registry, spark=spark, now=lambda: PULLED)
    registry.answers[PACKAGE_URL] = _with_stamp("2026-10-01T00:00:00")
    registry.answers[CSV_URL] = CSV.read_bytes() + b"2026-09-01,150,120\n"
    run(PACKAGE_URL, CSV_URL, target, state, get=registry, spark=spark, now=lambda: PULLED)
    frame = spark.read.format("delta").load(target)
    assert frame.count() == 5
    assert frame.select("batch_id").distinct().count() == 2


@pytest.mark.spark
def test_cli_prints_the_registry_lines(tmp_path, capsys):
    argv = [
        "--package",
        str(PACKAGE),
        "--csv",
        str(CSV),
        "--target",
        str(tmp_path / "daily"),
        "--state",
        str(tmp_path / "state.json"),
    ]
    assert daily_main(argv) == 0
    assert daily_main(argv) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("REGISTRY changed fetched=1 rows=2 batch=")
    assert lines[1] == "REGISTRY unchanged"
