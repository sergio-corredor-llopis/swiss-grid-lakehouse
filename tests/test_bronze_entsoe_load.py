from datetime import UTC, datetime
from pathlib import Path

import pytest

from swiss_grid_lakehouse.bronze import to_bronze_rows, write_bronze

FIXTURE = Path(__file__).parent / "fixtures" / "entsoe_ch_load_sample.xml"
PULLED = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


def _rows():
    return to_bronze_rows(FIXTURE.read_text(encoding="utf-8"), FIXTURE.name, PULLED)


def test_rows_shape():
    rows = _rows()
    assert len(rows) == 48
    assert {r[0] for r in rows} == {"entsoe"}
    assert {r[1] for r in rows} == {"CH"}
    assert len({r[3] for r in rows}) == 1
    assert rows[0][4] == "entsoe_ch_load_sample.xml"


def test_rows_reject_non_load_xml():
    with pytest.raises(ValueError):
        to_bronze_rows("<html/>", "x.xml", PULLED)


@pytest.mark.spark
def test_write_is_append_only_and_idempotent(spark, tmp_path):
    target = str(tmp_path / "bronze")
    rows = _rows()
    assert write_bronze(spark, rows, target) is True
    assert spark.read.format("delta").load(target).count() == 48
    assert write_bronze(spark, rows, target) is False
    frame = spark.read.format("delta").load(target)
    assert frame.count() == 48
    assert frame.select("batch_id").distinct().count() == 1
