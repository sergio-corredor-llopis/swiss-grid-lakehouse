"""Shared fixtures. The local Spark session is built once per test run."""

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from swiss_grid_lakehouse.spark import get_spark

    session = get_spark("local")
    yield session
    session.stop()


@pytest.fixture
def bronze_path(spark, tmp_path):
    """A Bronze Delta table built from the recorded ENTSO-E load response (48 hourly rows)."""
    from datetime import UTC, datetime
    from pathlib import Path

    from swiss_grid_lakehouse.bronze import to_bronze_rows, write_bronze

    xml = Path(__file__).parent / "fixtures" / "entsoe_ch_load_2026-08-30.xml"
    rows = to_bronze_rows(
        xml.read_text(encoding="utf-8"), xml.name, datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    )
    path = str(tmp_path / "bronze")
    write_bronze(spark, rows, path)
    return path
