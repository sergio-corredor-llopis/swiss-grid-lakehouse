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
