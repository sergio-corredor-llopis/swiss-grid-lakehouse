"""Spark session factory for the lakehouse (optional `spark` dependency group)."""

from swiss_grid_lakehouse.spark.session import RUNTIMES, get_spark

__all__ = ["RUNTIMES", "get_spark"]
