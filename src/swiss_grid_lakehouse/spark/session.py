"""Build a SparkSession for one of two runtimes: local or databricks.

Spark is an optional dependency (`pip install .[spark]`); nothing here is imported by the
core package. The local runtime needs a JVM on the machine. The databricks runtime uses
Databricks Connect and reads its workspace settings from the usual Databricks environment
variables or configuration profile.
"""

from __future__ import annotations

import os
from typing import Any

RUNTIMES = ("local", "databricks")
RUNTIME_ENV = "SGL_RUNTIME"
IVY_ENV = "SGL_IVY_DIR"


def _resolve(runtime: str | None) -> str:
    chosen = (runtime or os.environ.get(RUNTIME_ENV) or "local").strip().lower()
    if chosen not in RUNTIMES:
        raise ValueError(f"unknown runtime {chosen!r}; expected one of {RUNTIMES}")
    return chosen


def _local() -> Any:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("swiss-grid-lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    ivy_dir = os.environ.get(IVY_ENV)
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _databricks() -> Any:
    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        raise RuntimeError(
            "the databricks runtime needs databricks-connect (pip install databricks-connect)"
        ) from exc
    return DatabricksSession.builder.getOrCreate()


def get_spark(runtime: str | None = None) -> Any:
    """Return a SparkSession.

    runtime: "local" or "databricks"; when None, the SGL_RUNTIME environment variable is
    used, and "local" if that is unset.
    """
    return _databricks() if _resolve(runtime) == "databricks" else _local()
