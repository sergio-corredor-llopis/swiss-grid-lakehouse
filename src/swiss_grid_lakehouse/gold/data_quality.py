"""Mart g4: which days are short, re-pulled or hold rejected rows, built with Spark SQL.

One row per source and local day. `hours` counts the Silver hours of the day, `expected_hours`
is the length of the Europe/Zurich day (23, 24 or 25), `missing_hours` is the shortfall (never
negative) and `incomplete` flags a short day. `batches` counts the distinct batch ids of the
day and `repulled` is true when there is more than one. `rejected_rows` counts the rows of the
optional rejected table for that source and day; it is 0 when no such table is given.
`build` takes Spark DataFrames and never reads a table. Spark is imported lazily.
"""

from __future__ import annotations

from typing import Any

from swiss_grid_lakehouse.silver.daily_rollup import expected_hours

KEY = ["source", "local_date"]
CHANGE_COLS = [
    "hours",
    "expected_hours",
    "missing_hours",
    "incomplete",
    "batches",
    "repulled",
    "rejected_rows",
]

_SQL = """
WITH days AS (
    SELECT
        source,
        local_date,
        CAST(COUNT(load_mw) AS INT) AS hours,
        CAST(COUNT(DISTINCT batch_id) AS INT) AS batches
    FROM gold_data_quality_silver
    GROUP BY source, local_date
),
rejected_days AS (
    SELECT source, rejected_date AS local_date, CAST(COUNT(*) AS INT) AS rejected_rows
    FROM gold_data_quality_rejected
    GROUP BY source, rejected_date
)
SELECT
    d.source,
    d.local_date,
    d.hours,
    CAST(l.expected_hours AS INT) AS expected_hours,
    CAST(GREATEST(l.expected_hours - d.hours, 0) AS INT) AS missing_hours,
    d.hours < l.expected_hours AS incomplete,
    d.batches,
    d.batches > 1 AS repulled,
    CAST(COALESCE(r.rejected_rows, 0) AS INT) AS rejected_rows
FROM days d
LEFT JOIN gold_data_quality_day_length l ON d.local_date = l.local_date
LEFT JOIN rejected_days r ON d.source = r.source AND d.local_date = r.local_date
"""


def _day_length(spark: Any, silver: Any) -> Any:
    """A two column frame (local_date, expected_hours) for every day in `silver`."""
    days = [r["local_date"] for r in silver.select("local_date").distinct().collect()]
    rows = [(d, expected_hours(d)) for d in days if d is not None]
    return spark.createDataFrame(rows, schema="local_date date, expected_hours int")


def _rejected_days(spark: Any, rejected: Any) -> Any:
    """The rejected rows as (source, rejected_date); the day comes from local_date or ts_local."""
    from pyspark.sql import functions as f

    if "local_date" in rejected.columns:
        day = f.col("local_date").cast("date")
    else:
        day = f.col("ts_local").cast("date")
    return rejected.select(f.col("source"), day.alias("rejected_date"))


def build(spark: Any, silver: Any, rejected: Any = None) -> Any:
    """The data quality rows of `silver`; `rejected` is the rejected-rows frame or None."""
    if rejected is None:
        rejected_days = spark.createDataFrame([], schema="source string, rejected_date date")
    else:
        rejected_days = _rejected_days(spark, rejected)
    silver.createOrReplaceTempView("gold_data_quality_silver")
    rejected_days.createOrReplaceTempView("gold_data_quality_rejected")
    _day_length(spark, silver).createOrReplaceTempView("gold_data_quality_day_length")
    return spark.sql(_SQL)
