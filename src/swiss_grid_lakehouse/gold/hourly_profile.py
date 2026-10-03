"""Mart g3: the typical load by weekday and local hour, built with Spark SQL.

One row per source, weekday (1 = Monday .. 7 = Sunday) and local hour (0..23, read from
`ts_local`). On the day the clocks go back the hour 02:00 occurs twice; both readings count
towards hour_local 2 of that day, so `n_days` stays one for the day while the average, minimum
and maximum cover both. `build` takes a Spark DataFrame of Silver rows and never reads a table.
Spark is imported lazily.
"""

from __future__ import annotations

from typing import Any

KEY = ["source", "weekday", "hour_local"]
CHANGE_COLS = ["n_days", "avg_mw", "min_mw", "max_mw"]

_SQL = """
SELECT
    source,
    CAST(weekday(CAST(ts_local AS DATE)) + 1 AS INT) AS weekday,
    CAST(hour(ts_local) AS INT) AS hour_local,
    CAST(COUNT(DISTINCT CAST(ts_local AS DATE)) AS INT) AS n_days,
    CAST(AVG(load_mw) AS DOUBLE) AS avg_mw,
    CAST(MIN(load_mw) AS DOUBLE) AS min_mw,
    CAST(MAX(load_mw) AS DOUBLE) AS max_mw
FROM gold_hourly_profile_silver
GROUP BY source, weekday(CAST(ts_local AS DATE)), hour(ts_local)
"""


def build(spark: Any, silver: Any) -> Any:
    """The hourly profile of `silver` (all sources) as a DataFrame with the g3 columns."""
    silver.createOrReplaceTempView("gold_hourly_profile_silver")
    return spark.sql(_SQL)
