"""Mart g1, `gold_daily_load`: GWh per Swiss local day and source, built with Spark SQL.

    build(spark, silver) -> DataFrame

`silver` is a frame of hourly Silver rows. The mart has one row per `source` and `local_date`
and follows the rules of `silver.daily_rollup.rollup_rows`, which is the reference for the
numbers:

- `hours`: hourly rows of the day with a load value.
- `expected_hours`: the length of that day in Europe/Zurich, 23, 24 or 25. The repeated hour
  of the clock change is its own hour, because rows are told apart by `ts_utc`.
- `incomplete`: `hours < expected_hours`. A short day is reported with its partial sum and is
  never scaled up.
- `day_kind`: `short` (23 h), `normal` (24 h) or `long` (25 h).
- `gwh_total`: the sum of `load_mw` over the day divided by 1000.
- `gwh_enduser`: the same for `load_enduser_mw`, Swissgrid only. NULL when any hour of the day
  has no end-user value, so a partial sum is never shown.
- `gwh_difference`: the sum of `load_difference_mw` divided by 1000. NULL when the day is
  incomplete or any hour has no value. The column holds grid losses, the power plants' own use
  and pumps together; it is not the pumped-storage figure alone.
"""

from __future__ import annotations

from typing import Any

KEY = ["source", "local_date"]
CHANGE_COLS = [
    "hours",
    "expected_hours",
    "incomplete",
    "day_kind",
    "gwh_total",
    "gwh_enduser",
    "gwh_difference",
]

VIEW = "gold_daily_load_silver"

# The day length is the gap between two local midnights, measured in UTC. `convert_timezone`
# works on a timestamp without time zone, so the result does not depend on the session time zone.
_SQL = f"""
WITH day_sums AS (
  SELECT
    source,
    local_date,
    CAST(COUNT(load_mw) AS INT) AS hours,
    SUM(load_mw) AS mw_sum,
    COUNT(load_enduser_mw) AS enduser_hours,
    SUM(load_enduser_mw) AS enduser_sum,
    COUNT(load_difference_mw) AS difference_hours,
    SUM(load_difference_mw) AS difference_sum
  FROM {VIEW}
  WHERE load_mw IS NOT NULL
  GROUP BY source, local_date
),
with_length AS (
  SELECT
    *,
    CAST(timestampdiff(
      HOUR,
      convert_timezone('Europe/Zurich', 'UTC', CAST(local_date AS TIMESTAMP_NTZ)),
      convert_timezone('Europe/Zurich', 'UTC', CAST(date_add(local_date, 1) AS TIMESTAMP_NTZ))
    ) AS INT) AS expected_hours
  FROM day_sums
)
SELECT
  source,
  local_date,
  hours,
  expected_hours,
  hours < expected_hours AS incomplete,
  CASE expected_hours WHEN 23 THEN 'short' WHEN 25 THEN 'long' ELSE 'normal' END AS day_kind,
  CAST(mw_sum / 1000 AS DOUBLE) AS gwh_total,
  CAST(
    CASE WHEN source = 'swissgrid' AND enduser_hours = hours THEN enduser_sum / 1000 END AS DOUBLE
  ) AS gwh_enduser,
  CAST(
    CASE WHEN hours >= expected_hours AND difference_hours = hours
         THEN difference_sum / 1000 END AS DOUBLE
  ) AS gwh_difference
FROM with_length
"""


def build(spark: Any, silver: Any) -> Any:
    """Return the g1 mart for the Silver frame `silver`, computed with `spark.sql`."""
    silver.createOrReplaceTempView(VIEW)
    return spark.sql(_SQL)
