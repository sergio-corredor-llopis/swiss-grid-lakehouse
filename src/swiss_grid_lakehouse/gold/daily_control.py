"""Mart g2, `gold_daily_control`: the pipeline's daily total against the published figure.

    build(spark, silver, daily) -> DataFrame | None

`daily` is the daily Bronze table (the national consumption the Swiss Federal Office of Energy
publishes from Swissgrid data) or None. The mart has one row per `source`, `series` and
`local_date`, for the three comparisons listed in `reconcile.SERIES`: Swissgrid total, Swissgrid
end-user and ENTSO-E total. The rules are those of `reconcile.reconcile`, which is the reference
for the numbers:

- The latest batch of a day wins: the greatest `registry_modified`, ties broken by `batch_id`.
- A day is compared only when the pipeline and the published table both have it.
- `INCOMPLETE`: the day has fewer hours than the calendar day, or the figure is missing. The
  partial figure is shown and never scaled.
- `PRELIMINARY`: the figure is at most `PRELIM_DAYS` old at the registry stamp of its batch.
- Otherwise `PASS` when `abs(diff_pct)` is within the tolerance, else `FAIL`. The tolerance is
  `WIDE_PCT` for ENTSO-E and `TIGHT_PCT` for the two Swissgrid series.

The SQL is generated from the constants of `reconcile`, so the two cannot drift apart.
"""

from __future__ import annotations

from typing import Any

from swiss_grid_lakehouse.gold import daily_load
from swiss_grid_lakehouse.reconcile import PRELIM_DAYS, SERIES, TIGHT_PCT, WIDE_PCT

KEY = ["source", "series", "local_date"]
CHANGE_COLS = [
    "pipeline_gwh",
    "published_gwh",
    "diff_pct",
    "hours",
    "status",
    "registry_modified",
]

DAILY_VIEW = "gold_daily_control_daily"
ROLLUP_VIEW = "gold_daily_control_rollup"
LATEST_VIEW = "gold_daily_control_latest"

_LATEST_SQL = f"""
SELECT local_date, national_gwh, final_gwh, registry_modified, batch_id
FROM (
  SELECT
    CAST(local_date AS DATE) AS local_date,
    national_gwh,
    final_gwh,
    registry_modified,
    batch_id,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(local_date AS DATE)
      ORDER BY registry_modified DESC, COALESCE(batch_id, '') DESC
    ) AS rank_in_day
  FROM {DAILY_VIEW}
)
WHERE rank_in_day = 1
"""


def _series_sql(source: str, series: str, rollup_col: str, daily_col: str, kind: str) -> str:
    tol = TIGHT_PCT if kind == "tight" else WIDE_PCT
    return f"""
SELECT
  '{source}' AS source,
  '{series}' AS series,
  r.local_date AS local_date,
  r.{rollup_col} AS pipeline_gwh,
  CAST(p.{daily_col} AS DOUBLE) AS published_gwh,
  r.hours AS hours,
  r.incomplete AS incomplete,
  p.registry_modified AS registry_modified,
  datediff(to_date(substring(p.registry_modified, 1, 10)), r.local_date) - 1 AS age_days,
  CAST({tol} AS DOUBLE) AS tolerance
FROM {ROLLUP_VIEW} r
JOIN {LATEST_VIEW} p ON p.local_date = r.local_date
WHERE r.source = '{source}' AND p.{daily_col} IS NOT NULL
"""


def _compare_sql() -> str:
    parts = [_series_sql(s, n, key, col, kind) for s, n, _pub, key, col, kind in SERIES]
    union = "\nUNION ALL\n".join(parts)
    return f"""
WITH compared AS ({union}),
with_diff AS (
  SELECT
    *,
    CASE
      WHEN pipeline_gwh IS NULL THEN NULL
      WHEN published_gwh = 0 THEN
        CASE WHEN pipeline_gwh = 0 THEN 0.0 ELSE double('Infinity') END
      ELSE 100.0 * (pipeline_gwh - published_gwh) / published_gwh
    END AS diff_pct
  FROM compared
)
SELECT
  source,
  series,
  local_date,
  pipeline_gwh,
  published_gwh,
  diff_pct,
  hours,
  CASE
    WHEN incomplete OR pipeline_gwh IS NULL THEN 'INCOMPLETE'
    WHEN age_days <= {PRELIM_DAYS} THEN 'PRELIMINARY'
    WHEN abs(diff_pct) <= tolerance THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  registry_modified
FROM with_diff
"""


def build(spark: Any, silver: Any, daily: Any) -> Any | None:
    """Return the g2 mart, or None when `daily` is None or has no rows."""
    if daily is None or daily.limit(1).count() == 0:
        return None
    daily.createOrReplaceTempView(DAILY_VIEW)
    spark.sql(_LATEST_SQL).createOrReplaceTempView(LATEST_VIEW)
    daily_load.build(spark, silver).createOrReplaceTempView(ROLLUP_VIEW)
    return spark.sql(_compare_sql())
