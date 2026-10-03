"""Gold marts: daily and hourly views of the Silver Swiss load table, built with Spark SQL.

Four marts, each a Delta table:

- `gold_daily_load` (g1): GWh per Swiss local day and source, with 23 and 25 hour days flagged.
- `gold_daily_control` (g2): the pipeline's daily total against the published daily figure.
- `gold_hourly_profile` (g3): typical MW by weekday and local hour.
- `gold_data_quality` (g4): which days are short, rejected rows and re-pulled days.

Submodules: `merge` (Delta MERGE that updates only when a change column differs),
`maintain` (OPTIMIZE with ZORDER BY and VERSION AS OF time travel) and `gate` (the checks that
run before anything is merged). Nothing here imports Spark at package import time.
"""
