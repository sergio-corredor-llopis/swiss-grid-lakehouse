# Databricks notebook source
# MAGIC %md
# MAGIC # Gold marts: daily load, control, hourly profile and data quality
# MAGIC
# MAGIC Reads the Silver table, the daily Bronze table (when it exists) and the rejected Silver
# MAGIC rows, builds the four marts with the `build` function of each mart module (PySpark over
# MAGIC Spark SQL), runs the Gold gate and merges each mart into a Delta table with `merge_mart`.
# MAGIC These are the same functions as the command line entry point
# MAGIC `python -m swiss_grid_lakehouse.gold`, run on the notebook's built-in `spark` session.
# MAGIC
# MAGIC This notebook is identical on the dev target (Databricks Free Edition) and on the azure
# MAGIC target; only the job's `depends_on` differs. Free Edition has no daily Bronze table, so
# MAGIC the control mart `g2` is skipped there and the gate prints `checks=4 skipped=1`.
# MAGIC
# MAGIC Widgets: `catalog` and `schema` name where Silver lives and where the marts are written.
# MAGIC If the gate fails the summary is printed and the task raises: nothing is merged. After a
# MAGIC pass the last cell runs `OPTIMIZE ... ZORDER BY` on the largest mart (`g3`) and reads
# MAGIC version 0 of `g1` back with `VERSION AS OF`. Run the notebook twice: the second run
# MAGIC prints `inserted=0 updated=0` for every mart.

# COMMAND ----------

# MAGIC %pip install --quiet ..

# COMMAND ----------

from swiss_grid_lakehouse.gold import daily_control, daily_load, data_quality, hourly_profile
from swiss_grid_lakehouse.gold.gate import run_gold_gate
from swiss_grid_lakehouse.gold.maintain import (
    optimize,
    optimize_line,
    version_line,
    version_query,
)
from swiss_grid_lakehouse.gold.merge import mart_line, merge_mart
from swiss_grid_lakehouse.report_lines import ReportLines

SILVER = "ch_load_silver"
REJECTED = "ch_load_silver_rejected"
DAILY = "ch_consumption_daily_bronze"
TABLES = {
    "g1": "gold_daily_load",
    "g2": "gold_daily_control",
    "g3": "gold_hourly_profile",
    "g4": "gold_data_quality",
}
ZORDER_BY = ["source", "weekday"]
out = ReportLines()

# COMMAND ----------

try:
    environment_version = spark.conf.get("spark.databricks.environment.version")  # noqa: F821
except Exception:
    environment_version = "not recorded"
out.print(f"RUN spark_version={spark.version} environment_version={environment_version}")  # noqa: F821

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")  # noqa: F821
dbutils.widgets.text("schema", "swiss_grid", "Schema")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
prefix = f"{catalog}.{schema}"


def read_table(name):
    return spark.sql(f"SELECT * FROM {prefix}.{name}")  # noqa: F821


def read_optional(name):
    """The table as a DataFrame, or None when it does not exist (daily Bronze on Free Edition)."""
    if spark.catalog.tableExists(f"{prefix}.{name}"):  # noqa: F821
        return read_table(name)
    return None


silver = read_table(SILVER)
daily = read_optional(DAILY)
rejected = read_optional(REJECTED)

# COMMAND ----------

marts = {
    "g1": daily_load.build(spark, silver),  # noqa: F821
    "g2": daily_control.build(spark, silver, daily),  # noqa: F821
    "g3": hourly_profile.build(spark, silver),  # noqa: F821
    "g4": data_quality.build(spark, silver, rejected),  # noqa: F821
}
keys = {
    "g1": (daily_load.KEY, daily_load.CHANGE_COLS),
    "g2": (daily_control.KEY, daily_control.CHANGE_COLS),
    "g3": (hourly_profile.KEY, hourly_profile.CHANGE_COLS),
    "g4": (data_quality.KEY, data_quality.CHANGE_COLS),
}
if marts["g2"] is None:
    out.print("SKIPPED gold_daily_control reason=no_daily_bronze")

result = run_gold_gate(silver, marts)
out.print(result.summary())
figures = {
    "gate": result.summary().removeprefix("GOLD GATE:").strip(),
    "gold_inserted": None,
    "gold_updated": None,
}
try:
    if not result.passed:
        raise RuntimeError(f"Gold gate failed, nothing merged: {result.summary()}")
    gold_inserted = gold_updated = 0
    for short, df in marts.items():
        if df is None:
            continue
        key, change_cols = keys[short]
        target = f"{prefix}.{TABLES[short]}"
        inserted, updated = merge_mart(spark, df, target, key, change_cols)  # noqa: F821
        gold_inserted += inserted
        gold_updated += updated
        out.print(mart_line(short, read_table(TABLES[short]).count(), inserted, updated))
    figures.update(gold_inserted=gold_inserted, gold_updated=gold_updated)
finally:
    dbutils.jobs.taskValues.set(key="figures", value=figures)  # noqa: F821

# COMMAND ----------

out.print(optimize_line("g3", optimize(spark, f"{prefix}.{TABLES['g3']}", ZORDER_BY)))  # noqa: F821
g1_target = f"{prefix}.{TABLES['g1']}"
then_rows = version_query(spark, g1_target, 0).count()  # noqa: F821
out.print(version_line("g1", 0, then_rows, read_table(TABLES["g1"]).count()))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {prefix}.{TABLES['g1']}"))  # noqa: F821

# COMMAND ----------

out.exit(globals().get("dbutils"))
