# Databricks notebook source
# MAGIC %md
# MAGIC # Daily control totals: published national consumption against Silver
# MAGIC
# MAGIC Checks the registry of the Swiss Federal Office of Energy for a new batch of the
# MAGIC published daily consumption, appends it to the Bronze Delta table
# MAGIC `ch_consumption_daily_bronze`, then rolls both Silver sources up to local days and
# MAGIC compares them with the published figures. It calls the same `run` and `reconcile`
# MAGIC functions as the command line entry points `python -m swiss_grid_lakehouse.daily` and
# MAGIC `python -m swiss_grid_lakehouse.reconcile`, using the notebook's built-in `spark`
# MAGIC session. With the default widgets the only network calls are the registry package URL
# MAGIC and the CSV URL.
# MAGIC
# MAGIC Widgets: `catalog` and `schema` name where the Bronze daily table and the Silver
# MAGIC table live. `state` is the path of a small file that keeps the last registry
# MAGIC `metadata_modified` (a Unity Catalog Volume path keeps it between runs). `package` and
# MAGIC `csv` are the registry package answer and the published CSV; each is a URL or a file
# MAGIC path, and the defaults are the live URLs. The last cell prints the `DAY` lines and
# MAGIC three `RECONCILE` lines. A run fails when a final day is
# MAGIC beyond tolerance; a preliminary day is printed and counted but never fails it.

# COMMAND ----------

# MAGIC %pip install --quiet ..

# COMMAND ----------

from pathlib import Path

from swiss_grid_lakehouse.daily.ogd_client import run
from swiss_grid_lakehouse.reconcile import exit_code, reconcile
from swiss_grid_lakehouse.report_lines import ReportLines
from swiss_grid_lakehouse.silver.daily_rollup import SOURCES, rollup_daily

DAILY = "ch_consumption_daily_bronze"
SILVER = "ch_load_silver"
PACKAGE_URL = (
    "https://ckan.opendata.swiss/api/3/action/package_show"
    "?id=energiedashboard-ch-landesverbrauch-und-endverbrauch"
)
CSV_URL = "https://www.bfe-ogd.ch/ogd103_stromverbrauch_swissgrid_lv_und_endv.csv"
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
STATE_DEFAULT = "/Volumes/workspace/swiss_grid/raw/state/ogd_registry.json"
dbutils.widgets.text("state", STATE_DEFAULT, "Registry state file")  # noqa: F821
dbutils.widgets.text("package", PACKAGE_URL, "Registry package URL or file")  # noqa: F821
dbutils.widgets.text("csv", CSV_URL, "Published CSV URL or file")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
state = dbutils.widgets.get("state")  # noqa: F821
package = dbutils.widgets.get("package")  # noqa: F821
csv = dbutils.widgets.get("csv")  # noqa: F821
daily_table = f"{catalog}.{schema}.{DAILY}"
silver_table = f"{catalog}.{schema}.{SILVER}"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")  # noqa: F821
out.print(run(package, csv, daily_table, Path(state), spark=spark))  # noqa: F821

# COMMAND ----------

silver = spark.table(silver_table)  # noqa: F821
if "load_enduser_mw" not in silver.columns:
    from pyspark.sql import functions as F

    silver = silver.withColumn("load_enduser_mw", F.lit(None).cast("double"))
rollups = {source: rollup_daily(silver, source) for source in SOURCES}
daily_cols = ["local_date", "national_gwh", "final_gwh", "registry_modified", "batch_id"]
daily_rows = [r.asDict() for r in spark.table(daily_table).select(*daily_cols).collect()]  # noqa: F821

results = reconcile(rollups, daily_rows)
for result in results:
    out.print("\n".join(result.lines()))
if exit_code(results) != 0:
    raise RuntimeError("RECONCILE failed: a final day is beyond tolerance, or no overlap")

# COMMAND ----------

out.exit(globals().get("dbutils"))
