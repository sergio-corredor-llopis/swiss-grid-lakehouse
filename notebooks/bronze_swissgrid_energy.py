# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze load: Swissgrid energy workbook
# MAGIC
# MAGIC Loads the Swissgrid aggregated energy workbook (sheet `Zeitreihen0h15`, 15-minute slots)
# MAGIC into an append-only managed Delta table. It calls the same `to_bronze_rows` and
# MAGIC `write_bronze` as the command line entry point `python -m swiss_grid_lakehouse.bronze
# MAGIC --swissgrid`, using the notebook's built-in `spark` session. The reader finds the two
# MAGIC series it needs by their header, so the full 64-series workbook loads as published.
# MAGIC
# MAGIC Widgets: `source` is the path of the workbook (`.xlsx`) or of a CSV extract with the
# MAGIC same layout in a Unity Catalog Volume, for example
# MAGIC `/Volumes/workspace/swiss_grid/raw/swissgrid/<file>.xlsx`. `catalog` and `schema` name the
# MAGIC target. Loading the same file twice is detected by its hash: the second run appends
# MAGIC nothing.

# COMMAND ----------

# MAGIC %pip install openpyxl==3.1.5

# COMMAND ----------

# MAGIC %pip install --quiet ..

# COMMAND ----------

import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from swiss_grid_lakehouse.bronze.swissgrid_energy_bronze import (
    COLUMNS,
    to_bronze_rows,
    write_bronze,
)
from swiss_grid_lakehouse.report_lines import ReportLines

TABLE = "swissgrid_energy_bronze"
out = ReportLines()

# COMMAND ----------

try:
    environment_version = spark.conf.get("spark.databricks.environment.version")  # noqa: F821
except Exception:
    environment_version = "not recorded"
out.print(f"RUN spark_version={spark.version} environment_version={environment_version}")  # noqa: F821

# COMMAND ----------

dbutils.widgets.text("source", "", "Workbook or CSV (Volume path)")  # noqa: F821
dbutils.widgets.text("catalog", "workspace", "Target catalog")  # noqa: F821
dbutils.widgets.text("schema", "swiss_grid", "Target schema")  # noqa: F821

source = dbutils.widgets.get("source")  # noqa: F821
catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
if not source:
    raise ValueError("set the `source` widget to the Volume path of the workbook or CSV")

# COMMAND ----------

# Copy the file from the Volume to local disk; the reader opens it by path.
local_file = Path(tempfile.mkdtemp()) / Path(source).name
shutil.copy(source, local_file)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")  # noqa: F821
target = f"{catalog}.{schema}.{TABLE}"

rows = to_bronze_rows(local_file, datetime.now(UTC))
written = write_bronze(spark, rows, target)  # noqa: F821
batch_id = rows[0][COLUMNS.index("batch_id")]
out.print(f"{'wrote' if written else 'skipped'} {len(rows)} rows to {target} batch_id={batch_id}")
dbutils.jobs.taskValues.set(key="figures", value={"swissgrid_rows": len(rows)})  # noqa: F821

# COMMAND ----------

display(spark.table(target).limit(10))  # noqa: F821

# COMMAND ----------

out.exit(globals().get("dbutils"))
