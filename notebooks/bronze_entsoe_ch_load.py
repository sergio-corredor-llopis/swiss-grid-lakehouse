# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze load: ENTSO-E Swiss actual load
# MAGIC
# MAGIC Loads one recorded ENTSO-E Transparency Platform actual-load XML file (Switzerland) from a
# MAGIC Unity Catalog Volume into an append-only managed Delta table. It calls the same
# MAGIC `write_bronze` as the command line entry point `python -m swiss_grid_lakehouse.bronze`.
# MAGIC
# MAGIC Widgets: `source` is the XML file in a Volume, `catalog` and `schema` name the target.

# COMMAND ----------

# MAGIC %pip install --quiet ..

# COMMAND ----------

import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from swiss_grid_lakehouse.bronze import to_bronze_rows, write_bronze
from swiss_grid_lakehouse.spark import get_spark

TABLE = "entsoe_ch_load_bronze"

# COMMAND ----------

dbutils.widgets.text("source", "", "XML file (Volume path)")  # noqa: F821
dbutils.widgets.text("catalog", "main", "Target catalog")  # noqa: F821
dbutils.widgets.text("schema", "swiss_grid", "Target schema")  # noqa: F821

source = dbutils.widgets.get("source")  # noqa: F821
catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
if not source:
    raise ValueError("set the `source` widget to the path of an ENTSO-E load XML file")

# COMMAND ----------

# Copy the file from the Volume to local disk, then read it as text.
local_dir = Path(tempfile.mkdtemp())
local_xml = local_dir / Path(source).name
shutil.copy(source, local_xml)
xml_text = local_xml.read_text(encoding="utf-8")

# COMMAND ----------

spark = get_spark("databricks")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
target = f"{catalog}.{schema}.{TABLE}"

rows = to_bronze_rows(xml_text, local_xml.name, datetime.now(UTC))
written = write_bronze(spark, rows, target)
print(f"{'wrote' if written else 'skipped'} {len(rows)} rows to {target} batch_id={rows[0][3]}")

# COMMAND ----------

display(spark.table(target).limit(10))  # noqa: F821
