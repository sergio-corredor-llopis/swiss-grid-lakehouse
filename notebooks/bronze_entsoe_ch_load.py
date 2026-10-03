# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze load: ENTSO-E Swiss actual load
# MAGIC
# MAGIC Loads ENTSO-E Transparency Platform actual-load data (Switzerland) into an append-only
# MAGIC managed Delta table. It calls the same `write_bronze` as the command line entry point
# MAGIC `python -m swiss_grid_lakehouse.bronze`, using the notebook's built-in `spark` session.
# MAGIC
# MAGIC Widgets: `source` is either the path of a recorded XML file in a Unity Catalog Volume or
# MAGIC the word `api`, which fetches the last two days from the ENTSO-E RESTful API with the
# MAGIC token stored in the Databricks secret scope `entsoe`, key `token`. `catalog` and
# MAGIC `schema` name the target.

# COMMAND ----------

# MAGIC %pip install --quiet ..

# COMMAND ----------

import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from swiss_grid_lakehouse.bronze import to_bronze_rows, write_bronze
from swiss_grid_lakehouse.report_lines import ReportLines

TABLE = "entsoe_ch_load_bronze"
CH_AREA = "10YCH-SWISSGRIDZ"
out = ReportLines()

# COMMAND ----------

try:
    environment_version = spark.conf.get("spark.databricks.environment.version")  # noqa: F821
except Exception:
    environment_version = "not recorded"
out.print(f"RUN spark_version={spark.version} environment_version={environment_version}")  # noqa: F821

# COMMAND ----------

dbutils.widgets.text("source", "", "XML file (Volume path) or api")  # noqa: F821
dbutils.widgets.text("catalog", "workspace", "Target catalog")  # noqa: F821
dbutils.widgets.text("schema", "swiss_grid", "Target schema")  # noqa: F821

source = dbutils.widgets.get("source")  # noqa: F821
catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
if not source:
    raise ValueError("set the `source` widget to a Volume path of an XML file, or to `api`")

# COMMAND ----------

if source == "api":
    import pandas as pd
    from entsoe import EntsoeRawClient

    token = dbutils.secrets.get("entsoe", "token")  # noqa: F821
    end = pd.Timestamp.now(tz="Europe/Zurich").normalize()
    start = end - pd.Timedelta(days=2)
    xml_text = EntsoeRawClient(api_key=token).query_load(CH_AREA, start=start, end=end)
    source_name = f"entsoe_api_{start:%Y%m%d}_{end:%Y%m%d}.xml"
else:
    # Copy the file from the Volume to local disk, then read it as text.
    local_xml = Path(tempfile.mkdtemp()) / Path(source).name
    shutil.copy(source, local_xml)
    xml_text = local_xml.read_text(encoding="utf-8")
    source_name = local_xml.name

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")  # noqa: F821
target = f"{catalog}.{schema}.{TABLE}"

rows = to_bronze_rows(xml_text, source_name, datetime.now(UTC))
written = write_bronze(spark, rows, target)  # noqa: F821
out.print(f"{'wrote' if written else 'skipped'} {len(rows)} rows to {target} batch_id={rows[0][3]}")
dbutils.jobs.taskValues.set(key="figures", value={"bronze_rows": len(rows)})  # noqa: F821

# COMMAND ----------

display(spark.table(target).limit(10))  # noqa: F821

# COMMAND ----------

out.exit(globals().get("dbutils"))
