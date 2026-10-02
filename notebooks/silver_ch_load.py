# Databricks notebook source
# MAGIC %md
# MAGIC # Silver load: Swiss load from ENTSO-E and Swissgrid
# MAGIC
# MAGIC Reads the Bronze Delta table of each source, builds Silver with `to_silver` (for
# MAGIC Swissgrid after `to_hourly`, which sums the 15-minute slots to hours), runs the quality
# MAGIC gate and merges into the shared Silver Delta table with `merge_silver`. These are the
# MAGIC same functions as the command line entry point `python -m swiss_grid_lakehouse.silver`,
# MAGIC run on the notebook's built-in `spark` session. A batch that fails the gate is not
# MAGIC merged; its bad rows are appended to the `_rejected` table.
# MAGIC
# MAGIC Widgets: `sources` is a comma-separated list of `entsoe` and `swissgrid` (default both);
# MAGIC `catalog` and `schema` name where the Bronze tables live and where Silver is written.
# MAGIC Run the notebook twice: the second run merges nothing new. When both sources are loaded,
# MAGIC the last cell prints the `HOURLY` lines and the `COMPARE` line of
# MAGIC `python -m swiss_grid_lakehouse.compare`.

# COMMAND ----------

# MAGIC %pip install --quiet ..

# COMMAND ----------

from datetime import UTC, datetime

from swiss_grid_lakehouse.compare.__main__ import build_report
from swiss_grid_lakehouse.silver.__main__ import _current_count, _write_rejected
from swiss_grid_lakehouse.silver.ch_load_silver import merge_silver, to_silver
from swiss_grid_lakehouse.silver.quality import run_gate
from swiss_grid_lakehouse.silver.swissgrid_hourly import to_hourly

BRONZE = {"entsoe": "entsoe_ch_load_bronze", "swissgrid": "swissgrid_energy_bronze"}
SILVER = "ch_load_silver"
CHECKS = 6  # row checks (4) + missing hours + row count

# COMMAND ----------

try:
    environment_version = spark.conf.get("spark.databricks.environment.version")  # noqa: F821
except Exception:
    environment_version = "not recorded"
print(f"RUN spark_version={spark.version} environment_version={environment_version}")  # noqa: F821

# COMMAND ----------

dbutils.widgets.text("sources", "entsoe,swissgrid", "Sources (comma-separated)")  # noqa: F821
dbutils.widgets.text("catalog", "workspace", "Catalog")  # noqa: F821
dbutils.widgets.text("schema", "swiss_grid", "Schema")  # noqa: F821

sources = [x.strip() for x in dbutils.widgets.get("sources").split(",") if x.strip()]  # noqa: F821
unknown = [x for x in sources if x not in BRONZE]
if unknown or not sources:
    raise ValueError(f"`sources` must list entsoe and/or swissgrid, got {unknown or 'nothing'}")
catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
silver_table = f"{catalog}.{schema}.{SILVER}"

# COMMAND ----------

for name in sources:
    bronze = spark.table(f"{catalog}.{schema}.{BRONZE[name]}")  # noqa: F821
    if name == "swissgrid":
        silver = to_silver(to_hourly(bronze), value_col="energy_kwh")
    else:
        silver = to_silver(bronze)
    result = run_gate(silver, _current_count(spark, silver_table, name))  # noqa: F821
    if not result.passed:
        print(f"{name}: {result.summary()}")
        _write_rejected(result.rejected_df, silver_table)
        raise RuntimeError(f"quality gate failed for {name}, nothing merged: {result.summary()}")
    print(f"{name}: {result.summary()} checks={CHECKS}")
    inserted, updated = merge_silver(spark, silver, silver_table)  # noqa: F821
    rows = _current_count(spark, silver_table, name)  # noqa: F821
    print(f"{name}: merged inserted={inserted} updated={updated} rows={rows}")

# COMMAND ----------

if {"entsoe", "swissgrid"} <= set(sources):
    table = spark.table(silver_table)  # noqa: F821
    cols = ["source", "area", table["ts_utc"].cast("long").alias("epoch"), "load_mw"]
    if "load_enduser_mw" in table.columns:
        cols.append("load_enduser_mw")
    pairs = []
    for r in table.filter(table["source"].isin("entsoe", "swissgrid")).select(*cols).collect():
        d = r.asDict()
        d["ts_utc"] = datetime.fromtimestamp(d.pop("epoch"), UTC)
        pairs.append(d)
    report = build_report(pairs)
    for line in report.lines():
        if line.startswith(("HOURLY", "COMPARE")):
            print(line)
else:
    print("COMPARE skipped: `sources` must contain both entsoe and swissgrid")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver_table}"))  # noqa: F821
