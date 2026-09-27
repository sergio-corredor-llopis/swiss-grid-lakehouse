# Databricks notebook source
# MAGIC %md
# MAGIC # Silver load: ENTSO-E Swiss actual load
# MAGIC
# MAGIC Reads the Bronze Delta table, builds Silver with `to_silver`, runs the quality gate and
# MAGIC merges into the Silver Delta table with `merge_silver`. These are the same functions as
# MAGIC the command line entry point `python -m swiss_grid_lakehouse.silver`, run on the
# MAGIC notebook's built-in `spark` session. A batch that fails the gate is not merged; its bad
# MAGIC rows are appended to the `_rejected` table.
# MAGIC
# MAGIC Widgets: `catalog` and `schema` name where the Bronze table lives and where Silver is
# MAGIC written. Run the notebook twice: the second run merges nothing new.

# COMMAND ----------

# MAGIC %pip install --quiet ..

# COMMAND ----------

from swiss_grid_lakehouse.silver.ch_load_silver import merge_silver, to_silver
from swiss_grid_lakehouse.silver.quality import run_gate

BRONZE = "entsoe_ch_load_bronze"
SILVER = "ch_load_silver"
CHECKS = 6  # row checks (4) + missing hours + row count

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")  # noqa: F821
dbutils.widgets.text("schema", "swiss_grid", "Schema")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
bronze_table = f"{catalog}.{schema}.{BRONZE}"
silver_table = f"{catalog}.{schema}.{SILVER}"

# COMMAND ----------

silver = to_silver(spark.table(bronze_table))  # noqa: F821
current = spark.table(silver_table).count() if spark.catalog.tableExists(silver_table) else 0  # noqa: F821
result = run_gate(silver, current)

# COMMAND ----------

if result.passed:
    print(f"{result.summary()} checks={CHECKS}")
    inserted, updated = merge_silver(spark, silver, silver_table)  # noqa: F821
    rows = spark.table(silver_table).count()  # noqa: F821
    print(f"merged inserted={inserted} updated={updated} rows={rows}")
else:
    print(result.summary())
    result.rejected_df.write.format("delta").mode("append").saveAsTable(silver_table + "_rejected")
    raise RuntimeError(f"quality gate failed, nothing merged: {result.summary()}")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver_table}"))  # noqa: F821
