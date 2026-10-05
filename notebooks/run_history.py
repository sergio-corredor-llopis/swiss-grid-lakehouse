# Databricks notebook source
# MAGIC %md
# MAGIC # Run history: one row per pipeline run
# MAGIC
# MAGIC Last task of the scheduled job. It reads the figures that the earlier notebooks set with
# MAGIC `dbutils.jobs.taskValues` (bronze rows, Silver and Gold inserted and updated, the gate
# MAGIC text, the Silver comparison, the reconcile difference) and merges one row into the Delta
# MAGIC table `pipeline_runs` with `record_run`, the same function the command line runner
# MAGIC `python -m swiss_grid_lakehouse.runs` calls. The key is `run_id`, so recording a run twice
# MAGIC leaves one row.
# MAGIC
# MAGIC The task runs even when an earlier task failed (`run_if: ALL_DONE`). A figure that was
# MAGIC never set stays NULL, and the status of the row is computed from the figures only: a
# MAGIC missing figure makes the run PARTIAL, a failed gate makes it FAILED. A task state of the
# MAGIC scheduler is never read.
# MAGIC
# MAGIC Widgets: `run_id` (the job run id; blank makes a new uuid), `started_at` (ISO time, blank
# MAGIC means now), `target` (`ch-load` or `ch-load-azure`), `source` (`recorded` or `api`),
# MAGIC `catalog` and `schema`. The last cell returns the `RUN` and `HISTORY` lines.

# COMMAND ----------

# MAGIC %pip install --quiet ..

# COMMAND ----------

import uuid
from datetime import UTC, datetime

from swiss_grid_lakehouse.report_lines import ReportLines
from swiss_grid_lakehouse.runs import history_line, record_run, run_line

BRONZE_TASKS = ("bronze", "bronze_entsoe")  # Free Edition job, Azure job
out = ReportLines()

# COMMAND ----------

try:
    environment_version = spark.conf.get("spark.databricks.environment.version")  # noqa: F821
except Exception:
    environment_version = "not recorded"
out.print(f"RUN spark_version={spark.version} environment_version={environment_version}")  # noqa: F821

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run id (blank: new uuid)")  # noqa: F821
dbutils.widgets.text("started_at", "", "Start time, ISO (blank: now)")  # noqa: F821
dbutils.widgets.text("target", "ch-load", "Job target")  # noqa: F821
dbutils.widgets.text("source", "recorded", "Input: recorded or api")  # noqa: F821
dbutils.widgets.text("catalog", "workspace", "Catalog")  # noqa: F821
dbutils.widgets.text("schema", "swiss_grid", "Schema")  # noqa: F821

run_id = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())  # noqa: F821
started_text = dbutils.widgets.get("started_at").strip()  # noqa: F821
started_at = (
    datetime.fromisoformat(started_text.replace("Z", "+00:00"))
    if started_text
    else datetime.now(UTC)
)
target = dbutils.widgets.get("target")  # noqa: F821
source = dbutils.widgets.get("source")  # noqa: F821
catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821

# COMMAND ----------


def figures_of(task_key):
    """The `figures` dict a task set, or an empty dict when it never set one."""
    try:
        value = dbutils.jobs.taskValues.get(  # noqa: F821
            taskKey=task_key, key="figures", default={}, debugValue={}
        )
    except Exception:
        return {}
    return dict(value) if value else {}


bronze = {}
for task_key in BRONZE_TASKS:
    bronze = bronze or figures_of(task_key)
silver, reconcile, gold = figures_of("silver"), figures_of("reconcile"), figures_of("gold")

# A failed gate in either task decides the stored gate text; otherwise the Gold gate is the one.
gates = [g for g in (silver.get("gate"), gold.get("gate")) if g]
failed = [g for g in gates if g.upper().startswith("FAIL")]
gate = failed[0] if failed else (gold.get("gate") or silver.get("gate"))

row = {
    "run_id": run_id,
    "started_at": started_at,
    "target": target,
    "source": source,
    "bronze_rows": bronze.get("bronze_rows"),
    "silver_inserted": silver.get("silver_inserted"),
    "silver_updated": silver.get("silver_updated"),
    "gate": gate,
    "compare": silver.get("compare"),
    "reconcile_diff": reconcile.get("reconcile_diff"),
    "gold_inserted": gold.get("gold_inserted"),
    "gold_updated": gold.get("gold_updated"),
}

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")  # noqa: F821
ledger = f"{catalog}.{schema}.pipeline_runs"
inserted, updated = record_run(spark, row, ledger)  # noqa: F821
out.print(run_line(row))
out.print(f"ledger {ledger} inserted={inserted} updated={updated}")
out.print(history_line(spark, ledger))  # noqa: F821

# COMMAND ----------

display(spark.table(ledger).orderBy("recorded_at", ascending=False).limit(10))  # noqa: F821

# COMMAND ----------

out.exit(globals().get("dbutils"))
