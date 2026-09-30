"""Turn Bronze Swiss load rows into Silver and merge them into a Delta table.

`to_silver` keeps one row per (source, area, ts_utc), the latest pull, and adds the local
Europe/Zurich time. `merge_silver` inserts new keys and updates a row only when the incoming
pull is newer and the value differs, so running the same batch twice changes nothing.
`load_mw` is the value that is compared across sources. Two nullable columns carry the
Swissgrid end-user consumption and the difference between total and end-user consumption;
ENTSO-E rows hold NULL there. Spark is imported lazily.
"""

from __future__ import annotations

from typing import Any

SILVER_COLUMNS = [
    "source",
    "area",
    "ts_utc",
    "ts_local",
    "local_date",
    "resolution_min",
    "load_mw",
    "load_enduser_mw",
    "load_difference_mw",
    "batch_id",
    "valid_from",
    "ingested_at",
]
KEY = ["source", "area", "ts_utc"]
LOCAL_TZ = "Europe/Zurich"
RESOLUTION_MIN = 60
# Factor that converts a source unit to MW. ENTSO-E reports MW; Swissgrid reports the energy
# of one hour in kWh, and 1 kWh per hour is 0.001 MW.
UNIT_TO_MW = {"MW": 1.0, "kWh_per_h": 0.001}
SOURCE_UNIT = {"entsoe": "MW", "swissgrid": "kWh_per_h"}
# Optional value columns: Bronze/hourly column -> Silver column. A frame without the input
# column (every ENTSO-E frame) gets NULL.
EXTRA_MW = {
    "energy_enduser_kwh": "load_enduser_mw",
    "energy_difference_kwh": "load_difference_mw",
}
ENDUSER_COMMENT = (
    "Swissgrid 'Summe endverbrauchte Energie' as mean MW over the hour: energy consumed by "
    "end users. Swissgrid (sheet Uebersicht): not included are grid losses or energy consumed "
    "for power plant's own requirements or to drive the pumps in pumped storage hydro power "
    "plant. NULL for ENTSO-E rows."
)
DIFFERENCE_COMMENT = (
    "load_mw minus load_enduser_mw, in MW: what Swissgrid's total consumed energy holds beyond "
    "end-user consumption. Swissgrid (sheet Uebersicht), total consumed: 'Included are grid "
    "losses, energy consumed for a power plant's own requirements and to drive the pumps in "
    "pumped storage hydro power plant.' So the difference is grid losses, power plants' own "
    "requirements and the pumps of pumped storage plants; the three parts are not published "
    "separately. NULL for ENTSO-E rows."
)


def _silver_schema() -> Any:
    from pyspark.sql import types as t

    return t.StructType(
        [
            t.StructField("source", t.StringType(), False),
            t.StructField("area", t.StringType(), False),
            t.StructField("ts_utc", t.TimestampType(), False),
            t.StructField("ts_local", t.TimestampNTZType(), False),
            t.StructField("local_date", t.DateType(), False),
            t.StructField("resolution_min", t.IntegerType(), False),
            t.StructField("load_mw", t.DoubleType(), False),
            t.StructField("load_enduser_mw", t.DoubleType(), True, {"comment": ENDUSER_COMMENT}),
            t.StructField(
                "load_difference_mw", t.DoubleType(), True, {"comment": DIFFERENCE_COMMENT}
            ),
            t.StructField("batch_id", t.StringType(), False),
            t.StructField("valid_from", t.TimestampType(), False),
            t.StructField("ingested_at", t.TimestampType(), False),
        ]
    )


def to_silver(bronze_df: Any, value_col: str = "actual_load_mw") -> Any:
    """Return the Silver frame for a Bronze frame: one latest row per key, typed, in MW.

    `value_col` names the Bronze column that holds the value in the unit of its source; it is
    `actual_load_mw` for ENTSO-E and `energy_kwh` (total consumed) for the hourly Swissgrid
    frame. If the frame also has `energy_enduser_kwh` and `energy_difference_kwh` (the hourly
    Swissgrid frame does) they are converted by the same unit map into `load_enduser_mw` and
    `load_difference_mw`; otherwise those two columns are NULL. Gaps are not filled. The
    session time zone is set to UTC first so that the local wall-clock column is computed
    from the UTC instant.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as f

    bronze_df.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    factor = f.create_map(
        *[
            x
            for source, unit in SOURCE_UNIT.items()
            for x in (f.lit(source), f.lit(UNIT_TO_MW[unit]))
        ]
    )
    latest = Window.partitionBy(*KEY).orderBy(f.col("pulled_at").desc(), f.col("batch_id").desc())
    ranked = bronze_df.withColumn("_rn", f.row_number().over(latest)).filter("_rn = 1")
    local = f.from_utc_timestamp(f.col("ts_utc"), LOCAL_TZ).cast("timestamp_ntz")
    extra = [
        (f.col(src) * factor[f.col("source")] if src in bronze_df.columns else f.lit(None))
        .cast("double")
        .alias(dst)
        for src, dst in EXTRA_MW.items()
    ]
    return ranked.select(
        f.col("source"),
        f.col("area"),
        f.col("ts_utc"),
        local.alias("ts_local"),
        local.cast("date").alias("local_date"),
        f.lit(RESOLUTION_MIN).cast("int").alias("resolution_min"),
        (f.col(value_col) * factor[f.col("source")]).alias("load_mw"),
        *extra,
        f.col("batch_id"),
        f.col("pulled_at").alias("valid_from"),
        f.current_timestamp().alias("ingested_at"),
    )


def _is_path(target: str) -> bool:
    return "/" in target or "\\" in target


def _delta_table(spark: Any, target: str) -> Any:
    from delta.tables import DeltaTable

    if _is_path(target):
        return DeltaTable.forPath(spark, target)
    return DeltaTable.forName(spark, target)


def _exists(spark: Any, target: str) -> bool:
    from delta.tables import DeltaTable

    if _is_path(target):
        return DeltaTable.isDeltaTable(spark, target)
    return spark.catalog.tableExists(target)


def _create_empty(spark: Any, target: str) -> None:
    writer = spark.createDataFrame([], schema=_silver_schema()).write.format("delta")
    if _is_path(target):
        writer.save(target)
    else:
        writer.saveAsTable(target)


def merge_silver(spark: Any, df: Any, target: str) -> tuple[int, int]:
    """Merge `df` into the Silver Delta table at `target`; return (inserted, updated).

    The key is (source, area, ts_utc), never the local time, which repeats an hour when the
    clocks go back. A matched row is updated only if the incoming `valid_from` is newer and
    `load_mw` differs. The counts come from the MERGE metrics in the table history.
    """
    if not _exists(spark, target):
        _create_empty(spark, target)
    table = _delta_table(spark, target)
    cond = " AND ".join(f"t.{k} = s.{k}" for k in KEY)
    (
        table.alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll(condition="s.valid_from > t.valid_from AND s.load_mw <> t.load_mw")
        .whenNotMatchedInsertAll()
        .execute()
    )
    metrics = table.history(1).select("operationMetrics").first()[0]
    return int(metrics["numTargetRowsInserted"]), int(metrics["numTargetRowsUpdated"])
