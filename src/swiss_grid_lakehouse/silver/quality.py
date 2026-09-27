"""Quality gate for a Silver candidate, run before the MERGE.

`run_gate` checks the candidate DataFrame and returns a `GateResult`. When it fails, the caller
merges nothing, so the Silver table keeps its version, and writes `rejected_df` to a rejected
table. Checks: keys not null, key unique, load_mw in range, on the hour, missing hours, row
count not below the current Silver, and optionally freshness. Spark is imported lazily.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

KEY = ["source", "area", "ts_utc"]
LOAD_MIN_MW = 1000.0
LOAD_MAX_MW = 15000.0


@dataclass
class GateResult:
    passed: bool
    failures: dict[str, int]  # check name -> number of offending rows (or hours)
    rejected_df: Any  # rows that broke a row-level check, with `check` and `run_at`

    def summary(self) -> str:
        if self.passed:
            return "GATE: PASS"
        return "GATE: FAIL " + " ".join(f"{k}={v}" for k, v in self.failures.items())


def run_gate(
    df: Any,
    current_count: int,
    max_missing: int = 0,
    max_age: timedelta | None = None,
) -> GateResult:
    from pyspark.sql import Window
    from pyspark.sql import functions as f

    key_null = f.lit(False)
    for c in KEY:
        key_null = key_null | f.col(c).isNull()
    dup = f.count(f.lit(1)).over(Window.partitionBy(*KEY)) > 1
    epoch = f.col("ts_utc").cast("long")
    row_checks = {
        "keys_not_null": key_null,
        "key_unique": dup,
        "range_load_mw": ~f.col("load_mw").between(LOAD_MIN_MW, LOAD_MAX_MW)
        | f.col("load_mw").isNull(),
        "on_the_hour": (epoch % 3600) != 0,
    }
    flagged = df.select("*", *[cond.alias(name) for name, cond in row_checks.items()])
    run_at = datetime.now(UTC)
    failures: dict[str, int] = {}
    rejected = None
    for name in row_checks:
        bad = flagged.filter(f.coalesce(f.col(name), f.lit(True))).select(*df.columns)
        n = bad.count()
        if n:
            failures[name] = n
            part = bad.withColumn("check", f.lit(name)).withColumn("run_at", f.lit(run_at))
            rejected = part if rejected is None else rejected.unionByName(part)
    if rejected is None:
        rejected = df.limit(0).withColumn("check", f.lit("")).withColumn("run_at", f.lit(run_at))

    ok = df.filter(~key_null)
    span = ok.groupBy("source", "area").agg(
        f.min("ts_utc").cast("long").alias("lo"),
        f.max("ts_utc").cast("long").alias("hi"),
        f.countDistinct("ts_utc").alias("n"),
    )
    missing = 0
    for r in span.collect():
        missing += int((r["hi"] - r["lo"]) // 3600 + 1 - r["n"])
    if missing > max_missing:
        failures["missing_hours"] = missing

    total = df.count()
    if total < current_count:
        failures["count_below_current"] = current_count - total

    if max_age is not None:
        newest = ok.agg(f.max("ts_utc")).first()[0]
        if newest is None or datetime.now(UTC) - newest.astimezone(UTC) > max_age:
            failures["freshness"] = 1

    return GateResult(not failures, failures, rejected)
