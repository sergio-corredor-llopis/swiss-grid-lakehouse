"""Quality gate for the Gold marts, run before anything is merged.

`run_gold_gate` checks the freshly built marts against the Silver rows and returns a
`GoldGateResult`. When it fails the caller merges nothing, so the Gold tables keep their
version. Checks: (1) g1 GWh per source equals the Silver sum, (2) g2 has one row per series
and day, skipped when g2 is None, (3) no NULL in the key columns of any mart, (4) g3 counts at
least one day on every row, (5) g4 hours equal g1 hours per source and day. Spark is imported
lazily.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

GWH_TOLERANCE = 1e-6
KEYS = {
    "g1": ["source", "local_date"],
    "g2": ["source", "series", "local_date"],
    "g3": ["source", "weekday", "hour_local"],
    "g4": ["source", "local_date"],
}


@dataclass
class GoldGateResult:
    passed: bool
    failures: dict[str, int] = field(default_factory=dict)  # check name -> offending rows
    checks: int = 0  # checks that ran
    skipped: int = 0  # checks left out because their mart is None

    def summary(self) -> str:
        if self.passed:
            tail = f" skipped={self.skipped}" if self.skipped else ""
            return f"GOLD GATE: PASS checks={self.checks}{tail}"
        return "GOLD GATE: FAIL " + " ".join(f"{k}={v}" for k, v in self.failures.items())


def _gwh_mismatch(silver: Any, g1: Any) -> int:
    """Sources whose g1 GWh differs from the Silver sum of load_mw / 1000, or is missing."""
    from pyspark.sql import functions as f

    want = silver.groupBy("source").agg((f.sum("load_mw") / 1000).alias("want"))
    got = g1.groupBy("source").agg(f.sum("gwh_total").alias("got"))
    joined = want.join(got, "source", "full_outer")
    bad = (
        f.col("want").isNull()
        | f.col("got").isNull()
        | (f.abs(f.col("want") - f.col("got")) > GWH_TOLERANCE)
    )
    return joined.filter(bad).count()


def _duplicate_keys(df: Any, key: list[str]) -> int:
    """Rows beyond the first for a key: 0 when every key occurs once."""
    return df.count() - df.select(*key).distinct().count()


def _null_keys(df: Any, key: list[str]) -> int:
    from functools import reduce

    anynull = reduce(lambda a, b: a | b, [df[k].isNull() for k in key])
    return df.filter(anynull).count()


def _hours_mismatch(g1: Any, g4: Any) -> int:
    """Days where g4 hours differ from g1 hours, or that only one of them holds."""
    from pyspark.sql import functions as f

    a = g1.select("source", "local_date", f.col("hours").alias("h1"))
    b = g4.select("source", "local_date", f.col("hours").alias("h4"))
    joined = a.join(b, ["source", "local_date"], "full_outer")
    return joined.filter(~f.col("h1").eqNullSafe(f.col("h4"))).count()


def run_gold_gate(silver: Any, marts: dict[str, Any]) -> GoldGateResult:
    """Run the Gold checks; `marts` maps a short id (g1..g4) to a DataFrame or None.

    A missing g2 skips check 2. A missing g1, g3 or g4 fails the checks that need it, because
    the caller always builds those three.
    """
    from pyspark.sql import functions as f

    g1, g2, g3, g4 = (marts.get(k) for k in ("g1", "g2", "g3", "g4"))
    failures: dict[str, int] = {}
    checks = 0
    skipped = 0

    checks += 1
    n = _gwh_mismatch(silver, g1) if g1 is not None else 1
    if n:
        failures["gwh_total"] = n

    if g2 is None:
        skipped += 1
    else:
        checks += 1
        n = _duplicate_keys(g2, KEYS["g2"])
        if n:
            failures["g2_rows"] = n

    checks += 1
    nulls = {k: _null_keys(df, KEYS[k]) for k, df in marts.items() if df is not None and k in KEYS}
    n = sum(nulls.values())
    if n:
        failures["null_key"] = n

    checks += 1
    n = g3.filter(f.col("n_days").isNull() | (f.col("n_days") < 1)).count() if g3 is not None else 1
    if n:
        failures["n_days"] = n

    checks += 1
    n = _hours_mismatch(g1, g4) if g1 is not None and g4 is not None else 1
    if n:
        failures["hours_match"] = n

    return GoldGateResult(not failures, failures, checks, skipped)
