# Silver with Delta MERGE: a walkthrough

This page turns the Bronze table from the [Bronze walkthrough](bronze_delta.md)
into a Silver table, shows the quality gate refusing a bad batch, and shows that
a re-run changes nothing. No ENTSO-E token is needed for the fixture. To use
live data, request a token from the ENTSO-E Transparency Platform (see its
RESTful API guide) and set `ENTSOE_API_TOKEN`; the Silver steps are the same.

## Setup

```sh
pip install -e ".[dev,spark]"
python -m swiss_grid_lakehouse.bronze --xml tests/fixtures/entsoe_ch_load_2026-08-30.xml --target /tmp/bronze_ch_load
```

Local Spark needs a Java 17 or 21 runtime.

## 1. Build Silver

```sh
python -m swiss_grid_lakehouse.silver --bronze /tmp/bronze_ch_load --target /tmp/silver_ch_load
```

Expected:

```
GATE: PASS checks=6
merged inserted=48 updated=0 rows=48
```

`to_silver` keeps the latest pull for each (source, area, ts_utc), converts the
value to MW, and adds `ts_local` (Europe/Zurich wall clock), `local_date`,
`resolution_min`, `valid_from` (the pull time) and `ingested_at`.

## 2. Run it again

The same command prints `merged inserted=0 updated=0 rows=48`. The merge matches
every key and finds no newer pull with a different value, so no row changes.

## 3. Read it back

```python
from swiss_grid_lakehouse.spark import get_spark

spark = get_spark("local")
df = spark.read.format("delta").load("/tmp/silver_ch_load")
print(df.count(), df.select("source", "area", "ts_utc").distinct().count())
```

Expected: `48 48`, so there are no duplicate keys.

## 4. The gate

`run_gate` runs before the merge and checks the candidate frame:

- `keys_not_null` and `key_unique` on (source, area, ts_utc)
- `range_load_mw`: load between 1000 and 15000 MW
- `on_the_hour`: the timestamp falls on a full hour
- `missing_hours`: no hour missing between the first and last row
- `count_below_current`: the candidate has at least as many rows as Silver
- optionally `freshness`, when a maximum age is passed

If any check fails, the command prints `GATE: FAIL` with the failing checks,
merges nothing, appends the offending rows to `<target>_rejected` and exits with
code 2. The Silver table keeps its Delta version. `tests/test_silver_quality.py`
and `tests/test_silver_cli.py` plant a bad row and assert exactly that.

## Design decisions

- The key is UTC. Local time repeats an hour when the clocks go back in
  October, so `ts_local` is a derived column and never part of the key.
- The gate runs before the merge, so a bad batch cannot reach Silver. Rejected
  rows are kept, with the check name and the run time, for inspection.
- The update condition is "newer pull and different value". A retry of the same
  batch is a no-op, and an older batch cannot overwrite a newer one.
- Units are converted in one lookup table (`UNIT_TO_MW`), so a second source
  adds a row, not a code path.
- Gaps are reported by the gate, not filled. Inventing values would hide a
  data problem.
- As with Bronze, the same functions run from the command line and from the
  Databricks notebook (`notebooks/silver_ch_load.py`); the notebook ran on
  Databricks Free Edition on 2026-09-27, and the recorded output is in the
  README section "Databricks Free Edition run".

## Tests

`pytest -q -m "not spark"` runs the fast tests; `pytest -q -m spark` runs the
Delta tests and needs Java. CI runs both in separate jobs and then runs Bronze
once and Silver twice, checking the printed merge counts.
