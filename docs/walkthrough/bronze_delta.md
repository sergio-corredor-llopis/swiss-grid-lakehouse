# Bronze on Delta: a walkthrough

This page follows one recorded ENTSO-E response (Swiss actual load) into a
Bronze Delta table and back. No token is needed; the input is
`tests/fixtures/entsoe_ch_load_sample.xml`.

## Setup

```sh
pip install -e ".[dev,spark]"
```

Local Spark needs a Java 17 or 21 runtime. The `spark` extra pins PySpark 4.0.1
and delta-spark 4.0.0.

## 1. Rows without Spark

```python
from datetime import UTC, datetime
from pathlib import Path
from swiss_grid_lakehouse.bronze import to_bronze_rows

xml = Path("tests/fixtures/entsoe_ch_load_sample.xml")
rows = to_bronze_rows(xml.read_text(encoding="utf-8"), xml.name, datetime.now(UTC))
print(len(rows), rows[0])
```

You get 48 tuples. Each is (source, area, pulled_at, batch_id, source_file,
ts_utc, actual_load_mw). `batch_id` is the first 16 hex characters of the
SHA-256 of the XML text, so one file always maps to one batch. This step uses
pandas only.

## 2. Write to Delta

```sh
python -m swiss_grid_lakehouse.bronze --xml tests/fixtures/entsoe_ch_load_sample.xml --target /tmp/bronze_ch_load
```

Expected: `wrote 48 rows batch_id=...`. `--target` is a Delta path (it contains a
slash) or a table name. The session comes from `get_spark()`; set
`SGL_RUNTIME=databricks` to use Databricks Connect instead of local Spark.

## 3. Run it again

The same command now prints `skipped 48 rows ...`. Before appending,
`write_bronze` checks whether the table already holds that `batch_id`. The table
still has 48 rows and one batch.

## 4. Read it back

```python
from swiss_grid_lakehouse.spark import get_spark

spark = get_spark("local")
df = spark.read.format("delta").load("/tmp/bronze_ch_load")
print(df.count(), df.select("batch_id").distinct().count())
```

Expected: `48 1`.

## Design decisions

- Append-only: Bronze keeps what was received; nothing is updated or deleted.
  Cleaning belongs to a later layer.
- Idempotent by content hash: a retried job cannot duplicate a batch.
- Lazy Spark imports: the core package and `to_bronze_rows` stay usable with
  pandas alone, and the default CI job needs no JVM.
- One write function for two runtimes: the command line and the Databricks
  notebook (`notebooks/bronze_entsoe_ch_load.py`) both call `write_bronze`. The
  notebook has not been run on a Databricks workspace yet.

## Tests

`pytest -q -m "not spark"` runs the fast tests. `pytest -q -m spark` runs the
Delta write test and needs Java. CI runs both in separate jobs.
