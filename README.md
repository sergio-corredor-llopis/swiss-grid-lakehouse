# swiss-grid-lakehouse

Ingest of Swiss electricity grid data (actual total load) from the ENTSO-E
Transparency Platform, and a Bronze layer: the recorded response is written to
an append-only Delta table with Spark. It is tested against a real recorded API
response and checked by CI on every push. Bronze is built; Silver and Gold are
not.

Author: Sergio Corredor, data engineer.

## Architecture

```
ENTSO-E REST API --> fetch_ch_load() --> pandas DataFrame ("Actual Load", MW, tz-aware index)
                          ^
recorded XML file --> parse_ch_load_xml()   (pure: text in, DataFrame out)
                          |
                          v
                  to_bronze_rows()  -->  write_bronze()  -->  Bronze Delta table (append-only)
                                              ^
                                     get_spark("local" | "databricks")
```

- `parse_ch_load_xml`: pure parser, no network, no environment access.
- `fetch_ch_load`: thin I/O layer; the client is injectable, the token is read
  from `ENTSOE_API_TOKEN` (or `ENTSOE_TOKEN`) only.
- `to_bronze_rows`: one row per hour with source, area, pulled_at, batch_id,
  source_file, ts_utc and actual_load_mw. Needs only pandas.
- `write_bronze`: appends to a Delta table given by path or name. `batch_id` is
  a hash of the file, so loading the same file twice is skipped.
- `get_spark`: a local Spark session with Delta, or Databricks Connect.

## Run it

Needs Python 3.11+ and a Java 17 or 21 runtime for local Spark.

```sh
git clone https://github.com/sergio-corredor-llopis/swiss-grid-lakehouse.git
cd swiss-grid-lakehouse
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,spark]"
pytest -q
python -m swiss_grid_lakehouse.bronze --xml tests/fixtures/entsoe_ch_load_sample.xml --target /tmp/bronze_ch_load
```

The last command prints `wrote 48 rows batch_id=...`; running it again prints
`skipped 48 rows ...` and the table still holds 48 rows. No token is needed.
Walkthroughs: [first ingest](docs/walkthrough/first_ingest.md) and
[Bronze on Delta](docs/walkthrough/bronze_delta.md).

## What the tests and CI prove

- The parser turns a real, unmodified ENTSO-E response
  (`tests/fixtures/entsoe_ch_load_sample.xml`) into a one-column frame with a
  sorted, unique, timezone-aware index, at least 24 rows, float values in a
  plausible 0 to 25000 MW range.
- `fetch_ch_load` uses an injected client and fails clearly, before any network
  call, when no token is available.
- Bronze rows have the expected shape, non-load XML is rejected, and a test
  marked `spark` writes the fixture to a local Delta table (48 rows) and shows
  that a second write of the same file is skipped.
- GitHub Actions (`.github/workflows/ci.yml`) runs `ruff check`,
  `ruff format --check` and `pytest -m "not spark"` in one job, and the
  `spark` tests with Java 21 in a second job, with no secret.

## Stack

Python 3.11+, [entsoe-py](https://github.com/EnergieID/entsoe-py) 0.8.1,
pandas, pytest, ruff, GitHub Actions. Optional extra `spark`: PySpark 4.0.1 and
delta-spark 4.0.0. A Databricks notebook (`notebooks/bronze_entsoe_ch_load.py`)
calls the same `write_bronze`; I have not run it on a Databricks workspace, so
that path is untested. No dbt or Azure code is in this repository.

## Data source and attribution

Data: ENTSO-E Transparency Platform (https://transparency.entsoe.eu/), actual
total load, Switzerland (bidding zone `10YCH-SWISSGRIDZ`). The fixture is an
unmodified response from the platform's public RESTful API; the data belongs to
ENTSO-E and its data providers.

## Roadmap

Built: ingest and Bronze (Spark and Delta, run locally and in CI).

Planned, not implemented:

1. Run the Bronze notebook on Databricks Free Edition and record the result.
2. Silver layer: PySpark transformations, Delta `MERGE` and a data-quality gate.
3. Gold layer: dbt-databricks models, with `OPTIMIZE` / `Z-ORDER` and Delta time
   travel.
4. Orchestration with Databricks Workflows or Airflow, and a Streamlit view.
5. Possible extensions: Terraform for an ADLS Gen2 storage target, and
   streaming.
