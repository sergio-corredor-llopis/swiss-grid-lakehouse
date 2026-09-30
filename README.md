# swiss-grid-lakehouse

Ingest of Swiss electricity grid data (actual total load) from two publishers:
the ENTSO-E Transparency Platform and Swissgrid AG. Each recorded source is
written to an append-only Bronze Delta table with Spark. A Silver layer
deduplicates it, runs a data-quality gate and merges it into a second Delta
table with `MERGE`. A compare command then puts the two publishers side by side.
It is tested against real recorded data and checked by CI on every push. Bronze,
Silver and the comparison are built; Gold is not.

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
                                              |
                                              v
             to_silver() --> run_gate() --pass--> merge_silver() --> Silver Delta table
                                |
                                +--fail--> nothing merged, bad rows to <target>_rejected

Swissgrid workbook or CSV (15-minute kWh) --> Bronze --> hourly MW --> same Silver table
                                                                          |
                                                                          v
                                          compare: hourly differences reported, daily total gated
```

- `parse_ch_load_xml`: pure parser, no network, no environment access.
- `fetch_ch_load`: thin I/O layer; the client is injectable, the token is read
  from `ENTSOE_API_TOKEN` (or `ENTSOE_TOKEN`) only.
- `to_bronze_rows`: one row per hour with source, area, pulled_at, batch_id,
  source_file, ts_utc and actual_load_mw. Needs only pandas.
- `write_bronze`: appends to a Delta table given by path or name. `batch_id` is
  a hash of the file, so loading the same file twice is skipped.
- `get_spark`: a local Spark session with Delta, or Databricks Connect.
- `to_silver`: one latest row per (source, area, ts_utc), typed, in MW, with the
  Europe/Zurich wall-clock time added. Gaps are not filled.
- `run_gate`: keys not null, key unique, load in 1000 to 15000 MW, on the hour,
  no missing hours, row count not below the current Silver.
- `merge_silver`: Delta `MERGE` on (source, area, ts_utc); a matched row is
  updated only if the pull is newer and the value differs.
- Swissgrid Bronze (`bronze --swissgrid`): reads the sheet `Zeitreihen0h15` (or a
  CSV cut from it), selects two series by name and checks the unit of those two
  only. Labels are kept as printed.
- Swissgrid Silver (`silver --source swissgrid`): four quarter hours of kWh are
  summed to one hour and divided by 1000, giving MW. The label is the interval
  START.
- `compare`: for the hours present in both sources it reports the differences by
  hour of day and gates on the daily total only.

## Run it

Needs Python 3.11+ and a Java 17 or 21 runtime for local Spark. No token is
needed: both sources are recorded files in the repository.

```sh
git clone https://github.com/sergio-corredor-llopis/swiss-grid-lakehouse.git && cd swiss-grid-lakehouse
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,spark,swissgrid]"
pytest -q
python -m swiss_grid_lakehouse.bronze --xml tests/fixtures/entsoe_ch_load_2026-08-30.xml --target /tmp/lh/bronze_entsoe && python -m swiss_grid_lakehouse.bronze --swissgrid tests/fixtures/swissgrid_ch_energy_2026-08-30.csv --target /tmp/lh/bronze_swissgrid
python -m swiss_grid_lakehouse.silver --bronze /tmp/lh/bronze_entsoe --target /tmp/lh/silver --source entsoe && python -m swiss_grid_lakehouse.silver --bronze /tmp/lh/bronze_swissgrid --target /tmp/lh/silver --source swissgrid && python -m swiss_grid_lakehouse.compare --silver /tmp/lh/silver
```

The ENTSO-E Bronze command prints `wrote 48 rows batch_id=...`; the Swissgrid one
prints `BRONZE rows=384 rerun=0` (192 quarter hours of two series). Running a
Bronze command again does not add rows. Each Silver command prints `GATE: PASS ...`
and `merged inserted=48 updated=0 rows=48`; running one again prints
`merged inserted=0 updated=0 rows=48`. The last command prints the hourly
differences by hour of day and ends with
`COMPARE days=2 hours=48 daily_max_abs_pct=2.47 tol=3 PASS`.
Walkthroughs: [first ingest](docs/walkthrough/first_ingest.md),
[Bronze on Delta](docs/walkthrough/bronze_delta.md),
[Silver with MERGE](docs/walkthrough/silver_merge.md) and
[Swissgrid and the comparison](docs/walkthrough/swissgrid.md).

## What the tests and CI prove

- The parser turns a real, unmodified ENTSO-E response
  (`tests/fixtures/entsoe_ch_load_2026-08-30.xml`) into a one-column frame with a
  sorted, unique, timezone-aware index, at least 24 rows, float values in a
  plausible 0 to 25000 MW range.
- `fetch_ch_load` uses an injected client and fails clearly, before any network
  call, when no token is available.
- Bronze rows have the expected shape, non-load XML is rejected, and a test
  marked `spark` writes the fixture to a local Delta table (48 rows) and shows
  that a second write of the same file is skipped.
- Silver keeps one row per hour, converts to Europe/Zurich time without using
  local time as a key, and a re-run merges nothing. A batch with a bad row
  fails the gate, leaves the Silver table at its version and lands in a
  `_rejected` table.
- GitHub Actions (`.github/workflows/ci.yml`) runs `ruff check`,
  `ruff format --check` and `pytest -m "not spark"` in a job named
  `lint-and-unit`. A second job, `spark`, runs the Spark tests with Java 21 and
  then runs Bronze and Silver for both sources on the fixtures, checking the
  merge counts, and runs the comparison. No secret is used.
- The Swissgrid reader accepts the real 64-series workbook layout (units, price
  series and two-line headers included) and rejects a wrong unit on a selected
  series. The hourly conversion is tested on known slots, on a partial hour and on
  the October clock change (25 hours). The compare command is tested just below
  and just above the daily tolerance, and its hourly report never decides the
  exit code.

## Stack

Python 3.11+, [entsoe-py](https://github.com/EnergieID/entsoe-py) 0.8.1,
pandas, pytest, ruff, GitHub Actions, Databricks Asset Bundles. Optional extra `spark`: PySpark 4.0.1 and
delta-spark 4.0.0. A Databricks notebook (`notebooks/bronze_entsoe_ch_load.py`)
calls the same `write_bronze`, and `notebooks/silver_ch_load.py` calls the same
Silver functions; I have not run either on a Databricks workspace, so that path
is untested. No dbt or Azure code is in this repository.

## Data sources and attribution

| Source | Dataset | Resolution | Unit | Timestamp label | In this repository |
|---|---|---|---|---|---|
| ENTSO-E Transparency Platform | Actual Total Load, Switzerland (bidding zone `10YCH-SWISSGRIDZ`) | hourly | MW | UTC | `tests/fixtures/entsoe_ch_load_2026-08-30.xml` |
| Swissgrid AG | Energieuebersicht Schweiz 2026 / Aggregated energy data of the Swiss control block, sheet `Zeitreihen0h15` | 15 minutes | kWh per slot | interval START, local time | `tests/fixtures/swissgrid_ch_energy_2026-08-30.csv` |

Data: ENTSO-E Transparency Platform (https://transparency.entsoe.eu/), actual
total load, Switzerland. The ENTSO-E fixture is an unmodified response from the
platform's public RESTful API; the data belongs to ENTSO-E and its data
providers.

Data: Swissgrid AG, "Energieuebersicht Schweiz 2026 / Aggregated energy data of
the Swiss control block", from swissgrid.ch (English site: Operation > Grid data >
Generation). The data belongs to Swissgrid AG. I did not find a reuse or licence statement on that
page, so check Swissgrid's terms before reusing the extract.

## The recorded two-day extract

Both fixtures are real data, not synthetic: the local days 2026-08-30 and
2026-08-31 (Europe/Zurich), 48 hours, from the two publishers named above.

- ENTSO-E: the API response for those two days, unmodified.
- Swissgrid: `scripts/record_swissgrid_slice.py` cut two series out of the
  published 2026 workbook (`Summe endverbrauchte Energie Regelblock Schweiz` and
  `Summe verbrauchte Energie Regelblock Schweiz`, all 192 quarter hours of the
  two days). The workbook itself, 64 series, is not stored here.

On these two days the two publishers agree on the daily total (within about 2 %)
but not hour by hour (10 % on average, up to 36 %). The comparison reports the
hourly differences and gates on the daily total only. The cause of the hourly gap
is not established. Details, definitions and numbers:
[Swissgrid and the comparison](docs/walkthrough/swissgrid.md).

## Release and deploy

CI and deploy are separate. `databricks.yml` defines one job, `ch-load` (Bronze then Silver, serverless). Pushing a tag `v*` runs `deploy.yml`: an offline schema check of the bundle, then `databricks bundle validate` and `databricks bundle deploy -t dev` with the repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Without those secrets (for example in a fork) the deploy job is skipped and the run stays green. CI never sees a secret.

Step by step, including the one-time setup: [deploy walkthrough](docs/walkthrough/deploy.md).

## Roadmap

Built: ingest, Bronze and Silver (Spark and Delta, run locally and in CI).

Planned, not implemented:

1. Run the Bronze notebook on Databricks Free Edition and record the result.
2. Gold layer: dbt-databricks models, with `OPTIMIZE` / `Z-ORDER` and Delta time
   travel.
3. Orchestration with Databricks Workflows or Airflow, and a Streamlit view.
4. Possible extensions: Terraform for an ADLS Gen2 storage target, and
   streaming.

## Databricks Free Edition run

Bronze and Silver notebooks run on Databricks Free Edition; output as printed by the notebooks:

```
2026-09-27, Databricks Free Edition, serverless compute, catalog workspace, schema swiss_grid

bronze_entsoe_ch_load, first run (14:33:06):
wrote 48 rows to workspace.swiss_grid.entsoe_ch_load_bronze batch_id=3a7ceaa73409b117
bronze_entsoe_ch_load, second run (14:33:57):
skipped 48 rows to workspace.swiss_grid.entsoe_ch_load_bronze batch_id=3a7ceaa73409b117

silver_ch_load, first run (14:34:55):
GATE: PASS checks=6
merged inserted=48 updated=0 rows=48

silver_ch_load, second run (14:36:11):
GATE: PASS checks=6
merged inserted=0 updated=0 rows=48
```
