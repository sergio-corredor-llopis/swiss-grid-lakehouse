# swiss-grid-lakehouse

Ingest of Swiss electricity grid data (actual total load) from two publishers:
the ENTSO-E Transparency Platform and Swissgrid AG. Each recorded source is
written to an append-only Bronze Delta table with Spark. A Silver layer
deduplicates it, runs a data-quality gate and merges it into a second Delta
table with `MERGE`. A compare command then puts the two publishers side by side.
It is tested against real recorded data and checked by CI on every push. Bronze,
Silver, the comparison, the daily control totals and four Gold marts are built, and
the `ch-load` bundle job is defined to run ENTSO-E Bronze, Silver and Gold daily (no
scheduled run is recorded yet). The `azure` bundle target ran
Bronze, Silver, the comparison and the daily reconcile on an Azure Databricks
workspace on 2026-10-02, recorded in
[docs/runs/azure_2026-10-02.md](docs/runs/azure_2026-10-02.md).

Author: Sergio Corredor, data engineer.

## Milestones

Each milestone is one squash-merged pull request (the first two are single commits).
Run evidence names only runs recorded in this README or under `docs/runs/`.

<!-- milestones:begin -->
| Milestone | What a reader gains | PR | Docs page | Run evidence |
|---|---|---|---|---|
| M1 | ENTSO-E actual-load ingest: a pure XML parser, an injectable client, tests and CI | 8f25238 | [first ingest](docs/walkthrough/first_ingest.md) | CI only |
| M2 | Append-only Bronze Delta table, local Spark tests, a Databricks notebook | f43079b | [Bronze on Delta](docs/walkthrough/bronze_delta.md) | Databricks Free Edition, 2026-09-27: `bronze_entsoe_ch_load` |
| M3 | Silver with a data-quality gate and Delta `MERGE`; a Databricks Asset Bundle and a deploy workflow | #2, #4 | [Silver with MERGE](docs/walkthrough/silver_merge.md), [deploy](docs/walkthrough/deploy.md) | Databricks Free Edition, 2026-09-27: `silver_ch_load`; no tag deploy run recorded |
| M4 | Swissgrid as a second source and an hourly comparison gated on the daily total | #5 | [Swissgrid and the comparison](docs/walkthrough/swissgrid.md) | Azure Databricks, 2026-10-02: tasks `bronze_swissgrid` and `silver` |
| M5 | Daily control totals reconciled with the published national consumption | #6 | [Daily control totals](docs/walkthrough/daily_control.md) | Azure Databricks, 2026-10-02: task `reconcile` |
| M6 | The `azure` bundle target with OpenID Connect sign-in, report lines in the run output | #7, #8, #9, #10 | [Azure walkthrough](docs/walkthrough/azure.md), [run record](docs/runs/azure_2026-10-02.md) | Azure Databricks, 2026-10-02: first run and an idempotent second run |
| M7 | Four Gold marts in PySpark and Spark SQL with `OPTIMIZE` and time travel | #11 | [Gold marts](docs/walkthrough/gold.md) | not yet run |
| M8 | A scheduled `ch-load` job and a run ledger (`pipeline_runs`) | #12 | [schedule](docs/walkthrough/schedule.md) | not yet run |
| M9 | A docs coherence test in CI, this table, walkthroughs that match the code | #13 | [deploy](docs/walkthrough/deploy.md) | CI only |
<!-- milestones:end -->

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
- `tests/test_docs_coherence.py` checks offline that every repository path named
  in the README and `docs/` exists, that every task key of `databricks.yml` is
  named in the deploy docs, and that the milestone table above is complete.
- The Swissgrid reader accepts the real 64-series workbook layout (units, price
  series and two-line headers included) and rejects a wrong unit on a selected
  series. The hourly conversion is tested on known slots, on a partial hour and on
  the October clock change (25 hours). The compare command is tested just below
  and just above the daily tolerance, and its hourly report never decides the
  exit code.

## Stack

- Python 3.11+, [entsoe-py](https://github.com/EnergieID/entsoe-py) 0.8.1, pandas;
  openpyxl 3.1.5 in the optional extra `swissgrid`.
- Spark and Delta Lake (optional extra `spark`): PySpark 4.0.1 and delta-spark
  4.0.0, used by Bronze, Silver and Gold, locally and in CI.
- Databricks: a Databricks Asset Bundle (`databricks.yml`) with serverless jobs
  and six notebooks under `notebooks/` that call the same functions as the command
  line (`notebooks/bronze_entsoe_ch_load.py` calls `write_bronze`,
  `notebooks/silver_ch_load.py` the Silver functions).
- pytest, ruff, GitHub Actions. No Terraform code is in this repository.

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

<!-- daily-control:begin -->
## Daily control totals

The hourly Silver table is checked against the daily national consumption that the
Swiss Federal Office of Energy publishes from Swissgrid data. Two commands, run
here on the recorded files (after the Silver commands above have written
`/tmp/lh/silver`):

```sh
python -m swiss_grid_lakehouse.daily --package tests/fixtures/ckan_package_show_2026-09-29.json --csv tests/fixtures/ogd103_2026-08-30_31.csv --target /tmp/lh/daily --state /tmp/lh/state.json
python -m swiss_grid_lakehouse.reconcile --silver /tmp/lh/silver --daily /tmp/lh/daily
```

The first prints `REGISTRY changed fetched=1 rows=2 batch=...` and, run again,
`REGISTRY unchanged`. Without `--package` and `--csv` it reads the live dataset
record and CSV. The second prints one `DAY` line per day and one `RECONCILE` line
per series. On the recorded slice both days are preliminary, so it exits 0:

```text
RECONCILE source=swissgrid series=total published=landesverbrauch days=2 pipeline_gwh=309.72 published_gwh=298.00 diff_pct=3.93 tol=6.5 preliminary=2 final=0 PASS
RECONCILE source=swissgrid series=enduser published=endverbrauch days=2 pipeline_gwh=244.49 published_gwh=235.00 diff_pct=4.04 tol=6.5 preliminary=2 final=0 PASS
RECONCILE source=entsoe series=total published=landesverbrauch days=2 pipeline_gwh=303.08 published_gwh=298.00 diff_pct=1.71 tol=5 preliminary=2 final=0 PASS
```

A final day beyond tolerance exits 3. The definition of ENTSO-E actual total load
is not established, so the gap between it and the Swissgrid workbook is
unexplained. Tolerances, the preliminary window and the numbers behind them:
[Daily control totals](docs/walkthrough/daily_control.md).

Data: Swiss Federal Office of Energy SFOE (data from Swissgrid),
"energiedashboard.ch - National and final consumption",
https://opendata.swiss/en/dataset/energiedashboard-ch-landesverbrauch-und-endverbrauch.
<!-- daily-control:end -->

## Release and deploy

CI and deploy are separate. `databricks.yml` defines two jobs, both on serverless compute:

- `ch-load`, default `dev` target, tasks in order: `bronze` (ENTSO-E Bronze), `silver`, `gold`, then `run_history` (the run ledger, which runs when all earlier tasks are done, failed or not). Scheduled daily at 06:00 Europe/Zurich, see the [schedule walkthrough](docs/walkthrough/schedule.md). It has no Swissgrid Bronze task, so the Swissgrid source runs only from the command line on that target.
- `ch-load-azure`, `azure` target, tasks: `bronze_entsoe` and `bronze_swissgrid`, then `silver`, `reconcile`, `gold` and `run_history`. Its schedule is paused; see the Azure section below.

Pushing a tag `v*` runs `deploy.yml`: an offline schema check of the bundle, then `databricks bundle validate` and `databricks bundle deploy -t dev` with the repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Without those secrets (for example in a fork) the deploy job is skipped and the run stays green. No run of this tag deploy is recorded in this repository. CI never sees a secret.

Step by step, including the one-time setup: [deploy walkthrough](docs/walkthrough/deploy.md).

<!-- gold:begin -->
## Gold marts

Gold is four small Delta tables built from Silver with PySpark and Spark SQL: daily
load per Swiss day (`gold_daily_load`), the daily control against the published
figure (`gold_daily_control`), the typical load by weekday and hour
(`gold_hourly_profile`) and the data quality per day (`gold_data_quality`). Each is
written with a Delta `MERGE` that updates a row only when a value changed, so a
second run prints `inserted=0 updated=0`. The command also runs
`OPTIMIZE ... ZORDER BY` on the hourly profile and reads version 0 of the daily
load back with `VERSION AS OF`. Run it on the recorded files, after the Silver
commands above have written `/tmp/lh/silver`:

```sh
python -m swiss_grid_lakehouse.gold --silver /tmp/lh/silver --target /tmp/lh/gold
```

Without `--daily` the control mart is skipped and the command prints
`SKIPPED gold_daily_control reason=no_daily_bronze` and
`GOLD GATE: PASS checks=4 skipped=1`; with `--daily /tmp/lh/daily` it prints
`GOLD GATE: PASS checks=5`. A failed gate merges nothing and exits 2. The same code
runs as `notebooks/gold_marts.py`, a `gold` task in both bundle targets. No run
of that task on a Databricks workspace is recorded yet. Marts, output lines and the tests that pin them:
[Gold marts](docs/walkthrough/gold.md).

Data: Swissgrid AG is the source of the hourly figures (see Data sources and
attribution above). The Swiss Federal Office of Energy (SFOE) dataset page
https://opendata.swiss/en/dataset/energiedashboard-ch-landesverbrauch-und-endverbrauch
is the source of the published daily figures.

The Swissgrid terms are not assumed: check them before reusing the extract.
<!-- gold:end -->

<!-- azure:begin -->
## Azure Databricks target

`databricks.yml` also has a target `azure` with one job, `ch-load-azure`, on
serverless compute. Its six tasks run the whole pipeline in order: ENTSO-E Bronze
and Swissgrid Bronze, then Silver (which prints the hourly and comparison lines),
then the daily reconcile, Gold and the run ledger. The run of 2026-10-02 predates
the Gold and run ledger tasks and ran the first four. The inputs are a recorded slice, the two days 2026-08-30
and 2026-08-31, which the deploy job copies from `tests/fixtures/` into a Unity
Catalog Volume first. The job makes no live API call and holds no secret. The
`deploy-azure` job in `deploy.yml` starts only from a manual workflow run, in the
environment `azure`, and signs in by OpenID Connect federation with a service
principal: no token is stored.

What exists and is checked offline: the bundle schema of the target, a test that
`dev` is unchanged, the gate script of the workflow, and a renderer that turns the
job output into a run summary and exits non-zero if the output holds a host name,
a token or a GUID. <!-- run:begin -->
A run of the whole pipeline on an Azure Databricks workspace in Switzerland North on
2026-10-02 is recorded in [docs/runs/azure_2026-10-02.md](docs/runs/azure_2026-10-02.md):
the first run wrote 48 and 384 Bronze rows and passed both gates, the daily compare
and the three reconciliations; a second run the same day inserted no rows.
<!-- run:end --> Setup, the exact commands, what
only a workspace can prove and the teardown:
[Azure walkthrough](docs/walkthrough/azure.md).
<!-- azure:end -->

## Roadmap

Built (Spark and Delta, run locally and in CI): ingest, Bronze and Silver for both
sources, the hourly comparison, the daily control totals, the four Gold marts in
PySpark and Spark SQL with `OPTIMIZE` and Delta time travel, and the scheduled
`ch-load` job with its run ledger.

Run on a Databricks workspace: the ENTSO-E Bronze and Silver notebooks on
Databricks Free Edition on 2026-09-27 (recorded below); Bronze, Silver, the
comparison and the daily reconcile on Azure Databricks on 2026-10-02 (recorded in
`docs/runs/`).

Next:

1. Run the `gold` task on Databricks Free Edition and on Azure and record both runs.
2. <!-- schedule:begin -->Done in code and checked offline, not yet run on a schedule: the
   `ch-load` job has a daily 06:00 (Europe/Zurich) schedule that reads the ENTSO-E
   API, `max_concurrent_runs: 1`, and a last task that writes one row per run to
   the `pipeline_runs` table; the same ledger runs locally with
   `python -m swiss_grid_lakehouse.runs` and in CI. Next: three hourly runs on
   Databricks Free Edition and one hand run on Azure, recorded in `docs/runs/`.
   Steps: [schedule walkthrough](docs/walkthrough/schedule.md).<!-- schedule:end -->
3. Optional, last: Terraform for an ADLS Gen2 storage target (the `azure` target
   uses managed tables in a workspace catalog and needs no storage account).
   Streaming ingest is a possible extension.

## Databricks Free Edition run

The ENTSO-E Bronze notebook and the Silver notebook (ENTSO-E source only at the
time) ran on Databricks Free Edition; output as printed by the notebooks. The
Swissgrid Bronze and reconcile notebooks were not part of this run; they ran on
Azure Databricks on 2026-10-02 (see the Azure section above):

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
