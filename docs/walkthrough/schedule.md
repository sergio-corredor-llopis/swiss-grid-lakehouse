# Schedule and run history: a walkthrough

This page shows how the Databricks job runs on a timer and how every run leaves
one row in a ledger table, `pipeline_runs`. It builds on the [deploy
walkthrough](deploy.md). The offline parts need only Python and Java; the
scheduled parts need a Databricks workspace and your own ENTSO-E token
(request one from the ENTSO-E Transparency Platform; the RESTful API guide
describes the process).

What is established and what is not, stated once: the schedule, the run
limit, the ledger task and the local runner are checked offline by tests and by
CI. A scheduled run on a workspace is a hand-driven procedure, described in
steps H0 to H3 below. No scheduled run is recorded in this repository yet; the
only recorded workspace run is the Azure one in [docs/runs](../runs/azure_2026-10-02.md).

## 1. What the bundle declares

In `databricks.yml`:

- `schedule_cron`, a variable. Its default is the Quartz expression
  `0 0 6 * * ?`: every day at 06:00, in the time zone `Europe/Zurich` that both
  jobs declare. `0 0 * * * ?` is hourly.
- `source`, a variable whose default is now `api`. The scheduled job reads the
  last two days from the ENTSO-E API with the token in the secret scope
  `entsoe`, key `token`. CI and the tests keep using the recorded slice of
  2026-08-30 under `tests/fixtures/`.
- `max_concurrent_runs: 1` on both jobs, so an hourly run never overlaps the
  next one.
- the Free Edition job `ch-load` is unpaused; the Azure job `ch-load-azure`
  carries the same cron but stays `PAUSED`, and reads the recorded slice.
- a last task, `run_history`, with `run_if: ALL_DONE` in both jobs. It runs after
  success and after failure, so a failed night still writes its row.

The offline test is `tests/test_bundle_schedule.py`: it reads the file as YAML and
fails if the cron default, the source, the time zone, the run limit, the pause
state of the Azure job or the `ALL_DONE` condition changes.

## 2. The ledger

`pipeline_runs` holds one row per platform run:

| column | meaning |
| --- | --- |
| `run_id` | the job run id (`{{job.run_id}}`); the local runner uses a uuid. The MERGE key |
| `started_at`, `recorded_at` | start of the run, time the row was written |
| `target`, `source` | `ch-load` or `ch-load-azure`; `api` or `recorded` |
| `bronze_rows` | rows written to Bronze |
| `silver_inserted`, `silver_updated` | the Silver MERGE counts |
| `gate` | PASS or FAIL, with the checks |
| `compare`, `reconcile_diff` | the cross-source result and the reconcile difference |
| `gold_inserted`, `gold_updated` | the Gold MERGE counts |
| `status` | SUCCESS, PARTIAL or FAILED |

The status is computed from these figures, never copied from the state of a task:
SUCCESS needs a passing gate, every figure present and the reconcile difference
inside its tolerance. A failed gate gives FAILED. A figure that was never
recorded stays NULL and gives PARTIAL. The function is `compute_status` in
`src/swiss_grid_lakehouse/runs/history.py`.

The figures travel between tasks with `dbutils.jobs.taskValues`: each notebook
sets its figures in one line, and `notebooks/run_history.py` reads them and calls
`record_run`, which merges the row. Recording the same run id twice leaves one row.

## 3. Run it without a workspace

```sh
pip install -e ".[dev,spark]"
python -m swiss_grid_lakehouse.runs --times 2 --lake /tmp/lh_runs
```

The runner executes the same stages in the same order, stops at the first failing
stage, and calls the same `record_run`. After each run it prints a line of the
form `RUN n=1 id=<uuid> target=local status=SUCCESS`, and at the end
`HISTORY runs=2 success=2 failed=0`. The first run inserts 96 Silver rows (48 per
source); the second prints `inserted=0 updated=0` for Silver and for Gold, and the
ledger then holds two rows. CI runs exactly this command in the step "Pipeline
twice, run history" and greps those lines.

If a stage fails, the row is still written: a gate failure in Silver skips Gold,
gives the status FAILED and a non-zero exit code. `tests/test_run_history.py`
covers that case, the double record of one run id, and the status rules.

## 4. Put it on a schedule: the hand steps

These steps are done by hand because they touch a workspace and a secret. Each one
says what to check.

**H0. A secret scope for the token.** Create the scope once on the workspace:

```sh
databricks secrets create-scope entsoe
```

On Databricks Free Edition this scope exists on my workspace; on yours it is the
first thing to create. Check: `databricks secrets list-scopes` shows `entsoe`.

**H1. Store the token.** Put your ENTSO-E token under the key `token`. The command
asks for the value or reads it from standard input; it does not belong in a file,
a shell history or a commit:

```sh
databricks secrets put-secret entsoe token
```

Check: `databricks secrets list-secrets entsoe` lists the key `token` (never the
value).

**H2. Deploy with an hourly cron for one afternoon.** To see several runs in a short
time, override the cron on the command line instead of editing the file:

```sh
databricks bundle validate -t dev
databricks bundle deploy -t dev --var schedule_cron="0 0 * * * ?"
```

Check: the Jobs page shows `ch-load` with an active schedule (development mode
pauses schedules unless the file sets `UNPAUSED`, which it does). After three hourly
runs, each has a row: `SELECT run_id, started_at, source, bronze_rows, status FROM
pipeline_runs ORDER BY started_at`. A run over data that has not changed inserts
nothing; the Silver counts then read `inserted=0 updated=0`. Record the outputs under `docs/runs/` with
`scripts/render_run_summary.py`, which keeps the `RUN` and `HISTORY` lines and
refuses output that holds a host name, a token or a GUID.

**H3. Back to the daily schedule.** Deploy again without the override:

```sh
databricks bundle deploy -t dev
```

Check: the Jobs page shows the schedule `0 0 6 * * ?`, Europe/Zurich. For Azure,
leave the schedule `PAUSED` and start one run by hand:

```sh
databricks bundle deploy -t azure
databricks bundle run -t azure ch_load_azure
```

## Design decisions

- **The status comes from data.** A scheduler reports a task as succeeded when the
  notebook returned, even if the gate rejected the batch or a figure is missing.
  The ledger answers a different question: did the data arrive and pass its checks.
- **One row per run, keyed by run id.** A retry of the ledger task, or a second
  recording of the same run, updates the row; it never adds one.
- **`run_if: ALL_DONE` on the last task.** The runs that matter most are the failed
  ones, and a ledger that skips them is not a history.
- **`max_concurrent_runs: 1`.** An hourly run that overlaps the next would write the
  same Bronze batch twice at the same time.
- **The local runner shares `record_run`.** CI proves the ledger logic without a
  workspace; what only a workspace can prove is listed in the steps above.
- **Actions is not the scheduler.** A cron in GitHub Actions could start the job as a
  fallback, but the schedule belongs to the job, next to the run it controls, so no
  such file exists.
- **Recorded data in CI, the API on the schedule.** Tests must give the same answer
  on every day; the schedule exists to see new data.

Data source: ENTSO-E Transparency Platform (RESTful API, actual total load).
