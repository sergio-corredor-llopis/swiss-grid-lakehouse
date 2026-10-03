# Gold marts

This page follows the layer after Silver: four small tables that answer one
question each, built with PySpark and Spark SQL and written to Delta. The code is
in `src/swiss_grid_lakehouse/gold/`; the command is

```sh
python -m swiss_grid_lakehouse.gold --silver /tmp/lh/silver --target /tmp/lh/gold
```

It reads the Silver table (the two Silver commands of the README write
`/tmp/lh/silver`) and needs no token. Every output line shown below is the form
the code prints; the tests named in each section pin it. This page records no run
on Databricks Free Edition or on Azure: those runs are recorded separately.

## The four marts

Each mart is a module with one function, `build(spark, silver)`. It registers the
Silver frame as a temporary view and returns the result of one `spark.sql` query.
A builder never reads a table: the caller (the command or the notebook) does.

| mart | table | grain | key | question |
|---|---|---|---|---|
| g1 | `gold_daily_load` | source, Swiss local day | `source`, `local_date` | How many GWh per day, and which days have 23 or 25 hours? |
| g2 | `gold_daily_control` | source, series, local day | `source`, `series`, `local_date` | Does the pipeline's daily total match the published figure: PASS, PRELIMINARY, FAIL or INCOMPLETE? |
| g3 | `gold_hourly_profile` | source, weekday, local hour | `source`, `weekday`, `hour_local` | What is the typical load in MW by weekday and hour? |
| g4 | `gold_data_quality` | source, local day | `source`, `local_date` | Which days are short, re-pulled or hold rejected rows? |

On the recorded two-day extract (Sunday 2026-08-30 and Monday 2026-08-31, two
sources) the row counts are 4 for g1, 96 for g3 and 4 for g4; g2 holds 6 rows
(three series times two days) when the daily Bronze table is given. These counts
are asserted in `tests/test_gold_cli.py`.

- g1 columns: `hours`, `expected_hours` (23, 24 or 25), `incomplete`, `day_kind`
  (`short`, `normal`, `long`), `gwh_total`, `gwh_enduser`, `gwh_difference`. Its
  numbers equal `silver.daily_rollup.rollup_rows` to 1e-9
  (`tests/test_gold_daily_load.py`).
- g2 follows `reconcile.reconcile`: the latest batch of a day wins, the tolerance
  is `TIGHT_PCT` (6.5) for the two Swissgrid series and `WIDE_PCT` (5.0) for
  ENTSO-E, and a figure at most `PRELIM_DAYS` (30) old is PRELIMINARY. The SQL is
  generated from those constants, so the two cannot drift apart
  (`tests/test_gold_daily_control.py`).
- g3 numbers the weekday 1 (Monday) to 7 (Sunday) and the hour 0 to 23 from the
  local timestamp.
- g4 adds `missing_hours`, `batches` (distinct batch ids of the day), `repulled`
  (more than one batch) and `rejected_rows`.

## The `gwh_difference` caveat

`gwh_difference` is the sum of Silver `load_difference_mw` divided by 1000. That
column holds grid losses, the power plants' own use and pumps together. It is not
the pumped-storage figure, and pumped storage alone is not a mart: Silver does not
carry it. The column is NULL when the day is incomplete or an hour has no value.

## Why a rerun prints `inserted=0 updated=0`

`merge_mart(spark, df, target, key, change_cols)` in `gold/merge.py` does two
things:

1. On the first call the table does not exist, so it writes the frame as a new
   Delta table. Version 0 already holds the rows and the call returns
   `(rows, 0)`.
2. On later calls it runs a Delta `MERGE` on the key. A matched row is updated
   only when at least one change column differs, compared null-safe with
   `NOT (t.c <=> s.c)`. An unmatched row is inserted. Nothing is deleted. The two
   counts are read from the `MERGE` metrics in `DESCRIBE HISTORY`.

Equal rows therefore match and differ in no column, so nothing is written:

```text
MART g1 rows=4 inserted=4 updated=0      <- first run
MART g1 rows=4 inserted=0 updated=0      <- second run
```

The same holds for g3 and g4 (`test_second_run_changes_nothing` in
`tests/test_gold_cli.py`, and `tests/test_gold_merge.py` for a changed and an
unchanged non-key column).

## OPTIMIZE ... ZORDER BY

g3 is the largest mart (96 rows on the recorded days), so it is the one the
command compacts:

```text
OPTIMIZE table=g3 files_removed=R files_added=A
```

`gold/maintain.py` runs `OPTIMIZE <table> ZORDER BY (source, weekday)` and reads
`numRemovedFiles` and `numAddedFiles` from `DESCRIBE HISTORY`. After three appends
the test finds at least two files removed and one added
(`test_optimize_compacts_three_appends`). If a runtime refuses the statement, the
function does not raise; it returns a short reason and the line becomes

```text
OPTIMIZE table=g3 refused reason=<reason>
```

(`test_refused_optimize_is_reported_not_raised`).

## Time travel with `VERSION AS OF`

`version_query(spark, target, version)` runs
`SELECT * FROM <table> VERSION AS OF <v>` through `spark.sql`. The command reads
version 0 of g1 back and prints how many rows it held then and now:

```text
VERSION table=g1 v=0 rows=4 now=4
```

## The late hour

An hour can arrive after the day was first loaded. In
`test_the_late_hour_updates_one_row_and_the_old_version_stays_incomplete` the
first run has 23 of the 24 hours of 2026-08-30 for one source, so g1 holds
`incomplete = true`, with the partial sum. A second batch brings the missing
hour and the run prints

```text
MART g1 rows=1 inserted=0 updated=1
```

The row now has `hours = 24` and `incomplete = false`, while
`VERSION AS OF 0` still returns the row as incomplete.

## An incomplete day is reported, never scaled

When a day has fewer hours than its calendar length, g1 shows the partial sum with
`incomplete = true`, and g2 shows status INCOMPLETE with the partial figure.
Scaling it by 24 divided by the hours present would invent hours that were never
measured. `test_incomplete_day_is_reported_not_scaled` checks that the total is
below that scaled value and that `gwh_difference` is NULL. `gwh_enduser` is also
NULL when any end-user hour of the day is missing.

## Days of 23 and 25 hours

`expected_hours` is the gap between two Europe/Zurich local midnights, measured in
UTC with `convert_timezone`, so it does not depend on the session time zone.
`tests/test_gold_daily_load.py` pins two days:

- 2026-03-29, the clocks go forward: 23 hours, `day_kind = short`, complete.
- 2026-10-25, the clocks go back: 25 hours, `day_kind = long`, complete. The
  repeated wall-clock hour is its own row because rows are told apart by `ts_utc`.

In g3 the repeated hour counts towards the same `hour_local`, and `n_days` stays
the number of distinct dates (`tests/test_gold_hourly_profile.py`).

## The gate and why g2 can be skipped

`run_gold_gate` runs before anything is merged:

1. g1 `gwh_total` per source equals the Silver sum of `load_mw` / 1000 (1e-6).
2. g2 has one row per series and day of its input; skipped when g2 is None.
3. No NULL in the key columns of any mart present.
4. g3 has `n_days` of at least 1 on every row.
5. g4 `hours` equal g1 `hours` per source and day.

The summary is `GOLD GATE: PASS checks=5`. A failed gate prints
`GOLD GATE: FAIL <check>=<n> ...`, merges nothing and exits 2; a planted row with
a NULL date does this in `test_a_planted_row_with_a_null_date_fails_the_gate_and_merges_nothing`.

g2 needs the daily Bronze table (`ch_consumption_daily_bronze`). The `dev` job in
`databricks.yml`, which targets Databricks Free Edition, has Bronze, Silver and
Gold tasks and no daily task, so that table does not exist there. Without
`--daily` (or without the table) g2 is None and the command prints

```text
SKIPPED gold_daily_control reason=no_daily_bronze
GOLD GATE: PASS checks=4 skipped=1
```

`checks=4 skipped=1` reads: four checks ran, one (check 2) was left out because its
mart is absent. It is not a failure. The `azure` job runs `reconcile` first and
gold after it, so the same notebook (`notebooks/gold_marts.py`) is written to
print `checks=5` there. Only the job's `depends_on` differs between the two
targets; the notebook is identical.

## Design decisions

- One `build` per mart over a temporary view keeps each mart a single readable
  query that a test can compare with the older Python reference
  (`rollup_rows`, `reconcile`).
- Changing only on a differing change column keeps a rerun silent, so the counts
  mean something.
- The gate runs on frames before any merge, so a bad batch leaves the Gold
  tables at their version.
- Pumped storage is left out because Silver has no such series.
