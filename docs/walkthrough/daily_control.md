# Daily control totals

This page follows the third source of the repository: the daily national
consumption that the Swiss Federal Office of Energy (SFOE) publishes from
Swissgrid data. The pipeline uses it as a control total. Two commands do the work,
and both can be repeated from the recorded files in `tests/fixtures/`; no token is
needed.

## What the step does

1. `python -m swiss_grid_lakehouse.daily` reads the dataset record on
   opendata.swiss first and takes its `metadata_modified` stamp. If the stamp equals
   the one stored in the state file, it prints `REGISTRY unchanged` and fetches
   nothing. Otherwise it fetches the CSV (columns `Datum`,
   `Landesverbrauch_GWh`, `Endverbrauch_GWh`, one row per day, whole GWh) and appends
   one batch to the Delta table `ch_consumption_daily_bronze`, then prints
   `REGISTRY changed fetched=1 rows=<n> batch=<12 hex>`. The batch id is a hash of the
   CSV bytes, so a batch that is already in the table adds 0 rows. Every request
   carries a named `User-Agent`: the registry answered 403 to the default curl one.
2. `python -m swiss_grid_lakehouse.reconcile` rolls each Silver source up to
   Europe/Zurich days (23 hours on 2026-03-29, 25 hours on 2026-10-25) and sets the
   sum of each day beside the published figure. A day with fewer hours than its
   calendar length is `incomplete`: it is reported and never scaled up.

It prints one `DAY` line per day and then three `RECONCILE` lines:

| line | pipeline series | published series | tolerance |
|---|---|---|---|
| `source=swissgrid series=total` | Swissgrid total consumed energy | Landesverbrauch | tight |
| `source=swissgrid series=enduser` | Swissgrid end-user consumption | Endverbrauch | tight |
| `source=entsoe series=total` | ENTSO-E actual total load | Landesverbrauch | wide |

The two Swissgrid lines are the control totals proper: the published series and the
Swissgrid workbook share an origin, so they should agree to rounding. The ENTSO-E
line compares two different publishers and gets the wide tolerance.

Status of a day: `PASS`, `FAIL`, `INCOMPLETE` or `PRELIMINARY`. Exit code 0 unless
a final day is beyond tolerance, the two sides share no day, or a shared day is
incomplete; then it is 3. A preliminary day is printed and counted in
`preliminary=<p>`; it never sets the exit code.

## The two recorded days

Local days 2026-08-30 and 2026-08-31, fetched on 2026-09-29 (GWh):

| local day | ENTSO-E | Swissgrid workbook total consumed | published Landesverbrauch | published Endverbrauch | Swissgrid workbook end-user |
|---|---|---|---|---|---|
| 2026-08-30 | 146.69 | 149.47 | 149 | 110 | 109.73 |
| 2026-08-31 | 156.39 | 160.25 | 149 | 125 | 134.76 |
| sum | 303.08 | 309.72 | 298 | 235 | 244.49 |

- 2026-08-30: the workbook and the published figures agree to rounding
  (149.47 against 149, 109.73 against 110; -0.24 % and +0.31 %).
- 2026-08-31: the published figures are 149 and 125 against 160.25 and 134.76 in
  the workbook (7.55 % and 7.81 % apart). The dataset description says the newest
  values are first shown from figures that distribution grid operators report to
  Swissgrid, that these may be incomplete or faulty, and that they are corrected
  afterwards (German original: "Diese Daten können unvollständig oder fehlerhaft
  sein und werden nachkorrigiert"). I read 2026-08-31 as a preliminary figure; the
  description does not say when the correction happens.
- ENTSO-E is 1.86 % and 2.41 % below the workbook total on the two days.

## Measurement behind the tolerances and the preliminary window

I set the numbers below from the per-day difference between the 2026 Swissgrid
workbook and the published CSV over 243 local days (2026-01-01 to 2026-08-31), with
the registry stamp `metadata_modified` = 2026-09-30T02:56:07. The workbook is
downloaded for the measurement and is not committed. The age of a day is the
stamp's date minus the day. Absolute differences in percent:

Swissgrid total consumed against Landesverbrauch

| age of the day (days) | days | median | p95 | max | days above 1 % |
|---|---|---|---|---|---|
| 0-30 | 1 | 7.55 | 7.55 | 7.55 | 1 |
| 31-60 | 30 | 0.30 | 0.88 | 1.14 | 1 |
| 61-120 | 60 | 0.27 | 1.11 | 2.12 | 5 |
| older | 152 | 0.26 | 0.90 | 3.19 | 6 |

Swissgrid end-user consumption against Endverbrauch

| age of the day (days) | days | median | p95 | max | days above 1 % |
|---|---|---|---|---|---|
| 0-30 | 1 | 7.81 | 7.81 | 7.81 | 1 |
| 31-60 | 30 | 0.29 | 0.75 | 1.01 | 1 |
| 61-120 | 60 | 0.33 | 0.85 | 1.08 | 1 |
| older | 152 | 0.21 | 1.00 | 4.08 | 8 |

How the three constants were set (they are in `reconcile/__init__.py`):

- `PRELIM_DAYS = 30`. With this split, the 242 days older than 30 days have a
  95th percentile of 0.99 % (total) and 0.87 % (end-user); the one younger day is
  at 7.55 % and 7.81 %. The 60-day split leaves 212 final days with a 95th
  percentile of 1.00 % and 0.91 %, and does not change the maximum (3.19 % and
  4.08 %), so the measurement gives no reason to lengthen the window. It rests on
  one young day: it is thin evidence.
- `TIGHT_PCT = 6.5`. The largest difference of any day older than 30 days is
  4.08 %; the tolerance sits above it and below the 7.55 % of the young day. The
  maximum is well above the 95th percentile, so a handful of old days are
  revised by more than 1 %; the tolerance covers them rather than explaining them.
- `WIDE_PCT = 5.0`. ENTSO-E is 2.41 % from the workbook at most on the two
  recorded days; twice that is 4.82 %, rounded up to 5.0. This rests on two days
  only, because the ENTSO-E slice in the repository has two days.

`reconcile` counts the age of a day from the end of that day, so at that stamp
2026-08-30 is 30 days old and 2026-08-31 is 29, and both are preliminary. The
measurement above counted from the day itself (31 and 30), which puts 2026-08-30
just outside the window; the recorded slice therefore has two preliminary days in
the command and one young day in the table.

Whether a day is preliminary depends only on the stamp of the batch, never on the
current date or on when the batch was pulled: with the clock the same rows would
otherwise turn final after 30 days and the same commit would flip from PASS to
FAIL in CI. A test runs the same rows with the clock patched to 2030 and gets the
same result. A second test moves the stamp to 2027-01-15: both days are then final,
2026-08-31 is beyond tolerance and the command exits 3.

## What is not established

- The definition of ENTSO-E actual total load is not established. I did not fetch
  the ENTSO-E definition (the regulation text that defines it did not load), so
  whether that series counts the consumption of storage pumps is not known.
  Consequence: the gap between ENTSO-E and the Swissgrid workbook (1.86 % and
  2.41 % on the two days) is unexplained. The wide tolerance is a measured
  allowance, not an explanation.
- The published Landesverbrauch includes storage pumps (source: the dataset
  description on opendata.swiss: "Landesverbrauch inklusive des Verbrauchs der
  Speicherpumpen"). That it matches the workbook `verbrauchte` series is what the
  data show on the recorded days (0.31 % on 2026-08-30); I did not find a
  statement of it in the description.
- When and how much the published figures are corrected is not established beyond
  the sentence quoted above; the 30-day window is an observation of one young day.
- The Energie-Dashboard API serves the same rows and states no terms. It is a
  possible fallback and is not built.

## Source and attribution

Data: Swiss Federal Office of Energy SFOE (data from Swissgrid), "energiedashboard.ch -
National and final consumption",
https://opendata.swiss/en/dataset/energiedashboard-ch-landesverbrauch-und-endverbrauch.
The dataset is published under the opendata.swiss terms of use "open use, the source
must be given" (`terms_by_ask`); the two committed fixtures,
`tests/fixtures/ogd103_2026-08-30_31.csv` and
`tests/fixtures/ckan_package_show_2026-09-29.json`, are a two-day slice and a
trimmed copy of the registry record.
