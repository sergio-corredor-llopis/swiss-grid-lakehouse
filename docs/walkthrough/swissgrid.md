# Swissgrid and the comparison

This page follows the second source of the repository, from the recorded file to
the comparison with ENTSO-E. Everything below can be repeated from the two
fixtures in `tests/fixtures/`; no token is needed.

## The two datasets

- Swissgrid AG, "Energieuebersicht Schweiz 2026 / Aggregated energy data of the
  Swiss control block", published on swissgrid.ch
  (English site: Operation > Grid data > Generation) as one workbook per year. The sheet `Zeitreihen0h15` has 64 series, one row per
  quarter hour. Row 1 holds each series name as two lines (German, a line break,
  English), row 2 the unit (the units include `kWh` and `Euro/MWh`), and column A
  the timestamp.
- ENTSO-E Transparency Platform, Actual Total Load, Switzerland
  (`10YCH-SWISSGRIDZ`), hourly, in MW.

The fixtures are the two local days 2026-08-30 and 2026-08-31 from each
publisher: real data, recorded once. Swissgrid's file has 192 quarter hours per
series; the repository keeps two series of it.

## From quarter-hour kWh to hourly MW

The reader selects two series by their German name and validates the unit of
those two only (`kWh`):

- `Summe verbrauchte Energie Regelblock Schweiz` (total consumption), which the
  comparison uses;
- `Summe endverbrauchte Energie Regelblock Schweiz` (end-user consumption), kept
  as a second column.

For an hour with its four quarter hours E1..E4 (kWh):

```
load_mw(hour) = (E1 + E2 + E3 + E4) [kWh] / 1000 [kWh per MWh] / 1 [h]
```

That is the mean power over the hour in MW. An hour with fewer than four slots is
dropped, not scaled, and the quality gate then reports the missing hour. The
division by 1000 is one entry in the unit map used by the Silver layer, so every
conversion sits in one place.

## START labels

The timestamp in column A is the START of the 15-minute interval, in Swiss local
time: the 2026 sheet begins at 01.01.2026 00:00 and its last row of a day is
23:45. So the row `30.08.2026 00:00` covers 00:00 to 00:15. The Silver layer
turns the label into a UTC instant of the interval start and keys the hour by the
UTC start of the hour. In the repeated hour of the October clock change the same
local label occurs twice; the order in the sheet decides, so that day has 25
hours.

## Swissgrid's definitions

The sheet `Uebersicht` of the workbook describes each series in German and in
English. The text is quoted as it stands in the workbook.

`Summe verbrauchte Energie Regelblock Schweiz` (total consumption), German:

> Die verbrauchte Energie Regelblock Schweiz ist das Total aller Lastgänge der Bilanzgruppen in Viertelstundenauflösung. Diese werden von den Verteilnetzbetreibern an Swissgrid gemeldet. Die Summe beinhaltet sämtliche in der Schweiz aus dem Übertragungsnetz und den Verteilnetzen bezogene elektrische Energie. (Inklusive Verluste, Pumpenergie bei Pumpspeicherkraftwerken und  Eigenbedarf von Kraftwerken)

English:

> The total of the consumed energy in the control block Switzerland. The aggregations of the consumption sequences for the balancing group are sent from the distribution network operators to Swissgrid. The sum contains all the energy consumed in the transmission and distribution grids. Included are grid losses, energy consumed for a power plant’s own requirements and to drive the pumps in pumped storage hydro power plant.

`Summe endverbrauchte Energie Regelblock Schweiz` (end-user consumption), German:

> Die endverbrauchte Energie oder gemäss VSE Branchendokumente auch «Bruttolastgangsumme des eigenen Netz» (BLS/EN), ist die von den Endverbrauchern in allen Netzebenen bezogene Energie in Viertelstundenauflösung.  Diese Summe wird aus den BLS/EN der VNB im Regelblock Schweiz durch Swissgrid gebildet. (Nicht darin enthalten ist die Energie für Pumpen in Pumpspeicherkraftwerken, der Eigenbedarf von Kraftwerken sowie die Netzverluste)

English:

> The total of the end- user energy consumption includes all aggregates with the end-user consumptions for the Swiss Control block delivered by the distribution network operators to Swissgrid. Not included are grid losses or energy consumed for  power plant’s own requirements or to drive the pumps in pumped storage hydro power plant.

So Swissgrid's total includes the energy used to drive the pumps of pumped
storage plants, grid losses and power plants' own requirements; its end-user
figure does not.

## Run the comparison

The six commands of the README end with:

```sh
python -m swiss_grid_lakehouse.compare --silver /tmp/lh/silver
```

The command reads the Silver table and prints:

1. A table by hour of day (Europe/Zurich, 0 to 23), pooled over the days present:
   `n` hours and the minimum, median and maximum of
   `pct = 100 * (swissgrid - entsoe) / entsoe`, once for Swissgrid total
   consumption and once for end-user consumption. A positive figure means
   Swissgrid is higher than ENTSO-E.
2. Two `HOURLY` lines with the same statistics over all hours.
3. One `DAILY` line per complete local day: the two daily sums in MWh and the gap
   in percent.
4. The last line, `COMPARE days=.. hours=.. daily_max_abs_pct=.. tol=.. PASS` or
   `FAIL`. Only the daily gaps decide it: every complete day must be within
   `--tol-pct` (default 3). Exit code 0 is PASS, 3 is a failed gate or no
   complete day in both sources, 2 is a usage or input error. The hourly figures
   are reported and never decide the exit code.

The default tolerance is the larger of the two recorded daily gaps, 2.47 %,
rounded up to the next whole percent.

## Measured differences

Output of the command on the recorded days (48 hours). With two days there are
two values per hour of day, so the median of an hour is the mean of the two.
Values are percent of the ENTSO-E value.

| Hour (local) | Total min | Total median | Total max | End-user min | End-user median | End-user max |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | -9.36 | -4.92 | -0.48 | -19.13 | -16.85 | -14.56 |
| 1 | -16.38 | -12.06 | -7.73 | -31.05 | -24.28 | -17.50 |
| 2 | -23.21 | -18.26 | -13.30 | -31.59 | -30.72 | -29.86 |
| 3 | -8.59 | -3.67 | 1.24 | -27.05 | -22.39 | -17.73 |
| 4 | -22.08 | -8.49 | 5.09 | -38.19 | -26.81 | -15.44 |
| 5 | -13.54 | -12.91 | -12.27 | -30.63 | -28.19 | -25.74 |
| 6 | -9.33 | -6.96 | -4.59 | -28.00 | -21.20 | -14.40 |
| 7 | -3.25 | 0.00 | 3.26 | -29.28 | -17.69 | -6.11 |
| 8 | 15.64 | 18.65 | 21.67 | -25.61 | -7.28 | 11.05 |
| 9 | 7.20 | 15.14 | 23.07 | -26.58 | -15.47 | -4.35 |
| 10 | 11.78 | 24.02 | 36.26 | -17.67 | -13.32 | -8.96 |
| 11 | 3.77 | 6.63 | 9.48 | -36.49 | -25.39 | -14.28 |
| 12 | 8.56 | 15.74 | 22.92 | -33.63 | -21.75 | -9.86 |
| 13 | 5.80 | 6.13 | 6.46 | -36.76 | -30.76 | -24.76 |
| 14 | 15.14 | 20.69 | 26.24 | -33.23 | -24.13 | -15.03 |
| 15 | 25.94 | 28.13 | 30.33 | -23.88 | -18.15 | -12.42 |
| 16 | -2.93 | 8.67 | 20.27 | -26.99 | -22.63 | -18.27 |
| 17 | -12.52 | -8.66 | -4.80 | -24.33 | -22.54 | -20.74 |
| 18 | -8.22 | -3.54 | 1.13 | -17.30 | -12.58 | -7.86 |
| 19 | -3.37 | -2.83 | -2.28 | -13.15 | -11.93 | -10.71 |
| 20 | -7.43 | -3.79 | -0.15 | -15.34 | -12.68 | -10.01 |
| 21 | -2.20 | 3.07 | 8.35 | -12.37 | -6.80 | -1.22 |
| 22 | -6.13 | -4.59 | -3.05 | -15.96 | -13.88 | -11.81 |
| 23 | -9.53 | -6.59 | -3.66 | -17.84 | -15.85 | -13.86 |

Over all 48 hours (`HOURLY` lines):

| Swissgrid series | Min | Median | Max | Mean of absolute values |
|---|---:|---:|---:|---:|
| Total consumption | -23.21 | -1.34 | 36.26 | 10.83 |
| End-user consumption | -38.19 | -17.70 | 11.05 | 19.76 |

The mean of absolute values is not printed by the command; it is the mean of
`abs(pct)` over the same 48 hours, computed from the two fixtures.

Daily totals (`DAILY` lines):

| Local day | ENTSO-E (MWh) | Swissgrid total (MWh) | Gap |
|---|---:|---:|---:|
| 2026-08-30 | 146693.6 | 149466.5 | +1.89 % |
| 2026-08-31 | 156390.3 | 160252.6 | +2.47 % |

Reading it: the daily totals agree within about 2 %, the hourly values differ by
about 10 % on average and by up to 36 %. The end-user series is clearly further
from ENTSO-E than the total, which is why the comparison uses the total. The
hourly gap changes sign during the day: by median, Swissgrid total consumption is
lower than ENTSO-E in most night and evening hours and higher in the hours 8 to 16
(local) in this sample.

## What is not known

The cause of the hourly gap is not established. Two days are a small sample, and
the two publishers describe the quantity in their own words: Swissgrid's
definition is quoted above; how ENTSO-E's total load treats the energy used by
the pumps of pumped storage plants is UNKNOWN here, because I have not checked it
against ENTSO-E's own definition.

No separate hourly pump-consumption series was found in the sources checked (the
ENTSO-E generation-per-type response, the Energy-Charts public power API for
Switzerland, and the columns of the Swissgrid workbook), so the comparison does
not adjust for pumped storage.
