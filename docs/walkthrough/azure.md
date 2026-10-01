# Run the pipeline on Azure Databricks: a walkthrough

This page describes the `azure` target of the Databricks bundle: what it deploys,
what it reads, how GitHub Actions signs in, what is checked offline, and what only
a workspace can prove. It also gives the command that turns a run into a short
record. The job has not been run on Azure yet; the first run is recorded
separately. Nothing below reports a result.

## 1. The target and the job

`databricks.yml` holds a second target, `azure`, next to `dev`. The `dev` target
and the `ch-load` job are unchanged, so the Free Edition path still works as
before. The `azure` target takes the workspace URL from the bundle variable
`azure_host`, which is set at deploy time and never committed, and deploys one job,
`ch-load-azure`, on serverless compute (no cluster is declared). Its four tasks:

1. `bronze_entsoe` writes the ENTSO-E hourly load to a Bronze table.
2. `bronze_swissgrid` writes the Swissgrid quarter-hour energy series to a Bronze
   table.
3. `silver` runs after both, merges the two sources into the Silver table and
   prints the hourly differences and the `COMPARE` line.
4. `reconcile` runs after `silver` and checks the daily totals against the
   published daily figures; it prints the `DAY` and `RECONCILE` lines.

Tables are Unity Catalog managed tables in a catalog of the workspace, in the
schema `swiss_grid`. No storage account and no Terraform are involved.

## 2. The inputs are a recorded slice

The job reads a recorded slice: the two days 2026-08-30 and 2026-08-31 of every
source, committed in `tests/fixtures/`. The deploy job copies four files into the
Unity Catalog Volume `/Volumes/<catalog>/swiss_grid/raw` before the run:

- `entsoe_ch_load_2026-08-30.xml` (ENTSO-E response, unmodified)
- `swissgrid_ch_energy_2026-08-30.csv` (two Swissgrid series, 192 quarter hours)
- `ogd103_2026-08-30_31.csv` (published daily consumption)
- `ckan_package_show_2026-09-29.json` (the dataset record of that publication)

The job makes no live API call and needs no ENTSO-E token or any other secret. The
reconcile state file is also kept in the Volume. The expected counts are the ones
the offline fixture already produces: 48 rows and 384 rows in the two Bronze
tables, 96 Silver rows, `COMPARE days=2 hours=48 ... PASS`, and three `RECONCILE`
lines with `preliminary=2`.

## 3. Signing in without a stored secret

The workflow job `deploy-azure` in `.github/workflows/deploy.yml` authenticates by
OpenID Connect federation, so no password and no access token is stored in GitHub.

- In the Azure Databricks workspace there is a service principal. It has the
  rights the job needs: use of the catalog, the right to create tables in the
  schema `swiss_grid`, and use of the Volume.
- The service principal has a federation policy that trusts the GitHub Actions
  token issuer for one subject: this repository and the GitHub environment
  `azure`. A run from any other repository or environment is refused.
- The GitHub environment `azure` holds plain variables (not secrets):
  `DATABRICKS_HOST` (the workspace URL), `DATABRICKS_CLIENT_ID` (the application
  ID of the service principal) and, optionally, `DATABRICKS_CATALOG` (default
  `workspace`).
- The job declares `id-token: write` and `DATABRICKS_AUTH_TYPE: github-oidc`. The
  Databricks CLI exchanges the GitHub token for a short-lived Databricks token.

The job is skipped by `scripts/has_azure.sh` when the host or client ID variable is
empty, so a fork without the environment stays green. The job runs only on a manual
workflow run (`workflow_dispatch`), never on a tag push. One dispatch input,
`azure_second_run`, runs the job a second time to record the re-run lines
(`skipped 48 rows`, `merged inserted=0`).

## 4. What is checked offline

- `scripts/check_bundle.py databricks.yml` validates the bundle, including the
  `azure` target, against the CLI schema. It needs no login.
- `tests/test_bundle_azure.py` checks that `dev` is unchanged, that the `azure`
  job has no cluster key, and that no host is written in a committed file.
- `tests/test_has_azure.py` runs the gate script with both variables set and with
  one missing, and checks that no value is printed.
- `tests/test_notebooks_run_line.py` checks that each notebook prints a
  `RUN spark_version=...` line and that the reconcile notebook takes `package` and
  `csv` as widgets.
- `tests/test_run_summary.py` runs the summary renderer on the Free Edition run of
  2026-09-27 and on a file with planted leaks. The renderer exits with status 3
  and names the pattern and line, without echoing it, when a line holds a workspace
  ID, a token, a workspace host name or a GUID.

## 5. What only a workspace proves

- the token exchange between GitHub and Azure Databricks;
- the copy of the four files into the Volume;
- the start of serverless compute in Switzerland North;
- the package install in the notebooks;
- the row counts of a real run on Azure (48 and 384 in Bronze, 96 in Silver).

Until a run is recorded, these five points are open.

## 6. Run it and record it

From the Actions tab, run the workflow `Deploy` by hand, choose the branch and set
`azure_second_run` if the re-run lines are wanted. When it ends, download the
output and render the record:

```sh
gh run download <run id> -n azure-run-output
python scripts/render_run_summary.py run1.txt --date <YYYY-MM-DD> --region "Switzerland North" > docs/runs/azure_<YYYY-MM-DD>.md
```

The artifact holds `run1.txt` (and `run2.txt` when the second run was requested).
The record is `docs/runs/azure_<YYYY-MM-DD>.md`; it is added in a separate change
after a run exists, together with a sentence in the README.

## 7. Teardown

The job creates no compute that outlives the run. To remove everything, delete the
Azure resource group that holds the workspace; the workspace, the managed tables
and the Volume go with it. Then remove the GitHub environment `azure`.

## Design decisions

- **A recorded slice, not live sources.** The counts of a run can then be compared
  with the counts of the offline fixture, and the job needs no secret.
- **Federation instead of a token.** A personal access token is a secret that
  expires and has to be rotated; the federation policy binds the sign-in to one
  repository and one environment.
- **A new target, not an edit of `dev`.** The Free Edition path stays reproducible.
- **Managed tables.** They need no storage account and no credential; an external
  ADLS Gen2 location stays on the roadmap.
