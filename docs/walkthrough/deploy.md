# Deploy the Databricks bundle: a walkthrough

This page shows how the Bronze and Silver notebooks become one Databricks job
that is deployed from a reviewed file, and how a version tag deploys it from
GitHub Actions. The earlier pages ([first ingest](first_ingest.md),
[Bronze on Delta](bronze_delta.md), [Silver with MERGE](silver_merge.md)) run
locally; this one needs a Databricks workspace. No ENTSO-E token is needed for
the sample file.

## 1. What the bundle declares

`databricks.yml` at the repository root declares a Databricks Asset Bundle
named `swiss-grid-lakehouse`:

- three variables: `catalog` (default `workspace`), `schema` (default
  `swiss_grid`) and `source` (a Volume path to an XML file, or `api`);
- one job, `ch-load`, with two tasks on serverless compute: `bronze` runs
  `notebooks/bronze_entsoe_ch_load.py`, then `silver` runs
  `notebooks/silver_ch_load.py` after it. The variables are passed to the
  notebooks as parameters. This job has no Swissgrid Bronze task, so it loads the
  ENTSO-E source only;
- one target, `dev`, in development mode and the default target;
- a second target, `azure`, with its own job that covers both sources and the
  daily reconcile, described in [Azure Databricks](azure.md).

The job that was started by hand in the Databricks UI is now this file.

## 2. Prerequisites

1. The Databricks CLI, version 1.18.0 or later, on your `PATH`.
2. A schema in your workspace. The defaults are catalog `workspace` and schema
   `swiss_grid`:

   ```sh
   databricks schemas create swiss_grid workspace
   ```

3. A Volume named `raw` in that schema:

   ```sh
   databricks volumes create workspace swiss_grid raw MANAGED
   ```

4. The recorded ENTSO-E XML in the Volume, under the file name the default of
   the `source` variable expects:

   ```sh
   databricks fs cp tests/fixtures/entsoe_ch_load_2026-08-30.xml \
     dbfs:/Volumes/workspace/swiss_grid/raw/entsoe_ch_load_sample.xml
   ```

Before any login, the bundle can be checked against the CLI's own JSON schema.
This needs the CLI but no workspace and no token:

```sh
pip install pyyaml jsonschema
python scripts/check_bundle.py
```

It prints `BUNDLE schema ok`, and exits with status 1 when the file has schema
errors.

## 3. Validate, deploy, run

Log in once (`databricks auth login --host <workspace url>`), then:

```sh
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev ch_load
```

The run prints the output of both notebooks. The first run of a fresh table
shows:

```
wrote 48 rows to workspace.swiss_grid.entsoe_ch_load_bronze batch_id=...
GATE: PASS checks=6
merged inserted=48 updated=0 rows=48
```

Running the job again skips the Bronze rows it has already seen and merges
nothing (`skipped 48 rows ...`, then `merged inserted=0 updated=0 rows=48`).

## 4. Deploy from GitHub Actions

This part needs a workspace that issues personal access tokens. It is a
one-time setup.

1. Create a token that lasts 90 days:

   ```sh
   databricks tokens create --lifetime-seconds 7776000 --comment github-deploy
   ```

2. In the GitHub repository, open Settings, then Environments, and create an
   environment named `dev`.
3. Under Settings, then Secrets and variables, then Actions, add two
   repository secrets: `DATABRICKS_HOST` (the workspace URL) and
   `DATABRICKS_TOKEN` (the token from step 1). Paste the values in the GitHub
   form only; they do not belong in a file, a commit or a log.
4. Push a version tag:

   ```sh
   git tag v0.3.0 && git push origin v0.3.0
   ```

The `Deploy` workflow (`.github/workflows/deploy.yml`) runs on the tag, or by
hand from the Actions tab. Its `bundle-check` job always runs the offline schema
check and tests whether both secrets are set. Its `deploy` job runs
`databricks bundle validate -t dev` and `databricks bundle deploy -t dev` only
when they are. Without the secrets, for example in a fork, that job is skipped
and the run stays green. A manual run can also start the job by ticking
`run_job`.

The token expires after 90 days; create a new one and replace the secret.

## Design decisions

- **Deploy is separate from CI.** `ci.yml` stays local-Spark and holds no
  secret, so every pull request and every fork gets the same checks. Only a tag
  reaches the workspace, and only where the secrets exist.
- **Serverless compute.** The job declares no cluster: there is nothing to size,
  patch or leave running, and it matches the environment the notebooks were run
  in.
- **No schedule.** The job runs when someone starts it. The sample file is a
  fixed input, and a trigger would only repeat the same rows.
- **Development mode.** The `dev` target prefixes the deployed job with the
  user name and pauses any schedule, so a deploy cannot collide with someone
  else's copy of the job.
