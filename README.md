# swiss-grid-lakehouse

Ingest of Swiss electricity grid data (actual total load) from the ENTSO-E
Transparency Platform into pandas DataFrames, tested against a real recorded
API response and checked by CI on every push. This is the first stage of a
planned medallion lakehouse on Databricks and Delta Lake; the current code is
the local Python ingest only.

Author: Sergio Corredor, data engineer.

## Architecture

```
ENTSO-E REST API --> fetch_ch_load() --> pandas DataFrame ("Actual Load", MW, tz-aware index)
                          ^
recorded XML fixture --> parse_ch_load_xml()   (pure: text in, DataFrame out)
```

- `parse_ch_load_xml`: pure parser, no network, no environment access.
- `fetch_ch_load`: thin I/O layer; the client is injectable, the token is read
  from `ENTSOE_API_TOKEN` (or `ENTSOE_TOKEN`) only.

## Run it

```sh
git clone https://github.com/sergio-corredor-llopis/swiss-grid-lakehouse.git
cd swiss-grid-lakehouse
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

No token is needed to run the tests. To record a fresh response with your own
token, see [docs/walkthrough/first_ingest.md](docs/walkthrough/first_ingest.md).

## What the tests and CI prove

- The parser turns a real, unmodified ENTSO-E response
  (`tests/fixtures/entsoe_ch_load_sample.xml`) into a one-column frame with a
  sorted, unique, timezone-aware index, at least 24 rows, float values in a
  plausible 0 to 25000 MW range.
- `fetch_ch_load` uses an injected client and fails clearly, before any network
  call, when no token is available.
- GitHub Actions (`.github/workflows/ci.yml`) runs `ruff check`,
  `ruff format --check` and pytest on every push and pull request, with no
  secret.

## Stack

Python 3.11+, [entsoe-py](https://github.com/EnergieID/entsoe-py) 0.8.1,
pandas, pytest, ruff, GitHub Actions. No Spark, Delta Lake, Databricks or Azure
code is in this repository yet.

## Data source and attribution

Data: ENTSO-E Transparency Platform (https://transparency.entsoe.eu/), actual
total load, Switzerland (bidding zone `10YCH-SWISSGRIDZ`). The fixture is an
unmodified response from the platform's public RESTful API; the data belongs to
ENTSO-E and its data providers.

## Roadmap (planned, not implemented)

1. Bronze layer: land raw ingests as Delta tables on Databricks Free Edition
   (Volumes).
2. Silver layer: PySpark transformations, Delta `MERGE` and a data-quality gate.
3. Gold layer: dbt-databricks models, with `OPTIMIZE` / `Z-ORDER` and Delta time
   travel.
4. Orchestration with Databricks Workflows or Airflow, and a Streamlit view.
5. Possible extensions: Terraform for an ADLS Gen2 storage target, and
   streaming.
