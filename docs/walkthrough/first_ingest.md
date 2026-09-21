# Walkthrough: scaffold, CI and the first ENTSO-E ingest

This page follows the repository from a clean checkout to a passing test run,
then to your own recording of a real ENTSO-E response. You need Python 3.11+
and, for the last section only, your own ENTSO-E API token.

The CI workflow is `.github/workflows/ci.yml`. One term is this project's own:

- **Recorded fixture**: a real reply from a real API, saved once to a file and
  committed, so the tests run offline and without a key. Ours is
  `tests/fixtures/entsoe_ch_load_sample.xml`, an unmodified ENTSO-E
  Transparency Platform response: root element `<GL_MarketDocument`, process
  type `A16` (actual load).

## What the ingest does and why it matters

`parse_ch_load_xml(xml_text)` turns one ENTSO-E actual-load response for
Switzerland into a one-column pandas DataFrame (`"Actual Load"`, megawatts as
floats) indexed by a timezone-aware, sorted, unique timestamp index. The CI run
is green without any secret because the network call is not on the test path.

This avoids a common trap in data projects: tests that need a live API key get
skipped and quietly rot. Here nothing is skipped in the ingest tests, and a
missing fixture fails them; the recorded fixture takes the API's place.

## Read the code in this order

- **`pyproject.toml`**: package name, dependencies and tool settings.
  `entsoe-py` is pinned exactly (`==0.8.1`) because the project relies on its
  parser; `pandas>=2.0` is a range. `pytest` and `ruff` sit in an optional
  `dev` group. The code lives under `src/`, so tests always run against the
  installed package.
- **`.github/workflows/ci.yml`**: runs on every `push` and `pull_request`,
  cancels an older run of the same branch when a newer one starts, installs
  Python 3.12 with pip caching, then runs `ruff check`, `ruff format --check`
  and one `pytest -q`. Nothing is conditional: if the fixture file is missing,
  the ingest tests fail and the run is red.
- **`src/swiss_grid_lakehouse/ingest/entsoe_load.py`**: the ingest, split into
  a pure part and an I/O part.
  - `CH_AREA = "10YCH-SWISSGRIDZ"` is the ENTSO-E code of the Swiss bidding zone.
  - `parse_ch_load_xml` is the seam: XML text in, DataFrame out, no network, no
    environment variable, no file write. Three cheap guards raise `ValueError`
    if the text is not a `GL_MarketDocument`, has no `A16`, or has no
    `<TimeSeries`, instead of returning an empty frame. The parsing itself is
    delegated to `entsoe.parsers.parse_loads`.
  - `fetch_ch_load(start, end, client=None, token=None)` is the impure half.
    `client` is the second seam: any object with a
    `query_load(country_code, start, end)` method works, which is how the tests
    inject a fake. The token comes from the argument, then `ENTSOE_API_TOKEN`,
    then `ENTSOE_TOKEN`; if none is set it raises `RuntimeError` before any
    network call.
- **`src/swiss_grid_lakehouse/ingest/__init__.py`**: re-exports `CH_AREA`,
  `fetch_ch_load` and `parse_ch_load_xml`.
- **`tests/test_entsoe_load.py`**: offline tests against the recorded fixture.
  They check that the fixture is a real response (over 1000 bytes, contains
  `<GL_MarketDocument` and `A16`), that the columns are exactly
  `["Actual Load"]`, that the index is a sorted, unique, timezone-aware
  `DatetimeIndex` with at least 24 rows, and that values are floats between 0
  and 25000 MW. A `FakeClient` proves `fetch_ch_load` passes the injected
  client through and calls it once; another test proves the missing-token error.
- **`tests/test_smoke.py`**: package imports, `__version__`, the core files
  exist, and the Spark guard described below.
- **`scripts/record_fixture.py`**: how the fixture was produced. It reads the
  token from the environment only, never from a file and never prints it. It
  exits 2 if no token is set, 4 rather than overwrite an existing fixture, and
  3 (writing nothing) if the response is not a valid A16 document. It writes
  the raw response bytes unmodified and prints one summary line with byte count
  and sha256.

## Run it yourself

```sh
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

To record your own response, request RESTful API access for your account on the
[ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) (see the
Transparency Platform RESTful API guide for the procedure), then:

```sh
export ENTSOE_API_TOKEN=...      # your own token; never commit it
python scripts/record_fixture.py --out /tmp/my_sample.xml
```

The committed fixture covers 2026-09-01 to 2026-09-03; `--start` and `--end`
choose another window.

## Design decisions

**Why does CI need no secret?** The tests never call the API. They read the
fixture and feed it to the pure `parse_ch_load_xml`; the network-facing
`fetch_ch_load` is tested with an injected client.

**Why is parsing separate from fetching?** A pure function (text in, DataFrame
out) can be tested exactly against a real payload. The impure half stays thin
and is tested through a fake client.

**Why a recorded fixture rather than a hand-written sample?** A toy sample only
proves the parser handles the shape its author imagined. A real response
carries the real quirks of the API. The size and marker checks in the test stop
anyone replacing it with a toy.

**Why pandas at this stage?** The ingest is one request returning a few hundred
rows, and the `entsoe-py` parser returns pandas. Spark is not used anywhere in
the code today; see the roadmap in the README for where it is planned.

**Why is the token environment-only?** So it cannot land in git, a log or a
test. `.gitignore` also excludes `.env`.

**Why does the Spark guard exist?** `tests/test_smoke.py` fails if
`pyproject.toml` or the CI workflow mentions `pyspark` or `databricks-connect`.
It documents that the core package and this CI job are Spark-free today. When
Spark code arrives it will live outside the core package and run on
Databricks, and the guard will be scoped accordingly.

## Try a small change

Add `"scripts/record_fixture.py",` to the tuple of core files in
`tests/test_smoke.py` and run:

```sh
pytest -q -rs tests/test_smoke.py
```

Expected: `3 passed`. Undo with `git checkout -- tests/test_smoke.py`.
