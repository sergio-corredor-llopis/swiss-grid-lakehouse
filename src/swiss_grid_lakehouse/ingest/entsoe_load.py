"""First ingest: Swiss (CH) actual total load from the ENTSO-E Transparency Platform."""

from __future__ import annotations

import os

import pandas as pd

CH_AREA = "10YCH-SWISSGRIDZ"


def parse_ch_load_xml(xml_text: str) -> pd.DataFrame:
    """Parse one recorded ENTSO-E actual-load response for Switzerland.

    Args:
        xml_text: the response body exactly as returned by the ENTSO-E
            Transparency REST API for documentType=A65, processType=A16
            (a GL_MarketDocument). Read from disk in tests; never fetched here.
    Returns:
        A one-column pandas DataFrame, column "Actual Load", MW as float,
        indexed by a timezone-aware DatetimeIndex sorted ascending.
    Raises:
        ValueError: if the text is not a GL_MarketDocument, if processType A16
            is absent, or if no time series is present.
    Performs no network I/O, reads no environment variable, writes no file.
    """
    if "GL_MarketDocument" not in xml_text:
        raise ValueError("not a GL_MarketDocument")
    if "A16" not in xml_text:
        raise ValueError("processType A16 (actual load) is absent")
    if "<TimeSeries" not in xml_text:
        raise ValueError("no TimeSeries present")
    from entsoe.parsers import parse_loads

    return parse_loads(xml_text, process_type="A16")


def fetch_ch_load(
    start: pd.Timestamp,
    end: pd.Timestamp,
    client: object | None = None,
    token: str | None = None,
) -> pd.DataFrame:
    """Swiss actual load between start and end; same frame contract as above.

    Args:
        start, end: timezone-aware pandas Timestamps (Europe/Zurich).
        client: any object exposing query_load(country_code, start, end) ->
            pd.DataFrame. Injected by tests. When None, an
            entsoe.EntsoePandasClient is built from `token`.
        token: ENTSO-E security token; when None, read from ENTSOE_API_TOKEN,
            else ENTSOE_TOKEN.
    Raises:
        RuntimeError: no client and no token -- message names ENTSOE_API_TOKEN;
            no network call is attempted first.
    """
    if client is None:
        token = token or os.environ.get("ENTSOE_API_TOKEN") or os.environ.get("ENTSOE_TOKEN")
        if not token:
            raise RuntimeError("no client and no token: set ENTSOE_API_TOKEN (or ENTSOE_TOKEN)")
        from entsoe import EntsoePandasClient

        client = EntsoePandasClient(api_key=token)
    return client.query_load("CH", start=start, end=end)
