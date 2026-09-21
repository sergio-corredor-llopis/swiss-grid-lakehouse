from pathlib import Path

import pandas as pd
import pytest

from swiss_grid_lakehouse.ingest import CH_AREA, fetch_ch_load, parse_ch_load_xml

FIXTURE = Path(__file__).parent / "fixtures" / "entsoe_ch_load_sample.xml"


@pytest.fixture(scope="module")
def xml_text() -> str:
    return FIXTURE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def frame(xml_text: str) -> pd.DataFrame:
    return parse_ch_load_xml(xml_text)


class FakeClient:
    def __init__(self, result: pd.DataFrame):
        self.result = result
        self.calls: list[dict] = []

    def query_load(self, country_code, start, end):
        self.calls.append({"country_code": country_code, "start": start, "end": end})
        return self.result


def test_fixture_is_a_real_response(xml_text):
    assert FIXTURE.stat().st_size > 1000
    assert "<GL_MarketDocument" in xml_text
    assert "A16" in xml_text


def test_columns(frame):
    assert isinstance(frame, pd.DataFrame)
    assert list(frame.columns) == ["Actual Load"]


def test_index(frame):
    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame.index.tz is not None
    assert frame.index.is_monotonic_increasing and frame.index.is_unique
    assert len(frame) >= 24


def test_values(frame):
    col = frame["Actual Load"]
    assert pd.api.types.is_float_dtype(col)
    valid = col.dropna()
    assert len(valid) >= 1
    assert ((valid > 0) & (valid < 25000)).all()


def test_fetch_uses_injected_client(frame):
    fake = FakeClient(frame)
    start = pd.Timestamp("2026-09-01", tz="Europe/Zurich")
    end = pd.Timestamp("2026-09-03", tz="Europe/Zurich")
    out = fetch_ch_load(start, end, client=fake)
    assert out is frame
    assert len(fake.calls) == 1
    assert fake.calls[0]["country_code"] in ("CH", CH_AREA)


def test_fetch_without_client_or_token_raises(monkeypatch):
    monkeypatch.delenv("ENTSOE_API_TOKEN", raising=False)
    monkeypatch.delenv("ENTSOE_TOKEN", raising=False)
    start = pd.Timestamp("2026-09-01", tz="Europe/Zurich")
    end = pd.Timestamp("2026-09-03", tz="Europe/Zurich")
    with pytest.raises(RuntimeError, match="ENTSOE_API_TOKEN"):
        fetch_ch_load(start, end)
