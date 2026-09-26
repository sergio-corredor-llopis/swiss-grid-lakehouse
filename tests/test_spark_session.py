"""get_spark: runtime selection, plus a real local Delta round trip."""

import pytest

from swiss_grid_lakehouse.spark import session


def test_default_runtime_is_local(monkeypatch):
    monkeypatch.delenv("SGL_RUNTIME", raising=False)
    assert session._resolve(None) == "local"


def test_env_selects_runtime(monkeypatch):
    monkeypatch.setenv("SGL_RUNTIME", "Databricks")
    assert session._resolve(None) == "databricks"
    assert session._resolve("local") == "local"


def test_unknown_runtime_rejected():
    with pytest.raises(ValueError, match="unknown runtime"):
        session.get_spark("mainframe")


def test_databricks_without_connect_is_explicit(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake(name, *a, **k):
        if name.startswith("databricks"):
            raise ImportError(name)
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake)
    with pytest.raises(RuntimeError, match="databricks-connect"):
        session.get_spark("databricks")


@pytest.mark.spark
def test_local_delta_round_trip(tmp_path):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    spark = session.get_spark("local")
    path = str(tmp_path / "t")
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"]).write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 2
