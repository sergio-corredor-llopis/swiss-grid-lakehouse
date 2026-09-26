"""Smoke test: importable package, core install and CI stay Spark-free, core files exist."""

import tomllib
from pathlib import Path

import swiss_grid_lakehouse

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_package_version():
    assert swiss_grid_lakehouse.__version__ == "0.1.0"


def test_no_spark_guard():
    """Spark is allowed only in the optional `spark` group, never in core or dev deps or CI."""
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    core = [*project["dependencies"], *project["optional-dependencies"]["dev"]]
    for dep in core:
        assert "spark" not in dep.lower() and "databricks-connect" not in dep.lower(), dep
    spark_group = " ".join(project["optional-dependencies"]["spark"]).lower()
    assert "pyspark==4.0.1" in spark_group
    assert "delta-spark==4.0.0" in spark_group
    ci = (REPO_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8").lower()
    assert "pyspark" not in ci
    assert "databricks-connect" not in ci


def test_core_files_exist():
    for rel in (
        "pyproject.toml",
        "README.md",
        ".gitignore",
        ".github/workflows/ci.yml",
        "src/swiss_grid_lakehouse/__init__.py",
    ):
        assert (REPO_ROOT / rel).is_file(), rel
