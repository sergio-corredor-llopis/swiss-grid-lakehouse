"""Smoke test: importable package, core package and CI are Spark-free, core files exist."""

from pathlib import Path

import swiss_grid_lakehouse

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_package_version():
    assert swiss_grid_lakehouse.__version__ == "0.1.0"


def test_no_spark_guard():
    for rel in ("pyproject.toml", ".github/workflows/ci.yml"):
        text = (REPO_ROOT / rel).read_text(encoding="utf-8").lower()
        assert "pyspark" not in text
        assert "databricks-connect" not in text


def test_core_files_exist():
    for rel in (
        "pyproject.toml",
        "README.md",
        ".gitignore",
        ".github/workflows/ci.yml",
        "src/swiss_grid_lakehouse/__init__.py",
    ):
        assert (REPO_ROOT / rel).is_file(), rel
