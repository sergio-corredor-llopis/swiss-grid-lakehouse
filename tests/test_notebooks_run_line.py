"""The four notebooks print one RUN line, and the reconcile notebook has two input widgets."""

import re
from pathlib import Path

import pytest

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
NAMES = [
    "bronze_entsoe_ch_load",
    "bronze_swissgrid_energy",
    "silver_ch_load",
    "reconcile_daily",
]
RUN_PRINT = re.compile(r'print\(f"RUN spark_version=\{spark\.version\}[^"]*"\)')


def _text(name: str) -> str:
    return (NOTEBOOKS / f"{name}.py").read_text(encoding="utf-8")


@pytest.mark.parametrize("name", NAMES)
def test_notebook_keeps_databricks_format(name):
    text = _text(name)
    assert text.startswith("# Databricks notebook source\n")
    assert "# COMMAND ----------" in text


@pytest.mark.parametrize("name", NAMES)
def test_notebook_prints_one_run_line(name):
    text = _text(name)
    assert len(RUN_PRINT.findall(text)) == 1
    assert text.count("RUN spark_version=") == 1


@pytest.mark.parametrize("name", NAMES)
def test_run_line_comes_before_the_notebook_logic(name):
    text = _text(name)
    run_at = text.index("RUN spark_version=")
    assert run_at < text.index("spark.sql(")
    assert run_at < text.index("dbutils.widgets.get(")


def test_reconcile_notebook_has_package_and_csv_widgets():
    text = _text("reconcile_daily")
    assert 'dbutils.widgets.text("package", PACKAGE_URL' in text
    assert 'dbutils.widgets.text("csv", CSV_URL' in text
    assert 'dbutils.widgets.get("package")' in text
    assert 'dbutils.widgets.get("csv")' in text


def test_reconcile_notebook_reaches_inputs_only_through_the_widgets():
    text = _text("reconcile_daily")
    assert "run(package, csv, daily_table" in text
    assert "run(PACKAGE_URL" not in text
    assert text.count("https://") == 2


def test_reconcile_widget_defaults_are_the_live_urls():
    text = _text("reconcile_daily")
    assert "https://ckan.opendata.swiss/api/3/action/package_show" in text
    assert "https://www.bfe-ogd.ch/ogd103_stromverbrauch_swissgrid_lv_und_endv.csv" in text
