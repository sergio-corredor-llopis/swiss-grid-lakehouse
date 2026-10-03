"""The Gold notebook: its text only, no Spark. It runs the same code on both targets."""

import re
from pathlib import Path

import pytest

NOTEBOOK = Path(__file__).resolve().parent.parent / "notebooks" / "gold_marts.py"
TEXT = NOTEBOOK.read_text(encoding="utf-8")


def test_databricks_format_and_install_cell():
    assert TEXT.startswith("# Databricks notebook source\n")
    assert "# MAGIC %md" in TEXT
    assert "# MAGIC %pip install --quiet .." in TEXT


def test_docstring_says_it_is_identical_on_both_targets():
    assert "identical on the dev target" in TEXT
    assert "azure" in TEXT


def test_run_line_is_printed_once_and_first():
    assert TEXT.count("RUN spark_version=") == 1
    assert len(re.findall(r'print\(f"RUN spark_version=\{spark\.version\}[^"]*"\)', TEXT)) == 1
    assert TEXT.index("RUN spark_version=") < TEXT.index("spark.sql(")
    assert TEXT.index("RUN spark_version=") < TEXT.index("dbutils.widgets.get(")


def test_widgets_are_catalog_and_schema_only():
    assert re.findall(r'dbutils\.widgets\.text\("(\w+)"', TEXT) == ["catalog", "schema"]
    assert re.findall(r'dbutils\.widgets\.get\("(\w+)"\)', TEXT) == ["catalog", "schema"]


@pytest.mark.parametrize(
    "call",
    [
        "daily_load.build(spark, silver)",
        "daily_control.build(spark, silver, daily)",
        "hourly_profile.build(spark, silver)",
        "data_quality.build(spark, silver, rejected)",
    ],
)
def test_the_four_build_functions_are_called(call):
    assert call in TEXT


def test_daily_bronze_is_optional():
    assert "tableExists" in TEXT
    assert "ch_consumption_daily_bronze" in TEXT
    assert "SKIPPED gold_daily_control reason=no_daily_bronze" in TEXT


def test_gate_runs_before_any_merge_and_a_failure_raises():
    assert TEXT.index("run_gold_gate(silver, marts)") < TEXT.index("merge_mart(spark")
    assert "if not result.passed:" in TEXT
    assert "raise RuntimeError" in TEXT
    assert TEXT.index("raise RuntimeError") < TEXT.index("merge_mart(spark")


def test_merge_optimize_and_version_lines_are_printed():
    assert "merge_mart(spark, df, target, key, change_cols)" in TEXT
    assert "mart_line(short" in TEXT
    assert "optimize_line(" in TEXT
    assert "version_line(" in TEXT
    assert "version_query(" in TEXT
    assert "print(result.summary())" in TEXT


def test_marts_land_in_catalog_schema_tables():
    for table in (
        "gold_daily_load",
        "gold_daily_control",
        "gold_hourly_profile",
        "gold_data_quality",
    ):
        assert table in TEXT
    assert 'prefix = f"{catalog}.{schema}"' in TEXT
