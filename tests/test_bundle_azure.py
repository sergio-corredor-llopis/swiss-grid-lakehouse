"""Offline checks of the azure target in databricks.yml: no login, no Spark, no workspace."""

from pathlib import Path

import yaml
from leak_samples import HOST_FRAGMENT

REPO_ROOT = Path(__file__).resolve().parent.parent
BUNDLE = yaml.safe_load((REPO_ROOT / "databricks.yml").read_text(encoding="utf-8"))
AZURE = BUNDLE["targets"]["azure"]
AZURE_JOB = next(iter(AZURE["resources"]["jobs"].values()))
AZURE_TASKS = AZURE_JOB["tasks"]


def test_dev_target_unchanged():
    assert BUNDLE["targets"]["dev"] == {"mode": "development", "default": True}


def test_top_level_job_keeps_bronze_and_silver():
    tasks = BUNDLE["resources"]["jobs"]["ch_load"]["tasks"]
    assert [t["task_key"] for t in tasks] == ["bronze", "silver"]


def test_azure_host_comes_from_variable():
    assert AZURE["workspace"] == {"host": "${var.azure_host}"}
    assert "azure_host" in BUNDLE["variables"]
    assert "default" not in BUNDLE["variables"]["azure_host"]
    text = (REPO_ROOT / "databricks.yml").read_text(encoding="utf-8").lower()
    assert "https://" not in text
    assert HOST_FRAGMENT not in text


def test_azure_job_has_four_tasks_in_order():
    assert len(AZURE["resources"]["jobs"]) == 1
    keys = [t["task_key"] for t in AZURE_TASKS]
    assert keys == ["bronze_entsoe", "bronze_swissgrid", "silver", "reconcile"]


def test_azure_job_dependencies():
    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in AZURE_TASKS}
    assert deps["bronze_entsoe"] == []
    assert deps["bronze_swissgrid"] == []
    assert sorted(deps["silver"]) == ["bronze_entsoe", "bronze_swissgrid"]
    assert deps["reconcile"] == ["silver"]


def test_azure_job_is_serverless():
    forbidden = {"existing_cluster_id", "new_cluster", "job_cluster_key", "job_clusters"}
    assert not forbidden & set(AZURE_JOB)
    for task in AZURE_TASKS:
        assert not forbidden & set(task), task["task_key"]


def test_azure_notebook_paths_exist():
    for task in AZURE_TASKS:
        path = REPO_ROOT / task["notebook_task"]["notebook_path"]
        assert path.is_file(), path
