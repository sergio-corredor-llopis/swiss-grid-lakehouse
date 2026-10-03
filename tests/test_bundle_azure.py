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


def test_top_level_job_has_bronze_silver_and_gold():
    tasks = BUNDLE["resources"]["jobs"]["ch_load"]["tasks"]
    assert [t["task_key"] for t in tasks] == ["bronze", "silver", "gold"]
    gold = tasks[2]
    assert [d["task_key"] for d in gold["depends_on"]] == ["silver"]
    assert gold["notebook_task"]["notebook_path"] == "./notebooks/gold_marts.py"
    assert set(gold["notebook_task"]["base_parameters"]) == {"catalog", "schema"}


def test_azure_host_is_never_committed():
    # workspace.host may not interpolate a variable; DATABRICKS_HOST sets it at run time.
    assert "host" not in AZURE.get("workspace", {})
    assert "azure_host" not in BUNDLE["variables"]
    text = (REPO_ROOT / "databricks.yml").read_text(encoding="utf-8").lower()
    assert "https://" not in text
    assert HOST_FRAGMENT not in text


def test_azure_job_has_five_tasks_in_order():
    assert len(AZURE["resources"]["jobs"]) == 1
    keys = [t["task_key"] for t in AZURE_TASKS]
    assert keys == ["bronze_entsoe", "bronze_swissgrid", "silver", "reconcile", "gold"]


def test_azure_job_dependencies():
    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in AZURE_TASKS}
    assert deps["bronze_entsoe"] == []
    assert deps["bronze_swissgrid"] == []
    assert sorted(deps["silver"]) == ["bronze_entsoe", "bronze_swissgrid"]
    assert deps["reconcile"] == ["silver"]
    assert deps["gold"] == ["reconcile"]


def test_gold_task_is_the_same_notebook_on_both_jobs():
    top = BUNDLE["resources"]["jobs"]["ch_load"]["tasks"][2]
    azure = AZURE_TASKS[-1]
    assert top["task_key"] == azure["task_key"] == "gold"
    assert top["notebook_task"] == azure["notebook_task"]
    assert top["depends_on"] != azure["depends_on"]


def test_azure_job_is_serverless():
    forbidden = {"existing_cluster_id", "new_cluster", "job_cluster_key", "job_clusters"}
    assert not forbidden & set(AZURE_JOB)
    for task in AZURE_TASKS:
        assert not forbidden & set(task), task["task_key"]


def test_azure_notebook_paths_exist():
    for task in AZURE_TASKS:
        path = REPO_ROOT / task["notebook_task"]["notebook_path"]
        assert path.is_file(), path
