"""Offline checks of the schedule, the run ledger task and the run limit in databricks.yml.

No login, no Spark, no workspace: the bundle is read as plain YAML.
"""

import copy
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
BUNDLE = yaml.safe_load((REPO_ROOT / "databricks.yml").read_text(encoding="utf-8"))
TARGET_DEFAULT_TZ = "Europe/Zurich"


def jobs_of(bundle):
    """The two jobs by name: the Free Edition job and the Azure job."""
    dev = bundle["resources"]["jobs"]["ch_load"]
    azure = bundle["targets"]["azure"]["resources"]["jobs"]["ch_load_azure"]
    return {"ch-load": dev, "ch-load-azure": azure}


def problems(bundle):
    """Every way the bundle breaks the schedule rules; an empty list means it keeps them."""
    found = []
    variables = bundle["variables"]
    if variables.get("schedule_cron", {}).get("default") != "0 0 6 * * ?":
        found.append("schedule_cron default is not the daily 06:00 cron")
    if variables.get("source", {}).get("default") != "api":
        found.append("the scheduled source is not api")
    for name, job in jobs_of(bundle).items():
        tasks = job["tasks"]
        last = tasks[-1]
        if last["task_key"] != "run_history":
            found.append(f"{name}: last task is not run_history")
        if last.get("run_if") != "ALL_DONE":
            found.append(f"{name}: run_history is not run_if ALL_DONE")
        earlier = {t["task_key"] for t in tasks[:-1]}
        waits_for = {d["task_key"] for d in last.get("depends_on", [])}
        if waits_for != earlier:
            found.append(f"{name}: run_history does not wait for every other task")
        if any("run_if" in t for t in tasks[:-1]):
            found.append(f"{name}: only run_history may carry run_if")
        if job.get("max_concurrent_runs") != 1:
            found.append(f"{name}: max_concurrent_runs is not 1")
        schedule = job.get("schedule", {})
        if schedule.get("timezone_id") != TARGET_DEFAULT_TZ:
            found.append(f"{name}: schedule is not in Europe/Zurich")
        if schedule.get("quartz_cron_expression") != "${var.schedule_cron}":
            found.append(f"{name}: schedule does not use the schedule_cron variable")
    if jobs_of(bundle)["ch-load-azure"].get("schedule", {}).get("pause_status") != "PAUSED":
        found.append("the azure schedule is not PAUSED")
    if jobs_of(bundle)["ch-load"].get("schedule", {}).get("pause_status") == "PAUSED":
        found.append("the Free Edition schedule is PAUSED")
    dev = jobs_of(bundle)["ch-load"]
    if dev.get("parameters") != [{"name": "source", "default": "${var.source}"}]:
        found.append("the Free Edition job does not take its source from the source variable")
    by_key = {t["task_key"]: t for t in dev["tasks"]}
    for key in ("bronze", "run_history"):
        got = by_key[key]["notebook_task"]["base_parameters"]["source"]
        if got != "{{job.parameters.source}}":
            found.append(f"{key}: source is not the job parameter")
    return found


def test_bundle_keeps_the_schedule_rules():
    assert problems(BUNDLE) == []


def test_both_jobs_end_in_run_history_with_all_done():
    for name, job in jobs_of(BUNDLE).items():
        last = job["tasks"][-1]
        assert last["task_key"] == "run_history", name
        assert last["run_if"] == "ALL_DONE", name
        assert last["notebook_task"]["notebook_path"] == "./notebooks/run_history.py", name


def test_azure_is_paused_and_the_free_edition_job_is_not():
    jobs = jobs_of(BUNDLE)
    assert jobs["ch-load-azure"]["schedule"]["pause_status"] == "PAUSED"
    assert jobs["ch-load"]["schedule"]["pause_status"] == "UNPAUSED"


def test_one_run_at_a_time_in_europe_zurich():
    for name, job in jobs_of(BUNDLE).items():
        assert job["max_concurrent_runs"] == 1, name
        assert job["schedule"]["timezone_id"] == "Europe/Zurich", name


def test_scheduled_source_is_the_api():
    assert BUNDLE["variables"]["source"]["default"] == "api"
    dev = jobs_of(BUNDLE)["ch-load"]
    assert dev["parameters"] == [{"name": "source", "default": "${var.source}"}]


def test_run_history_notebook_exists():
    assert (REPO_ROOT / "notebooks" / "run_history.py").is_file()


def test_checker_rejects_a_bundle_that_breaks_each_rule():
    def broken(edit):
        copy_of = copy.deepcopy(BUNDLE)
        edit(copy_of)
        return problems(copy_of)

    dev = lambda b: b["resources"]["jobs"]["ch_load"]  # noqa: E731
    azure = lambda b: b["targets"]["azure"]["resources"]["jobs"]["ch_load_azure"]  # noqa: E731
    assert broken(lambda b: dev(b)["tasks"][-1].pop("run_if"))
    assert broken(lambda b: azure(b)["tasks"][-1].update(run_if="ALL_SUCCESS"))
    assert broken(lambda b: azure(b)["schedule"].update(pause_status="UNPAUSED"))
    assert broken(lambda b: dev(b).pop("max_concurrent_runs"))
    assert broken(lambda b: azure(b).update(max_concurrent_runs=2))
    assert broken(lambda b: dev(b)["schedule"].update(timezone_id="UTC"))
    assert broken(lambda b: b["variables"]["source"].update(default="/Volumes/x/y.xml"))
    assert broken(lambda b: dev(b)["tasks"][-1]["depends_on"].pop())
