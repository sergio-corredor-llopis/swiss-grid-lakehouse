"""The Azure deploy gate prints ok=true or ok=false and never prints a value."""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "has_azure.sh"
HOST = "host-value-that-must-not-appear"
CLIENT = "client-value-that-must-not-appear"


def run_gate(host: str | None, client: str | None) -> subprocess.CompletedProcess:
    env = {"PATH": os.environ["PATH"]}
    if host is not None:
        env["H"] = host
    if client is not None:
        env["C"] = client
    return subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)


def test_both_set_prints_ok_true_and_no_value():
    r = run_gate(HOST, CLIENT)
    assert r.returncode == 0
    assert r.stdout.strip() == "ok=true"
    assert r.stderr == ""
    assert HOST not in r.stdout + r.stderr
    assert CLIENT not in r.stdout + r.stderr


def test_missing_host_prints_ok_false_and_notice():
    r = run_gate("", CLIENT)
    assert r.returncode == 0
    assert r.stdout.strip() == "ok=false"
    assert r.stderr.startswith("::notice::")
    assert CLIENT not in r.stdout + r.stderr


def test_missing_client_prints_ok_false_and_notice():
    r = run_gate(HOST, "")
    assert r.returncode == 0
    assert r.stdout.strip() == "ok=false"
    assert r.stderr.startswith("::notice::")
    assert HOST not in r.stdout + r.stderr


def test_unset_variables_print_ok_false():
    r = run_gate(None, None)
    assert r.returncode == 0
    assert r.stdout.strip() == "ok=false"
    assert r.stderr.startswith("::notice::")
