"""Tests for scripts/render_run_summary.py: the summary, the RUN line, the leak check.

The leak samples come from tests/leak_samples.py, which joins them at run time; no
committed file holds a host, token or identifier.
"""

import subprocess
import sys
from pathlib import Path

import pytest
from leak_samples import LEAK_ORDER, SAMPLES, leak_output_text

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "render_run_summary.py"
CLEAN = ROOT / "tests" / "fixtures" / "databricks_run_2026-09-27.txt"
GOLD_SAMPLE = ROOT / "tests" / "fixtures" / "gold_run_sample.txt"

FAKE_LEAKS = SAMPLES


def run(args, stdin=None):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], input=stdin, capture_output=True, text=True
    )


def render(path):
    return run([str(path), "--date", "2026-09-27", "--region", "test"])


def fenced_blocks(markdown):
    blocks, current = [], None
    for line in markdown.splitlines():
        if line.startswith("```"):
            if current is None:
                current = []
            else:
                blocks.append("\n".join(current))
                current = None
        elif current is not None:
            current.append(line)
    return blocks


def test_clean_input_renders_summary():
    result = render(CLEAN)
    assert result.returncode == 0
    lines = result.stdout.splitlines()
    assert lines[0] == "# Run summary"
    assert "- Date: 2026-09-27" in lines
    assert "- Region: test" in lines
    assert "- spark.version: not recorded" in lines
    blocks = fenced_blocks(result.stdout)
    assert any("GATE: PASS checks=6" in b for b in blocks)
    assert any("merged inserted=48 updated=0 rows=48" in b for b in blocks)
    assert any("wrote 48 rows" in b for b in blocks)
    assert any("skipped 48 rows" in b for b in blocks)


def test_run_line_is_picked_up(tmp_path):
    source = tmp_path / "out.txt"
    source.write_text(
        "RUN spark_version=4.0.0 environment_version=3\n" + CLEAN.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    result = render(source)
    assert result.returncode == 0
    assert "- spark.version: 4.0.0" in result.stdout.splitlines()
    assert "- Serverless environment version: 3" in result.stdout.splitlines()
    assert "not recorded" not in result.stdout


def test_reads_standard_input():
    result = run(
        ["-", "--date", "2026-09-27", "--region", "test"], stdin=CLEAN.read_text(encoding="utf-8")
    )
    assert result.returncode == 0
    assert "GATE: PASS" in result.stdout


def test_hourly_compare_and_reconcile_lines_are_kept(tmp_path):
    source = tmp_path / "out.txt"
    source.write_text(
        "silver_ch_load (10:00:00):\nGATE: PASS checks=6\nHOURLY x\n"
        "COMPARE days=2 hours=48 PASS\n\n"
        "reconcile (10:01:00):\nDAY 2026-08-30 final\nRECONCILE ok preliminary=2\nnoise line\n",
        encoding="utf-8",
    )
    result = render(source)
    assert result.returncode == 0
    joined = "\n".join(fenced_blocks(result.stdout))
    for text in (
        "HOURLY x",
        "COMPARE days=2 hours=48 PASS",
        "DAY 2026-08-30 final",
        "RECONCILE ok preliminary=2",
    ):
        assert text in joined
    assert "noise line" not in result.stdout


def test_gold_lines_are_kept_verbatim():
    result = render(GOLD_SAMPLE)
    assert result.returncode == 0
    joined = fenced_blocks(result.stdout)
    kept = "\n".join(joined).splitlines()
    wanted = [line for line in GOLD_SAMPLE.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert kept == wanted
    for kind in ("MART ", "SKIPPED ", "GOLD GATE:", "OPTIMIZE ", "VERSION "):
        assert any(line.startswith(kind) for line in kept), kind


def test_gold_sample_has_the_five_line_kinds_and_no_leak():
    text = GOLD_SAMPLE.read_text(encoding="utf-8")
    assert "MART g1 rows=4 inserted=4 updated=0" in text
    assert "VERSION table=g1 v=0 rows=4 now=4" in text
    assert "http" not in text


def test_unrelated_lines_next_to_gold_lines_are_dropped(tmp_path):
    source = tmp_path / "out.txt"
    source.write_text(
        "gold_marts (10:00:00):\nMART g1 rows=4 inserted=4 updated=0\nmartian noise\n"
        "GOLD GATE: PASS checks=5\nOPTIMIZE table=g3 files_removed=3 files_added=1\n",
        encoding="utf-8",
    )
    result = render(source)
    assert result.returncode == 0
    assert "martian noise" not in result.stdout
    assert "OPTIMIZE table=g3 files_removed=3 files_added=1" in result.stdout


@pytest.mark.parametrize("name", sorted(FAKE_LEAKS))
def test_each_leak_pattern_alone_exits_3(tmp_path, name):
    value = FAKE_LEAKS[name]
    source = tmp_path / "out.txt"
    source.write_text("GATE: PASS checks=6\nnote " + value + " end\n", encoding="utf-8")
    result = render(source)
    assert result.returncode == 3
    assert result.stdout.strip() == "LEAK: %s at line 2" % name
    assert value not in result.stdout
    assert value not in result.stderr
    assert "GATE" not in result.stdout


def test_leak_output_exits_3(tmp_path):
    source = tmp_path / "out.txt"
    source.write_text(leak_output_text(), encoding="utf-8")
    result = render(source)
    assert result.returncode == 3
    assert result.stdout.split()[1] == LEAK_ORDER[0]


def test_missing_file_exits_2(tmp_path):
    result = render(tmp_path / "absent.txt")
    assert result.returncode == 2
    assert result.stdout == ""


def test_missing_option_exits_2():
    assert run([str(CLEAN), "--date", "2026-09-27"]).returncode == 2
    assert run([str(CLEAN), "--region", "test"]).returncode == 2


def test_bad_date_exits_2():
    assert run([str(CLEAN), "--date", "yesterday", "--region", "test"]).returncode == 2


BUNDLE_RUN = ROOT / "tests" / "fixtures" / "azure_run1_expected.txt"
BUNDLE_EMPTY = "Output:\n=======\nTask bronze_swissgrid:\n\n=======\nTask silver:\n\n"
KEPT = ("RUN ", "GATE:", "merged ", "HOURLY", "COMPARE", "DAY ", "RECONCILE")


def test_bundle_run_output_keeps_every_report_line_verbatim():
    result = render(BUNDLE_RUN)
    assert result.returncode == 0
    kept = [
        line
        for line in BUNDLE_RUN.read_text(encoding="utf-8").splitlines()
        if line.startswith(KEPT) or line.split(": ", 1)[-1].startswith(("GATE:", "merged "))
    ]
    assert len([x for x in kept if x.startswith("RUN ")]) == 4
    assert len(kept) == 4 + 4 + 2 + 1 + 9  # RUN, GATE+merged, HOURLY, COMPARE, DAY+RECONCILE
    shown = result.stdout.splitlines()
    for line in kept:
        assert line in shown
    for header in ("Task bronze_swissgrid:", "Task bronze_entsoe:", "Task silver:"):
        assert header in shown
    assert "Task reconcile:" in shown
    assert "- spark.version: 4.0.0" in shown
    assert "- Serverless environment version: 3" in shown


def test_bundle_run_without_report_lines_renders_no_blocks(tmp_path):
    source = tmp_path / "out.txt"
    source.write_text(BUNDLE_EMPTY, encoding="utf-8")
    result = render(source)
    assert result.returncode == 0
    assert fenced_blocks(result.stdout) == []


def test_bundle_run_with_a_leak_exits_3(tmp_path):
    source = tmp_path / "out.txt"
    text = BUNDLE_RUN.read_text(encoding="utf-8")
    source.write_text(text + "note " + FAKE_LEAKS["workspace-id"] + "\n", encoding="utf-8")
    result = render(source)
    assert result.returncode == 3
    assert result.stdout.strip() == "LEAK: workspace-id at line %d" % (len(text.splitlines()) + 1)
    assert "GATE" not in result.stdout
