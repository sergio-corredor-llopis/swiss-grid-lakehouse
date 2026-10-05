"""Docs coherence: the README and docs/ must agree with the files and the bundle in the repo.

Offline, no Spark, no network; standard library and pytest only. Five checks:

- links: every backticked repo path in README.md and docs/**/*.md exists in the file set;
- notebook_run: a sentence saying a notebook "has not been run" must name a notebook that the
  recorded runs (docs/runs/*.md and the README run sections) do not record as run;
- task_keys: every task key of databricks.yml appears in docs/walkthrough/deploy.md and in the
  README section "Release and deploy";
- milestones: the README milestone table has nine rows, M followed by 1 to 9 in order, each
  with a PR reference and an existing docs page;
- fixture_name: the removed sample fixture name is not mentioned anywhere in the docs.

Script form (no pytest needed):

    python3 tests/test_docs_coherence.py [--root DIR] [--file REL ...] [--self-test]

prints one `FAIL <check> <file>:<line> <message>` line per failure and a last line
`COHERENCE: PASS` or `COHERENCE: FAIL <n>`; exit 1 on failure. `--file REL` keeps only the
failures attributed to REL.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

try:
    import pytest
except ImportError:  # the script form works without pytest
    pytest = None

DEFAULT_ROOT = Path(__file__).resolve().parent.parent

PATH_PREFIXES = ("tests/", "src/", "docs/", "notebooks/", "scripts/", ".github/")
ROOT_FILES = (
    "README.md", "databricks.yml", "pyproject.toml", "requirements.txt", "LICENSE",
    ".gitignore", "Makefile", "uv.lock", "setup.py", "setup.cfg", "CHANGELOG.md",
)  # fmt: skip
ALLOWLIST_PREFIXES = ("/tmp", "dbfs:", "/Volumes")
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", ".venv", ".ruff_cache"}
REMOVED_FIXTURE = "entsoe_ch_load_sample.xml"
GENERIC_NOTEBOOK_WORDS = {"ch", "load", "energy", "daily", "marts"}
NOT_RUN = re.compile(
    r"\b(?:not|never)\s+(?:yet\s+)?been\s+run\b|\bha(?:s|ve)\s+(?:not|never)\s+(?:yet\s+)?run\b",
    re.IGNORECASE,
)
MILESTONES_BEGIN = "<!-- milestones:begin -->"
MILESTONES_END = "<!-- milestones:end -->"
FENCE = re.compile(r"^\s*(```|~~~)")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
ITEM = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")


class Failure(NamedTuple):
    check: str
    file: str
    line: int
    message: str

    def render(self) -> str:
        return f"FAIL {self.check} {self.file}:{self.line} {self.message}"


# --------------------------------------------------------------------------- file set


def file_set(root: Path) -> set[str]:
    """Repo-relative POSIX paths: `git ls-files` when .git exists, else a walk without .git."""
    if (root / ".git").exists():
        done = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"], capture_output=True, check=False
        )
        if done.returncode == 0:
            return {p for p in done.stdout.decode("utf-8").split("\0") if p}
    found: set[str] = set()
    for base, dirs, names in os.walk(root):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for name in names:
            found.add((Path(base) / name).relative_to(root).as_posix())
    return found


def doc_files(files: set[str]) -> list[str]:
    docs = [f for f in files if f == "README.md" or (f.startswith("docs/") and f.endswith(".md"))]
    return sorted(docs)


def read_docs(root: Path, files: set[str]) -> dict[str, str]:
    return {rel: (root / rel).read_text(encoding="utf-8") for rel in doc_files(files)}


# --------------------------------------------------------------------------- helpers


def prose_lines(text: str):
    """Yield (lineno, line, in_fence) for every line."""
    fenced = False
    for no, line in enumerate(text.splitlines(), 1):
        if FENCE.match(line):
            fenced = not fenced
            yield no, line, True
            continue
        yield no, line, fenced


def exists_in(token: str, files: set[str]) -> bool:
    if token in files:
        return True
    prefix = token.rstrip("/") + "/"
    return any(f.startswith(prefix) for f in files)


def clean_token(token: str) -> str:
    token = token.strip()
    token = re.split(r"::|#", token)[0]
    return re.sub(r":\d[\d,-]*$", "", token)


# --------------------------------------------------------------------------- (a) links


def check_links(docs: dict[str, str], files: set[str]) -> list[Failure]:
    failures = []
    for rel, text in docs.items():
        for no, line, fenced in prose_lines(text):
            if fenced:
                continue
            for raw in re.findall(r"`([^`\n]+)`", line):
                if re.search(r"[<>*{}\s]|\.\.\.|://", raw):
                    continue
                if raw.startswith(ALLOWLIST_PREFIXES) or REMOVED_FIXTURE in raw:
                    continue  # the removed fixture name is reported by fixture_name
                token = clean_token(raw)
                if not (token.startswith(PATH_PREFIXES) or token in ROOT_FILES):
                    continue
                if not exists_in(token, files):
                    failures.append(
                        Failure("links", rel, no, f"`{token}` is not a file or directory")
                    )
    return failures


# --------------------------------------------------------------------------- (b) notebooks


def notebook_stems(files: set[str]) -> list[str]:
    return sorted(Path(f).stem for f in files if f.startswith("notebooks/") and f.endswith(".py"))


def key_words(stem: str) -> set[str]:
    return {w for w in stem.split("_") if w not in GENERIC_NOTEBOOK_WORDS}


def mentions_stem(text: str, stem: str) -> bool:
    return re.search(rf"(?<![\w]){re.escape(stem)}(?![\w])", text) is not None


def blocks(text: str):
    """Split a document into blocks (a paragraph or a list item): lists of (lineno, line)."""
    current: list[tuple[int, str]] = []
    fenced = False
    for no, line in enumerate(text.splitlines(), 1):
        if FENCE.match(line):
            fenced = not fenced
            if current:
                yield current
                current = []
            continue
        if fenced:
            continue
        if not line.strip() or HEADING.match(line) or ITEM.match(line):
            if current:
                yield current
                current = []
            if line.strip() and not HEADING.match(line):
                current.append((no, line))
            continue
        current.append((no, line))
    if current:
        yield current


def clauses_not_run(text: str):
    """Yield (lineno, clause, block_lines) for each clause that says something was not run."""
    for block in blocks(text):
        joined = ""
        starts = []  # (offset, lineno)
        for no, line in block:
            starts.append((len(joined), no))
            joined += line + "\n"
        for match in NOT_RUN.finditer(joined):
            lo = max((m.end() for m in re.finditer(r"[.;!?](?=\s)", joined[: match.start()])),
                     default=0)  # fmt: skip
            end = re.search(r"[.;!?](?=\s|$)", joined[match.end() :])
            hi = match.end() + end.end() if end else len(joined)
            line_no = max(no for off, no in starts if off <= match.end() - 1)
            yield line_no, joined[lo:hi], block


def run_sections(text: str) -> list[tuple[int, str]]:
    """(heading line, body) of every section whose heading contains the word run."""
    lines = text.splitlines()
    heads = [(no, len(m.group(1)), m.group(2)) for no, _, fenced in prose_lines(text)
             if not fenced and (m := HEADING.match(lines[no - 1]))]  # fmt: skip
    out = []
    for i, (no, level, title) in enumerate(heads):
        if not re.search(r"\brun\b", title, re.IGNORECASE):
            continue
        stop = len(lines) + 1
        for no2, level2, _ in heads[i + 1 :]:
            if level2 <= level:
                stop = no2
                break
        out.append((no, "\n".join(lines[no - 1 : stop - 1])))
    return out


def strip_not_run(text: str) -> str:
    for _, clause, _ in list(clauses_not_run(text)):
        text = text.replace(clause, " ")
    return text


def recorded_notebooks(docs: dict[str, str], stems: list[str]) -> dict[str, str]:
    """notebook stem -> where a run of it is recorded (path or stem is named in a run record)."""
    sources = [
        (rel, strip_not_run(text)) for rel, text in docs.items() if rel.startswith("docs/runs/")
    ]
    if "README.md" in docs:
        sources += [
            (f"README.md:{no}", strip_not_run(body)) for no, body in run_sections(docs["README.md"])
        ]
    recorded: dict[str, str] = {}
    for stem in stems:
        for where, text in sources:
            if mentions_stem(text, stem):
                recorded.setdefault(stem, where)
    return recorded


def identify_notebooks(clause: str, block, line_no: int, stems: list[str]) -> list[str]:
    explicit = [s for s in stems if mentions_stem(clause, s)]
    if explicit:
        return explicit
    words = set(re.findall(r"[a-z0-9]+", clause.lower().replace("entso-e", "entsoe")))
    by_words = [s for s in stems if key_words(s) and key_words(s) <= words]
    if by_words:
        return by_words
    if re.search(r"\b(?:the|this)\s+notebook\b", clause, re.IGNORECASE):
        near = []
        for no, line in block:
            for stem in stems:
                if mentions_stem(line, stem):
                    near.append((abs(no - line_no) + (0.5 if no > line_no else 0), stem))
        if near:
            return [min(near)[1]]
    return []


def check_notebook_run(docs: dict[str, str], files: set[str]) -> list[Failure]:
    stems = notebook_stems(files)
    recorded = recorded_notebooks(docs, stems)
    failures = []
    for rel, text in docs.items():
        for line_no, clause, block in clauses_not_run(text):
            if not re.search(r"notebook", clause, re.IGNORECASE):
                continue  # about a job or target: out of scope
            named = identify_notebooks(clause, block, line_no, stems)
            if not named:
                failures.append(
                    Failure(
                        "notebook_run",
                        rel,
                        line_no,
                        "says a notebook has not been run but names no notebook",
                    )
                )
                continue
            for stem in named:
                if stem in recorded:
                    failures.append(
                        Failure(
                            "notebook_run",
                            rel,
                            line_no,
                            f"says {stem} has not been run, "
                            f"but a run is recorded in {recorded[stem]}",
                        )
                    )
    return failures


# --------------------------------------------------------------------------- (c) task keys


def task_keys(yml_text: str) -> list[str]:
    keys: list[str] = []
    for key in re.findall(r"task_key:\s*[\"']?([\w-]+)", yml_text):
        if key not in keys:
            keys.append(key)
    return keys


def section_text(text: str, title: str) -> tuple[int, str] | None:
    lines = text.splitlines()
    for no, line in enumerate(lines, 1):
        m = HEADING.match(line)
        if m and m.group(2).strip().lower() == title.lower():
            level = len(m.group(1))
            body = []
            for line2 in lines[no:]:
                m2 = HEADING.match(line2)
                if m2 and len(m2.group(1)) <= level:
                    break
                body.append(line2)
            return no, "\n".join(body)
    return None


def has_word(text: str, word: str) -> bool:
    return re.search(rf"(?<![\w-]){re.escape(word)}(?![\w-])", text, re.IGNORECASE) is not None


def check_task_keys(docs: dict[str, str], yml_text: str | None) -> list[Failure]:
    if yml_text is None:
        return []
    keys = task_keys(yml_text)
    failures = []
    deploy = docs.get("docs/walkthrough/deploy.md")
    if deploy is None:
        failures.append(Failure("task_keys", "docs/walkthrough/deploy.md", 1, "page is missing"))
    else:
        for key in keys:
            if not has_word(deploy, key):
                failures.append(
                    Failure(
                        "task_keys",
                        "docs/walkthrough/deploy.md",
                        1,
                        f"task key {key} of databricks.yml is not named",
                    )
                )
    readme = docs.get("README.md", "")
    found = section_text(readme, "Release and deploy")
    if found is None:
        failures.append(Failure("task_keys", "README.md", 1, "no section 'Release and deploy'"))
    else:
        no, body = found
        for key in keys:
            if not has_word(body, key):
                failures.append(
                    Failure(
                        "task_keys",
                        "README.md",
                        no,
                        f"task key {key} of databricks.yml is not named in 'Release and deploy'",
                    )
                )
    return failures


# --------------------------------------------------------------------------- (d) milestones


def cells(row: str) -> list[str]:
    return [c.strip() for c in row.strip().strip("|").split("|")]


def check_milestones(docs: dict[str, str], files: set[str]) -> list[Failure]:
    readme = docs.get("README.md", "")

    def fail(no: int, msg: str) -> list[Failure]:
        return [Failure("milestones", "README.md", no, msg)]

    if MILESTONES_BEGIN not in readme or MILESTONES_END not in readme:
        return fail(1, f"no milestone table between {MILESTONES_BEGIN} and {MILESTONES_END}")
    lines = readme.splitlines()
    begin = next(i for i, ln in enumerate(lines) if MILESTONES_BEGIN in ln)
    end = next((i for i, ln in enumerate(lines) if MILESTONES_END in ln and i > begin), None)
    if end is None:
        return fail(begin + 1, "milestone block is not closed")
    rows = [(i + 1, ln) for i, ln in enumerate(lines[begin + 1: end], begin + 1)
            if ln.strip().startswith("|")]  # fmt: skip
    if len(rows) < 3:
        return fail(begin + 1, "the milestone block is not a table with a header and rows")
    header = [h.lower() for h in cells(rows[0][1])]
    pr_col = next((i for i, h in enumerate(header) if re.match(r"(pr|pull request)\b", h)), None)
    doc_re = r"docs|page|walkthrough"
    doc_col = next((i for i, h in enumerate(header) if re.search(doc_re, h)), None)
    if pr_col is None or doc_col is None:
        return fail(rows[0][0], "the table needs a PR column and a docs page column")
    data = [(no, cells(ln)) for no, ln in rows[1:] if not re.fullmatch(r"[|\s:-]+", ln.strip())]
    failures: list[Failure] = []
    names = [re.sub(r"[*_`\s]", "", c[0]).upper() if c else "" for _, c in data]
    wanted = [f"M{i}" for i in range(1, 10)]
    if names != wanted:
        found = ",".join(names) or "none"
        failures += fail(rows[0][0], f"rows must be M followed by 1 to 9 in order, found {found}")
    for (no, row), name in zip(data, names):
        if len(row) <= max(pr_col, doc_col):
            failures += fail(no, f"{name}: row has too few columns")
            continue
        pr = row[pr_col]
        sha = re.search(r"(?<![0-9A-Za-z])[0-9a-f]{7}(?![0-9A-Za-z])", pr)
        if not (re.search(r"#\d+", pr) or sha):
            failures += fail(no, f"{name}: PR column holds no #<number> or 7-hex commit: {pr!r}")
        pages = re.findall(r"docs/[^\s)`|\]]+\.md", row[doc_col])
        if not pages:
            failures += fail(no, f"{name}: docs page column names no docs/*.md path")
        for page in pages:
            if page not in files:
                failures += fail(no, f"{name}: docs page {page} does not exist")
    return failures


# --------------------------------------------------------------------------- (e) fixture name


def check_fixture_name(docs: dict[str, str]) -> list[Failure]:
    return [
        Failure("fixture_name", rel, no, f"{REMOVED_FIXTURE} is not a committed fixture")
        for rel, text in docs.items()
        for no, line in enumerate(text.splitlines(), 1)
        if REMOVED_FIXTURE in line
    ]


# --------------------------------------------------------------------------- driver


def run_checks(root: Path = DEFAULT_ROOT) -> list[Failure]:
    root = Path(root)
    files = file_set(root)
    docs = read_docs(root, files)
    yml = None
    if "databricks.yml" in files:
        yml = (root / "databricks.yml").read_text(encoding="utf-8")
    return check_all(docs, files, yml)


def check_all(docs: dict[str, str], files: set[str], yml_text: str | None) -> list[Failure]:
    found = (
        check_links(docs, files)
        + check_notebook_run(docs, files)
        + check_task_keys(docs, yml_text)
        + check_milestones(docs, files)
        + check_fixture_name(docs)
    )
    return sorted(found, key=lambda f: (f.file, f.line, f.check, f.message))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check README and docs against the repo.")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="repository root")
    parser.add_argument("--file", nargs="+", action="append", default=[], metavar="REL",
                        help="keep only the failures attributed to these files")  # fmt: skip
    parser.add_argument("--self-test", action="store_true", help="run the built-in unit cases")
    args = parser.parse_args(argv)
    if args.self_test:
        names = sorted(n for n in globals() if n.startswith("test_unit_"))
        for name in names:
            globals()[name]()
            print(f"ok {name}")
        print(f"SELFTEST: PASS {len(names)}")
        return 0
    failures = run_checks(Path(args.root))
    wanted = {r.removeprefix("./") for group in args.file for r in group}
    if wanted:
        failures = [f for f in failures if f.file in wanted]
    for failure in failures:
        print(failure.render())
    print("COHERENCE: PASS" if not failures else f"COHERENCE: FAIL {len(failures)}")
    return 1 if failures else 0


# --------------------------------------------------------------------------- unit cases

GOOD_FILES = {
    "README.md", "databricks.yml", "docs/runs/r.md", "docs/walkthrough/deploy.md",
    "docs/walkthrough/a.md", "notebooks/silver_ch_load.py", "notebooks/reconcile_daily.py",
    "src/pkg/mod.py", "tests/test_x.py",
}  # fmt: skip
GOOD_YML = "tasks:\n  - task_key: silver\n  - task_key: reconcile\n"
GOOD_TABLE = "\n".join(
    [MILESTONES_BEGIN, "| M | What | PR | Docs page |", "|---|---|---|---|"]
    + [f"| M{i} | x | {'#%d' % i if i > 1 else 'abc1234'} | `docs/walkthrough/a.md` |"
       for i in range(1, 10)]
    + [MILESTONES_END]
)  # fmt: skip
GOOD_README = (
    "# Title\n\nSee `tests/test_x.py` and `databricks.yml`; `/tmp/x` and `dbfs:/a` are skipped.\n\n"
    "## Release and deploy\n\nTasks `silver` and `reconcile` run in order.\n\n"
    f"{GOOD_TABLE}\n\n## Databricks run\n\nsilver_ch_load ran: GATE: PASS\n"
)
GOOD_DOCS = {
    "README.md": GOOD_README,
    "docs/runs/r.md": "# Run\n\nTask silver: ok\n",
    "docs/walkthrough/deploy.md": "# Deploy\n\n`silver` then `reconcile`.\n",
    "docs/walkthrough/a.md": "# A\n\nThe reconcile notebook is `notebooks/reconcile_daily.py`.\n",
}


def with_doc(rel: str, text: str) -> dict[str, str]:
    return {**GOOD_DOCS, rel: text}


def test_unit_good_case_passes():
    assert check_all(GOOD_DOCS, GOOD_FILES, GOOD_YML) == []


def test_unit_planted_bad_link_fails():
    docs = with_doc("docs/walkthrough/a.md", "See `tests/test_missing.py` for it.\n")
    found = check_all(docs, GOOD_FILES, GOOD_YML)
    assert [(f.check, f.file, f.line) for f in found] == [("links", "docs/walkthrough/a.md", 1)]


def test_unit_planted_recorded_notebook_sentence_fails():
    text = "The notebook `notebooks/silver_ch_load.py` has not been run on a workspace.\n"
    found = check_all(with_doc("docs/walkthrough/a.md", text), GOOD_FILES, GOOD_YML)
    assert [f.check for f in found] == ["notebook_run"]
    assert "silver_ch_load" in found[0].message


def test_unit_unrecorded_notebook_sentence_passes():
    text = "The reconcile notebook has not been run on a workspace.\n"
    assert check_all(with_doc("docs/walkthrough/a.md", text), GOOD_FILES, GOOD_YML) == []


def test_unit_sentence_without_notebook_name_fails():
    text = "A notebook has not been run on a workspace.\n"
    found = check_all(with_doc("docs/walkthrough/a.md", text), GOOD_FILES, GOOD_YML)
    assert [f.check for f in found] == ["notebook_run"]


def test_unit_job_sentence_is_out_of_scope():
    text = "The job has not been run on Azure yet.\n"
    assert check_all(with_doc("docs/walkthrough/a.md", text), GOOD_FILES, GOOD_YML) == []


def test_unit_planted_missing_task_key_fails():
    found = check_all(GOOD_DOCS, GOOD_FILES, GOOD_YML + "  - task_key: gold\n")
    files = sorted((f.check, f.file) for f in found)
    assert files == [("task_keys", "README.md"), ("task_keys", "docs/walkthrough/deploy.md")]


def test_unit_milestone_table_rules():
    no_table = GOOD_README.replace(GOOD_TABLE, "")
    assert [f.check for f in check_all(with_doc("README.md", no_table), GOOD_FILES, GOOD_YML)] == [
        "milestones"
    ]
    row = "| M{} | x | ".format(3)
    bad_pr = GOOD_README.replace(row + "#3 |", row + "soon |")
    found = check_all(with_doc("README.md", bad_pr), GOOD_FILES, GOOD_YML)
    assert len(found) == 1 and "M{}".format(3) in found[0].message
    missing_page = GOOD_README.replace("`docs/walkthrough/a.md`", "`docs/walkthrough/nope.md`", 1)
    found = check_all(with_doc("README.md", missing_page), GOOD_FILES, GOOD_YML)
    assert [f.check for f in found] == ["links", "milestones"]
    assert "nope.md" in found[1].message


def test_unit_removed_fixture_name_fails():
    text = f"Input: `tests/fixtures/{REMOVED_FIXTURE}`.\n"
    found = check_all(with_doc("docs/walkthrough/a.md", text), GOOD_FILES, GOOD_YML)
    assert [f.check for f in found] == ["fixture_name"]


# --------------------------------------------------------------------------- pytest wrappers

if pytest is not None:
    _DOCS = doc_files(file_set(DEFAULT_ROOT))
    _FAILURES = run_checks(DEFAULT_ROOT)

    @pytest.mark.parametrize("rel", _DOCS, ids=_DOCS)
    def test_docs_coherent(rel):
        mine = [f.render() for f in _FAILURES if f.file == rel]
        assert not mine, "\n".join(mine)

    def test_every_failure_has_a_page():
        orphans = [f.render() for f in _FAILURES if f.file not in _DOCS]
        assert not orphans, "\n".join(orphans)


if __name__ == "__main__":
    sys.exit(main())
