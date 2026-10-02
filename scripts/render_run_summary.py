#!/usr/bin/env python3
"""Render the text a Databricks job printed as a short Markdown run summary.

Usage: python scripts/render_run_summary.py INPUT --date YYYY-MM-DD --region TEXT
       (INPUT is a file, or - for standard input)

The summary holds the date, the region, the Spark version from a printed
`RUN spark_version=<v> ...` line, the serverless environment version when one
is printed, and the per-run lines (row counts, GATE, merged, HOURLY, COMPARE,
DAY and RECONCILE) verbatim, each in a fenced block.

Before anything is printed, every line is checked for a workspace host, an
access token or a GUID. On a match the script prints `LEAK: <name> at line <n>`
and nothing else to standard output, without repeating the matched text.

Exit codes: 0 clean, 2 unreadable input or bad option, 3 a leak pattern matched.
Standard library only.
"""

import argparse
import re
import sys
from pathlib import Path

LEAK_PATTERNS = (
    ("workspace-id", re.compile(r"adb-\d")),
    ("access-token", re.compile(r"dapi[0-9a-fA-F]{8}")),
    ("workspace-host", re.compile(r"azuredatabricks\.net")),
    (
        "guid",
        re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"),
    ),
)

RUN_LINE = re.compile(r"^\s*RUN\s")
SPARK_VERSION = re.compile(r"\bspark_version=(\S+)")
ENV_VERSION = re.compile(r"\b(?:serverless_)?environment_version=(\S+)")
SECTION = re.compile(r"^\S.*\(\d{2}:\d{2}:\d{2}\):\s*$")
KEEP = re.compile(
    r"^\s*(?:wrote \d+ rows|skipped \d+ rows|GATE:|merged\s|HOURLY\b|COMPARE\b|DAY\b|RECONCILE\b)"
)
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def find_leaks(lines):
    """Return (pattern name, 1-based line number) pairs; never the matched text."""
    found = []
    for number, line in enumerate(lines, start=1):
        for name, pattern in LEAK_PATTERNS:
            if pattern.search(line):
                found.append((name, number))
    return found


def first_group(pattern, lines):
    for line in lines:
        if RUN_LINE.match(line):
            match = pattern.search(line)
            if match:
                return match.group(1)
    return None


def blocks(lines):
    """Group the lines worth keeping into blocks: a section header starts a new block."""
    result, current = [], []
    for line in lines:
        if RUN_LINE.match(line):
            continue
        if SECTION.match(line):
            if current:
                result.append(current)
            current = [line.rstrip()]
        elif KEEP.match(line):
            current.append(line.rstrip())
        elif not line.strip() and current:
            result.append(current)
            current = []
    if current:
        result.append(current)
    return [b for b in result if any(KEEP.match(x) for x in b)]


def render(lines, date, region):
    spark = first_group(SPARK_VERSION, lines) or "not recorded"
    out = [
        "# Run summary",
        "",
        "- Date: " + date,
        "- Region: " + region,
        "- spark.version: " + spark,
    ]
    env = first_group(ENV_VERSION, lines)
    if env:
        out.append("- Serverless environment version: " + env)
    for block in blocks(lines):
        out += ["", "```", *block, "```"]
    return "\n".join(out) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Render a Databricks job output as a Markdown run summary."
    )
    parser.add_argument("input", help="job output text file, or - for standard input")
    parser.add_argument("--date", required=True, help="run date, YYYY-MM-DD")
    parser.add_argument("--region", required=True, help="workspace region, free text")
    args = parser.parse_args(argv)
    if not DATE.match(args.date):
        print("error: --date must look like YYYY-MM-DD", file=sys.stderr)
        return 2
    try:
        text = (
            sys.stdin.read() if args.input == "-" else Path(args.input).read_text(encoding="utf-8")
        )
    except (OSError, UnicodeDecodeError) as exc:
        print("error: cannot read input (%s)" % type(exc).__name__, file=sys.stderr)
        return 2
    lines = text.splitlines()
    leaks = find_leaks(lines)
    if leaks:
        for name, number in leaks:
            print("LEAK: %s at line %d" % (name, number))
        return 3
    sys.stdout.write(render(lines, args.date, args.region))
    return 0


if __name__ == "__main__":
    sys.exit(main())
