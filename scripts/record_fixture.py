#!/usr/bin/env python3
"""Record one real ENTSO-E CH actual-load response as a test fixture.

Usage:
    export ENTSOE_API_TOKEN=...   # or ENTSOE_TOKEN
    python scripts/record_fixture.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                                      [--out PATH] [--force]

Run this by hand with your own ENTSO-E token. CI never runs it: the token must
never enter a repo file or a log. It reads the token only from the environment,
writes the raw ENTSO-E response bytes to disk unmodified, and prints one
summary line.

To get a token: register on the ENTSO-E Transparency Platform and request
RESTful API access, as described in the platform's RESTful API guide, then
export it as ENTSOE_API_TOKEN.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_START = "2026-09-01"
DEFAULT_END = "2026-09-03"
DEFAULT_OUT = "tests/fixtures/entsoe_ch_load_sample.xml"
CH_AREA = "10YCH-SWISSGRIDZ"

TOKEN_HELP = "See the ENTSO-E Transparency Platform RESTful API guide for how to request a token."


def _read_token() -> str | None:
    """Read the ENTSO-E token from the environment only.

    Prefers ENTSOE_API_TOKEN, falls back to ENTSOE_TOKEN. Never reads a
    repo file, never has a default, never prints the value.
    """
    return os.environ.get("ENTSOE_API_TOKEN") or os.environ.get("ENTSOE_TOKEN")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record one real ENTSO-E CH actual-load response to a test "
            "fixture file. Run by hand with your own token."
        )
    )
    parser.add_argument(
        "--start", default=DEFAULT_START, help="Window start (Europe/Zurich date, YYYY-MM-DD)."
    )
    parser.add_argument(
        "--end", default=DEFAULT_END, help="Window end (Europe/Zurich date, YYYY-MM-DD)."
    )
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output fixture path.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing fixture file.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)

    token = _read_token()
    if not token:
        print(
            "ERROR: no ENTSO-E token found in the environment. Set "
            "ENTSOE_API_TOKEN (preferred) or ENTSOE_TOKEN before running "
            f"this script. {TOKEN_HELP}",
            file=sys.stderr,
        )
        return 2

    out_path = Path(args.out)
    if out_path.exists() and not args.force:
        print(
            f"ERROR: fixture already exists at {out_path} -- refusing to "
            "silently overwrite it. Pass --force to overwrite.",
            file=sys.stderr,
        )
        return 4

    try:
        import pandas as pd
        from entsoe import EntsoeRawClient
    except ImportError as exc:  # pragma: no cover - environment problem, not logic
        print(
            f"ERROR: could not import entsoe/pandas ({exc}). Install the "
            'project first: pip install -e ".[dev]"',
            file=sys.stderr,
        )
        return 2

    start = pd.Timestamp(args.start, tz="Europe/Zurich")
    end = pd.Timestamp(args.end, tz="Europe/Zurich")

    client = EntsoeRawClient(api_key=token)
    text = client.query_load(CH_AREA, start=start, end=end)

    if (
        not text
        or "GL_MarketDocument" not in text
        or "A16" not in text
        or len(text.encode("utf-8")) <= 1000
    ):
        print(
            "ERROR: ENTSO-E response is empty or does not look like a "
            "valid A16 GL_MarketDocument -- nothing written. Check the "
            "token and the requested window.",
            file=sys.stderr,
        )
        return 3

    out_path.parent.mkdir(parents=True, exist_ok=True)
    raw_bytes = text.encode("utf-8")
    out_path.write_bytes(raw_bytes)

    digest = hashlib.sha256(raw_bytes).hexdigest()
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(
        f"RECORDED {timestamp} bytes={len(raw_bytes)} sha256={digest} "
        f"window={args.start}..{args.end}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
