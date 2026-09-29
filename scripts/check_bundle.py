"""Check databricks.yml against the Databricks bundle JSON schema, offline.

Usage: python scripts/check_bundle.py [FILE]

The schema comes from `databricks bundle schema`, which needs the Databricks CLI but no login,
no workspace and no token. The schema uses the regex classes \\p{L} and \\p{N}, which Python's
`re` does not support, so they are rewritten to `[^\\W\\d_]` and `\\d` before validation.

Exit status: 0 when the file is valid, 1 when it has schema errors, 2 when the CLI or the file
is missing.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

DEFAULT_FILE = Path(__file__).resolve().parent.parent / "databricks.yml"


def load_schema() -> dict:
    """Return the bundle schema with Python-compatible regex classes."""
    out = subprocess.run(
        ["databricks", "bundle", "schema"], check=True, capture_output=True, text=True
    ).stdout
    out = out.replace("\\\\p{L}", "[^\\\\W\\\\d_]").replace("\\\\p{N}", "\\\\d")
    return json.loads(out)


def main(argv: list[str]) -> int:
    path = Path(argv[1]) if len(argv) > 1 else DEFAULT_FILE
    if shutil.which("databricks") is None:
        print("databricks CLI not found on PATH")
        return 2
    if not path.is_file():
        print(f"file not found: {path}")
        return 2
    try:
        schema = load_schema()
    except subprocess.CalledProcessError as exc:
        print(f"databricks bundle schema failed: {exc.stderr.strip()}")
        return 2
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    errors = sorted(
        Draft202012Validator(schema).iter_errors(document), key=lambda e: list(e.absolute_path)
    )
    if not errors:
        print("BUNDLE schema ok")
        return 0
    for error in errors:
        location = "/".join(str(p) for p in error.absolute_path) or "<root>"
        print(f"{location}: {error.message}")
    print(f"BUNDLE schema errors={len(errors)}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
