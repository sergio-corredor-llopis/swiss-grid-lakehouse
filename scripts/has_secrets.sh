#!/usr/bin/env bash
# Usage: H="$DATABRICKS_HOST" T="$DATABRICKS_TOKEN" bash scripts/has_secrets.sh >> "$GITHUB_OUTPUT"
# Prints ok=true on stdout when both H and T are non-empty, otherwise ok=false
# on stdout and a workflow notice on stderr. It never prints the values of H or T.
set -euo pipefail

if [ -n "${H:-}" ] && [ -n "${T:-}" ]; then
  echo "ok=true"
else
  echo "ok=false"
  echo "::notice::DATABRICKS_HOST or DATABRICKS_TOKEN is not set; the deploy job is skipped." >&2
fi
