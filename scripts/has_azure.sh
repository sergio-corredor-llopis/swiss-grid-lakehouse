#!/usr/bin/env bash
# Usage: H="$DATABRICKS_HOST" C="$DATABRICKS_CLIENT_ID" bash scripts/has_azure.sh >> "$GITHUB_OUTPUT"
# Prints ok=true on stdout when both H and C are non-empty, otherwise ok=false
# on stdout and a workflow notice on stderr. It never prints the values of H or C.
set -euo pipefail

if [ -n "${H:-}" ] && [ -n "${C:-}" ]; then
  echo "ok=true"
else
  echo "ok=false"
  echo "::notice::The environment variables DATABRICKS_HOST or DATABRICKS_CLIENT_ID are not set; the Azure deploy steps are skipped." >&2
fi
