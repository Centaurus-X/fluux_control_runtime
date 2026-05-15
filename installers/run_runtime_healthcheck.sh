#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v35_1_preproduction_final_runtime}"
RUNTIME_PYTHON_OVERRIDE="${RUNTIME_PYTHON:-}"
HEALTH_FILE="${HEALTH_FILE:-}"
MAX_AGE_S="${MAX_AGE_S:-180}"

while [ $# -gt 0 ]; do
  case "$1" in
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
    --runtime-python) RUNTIME_PYTHON_OVERRIDE="$2"; shift 2 ;;
    --health-file) HEALTH_FILE="$2"; shift 2 ;;
    --max-age-s) MAX_AGE_S="$2"; shift 2 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

if [ -z "$HEALTH_FILE" ]; then
  HEALTH_FILE="${RUNTIME_ROOT}/product/logs/system_logs/runtime_health.json"
fi

if [ -n "$RUNTIME_PYTHON_OVERRIDE" ]; then
  RUNTIME_PYTHON="$RUNTIME_PYTHON_OVERRIDE"
elif [ -x "${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python" ]; then
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python"
else
  RUNTIME_PYTHON="python3"
fi

"$RUNTIME_PYTHON" "${RUNTIME_ROOT}/tools/runtime_healthcheck.py" \
  --health-file "$HEALTH_FILE" \
  --expected-version v35.1 \
  --max-age-s "$MAX_AGE_S"
