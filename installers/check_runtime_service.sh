#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

<<<<<<< HEAD
SERVICE_NAME="${SERVICE_NAME:-product-runtime-v34-3.service}"
RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v34_3_preproduction_final_runtime}"
=======
SERVICE_NAME="${SERVICE_NAME:-product-runtime-v35-1.service}"
RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v35_1_preproduction_final_runtime}"
RUNTIME_PYTHON_OVERRIDE="${RUNTIME_PYTHON:-}"
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)

while [ $# -gt 0 ]; do
  case "$1" in
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
<<<<<<< HEAD
=======
    --runtime-python) RUNTIME_PYTHON_OVERRIDE="$2"; shift 2 ;;
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

<<<<<<< HEAD
=======
if [ -n "$RUNTIME_PYTHON_OVERRIDE" ]; then
  RUNTIME_PYTHON="$RUNTIME_PYTHON_OVERRIDE"
elif [ -x "${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python" ]; then
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python"
else
  RUNTIME_PYTHON="python3"
fi

>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
systemctl is-active --quiet "$SERVICE_NAME"
systemctl status "$SERVICE_NAME" --no-pager
journalctl -u "$SERVICE_NAME" -n 200 --no-pager

if [ -f "${RUNTIME_ROOT}/product/logs/system_logs/system.log" ]; then
  tail -n 80 "${RUNTIME_ROOT}/product/logs/system_logs/system.log" || true
else
  printf 'Runtime system log file is not present yet: %s\n' "${RUNTIME_ROOT}/product/logs/system_logs/system.log"
fi
<<<<<<< HEAD
=======

if [ -f "${RUNTIME_ROOT}/product/logs/system_logs/runtime_health.json" ]; then
  "$RUNTIME_PYTHON" "${RUNTIME_ROOT}/tools/runtime_healthcheck.py" \
    --health-file "${RUNTIME_ROOT}/product/logs/system_logs/runtime_health.json" \
    --expected-version v35.1 \
    --max-age-s 180
fi
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
