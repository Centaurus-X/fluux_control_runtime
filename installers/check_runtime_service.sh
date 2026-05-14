#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

SERVICE_NAME="${SERVICE_NAME:-product-runtime-v34-3.service}"
RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v34_3_preproduction_final_runtime}"

while [ $# -gt 0 ]; do
  case "$1" in
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

systemctl is-active --quiet "$SERVICE_NAME"
systemctl status "$SERVICE_NAME" --no-pager
journalctl -u "$SERVICE_NAME" -n 200 --no-pager

if [ -f "${RUNTIME_ROOT}/product/logs/system_logs/system.log" ]; then
  tail -n 80 "${RUNTIME_ROOT}/product/logs/system_logs/system.log" || true
else
  printf 'Runtime system log file is not present yet: %s\n' "${RUNTIME_ROOT}/product/logs/system_logs/system.log"
fi
