#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v35_1_preproduction_final_runtime}"
LOGROTATE_NAME="${LOGROTATE_NAME:-product-runtime-v35-1}"
RUNTIME_USER="${RUNTIME_USER:-${USER}}"

while [ $# -gt 0 ]; do
  case "$1" in
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
    --logrotate-name) LOGROTATE_NAME="$2"; shift 2 ;;
    --runtime-user) RUNTIME_USER="$2"; shift 2 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

sudo tee "/etc/logrotate.d/${LOGROTATE_NAME}" >/dev/null <<EOF
${RUNTIME_ROOT}/product/logs/system_logs/*.log {
    daily
    rotate 14
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    create 0640 ${RUNTIME_USER} ${RUNTIME_USER}
}
EOF

printf 'Logrotate config installed: /etc/logrotate.d/%s\n' "$LOGROTATE_NAME"
