#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v34_3_preproduction_final_runtime}"
SERVICE_NAME="${SERVICE_NAME:-product-runtime-v34-3.service}"
RUNTIME_USER="${RUNTIME_USER:-${USER}}"
RUNTIME_PYTHON_OVERRIDE="${RUNTIME_PYTHON:-}"

while [ $# -gt 0 ]; do
  case "$1" in
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --runtime-user) RUNTIME_USER="$2"; shift 2 ;;
    --runtime-python) RUNTIME_PYTHON_OVERRIDE="$2"; shift 2 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

if [ -n "$RUNTIME_PYTHON_OVERRIDE" ]; then
  RUNTIME_PYTHON="$RUNTIME_PYTHON_OVERRIDE"
elif [ -x "${RUNTIME_ROOT}/.venv-runtime-v34_3/bin/python" ]; then
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v34_3/bin/python"
elif [ -x "${RUNTIME_ROOT}/.venv-runtime-v34_2/bin/python" ]; then
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v34_2/bin/python"
else
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v34_1/bin/python"
fi

if [ ! -x "$RUNTIME_PYTHON" ]; then
  printf 'Runtime Python is missing or not executable: %s\n' "$RUNTIME_PYTHON" >&2
  exit 1
fi

if ! "$RUNTIME_PYTHON" "${RUNTIME_ROOT}/installers/check_python_free_threading.py"; then
  printf 'Runtime Python is not a free-threading/no-GIL build.\n' >&2
  exit 1
fi

sudo mkdir -p "${RUNTIME_ROOT}/product/logs/system_logs"
sudo touch "${RUNTIME_ROOT}/product/logs/system_logs/system.log"
sudo chown -R "${RUNTIME_USER}:${RUNTIME_USER}" "${RUNTIME_ROOT}/product/logs"

pkill -f 'src/sync_xserver_main.py' 2>/dev/null || true
pkill -f 'sync_xserver_main.py' 2>/dev/null || true

UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}"
sudo tee "$UNIT_PATH" >/dev/null <<EOF
[Unit]
Description=Product Runtime v34.3 Preproduction Worker fn01
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUNTIME_USER}
WorkingDirectory=${RUNTIME_ROOT}/product
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=PYTHONUNBUFFERED=1
ExecStart=${RUNTIME_PYTHON} ${RUNTIME_ROOT}/product/src/sync_xserver_main.py
Restart=on-failure
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"
printf 'Runtime service installed and restarted: %s\n' "$SERVICE_NAME"
