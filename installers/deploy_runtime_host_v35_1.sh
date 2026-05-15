#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v35_1_preproduction_final_runtime}"
SERVICE_NAME="${SERVICE_NAME:-product-runtime-v35-1.service}"
RUNTIME_USER="${RUNTIME_USER:-${USER}}"
RUNTIME_PYTHON="${RUNTIME_PYTHON:-python3}"
SOURCE_DIR=""
ARCHIVE_PATH=""
SKIP_SERVICE="false"
SKIP_TESTS="false"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

while [ $# -gt 0 ]; do
  case "$1" in
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --runtime-user) RUNTIME_USER="$2"; shift 2 ;;
    --runtime-python) RUNTIME_PYTHON="$2"; shift 2 ;;
    --source-dir) SOURCE_DIR="$2"; shift 2 ;;
    --archive) ARCHIVE_PATH="$2"; shift 2 ;;
    --skip-service) SKIP_SERVICE="true"; shift ;;
    --skip-tests) SKIP_TESTS="true"; shift ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

if [ -z "$SOURCE_DIR" ]; then
  SOURCE_DIR="$PROJECT_DIR"
fi

copy_source_tree() {
  local source_dir="$1"
  local target_dir="$2"
  sudo mkdir -p "$target_dir"
  if command -v rsync >/dev/null 2>&1; then
    sudo rsync -a --delete \
      --exclude '.git' \
      --exclude '_private' \
      --exclude '__pycache__' \
      --exclude '.pytest_cache' \
      --exclude '*.pyc' \
      --exclude 'product/logs/system_logs/*.log' \
      --exclude 'product/logs/system_logs/runtime_health.json' \
      "${source_dir}/" "${target_dir}/"
  else
    (cd "$source_dir" && tar \
      --exclude='.git' \
      --exclude='_private' \
      --exclude='__pycache__' \
      --exclude='.pytest_cache' \
      --exclude='*.pyc' \
      -cf - .) | (cd "$target_dir" && sudo tar -xf -)
  fi
}

extract_archive() {
  local archive_path="$1"
  local target_dir="$2"
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  if command -v unzip >/dev/null 2>&1; then
    unzip -q "$archive_path" -d "$tmp_dir"
  else
    python3 - <<PY
from zipfile import ZipFile
ZipFile("$archive_path").extractall("$tmp_dir")
PY
  fi
  local first_dir
  first_dir="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  if [ -z "$first_dir" ]; then
    printf 'Archive did not contain a top-level project directory: %s\n' "$archive_path" >&2
    exit 1
  fi
  copy_source_tree "$first_dir" "$target_dir"
  rm -rf "$tmp_dir"
}

if [ -n "$ARCHIVE_PATH" ]; then
  extract_archive "$ARCHIVE_PATH" "$RUNTIME_ROOT"
else
  copy_source_tree "$SOURCE_DIR" "$RUNTIME_ROOT"
fi

sudo chown -R "${RUNTIME_USER}:${RUNTIME_USER}" "$RUNTIME_ROOT"

if [ ! -x "${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python" ]; then
  sudo -u "$RUNTIME_USER" "$RUNTIME_PYTHON" -m venv "${RUNTIME_ROOT}/.venv-runtime-v35_1"
fi

sudo -u "$RUNTIME_USER" "${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python" -m pip install --upgrade pip setuptools wheel
if [ -f "${RUNTIME_ROOT}/product/requirements.txt" ]; then
  sudo -u "$RUNTIME_USER" "${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python" -m pip install -r "${RUNTIME_ROOT}/product/requirements.txt"
fi

if [ "$SKIP_TESTS" != "true" ]; then
  bash "${RUNTIME_ROOT}/installers/run_runtime_smoke_tests.sh" \
    --runtime-root "$RUNTIME_ROOT" \
    --runtime-python "${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python"
fi

bash "${RUNTIME_ROOT}/installers/install_runtime_logrotate.sh" --runtime-root "$RUNTIME_ROOT" || true

if [ "$SKIP_SERVICE" != "true" ]; then
  bash "${RUNTIME_ROOT}/installers/install_runtime_service.sh" \
    --runtime-root "$RUNTIME_ROOT" \
    --service-name "$SERVICE_NAME" \
    --runtime-user "$RUNTIME_USER" \
    --runtime-python "${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python"
fi

printf 'Runtime v35.1 deployment completed at: %s\n' "$RUNTIME_ROOT"
