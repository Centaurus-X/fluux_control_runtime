#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

<<<<<<< HEAD
RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v34_3_preproduction_final_runtime}"
=======
RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v35_1_preproduction_final_runtime}"
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
RUNTIME_PYTHON_OVERRIDE="${RUNTIME_PYTHON:-}"

while [ $# -gt 0 ]; do
  case "$1" in
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
    --runtime-python) RUNTIME_PYTHON_OVERRIDE="$2"; shift 2 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

if [ -n "$RUNTIME_PYTHON_OVERRIDE" ]; then
  RUNTIME_PYTHON="$RUNTIME_PYTHON_OVERRIDE"
<<<<<<< HEAD
elif [ -x "${RUNTIME_ROOT}/.venv-runtime-v34_3/bin/python" ]; then
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v34_3/bin/python"
elif [ -x "${RUNTIME_ROOT}/.venv-runtime-v34_2/bin/python" ]; then
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v34_2/bin/python"
else
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v34_1/bin/python"
=======
elif [ -x "${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python" ]; then
  RUNTIME_PYTHON="${RUNTIME_ROOT}/.venv-runtime-v35_1/bin/python"
else
  RUNTIME_PYTHON="python3"
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
fi

cd "${RUNTIME_ROOT}/product"
"$RUNTIME_PYTHON" -m compileall -q src tests
"$RUNTIME_PYTHON" -m pytest -q \
  tests/test_v32_proxy_worker_bridge.py \
  tests/test_v33_runtime_command_binding.py \
<<<<<<< HEAD
  tests/test_v34_preproduction_runtime_binding.py \
  tests/test_v34_preproduction_final_runtime_extra.py
=======
  tests/test_v35_1_preproduction_runtime_binding.py \
  tests/test_v35_1_preproduction_final_runtime_extra.py \
  tests/test_v35_1_security_monitoring_fieldbus.py
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
