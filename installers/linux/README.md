# Linux Setup Notes v35.1

The runtime service path expects a Python free-threading / no-GIL interpreter for the real host deployment path.

Recommended virtual environment name:

```bash
RUNTIME_PYTHON=/opt/python-nogil/current/bin/python3
$RUNTIME_PYTHON -m venv .venv-runtime-v35_1
. .venv-runtime-v35_1/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r product/requirements.txt
python installers/check_python_free_threading.py
```

Convenience deployment:

```bash
bash installers/deploy_runtime_host_v35_1.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
  --runtime-python /opt/python-nogil/current/bin/python3 \
  --runtime-user product-runtime
```

Operational checks:

```bash
bash installers/run_runtime_smoke_tests.sh --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
bash installers/run_runtime_healthcheck.sh --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
bash installers/check_runtime_service.sh --service-name product-runtime-v35-1.service
```
