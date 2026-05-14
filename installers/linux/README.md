# Linux Setup Notes

The runtime requires Python 3.14 free-threading / no-GIL for the real service start path.

If the host already has a working interpreter, use it to create `.venv-runtime-v34_3`:

```bash
RUNTIME_PYTHON=/opt/python-nogil/current/bin/python3
$RUNTIME_PYTHON -m venv .venv-runtime-v34_3
. .venv-runtime-v34_3/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r product/requirements.txt
python installers/check_python_free_threading.py
```

A hardened, distribution-specific Python build installer should be maintained separately or imported from the infrastructure repository once the final production toolchain is frozen.
