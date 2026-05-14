# GitHub Release Preparation v34.3

## Repository root

Commit the full repository root, not only `product/`.

## Excluded local-only paths

Do not commit:

```text
_private/
product/logs/**/*.log
product/config/ssl/certs/mqtt/*.pem
product/config/ssl/certs/mqtt/*.key
__pycache__/
.pytest_cache/
```

## License model

```text
GPL-3.0-or-later OR Commercial License
```

Include:

```text
LICENSE.md
COMMERCIAL_LICENSE.md
```

## Suggested commit message

```text
Prepare runtime v34.3 preproduction release
```

## Suggested release title

```text
Runtime v34.3 Preproduction Final Runtime
```

## Release statement

```text
This release is pre-production ready and suitable for controlled production tests.
It is not yet declared final unattended production-ready.
```

## Last local checks before commit

```bash
bash tools/purge_pycache.sh
python -m compileall -q product/src product/tests tools
bash installers/run_runtime_smoke_tests.sh --runtime-root "$PWD"
```
