# GitHub Release Preparation v35.1

## Repository root

Commit the full repository root, not only `product/`.

## Excluded local-only paths

Do not commit:

```text
_private/
product/logs/**/*.log
product/logs/runtime_health.json
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
Prepare runtime v35.1 preproduction final release
```

## Suggested release title

```text
Runtime v35.1 Preproduction Final Runtime
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

## Convenient upload helper

Use the external script generated next to the release archive:

```text
git_merge_commit_upload_v35_1.ps1
```

The script is intentionally outside the project directory so it cannot accidentally become part of the runtime release package.
