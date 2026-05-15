# Pull Request

## Summary

Describe the change in one or two sentences.

## Change type

Select the relevant type:

```text
[ ] Bug fix
[ ] Runtime behavior change
[ ] Configuration or contract change
[ ] Security hardening
[ ] Documentation
[ ] Tests only
[ ] Tooling / installer / service template
```

## Affected area

```text
[ ] product/src runtime code
[ ] product/config runtime configuration
[ ] product/contracts or contracts
[ ] product/tests
[ ] installers or service template
[ ] documentation or reports
[ ] GitHub repository files
```

## Validation performed

Paste the exact commands and results:

```bash
python -m pytest -q
python -m compileall -q product/src product/tests installers tools
for f in installers/*.sh tools/*.sh; do bash -n "$f"; done
```

## Runtime impact

Explain whether the change affects:

```text
- runtime startup
- thread management
- event routing
- MQTT/proxy bridge
- runtime command binding
- fieldbus adapters
- compiled configuration/contracts
- systemd installation
```

## Security impact

```text
[ ] No security impact expected
[ ] Security hardening included
[ ] Requires private security review
[ ] Touches credentials, certificates, TLS, MQTT ACLs or deployment scripts
```

Security-sensitive details must not be posted publicly. Use `SECURITY.md` for vulnerability handling.

## Production readiness impact

```text
[ ] No change to production-readiness status
[ ] Moves one hardening item closer to completion
[ ] Requires documentation update
[ ] Requires fieldbus/MQTT/PostgreSQL/operator validation
```

## Checklist

```text
[ ] I did not include secrets, private keys, credentials or local logs.
[ ] I did not include _private/, __pycache__/ or .pytest_cache/ artifacts.
[ ] YAML/JSON files are syntactically valid.
[ ] Tests were added or updated where behavior changed.
[ ] Documentation was updated where operations changed.
[ ] Rollback or mitigation is clear.
```
