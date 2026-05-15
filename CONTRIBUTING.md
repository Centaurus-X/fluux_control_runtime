# Contributing

Thank you for helping improve this runtime project. Contributions should be precise, testable and aligned with the current v35.1 pre-production release boundary.

## Current project state

```text
Status: pre-production ready / controlled pilot ready
Primary runtime: product/src/sync_xserver_main.py
Runtime contract marker: v35_1_preproduction_final_runtime
Final unattended production readiness: not yet declared
```

Before working on production-readiness topics, read:

```text
documentation/release/PREPRODUCTION_STATUS_V35_1.md
documentation/hardening/PRODUCTION_READINESS_GAP_LIST.md
documentation/hardening/ONE_STEP_FINISHING_PLAN_V35_1.md
reports/acceptance/V35_1_PREPRODUCTION_ACCEPTANCE_REPORT.md
Diff-System-Report.md
```

## Contribution workflow

```text
1. Open an issue with the matching template.
2. Describe expected behavior, current behavior and target component.
3. Keep changes small enough to review.
4. Add or update tests when behavior changes.
5. Run the local validation commands.
6. Open a pull request with the PR template filled out.
```

## Local validation

Run these checks before opening a pull request:

```bash
python -m pytest -q
python -m compileall -q product/src product/tests installers tools
for f in installers/*.sh tools/*.sh; do bash -n "$f"; done
```

When the optional quality tools are installed, also run the repository standards checks:

```bash
python tools/_check_code_specific_standards.py check \
  --cwd . \
  --paths product/src product/tests tools installers \
  --pretty \
  --allow-findings \
  --allow-errors

python tools/_check_code_standards.py suite \
  --cwd . \
  --paths product/src product/tests tools installers \
  --pretty \
  --allow-errors
```

## Runtime and architecture rules

Keep runtime changes compatible with the existing architecture:

```text
- preserve deterministic queue and event-routing behavior
- preserve v35.1 runtime contract marker compatibility
- keep legacy V34/V33/V32 command events accepted unless a contract removes them
- keep MQTT authentication, mTLS and topic ACL behavior explicit and configurable
- keep fieldbus controller activation profile-driven
- do not add worker-side PostgreSQL persistence without a new contract
- do not introduce hidden network side effects in tests
- do not hardcode credentials, certificates, hostnames or customer paths
- prefer small composable functions and explicit error handling
- keep configuration in YAML/JSON files, not in runtime code
- document operational changes in documentation/operations or documentation/hardening
```

## Testing expectations

A contribution that changes behavior should include one of the following:

```text
- a unit test in product/tests
- an integration-style test for the affected runtime path
- a documented manual validation step if automated testing is not currently practical
```

For MQTT, mTLS, systemd, certificate, fieldbus or Gateway/PostgreSQL changes, include exact environment assumptions and whether the test uses simulation or real infrastructure.

## Security rules

Do not commit:

```text
- private keys
- certificates with private material
- passwords or tokens
- production host secrets
- customer data
- local logs containing sensitive runtime payloads
- _private/ development history
- __pycache__/ or .pytest_cache/ artifacts
```

Security vulnerabilities must be handled through `SECURITY.md`, not through public issues.

## Documentation rules

Update documentation when a change affects:

```text
- startup or installation
- runtime configuration
- MQTT topics or security
- mTLS/PKI handling
- fieldbus targets
- production readiness state
- release boundary
- operational monitoring or backup behavior
```

## Pull request quality bar

A pull request is ready for review when:

```text
- the PR template is complete
- tests pass or known limitations are explained
- changed configuration files are valid YAML/JSON
- no ignored cache/private files are included
- security-sensitive material is absent
- the change has a clear rollback path
```
