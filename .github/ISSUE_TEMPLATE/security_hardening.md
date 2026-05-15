---
name: Security hardening task
about: Track non-private hardening tasks such as ACLs, PKI, backups or monitoring
title: "security-hardening: "
labels: security, hardening
assignees: ""
---

# Security hardening task

This template is for non-sensitive hardening work. Do not disclose private vulnerabilities here. Use `SECURITY.md` for vulnerability reports.

## Hardening area

```text
[ ] MQTT authentication
[ ] MQTT topic ACLs
[ ] TLS/PKI/certificate rotation
[ ] private key handling
[ ] PostgreSQL backup/restore
[ ] monitoring/alerting
[ ] runtime log handling
[ ] release artifact cleanup
[ ] negative security tests
[ ] fieldbus safety profile
```

## Current state

Describe the current behavior.

## Target state

Describe the desired secure behavior.

## Implementation notes

List the intended repository paths, scripts, configs or tests.

## Acceptance criteria

```text
[ ] No secrets are committed.
[ ] Negative test or validation procedure exists.
[ ] Documentation is updated.
[ ] Rollback path is defined.
[ ] Production-readiness gap list can be updated after completion.
```
