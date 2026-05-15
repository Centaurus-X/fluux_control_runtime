# v35.1 Preproduction Acceptance Report

## Result

```text
Status: accepted for controlled pre-production / pilot deployment
Unattended production: pending final hardening and soak validation
```

## Local validation

```text
compileall: OK
pytest: 344 passed
bash syntax: OK for installer scripts
YAML parse: OK
```

## Accepted scope

```text
- Runtime worker source and configuration.
- Runtime command binding v35.1.
- Proxy Worker Bridge with configurable MQTT auth, ACL and TLS/mTLS behavior.
- Runtime health JSON for monitoring integration.
- Fieldbus active-controller profile for simulation subset.
- Linux deployment/service/logrotate/smoke-test helpers.
```

## Corrected PostgreSQL scope

PostgreSQL is accepted as an external Proxy/Gateway responsibility. No worker-local PostgreSQL dependency is introduced or required in v35.1.

## Remaining sign-off items

```text
- Broker-side ACL/user enforcement.
- Production PKI rollout.
- Gateway/PostgreSQL backup and restore drill.
- 24h+ soak test with restart events.
- Full fieldbus matrix validation or documented optional-controller state.
```
