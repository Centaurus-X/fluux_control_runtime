# v35.1 Repository and Runtime Diagnosis

## Summary

v35.1 fixes the previous version-marker inconsistency and integrates the final preproduction hardening points that can be implemented worker-side without changing the runtime architecture.

## Worker architecture diagnosis

```text
Entrypoint: product/src/sync_xserver_main.py
Data model: JSON/config contracts + in-memory resources
Coordination: thread-safe queues and event router
External bridge: MQTT/MQTTS Proxy Worker Bridge
Database: no local PostgreSQL worker dependency
```

## Implemented hardening

```text
MQTT authentication: configurable, environment secrets supported
Topic ACL: worker-side publish/subscribe guard implemented
PKI/mTLS: configurable mode with required-mode validation
Monitoring: runtime health JSON writer and CLI checker
Logging: systemd/journald/logrotate helpers
Fieldbus: active simulation subset configurable
Release hygiene: v35.1 marker, manifest/checksum regeneration planned
```

## PostgreSQL finding

No worker-side PostgreSQL integration exists in source code. Previous PostgreSQL backup/restore notes referred to the external Proxy/Gateway stack. v35.1 documents this explicitly and reports it in runtime health snapshots.

## Open production items

```text
- EMQX broker-side users and ACLs.
- Production PKI and cert rotation execution.
- Gateway/PostgreSQL backup/restore validation.
- 24h+ soak test with EMQX/runtime restarts.
- Full fieldbus matrix validation or explicit optional-controller acceptance.
```
