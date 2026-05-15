# Contracts

This directory keeps runtime contract source material that is still needed by the package. The v35.1 package keeps the active runtime release metadata, GitHub documentation and generated release assets in sync.

Canonical runtime contract registry:

```text
product/contracts/contract_registry.yaml
```

Important boundary:

```text
Worker Runtime v35.1: JSON / memory / queues / MQTT / local health snapshots
Gateway/Proxy scope:  PostgreSQL registry/auth/audit and database backup/restore
```
