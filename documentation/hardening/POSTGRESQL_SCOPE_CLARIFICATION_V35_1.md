# PostgreSQL Scope Clarification v35.1

## Diagnosis

The v35.1 worker runtime has no local PostgreSQL adapter, no local database connection setup and no database migration path. Runtime data flow is based on:

```text
JSON configuration / device-state input
in-memory resource registry
thread-safe queues
MQTT/MQTTS proxy bridge
local runtime health JSON
local log files
```

PostgreSQL references in previous documentation described the external Product Proxy Gateway chain, where PostgreSQL can provide registry, authentication, session and audit persistence.

## Correct ownership

```text
Component                       PostgreSQL responsibility
Worker Runtime v35.1            No local PostgreSQL dependency
Product Proxy Gateway           PostgreSQL registry/auth/audit owner
Runtime Host                    Only observes worker health and MQTT bridge state
Release Candidate Sign-off      Gateway/PostgreSQL backup/restore validated separately
```

## Corrected backlog item

The previous item:

```text
PostgreSQL Backup/Restore praktisch validieren.
```

is now scoped as:

```text
Validate PostgreSQL backup/restore on the external Proxy/Gateway instance. Do not add PostgreSQL to the worker runtime unless a future contract explicitly introduces worker-side database persistence.
```

## Runtime health marker

The worker reports this scope as:

```text
gateway_proxy_external_not_worker_runtime_dependency
```
