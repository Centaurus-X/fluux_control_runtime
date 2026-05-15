# Runtime v35.1 Documentation Index

```text
Release marker: v35_1_preproduction_final_runtime
Runtime event:  V35_1_PROXY_RUNTIME_COMMAND_RECEIVED
Entrypoint:     product/src/sync_xserver_main.py
```

## Core documents

```text
release/PREPRODUCTION_STATUS_V35_1.md
release/RELEASE_NOTES_V35_1.md
operations/RUNTIME_ROLLOUT_GUIDE_V35_1.md
operations/RUNTIME_TEST_PLAN_V35_1.md
hardening/ONE_STEP_FINISHING_PLAN_V35_1.md
hardening/POSTGRESQL_SCOPE_CLARIFICATION_V35_1.md
architecture/RUNTIME_PROXY_MODBUS_INTEGRATION.md
```

## Important scope note

The worker runtime is memory/JSON/queue/MQTT based. PostgreSQL backup/restore is owned by the external Proxy/Gateway deployment and is not a worker-local database task.
