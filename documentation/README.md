<<<<<<< HEAD
# Runtime v34.3 Documentation Index

This documentation set describes the current runtime package as **Pre-Production Ready** and **ready for a controlled production test**.

It does not claim final unattended production readiness. The remaining hardening work is listed explicitly.

## Recommended reading order

```text
1. ../README.md
2. directory_structure.md
3. release/PREPRODUCTION_STATUS_V34_3.md
4. operations/RUNTIME_ROLLOUT_GUIDE_V34_3.md
5. operations/RUNTIME_TEST_PLAN_V34_3.md
6. architecture/RUNTIME_PROXY_MODBUS_INTEGRATION.md
7. hardening/PRODUCTION_READINESS_GAP_LIST.md
8. hardening/QUICK_AND_DIRTY_HARDENING_PATCH_PLAN.md
9. github/REPOSITORY_STRATEGY.md
10. github/GITHUB_RELEASE_PREPARATION.md
```

## Active status

```text
Pre-Production Ready: yes
Controlled production test ready: yes
Final unattended production ready: no
```

## Publication rule

Publish the complete repository root, excluding `_private/`. Do not publish only `product/`.
=======
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
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
