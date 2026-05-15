# Product Runtime v35.1 Preproduction Final Runtime

Status: **Pre-Production Ready / Controlled Pilot Ready**  
Package marker: `v35_1_preproduction_final_runtime`  
Runtime contract marker: `v35_1_preproduction_final_runtime`  
Validated locally with: `compileall` and `pytest`.

This repository contains the runtime worker component for the Centaurus-X industrial automation stack. The runtime remains a worker-side system: it uses configuration contracts, JSON/device-state input, memory-state resources, queues, MQTT/MQTTS bridge traffic and local runtime health snapshots. PostgreSQL is **not** a local worker runtime dependency; PostgreSQL backup/restore belongs to the external Proxy/Gateway instance.

## GitHub release boundary

Publish the whole repository root, not only `product/`.

The root contains runtime source code, installers, service templates, operational documentation, contracts, diagnostics reports, tools, GitHub governance files and licensing material. Do not publish `_private/`, caches, generated logs or local environment artifacts.

## Architecture

![Sync XServer Target Architecture](documentation/architecture/sync_xserver_target_architecture/sync_xserver_target_architecture.svg)

![Runtime v35.1 Gesamtarchitektur](documentation/architecture/sync_xserver_target_architecture/runtime_v35_1_gesamtarchitektur.svg)

![Thread Management Architecture](documentation/architecture/sync_xserver_target_architecture/threadmanagement_architektur.svg)

## Runtime chain

```text
Client
  -> Product Proxy Gateway
  -> EMQX/MQTTS
  -> Runtime Worker v35.1
  -> Runtime Command Binding
  -> Fieldbus profile / Modbus simulation subset
  -> Runtime Reply
  -> Proxy
  -> Client
```

Gateway-side PostgreSQL registry/auth/audit can be part of the complete external deployment chain, but it is not embedded in this worker package.

## What changed in v35.1

```text
- Runtime command marker migrated to v35_1_preproduction_final_runtime.
- Native bridge event type migrated to V35_1_PROXY_RUNTIME_COMMAND_RECEIVED.
- v34/v33/v32 command event names remain accepted as legacy compatibility events.
- MQTT auth, topic ACL, PKI/mTLS and certificate rotation metadata are configurable.
- Runtime health snapshots are written as JSON for monitoring/alerting integration.
- Fieldbus runtime profile can restrict active controllers to the currently simulated subset.
- PostgreSQL scope is corrected: Gateway/Proxy responsibility, no worker-local DB requirement.
- systemd, logrotate, smoke-test and host rollout scripts are updated for v35.1.
```

## Main runtime entrypoint

```bash
cd product
python src/sync_xserver_main.py
```

## Linux rollout

```bash
bash installers/deploy_runtime_host_v35_1.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
  --runtime-python /opt/python-nogil/current/bin/python3 \
  --runtime-user product-runtime
```

Service-only install:

```bash
bash installers/install_runtime_service.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
  --service-name product-runtime-v35-1.service
```

Health check:

```bash
bash installers/run_runtime_healthcheck.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
```

## Important runtime prerequisite

The real runtime service path expects a Python free-threading / no-GIL interpreter.

```bash
python installers/check_python_free_threading.py
```

Unit tests can run on a normal Python interpreter, but the production service installer intentionally checks the free-threading runtime requirement.

## Useful documentation

```text
documentation/README.md
documentation/directory_structure.md
documentation/operations/RUNTIME_ROLLOUT_GUIDE_V35_1.md
documentation/operations/RUNTIME_TEST_PLAN_V35_1.md
documentation/release/PREPRODUCTION_STATUS_V35_1.md
documentation/release/RELEASE_NOTES_V35_1.md
documentation/hardening/ONE_STEP_FINISHING_PLAN_V35_1.md
documentation/hardening/POSTGRESQL_SCOPE_CLARIFICATION_V35_1.md
reports/acceptance/V35_1_PREPRODUCTION_ACCEPTANCE_REPORT.md
reports/diagnostics/V35_1_REPOSITORY_AND_RUNTIME_DIAGNOSIS.md
Diff-System-Report.md
```

## Licensing

This project is intended to be distributed under a dual licensing model:

```text
GPL-3.0-or-later OR Commercial License
```

See `LICENSE.md` and `COMMERCIAL_LICENSE.md`.
