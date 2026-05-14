# Product Runtime v34.3 Preproduction Final Runtime

Status: **Pre-Production Ready**  
Package marker: `v34_3_preproduction_final_runtime`  
Runtime contract marker: `v34_preproduction_final_runtime`  
Validated with: **Product Proxy Gateway v01.8.6**, PostgreSQL, EMQX/MQTTS and one Modbus simulation server.

This repository package contains the runtime worker component and its release metadata. The proxy gateway, PostgreSQL/EMQX rollout package and Modbus simulation server are expected to be published as separate repositories.

## GitHub release boundary

Publish the whole repository root, not only `product/`.

The root contains the runtime source tree plus installers, service templates, release documentation, contracts, reports, tools and license files. `product/` alone is useful for execution, but incomplete as a GitHub release.

Do not publish `_private/`. It is local-only development history and is excluded by `.gitignore`.


## Architecture

![Sync XServer Target Architecture](documentation/architecture/sync_xserver_target_architecture/sync_xserver_target_architecture.svg)

Additional architecture artifacts:

- [Architecture documentation](documentation/architecture/sync_xserver_target_architecture/README.md)
- [Mermaid source](documentation/architecture/sync_xserver_target_architecture/sync_xserver_target_architecture.mmd)
- [HTML architecture viewer](documentation/architecture/sync_xserver_target_architecture/sync_xserver_target_architecture.html)


## Honest release status

The current integration state is production-like and functionally green. It is ready for a controlled production test, pilot run or pre-production deployment.

It is **not yet declared fully production-ready for unattended long-term operation**. The remaining hardening work is documented in:

```text
documentation/release/PREPRODUCTION_STATUS_V34_3.md
documentation/hardening/PRODUCTION_READINESS_GAP_LIST.md
documentation/hardening/QUICK_AND_DIRTY_HARDENING_PATCH_PLAN.md
```

## Validated live chain

```text
Client
  -> Proxy v01.8.6
  -> PostgreSQL registry/auth/audit
  -> MQTTS/EMQX
  -> Runtime v34.3 package / v34 preproduction command binding
  -> Modbus simulation path
  -> Runtime reply
  -> Proxy
  -> Client
```

## Current acceptance result

```text
PostgreSQL v01.8.6: OK
MQTT/EMQX v01.8.6: OK
Proxy v01.8.6: OK
Runtime v34.3 package: OK
Runtime binding v34_preproduction_final_runtime: OK
Client E2E: OK
Registry/session tracking: OK
Modbus simulation server 192.168.0.3:5020: OK for the configured test subset
Full C1/C2/C3/C4 fieldbus matrix: intentionally not fully covered yet
```

The observed C3/C4 Modbus failures are expected in the current test setup because only one Modbus simulation server is active.

## Main runtime entrypoint

```bash
cd product
python src/sync_xserver_main.py
```

For deployment, use the systemd installer:

```bash
bash installers/install_runtime_service.sh   --runtime-root /opt/projektstand_v34_3_preproduction_final_runtime
```

## Important runtime prerequisites

The runtime start path requires Python 3.14 free-threading / no-GIL.

```bash
python installers/check_python_free_threading.py
```

A normal Python build may run many unit tests, but the real runtime service is expected to reject non-free-threading execution.

## Useful documentation

```text
documentation/README.md
documentation/directory_structure.md
documentation/operations/RUNTIME_ROLLOUT_GUIDE_V34_3.md
documentation/release/PREPRODUCTION_STATUS_V34_3.md
documentation/hardening/PRODUCTION_READINESS_GAP_LIST.md
documentation/hardening/QUICK_AND_DIRTY_HARDENING_PATCH_PLAN.md
documentation/github/GITHUB_RELEASE_PREPARATION.md
documentation/github/REPOSITORY_STRATEGY.md
reports/acceptance/V34_3_PREPRODUCTION_ACCEPTANCE_REPORT.md
reports/diagnostics/V34_3_OUTPUT_LOG_DIAGNOSIS.md
```

## Licensing

This project is intended to be distributed under a dual licensing model:

```text
GPL-3.0-or-later OR Commercial License
```

See `LICENSE.md` and `COMMERCIAL_LICENSE.md`.

