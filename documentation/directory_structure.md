# Directory Structure Runtime v34.3

Status: active  
Scope: public repository layout for the runtime pre-production release  
Release marker: `v34_3_preproduction_final_runtime`  
Runtime contract marker: `v34_preproduction_final_runtime`

## GitHub publication boundary

Publish the whole repository root, not only `product/`.

`product/` alone is not sufficient because the release also needs:

```text
installers/     runtime rollout and systemd helpers
service/        service template
contracts/      contract and mapping documents
documentation/  release, operation, hardening and architecture documentation
reports/        acceptance and diagnostic reports
tools/          development and repository hygiene utilities
LICENSE.md      GPLv3 license text summary
COMMERCIAL_LICENSE.md
```

Do not publish `_private/`. It is local-only development history and is excluded by `.gitignore`.

## Public package tree

```text
projektstand_v34_3_preproduction_final_runtime/
├─ README.md
├─ LICENSE.md
├─ COMMERCIAL_LICENSE.md
├─ V34_RELEASE_MARKER.txt
├─ PROJECT_MANIFEST_V34_3.json
├─ FILE_CHECKSUMS_SHA256_V34_3.txt
├─ .gitignore
├─ archive/
│  └─ README.md
├─ contracts/
│  ├─ README.md
│  ├─ formal_contracts/
│  └─ runtime_mapping/
├─ documentation/
│  ├─ README.md
│  ├─ directory_structure.md
│  ├─ architecture/
│  │  ├─ RUNTIME_PROXY_MODBUS_INTEGRATION.md
│  │  └─ sync_xserver_target_architecture/
│  ├─ github/
│  ├─ hardening/
│  ├─ operations/
│  └─ release/
├─ installers/
│  ├─ check_python_free_threading.py
│  ├─ check_runtime_service.sh
│  ├─ copy_mqtt_ca.sh
│  ├─ install_runtime_service.sh
│  ├─ patch_runtime_proxy_bridge_config.sh
│  └─ run_runtime_smoke_tests.sh
├─ product/
│  ├─ config/
│  │  ├─ application/
│  │  ├─ compiled/
│  │  ├─ cud/
│  │  ├─ external/
│  │  ├─ other_runtime_examples/
│  │  └─ ssl/
│  ├─ contracts/
│  ├─ logs/
│  ├─ src/
│  ├─ tests/
│  ├─ vendor/
│  ├─ requirements.txt
│  └─ run.py
├─ reports/
├─ service/
│  └─ product-runtime-v34-3.service.template
└─ tools/
```

## Local-only package tree

```text
_private/
└─ development_history/
   ├─ archive.zip
   └─ config_backups/
```

This local-only tree is kept for traceability but must not be part of the public GitHub repository.

## Runtime entrypoint

```text
product/src/sync_xserver_main.py
```

## Active runtime configuration

```text
product/config/application/app_config.yaml
```

## Runtime proxy bridge

```text
product/src/core/proxy_worker_bridge.py
product/src/core/_runtime_command_binding.py
```

## Current validated integration

```text
Proxy Gateway: v01.8.6
Runtime package: v34.3 preproduction package
Runtime contract: v34_preproduction_final_runtime
MQTT broker: EMQX/MQTTS
Database: PostgreSQL registry/auth/audit
Fieldbus test path: one active Modbus simulation server
```
