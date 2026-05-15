# Description

Product Runtime v35.1 Preproduction Final Runtime is the runtime worker component for an event-driven industrial control and automation stack.

The system connects an external client-facing proxy gateway and EMQX/MQTTS transport with a deterministic worker runtime, runtime command binding, shared memory resources, queue-based event flow, compiled configuration/contracts and fieldbus adapters such as Modbus TCP. The validated scope is a controlled pre-production or supervised pilot deployment, not unattended final industrial production operation.

## Repository purpose

```text
product/          runtime source, configuration, tests and vendor compiler bridge
contracts/        formal runtime mapping and compatibility contracts
documentation/    architecture, rollout, release and hardening documentation
installers/       Linux service installation, rollout and smoke-check helpers
service/          systemd service template
reports/          acceptance and diagnostics reports
tools/            local analysis, conversion, health and standards-check tooling
.github/          GitHub issue and pull-request templates
```

The repository root is the intended release boundary. Publishing only `product/` would omit operational documentation, contracts, service templates, release reports and repository governance files.

## Current status

```text
Release state: pre-production ready
Controlled production test: ready with supervision
Unattended production readiness: not yet declared
Runtime contract marker: v35_1_preproduction_final_runtime
Native runtime command event: V35_1_PROXY_RUNTIME_COMMAND_RECEIVED
Primary runtime entrypoint: product/src/sync_xserver_main.py
```

v35.1 corrects the PostgreSQL scope: PostgreSQL belongs to the external Proxy/Gateway stack, not to the worker runtime. The worker remains memory/JSON/queue/MQTT based and now exposes runtime health snapshots for monitoring integration.

## Configurable hardening points

```text
MQTT authentication: proxy_worker_bridge.authentication_required
MQTT secrets: proxy_worker_bridge.username_env / password_env
Topic ACLs: proxy_worker_bridge.topic_acl_enforcement and allowed topic lists
PKI/mTLS: proxy_worker_bridge.mtls_mode and certificate metadata
Monitoring: monitoring.health and monitoring.alerting
Fieldbus subset: fieldbus.runtime_profile.active_controller_ids
```

## Validated local release checks

```text
python -m compileall -q .
python -m pytest -q
```

## Non-goals of the current package

This package does not claim final unattended industrial production readiness. Complete production PKI rollout, external Gateway/PostgreSQL backup drills, 24h+ soak testing and full real fieldbus matrix validation remain release-candidate tasks.
