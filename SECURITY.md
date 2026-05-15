# Security Policy

## Supported versions

```text
Version / line                         Security support
v35.1 pre-production runtime            Supported for hardening and pilot fixes
Older local development snapshots        Not supported
_private/ development history            Not supported and must not be published
```

## Reporting a vulnerability

Do not report security vulnerabilities in public issues or public pull requests.

Use one of these private paths:

```text
1. GitHub private vulnerability reporting, if enabled for the repository.
2. A private maintainer contact path through the Centaurus-X GitHub account or organization.
3. A private written channel agreed with the project owner for commercial users.
```

When reporting, include the affected package marker, component path, impact, minimal reproduction steps, sanitized logs and whether the issue affects MQTT, certificates, runtime command binding, fieldbus adapters, deployment scripts or the external Gateway/Proxy stack.

## Worker-side security-sensitive areas

```text
- MQTT authentication and topic ACL bypass
- leaked broker/runtime credentials
- private key or certificate handling problems
- unsafe TLS verification behavior
- command injection in installers or tooling
- unauthorized runtime command execution
- unsafe fieldbus write behavior
- privilege escalation through service files or scripts
- denial-of-service risks in runtime queues, payloads or command handling
- accidental publication of _private/, caches, logs or local environment metadata
```

## PostgreSQL scope

PostgreSQL is not a local worker runtime dependency in v35.1. PostgreSQL backup/restore and database hardening belong to the external Proxy/Gateway instance. The worker documents this in runtime health snapshots as:

```text
gateway_proxy_external_not_worker_runtime_dependency
```

## Hardening implemented/configurable in v35.1

```text
- MQTT authentication can be required by configuration.
- MQTT credentials can be supplied through environment variables.
- Topic ACL checks are enforced in the worker bridge before publish/subscribe.
- mTLS mode is configurable as disabled, optional or required.
- Certificate rotation metadata is configurable for operational tracking.
- Runtime health snapshots expose worker, queue, bridge, security and fieldbus state.
- systemd, journald and logrotate helper scripts are provided.
```

## Remaining production security work

```text
- Disable anonymous MQTT on the broker side.
- Enforce broker-side EMQX users and topic ACLs matching the worker profile.
- Deploy production CA/client certificates and rotation process.
- Keep private keys outside repository and release archives.
- Run negative ACL/security tests against the real broker.
- Validate external Gateway/PostgreSQL backup and restore separately.
- Complete 24h+ soak test including broker/runtime restarts.
```

The detailed plan is maintained in `documentation/hardening/ONE_STEP_FINISHING_PLAN_V35_1.md`.

## Safety note for industrial automation

Do not test exploits, fieldbus writes or command replay behavior against live production equipment unless the environment is explicitly approved for that test and has a safe rollback path.
