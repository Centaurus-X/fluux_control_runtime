---
name: Runtime integration issue
about: Track proxy, MQTT, PostgreSQL, command binding or fieldbus integration work
title: "integration: "
labels: integration
assignees: ""
---

# Runtime integration issue

## Integration path

Select the affected path:

```text
[ ] Client -> Proxy -> Runtime command/reply
[ ] PostgreSQL registry/auth/audit
[ ] EMQX/MQTTS transport
[ ] Runtime command binding
[ ] Worker presence/snapshot/event topics
[ ] Modbus simulation subset
[ ] Full C1/C2/C3/C4 fieldbus matrix
[ ] systemd runtime service
```

## Current state

Describe what is already working.

## Gap

Describe what is still missing or failing.

## Target environment

```text
Runtime package:
Proxy version:
Broker host/port:
MQTT TLS: yes/no
Database: yes/no
Simulation or real fieldbus:
```

## Validation command or procedure

```text
1.
2.
3.
```

## Acceptance criteria

```text
[ ] Integration path is reproducible.
[ ] Logs are captured with secrets removed.
[ ] Failure mode is documented.
[ ] Required configuration change is documented.
```
