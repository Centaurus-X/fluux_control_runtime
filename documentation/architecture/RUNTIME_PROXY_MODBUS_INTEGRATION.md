# Runtime, Proxy and Modbus Integration

## Components

```text
Runtime repository:
  Product Runtime worker, proxy worker bridge, runtime command binding.

Proxy repository:
  WebSocket gateway, PostgreSQL registry/auth/audit, MQTT bridge, EMQX/PostgreSQL installers.

Modbus simulation repository:
  Independent simulation server for fieldbus testing.

Master repository:
  Planned later as a separate repository or as an optional master component delivered through the proxy ecosystem.
```

## Validated path

```text
Client -> WebSocket -> Proxy -> MQTTS -> Runtime -> Modbus simulation -> Runtime -> MQTTS -> Proxy -> Client
```

## Modbus interpretation

The current validation uses one active Modbus simulation server. Therefore C3/C4 failures are expected when the runtime still references additional production-like endpoints that are not currently simulated.

This should be handled by a later profile cleanup:

```text
- preproduction profile: only active simulation endpoints
- production profile: real fieldbus endpoints
- full simulation profile: all C1/C2/C3/C4 endpoints simulated
```
