# Runtime Test Plan v34.3

## Required checks

```text
1. Python free-threading check
2. MQTTS CA copy and TLS verification
3. Runtime proxy bridge config patch
4. Runtime systemd service start
5. Worker presence in PostgreSQL
6. Client E2E through proxy to runtime
7. Modbus simulation path observation
```

## Tests intentionally not repeated in this release step

Proxy restart, MQTT restart and PostgreSQL restart tests were already validated during the proxy v01.8.6 rollout. They can be repeated during the next hardening or soak-test phase.

## Modbus note

C3/C4 failures are expected while only one simulation server is active. They do not invalidate the runtime/proxy integration test.
