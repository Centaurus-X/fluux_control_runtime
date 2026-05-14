# Production Readiness Gap List

Status: **open for a later hardening sprint**

The current state is pre-production ready. The following items must be completed before declaring unattended final production readiness.

## 1. Certificate and PKI strategy

Current state:

```text
MQTTS works with generated EMQX CA material.
Runtime and proxy can verify the broker certificate.
```

Required later:

```text
- Define long-lived internal CA or enterprise PKI integration.
- Define certificate rotation policy.
- Define broker certificate SAN policy for IP and DNS names.
- Define client certificate usage or explicitly decide against mTLS.
- Store private keys outside repository packages.
```

## 2. MQTT authentication and ACL

Current state:

```text
The validated setup is focused on functional MQTTS transport.
```

Required later:

```text
- Disable anonymous MQTT access.
- Create runtime worker credentials.
- Create proxy credentials.
- Restrict worker clients to their own command/reply/event/presence topics.
- Restrict proxy client to bridge topics only.
- Add negative ACL tests.
```

## 3. PostgreSQL backup and restore

Current state:

```text
PostgreSQL registry/auth/audit works.
```

Required later:

```text
- Define backup schedule.
- Define retention policy.
- Add restore test script.
- Add schema migration rollback guidance.
- Add point-in-time recovery decision if needed.
```

## 4. Monitoring and alerting

Current state:

```text
Manual checks and logs are sufficient for pre-production testing.
```

Required later:

```text
- Monitor proxy health.
- Monitor EMQX health and listener status.
- Monitor PostgreSQL health.
- Monitor runtime systemd service state.
- Monitor worker presence freshness.
- Alert on stale worker, stale proxy, failed client E2E smoke test, and certificate expiry.
```

## 5. Runtime logging path and log shipping

Current state:

```text
systemd journal logging is available.
The installer now creates product/logs/system_logs/system.log to avoid missing-path checks.
```

Required later:

```text
- Confirm the canonical runtime log sink.
- Configure log rotation consistently.
- Ship logs to central logging.
- Avoid duplicate or silent log destinations.
```

## 6. Environment metadata

Current state:

```text
The proxy package may still report environment metadata such as test/preproduction.
```

Required later:

```text
- Decide canonical environment values: test, preproduction, production.
- Configure proxy/runtime metadata consistently.
- Avoid marking pilot production as final production prematurely.
```

## 7. Fieldbus matrix

Current state:

```text
One Modbus simulation server is active and validated.
C3/C4 failures are expected because not all configured fieldbus targets are currently simulated.
```

Required later:

```text
- Either simulate all configured C1/C2/C3/C4 devices,
- or disable unused targets in the runtime configuration,
- or mark unused targets as optional/non-failing for the current environment.
```

## 8. Soak and failure-mode tests

Current state:

```text
Restart tests for proxy, MQTT and PostgreSQL have passed earlier.
Runtime integration is green.
```

Required later:

```text
- Run multi-hour or multi-day soak tests.
- Add repeated command bursts.
- Test broker outage duration windows.
- Test DB outage handling and recovery.
- Test Runtime restart under active client load.
```
