<<<<<<< HEAD
# Quick Hardening Patch Plan

Purpose: provide a practical one-shot implementation plan for the next hardening sprint. This is intentionally not applied in v34.3 because the current system must go online today as a pre-production release.

## Patch 1: Broker authentication and ACL

Target repositories:

```text
proxy gateway repository
runtime repository
EMQX/MQTT deployment assets
```

Quick implementation:

```text
1. Create EMQX users:
   - proxy_bridge
   - runtime_worker_fn01
2. Disable anonymous access.
3. Add topic ACLs:
   - runtime_worker_fn01 can subscribe worker/worker_fn_01/command/+
   - runtime_worker_fn01 can publish worker/worker_fn_01/reply/+, event/+, presence, snapshot/+
   - proxy_bridge can publish command topics and subscribe reply/event/presence/snapshot topics.
4. Add credentials to environment files only, never to repository defaults.
5. Add a negative test: unauthorized client cannot subscribe to worker/#.
```

## Patch 2: Production certificate policy

Quick implementation:

```text
1. Replace generated test CA with an environment-owned CA.
2. Issue broker certificate with DNS and IP SANs.
3. Copy only CA certificate to runtime and proxy hosts.
4. Keep private keys out of GitHub and ZIP release artifacts.
5. Add certificate expiry check to installer/check scripts.
```

## Patch 3: PostgreSQL backup/restore

Quick implementation:

```text
1. Add backup script using pg_dump custom format.
2. Add restore script for an empty target database.
3. Add daily cron or systemd timer.
4. Add retention cleanup.
5. Run restore validation before final production sign-off.
```

## Patch 4: Monitoring and alerting

Quick implementation:

```text
1. Add a lightweight health-check runner.
2. Check proxy WebSocket handshake.
3. Check EMQX TLS listener.
4. Check PostgreSQL TCP auth.
5. Check runtime systemd service state.
6. Check worker presence freshness in PostgreSQL.
7. Emit one JSON status report.
8. Later connect this to Prometheus, Grafana, Loki or a comparable stack.
```

## Patch 5: Runtime log path

Quick implementation:

```text
1. Keep journalctl as primary source of truth for the service.
2. Ensure product/logs/system_logs exists at installation time.
3. Create system.log to avoid missing file checks.
4. Confirm whether runtime writes to system.log or timestamped files.
5. Add logrotate config or systemd-journald retention policy.
```

## Patch 6: Environment marker

Quick implementation:

```text
1. Introduce explicit environment variable: RUNTIME_ENVIRONMENT=preproduction.
2. Introduce proxy metadata variable: PROXY_ENVIRONMENT=preproduction.
3. Keep production marker blocked until security and operations hardening is complete.
```

## Patch 7: Fieldbus configuration cleanup

Quick implementation:

```text
1. Create a preproduction Modbus profile with only the active simulation server.
2. Move unused C3/C4 targets to a disabled profile.
3. Add a fieldbus matrix document listing simulated, optional and production-only endpoints.
4. Re-run runtime E2E and Modbus simulation checks.
```

## Patch 8: Long-running validation

Quick implementation:

```text
1. Run client E2E every minute for 24 hours.
2. Record success rate and latency.
3. Restart EMQX once during the run.
4. Restart runtime once during the run.
5. Capture memory growth and reconnect behavior.
```

## Recommended order

```text
1. MQTT auth/ACL
2. Certificate policy
3. Runtime fieldbus profile cleanup
4. Backup/restore
5. Monitoring/alerting
6. Soak test
7. Final production sign-off
```


## v34.3 note

The v34.3 package intentionally does not implement the production hardening items yet. It documents them as a later controlled patch sequence so the current pre-production system can go online without introducing untested operational changes.
=======
# Hardening Patch Status v35.1

The earlier quick-and-dirty hardening backlog has been folded into the v35.1 controlled migration.

## Implemented as configurable worker features

```text
- MQTT authentication requirement switch
- MQTT username/password resolution from environment variables
- Worker-side topic ACL checks for publish and subscribe
- mTLS mode: disabled, optional, required
- Certificate rotation metadata
- Runtime health JSON snapshots
- CLI healthcheck helper
- Fieldbus runtime profile for simulated-controller subsets
- v35.1 systemd, journald and logrotate helpers
```

## Corrected PostgreSQL scope

The worker runtime does not own PostgreSQL. PostgreSQL backup/restore is a Gateway/Proxy validation topic and is documented in:

```text
documentation/hardening/POSTGRESQL_SCOPE_CLARIFICATION_V35_1.md
```

## Remaining production sign-off work

```text
1. Enforce the same MQTT user/topic ACL rules on the broker side.
2. Install production CA/client material outside the repository.
3. Run Gateway/PostgreSQL backup/restore in the external proxy environment.
4. Run a 24h+ soak test with runtime and broker restarts.
5. Tag v35.1 only after the operational sign-off is complete.
```
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
