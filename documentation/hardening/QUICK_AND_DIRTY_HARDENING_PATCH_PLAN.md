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
