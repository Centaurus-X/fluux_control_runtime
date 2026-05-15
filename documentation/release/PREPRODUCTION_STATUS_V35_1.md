# Pre-Production Status v35.1

## Status

```text
Worker runtime: controlled pilot ready
Unattended final production: not yet declared
Local validation: green
```

## Functional state

The runtime worker starts from `product/src/sync_xserver_main.py`, loads effective configuration/contracts, initializes resources and queues, starts managed runtime threads and optionally connects the Proxy Worker Bridge through MQTT/MQTTS.

## Hardening state

```text
MQTT auth: configurable
Topic ACLs: worker-side checks implemented, broker-side ACLs still required
PKI/mTLS: configurable, production certificate rollout still required
Monitoring: runtime health JSON implemented
Logging: system.log + journald + logrotate helper available
Fieldbus: active simulation subset configurable
PostgreSQL: external Gateway/Proxy scope, not worker-local
```

## Remaining before unattended production

```text
- Enforce EMQX users and broker-side ACLs.
- Install production CA/client certs and key rotation process.
- Validate Gateway/PostgreSQL backup/restore separately.
- Run 24h+ soak test with broker/runtime restarts.
- Validate complete fieldbus matrix or keep inactive controllers explicitly optional.
```
