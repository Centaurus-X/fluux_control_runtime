# Pre-Production Status v34.3

## Release classification

```text
Pre-Production Ready: yes
Controlled Production Test Ready: yes
Pilot Production Ready with supervision: yes
Final unattended production-ready: no
```

## Honest status statement

The current integration state is production-like and functionally green. It is ready for a controlled production test, pilot run or pre-production deployment.

It is not yet declared fully production-ready for unattended long-term operation.

## Validated integration chain

```text
Client
  -> Product Proxy Gateway v01.8.6
  -> PostgreSQL registry/auth/audit
  -> EMQX over MQTTS
  -> Runtime v34.3 package with v34 preproduction final binding
  -> Modbus simulation path
  -> Runtime reply
  -> Proxy
  -> Client
```

## Current Modbus scope

Only one Modbus simulation server is active in the validated setup. The endpoint `192.168.0.3:5020` is validated for the configured test subset.

C3/C4 Modbus failures are expected while only one simulation endpoint is active. They do not invalidate the runtime/proxy/client integration result.

## Remaining hardening items

```text
MQTT authentication and ACL policy
Certificate / PKI strategy
PostgreSQL backup and restore procedure
Monitoring and alerting
Runtime logging path and log shipping
Environment metadata cleanup
Full fieldbus matrix validation
Long-running soak tests
```

See `documentation/hardening/PRODUCTION_READINESS_GAP_LIST.md` and `documentation/hardening/QUICK_AND_DIRTY_HARDENING_PATCH_PLAN.md`.
