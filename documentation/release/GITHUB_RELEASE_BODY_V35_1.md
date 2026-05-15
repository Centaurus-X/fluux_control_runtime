# Runtime v35.1 Preproduction Final

Status:

- Pre-Production Ready
- Controlled Pilot Ready
- Runtime Worker release package
- Not yet final unattended production-ready without the documented long-running soak and operations sign-off

Validated integration:

- Runtime Worker v35.1
- Product Proxy Gateway bridge contract C27
- EMQX/MQTTS worker bridge
- Controller worker chunking for active C2 simulation profile
- Modbus simulation path on the configured runtime host setup
- PID liveness hotfix for deterministic control-loop refreshes

Key changes:

- Migrated active runtime identifiers to `v35_1_preproduction_final_runtime`.
- Added configurable MQTT authentication, topic ACL, PKI/mTLS metadata and certificate rotation metadata.
- Added runtime health JSON snapshots and healthcheck tooling.
- Added fieldbus runtime profile for restricting active controllers to the simulated subset.
- Corrected PostgreSQL scope: PostgreSQL belongs to the external Proxy/Gateway instance, not to the worker-local runtime.
- Added systemd, logrotate, smoke-test and runtime host rollout helpers for v35.1.
- Fixed PID/control-loop liveness so PID/PI/PD sensors can continue driving automation even when sensor values remain constant.
- Fixed config chunking for process states referenced indirectly through controller-owned sensors or actuators.

Recommended deployment:

```bash
bash installers/deploy_runtime_host_v35_1.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
  --runtime-python /opt/python-nogil/current/bin/python3 \
  --runtime-user product-runtime
```

Runtime entrypoint:

```bash
cd product
python src/sync_xserver_main.py
```

Important runtime prerequisite:

The real runtime service path expects a Python free-threading / no-GIL interpreter. Unit tests can run on a normal Python interpreter, but the production service installer intentionally checks the free-threading runtime requirement.

License:

```text
GPL-3.0-or-later OR Commercial License
```

Main references:

- `documentation/release/RELEASE_NOTES_V35_1.md`
- `documentation/release/PREPRODUCTION_STATUS_V35_1.md`
- `documentation/operations/RUNTIME_ROLLOUT_GUIDE_V35_1.md`
- `documentation/operations/RUNTIME_TEST_PLAN_V35_1.md`
- `documentation/operations/PID_CONTROL_LIVENESS_V35_1.md`
- `documentation/hardening/POSTGRESQL_SCOPE_CLARIFICATION_V35_1.md`
- `Diff-System-Report.md`