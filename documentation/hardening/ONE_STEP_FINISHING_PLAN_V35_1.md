# One-Step Finishing Plan v35.1

v35.1 folds the previous hardening backlog into one coherent release-candidate path while keeping the worker runtime directly testable.

## Implemented in this patch

```text
1. MQTT auth remains configurable and can be required.
2. Worker-side topic ACL checks are enforced before publish/subscribe.
3. PKI/mTLS mode is configurable: disabled, optional or required.
4. Certificate rotation metadata is configurable.
5. Runtime health snapshots provide monitoring integration data.
6. Fieldbus profile limits active controllers to the simulated subset.
7. PostgreSQL scope is corrected to Gateway/Proxy, not Worker Runtime.
8. systemd/journald/logrotate/deploy/smoke-test helpers are updated for v35.1.
9. Version markers, runtime command binding, contracts and tests are migrated to v35.1.
```

## Final release-candidate step

Execute this as one controlled finalization pass:

```text
1. Broker/Gateway security
   - Disable anonymous MQTT on EMQX.
   - Create dedicated proxy/runtime users.
   - Mirror the worker topic ACL profile on the broker side.
   - Decide production mTLS mode and install CA/client material outside the repo.

2. External Gateway/PostgreSQL validation
   - Run backup and restore on the Gateway/Proxy PostgreSQL instance.
   - Verify registry/auth/audit recovery.
   - Keep this out of the worker release because the worker has no local PostgreSQL DB.

3. Runtime host deployment
   - Deploy with installers/deploy_runtime_host_v35_1.sh.
   - Install service product-runtime-v35-1.service.
   - Enable logrotate and runtime health check.

4. Soak and restart validation
   - Run 24h+ command/reply loop.
   - Restart EMQX and runtime service during the test.
   - Record queue backlog, reconnects, memory trend, command latency and health snapshot age.

5. Release hygiene
   - Remove _private/, pycache, pytest-cache, logs and local health snapshots.
   - Regenerate manifest and checksums.
   - Tag the release after all checks are green.
```
