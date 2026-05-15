<<<<<<< HEAD
# Runtime Service Installation
=======
# Runtime Service Installation v35.1
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)

Use the top-level installer scripts. Do not place deployment scripts inside `product/`.

```bash
bash installers/install_runtime_service.sh \
<<<<<<< HEAD
  --runtime-root /opt/projektstand_v34_3_preproduction_final_runtime \
=======
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
  --runtime-user "$USER"
```

The installer writes:

```text
<<<<<<< HEAD
/etc/systemd/system/product-runtime-v34-3.service
=======
/etc/systemd/system/product-runtime-v35-1.service
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
```

It also ensures that this path exists:

```text
product/logs/system_logs/system.log
```

The primary runtime service log remains available through systemd:

```bash
<<<<<<< HEAD
journalctl -u product-runtime-v34-3.service -n 200 --no-pager
=======
journalctl -u product-runtime-v35-1.service -n 200 --no-pager
```

Optional log rotation:

```bash
bash installers/install_runtime_logrotate.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
```

Optional health check:

```bash
bash installers/run_runtime_healthcheck.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
```

A full runtime-host rollout can be started with:

```bash
bash installers/deploy_runtime_host_v35_1.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
  --runtime-user "$USER"
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
```
