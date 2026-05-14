# Runtime Service Installation

Use the top-level installer scripts. Do not place deployment scripts inside `product/`.

```bash
bash installers/install_runtime_service.sh \
  --runtime-root /opt/projektstand_v34_3_preproduction_final_runtime \
  --runtime-user "$USER"
```

The installer writes:

```text
/etc/systemd/system/product-runtime-v34-3.service
```

It also ensures that this path exists:

```text
product/logs/system_logs/system.log
```

The primary runtime service log remains available through systemd:

```bash
journalctl -u product-runtime-v34-3.service -n 200 --no-pager
```
