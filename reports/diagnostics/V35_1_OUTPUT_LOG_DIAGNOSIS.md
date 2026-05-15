# v35.1 Output Log Diagnosis

## Logging state

```text
Runtime file log: product/logs/system_logs/system.log
Runtime health:   product/logs/system_logs/runtime_health.json
Systemd output:   journald via product-runtime-v35-1.service
Log rotation:     installers/install_runtime_logrotate.sh
```

## Diagnosis

The runtime logging path remains file-based plus journald forwarding through systemd. v35.1 adds a structured health JSON snapshot for machine checks. This avoids scraping human log output for basic liveness, queue, bridge, security and fieldbus state.

## Recommended operational check

```bash
sudo journalctl -u product-runtime-v35-1.service -n 200 --no-pager
bash installers/run_runtime_healthcheck.sh --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
```
