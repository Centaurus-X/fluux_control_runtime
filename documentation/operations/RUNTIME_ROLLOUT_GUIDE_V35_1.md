# Runtime Rollout Guide v35.1

## Target path

```text
/opt/projektstand_v35_1_preproduction_final_runtime
```

## Deploy from source directory

```bash
bash installers/deploy_runtime_host_v35_1.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
  --runtime-python /opt/python-nogil/current/bin/python3 \
  --runtime-user product-runtime
```

## Deploy from ZIP

```bash
bash installers/deploy_runtime_host_v35_1.sh \
  --archive /tmp/projektstand_v35_1_preproduction_final_runtime.zip \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
  --runtime-python /opt/python-nogil/current/bin/python3 \
  --runtime-user product-runtime
```

## Patch bridge configuration

```bash
bash installers/patch_runtime_proxy_bridge_config.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime \
  --broker-host 192.168.0.31 \
  --worker-id worker_fn_01 \
  --node-id fn-01 \
  --auth-required false \
  --mtls-mode optional
```

Secrets can be supplied through `/etc/default/product-runtime-v35-1`:

```text
PROXY_WORKER_BRIDGE_USERNAME=runtime_worker
PROXY_WORKER_BRIDGE_PASSWORD=change-me-outside-the-repo
```

## Service operations

```bash
sudo systemctl status product-runtime-v35-1.service --no-pager
sudo journalctl -u product-runtime-v35-1.service -n 200 --no-pager
bash installers/check_runtime_service.sh --service-name product-runtime-v35-1.service
```

## Health check

```bash
bash installers/run_runtime_healthcheck.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
```

## Smoke tests

```bash
bash installers/run_runtime_smoke_tests.sh \
  --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
```
