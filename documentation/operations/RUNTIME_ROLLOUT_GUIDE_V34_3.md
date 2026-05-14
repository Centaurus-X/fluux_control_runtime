# Runtime Rollout Guide v34.3

This guide assumes the proxy gateway stack v01.8.6 is already deployed and healthy.

## Target host

```text
Runtime host: 192.168.0.80
Runtime root: /opt/projektstand_v34_3_preproduction_final_runtime
Service: product-runtime-v34-3.service
Worker ID: worker_fn_01
MQTT broker: 192.168.0.31:8883
Proxy endpoint: ws://192.168.0.26:8765
```

## 1. Unpack the runtime package

```bash
sudo mkdir -p /opt
cd /opt
sudo rm -rf /opt/projektstand_v34_3_preproduction_final_runtime
sudo unzip -o ~/projektstand_v34_3_preproduction_final_runtime.zip -d /opt
sudo chown -R "$USER":"$USER" /opt/projektstand_v34_3_preproduction_final_runtime
cd /opt/projektstand_v34_3_preproduction_final_runtime
```

## 2. Create the runtime virtual environment

Use Python 3.14 free-threading/no-GIL.

```bash
cd /opt/projektstand_v34_3_preproduction_final_runtime
RUNTIME_PYTHON="/opt/python-nogil/current/bin/python3"
"$RUNTIME_PYTHON" -m venv .venv-runtime-v34_3
. .venv-runtime-v34_3/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r product/requirements.txt
python installers/check_python_free_threading.py
```

## 3. Copy the EMQX CA from the MQTT host

```bash
cd /opt/projektstand_v34_3_preproduction_final_runtime
bash installers/copy_mqtt_ca.sh \
  --runtime-root /opt/projektstand_v34_3_preproduction_final_runtime \
  --mqtt-host 192.168.0.31 \
  --proxy-release-root /opt/product_proxy_gateway_v01_8_6
```

Verify TLS:

```bash
openssl s_client \
  -connect 192.168.0.31:8883 \
  -CAfile product/config/ssl/certs/mqtt/emqx-root-ca.pem \
  -verify_return_error \
  -verify_ip 192.168.0.31 \
  -brief < /dev/null
```

Expected:

```text
Verification: OK
```

## 4. Patch runtime proxy bridge configuration

```bash
cd /opt/projektstand_v34_3_preproduction_final_runtime
bash installers/patch_runtime_proxy_bridge_config.sh \
  --runtime-root /opt/projektstand_v34_3_preproduction_final_runtime \
  --broker-host 192.168.0.31 \
  --worker-id worker_fn_01 \
  --client-id v34-worker-worker_fn_01
```

## 5. Stop the dummy worker

The real runtime and dummy worker must not subscribe to the same command topic at the same time.

```bash
sudo docker rm -f product-proxy-dummy-worker-fn01 2>/dev/null || true
pkill -f 'src/sync_xserver_main.py' 2>/dev/null || true
pkill -f 'sync_xserver_main.py' 2>/dev/null || true
```

## 6. Install and start systemd service

```bash
cd /opt/projektstand_v34_3_preproduction_final_runtime
bash installers/install_runtime_service.sh \
  --runtime-root /opt/projektstand_v34_3_preproduction_final_runtime \
  --runtime-user "$USER"
```

## 7. Check runtime service

```bash
bash installers/check_runtime_service.sh \
  --runtime-root /opt/projektstand_v34_3_preproduction_final_runtime
```

## 8. Check worker registry on PostgreSQL host

```bash
cd /opt/product_proxy_gateway_v01_8_6
sudo docker exec postgres-proxy-gateway-preprod \
  psql -U proxy_gateway_test -d proxy_gateway_test \
  -c "select worker_id, status, last_seen_at, last_presence_json->>'runtime_version' as runtime_version, last_presence_json from proxy_gateway.workers where worker_id = 'worker_fn_01';"
```

Expected:

```text
worker_fn_01 | online | ... | v34_preproduction_final_runtime
```

## 9. Run client E2E on the client host

```bash
cd /opt/product_proxy_gateway_v01_8_6
set -o pipefail
sudo bash installers/install_client_test_system.sh \
  --master-password-file config/production_master_passwords.env \
  --skip-docker-install \
  | tee runtime_v34_3_client_e2e.log

echo "Client-Test ExitCode=${PIPESTATUS[0]}"
```

Expected:

```text
hello_ack
subscribe_ack
command_ack
delivery
Client-Test ExitCode=0
```
