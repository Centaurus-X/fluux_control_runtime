#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v34_3_preproduction_final_runtime}"
BROKER_HOST="${BROKER_HOST:-192.168.0.31}"
BROKER_PORT="${BROKER_PORT:-1883}"
SECURE_PORT="${SECURE_PORT:-8883}"
WORKER_ID="${WORKER_ID:-worker_fn_01}"
NODE_ID="${NODE_ID:-fn-01}"
CLIENT_ID="${CLIENT_ID:-v34-worker-worker_fn_01}"
CA_CERT_FILE="${CA_CERT_FILE:-config/ssl/certs/mqtt/emqx-root-ca.pem}"
BINDING_VERSION="${BINDING_VERSION:-v34_preproduction_final_runtime}"

while [ $# -gt 0 ]; do
  case "$1" in
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
    --broker-host) BROKER_HOST="$2"; shift 2 ;;
    --broker-port) BROKER_PORT="$2"; shift 2 ;;
    --secure-port) SECURE_PORT="$2"; shift 2 ;;
    --worker-id) WORKER_ID="$2"; shift 2 ;;
    --node-id) NODE_ID="$2"; shift 2 ;;
    --client-id) CLIENT_ID="$2"; shift 2 ;;
    --ca-cert-file) CA_CERT_FILE="$2"; shift 2 ;;
    --binding-version) BINDING_VERSION="$2"; shift 2 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

CONFIG_PATH="${RUNTIME_ROOT}/product/config/application/app_config.yaml"
if [ ! -f "$CONFIG_PATH" ]; then
  printf 'Configuration file not found: %s\n' "$CONFIG_PATH" >&2
  exit 1
fi

export CONFIG_PATH BROKER_HOST BROKER_PORT SECURE_PORT WORKER_ID NODE_ID CLIENT_ID CA_CERT_FILE BINDING_VERSION
python - <<'PY'
from pathlib import Path
import os
import yaml

config_path = Path(os.environ["CONFIG_PATH"])
data = yaml.safe_load(config_path.read_text(encoding="utf-8"))

base = data.setdefault("base", {})
app = base.setdefault("app", {})
bridge = base.setdefault("proxy_worker_bridge", {})
network = base.setdefault("network", {})
mqtt_client = network.setdefault("mqtt_client", {})

app["node_id"] = os.environ["NODE_ID"]
bridge["enabled"] = True
bridge["contract_id"] = "C27"
bridge["worker_id"] = os.environ["WORKER_ID"]
bridge["broker_host"] = os.environ["BROKER_HOST"]
bridge["broker_port"] = int(os.environ["BROKER_PORT"])
bridge["secure_port"] = int(os.environ["SECURE_PORT"])
bridge["use_mqtts"] = True
bridge["ca_cert_file"] = os.environ["CA_CERT_FILE"]
bridge["client_cert_file"] = None
bridge["client_key_file"] = None
bridge["tls_ciphers"] = None
bridge["tls_allow_insecure"] = False
bridge["check_hostname"] = True
bridge["username"] = None
bridge["password"] = None
bridge["client_id"] = os.environ["CLIENT_ID"]
bridge["keepalive"] = 60
bridge["command_qos"] = 2
bridge["reply_qos"] = 1
bridge["presence_qos"] = 1
bridge["event_qos"] = 1
bridge["presence_interval_s"] = 30.0
bridge["auto_reply_enabled"] = True
bridge["emit_command_events"] = True
bridge["snapshot_enabled"] = True
bridge["runtime_binding_enabled"] = True
bridge["runtime_event_target"] = "thread_management"
bridge["runtime_state_resource"] = "proxy_runtime_command_state"
bridge["runtime_command_binding_version"] = os.environ["BINDING_VERSION"]

mqtt_client["enabled"] = False
mqtt_client["broker_ip"] = os.environ["BROKER_HOST"]
mqtt_client["broker_port"] = int(os.environ["SECURE_PORT"])
mqtt_client.setdefault("ssl", {})
mqtt_client["ssl"]["enabled"] = True
mqtt_client["ssl"]["cafile"] = os.environ["CA_CERT_FILE"]

config_path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
print("Runtime proxy bridge configuration patched:", config_path)
PY
