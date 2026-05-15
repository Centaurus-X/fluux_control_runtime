#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v35_1_preproduction_final_runtime}"
BROKER_HOST="${BROKER_HOST:-192.168.0.31}"
BROKER_PORT="${BROKER_PORT:-1883}"
SECURE_PORT="${SECURE_PORT:-8883}"
WORKER_ID="${WORKER_ID:-worker_fn_01}"
NODE_ID="${NODE_ID:-fn-01}"
CLIENT_ID="${CLIENT_ID:-v35-1-worker-worker_fn_01}"
CA_CERT_FILE="${CA_CERT_FILE:-config/ssl/certs/mqtt/emqx-root-ca.pem}"
AUTH_REQUIRED="${AUTH_REQUIRED:-false}"
USERNAME_ENV="${USERNAME_ENV:-PROXY_WORKER_BRIDGE_USERNAME}"
PASSWORD_ENV="${PASSWORD_ENV:-PROXY_WORKER_BRIDGE_PASSWORD}"
MTLS_MODE="${MTLS_MODE:-optional}"
CERT_ROTATION_ENABLED="${CERT_ROTATION_ENABLED:-false}"
CERT_RENEW_BEFORE_DAYS="${CERT_RENEW_BEFORE_DAYS:-30}"
ACL_ENFORCEMENT="${ACL_ENFORCEMENT:-true}"
ACL_DEFAULT_POLICY="${ACL_DEFAULT_POLICY:-deny}"
BINDING_VERSION="${BINDING_VERSION:-v35_1_preproduction_final_runtime}"
HEALTH_PATH="${HEALTH_PATH:-logs/system_logs/runtime_health.json}"

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
    --auth-required) AUTH_REQUIRED="$2"; shift 2 ;;
    --username-env) USERNAME_ENV="$2"; shift 2 ;;
    --password-env) PASSWORD_ENV="$2"; shift 2 ;;
    --mtls-mode) MTLS_MODE="$2"; shift 2 ;;
    --cert-rotation-enabled) CERT_ROTATION_ENABLED="$2"; shift 2 ;;
    --cert-renew-before-days) CERT_RENEW_BEFORE_DAYS="$2"; shift 2 ;;
    --acl-enforcement) ACL_ENFORCEMENT="$2"; shift 2 ;;
    --acl-default-policy) ACL_DEFAULT_POLICY="$2"; shift 2 ;;
    --binding-version) BINDING_VERSION="$2"; shift 2 ;;
    --health-path) HEALTH_PATH="$2"; shift 2 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

CONFIG_PATH="${RUNTIME_ROOT}/product/config/application/app_config.yaml"
if [ ! -f "$CONFIG_PATH" ]; then
  printf 'Configuration file not found: %s\n' "$CONFIG_PATH" >&2
  exit 1
fi

export CONFIG_PATH BROKER_HOST BROKER_PORT SECURE_PORT WORKER_ID NODE_ID CLIENT_ID CA_CERT_FILE AUTH_REQUIRED USERNAME_ENV PASSWORD_ENV MTLS_MODE CERT_ROTATION_ENABLED CERT_RENEW_BEFORE_DAYS ACL_ENFORCEMENT ACL_DEFAULT_POLICY BINDING_VERSION HEALTH_PATH
python3 - <<'PY'
from pathlib import Path
import os
import yaml


def as_bool(value):
    return str(value).strip().lower() in ("1", "true", "yes", "on", "enabled")


config_path = Path(os.environ["CONFIG_PATH"])
data = yaml.safe_load(config_path.read_text(encoding="utf-8"))

base = data.setdefault("base", {})
app = base.setdefault("app", {})
bridge = base.setdefault("proxy_worker_bridge", {})
security = base.setdefault("security", {})
security_mqtt = security.setdefault("mqtt", {})
security_pki = security.setdefault("pki", {})
monitoring = base.setdefault("monitoring", {})
health = monitoring.setdefault("health", {})
alerting = monitoring.setdefault("alerting", {})
fieldbus = base.setdefault("fieldbus", {})
runtime_profile = fieldbus.setdefault("runtime_profile", {})
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
bridge["authentication_required"] = as_bool(os.environ["AUTH_REQUIRED"])
bridge["username"] = None
bridge["password"] = None
bridge["username_env"] = os.environ["USERNAME_ENV"]
bridge["password_env"] = os.environ["PASSWORD_ENV"]
bridge["client_id"] = os.environ["CLIENT_ID"]
bridge["mtls_mode"] = os.environ["MTLS_MODE"]
bridge["certificate_rotation_enabled"] = as_bool(os.environ["CERT_ROTATION_ENABLED"])
bridge["certificate_renew_before_days"] = int(os.environ["CERT_RENEW_BEFORE_DAYS"])
bridge["topic_acl_enforcement"] = as_bool(os.environ["ACL_ENFORCEMENT"])
bridge["topic_acl_default_policy"] = os.environ["ACL_DEFAULT_POLICY"]
bridge["allowed_publish_topics"] = [
    "worker/{worker_id}/reply/#",
    "worker/{worker_id}/presence",
    "worker/{worker_id}/snapshot/#",
    "worker/{worker_id}/event/#",
]
bridge["allowed_subscribe_topics"] = ["worker/{worker_id}/command/+"]
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

security_mqtt["authentication_required"] = bridge["authentication_required"]
security_mqtt["topic_acl_enforcement"] = bridge["topic_acl_enforcement"]
security_mqtt["topic_acl_default_policy"] = bridge["topic_acl_default_policy"]
security_pki["mtls_mode"] = bridge["mtls_mode"]
security_pki["certificate_rotation_enabled"] = bridge["certificate_rotation_enabled"]
security_pki["certificate_renew_before_days"] = bridge["certificate_renew_before_days"]

health["enabled"] = True
health["path"] = os.environ["HEALTH_PATH"]
health.setdefault("interval_s", 15.0)
health.setdefault("max_age_s", 120.0)
alerting.setdefault("enabled", False)
alerting.setdefault("log_only", True)
alerting.setdefault("worker_presence_max_age_s", 120.0)

runtime_profile.setdefault("enabled", True)
runtime_profile.setdefault("mode", "simulation_subset")
runtime_profile.setdefault("active_controller_ids", [2])
runtime_profile.setdefault("optional_controller_ids", [1, 3, 4, 5])
runtime_profile.setdefault("strict_missing_simulators", False)

mqtt_client["enabled"] = False
mqtt_client["broker_ip"] = os.environ["BROKER_HOST"]
mqtt_client["broker_port"] = int(os.environ["SECURE_PORT"])
mqtt_client.setdefault("ssl", {})
mqtt_client["ssl"]["enabled"] = True
mqtt_client["ssl"]["cafile"] = os.environ["CA_CERT_FILE"]

config_path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
print("Runtime proxy bridge configuration patched:", config_path)
PY
