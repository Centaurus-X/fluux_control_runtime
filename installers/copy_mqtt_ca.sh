#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

RUNTIME_ROOT="${RUNTIME_ROOT:-/opt/projektstand_v35_1_preproduction_final_runtime}"
MQTT_HOST="${MQTT_HOST:-192.168.0.31}"
MQTT_USER="${MQTT_USER:-${USER}}"
PROXY_RELEASE_ROOT="${PROXY_RELEASE_ROOT:-/opt/product_proxy_gateway_v01_8_6}"
SOURCE_CA="${SOURCE_CA:-${PROXY_RELEASE_ROOT}/docker/mqtt/emqx/certs/ca.pem}"
TARGET_CA="${TARGET_CA:-${RUNTIME_ROOT}/product/config/ssl/certs/mqtt/emqx-root-ca.pem}"

while [ $# -gt 0 ]; do
  case "$1" in
    --runtime-root) RUNTIME_ROOT="$2"; shift 2 ;;
    --mqtt-host) MQTT_HOST="$2"; shift 2 ;;
    --mqtt-user) MQTT_USER="$2"; shift 2 ;;
    --proxy-release-root) PROXY_RELEASE_ROOT="$2"; SOURCE_CA="${PROXY_RELEASE_ROOT}/docker/mqtt/emqx/certs/ca.pem"; shift 2 ;;
    --source-ca) SOURCE_CA="$2"; shift 2 ;;
    --target-ca) TARGET_CA="$2"; shift 2 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

mkdir -p "$(dirname "$TARGET_CA")"
scp "${MQTT_USER}@${MQTT_HOST}:${SOURCE_CA}" "$TARGET_CA"
chmod 0644 "$TARGET_CA"
printf 'MQTT CA copied to: %s\n' "$TARGET_CA"
