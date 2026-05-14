# -*- coding: utf-8 -*-

# src/core/generic_mqtt_interface_thread.py

"""
Generische synchrone MQTT-Transport-Schnittstelle als Thread-Modul.

Dieses Modul stellt eine einzige funktionale MQTT-Instanz bereit, die sowohl
auf Worker-Seite als auch auf Gateway-Proxy-Seite laeuft. Es ersetzt die zuvor
getrennten Rollenmodule und kapselt die komplette Transport-Logik in einem
expliziten, thread-basierten und deterministischen Interface.

-----------------------------------------------------------------------
Zielbild
-----------------------------------------------------------------------

    [worker]n <-mqtt-> gateway-proxy <-websocket-> [client]n

Das MQTT-Interface ist absichtlich transportorientiert aufgebaut:

- interne Anwendungsschnittstelle:
    * queue_send / queue_ws
    * queue_recv / queue_io
    * queue_event_send
- externe Brokerschnittstelle:
    * definierte MQTT-Topics
    * standardisierte JSON-Envelopes
    * retained Discovery-/Registry-Themen

-----------------------------------------------------------------------
Wesentliche Eigenschaften
-----------------------------------------------------------------------

- rein funktionaler Aufbau, keine Klassen als Systemgrundlage
- keine Decorators, keine implizite Magie
- explizite Context-/Resource-Strukturen fuer Thread-Interop
- standardisierte Events und Transport-Metadaten
- thread-sichere Node-Registry und Route-Table
- deterministisches Routing
- deduplizierende Inbound-Verarbeitung fuer QoS-Redelivery-Faelle
- TLS optional, unverschluesselt ebenfalls moeglich
- runtime-konfigurierbar
- produktionstaugliche Haken fuer Metriken, Health, Backpressure-Diagnose

-----------------------------------------------------------------------
Routingmodell
-----------------------------------------------------------------------

Die Route-Table ist an klassischen Routing-Tabellen orientiert:

1) exact route
2) laengste passende prefix route
3) default route
4) kleinere metric gewinnt
5) hoehere priority gewinnt
6) stabiler lexikographischer Tie-Breaker

Dadurch kann spaeter ein dedizierter Routing-Thread auf derselben Registerbasis
aufsetzen, ohne den MQTT-Transportkern neu zu bauen.

-----------------------------------------------------------------------
Hinweis zu "production ready"
-----------------------------------------------------------------------

Die Implementierung ist bewusst fuer robuste Integrations- und Produktionsnaehe
gehaertet. Die endgueltige Freigabe fuer ein konkretes Zielsystem bleibt dennoch
an reale Broker-, Last-, TLS- und Failover-Tests gebunden.
"""

import copy
import json
import logging
import os
import queue as queue_module
import socket
import ssl
import threading
import time
import urllib.parse
import uuid
from functools import partial
from typing import Any

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

try:
    from src.libraries._evt_interface import make_event as _evt_make_event
except Exception:
    _evt_make_event = None

try:
    import src.libraries._type_notations  # noqa: F401
except Exception:
    pass

try:
    import src.libraries._type_dynamics  # noqa: F401
except Exception:
    pass


logger = logging.getLogger(__name__)


MQTT_DEFAULT_HOST = "127.0.0.1"
MQTT_DEFAULT_PORT = 1883
MQTT_DEFAULT_TLS_PORT = 8883
MQTT_DEFAULT_KEEPALIVE = 60
MQTT_DEFAULT_QOS = 1
MQTT_DEFAULT_TOPIC_ROOT = "xserver/v1"
MQTT_DEFAULT_PROTOCOL = "MQTTv311"
MQTT_DEFAULT_TRANSPORT = "tcp"

QUEUE_POLL_TIMEOUT_S = 0.20
HEARTBEAT_INTERVAL_S = 15.0
ROUTE_ADVERTISE_INTERVAL_S = 30.0
REGISTRY_STALE_AFTER_S = 45.0
ROUTE_PRUNE_AFTER_S = 300.0
CONNECT_WAIT_TIMEOUT_S = 10.0
SHUTDOWN_JOIN_TIMEOUT_S = 2.0
RECONNECT_MIN_DELAY_S = 1
RECONNECT_MAX_DELAY_S = 30
DEDUP_TTL_S = 180.0
DEDUP_MAX_ENTRIES = 20000
MAX_INFLIGHT_MESSAGES = 256
MAX_QUEUED_MESSAGES = 0

STOP_MARKERS = frozenset(("__STOP__",))
ALLOWED_INTERFACE_ROLES = frozenset(("worker", "gateway_proxy"))
MQTT_STANDARD_VERSION = 1

LOCAL_SYSTEM_API_ROUTES = {
    "connected": "MQTT_CONNECTED",
    "connect_failed": "MQTT_CONNECT_FAILED",
    "disconnected": "MQTT_DISCONNECTED",
    "node_registry_update": "MQTT_NODE_REGISTRY_UPDATE",
    "route_table_update": "MQTT_ROUTE_TABLE_UPDATE",
    "route_resolution_error": "MQTT_ROUTE_RESOLUTION_ERROR",
    "publish_error": "MQTT_PUBLISH_ERROR",
    "queue_drop": "MQTT_QUEUE_DROP",
}

MQTT_ROUTING_KEYS = (
    "target_node",
    "target_nodes",
    "target_role",
    "target_roles",
    "target_tag",
    "target_tags",
    "target_capability",
    "target_capabilities",
    "broadcast",
    "broadcast_include_self",
    "exclude_nodes",
    "route_hint",
)

ROLE_ALIASES = {
    "worker": "worker",
    "server": "worker",
    "node": "worker",
    "gateway": "gateway_proxy",
    "gateway-proxy": "gateway_proxy",
    "gateway_proxy": "gateway_proxy",
    "proxy": "gateway_proxy",
}

PROTOCOL_RANK = {
    "static": 0,
    "self": 1,
    "dynamic": 2,
    "fallback": 3,
}

STATE_RANK = {
    "up": 0,
    "online": 0,
    "stale": 1,
    "down": 2,
    "offline": 2,
}


# =============================================================================
# Basis-Helfer
# =============================================================================

def _require_paho(mqtt_module: Any = None) -> Any:
    module_obj = mqtt_module if mqtt_module is not None else mqtt
    if module_obj is None:
        raise ImportError(
            "generic_mqtt_interface_thread.py benoetigt 'paho-mqtt'. "
            "Bitte installiere das Paket oder injiziere mqtt_module fuer Tests."
        )
    return module_obj


def _now_ts() -> float:
    return time.time()


def _make_id() -> str:
    return str(uuid.uuid4())


def _deepcopy_safe(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _safe_str(value: Any, default: str = "") -> str:
    try:
        if value is None:
            return str(default)
        return str(value)
    except Exception:
        return str(default)


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False

    return bool(default)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_json_loads(payload: Any) -> Any:
    if isinstance(payload, dict):
        return payload

    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = bytes(payload).decode("utf-8")
        except Exception:
            return None

    if not isinstance(payload, str):
        return None

    try:
        return json.loads(payload)
    except Exception:
        return None


def _json_dumps(payload: Any) -> str:
    return json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
        sort_keys=False,
    )


def _normalize_collection(values: Any) -> list[str]:
    if values is None:
        return []

    if isinstance(values, (list, tuple, set, frozenset)):
        result = []
        seen = set()
        for item in values:
            item_text = _safe_str(item, "").strip()
            if not item_text or item_text in seen:
                continue
            seen.add(item_text)
            result.append(item_text)
        return result

    single = _safe_str(values, "").strip()
    if not single:
        return []
    return [single]


def _normalize_role(value: Any) -> str:
    normalized = _safe_str(value, "worker").strip().lower().replace(" ", "_")
    if not normalized:
        normalized = "worker"
    return ROLE_ALIASES.get(normalized, normalized)


def _normalize_tags(values: Any) -> list[str]:
    result = []
    seen = set()
    for item in _normalize_collection(values):
        text = item.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    result.sort()
    return result


def _normalize_capabilities(values: Any) -> dict[str, Any]:
    if isinstance(values, dict):
        return _deepcopy_safe(values)

    if isinstance(values, (list, tuple, set, frozenset)):
        result = {}
        for item in values:
            text = _safe_str(item, "").strip()
            if not text:
                continue
            result[text] = True
        return result

    text = _safe_str(values, "").strip()
    if not text:
        return {}
    return {text: True}


def _ensure_lock(lock_obj: Any) -> Any:
    try:
        if lock_obj is not None and hasattr(lock_obj, "acquire") and hasattr(lock_obj, "release"):
            return lock_obj
    except Exception:
        pass
    return threading.RLock()


def _queue_get(queue_obj: Any, timeout_s: float) -> Any:
    if queue_obj is None:
        return None

    try:
        return queue_obj.get(timeout=max(float(timeout_s), 0.01))
    except queue_module.Empty:
        return None
    except TypeError:
        try:
            return queue_obj.get()
        except Exception:
            return None
    except Exception:
        return None


def _queue_put(queue_obj: Any, item: Any, timeout_s: float) -> bool:
    if queue_obj is None:
        return False

    try:
        queue_obj.put(item, timeout=max(float(timeout_s), 0.01))
        return True
    except queue_module.Full:
        return False
    except TypeError:
        try:
            queue_obj.put(item)
            return True
        except Exception:
            return False
    except Exception:
        return False


def _queue_qsize(queue_obj: Any) -> int:
    if queue_obj is None:
        return -1
    try:
        return int(queue_obj.qsize())
    except Exception:
        return -1


def _is_stop_message(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    marker_type = _safe_str(payload.get("_type"), "").strip().upper()
    marker_event_type = _safe_str(payload.get("event_type"), "").strip().upper()
    return marker_type in STOP_MARKERS or marker_event_type in STOP_MARKERS


def _best_effort_local_ip(remote_host: str, remote_port: int) -> str:
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect((_safe_str(remote_host, MQTT_DEFAULT_HOST), int(remote_port)))
        return _safe_str(sock.getsockname()[0], "")
    except Exception:
        try:
            return _safe_str(socket.gethostbyname(socket.gethostname()), "")
        except Exception:
            return ""
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def _validate_topic_root(topic_root: Any) -> str:
    normalized = _safe_str(topic_root, MQTT_DEFAULT_TOPIC_ROOT).strip().strip("/")
    if not normalized:
        raise ValueError("topic_root darf nicht leer sein.")
    if "+" in normalized or "#" in normalized:
        raise ValueError("topic_root darf keine MQTT-Wildcards enthalten.")
    return normalized


def _validate_node_id(node_id: Any) -> str:
    normalized = _safe_str(node_id, "").strip()
    if not normalized:
        raise ValueError("node_id darf nicht leer sein.")
    forbidden = set("#+/\\")
    for ch in normalized:
        if ch in forbidden:
            raise ValueError("node_id enthaelt ungueltige Zeichen fuer MQTT-Topics: %r" % ch)
    return normalized


def _validate_interface_role(value: Any) -> str:
    normalized = _normalize_role(value)
    if normalized not in ALLOWED_INTERFACE_ROLES:
        raise ValueError(
            "interface_role/path_gate ungueltig: %r. Erlaubt: %s"
            % (_safe_str(value, ""), sorted(ALLOWED_INTERFACE_ROLES))
        )
    return normalized


# =============================================================================
# Topic-Helfer
# =============================================================================

def _encode_topic_part(value: Any) -> str:
    return urllib.parse.quote(_safe_str(value, "").strip(), safe="-_.:")


def _normalize_topic_root(root: Any) -> str:
    return _validate_topic_root(root)


def make_mqtt_request_topic(topic_root: Any, node_id: Any) -> str:
    return "%s/nodes/%s/request" % (
        _normalize_topic_root(topic_root),
        _encode_topic_part(node_id),
    )


def make_mqtt_node_status_topic(topic_root: Any, node_id: Any) -> str:
    return "%s/discovery/nodes/%s/status" % (
        _normalize_topic_root(topic_root),
        _encode_topic_part(node_id),
    )


def make_mqtt_node_routes_topic(topic_root: Any, node_id: Any) -> str:
    return "%s/discovery/nodes/%s/routes" % (
        _normalize_topic_root(topic_root),
        _encode_topic_part(node_id),
    )


def make_mqtt_probe_topic(topic_root: Any) -> str:
    return "%s/discovery/probe" % _normalize_topic_root(topic_root)


def make_mqtt_node_status_wildcard(topic_root: Any) -> str:
    return "%s/discovery/nodes/+/status" % _normalize_topic_root(topic_root)


def make_mqtt_node_routes_wildcard(topic_root: Any) -> str:
    return "%s/discovery/nodes/+/routes" % _normalize_topic_root(topic_root)


def _extract_node_id_from_topic(topic_root: Any, topic: Any, suffix: str) -> str:
    topic_text = _safe_str(topic, "").strip()
    prefix = "%s/discovery/nodes/" % _normalize_topic_root(topic_root)
    expected_suffix = "/%s" % _safe_str(suffix, "")

    if not topic_text.startswith(prefix):
        return ""
    if not topic_text.endswith(expected_suffix):
        return ""

    payload = topic_text[len(prefix):]
    payload = payload[: len(payload) - len(expected_suffix)]
    if not payload:
        return ""
    return urllib.parse.unquote(payload)


def _extract_node_id_from_status_topic(topic_root: Any, topic: Any) -> str:
    return _extract_node_id_from_topic(topic_root, topic, "status")


def _extract_node_id_from_routes_topic(topic_root: Any, topic: Any) -> str:
    return _extract_node_id_from_topic(topic_root, topic, "routes")


# =============================================================================
# TLS / SSL
# =============================================================================

def _normalize_tls_version_name(value: Any) -> str:
    normalized = _safe_str(value, "").strip().upper()
    normalized = normalized.replace(" ", "")
    normalized = normalized.replace(".", "_")
    return normalized


def _resolve_tls_version(value: Any) -> Any:
    normalized = _normalize_tls_version_name(value)
    mapping = {
        "TLSV1_2": getattr(ssl.TLSVersion, "TLSv1_2", None),
        "TLS1_2": getattr(ssl.TLSVersion, "TLSv1_2", None),
        "TLSV1_3": getattr(ssl.TLSVersion, "TLSv1_3", None),
        "TLS1_3": getattr(ssl.TLSVersion, "TLSv1_3", None),
    }
    return mapping.get(normalized)


def _abs_path_text(path_value: Any) -> str:
    text = _safe_str(path_value, "").strip()
    if not text:
        return ""
    return os.path.abspath(os.path.expanduser(os.path.expandvars(text)))


def _validate_tls_path(
    path_value: Any,
    *,
    label: str,
    required: bool,
    expect_directory: bool,
) -> str:
    text = _safe_str(path_value, "").strip()
    if not text:
        if required:
            raise FileNotFoundError("TLS-Konfigurationsfehler: %s fehlt." % label)
        return ""

    abs_path = _abs_path_text(text)
    if expect_directory:
        if not os.path.isdir(abs_path):
            raise FileNotFoundError(
                "TLS-Konfigurationsfehler: %s Verzeichnis nicht gefunden: %s"
                % (label, abs_path)
            )
    else:
        if not os.path.isfile(abs_path):
            raise FileNotFoundError(
                "TLS-Konfigurationsfehler: %s Datei nicht gefunden: %s"
                % (label, abs_path)
            )
    return abs_path


def build_mqtt_ssl_context(
    *,
    enabled: bool,
    cafile: Any = None,
    capath: Any = None,
    cadata: Any = None,
    certfile: Any = None,
    keyfile: Any = None,
    password: Any = None,
    minimum_tls_version: Any = "TLSv1_2",
    ciphers: Any = None,
    allow_unverified: bool = False,
    auto_allow_unverified_if_no_ca: bool = False,
) -> Any:
    if not _safe_bool(enabled, False):
        return None

    cafile_path = ""
    capath_path = ""
    certfile_path = ""
    keyfile_path = ""

    if _safe_str(cafile, "").strip():
        cafile_path = _validate_tls_path(
            cafile,
            label="ssl_cafile",
            required=False,
            expect_directory=False,
        )
    if _safe_str(capath, "").strip():
        capath_path = _validate_tls_path(
            capath,
            label="ssl_capath",
            required=False,
            expect_directory=True,
        )
    if _safe_str(certfile, "").strip():
        certfile_path = _validate_tls_path(
            certfile,
            label="ssl_certfile",
            required=False,
            expect_directory=False,
        )
    if _safe_str(keyfile, "").strip():
        keyfile_path = _validate_tls_path(
            keyfile,
            label="ssl_keyfile",
            required=False,
            expect_directory=False,
        )

    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

    tls_version = _resolve_tls_version(minimum_tls_version)
    if tls_version is not None:
        ssl_context.minimum_version = tls_version

    if cafile_path or capath_path or cadata:
        ssl_context.load_verify_locations(
            cafile=cafile_path or None,
            capath=capath_path or None,
            cadata=cadata,
        )

    if certfile_path:
        ssl_context.load_cert_chain(
            certfile=certfile_path,
            keyfile=keyfile_path or None,
            password=password,
        )

    ciphers_text = _safe_str(ciphers, "").strip()
    if ciphers_text:
        ssl_context.set_ciphers(ciphers_text)

    allow_unverified_effective = _safe_bool(allow_unverified, False)
    if not allow_unverified_effective and _safe_bool(auto_allow_unverified_if_no_ca, False):
        if not (cafile_path or capath_path or cadata):
            allow_unverified_effective = True

    if allow_unverified_effective:
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    return ssl_context


# =============================================================================
# Resource-Helfer / Shared-State
# =============================================================================

def _extract_resource_pair(resources: Any, name: str) -> tuple[Any, Any]:
    if not isinstance(resources, dict):
        return None, None

    entry = resources.get(name)
    if isinstance(entry, dict) and ("data" in entry or "lock" in entry):
        return entry.get("data"), entry.get("lock")

    if entry is None:
        return None, None

    return entry, None


def _ensure_shared_resource(
    resources: Any,
    resource_name: str,
    data_candidate: Any = None,
    lock_candidate: Any = None,
) -> tuple[dict[str, Any], Any]:
    existing_data, existing_lock = _extract_resource_pair(resources, resource_name)

    if isinstance(existing_data, dict):
        data_obj = existing_data
    elif isinstance(data_candidate, dict):
        data_obj = data_candidate
    else:
        data_obj = {}

    lock_obj = _ensure_lock(existing_lock or lock_candidate)

    if isinstance(resources, dict):
        resources[resource_name] = {
            "data": data_obj,
            "lock": lock_obj,
        }

    return data_obj, lock_obj


def _create_runtime_metrics_state() -> dict[str, Any]:
    now_ts = _now_ts()
    return {
        "created_at": now_ts,
        "last_updated": now_ts,
        "connection_attempts": 0,
        "connect_success": 0,
        "connect_failures": 0,
        "disconnects": 0,
        "reconnects": 0,
        "publish_attempts": 0,
        "published_messages": 0,
        "publish_errors": 0,
        "received_messages": 0,
        "received_request_messages": 0,
        "received_discovery_messages": 0,
        "inbound_duplicates_dropped": 0,
        "inbound_queue_forwarded": 0,
        "inbound_queue_drops": 0,
        "local_system_messages_emitted": 0,
        "event_bus_messages_emitted": 0,
        "event_bus_queue_drops": 0,
        "route_resolution_failures": 0,
        "outbound_queue_drops": 0,
        "node_registry_updates": 0,
        "route_table_updates": 0,
        "housekeeping_runs": 0,
        "status_publish_count": 0,
        "route_publish_count": 0,
        "probe_publish_count": 0,
        "send_queue_depth": 0,
        "recv_queue_depth": 0,
        "incoming_queue_depth": 0,
        "connected": False,
        "subscriptions_ready": False,
        "last_connect_ts": 0.0,
        "last_disconnect_ts": 0.0,
        "last_message_ts": 0.0,
        "last_publish_ts": 0.0,
        "last_housekeeping_ts": 0.0,
    }


def _metrics_touch(metrics: dict[str, Any]) -> None:
    metrics["last_updated"] = _now_ts()


def _metrics_inc(ctx: dict[str, Any], key: str, amount: int = 1) -> None:
    metrics = ctx["tables"]["runtime_metrics"]
    lock = ctx["tables"]["runtime_metrics_lock"]
    with lock:
        metrics[key] = _safe_int(metrics.get(key), 0) + int(amount)
        _metrics_touch(metrics)


def _metrics_set(ctx: dict[str, Any], key: str, value: Any) -> None:
    metrics = ctx["tables"]["runtime_metrics"]
    lock = ctx["tables"]["runtime_metrics_lock"]
    with lock:
        metrics[key] = value
        _metrics_touch(metrics)


def _metrics_mark_now(ctx: dict[str, Any], key: str) -> None:
    _metrics_set(ctx, key, _now_ts())


def _metrics_refresh_queue_depths(ctx: dict[str, Any]) -> None:
    metrics = ctx["tables"]["runtime_metrics"]
    lock = ctx["tables"]["runtime_metrics_lock"]
    with lock:
        metrics["send_queue_depth"] = _queue_qsize(ctx.get("queues", {}).get("send"))
        metrics["recv_queue_depth"] = _queue_qsize(ctx.get("queues", {}).get("recv"))
        metrics["incoming_queue_depth"] = _queue_qsize(ctx.get("queues", {}).get("incoming"))
        metrics["connected"] = bool(ctx.get("runtime", {}).get("connected_to_broker", threading.Event()).is_set())
        metrics["subscriptions_ready"] = bool(ctx.get("runtime", {}).get("subscriptions_ready", threading.Event()).is_set())
        _metrics_touch(metrics)


def snapshot_mqtt_runtime_metrics(runtime_ctx: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(runtime_ctx, dict):
        return {}

    tables = runtime_ctx.get("tables") if isinstance(runtime_ctx.get("tables"), dict) else runtime_ctx
    metrics = tables.get("runtime_metrics")
    metrics_lock = tables.get("runtime_metrics_lock")

    if not isinstance(metrics, dict):
        return {}

    lock_obj = _ensure_lock(metrics_lock)
    with lock_obj:
        snapshot = _deepcopy_safe(metrics)

    if not isinstance(snapshot, dict):
        return {}

    ordered = {}
    for key in sorted(snapshot.keys()):
        ordered[key] = snapshot[key]
    return ordered


def get_generic_mqtt_interface_health(runtime_ctx: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(runtime_ctx, dict):
        return {}

    ctx = runtime_ctx.get("ctx") if isinstance(runtime_ctx.get("ctx"), dict) else runtime_ctx
    if not isinstance(ctx, dict):
        return {}

    thread_states = {}
    for thread_obj in list(ctx.get("runtime", {}).get("threads") or []):
        try:
            thread_states[thread_obj.name] = bool(thread_obj.is_alive())
        except Exception:
            thread_states[_safe_str(getattr(thread_obj, "name", "unknown"), "unknown")] = False

    health = {
        "node_id": ctx.get("node_id"),
        "interface_role": ctx.get("interface_role"),
        "broker_host": ctx.get("broker_host"),
        "broker_port": ctx.get("broker_port"),
        "topic_root": ctx.get("topic_root"),
        "ssl_enabled": bool(ctx.get("ssl_enabled")),
        "connected": bool(ctx.get("runtime", {}).get("connected_to_broker", threading.Event()).is_set()),
        "subscriptions_ready": bool(ctx.get("runtime", {}).get("subscriptions_ready", threading.Event()).is_set()),
        "shutdown_requested": bool(ctx.get("runtime", {}).get("stop_requested", threading.Event()).is_set()),
        "thread_states": thread_states,
        "metrics": snapshot_mqtt_runtime_metrics(ctx),
        "node_registry_size": len(snapshot_mqtt_node_registry(ctx)),
        "route_table_size": len(snapshot_mqtt_route_table(ctx, grouped=False)),
    }
    return health


# =============================================================================
# Deduplizierung
# =============================================================================

def _make_dedup_fingerprint(message: dict[str, Any], scope: str) -> str:
    if not isinstance(message, dict):
        return ""

    message_id = _safe_str(message.get("message_id") or message.get("id"), "").strip()
    source_node = _safe_str(message.get("source_node") or message.get("source"), "").strip()
    api_route = _safe_str(
        message.get("api_route")
        or message.get("route")
        or message.get("command")
        or message.get("message_type"),
        "",
    ).strip()

    if message_id:
        return "%s|%s|%s|%s" % (scope, source_node, api_route, message_id)

    payload = message.get("payload")
    payload_text = _json_dumps(payload) if isinstance(payload, (dict, list, tuple, str, int, float, bool, type(None))) else _safe_str(payload, "")
    timestamp = _safe_str(message.get("timestamp"), "")
    return "%s|%s|%s|%s|%s" % (scope, source_node, api_route, timestamp, payload_text)


def _dedup_expiry_order_key(item: tuple[str, float]) -> float:
    return _safe_float(item[1], 0.0)


def _dedup_prune_locked(dedup_entries: dict[str, float], now_ts: float, max_entries: int) -> None:
    expired_keys = []
    for key, expires_at in list(dedup_entries.items()):
        if _safe_float(expires_at, 0.0) <= now_ts:
            expired_keys.append(key)

    for key in expired_keys:
        dedup_entries.pop(key, None)

    if len(dedup_entries) <= max_entries:
        return

    overflow = len(dedup_entries) - max_entries
    ordered = sorted(dedup_entries.items(), key=_dedup_expiry_order_key)
    for key, _value in ordered[:overflow]:
        dedup_entries.pop(key, None)


def _dedup_register_message(
    ctx: dict[str, Any],
    scope: str,
    message: dict[str, Any],
) -> bool:
    fingerprint = _make_dedup_fingerprint(message, scope)
    if not fingerprint:
        return False

    state = ctx.get("runtime", {}).get("dedup_state")
    if not isinstance(state, dict):
        return False

    entries = state.get("entries")
    lock = state.get("lock")
    ttl_s = max(_safe_float(state.get("ttl_s"), DEDUP_TTL_S), 1.0)
    max_entries = max(_safe_int(state.get("max_entries"), DEDUP_MAX_ENTRIES), 100)
    now_ts = _now_ts()

    if not isinstance(entries, dict):
        return False

    lock_obj = _ensure_lock(lock)
    with lock_obj:
        _dedup_prune_locked(entries, now_ts, max_entries)
        expires_at = entries.get(fingerprint)
        if _safe_float(expires_at, 0.0) > now_ts:
            return True
        entries[fingerprint] = now_ts + ttl_s
    return False


def _dedup_housekeeping(ctx: dict[str, Any]) -> None:
    state = ctx.get("runtime", {}).get("dedup_state")
    if not isinstance(state, dict):
        return

    entries = state.get("entries")
    if not isinstance(entries, dict):
        return

    lock = _ensure_lock(state.get("lock"))
    now_ts = _now_ts()
    max_entries = max(_safe_int(state.get("max_entries"), DEDUP_MAX_ENTRIES), 100)
    with lock:
        _dedup_prune_locked(entries, now_ts, max_entries)


# =============================================================================
# Event-Emission / Queue-Wrapper
# =============================================================================

def _put_to_named_queue(
    ctx: dict[str, Any],
    queue_name: str,
    item: Any,
    timeout_s: float,
    *,
    ok_metric: str | None = None,
    fail_metric: str | None = None,
    log_label: str,
) -> bool:
    queue_obj = ctx.get("queues", {}).get(queue_name)
    ok = _queue_put(queue_obj, item, timeout_s)
    if ok:
        if ok_metric:
            _metrics_inc(ctx, ok_metric)
    else:
        if fail_metric:
            _metrics_inc(ctx, fail_metric)
        logger.warning("MQTT-Interface Queue-Drop: queue=%s label=%s", queue_name, log_label)
        if queue_name != "recv":
            _emit_local_system_message(
                ctx,
                subtype="queue_drop",
                payload={
                    "queue_name": queue_name,
                    "label": log_label,
                },
                meta={"drop": True},
            )
    return ok


def _build_bus_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    source: str,
    target: str | None,
    meta: dict[str, Any] | None,
) -> dict[str, Any]:
    if callable(_evt_make_event):
        event_obj = _evt_make_event(event_type, payload or {}, source=source, meta=meta or {})
    else:
        event_obj = {
            "event_id": _make_id(),
            "event_type": event_type,
            "timestamp": _now_ts(),
            "payload": payload or {},
            "source": source,
            "meta": meta or {},
        }

    if target:
        event_obj["target"] = target
    return event_obj


def _emit_bus_event(
    ctx: dict[str, Any],
    event_type: str,
    payload: dict[str, Any],
    *,
    target: str | None = "telemetry",
    meta: dict[str, Any] | None = None,
) -> bool:
    queue_event_send = ctx.get("queues", {}).get("event_send")
    if queue_event_send is None:
        return False

    source = "mqtt:%s" % _safe_str(ctx.get("node_id"), "node")
    event_obj = _build_bus_event(
        event_type,
        payload,
        source=source,
        target=target,
        meta=meta,
    )
    ok = _queue_put(queue_event_send, event_obj, QUEUE_POLL_TIMEOUT_S)
    if ok:
        _metrics_inc(ctx, "event_bus_messages_emitted")
    else:
        _metrics_inc(ctx, "event_bus_queue_drops")
    return ok


def _emit_local_system_message(
    ctx: dict[str, Any],
    *,
    subtype: str,
    payload: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
) -> bool:
    if not _safe_bool(ctx.get("recv_system_events"), False):
        return False

    api_route = LOCAL_SYSTEM_API_ROUTES.get(subtype, "MQTT_SYSTEM")
    message = {
        "type": "system",
        "subtype": subtype,
        "message_id": _make_id(),
        "timestamp": _now_ts(),
        "source": "mqtt-interface",
        "source_node": _safe_str(ctx.get("node_id"), ""),
        "destination": "",
        "api_route": api_route,
        "payload": payload or {},
        "meta": meta or {},
        "target_node": "",
    }
    return _put_to_named_queue(
        ctx,
        "recv",
        message,
        QUEUE_POLL_TIMEOUT_S,
        ok_metric="local_system_messages_emitted",
        fail_metric="inbound_queue_drops",
        log_label="local_system_message",
    )


# =============================================================================
# Paho / MQTT Client-Helfer
# =============================================================================

def _resolve_callback_api_version(mqtt_mod: Any) -> Any:
    enum_type = getattr(mqtt_mod, "CallbackAPIVersion", None)
    if enum_type is None:
        return None

    for attr_name in ("VERSION2", "API_VERSION2", "VERSION1"):
        value = getattr(enum_type, attr_name, None)
        if value is not None:
            return value
    return None


def _resolve_protocol_constant(mqtt_mod: Any, protocol_version: Any) -> Any:
    normalized = _safe_str(protocol_version, MQTT_DEFAULT_PROTOCOL).strip().upper()
    mapping = {
        "5": getattr(mqtt_mod, "MQTTv5", None),
        "MQTTV5": getattr(mqtt_mod, "MQTTv5", None),
        "3.1.1": getattr(mqtt_mod, "MQTTv311", None),
        "311": getattr(mqtt_mod, "MQTTv311", None),
        "MQTTV311": getattr(mqtt_mod, "MQTTv311", None),
        "3.1": getattr(mqtt_mod, "MQTTv31", None),
        "31": getattr(mqtt_mod, "MQTTv31", None),
        "MQTTV31": getattr(mqtt_mod, "MQTTv31", None),
    }
    protocol_constant = mapping.get(normalized)
    if protocol_constant is None:
        protocol_constant = getattr(mqtt_mod, "MQTTv311", None)
    return protocol_constant


def _create_paho_client(
    *,
    mqtt_mod: Any,
    client_id: str,
    userdata: Any,
    protocol_version: Any,
    clean_session: bool,
    transport: str,
    reconnect_on_failure: bool,
) -> Any:
    callback_api_version = _resolve_callback_api_version(mqtt_mod)
    protocol_constant = _resolve_protocol_constant(mqtt_mod, protocol_version)
    mqtt_v5_constant = getattr(mqtt_mod, "MQTTv5", None)

    base_kwargs = {
        "client_id": client_id,
        "userdata": userdata,
        "protocol": protocol_constant,
        "transport": _safe_str(transport, MQTT_DEFAULT_TRANSPORT) or MQTT_DEFAULT_TRANSPORT,
    }

    if protocol_constant != mqtt_v5_constant:
        base_kwargs["clean_session"] = bool(clean_session)

    client_obj = None

    if callback_api_version is not None:
        try:
            client_obj = mqtt_mod.Client(
                callback_api_version=callback_api_version,
                reconnect_on_failure=bool(reconnect_on_failure),
                **base_kwargs,
            )
        except TypeError:
            try:
                client_obj = mqtt_mod.Client(
                    callback_api_version,
                    reconnect_on_failure=bool(reconnect_on_failure),
                    **base_kwargs,
                )
            except TypeError:
                client_obj = None

    if client_obj is None:
        try:
            client_obj = mqtt_mod.Client(
                reconnect_on_failure=bool(reconnect_on_failure),
                **base_kwargs,
            )
        except TypeError:
            client_obj = mqtt_mod.Client(**base_kwargs)

    try:
        client_obj.user_data_set(userdata)
    except Exception:
        pass

    return client_obj


def _reason_code_text(reason_code: Any) -> str:
    try:
        if reason_code is None:
            return "0"
        return str(reason_code)
    except Exception:
        return "unknown"


def _configure_client_authentication(client_obj: Any, username: Any, password: Any) -> None:
    username_text = _safe_str(username, "").strip()
    if not username_text:
        return

    try:
        client_obj.username_pw_set(username_text, _safe_str(password, "") or None)
    except Exception as exc:
        raise ValueError("MQTT username/password Konfiguration fehlgeschlagen: %s" % exc) from exc


def _configure_client_tls(
    client_obj: Any,
    *,
    ssl_enabled: bool,
    ssl_cafile: Any,
    ssl_capath: Any,
    ssl_cadata: Any,
    ssl_certfile: Any,
    ssl_keyfile: Any,
    ssl_password: Any,
    ssl_minimum_tls_version: Any,
    ssl_ciphers: Any,
    ssl_allow_unverified: bool,
    ssl_auto_allow_unverified_if_no_ca: bool,
) -> None:
    if not _safe_bool(ssl_enabled, False):
        return

    ssl_context = build_mqtt_ssl_context(
        enabled=True,
        cafile=ssl_cafile,
        capath=ssl_capath,
        cadata=ssl_cadata,
        certfile=ssl_certfile,
        keyfile=ssl_keyfile,
        password=ssl_password,
        minimum_tls_version=ssl_minimum_tls_version,
        ciphers=ssl_ciphers,
        allow_unverified=ssl_allow_unverified,
        auto_allow_unverified_if_no_ca=ssl_auto_allow_unverified_if_no_ca,
    )

    client_obj.tls_set_context(ssl_context)

    insecure_effective = _safe_bool(ssl_allow_unverified, False)
    if not insecure_effective and _safe_bool(ssl_auto_allow_unverified_if_no_ca, False):
        no_ca_material = not (
            _safe_str(ssl_cafile, "").strip()
            or _safe_str(ssl_capath, "").strip()
            or ssl_cadata
        )
        insecure_effective = bool(no_ca_material)

    if insecure_effective:
        try:
            client_obj.tls_insecure_set(True)
        except Exception:
            pass


def _configure_client_runtime_limits(
    client_obj: Any,
    *,
    max_inflight_messages: int,
    max_queued_messages: int,
    queue_qos0_messages: bool,
) -> None:
    try:
        client_obj.max_inflight_messages_set(max(1, int(max_inflight_messages)))
    except Exception:
        pass

    try:
        client_obj.max_queued_messages_set(max(0, int(max_queued_messages)))
    except Exception:
        pass

    try:
        client_obj.queue_qos0_messages = bool(queue_qos0_messages)
    except Exception:
        pass


def _configure_disconnect_will(ctx: dict[str, Any]) -> None:
    client_obj = ctx.get("runtime", {}).get("client_obj")
    if client_obj is None:
        return

    will_topic = make_mqtt_node_status_topic(ctx["topic_root"], ctx["node_id"])
    will_payload = _json_dumps(_build_node_status_payload(ctx, state="offline"))

    try:
        client_obj.will_set(
            will_topic,
            will_payload,
            qos=int(ctx["qos"]),
            retain=True,
        )
    except Exception as exc:
        logger.warning("MQTT Last-Will Konfiguration fehlgeschlagen: %s", exc)


def _client_publish(
    ctx: dict[str, Any],
    topic: str,
    payload: dict[str, Any],
    *,
    qos: int | None = None,
    retain: bool = False,
) -> bool:
    client_obj = ctx.get("runtime", {}).get("client_obj")
    if client_obj is None:
        _metrics_inc(ctx, "publish_errors")
        return False

    effective_qos = int(ctx.get("qos")) if qos is None else int(qos)
    mqtt_lock = ctx.get("runtime", {}).get("mqtt_lock")
    if mqtt_lock is None:
        mqtt_lock = threading.RLock()

    _metrics_inc(ctx, "publish_attempts")

    try:
        mqtt_lock.acquire()
        message_info = client_obj.publish(
            topic,
            _json_dumps(payload),
            qos=effective_qos,
            retain=bool(retain),
        )
    except Exception as exc:
        logger.warning("MQTT publish fehlgeschlagen fuer topic=%s: %s", topic, exc)
        _metrics_inc(ctx, "publish_errors")
        return False
    finally:
        try:
            mqtt_lock.release()
        except Exception:
            pass

    rc = getattr(message_info, "rc", 0)
    try:
        ok = int(rc) == 0
    except Exception:
        ok = False

    if ok:
        _metrics_inc(ctx, "published_messages")
        _metrics_mark_now(ctx, "last_publish_ts")
    else:
        _metrics_inc(ctx, "publish_errors")
    return ok


def _client_subscribe_many(ctx: dict[str, Any], topic_qos_pairs: list[tuple[str, int]]) -> bool:
    if not topic_qos_pairs:
        return True

    client_obj = ctx.get("runtime", {}).get("client_obj")
    if client_obj is None:
        return False

    mqtt_lock = ctx.get("runtime", {}).get("mqtt_lock")
    if mqtt_lock is None:
        mqtt_lock = threading.RLock()

    try:
        mqtt_lock.acquire()
        result, _mid = client_obj.subscribe(topic_qos_pairs)
        return int(result) == 0
    except Exception as exc:
        logger.warning("MQTT subscribe fehlgeschlagen: %s", exc)
        return False
    finally:
        try:
            mqtt_lock.release()
        except Exception:
            pass


def _client_disconnect(ctx: dict[str, Any]) -> bool:
    client_obj = ctx.get("runtime", {}).get("client_obj")
    if client_obj is None:
        return False

    mqtt_lock = ctx.get("runtime", {}).get("mqtt_lock")
    if mqtt_lock is None:
        mqtt_lock = threading.RLock()

    try:
        mqtt_lock.acquire()
        client_obj.disconnect()
        return True
    except Exception:
        return False
    finally:
        try:
            mqtt_lock.release()
        except Exception:
            pass


# =============================================================================
# Routing / Route-Table
# =============================================================================

def _route_key(entry: dict[str, Any]) -> str:
    parts = [
        _safe_str(entry.get("destination_prefix"), "default"),
        _safe_str(entry.get("match_mode"), "exact"),
        _safe_str(entry.get("next_hop"), ""),
        _safe_str(entry.get("protocol"), "dynamic"),
        _safe_str(entry.get("owner_node"), ""),
        _safe_str(entry.get("interface"), "mqtt"),
    ]
    return "|".join(parts)


def _derive_match_mode(destination_prefix: str, explicit_match_mode: Any = None) -> str:
    explicit = _safe_str(explicit_match_mode, "").strip().lower()
    if explicit in ("exact", "prefix", "default"):
        return explicit

    normalized = _safe_str(destination_prefix, "").strip()
    if not normalized or normalized.lower() in ("default", "*"):
        return "default"
    if normalized.endswith("*"):
        return "prefix"
    return "exact"


def _route_order_key(entry: dict[str, Any], destination_node: str) -> tuple[Any, ...]:
    match_mode = _safe_str(entry.get("match_mode"), "default").lower()
    mode_rank = {
        "exact": 0,
        "prefix": 1,
        "default": 2,
    }.get(match_mode, 9)

    destination_prefix = _safe_str(entry.get("destination_prefix"), "")
    if match_mode == "default":
        match_len = 0
    elif match_mode == "prefix" and destination_prefix.endswith("*"):
        match_len = len(destination_prefix[:-1])
    else:
        match_len = len(destination_prefix)

    metric = max(_safe_int(entry.get("metric"), 0), 0)
    priority = _safe_int(entry.get("priority"), 0)
    protocol = _safe_str(entry.get("protocol"), "dynamic").lower()
    protocol_rank = PROTOCOL_RANK.get(protocol, 99)
    state_rank = STATE_RANK.get(_safe_str(entry.get("state"), "up").lower(), 99)
    next_hop = _safe_str(entry.get("next_hop"), "")
    request_topic = _safe_str(entry.get("request_topic"), "")

    return (
        mode_rank,
        -match_len,
        state_rank,
        metric,
        -priority,
        protocol_rank,
        next_hop,
        request_topic,
    )


def _route_matches_destination(entry: dict[str, Any], destination_node: str) -> bool:
    destination = _safe_str(destination_node, "").strip()
    if not destination:
        return False

    match_mode = _safe_str(entry.get("match_mode"), "default").lower()
    destination_prefix = _safe_str(entry.get("destination_prefix"), "").strip()

    if match_mode == "default":
        return True
    if match_mode == "exact":
        return destination == destination_prefix
    if match_mode == "prefix":
        if destination_prefix.endswith("*"):
            return destination.startswith(destination_prefix[:-1])
        return destination.startswith(destination_prefix)
    return False


def _normalize_route_entry(
    ctx: dict[str, Any],
    entry: dict[str, Any],
    *,
    owner_node: str,
    protocol: str,
    now_ts: float,
) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None

    destination_prefix = _safe_str(
        entry.get("destination_prefix") or entry.get("destination") or entry.get("prefix"),
        "",
    ).strip()
    if not destination_prefix:
        destination_prefix = _safe_str(owner_node, "").strip() or "default"

    match_mode = _derive_match_mode(destination_prefix, entry.get("match_mode"))
    next_hop = _safe_str(entry.get("next_hop") or owner_node, "").strip()
    request_topic = _safe_str(entry.get("request_topic"), "").strip()
    if not request_topic and next_hop:
        request_topic = make_mqtt_request_topic(ctx["topic_root"], next_hop)

    metric = max(_safe_int(entry.get("metric"), 0), 0)
    priority = _safe_int(entry.get("priority"), 100)
    state = _safe_str(entry.get("state"), "up").strip().lower() or "up"
    node_role = _normalize_role(entry.get("node_role") or entry.get("role") or "worker")
    capabilities = _normalize_capabilities(entry.get("capabilities") or {})
    node_tags = _normalize_tags(entry.get("node_tags") or entry.get("tags") or [])

    expires_at = entry.get("expires_at")
    if expires_at is None:
        ttl_s = max(
            _safe_float(
                entry.get("ttl_s"),
                ctx.get("registry_stale_after_s", REGISTRY_STALE_AFTER_S),
            ),
            1.0,
        )
        expires_at = now_ts + ttl_s

    normalized = {
        "route_id": _route_key(
            {
                "destination_prefix": destination_prefix,
                "match_mode": match_mode,
                "next_hop": next_hop,
                "protocol": protocol,
                "owner_node": owner_node,
                "interface": "mqtt",
            }
        ),
        "destination_prefix": destination_prefix,
        "match_mode": match_mode,
        "next_hop": next_hop,
        "request_topic": request_topic,
        "metric": metric,
        "priority": priority,
        "protocol": _safe_str(protocol, "dynamic").strip().lower() or "dynamic",
        "interface": "mqtt",
        "owner_node": _safe_str(owner_node, "").strip(),
        "node_role": node_role,
        "state": state,
        "last_seen": now_ts,
        "expires_at": _safe_float(expires_at, now_ts + REGISTRY_STALE_AFTER_S),
        "capabilities": capabilities,
        "node_tags": node_tags,
        "meta": _deepcopy_safe(entry.get("meta") or {}),
        "advertise": _safe_bool(entry.get("advertise"), True),
    }
    return normalized


def _make_self_route_entry(ctx: dict[str, Any], now_ts: float) -> dict[str, Any]:
    entry = {
        "destination_prefix": ctx["node_id"],
        "next_hop": ctx["node_id"],
        "request_topic": make_mqtt_request_topic(ctx["topic_root"], ctx["node_id"]),
        "metric": 0,
        "priority": 100,
        "state": "up",
        "node_role": ctx["interface_role"],
        "capabilities": _deepcopy_safe(ctx.get("capabilities") or {}),
        "node_tags": list(ctx.get("node_tags") or []),
        "meta": {"origin": "self"},
        "advertise": True,
        "ttl_s": max(_safe_float(ctx.get("registry_stale_after_s"), REGISTRY_STALE_AFTER_S), 1.0),
    }
    normalized = _normalize_route_entry(
        ctx,
        entry,
        owner_node=ctx["node_id"],
        protocol="self",
        now_ts=now_ts,
    )
    return normalized or {}


def _normalize_static_routes(ctx: dict[str, Any], static_routes: Any, now_ts: float) -> list[dict[str, Any]]:
    result = []
    iterable = static_routes if isinstance(static_routes, list) else []
    for item in iterable:
        if not isinstance(item, dict):
            continue
        owner_node = _safe_str(item.get("owner_node") or ctx["node_id"], ctx["node_id"])
        protocol = _safe_str(item.get("protocol"), "static") or "static"
        normalized = _normalize_route_entry(
            ctx,
            item,
            owner_node=owner_node,
            protocol=protocol,
            now_ts=now_ts,
        )
        if normalized is None:
            continue
        normalized["advertise"] = _safe_bool(item.get("advertise"), False)
        result.append(normalized)

    result.sort(key=partial(_route_order_key, destination_node=""))
    return result


def snapshot_mqtt_node_registry(runtime_ctx: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(runtime_ctx, dict):
        return {}

    tables = runtime_ctx.get("tables") if isinstance(runtime_ctx.get("tables"), dict) else runtime_ctx
    registry = tables.get("node_registry")
    registry_lock = tables.get("node_registry_lock")

    if not isinstance(registry, dict):
        return {}

    lock_obj = _ensure_lock(registry_lock)
    with lock_obj:
        snapshot = _deepcopy_safe(registry)

    if not isinstance(snapshot, dict):
        return {}

    ordered = {}
    for node_id in sorted(snapshot.keys()):
        ordered[node_id] = snapshot[node_id]
    return ordered


def snapshot_mqtt_route_table(runtime_ctx: dict[str, Any], grouped: bool = False) -> Any:
    if not isinstance(runtime_ctx, dict):
        return {} if grouped else []

    tables = runtime_ctx.get("tables") if isinstance(runtime_ctx.get("tables"), dict) else runtime_ctx
    route_table = tables.get("route_table")
    route_lock = tables.get("route_table_lock")

    if not isinstance(route_table, dict):
        return {} if grouped else []

    lock_obj = _ensure_lock(route_lock)
    with lock_obj:
        snapshot = _deepcopy_safe(route_table)

    if not isinstance(snapshot, dict):
        return {} if grouped else []

    entries = list(snapshot.values())
    entries.sort(key=partial(_route_order_key, destination_node=""))

    if not grouped:
        return entries

    grouped_entries = {}
    for entry in entries:
        prefix = _safe_str(entry.get("destination_prefix"), "default")
        grouped_entries.setdefault(prefix, []).append(entry)
    return grouped_entries


def resolve_mqtt_route(
    runtime_ctx: dict[str, Any],
    destination_node: str,
    allow_stale: bool = False,
) -> dict[str, Any] | None:
    destination = _safe_str(destination_node, "").strip()
    if not destination:
        return None

    entries = snapshot_mqtt_route_table(runtime_ctx, grouped=False)
    candidates = []
    stale_candidates = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if not _route_matches_destination(entry, destination):
            continue

        state = _safe_str(entry.get("state"), "up").lower()
        if state in ("up", "online"):
            candidates.append(entry)
        elif allow_stale and state == "stale":
            stale_candidates.append(entry)

    if not candidates and allow_stale:
        candidates = stale_candidates
    if not candidates:
        return None

    candidates.sort(key=partial(_route_order_key, destination_node=destination))
    return _deepcopy_safe(candidates[0])


def _upsert_route_entry(ctx: dict[str, Any], entry: dict[str, Any]) -> bool:
    route_table = ctx["tables"]["route_table"]
    route_lock = ctx["tables"]["route_table_lock"]
    route_id = _safe_str(entry.get("route_id"), "").strip() or _route_key(entry)
    normalized = dict(entry)
    normalized["route_id"] = route_id

    changed = False
    with route_lock:
        current = route_table.get(route_id)
        if current != normalized:
            route_table[route_id] = normalized
            changed = True

    if changed:
        _metrics_inc(ctx, "route_table_updates")
    return changed


def _replace_owner_routes(
    ctx: dict[str, Any],
    owner_node: str,
    entries: list[dict[str, Any]],
    *,
    protocols_to_replace: tuple[str, ...],
) -> bool:
    route_table = ctx["tables"]["route_table"]
    route_lock = ctx["tables"]["route_table_lock"]
    owner = _safe_str(owner_node, "").strip()
    if not owner:
        return False

    new_entries = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        route_id = _safe_str(entry.get("route_id"), "").strip() or _route_key(entry)
        normalized = dict(entry)
        normalized["route_id"] = route_id
        new_entries[route_id] = normalized

    changed = False

    with route_lock:
        keys_to_remove = []
        for route_id, current in list(route_table.items()):
            if not isinstance(current, dict):
                continue
            if _safe_str(current.get("owner_node"), "") != owner:
                continue
            protocol_name = _safe_str(current.get("protocol"), "dynamic").lower()
            if protocol_name not in protocols_to_replace:
                continue
            if route_id not in new_entries:
                keys_to_remove.append(route_id)

        for route_id in keys_to_remove:
            route_table.pop(route_id, None)
            changed = True

        for route_id, entry in new_entries.items():
            if route_table.get(route_id) != entry:
                route_table[route_id] = entry
                changed = True

    if changed:
        _metrics_inc(ctx, "route_table_updates")
    return changed


def _mark_owner_routes_state(ctx: dict[str, Any], owner_node: str, state: str) -> bool:
    route_table = ctx["tables"]["route_table"]
    route_lock = ctx["tables"]["route_table_lock"]
    owner = _safe_str(owner_node, "").strip()
    normalized_state = _safe_str(state, "stale").strip().lower() or "stale"
    changed = False

    with route_lock:
        for route_id, entry in list(route_table.items()):
            if not isinstance(entry, dict):
                continue
            if _safe_str(entry.get("owner_node"), "") != owner:
                continue
            protocol_name = _safe_str(entry.get("protocol"), "dynamic").lower()
            if protocol_name == "static":
                continue
            if _safe_str(entry.get("state"), "") != normalized_state:
                updated = dict(entry)
                updated["state"] = normalized_state
                updated["last_seen"] = _now_ts()
                route_table[route_id] = updated
                changed = True

    if changed:
        _metrics_inc(ctx, "route_table_updates")
    return changed


def _prune_expired_routes(ctx: dict[str, Any], now_ts: float) -> bool:
    route_table = ctx["tables"]["route_table"]
    route_lock = ctx["tables"]["route_table_lock"]
    prune_after_s = max(_safe_float(ctx.get("route_prune_after_s"), ROUTE_PRUNE_AFTER_S), 1.0)
    changed = False

    with route_lock:
        for route_id, entry in list(route_table.items()):
            if not isinstance(entry, dict):
                continue
            protocol_name = _safe_str(entry.get("protocol"), "dynamic").lower()
            if protocol_name == "static":
                continue
            expires_at = _safe_float(entry.get("expires_at"), now_ts + prune_after_s)
            if expires_at + prune_after_s < now_ts:
                route_table.pop(route_id, None)
                changed = True

    if changed:
        _metrics_inc(ctx, "route_table_updates")
    return changed


# =============================================================================
# Kontextaufbau
# =============================================================================

def _build_local_registry_entry(ctx: dict[str, Any], state: str, now_ts: float) -> dict[str, Any]:
    normalized_state = _safe_str(state, "offline").strip().lower() or "offline"
    heartbeat_interval_s = max(_safe_float(ctx.get("heartbeat_interval_s"), HEARTBEAT_INTERVAL_S), 1.0)
    stale_after_s = max(_safe_float(ctx.get("registry_stale_after_s"), REGISTRY_STALE_AFTER_S), 1.0)
    return {
        "node_id": ctx["node_id"],
        "node_role": ctx["interface_role"],
        "state": normalized_state,
        "transport": "mqtt",
        "timestamp": now_ts,
        "request_topic": make_mqtt_request_topic(ctx["topic_root"], ctx["node_id"]),
        "routes_topic": make_mqtt_node_routes_topic(ctx["topic_root"], ctx["node_id"]),
        "broker_host": ctx.get("broker_host"),
        "broker_port": ctx.get("broker_port"),
        "client_ip": _best_effort_local_ip(ctx["broker_host"], ctx["broker_port"]),
        "heartbeat_interval_s": heartbeat_interval_s,
        "capabilities": _deepcopy_safe(ctx.get("capabilities") or {}),
        "node_tags": list(ctx.get("node_tags") or []),
        "node_meta": _deepcopy_safe(ctx.get("node_meta") or {}),
        "last_seen": now_ts,
        "expires_at": now_ts + max(heartbeat_interval_s * 2.0, stale_after_s),
        "mqtt": {
            "topic": make_mqtt_node_status_topic(ctx["topic_root"], ctx["node_id"]),
            "qos": int(ctx["qos"]),
            "retain": True,
        },
    }


def _set_local_registry_state(ctx: dict[str, Any], state: str) -> bool:
    registry = ctx["tables"]["node_registry"]
    registry_lock = ctx["tables"]["node_registry_lock"]
    entry = _build_local_registry_entry(ctx, state, _now_ts())
    changed = False
    with registry_lock:
        if registry.get(ctx["node_id"]) != entry:
            registry[ctx["node_id"]] = entry
            changed = True
    if changed:
        _metrics_inc(ctx, "node_registry_updates")
    if state in ("offline", "down"):
        _mark_owner_routes_state(ctx, ctx["node_id"], "down")
    else:
        _upsert_route_entry(ctx, _make_self_route_entry(ctx, _now_ts()))
    return changed


def build_generic_mqtt_interface_ctx(
    *,
    node_id: Any,
    interface_role: Any = None,
    path_gate: Any = None,
    broker_host: Any = None,
    broker_port: Any = None,
    topic_root: Any = MQTT_DEFAULT_TOPIC_ROOT,
    queue_send: Any = None,
    queue_recv: Any = None,
    queue_ws: Any = None,
    queue_io: Any = None,
    queue_event_send: Any = None,
    resources: Any = None,
    registry_table: Any = None,
    registry_lock: Any = None,
    route_table: Any = None,
    route_lock: Any = None,
    runtime_metrics: Any = None,
    runtime_metrics_lock: Any = None,
    shutdown_event: Any = None,
    qos: Any = MQTT_DEFAULT_QOS,
    keepalive: Any = MQTT_DEFAULT_KEEPALIVE,
    ssl_enabled: bool = False,
    default_target_node: Any = None,
    recv_system_events: bool = False,
    node_meta: Any = None,
    node_tags: Any = None,
    capabilities: Any = None,
    registry_stale_after_s: Any = REGISTRY_STALE_AFTER_S,
    route_prune_after_s: Any = ROUTE_PRUNE_AFTER_S,
    heartbeat_interval_s: Any = HEARTBEAT_INTERVAL_S,
    route_advertise_interval_s: Any = ROUTE_ADVERTISE_INTERVAL_S,
    static_routes: Any = None,
    auto_probe_on_connect: bool = True,
    auto_select_single_peer: bool = True,
    loopback_local_delivery: bool = False,
    dedup_ttl_s: Any = DEDUP_TTL_S,
    dedup_max_entries: Any = DEDUP_MAX_ENTRIES,
    runtime_ctx_out: Any = None,
) -> dict[str, Any]:
    effective_role = _validate_interface_role(path_gate or interface_role or "worker")
    effective_node_id = _validate_node_id(node_id or "node-01")
    effective_queue_send = queue_send if queue_send is not None else queue_ws
    effective_queue_recv = queue_recv if queue_recv is not None else queue_io

    if effective_queue_send is None:
        raise ValueError("queue_send/queue_ws fehlt - Outbound-Queue ist erforderlich.")
    if effective_queue_recv is None:
        raise ValueError("queue_recv/queue_io fehlt - Inbound-Queue ist erforderlich.")

    effective_resources = resources if isinstance(resources, dict) else {}

    shared_registry, shared_registry_lock = _ensure_shared_resource(
        effective_resources,
        "mqtt_node_registry",
        registry_table,
        registry_lock,
    )
    shared_route_table, shared_route_lock = _ensure_shared_resource(
        effective_resources,
        "mqtt_route_table",
        route_table,
        route_lock,
    )
    shared_runtime_metrics, shared_runtime_metrics_lock = _ensure_shared_resource(
        effective_resources,
        "mqtt_runtime_metrics",
        runtime_metrics if isinstance(runtime_metrics, dict) else _create_runtime_metrics_state(),
        runtime_metrics_lock,
    )

    if not shared_runtime_metrics:
        shared_runtime_metrics.update(_create_runtime_metrics_state())

    effective_topic_root = _validate_topic_root(topic_root)
    effective_broker_host = _safe_str(broker_host, MQTT_DEFAULT_HOST).strip() or MQTT_DEFAULT_HOST
    effective_broker_port = _safe_int(
        broker_port,
        MQTT_DEFAULT_TLS_PORT if _safe_bool(ssl_enabled, False) else MQTT_DEFAULT_PORT,
    )
    effective_qos = max(0, min(_safe_int(qos, MQTT_DEFAULT_QOS), 2))
    effective_keepalive = max(_safe_int(keepalive, MQTT_DEFAULT_KEEPALIVE), 5)
    effective_shutdown_event = shutdown_event or threading.Event()

    ctx = {
        "node_id": effective_node_id,
        "interface_role": effective_role,
        "path_gate": effective_role,
        "broker_host": effective_broker_host,
        "broker_port": effective_broker_port,
        "topic_root": effective_topic_root,
        "qos": effective_qos,
        "keepalive": effective_keepalive,
        "ssl_enabled": _safe_bool(ssl_enabled, False),
        "default_target_node": _safe_str(default_target_node, "").strip(),
        "recv_system_events": _safe_bool(recv_system_events, False),
        "node_meta": _deepcopy_safe(node_meta if isinstance(node_meta, dict) else {}),
        "node_tags": _normalize_tags(node_tags),
        "capabilities": _normalize_capabilities(capabilities),
        "registry_stale_after_s": max(_safe_float(registry_stale_after_s, REGISTRY_STALE_AFTER_S), 1.0),
        "route_prune_after_s": max(_safe_float(route_prune_after_s, ROUTE_PRUNE_AFTER_S), 1.0),
        "heartbeat_interval_s": max(_safe_float(heartbeat_interval_s, HEARTBEAT_INTERVAL_S), 1.0),
        "route_advertise_interval_s": max(_safe_float(route_advertise_interval_s, ROUTE_ADVERTISE_INTERVAL_S), 1.0),
        "auto_probe_on_connect": _safe_bool(auto_probe_on_connect, True),
        "auto_select_single_peer": _safe_bool(auto_select_single_peer, True),
        "loopback_local_delivery": _safe_bool(loopback_local_delivery, False),
        "static_routes": [],
        "queues": {
            "send": effective_queue_send,
            "recv": effective_queue_recv,
            "event_send": queue_event_send,
            "incoming": queue_module.Queue(),
        },
        "tables": {
            "node_registry": shared_registry,
            "node_registry_lock": shared_registry_lock,
            "route_table": shared_route_table,
            "route_table_lock": shared_route_lock,
            "runtime_metrics": shared_runtime_metrics,
            "runtime_metrics_lock": shared_runtime_metrics_lock,
        },
        "resources": effective_resources,
        "shutdown_event": effective_shutdown_event,
        "runtime": {
            "stop_requested": threading.Event(),
            "connected_to_broker": threading.Event(),
            "subscriptions_ready": threading.Event(),
            "threads": [],
            "client_obj": None,
            "mqtt_lock": threading.RLock(),
            "last_status_publish": 0.0,
            "last_route_publish": 0.0,
            "last_probe_publish": 0.0,
            "last_housekeeping_at": 0.0,
            "had_successful_connect": False,
            "dedup_state": {
                "entries": {},
                "lock": threading.RLock(),
                "ttl_s": max(_safe_float(dedup_ttl_s, DEDUP_TTL_S), 1.0),
                "max_entries": max(_safe_int(dedup_max_entries, DEDUP_MAX_ENTRIES), 100),
            },
        },
    }

    now_ts = _now_ts()
    self_route = _make_self_route_entry(ctx, now_ts)
    if self_route:
        _upsert_route_entry(ctx, self_route)
    ctx["static_routes"] = _normalize_static_routes(ctx, static_routes, now_ts)
    for route_entry in ctx["static_routes"]:
        _upsert_route_entry(ctx, route_entry)

    _set_local_registry_state(ctx, "offline")
    _metrics_refresh_queue_depths(ctx)

    if isinstance(runtime_ctx_out, dict):
        runtime_ctx_out["ctx"] = ctx
        runtime_ctx_out["snapshot_node_registry"] = snapshot_mqtt_node_registry
        runtime_ctx_out["snapshot_route_table"] = snapshot_mqtt_route_table
        runtime_ctx_out["snapshot_runtime_metrics"] = snapshot_mqtt_runtime_metrics
        runtime_ctx_out["resolve_route"] = resolve_mqtt_route
        runtime_ctx_out["get_health"] = get_generic_mqtt_interface_health

    return ctx


# =============================================================================
# Registry- / Route-Advertisement
# =============================================================================

def _build_node_status_payload(ctx: dict[str, Any], state: str) -> dict[str, Any]:
    current_state = _safe_str(state, "online").strip().lower() or "online"
    return {
        "type": "system",
        "subtype": "node_status",
        "standard_version": MQTT_STANDARD_VERSION,
        "node_id": ctx["node_id"],
        "node_role": ctx["interface_role"],
        "state": current_state,
        "timestamp": _now_ts(),
        "transport": "mqtt",
        "broker_host": ctx.get("broker_host"),
        "broker_port": ctx.get("broker_port"),
        "request_topic": make_mqtt_request_topic(ctx["topic_root"], ctx["node_id"]),
        "routes_topic": make_mqtt_node_routes_topic(ctx["topic_root"], ctx["node_id"]),
        "heartbeat_interval_s": ctx.get("heartbeat_interval_s"),
        "capabilities": _deepcopy_safe(ctx.get("capabilities") or {}),
        "node_tags": list(ctx.get("node_tags") or []),
        "node_meta": _deepcopy_safe(ctx.get("node_meta") or {}),
        "client_ip": _best_effort_local_ip(ctx["broker_host"], ctx["broker_port"]),
    }


def _routes_for_advertisement(ctx: dict[str, Any]) -> list[dict[str, Any]]:
    advertised_entries = []
    for entry in snapshot_mqtt_route_table(ctx, grouped=False):
        if not isinstance(entry, dict):
            continue
        if _safe_str(entry.get("owner_node"), "") != ctx["node_id"]:
            continue
        if not _safe_bool(entry.get("advertise"), False):
            continue
        advertised_entries.append(
            {
                "destination_prefix": entry.get("destination_prefix"),
                "match_mode": entry.get("match_mode"),
                "next_hop": entry.get("next_hop"),
                "request_topic": entry.get("request_topic"),
                "metric": entry.get("metric"),
                "priority": entry.get("priority"),
                "protocol": entry.get("protocol"),
                "node_role": entry.get("node_role"),
                "state": entry.get("state"),
                "capabilities": _deepcopy_safe(entry.get("capabilities") or {}),
                "node_tags": list(entry.get("node_tags") or []),
                "meta": _deepcopy_safe(entry.get("meta") or {}),
                "advertise": True,
            }
        )

    advertised_entries.sort(key=partial(_route_order_key, destination_node=""))
    return advertised_entries


def _build_route_advertisement_payload(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "system",
        "subtype": "route_table",
        "standard_version": MQTT_STANDARD_VERSION,
        "node_id": ctx["node_id"],
        "node_role": ctx["interface_role"],
        "timestamp": _now_ts(),
        "transport": "mqtt",
        "routes": _routes_for_advertisement(ctx),
    }


def _publish_node_status(ctx: dict[str, Any], state: str, retain: bool = True) -> bool:
    topic = make_mqtt_node_status_topic(ctx["topic_root"], ctx["node_id"])
    ok = _client_publish(
        ctx,
        topic,
        _build_node_status_payload(ctx, state),
        qos=int(ctx["qos"]),
        retain=bool(retain),
    )
    if ok:
        ctx["runtime"]["last_status_publish"] = _now_ts()
        _metrics_inc(ctx, "status_publish_count")
    return ok


def _publish_route_table(ctx: dict[str, Any], retain: bool = True) -> bool:
    topic = make_mqtt_node_routes_topic(ctx["topic_root"], ctx["node_id"])
    ok = _client_publish(
        ctx,
        topic,
        _build_route_advertisement_payload(ctx),
        qos=int(ctx["qos"]),
        retain=bool(retain),
    )
    if ok:
        ctx["runtime"]["last_route_publish"] = _now_ts()
        _metrics_inc(ctx, "route_publish_count")
    return ok


def _probe_peers(ctx: dict[str, Any]) -> bool:
    topic = make_mqtt_probe_topic(ctx["topic_root"])
    payload = {
        "type": "system",
        "subtype": "discovery_probe",
        "standard_version": MQTT_STANDARD_VERSION,
        "node_id": ctx["node_id"],
        "node_role": ctx["interface_role"],
        "timestamp": _now_ts(),
    }
    ok = _client_publish(
        ctx,
        topic,
        payload,
        qos=int(ctx["qos"]),
        retain=False,
    )
    if ok:
        ctx["runtime"]["last_probe_publish"] = _now_ts()
        _metrics_inc(ctx, "probe_publish_count")
    return ok


# =============================================================================
# Registry- / Route-Learning
# =============================================================================

def _apply_node_status_payload(
    ctx: dict[str, Any],
    node_id: str,
    payload: dict[str, Any],
    *,
    topic: str,
    qos: int,
    retain: bool,
) -> bool:
    if not isinstance(payload, dict):
        return False

    normalized_node_id = _safe_str(node_id or payload.get("node_id"), "").strip()
    if not normalized_node_id:
        return False

    now_ts = _now_ts()
    node_role = _normalize_role(payload.get("node_role") or payload.get("role") or "worker")
    state = _safe_str(payload.get("state"), "online").strip().lower() or "online"

    if normalized_node_id == ctx["node_id"]:
        stopping = bool(ctx.get("runtime", {}).get("stop_requested", threading.Event()).is_set())
        connected = bool(ctx.get("runtime", {}).get("connected_to_broker", threading.Event()).is_set())
        if connected and not stopping and state in ("offline", "down", "stale"):
            state = "online"

    registry_entry = {
        "node_id": normalized_node_id,
        "node_role": node_role,
        "state": state,
        "transport": "mqtt",
        "timestamp": payload.get("timestamp") or now_ts,
        "request_topic": _safe_str(payload.get("request_topic"), "").strip() or make_mqtt_request_topic(ctx["topic_root"], normalized_node_id),
        "routes_topic": _safe_str(payload.get("routes_topic"), "").strip() or make_mqtt_node_routes_topic(ctx["topic_root"], normalized_node_id),
        "broker_host": payload.get("broker_host"),
        "broker_port": payload.get("broker_port"),
        "client_ip": payload.get("client_ip"),
        "heartbeat_interval_s": max(
            _safe_float(
                payload.get("heartbeat_interval_s"),
                ctx.get("heartbeat_interval_s", HEARTBEAT_INTERVAL_S),
            ),
            1.0,
        ),
        "capabilities": _normalize_capabilities(payload.get("capabilities") or {}),
        "node_tags": _normalize_tags(payload.get("node_tags") or payload.get("tags") or []),
        "node_meta": _deepcopy_safe(payload.get("node_meta") or payload.get("meta") or {}),
        "last_seen": now_ts,
        "expires_at": now_ts + max(
            _safe_float(payload.get("heartbeat_interval_s"), ctx.get("registry_stale_after_s", REGISTRY_STALE_AFTER_S)) * 2.0,
            ctx.get("registry_stale_after_s", REGISTRY_STALE_AFTER_S),
        ),
        "mqtt": {
            "topic": topic,
            "qos": int(qos),
            "retain": bool(retain),
        },
    }

    registry = ctx["tables"]["node_registry"]
    registry_lock = ctx["tables"]["node_registry_lock"]
    changed = False
    previous_state = ""
    existed = False

    with registry_lock:
        current = registry.get(normalized_node_id)
        existed = isinstance(current, dict)
        if existed:
            previous_state = _safe_str(current.get("state"), "")
        if current != registry_entry:
            registry[normalized_node_id] = registry_entry
            changed = True

    if changed:
        _metrics_inc(ctx, "node_registry_updates")

    if state in ("offline", "down"):
        _mark_owner_routes_state(ctx, normalized_node_id, "down")
    else:
        if normalized_node_id == ctx["node_id"]:
            _upsert_route_entry(ctx, _make_self_route_entry(ctx, now_ts))

    if changed:
        _emit_local_system_message(
            ctx,
            subtype="node_registry_update",
            payload={"node_id": normalized_node_id, "node": registry_entry},
            meta={"mqtt": {"topic": topic, "qos": int(qos), "retain": bool(retain)}},
        )

        if normalized_node_id != ctx["node_id"]:
            if not existed and state in ("online", "up"):
                _emit_bus_event(
                    ctx,
                    "NODE_REGISTERED",
                    {
                        "node_id": normalized_node_id,
                        "zone": registry_entry.get("node_meta", {}).get("zone"),
                        "capabilities": _deepcopy_safe(registry_entry.get("capabilities") or {}),
                        "priority": 100,
                        "timestamp": registry_entry.get("timestamp"),
                    },
                )
            elif state in ("online", "up"):
                _emit_bus_event(
                    ctx,
                    "NODE_HEARTBEAT",
                    {
                        "node_id": normalized_node_id,
                        "timestamp": registry_entry.get("timestamp"),
                        "role": node_role,
                    },
                )
            elif previous_state != state:
                _emit_bus_event(
                    ctx,
                    "STATUS_UPDATE",
                    {
                        "service_name": "mqtt_node_registry",
                        "status": state,
                        "details": {"node_id": normalized_node_id, "role": node_role},
                    },
                )

    return changed


def _apply_route_advertisement(
    ctx: dict[str, Any],
    node_id: str,
    payload: dict[str, Any],
    *,
    topic: str,
    qos: int,
    retain: bool,
) -> bool:
    if not isinstance(payload, dict):
        return False

    owner_node = _safe_str(node_id or payload.get("node_id"), "").strip()
    if not owner_node:
        return False

    now_ts = _now_ts()
    raw_routes = payload.get("routes")
    advertised_role = _normalize_role(payload.get("node_role") or payload.get("role") or "worker")
    advertised_tags = _normalize_tags(payload.get("node_tags") or payload.get("tags") or [])
    advertised_capabilities = _normalize_capabilities(payload.get("capabilities") or {})

    normalized_routes = []
    if isinstance(raw_routes, list):
        for item in raw_routes:
            if not isinstance(item, dict):
                continue
            route_input = dict(item)
            if not route_input.get("node_role") and not route_input.get("role"):
                route_input["node_role"] = advertised_role
            if not route_input.get("node_tags") and not route_input.get("tags") and advertised_tags:
                route_input["node_tags"] = list(advertised_tags)
            if not route_input.get("capabilities") and advertised_capabilities:
                route_input["capabilities"] = _deepcopy_safe(advertised_capabilities)

            normalized = _normalize_route_entry(
                ctx,
                route_input,
                owner_node=owner_node,
                protocol=_safe_str(route_input.get("protocol"), "dynamic") or "dynamic",
                now_ts=now_ts,
            )
            if normalized is None:
                continue
            normalized["state"] = _safe_str(route_input.get("state"), normalized.get("state", "up")).strip().lower() or "up"
            normalized["meta"] = _deepcopy_safe(route_input.get("meta") or {})
            normalized_routes.append(normalized)

    changed = _replace_owner_routes(
        ctx,
        owner_node,
        normalized_routes,
        protocols_to_replace=("dynamic", "self", "fallback"),
    )

    if changed:
        _emit_local_system_message(
            ctx,
            subtype="route_table_update",
            payload={"node_id": owner_node, "routes": _deepcopy_safe(normalized_routes)},
            meta={"mqtt": {"topic": topic, "qos": int(qos), "retain": bool(retain)}},
        )
    return changed


# =============================================================================
# Zielauflösung / Routing
# =============================================================================

def _registry_matches_filters(
    registry_entry: dict[str, Any],
    *,
    target_roles: list[str],
    target_tags: list[str],
    target_capabilities: list[str],
) -> bool:
    if not isinstance(registry_entry, dict):
        return False

    state = _safe_str(registry_entry.get("state"), "offline").lower()
    if state not in ("online", "up"):
        return False

    node_role = _normalize_role(registry_entry.get("node_role") or registry_entry.get("role") or "worker")
    if target_roles and node_role not in target_roles:
        return False

    node_tags = set(_normalize_tags(registry_entry.get("node_tags") or []))
    for tag in target_tags:
        if tag not in node_tags:
            return False

    capabilities = registry_entry.get("capabilities")
    if not isinstance(capabilities, dict):
        capabilities = {}
    for capability in target_capabilities:
        if capability not in capabilities:
            return False

    return True


def _select_target_nodes(ctx: dict[str, Any], envelope: dict[str, Any]) -> list[str]:
    targets = []
    seen = set()

    def _add_target(node_text: Any) -> None:
        target = _safe_str(node_text, "").strip()
        if not target:
            return
        if target in seen:
            return
        seen.add(target)
        targets.append(target)

    explicit_target = _safe_str(envelope.get("target_node"), "").strip()
    if explicit_target:
        _add_target(explicit_target)

    for item in _normalize_collection(envelope.get("target_nodes")):
        _add_target(item)

    destination = _safe_str(envelope.get("destination"), "").strip()
    if destination.startswith("node:"):
        _add_target(destination.split(":", 1)[1].strip())

    target_roles = [_normalize_role(item) for item in _normalize_collection(envelope.get("target_roles") or envelope.get("target_role"))]
    target_tags = _normalize_tags(envelope.get("target_tags") or envelope.get("target_tag"))
    target_capabilities = _normalize_tags(envelope.get("target_capabilities") or envelope.get("target_capability"))

    if target_roles or target_tags or target_capabilities or _safe_bool(envelope.get("broadcast"), False):
        registry_snapshot = snapshot_mqtt_node_registry(ctx)
        for node_id in sorted(registry_snapshot.keys()):
            entry = registry_snapshot[node_id]
            if not isinstance(entry, dict):
                continue
            if _safe_bool(envelope.get("broadcast"), False):
                if _safe_str(entry.get("state"), "offline").lower() not in ("online", "up"):
                    continue
            if not _registry_matches_filters(
                entry,
                target_roles=target_roles,
                target_tags=target_tags,
                target_capabilities=target_capabilities,
            ):
                if target_roles or target_tags or target_capabilities:
                    continue
                if not _safe_bool(envelope.get("broadcast"), False):
                    continue
            _add_target(node_id)

    if not targets:
        default_target = _safe_str(ctx.get("default_target_node"), "").strip()
        if default_target:
            _add_target(default_target)

    if not targets and _safe_bool(ctx.get("auto_select_single_peer"), True):
        registry_snapshot = snapshot_mqtt_node_registry(ctx)
        online_peers = []
        for node_id, entry in registry_snapshot.items():
            if not isinstance(entry, dict):
                continue
            if node_id == ctx["node_id"]:
                continue
            if _safe_str(entry.get("state"), "offline").lower() not in ("online", "up"):
                continue
            online_peers.append(node_id)
        online_peers.sort()
        if len(online_peers) == 1:
            _add_target(online_peers[0])

    exclude_nodes = set(_normalize_collection(envelope.get("exclude_nodes")))
    broadcast_include_self = _safe_bool(envelope.get("broadcast_include_self"), False)

    filtered = []
    for node_id in targets:
        if node_id in exclude_nodes:
            continue
        if node_id == ctx["node_id"] and not broadcast_include_self and _safe_bool(envelope.get("broadcast"), False):
            continue
        filtered.append(node_id)

    filtered.sort()
    return filtered


def _resolve_route_for_target(ctx: dict[str, Any], target_node: str) -> dict[str, Any] | None:
    route = resolve_mqtt_route(ctx, target_node, allow_stale=False)
    if route is not None:
        return route

    request_topic = make_mqtt_request_topic(ctx["topic_root"], target_node)
    return {
        "route_id": "fallback|%s" % target_node,
        "destination_prefix": target_node,
        "match_mode": "exact",
        "next_hop": target_node,
        "request_topic": request_topic,
        "metric": 999,
        "priority": -1,
        "protocol": "fallback",
        "interface": "mqtt",
        "owner_node": target_node,
        "node_role": "worker",
        "state": "up",
        "last_seen": _now_ts(),
        "expires_at": _now_ts() + ctx.get("registry_stale_after_s", REGISTRY_STALE_AFTER_S),
        "capabilities": {},
        "node_tags": [],
        "meta": {"origin": "implicit_direct_request_topic"},
        "advertise": False,
    }


# =============================================================================
# Envelope-Normalisierung
# =============================================================================

def _ensure_outbound_envelope(ctx: dict[str, Any], msg: dict[str, Any]) -> dict[str, Any]:
    payload = msg.get("payload")
    if payload is None:
        reserved = {
            "type",
            "target_node",
            "target_nodes",
            "target_role",
            "target_roles",
            "target_tag",
            "target_tags",
            "target_capability",
            "target_capabilities",
            "route_hint",
            "broadcast",
            "broadcast_include_self",
            "exclude_nodes",
            "api_route",
            "route",
            "command",
            "message_type",
            "source",
            "source_node",
            "destination",
            "meta",
        }
        payload = {}
        for key, value in msg.items():
            if key in reserved:
                continue
            payload[key] = value

    source_value = _safe_str(msg.get("source"), "").strip() or ctx["interface_role"]
    envelope = {
        "type": msg.get("type") or "event",
        "message_id": msg.get("message_id") or msg.get("msg_id") or _make_id(),
        "timestamp": msg.get("timestamp") or _now_ts(),
        "source": source_value,
        "source_node": msg.get("source_node") or ctx["node_id"],
        "destination": msg.get("destination") or "",
        "api_route": (
            msg.get("api_route")
            or msg.get("route")
            or msg.get("command")
            or msg.get("message_type")
            or "CLIENT_COMMAND"
        ),
        "payload": payload or {},
        "meta": _deepcopy_safe(msg.get("meta") or {}),
        "client_id": msg.get("client_id"),
        "target_node": _safe_str(msg.get("target_node"), "").strip(),
    }

    for key in MQTT_ROUTING_KEYS:
        if key in msg:
            envelope[key] = _deepcopy_safe(msg[key])

    meta = envelope.get("meta") if isinstance(envelope.get("meta"), dict) else {}
    mqtt_meta = meta.get("mqtt") if isinstance(meta.get("mqtt"), dict) else {}
    mqtt_meta["standard_version"] = MQTT_STANDARD_VERSION
    meta["mqtt"] = mqtt_meta
    envelope["meta"] = meta
    return envelope


def _normalize_inbound_application_message(
    ctx: dict[str, Any],
    data: dict[str, Any],
    *,
    topic: str,
    qos: int,
    retain: bool,
) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None

    payload = data.get("payload")
    if payload is None:
        reserved = {
            "type",
            "target_node",
            "target_nodes",
            "api_route",
            "route",
            "command",
            "message_type",
            "meta",
            "destination",
            "broadcast",
            "source",
            "source_node",
        }
        payload = {}
        for key, value in data.items():
            if key in reserved:
                continue
            payload[key] = value

    source_node = _safe_str(data.get("source_node"), "").strip()
    if not source_node:
        source_node = _safe_str(data.get("source") or data.get("origin_node"), "").strip()
        if source_node.startswith("node:"):
            source_node = source_node.split(":", 1)[1].strip()

    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    meta = _deepcopy_safe(meta)
    mqtt_meta = meta.get("mqtt") if isinstance(meta.get("mqtt"), dict) else {}
    mqtt_meta["topic"] = topic
    mqtt_meta["qos"] = int(qos)
    mqtt_meta["retain"] = bool(retain)
    mqtt_meta["standard_version"] = MQTT_STANDARD_VERSION
    meta["mqtt"] = mqtt_meta

    target_node = _safe_str(data.get("target_node"), "").strip() or ctx["node_id"]

    envelope = {
        "type": data.get("type") or "event",
        "message_id": data.get("message_id") or data.get("id") or _make_id(),
        "timestamp": data.get("timestamp") or _now_ts(),
        "source": data.get("source") or "mqtt",
        "source_node": source_node,
        "destination": data.get("destination") or ("node:%s" % target_node if target_node else ""),
        "api_route": (
            data.get("api_route")
            or data.get("route")
            or data.get("command")
            or data.get("message_type")
            or "CLIENT_COMMAND"
        ),
        "payload": payload or {},
        "meta": meta,
        "client_id": data.get("client_id"),
        "target_node": target_node,
    }
    return envelope


# =============================================================================
# MQTT Callbacks
# =============================================================================

def _mqtt_on_connect(
    client_obj: Any,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any = None,
    *extra: Any,
) -> None:
    ctx = userdata if isinstance(userdata, dict) else {}

    logger.info(
        "MQTT-Interface verbunden: broker=%s:%s node_id=%s role=%s reason=%s",
        ctx.get("broker_host"),
        ctx.get("broker_port"),
        ctx.get("node_id"),
        ctx.get("interface_role"),
        _reason_code_text(reason_code),
    )

    if bool(ctx.get("runtime", {}).get("had_successful_connect")):
        _metrics_inc(ctx, "reconnects")
    ctx["runtime"]["had_successful_connect"] = True

    ctx["runtime"]["connected_to_broker"].set()
    ctx["runtime"]["subscriptions_ready"].clear()
    _metrics_inc(ctx, "connect_success")
    _metrics_mark_now(ctx, "last_connect_ts")
    _set_local_registry_state(ctx, "online")

    subscriptions = [
        (make_mqtt_request_topic(ctx["topic_root"], ctx["node_id"]), int(ctx["qos"])),
        (make_mqtt_probe_topic(ctx["topic_root"]), int(ctx["qos"])),
        (make_mqtt_node_status_wildcard(ctx["topic_root"]), int(ctx["qos"])),
        (make_mqtt_node_routes_wildcard(ctx["topic_root"]), int(ctx["qos"])),
    ]

    if _client_subscribe_many(ctx, subscriptions):
        ctx["runtime"]["subscriptions_ready"].set()
    else:
        logger.warning("MQTT-Interface: Subscribe-Setup teilweise fehlgeschlagen")

    _publish_node_status(ctx, "online", retain=True)
    _publish_route_table(ctx, retain=True)

    if _safe_bool(ctx.get("auto_probe_on_connect"), True):
        _probe_peers(ctx)

    _emit_local_system_message(
        ctx,
        subtype="connected",
        payload={
            "node_id": ctx.get("node_id"),
            "role": ctx.get("interface_role"),
            "broker_host": ctx.get("broker_host"),
            "broker_port": ctx.get("broker_port"),
        },
    )
    _emit_bus_event(
        ctx,
        "STATUS_UPDATE",
        {
            "service_name": "mqtt_interface",
            "status": "connected",
            "details": {
                "node_id": ctx.get("node_id"),
                "role": ctx.get("interface_role"),
                "broker_host": ctx.get("broker_host"),
                "broker_port": ctx.get("broker_port"),
            },
        },
    )
    _metrics_refresh_queue_depths(ctx)


def _mqtt_on_connect_fail(client_obj: Any, userdata: Any) -> None:
    ctx = userdata if isinstance(userdata, dict) else {}
    logger.warning(
        "MQTT-Interface Verbindungsaufbau fehlgeschlagen: broker=%s:%s node_id=%s",
        ctx.get("broker_host"),
        ctx.get("broker_port"),
        ctx.get("node_id"),
    )
    _metrics_inc(ctx, "connect_failures")
    _emit_local_system_message(
        ctx,
        subtype="connect_failed",
        payload={
            "node_id": ctx.get("node_id"),
            "broker_host": ctx.get("broker_host"),
            "broker_port": ctx.get("broker_port"),
        },
    )
    _emit_bus_event(
        ctx,
        "ERROR_OCCURRED",
        {
            "error_code": "MQTT_CONNECT_FAILED",
            "message": "MQTT-Verbindungsaufbau fehlgeschlagen",
            "context": {
                "node_id": ctx.get("node_id"),
                "broker_host": ctx.get("broker_host"),
                "broker_port": ctx.get("broker_port"),
            },
            "service_name": "mqtt_interface",
        },
    )


def _mqtt_on_disconnect(
    client_obj: Any,
    userdata: Any,
    disconnect_flags_or_rc: Any,
    reason_code: Any = None,
    properties: Any = None,
    *extra: Any,
) -> None:
    ctx = userdata if isinstance(userdata, dict) else {}
    if reason_code is None:
        reason_code = disconnect_flags_or_rc

    ctx["runtime"]["connected_to_broker"].clear()
    ctx["runtime"]["subscriptions_ready"].clear()
    _metrics_inc(ctx, "disconnects")
    _metrics_mark_now(ctx, "last_disconnect_ts")
    _set_local_registry_state(ctx, "offline")

    logger.info(
        "MQTT-Interface getrennt: broker=%s:%s node_id=%s reason=%s",
        ctx.get("broker_host"),
        ctx.get("broker_port"),
        ctx.get("node_id"),
        _reason_code_text(reason_code),
    )

    _emit_local_system_message(
        ctx,
        subtype="disconnected",
        payload={
            "node_id": ctx.get("node_id"),
            "reason": _reason_code_text(reason_code),
        },
    )
    _emit_bus_event(
        ctx,
        "STATUS_UPDATE",
        {
            "service_name": "mqtt_interface",
            "status": "disconnected",
            "details": {
                "node_id": ctx.get("node_id"),
                "reason": _reason_code_text(reason_code),
            },
        },
    )
    _metrics_refresh_queue_depths(ctx)


def _mqtt_on_message(client_obj: Any, userdata: Any, message: Any) -> None:
    ctx = userdata if isinstance(userdata, dict) else {}
    topic = _safe_str(getattr(message, "topic", ""), "")
    payload = getattr(message, "payload", None)
    qos = _safe_int(getattr(message, "qos", 0), 0)
    retain = _safe_bool(getattr(message, "retain", False), False)

    _metrics_inc(ctx, "received_messages")
    _metrics_mark_now(ctx, "last_message_ts")

    try:
        if topic == make_mqtt_probe_topic(ctx["topic_root"]):
            _metrics_inc(ctx, "received_discovery_messages")
            _publish_node_status(ctx, "online", retain=True)
            _publish_route_table(ctx, retain=True)
            return

        if topic.startswith("%s/discovery/nodes/" % _normalize_topic_root(ctx["topic_root"])):
            _metrics_inc(ctx, "received_discovery_messages")

            node_id_from_status = _extract_node_id_from_status_topic(ctx["topic_root"], topic)
            if node_id_from_status:
                data = _safe_json_loads(payload)
                if isinstance(data, dict):
                    _apply_node_status_payload(
                        ctx,
                        node_id_from_status,
                        data,
                        topic=topic,
                        qos=qos,
                        retain=retain,
                    )
                return

            node_id_from_routes = _extract_node_id_from_routes_topic(ctx["topic_root"], topic)
            if node_id_from_routes:
                data = _safe_json_loads(payload)
                if isinstance(data, dict):
                    _apply_route_advertisement(
                        ctx,
                        node_id_from_routes,
                        data,
                        topic=topic,
                        qos=qos,
                        retain=retain,
                    )
                return

        if topic == make_mqtt_request_topic(ctx["topic_root"], ctx["node_id"]):
            _metrics_inc(ctx, "received_request_messages")
            data = _safe_json_loads(payload)
            if not isinstance(data, dict):
                logger.warning("MQTT-Interface ignoriert unlesbare Request-Nachricht: topic=%s", topic)
                return

            normalized = _normalize_inbound_application_message(
                ctx,
                data,
                topic=topic,
                qos=qos,
                retain=retain,
            )
            if normalized is None:
                return

            if _dedup_register_message(ctx, "inbound", normalized):
                _metrics_inc(ctx, "inbound_duplicates_dropped")
                return

            _put_to_named_queue(
                ctx,
                "incoming",
                normalized,
                QUEUE_POLL_TIMEOUT_S,
                ok_metric=None,
                fail_metric="inbound_queue_drops",
                log_label="incoming_from_mqtt",
            )
            return
    except Exception as exc:
        logger.exception("MQTT-Interface on_message Fehler: %s", exc)


# =============================================================================
# Transport / Publish
# =============================================================================

def _publish_outbound_to_target(
    ctx: dict[str, Any],
    envelope: dict[str, Any],
    target_node: str,
) -> bool:
    resolved_route = _resolve_route_for_target(ctx, target_node)
    if resolved_route is None:
        logger.warning("Keine MQTT-Route fuer target_node=%s", target_node)
        _metrics_inc(ctx, "route_resolution_failures")
        _emit_local_system_message(
            ctx,
            subtype="route_resolution_error",
            payload={
                "target_node": target_node,
                "message_id": envelope.get("message_id"),
            },
        )
        _emit_bus_event(
            ctx,
            "ERROR_OCCURRED",
            {
                "error_code": "MQTT_ROUTE_NOT_FOUND",
                "message": "Keine Route fuer Zielnode gefunden",
                "context": {
                    "target_node": target_node,
                    "message_id": envelope.get("message_id"),
                },
                "service_name": "mqtt_interface",
            },
        )
        return False

    request_topic = _safe_str(resolved_route.get("request_topic"), "").strip()
    if not request_topic:
        request_topic = make_mqtt_request_topic(ctx["topic_root"], target_node)

    outbound = _deepcopy_safe(envelope)
    outbound["target_node"] = target_node
    outbound["destination"] = outbound.get("destination") or ("node:%s" % target_node)
    outbound["source_node"] = outbound.get("source_node") or ctx["node_id"]

    meta = outbound.get("meta") if isinstance(outbound.get("meta"), dict) else {}
    mqtt_meta = meta.get("mqtt") if isinstance(meta.get("mqtt"), dict) else {}
    mqtt_meta["route"] = {
        "target_node": target_node,
        "next_hop": resolved_route.get("next_hop"),
        "protocol": resolved_route.get("protocol"),
        "metric": resolved_route.get("metric"),
        "request_topic": request_topic,
    }
    mqtt_meta["standard_version"] = MQTT_STANDARD_VERSION
    meta["mqtt"] = mqtt_meta
    outbound["meta"] = meta

    if target_node == ctx["node_id"] and _safe_bool(ctx.get("loopback_local_delivery"), False):
        normalized = _normalize_inbound_application_message(
            ctx,
            outbound,
            topic=request_topic,
            qos=int(ctx["qos"]),
            retain=False,
        )
        if normalized is None:
            return False
        return _put_to_named_queue(
            ctx,
            "incoming",
            normalized,
            QUEUE_POLL_TIMEOUT_S,
            ok_metric=None,
            fail_metric="inbound_queue_drops",
            log_label="loopback_local_delivery",
        )

    ok = _client_publish(
        ctx,
        request_topic,
        outbound,
        qos=int(ctx["qos"]),
        retain=False,
    )
    if not ok:
        _emit_local_system_message(
            ctx,
            subtype="publish_error",
            payload={
                "target_node": target_node,
                "message_id": outbound.get("message_id"),
                "request_topic": request_topic,
            },
        )
    return ok


# =============================================================================
# Interne Worker-Loops
# =============================================================================

def _outbound_publish_loop(ctx: dict[str, Any]) -> None:
    logger.info("MQTT-Interface Outbound-Thread gestartet")

    while not ctx["runtime"]["stop_requested"].is_set():
        if ctx["shutdown_event"] is not None and ctx["shutdown_event"].is_set():
            break

        raw = _queue_get(ctx["queues"]["send"], QUEUE_POLL_TIMEOUT_S)
        if raw is None:
            continue

        if _is_stop_message(raw):
            logger.info("MQTT-Interface Outbound-Thread Stop-Sentinel empfangen")
            break

        message = raw if isinstance(raw, dict) else _safe_json_loads(raw)
        if not isinstance(message, dict):
            logger.warning(
                "MQTT-Interface ignoriert unlesbare Outbound-Nachricht: %s",
                _safe_str(raw, "")[:200],
            )
            continue

        envelope = _ensure_outbound_envelope(ctx, message)
        target_nodes = _select_target_nodes(ctx, envelope)
        if not target_nodes:
            logger.warning(
                "MQTT-Interface konnte kein Ziel aufloesen: message_id=%s api_route=%s",
                envelope.get("message_id"),
                envelope.get("api_route"),
            )
            _metrics_inc(ctx, "route_resolution_failures")
            _emit_local_system_message(
                ctx,
                subtype="route_resolution_error",
                payload={
                    "message_id": envelope.get("message_id"),
                    "api_route": envelope.get("api_route"),
                },
            )
            continue

        for target_node in target_nodes:
            _publish_outbound_to_target(ctx, envelope, target_node)

    logger.info("MQTT-Interface Outbound-Thread beendet")


def _inbound_forward_loop(ctx: dict[str, Any]) -> None:
    logger.info("MQTT-Interface Inbound-Forward-Thread gestartet")

    while not ctx["runtime"]["stop_requested"].is_set():
        if ctx["shutdown_event"] is not None and ctx["shutdown_event"].is_set():
            break

        raw = _queue_get(ctx["queues"]["incoming"], QUEUE_POLL_TIMEOUT_S)
        if raw is None:
            continue

        if _is_stop_message(raw):
            logger.info("MQTT-Interface Inbound-Forward-Thread Stop-Sentinel empfangen")
            break

        message = raw if isinstance(raw, dict) else _safe_json_loads(raw)
        if not isinstance(message, dict):
            logger.warning(
                "MQTT-Interface Inbound-Forward ignoriert unlesbare Nachricht: %s",
                _safe_str(raw, "")[:200],
            )
            continue

        _put_to_named_queue(
            ctx,
            "recv",
            message,
            QUEUE_POLL_TIMEOUT_S,
            ok_metric="inbound_queue_forwarded",
            fail_metric="inbound_queue_drops",
            log_label="inbound_forward",
        )

    logger.info("MQTT-Interface Inbound-Forward-Thread beendet")


def _announce_loop(ctx: dict[str, Any]) -> None:
    logger.info("MQTT-Interface Announce-Thread gestartet")
    heartbeat_interval_s = max(_safe_float(ctx.get("heartbeat_interval_s"), HEARTBEAT_INTERVAL_S), 1.0)
    route_interval_s = max(_safe_float(ctx.get("route_advertise_interval_s"), ROUTE_ADVERTISE_INTERVAL_S), 1.0)

    while not ctx["runtime"]["stop_requested"].is_set():
        if ctx["shutdown_event"] is not None and ctx["shutdown_event"].is_set():
            break

        if ctx["runtime"]["connected_to_broker"].is_set():
            now_ts = _now_ts()
            if now_ts - _safe_float(ctx["runtime"].get("last_status_publish"), 0.0) >= heartbeat_interval_s:
                _publish_node_status(ctx, "online", retain=True)
            if now_ts - _safe_float(ctx["runtime"].get("last_route_publish"), 0.0) >= route_interval_s:
                _publish_route_table(ctx, retain=True)

        time.sleep(min(heartbeat_interval_s, route_interval_s, 1.0))

    logger.info("MQTT-Interface Announce-Thread beendet")


def _housekeeping_loop(ctx: dict[str, Any]) -> None:
    logger.info("MQTT-Interface Housekeeping-Thread gestartet")
    stale_after_s = max(_safe_float(ctx.get("registry_stale_after_s"), REGISTRY_STALE_AFTER_S), 1.0)

    while not ctx["runtime"]["stop_requested"].is_set():
        if ctx["shutdown_event"] is not None and ctx["shutdown_event"].is_set():
            break

        now_ts = _now_ts()
        registry = ctx["tables"]["node_registry"]
        registry_lock = ctx["tables"]["node_registry_lock"]
        registry_changed = False

        with registry_lock:
            for node_id, entry in list(registry.items()):
                if not isinstance(entry, dict):
                    continue
                if node_id == ctx["node_id"]:
                    continue

                last_seen = _safe_float(entry.get("last_seen"), 0.0)
                expires_at = _safe_float(entry.get("expires_at"), last_seen + stale_after_s)
                state = _safe_str(entry.get("state"), "offline").lower()

                next_state = state
                if state not in ("offline", "down"):
                    if last_seen + stale_after_s < now_ts:
                        next_state = "stale"
                    if expires_at + stale_after_s < now_ts:
                        next_state = "down"

                if next_state != state:
                    updated = dict(entry)
                    updated["state"] = next_state
                    registry[node_id] = updated
                    registry_changed = True

        if registry_changed:
            _metrics_inc(ctx, "node_registry_updates")
            registry_snapshot = snapshot_mqtt_node_registry(ctx)
            for node_id, entry in registry_snapshot.items():
                if not isinstance(entry, dict):
                    continue
                if node_id == ctx["node_id"]:
                    continue
                state = _safe_str(entry.get("state"), "offline")
                if state == "stale":
                    _mark_owner_routes_state(ctx, node_id, "stale")
                elif state in ("down", "offline"):
                    _mark_owner_routes_state(ctx, node_id, "down")
                else:
                    continue
                _emit_bus_event(
                    ctx,
                    "STATUS_UPDATE",
                    {
                        "service_name": "mqtt_node_registry",
                        "status": state,
                        "details": {"node_id": node_id, "role": entry.get("node_role")},
                    },
                )

        _prune_expired_routes(ctx, now_ts)
        _dedup_housekeeping(ctx)
        _metrics_inc(ctx, "housekeeping_runs")
        _metrics_mark_now(ctx, "last_housekeeping_ts")
        _metrics_refresh_queue_depths(ctx)
        ctx["runtime"]["last_housekeeping_at"] = now_ts
        time.sleep(1.0)

    logger.info("MQTT-Interface Housekeeping-Thread beendet")


# =============================================================================
# Thread- und Laufzeitsteuerung
# =============================================================================

def _start_thread(ctx: dict[str, Any], name: str, target_func: Any) -> Any:
    thread_obj = threading.Thread(
        target=partial(target_func, ctx),
        name=name,
        daemon=False,
    )
    thread_obj.start()
    ctx["runtime"]["threads"].append(thread_obj)
    return thread_obj


def _wait_for_connection(ctx: dict[str, Any], timeout_s: float) -> bool:
    return ctx["runtime"]["connected_to_broker"].wait(max(float(timeout_s), 0.01))


def _join_runtime_threads(ctx: dict[str, Any], join_timeout_s: float) -> None:
    for thread_obj in list(ctx["runtime"].get("threads") or []):
        if not thread_obj.is_alive():
            continue
        if thread_obj is threading.current_thread():
            continue
        try:
            thread_obj.join(max(float(join_timeout_s), 0.01))
        except Exception:
            pass


# =============================================================================
# Hauptlauf
# =============================================================================

def _mqtt_shutdown_aware_wait(ctx: Any, total_wait_s: float) -> bool:
    """
    Wartet bis zu `total_wait_s` Sekunden, pollt dabei alle 500ms sowohl
    runtime.stop_requested als auch ctx['shutdown_event'] (falls vorhanden).

    Returns:
        True  -> Shutdown-Signal empfangen, Caller sollte sich beenden.
        False -> Wartezeit vollstaendig abgelaufen, normaler Weiterlauf.
    """
    poll_interval = 0.5
    waited = 0.0
    shutdown_event = ctx.get("shutdown_event")
    stop_requested = ctx["runtime"]["stop_requested"]

    while waited < total_wait_s:
        if stop_requested.is_set():
            return True
        if shutdown_event is not None and shutdown_event.is_set():
            return True
        sleep_for = min(poll_interval, total_wait_s - waited)
        if sleep_for <= 0:
            break
        time.sleep(sleep_for)
        waited += sleep_for

    # Final check nach Ablauf der Wartezeit
    if stop_requested.is_set():
        return True
    if shutdown_event is not None and shutdown_event.is_set():
        return True
    return False


def generic_mqtt_interface_thread(
    *,
    node_id: Any = None,
    interface_role: Any = None,
    path_gate: Any = None,
    broker_host: Any = None,
    broker_port: Any = None,
    host: Any = None,
    port: Any = None,
    mqtt_client_id: Any = None,
    queue_send: Any = None,
    queue_recv: Any = None,
    queue_ws: Any = None,
    queue_io: Any = None,
    queue_event_send: Any = None,
    resources: Any = None,
    registry_table: Any = None,
    registry_lock: Any = None,
    route_table: Any = None,
    route_lock: Any = None,
    runtime_metrics: Any = None,
    runtime_metrics_lock: Any = None,
    topic_root: Any = MQTT_DEFAULT_TOPIC_ROOT,
    username: Any = None,
    password: Any = None,
    keepalive: Any = MQTT_DEFAULT_KEEPALIVE,
    qos: Any = MQTT_DEFAULT_QOS,
    protocol_version: Any = MQTT_DEFAULT_PROTOCOL,
    clean_session: bool = True,
    reconnect_on_failure: bool = True,
    reconnect_min_delay: Any = RECONNECT_MIN_DELAY_S,
    reconnect_max_delay: Any = RECONNECT_MAX_DELAY_S,
    transport: str = MQTT_DEFAULT_TRANSPORT,
    ssl_enabled: bool = False,
    ssl_cafile: Any = None,
    ssl_capath: Any = None,
    ssl_cadata: Any = None,
    ssl_certfile: Any = None,
    ssl_keyfile: Any = None,
    ssl_password: Any = None,
    ssl_minimum_tls_version: Any = "TLSv1_2",
    ssl_ciphers: Any = None,
    ssl_allow_unverified: bool = False,
    ssl_auto_allow_unverified_if_no_ca: bool = False,
    default_target_node: Any = None,
    recv_system_events: bool = False,
    node_meta: Any = None,
    node_tags: Any = None,
    capabilities: Any = None,
    static_routes: Any = None,
    heartbeat_interval_s: Any = HEARTBEAT_INTERVAL_S,
    route_advertise_interval_s: Any = ROUTE_ADVERTISE_INTERVAL_S,
    registry_stale_after_s: Any = REGISTRY_STALE_AFTER_S,
    route_prune_after_s: Any = ROUTE_PRUNE_AFTER_S,
    auto_probe_on_connect: bool = True,
    auto_select_single_peer: bool = True,
    loopback_local_delivery: bool = False,
    connect_wait_timeout_s: Any = CONNECT_WAIT_TIMEOUT_S,
    shutdown_join_timeout_s: Any = SHUTDOWN_JOIN_TIMEOUT_S,
    dedup_ttl_s: Any = DEDUP_TTL_S,
    dedup_max_entries: Any = DEDUP_MAX_ENTRIES,
    max_inflight_messages: Any = MAX_INFLIGHT_MESSAGES,
    max_queued_messages: Any = MAX_QUEUED_MESSAGES,
    queue_qos0_messages: bool = True,
    shutdown_event: Any = None,
    runtime_ctx_out: Any = None,
    mqtt_module: Any = None,
) -> bool:
    mqtt_mod = _require_paho(mqtt_module)

    effective_broker_host = _safe_str(broker_host or host, MQTT_DEFAULT_HOST).strip() or MQTT_DEFAULT_HOST
    effective_broker_port = _safe_int(
        broker_port if broker_port is not None else port,
        MQTT_DEFAULT_TLS_PORT if _safe_bool(ssl_enabled, False) else MQTT_DEFAULT_PORT,
    )
    effective_node_id = _validate_node_id(node_id or "node-01")

    ctx = build_generic_mqtt_interface_ctx(
        node_id=effective_node_id,
        interface_role=interface_role,
        path_gate=path_gate,
        broker_host=effective_broker_host,
        broker_port=effective_broker_port,
        topic_root=topic_root,
        queue_send=queue_send,
        queue_recv=queue_recv,
        queue_ws=queue_ws,
        queue_io=queue_io,
        queue_event_send=queue_event_send,
        resources=resources,
        registry_table=registry_table,
        registry_lock=registry_lock,
        route_table=route_table,
        route_lock=route_lock,
        runtime_metrics=runtime_metrics,
        runtime_metrics_lock=runtime_metrics_lock,
        shutdown_event=shutdown_event,
        qos=qos,
        keepalive=keepalive,
        ssl_enabled=ssl_enabled,
        default_target_node=default_target_node,
        recv_system_events=recv_system_events,
        node_meta=node_meta,
        node_tags=node_tags,
        capabilities=capabilities,
        registry_stale_after_s=registry_stale_after_s,
        route_prune_after_s=route_prune_after_s,
        heartbeat_interval_s=heartbeat_interval_s,
        route_advertise_interval_s=route_advertise_interval_s,
        static_routes=static_routes,
        auto_probe_on_connect=auto_probe_on_connect,
        auto_select_single_peer=auto_select_single_peer,
        loopback_local_delivery=loopback_local_delivery,
        dedup_ttl_s=dedup_ttl_s,
        dedup_max_entries=dedup_max_entries,
        runtime_ctx_out=runtime_ctx_out,
    )

    effective_client_id = _safe_str(mqtt_client_id, "").strip()
    if not effective_client_id:
        role_fragment = _safe_str(ctx.get("interface_role"), "node").replace("_", "-")
        effective_client_id = "mqtt-%s-%s-%s" % (role_fragment, ctx["node_id"], _make_id()[:8])

    client_obj = _create_paho_client(
        mqtt_mod=mqtt_mod,
        client_id=effective_client_id,
        userdata=ctx,
        protocol_version=protocol_version,
        clean_session=bool(clean_session),
        transport=_safe_str(transport, MQTT_DEFAULT_TRANSPORT) or MQTT_DEFAULT_TRANSPORT,
        reconnect_on_failure=bool(reconnect_on_failure),
    )
    ctx["runtime"]["client_obj"] = client_obj
    ctx["runtime"]["client_id"] = effective_client_id

    try:
        client_obj.enable_logger(logger)
    except Exception:
        pass

    try:
        client_obj.suppress_exceptions = True
    except Exception:
        pass

    client_obj.on_connect = _mqtt_on_connect
    client_obj.on_connect_fail = _mqtt_on_connect_fail
    client_obj.on_disconnect = _mqtt_on_disconnect
    client_obj.on_message = _mqtt_on_message

    _configure_client_authentication(client_obj, username, password)
    _configure_client_tls(
        client_obj,
        ssl_enabled=_safe_bool(ssl_enabled, False),
        ssl_cafile=ssl_cafile,
        ssl_capath=ssl_capath,
        ssl_cadata=ssl_cadata,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
        ssl_password=ssl_password,
        ssl_minimum_tls_version=ssl_minimum_tls_version,
        ssl_ciphers=ssl_ciphers,
        ssl_allow_unverified=ssl_allow_unverified,
        ssl_auto_allow_unverified_if_no_ca=ssl_auto_allow_unverified_if_no_ca,
    )
    _configure_client_runtime_limits(
        client_obj,
        max_inflight_messages=max(1, _safe_int(max_inflight_messages, MAX_INFLIGHT_MESSAGES)),
        max_queued_messages=max(0, _safe_int(max_queued_messages, MAX_QUEUED_MESSAGES)),
        queue_qos0_messages=bool(queue_qos0_messages),
    )
    _configure_disconnect_will(ctx)

    try:
        client_obj.reconnect_delay_set(
            min_delay=max(_safe_int(reconnect_min_delay, RECONNECT_MIN_DELAY_S), 1),
            max_delay=max(_safe_int(reconnect_max_delay, RECONNECT_MAX_DELAY_S), 1),
        )
    except Exception:
        pass

    logger.info(
        "Starte generic_mqtt_interface_thread auf mqtt%s://%s:%s topic_root=%s node_id=%s role=%s qos=%s client_id=%s",
        "s" if _safe_bool(ssl_enabled, False) else "",
        effective_broker_host,
        effective_broker_port,
        ctx["topic_root"],
        ctx["node_id"],
        ctx["interface_role"],
        ctx["qos"],
        effective_client_id,
    )

    started_loop = False

    # --- Robust connect mit exponentiellem Backoff -----------------------
    # Connection refused / transient network errors => WARNING (kein Traceback),
    # dann Retry mit exponentiellem Backoff (1s -> 2s -> 4s ... cap 60s).
    # Shutdown-aware: bei stop_requested wird die Retry-Schleife verlassen.
    backoff_s = 1.0
    backoff_cap_s = 60.0
    connect_wait_s = _safe_float(connect_wait_timeout_s, CONNECT_WAIT_TIMEOUT_S)
    connected_ok = False

    while not connected_ok:
        if ctx["runtime"]["stop_requested"].is_set():
            logger.info("generic_mqtt_interface_thread: Abbruch waehrend Connect-Retry")
            return False
        if ctx["shutdown_event"] is not None and ctx["shutdown_event"].is_set():
            logger.info("generic_mqtt_interface_thread: Shutdown waehrend Connect-Retry")
            return False

        _metrics_inc(ctx, "connection_attempts")
        try:
            client_obj.connect(
                effective_broker_host,
                port=int(effective_broker_port),
                keepalive=int(ctx["keepalive"]),
            )
            client_obj.loop_start()
            started_loop = True

            if not _wait_for_connection(ctx, connect_wait_s):
                logger.warning(
                    "MQTT %s:%s kein CONNACK in %.1fs - Retry in %.1fs",
                    effective_broker_host, effective_broker_port,
                    connect_wait_s, backoff_s,
                )
                try:
                    client_obj.loop_stop()
                except Exception:
                    pass
                started_loop = False
                if _mqtt_shutdown_aware_wait(ctx, backoff_s):
                    return False
                backoff_s = min(backoff_s * 2.0, backoff_cap_s)
                continue

            connected_ok = True
            logger.info(
                "MQTT verbunden: %s:%s",
                effective_broker_host, effective_broker_port,
            )
        except (ConnectionRefusedError, TimeoutError, OSError) as conn_exc:
            logger.warning(
                "MQTT connect %s:%s fehlgeschlagen (%s) - Retry in %.1fs",
                effective_broker_host, effective_broker_port,
                type(conn_exc).__name__, backoff_s,
            )
            try:
                client_obj.loop_stop()
            except Exception:
                pass
            started_loop = False
            if _mqtt_shutdown_aware_wait(ctx, backoff_s):
                return False
            backoff_s = min(backoff_s * 2.0, backoff_cap_s)

    try:

        _start_thread(ctx, "MQTT_Interface_Outbound", _outbound_publish_loop)
        _start_thread(ctx, "MQTT_Interface_Inbound_Forward", _inbound_forward_loop)
        _start_thread(ctx, "MQTT_Interface_Announce", _announce_loop)
        _start_thread(ctx, "MQTT_Interface_Housekeeping", _housekeeping_loop)

        while not ctx["runtime"]["stop_requested"].is_set():
            if ctx["shutdown_event"] is not None and ctx["shutdown_event"].is_set():
                break
            time.sleep(0.20)

        return True
    except KeyboardInterrupt:
        logger.info("generic_mqtt_interface_thread: Abbruch per KeyboardInterrupt")
        return False
    except (ConnectionRefusedError, TimeoutError, OSError) as net_exc:
        logger.warning(
            "MQTT Netzwerkfehler im Runtime-Loop: %s - Thread beendet sich geordnet",
            type(net_exc).__name__,
        )
        return False
    except Exception as exc:
        logger.exception("generic_mqtt_interface_thread Fehler: %s", exc)
        _emit_bus_event(
            ctx,
            "ERROR_OCCURRED",
            {
                "error_code": "MQTT_INTERFACE_RUNTIME_ERROR",
                "message": "Unbehandelte Ausnahme im MQTT-Interface",
                "context": {"exception": _safe_str(exc, "")},
                "service_name": "mqtt_interface",
            },
        )
        return False
    finally:
        ctx["runtime"]["stop_requested"].set()
        _put_to_named_queue(
            ctx,
            "send",
            {"_type": "__STOP__"},
            0.01,
            ok_metric=None,
            fail_metric="outbound_queue_drops",
            log_label="shutdown_send_stop",
        )
        _put_to_named_queue(
            ctx,
            "incoming",
            {"_type": "__STOP__"},
            0.01,
            ok_metric=None,
            fail_metric="inbound_queue_drops",
            log_label="shutdown_incoming_stop",
        )

        _set_local_registry_state(ctx, "offline")

        try:
            if bool(ctx.get("runtime", {}).get("connected_to_broker", threading.Event()).is_set()):
                _publish_node_status(ctx, "offline", retain=True)
        except Exception:
            pass

        try:
            _client_disconnect(ctx)
        except Exception:
            pass

        if started_loop:
            try:
                client_obj.loop_stop()
            except Exception:
                pass

        _join_runtime_threads(ctx, _safe_float(shutdown_join_timeout_s, SHUTDOWN_JOIN_TIMEOUT_S))
        _metrics_refresh_queue_depths(ctx)
        logger.info("generic_mqtt_interface_thread beendet")


def run_generic_mqtt_interface_thread(**kwargs: Any) -> bool:
    """
    Thread-Factory Entry-Point.

    Filtert Orchestrierungs-Kwargs (z.B. 'thread_ctx', 'thread_name', ...)
    gegen die tatsaechliche Signatur von generic_mqtt_interface_thread heraus,
    bevor weitergereicht wird. Verhindert TypeError bei unerwarteten Kwargs
    aus dem Thread-Factory-Mechanismus.
    """
    from src.orchestration.thread_adapters import filter_kwargs
    inner_kwargs = filter_kwargs(generic_mqtt_interface_thread, kwargs)
    return generic_mqtt_interface_thread(**inner_kwargs)
