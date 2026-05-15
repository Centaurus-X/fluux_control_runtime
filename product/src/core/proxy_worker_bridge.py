# -*- coding: utf-8 -*-

# src/core/proxy_worker_bridge.py
#
# v35.1: Native Bruecke zwischen Runtime Worker und externem
# MQTT/WebSocket Proxy-Gateway.
#
# Rolle:
#   * Der Worker bleibt lokale Runtime-Wahrheit.
#   * Der Proxy bleibt Edge/WebSocket/MQTT-Gateway.
#   * Die Bridge spricht ausschliesslich ueber MQTT/MQTTS mit dem Broker.
#   * Kein Master-Zwang, kein direkter Proxy-Core-Import.
#
# Stilregeln:
#   * Kein OOP, keine Decorators, keine lambda-Funktionen.
#   * Funktionen + ctx-Dict.
#   * functools.partial nur dort, wo Callbacks gebunden werden muessen.

import copy
import json
import os
import ssl
import threading
import time
import uuid

from functools import partial
from queue import Empty, Full
from typing import Any

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

try:
    from src.core import _runtime_command_binding as runtime_command_binding
except Exception:
    try:
        import _runtime_command_binding as runtime_command_binding
    except Exception:
        runtime_command_binding = None


PROXY_ENVELOPE_VERSION = "proxy-envelope-v1"
DEFAULT_WORKER_ID = "worker_fn_01"
DEFAULT_BROKER_HOST = "127.0.0.1"
DEFAULT_BROKER_PORT = 1883
DEFAULT_BROKER_SECURE_PORT = 8883
DEFAULT_KEEPALIVE = 60
DEFAULT_QOS = 1
DEFAULT_COMMAND_QOS = 2
DEFAULT_REPLY_QOS = 1
DEFAULT_PRESENCE_QOS = 1
DEFAULT_EVENT_QOS = 1
DEFAULT_RETAIN = False
DEFAULT_PRESENCE_RETAIN = True
DEFAULT_LOOP_SLEEP_S = 0.1
DEFAULT_PRESENCE_INTERVAL_S = 30.0
DEFAULT_OUTBOUND_QUEUE_TIMEOUT_S = 0.05
DEFAULT_COMMAND_EVENT_TIMEOUT_S = 0.05
DEFAULT_MAX_PAYLOAD_BYTES = 262144
BRIDGE_SOURCE = "v35_1_preproduction_final_runtime"
RUNTIME_COMMAND_EVENT_TYPE = "V35_1_PROXY_RUNTIME_COMMAND_RECEIVED"
LEGACY_RUNTIME_COMMAND_EVENT_TYPE = "V34_PROXY_RUNTIME_COMMAND_RECEIVED"
PREVIOUS_RUNTIME_COMMAND_EVENT_TYPE = "V33_PROXY_WORKER_COMMAND_RECEIVED"
OLDER_RUNTIME_COMMAND_EVENT_TYPE = "V32_PROXY_WORKER_COMMAND_RECEIVED"
RUNTIME_COMMAND_TARGET = "thread_management"


# ---------------------------------------------------------------------------
# Sichere Basis-Helfer
# ---------------------------------------------------------------------------


def _now_epoch() -> float:
    return time.time()


def _now_iso_z() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + (".%03dZ" % int((_now_epoch() % 1) * 1000))


def _make_id(prefix: str) -> str:
    return "%s-%s" % (str(prefix).strip() or "id", uuid.uuid4().hex)


def _safe_str(value: Any, default: str = "") -> str:
    try:
        if value is None:
            return str(default)
        return str(value)
    except Exception:
        return str(default)


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled", "y"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled", "n", ""):
            return False
    return bool(default)


def _deepcopy_safe(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return json.dumps({"__unserializable__": True}, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return None
    try:
        if isinstance(value, (bytes, bytearray)):
            return json.loads(value.decode("utf-8", errors="replace"))
        return json.loads(_safe_str(value, ""))
    except Exception:
        return None


def _normalize_optional_text(value: Any) -> str | None:
    text = _safe_str(value, "").strip()
    if not text:
        return None
    if text.lower() in ("none", "null", "nil", "false", "off", "disabled"):
        return None
    return text


def _is_existing_file(path_value: Any) -> bool:
    path_text = _normalize_optional_text(path_value)
    if path_text is None:
        return False
    try:
        return os.path.isfile(os.path.expanduser(os.path.expandvars(path_text)))
    except Exception:
        return False


def _optional_existing_file(path_value: Any) -> str | None:
    path_text = _normalize_optional_text(path_value)
    if path_text is None:
        return None
    expanded = os.path.expanduser(os.path.expandvars(path_text))
    if not os.path.isfile(expanded):
        return None
    return expanded



def _read_env_text(env_name: Any) -> str | None:
    name = _normalize_optional_text(env_name)
    if name is None:
        return None
    try:
        return _normalize_optional_text(os.environ.get(name))
    except Exception:
        return None


def _first_present_text(*values: Any) -> str | None:
    for value in values:
        text = _normalize_optional_text(value)
        if text is not None:
            return text
    return None


def _normalize_mtls_mode(value: Any, default: str = "optional") -> str:
    text = _safe_str(value, default).strip().lower().replace("-", "_")
    if text in ("required", "require", "strict", "mutual_tls", "mtls"):
        return "required"
    if text in ("disabled", "disable", "off", "false", "none", "no"):
        return "disabled"
    return "optional"


def _normalize_acl_policy(value: Any, default: str = "deny") -> str:
    text = _safe_str(value, default).strip().lower()
    if text in ("allow", "permit", "accept"):
        return "allow"
    return "deny"


def _normalize_text_sequence(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items = [item.strip() for item in value.replace(";", ",").split(",")]
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]
    result = []
    for item in items:
        text = _normalize_optional_text(item)
        if text is not None:
            result.append(text)
    return tuple(result)


def _expand_topic_pattern(pattern: Any, worker_id: Any) -> str | None:
    text = _normalize_optional_text(pattern)
    if text is None:
        return None
    worker = normalize_worker_id(worker_id)
    try:
        text = text.replace("{worker_id}", worker)
    except Exception:
        pass
    if "%s" in text:
        try:
            text = text % worker
        except Exception:
            pass
    return text.strip("/")


def _default_publish_topic_patterns(worker_id: Any) -> tuple[str, ...]:
    worker = normalize_worker_id(worker_id)
    return (
        "worker/%s/reply/#" % worker,
        "worker/%s/presence" % worker,
        "worker/%s/snapshot/#" % worker,
        "worker/%s/event/#" % worker,
    )


def _default_subscribe_topic_patterns(worker_id: Any) -> tuple[str, ...]:
    worker = normalize_worker_id(worker_id)
    return ("worker/%s/command/+" % worker,)


def _normalize_acl_topic_patterns(value: Any, worker_id: Any, defaults: tuple[str, ...]) -> tuple[str, ...]:
    source = _normalize_text_sequence(value)
    if not source:
        source = defaults
    result = []
    for item in source:
        expanded = _expand_topic_pattern(item, worker_id)
        if expanded is not None:
            result.append(expanded)
    return tuple(result)


def mqtt_topic_matches(pattern: Any, topic: Any) -> bool:
    pattern_text = _normalize_optional_text(pattern)
    topic_text = _normalize_optional_text(topic)
    if pattern_text is None or topic_text is None:
        return False
    pattern_parts = pattern_text.strip("/").split("/") if pattern_text.strip("/") else []
    topic_parts = topic_text.strip("/").split("/") if topic_text.strip("/") else []
    index = 0
    while index < len(pattern_parts):
        pattern_part = pattern_parts[index]
        if pattern_part == "#":
            return index == len(pattern_parts) - 1
        if index >= len(topic_parts):
            return False
        if pattern_part != "+" and pattern_part != topic_parts[index]:
            return False
        index += 1
    return index == len(topic_parts)


def topic_acl_allows(config: dict[str, Any], action: Any, topic: Any) -> bool:
    cfg = config if isinstance(config, dict) else {}
    if not _safe_bool(cfg.get("topic_acl_enforcement"), True):
        return True
    action_text = _safe_str(action, "").strip().lower()
    if action_text == "subscribe":
        patterns = cfg.get("allowed_subscribe_topics")
    else:
        patterns = cfg.get("allowed_publish_topics")
    for pattern in _normalize_text_sequence(patterns):
        if mqtt_topic_matches(pattern, topic):
            return True
    return _normalize_acl_policy(cfg.get("topic_acl_default_policy"), "deny") == "allow"


def _queue_put(queue_obj: Any, item: Any, timeout_s: float) -> bool:
    if queue_obj is None:
        return False
    try:
        queue_obj.put(item, timeout=max(float(timeout_s), 0.01))
        return True
    except Full:
        return False
    except Exception:
        try:
            queue_obj.put_nowait(item)
            return True
        except Exception:
            return False


def _queue_get(queue_obj: Any, timeout_s: float) -> Any:
    if queue_obj is None:
        return None
    try:
        return queue_obj.get(timeout=max(float(timeout_s), 0.01))
    except Empty:
        return None
    except Exception:
        try:
            return queue_obj.get_nowait()
        except Exception:
            return None


def _payload_size_ok(payload: Any, max_payload_bytes: int) -> bool:
    if max_payload_bytes <= 0:
        return True
    try:
        return len(_json_dumps(payload).encode("utf-8")) <= int(max_payload_bytes)
    except Exception:
        return False

def _resource_entry(resources: Any, name: str) -> dict[str, Any] | None:
    if not isinstance(resources, dict):
        return None
    value = resources.get(name)
    if isinstance(value, dict) and "data" in value and "lock" in value:
        return value
    if isinstance(value, dict):
        return {"data": value, "lock": None}
    return None


def _read_resource_data(resources: Any, name: str, deep_copy: bool = True) -> Any:
    entry = _resource_entry(resources, name)
    if entry is None:
        return None
    lock = entry.get("lock")
    data = entry.get("data")
    if lock is not None:
        try:
            with lock:
                return _deepcopy_safe(data) if deep_copy else data
        except Exception:
            return None
    return _deepcopy_safe(data) if deep_copy else data


def _write_resource_key(resources: Any, name: str, key: str, value: Any) -> dict[str, Any]:
    entry = _resource_entry(resources, name)
    if entry is None:
        return {"ok": False, "reason": "missing_resource", "resource": name}
    lock = entry.get("lock")
    data = entry.get("data")
    if not isinstance(data, dict):
        return {"ok": False, "reason": "resource_not_mapping", "resource": name}
    key_text = _safe_str(key, "").strip()
    if not key_text:
        return {"ok": False, "reason": "empty_key", "resource": name}
    try:
        if lock is not None:
            with lock:
                previous = _deepcopy_safe(data.get(key_text))
                data[key_text] = _deepcopy_safe(value)
                current = _deepcopy_safe(data.get(key_text))
        else:
            previous = _deepcopy_safe(data.get(key_text))
            data[key_text] = _deepcopy_safe(value)
            current = _deepcopy_safe(data.get(key_text))
        return {"ok": True, "resource": name, "key": key_text, "previous": previous, "current": current}
    except Exception as exc:
        return {"ok": False, "reason": "write_failed:%s" % exc, "resource": name, "key": key_text}


def _nested_runtime_parameters(resources: Any) -> dict[str, Any]:
    entry = _resource_entry(resources, "user_config")
    if entry is None:
        return {}
    data = entry.get("data")
    if not isinstance(data, dict):
        return {}
    if not isinstance(data.get("runtime_parameters"), dict):
        data["runtime_parameters"] = {}
    return data["runtime_parameters"]


def _update_runtime_parameter(resources: Any, parameter_key: str, value: Any) -> dict[str, Any]:
    entry = _resource_entry(resources, "user_config")
    if entry is None:
        return {"ok": False, "reason": "missing_user_config"}
    lock = entry.get("lock")
    data = entry.get("data")
    if not isinstance(data, dict):
        return {"ok": False, "reason": "user_config_not_mapping"}
    key_text = _safe_str(parameter_key, "").strip()
    if not key_text:
        return {"ok": False, "reason": "empty_parameter_key"}
    try:
        if lock is not None:
            with lock:
                params = data.setdefault("runtime_parameters", {})
                if not isinstance(params, dict):
                    params = {}
                    data["runtime_parameters"] = params
                previous = _deepcopy_safe(params.get(key_text))
                params[key_text] = _deepcopy_safe(value)
                current = _deepcopy_safe(params.get(key_text))
        else:
            params = data.setdefault("runtime_parameters", {})
            if not isinstance(params, dict):
                params = {}
                data["runtime_parameters"] = params
            previous = _deepcopy_safe(params.get(key_text))
            params[key_text] = _deepcopy_safe(value)
            current = _deepcopy_safe(params.get(key_text))
        return {"ok": True, "parameter_key": key_text, "previous": previous, "current": current}
    except Exception as exc:
        return {"ok": False, "reason": "parameter_update_failed:%s" % exc, "parameter_key": key_text}


def _append_event_store(resources: Any, event_name: str, item: dict[str, Any]) -> dict[str, Any]:
    entry = _resource_entry(resources, "event_store")
    if entry is None:
        return {"ok": False, "reason": "missing_event_store"}
    data = entry.get("data")
    lock = entry.get("lock")
    if not isinstance(data, dict):
        return {"ok": False, "reason": "event_store_not_mapping"}
    event_key = _safe_str(event_name, "proxy_runtime_commands").strip() or "proxy_runtime_commands"
    try:
        record = _deepcopy_safe(item)
        record.setdefault("recorded_at", _now_iso_z())
        if lock is not None:
            with lock:
                items = data.setdefault(event_key, [])
                if not isinstance(items, list):
                    items = []
                    data[event_key] = items
                items.append(record)
                index = len(items) - 1
        else:
            items = data.setdefault(event_key, [])
            if not isinstance(items, list):
                items = []
                data[event_key] = items
            items.append(record)
            index = len(items) - 1
        return {"ok": True, "event_key": event_key, "index": index, "record": record}
    except Exception as exc:
        return {"ok": False, "reason": "event_store_append_failed:%s" % exc, "event_key": event_key}


def _mapping_summary(value: Any, max_keys: int = 40) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"kind": type(value).__name__, "mapping": False}
    keys = sorted([_safe_str(key, "") for key in value.keys()])
    return {
        "kind": "dict",
        "mapping": True,
        "count": len(keys),
        "keys": keys[:max(0, int(max_keys))],
        "truncated": len(keys) > max(0, int(max_keys)),
    }


def _resource_value_by_key(value: Any, key: Any, default: Any = None) -> Any:
    if not isinstance(value, dict):
        return default
    key_text = _safe_str(key, "").strip()
    if not key_text:
        return value
    if "." not in key_text:
        return _deepcopy_safe(value.get(key_text, default))
    current = value
    for part in [item for item in key_text.split(".") if item]:
        if not isinstance(current, dict):
            return default
        current = current.get(part, default)
    return _deepcopy_safe(current)


def _allowed_runtime_resource_names() -> tuple[str, ...]:
    return (
        "worker_state",
        "connected",
        "sensor_values",
        "actuator_values",
        "local_sensor_values",
        "local_actuator_values",
        "user_config",
        "client_config",
        "event_store",
        "config_data",
        "cpu_probe_information",
    )


def _build_resource_snapshot(resources: Any, *, include_full: bool = False, max_keys: int = 40) -> dict[str, Any]:
    snapshot = {}
    for name in _allowed_runtime_resource_names():
        value = _read_resource_data(resources, name, deep_copy=True)
        if value is None:
            continue
        if include_full and name != "config_data":
            snapshot[name] = value
        else:
            snapshot[name] = _mapping_summary(value, max_keys=max_keys)
    return snapshot


def _build_state_read_result(ctx: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    resources = ctx.get("resources")
    requested_resource = _safe_str(payload.get("resource") or payload.get("resource_name") or "worker_state", "worker_state").strip()
    if requested_resource not in _allowed_runtime_resource_names():
        return {"status": "rejected", "reason": "resource_not_allowed", "resource": requested_resource}
    resource_value = _read_resource_data(resources, requested_resource, deep_copy=True)
    key = payload.get("key") or payload.get("path")
    include_full = _safe_bool(payload.get("include_full"), False)
    result = {
        "status": "ok",
        "resource": requested_resource,
        "summary": _mapping_summary(resource_value),
        "runtime_view": {
            "bridge_enabled": True,
            "binding_version": BRIDGE_SOURCE,
        },
    }
    if key:
        result["key"] = _safe_str(key, "")
        result["value"] = _resource_value_by_key(resource_value, key)
    elif include_full and requested_resource != "config_data":
        result["value"] = resource_value
    return result


# ---------------------------------------------------------------------------
# Proxy-v01.7 Topic- und Envelope-Helfer
# ---------------------------------------------------------------------------


def normalize_worker_id(worker_id: Any, default: str = DEFAULT_WORKER_ID) -> str:
    text = _safe_str(worker_id, default).strip().strip("/")
    if not text:
        text = str(default)
    for ch in ("#", "+", "\\"):
        text = text.replace(ch, "_")
    return text


def normalize_topic_part(value: Any, default: str = "runtime") -> str:
    text = _safe_str(value, default).strip().strip("/")
    if not text:
        text = str(default)
    for ch in ("#", "+", "\\"):
        text = text.replace(ch, "_")
    return text


def make_proxy_command_topic(worker_id: Any, domain: Any = "runtime") -> str:
    return "worker/%s/command/%s" % (normalize_worker_id(worker_id), normalize_topic_part(domain))


def make_proxy_command_wildcard(worker_id: Any) -> str:
    return "worker/%s/command/+" % normalize_worker_id(worker_id)


def make_proxy_reply_topic(worker_id: Any, client_id: Any) -> str:
    return "worker/%s/reply/%s" % (normalize_worker_id(worker_id), normalize_topic_part(client_id, "unknown_client"))


def make_proxy_event_topic(worker_id: Any, domain: Any = "runtime") -> str:
    return "worker/%s/event/%s" % (normalize_worker_id(worker_id), normalize_topic_part(domain))


def make_proxy_snapshot_topic(worker_id: Any, domain: Any = "runtime") -> str:
    return "worker/%s/snapshot/%s" % (normalize_worker_id(worker_id), normalize_topic_part(domain))


def make_proxy_presence_topic(worker_id: Any) -> str:
    return "worker/%s/presence" % normalize_worker_id(worker_id)


def parse_proxy_worker_topic(topic: Any) -> dict[str, Any]:
    text = _safe_str(topic, "").strip().strip("/")
    parts = [part for part in text.split("/") if part]
    result = {
        "ok": False,
        "topic": text,
        "worker_id": "",
        "channel": "",
        "domain": "",
        "client_id": "",
    }
    if len(parts) < 3:
        return result
    if parts[0] != "worker":
        return result
    result["worker_id"] = parts[1]
    result["channel"] = parts[2]
    if result["channel"] in ("command", "event", "snapshot") and len(parts) >= 4:
        result["domain"] = parts[3]
        result["ok"] = True
        return result
    if result["channel"] == "reply" and len(parts) >= 4:
        result["client_id"] = parts[3]
        result["ok"] = True
        return result
    if result["channel"] == "presence":
        result["ok"] = True
        return result
    return result


def build_proxy_envelope(
    *,
    kind: str,
    domain: str,
    worker_id: str,
    payload: Any,
    target_kind: str = "",
    target_ref: str = "",
    request_id: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    idempotency_key: str | None = None,
    mqtt_qos: int = DEFAULT_REPLY_QOS,
    app_priority: int = 2,
    retain: bool = False,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective_request_id = request_id or _make_id(kind)
    return {
        "envelope_version": PROXY_ENVELOPE_VERSION,
        "envelope_id": _make_id(kind),
        "kind": _safe_str(kind, "event"),
        "domain": normalize_topic_part(domain, "runtime"),
        "created_at": _now_iso_z(),
        "source": {
            "kind": "worker",
            "ref": normalize_worker_id(worker_id),
        },
        "target": {
            "kind": _safe_str(target_kind, ""),
            "ref": _safe_str(target_ref, ""),
        } if target_kind or target_ref else {},
        "request_id": effective_request_id,
        "correlation_id": correlation_id or effective_request_id,
        "causation_id": causation_id,
        "idempotency_key": idempotency_key,
        "delivery": {
            "mqtt_qos": int(mqtt_qos),
            "app_priority": int(app_priority),
            "retain": bool(retain),
        },
        "payload": payload if payload is not None else {},
        "metadata": metadata if isinstance(metadata, dict) else {},
    }


def normalize_proxy_envelope(value: Any) -> dict[str, Any] | None:
    data = _json_loads(value)
    if not isinstance(data, dict):
        return None
    if data.get("envelope_version") != PROXY_ENVELOPE_VERSION:
        return None
    if not isinstance(data.get("source"), dict):
        data["source"] = {}
    if not isinstance(data.get("target"), dict):
        data["target"] = {}
    if not isinstance(data.get("payload"), dict):
        data["payload"] = {"value": data.get("payload")}
    if not isinstance(data.get("delivery"), dict):
        data["delivery"] = {}
    if not isinstance(data.get("metadata"), dict):
        data["metadata"] = {}
    return data


def extract_client_id_from_command(envelope: dict[str, Any]) -> str:
    source = envelope.get("source") if isinstance(envelope.get("source"), dict) else {}
    if _safe_str(source.get("kind"), "").lower() == "client":
        ref = normalize_topic_part(source.get("ref"), "")
        if ref:
            return ref
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
    for key in ("reply_to_client_id", "client_id", "target_client_id"):
        candidate = normalize_topic_part(payload.get(key), "")
        if candidate:
            return candidate
    target = envelope.get("target") if isinstance(envelope.get("target"), dict) else {}
    if _safe_str(target.get("kind"), "").lower() == "client":
        candidate = normalize_topic_part(target.get("ref"), "")
        if candidate:
            return candidate
    return "unknown_client"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def build_bridge_config(settings: dict[str, Any] | None, runtime_bundle: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings if isinstance(settings, dict) else {}
    runtime_bundle = runtime_bundle if isinstance(runtime_bundle, dict) else {}
    proxy_cfg = settings.get("proxy_worker_bridge") if isinstance(settings.get("proxy_worker_bridge"), dict) else {}
    mqtt_cfg = settings.get("mqtt_client") if isinstance(settings.get("mqtt_client"), dict) else {}

    worker_id = normalize_worker_id(proxy_cfg.get("worker_id") or settings.get("node_id") or DEFAULT_WORKER_ID)
    broker_host = proxy_cfg.get("broker_host") or mqtt_cfg.get("broker_host") or mqtt_cfg.get("broker_ip")
    broker_port = proxy_cfg.get("broker_port") or mqtt_cfg.get("broker_port") or DEFAULT_BROKER_PORT
    secure_port = proxy_cfg.get("secure_port") or proxy_cfg.get("broker_secure_port") or DEFAULT_BROKER_SECURE_PORT
    use_mqtts = _safe_bool(proxy_cfg.get("use_mqtts"), _safe_bool(mqtt_cfg.get("ssl_enabled"), False))

    ca_file = (
        proxy_cfg.get("ca_cert_file")
        or proxy_cfg.get("ca_file")
        or mqtt_cfg.get("ssl_cafile")
        or None
    )

    # mTLS is configurable but not forced. Generic MQTT cert/key values are not
    # inherited automatically because they often point to placeholder files.
    client_cert_file = proxy_cfg.get("client_cert_file")
    client_key_file = proxy_cfg.get("client_key_file")
    mtls_mode = _normalize_mtls_mode(proxy_cfg.get("mtls_mode"), "optional")

    username = _first_present_text(
        proxy_cfg.get("username"),
        _read_env_text(proxy_cfg.get("username_env")),
        mqtt_cfg.get("username"),
        _read_env_text(mqtt_cfg.get("username_env")),
    )
    password = _first_present_text(
        proxy_cfg.get("password"),
        _read_env_text(proxy_cfg.get("password_env")),
        mqtt_cfg.get("password"),
        _read_env_text(mqtt_cfg.get("password_env")),
    )

    resolved_port = _safe_int(secure_port if use_mqtts else broker_port, DEFAULT_BROKER_SECURE_PORT if use_mqtts else DEFAULT_BROKER_PORT)
    allowed_publish_topics = _normalize_acl_topic_patterns(
        proxy_cfg.get("allowed_publish_topics"),
        worker_id,
        _default_publish_topic_patterns(worker_id),
    )
    allowed_subscribe_topics = _normalize_acl_topic_patterns(
        proxy_cfg.get("allowed_subscribe_topics"),
        worker_id,
        _default_subscribe_topic_patterns(worker_id),
    )

    return {
        "enabled": _safe_bool(proxy_cfg.get("enabled"), False),
        "contract_id": _safe_str(proxy_cfg.get("contract_id"), "C27"),
        "worker_id": worker_id,
        "broker_host": _safe_str(broker_host, DEFAULT_BROKER_HOST),
        "broker_port": _safe_int(broker_port, DEFAULT_BROKER_PORT),
        "secure_port": _safe_int(secure_port, DEFAULT_BROKER_SECURE_PORT),
        "effective_port": resolved_port,
        "use_mqtts": use_mqtts,
        "ca_cert_file": _normalize_optional_text(ca_file),
        "client_cert_file": _normalize_optional_text(client_cert_file),
        "client_key_file": _normalize_optional_text(client_key_file),
        "tls_ciphers": _normalize_optional_text(proxy_cfg.get("tls_ciphers") or proxy_cfg.get("ciphers")),
        "tls_allow_insecure": _safe_bool(proxy_cfg.get("tls_allow_insecure"), False),
        "check_hostname": _safe_bool(proxy_cfg.get("check_hostname"), True),
        "mtls_mode": mtls_mode,
        "certificate_rotation_enabled": _safe_bool(proxy_cfg.get("certificate_rotation_enabled"), False),
        "certificate_renew_before_days": _safe_int(proxy_cfg.get("certificate_renew_before_days"), 30),
        "authentication_required": _safe_bool(proxy_cfg.get("authentication_required"), False),
        "username": username,
        "password": password,
        "username_env": _normalize_optional_text(proxy_cfg.get("username_env")),
        "password_env": _normalize_optional_text(proxy_cfg.get("password_env")),
        "client_id": _safe_str(proxy_cfg.get("client_id"), "v35-1-worker-%s" % worker_id),
        "keepalive": _safe_int(proxy_cfg.get("keepalive"), DEFAULT_KEEPALIVE),
        "command_qos": _safe_int(proxy_cfg.get("command_qos"), DEFAULT_COMMAND_QOS),
        "reply_qos": _safe_int(proxy_cfg.get("reply_qos"), DEFAULT_REPLY_QOS),
        "presence_qos": _safe_int(proxy_cfg.get("presence_qos"), DEFAULT_PRESENCE_QOS),
        "event_qos": _safe_int(proxy_cfg.get("event_qos"), DEFAULT_EVENT_QOS),
        "presence_interval_s": _safe_float(proxy_cfg.get("presence_interval_s"), DEFAULT_PRESENCE_INTERVAL_S),
        "loop_sleep_s": _safe_float(proxy_cfg.get("loop_sleep_s"), DEFAULT_LOOP_SLEEP_S),
        "auto_reply_enabled": _safe_bool(proxy_cfg.get("auto_reply_enabled"), True),
        "emit_command_events": _safe_bool(proxy_cfg.get("emit_command_events"), True),
        "snapshot_enabled": _safe_bool(proxy_cfg.get("snapshot_enabled"), True),
        "runtime_binding_enabled": _safe_bool(proxy_cfg.get("runtime_binding_enabled"), True),
        "runtime_event_target": _safe_str(proxy_cfg.get("runtime_event_target"), RUNTIME_COMMAND_TARGET),
        "runtime_state_resource": _safe_str(proxy_cfg.get("runtime_state_resource"), "proxy_runtime_command_state"),
        "runtime_command_binding_version": _safe_str(proxy_cfg.get("runtime_command_binding_version"), BRIDGE_SOURCE),
        "topic_acl_enforcement": _safe_bool(proxy_cfg.get("topic_acl_enforcement"), True),
        "topic_acl_default_policy": _normalize_acl_policy(proxy_cfg.get("topic_acl_default_policy"), "deny"),
        "allowed_publish_topics": allowed_publish_topics,
        "allowed_subscribe_topics": allowed_subscribe_topics,
        "max_payload_bytes": _safe_int(proxy_cfg.get("max_payload_bytes"), DEFAULT_MAX_PAYLOAD_BYTES),
    }

def get_effective_broker_port(config: dict[str, Any]) -> int:
    if _safe_bool(config.get("use_mqtts"), False):
        return _safe_int(config.get("secure_port") or config.get("effective_port"), DEFAULT_BROKER_SECURE_PORT)
    return _safe_int(config.get("broker_port") or config.get("effective_port"), DEFAULT_BROKER_PORT)


# ---------------------------------------------------------------------------
# MQTT TLS und Client-Aufbau
# ---------------------------------------------------------------------------


def _require_mqtt_module() -> Any:
    if mqtt is None:
        raise RuntimeError("proxy_worker_bridge benoetigt paho-mqtt fuer Live-Betrieb")
    return mqtt


def build_tls_arguments(config: dict[str, Any]) -> dict[str, Any]:
    allow_insecure = _safe_bool(config.get("tls_allow_insecure"), False)
    mtls_mode = _normalize_mtls_mode(config.get("mtls_mode"), "optional")
    ca_file = _normalize_optional_text(config.get("ca_cert_file"))
    client_cert_file = _normalize_optional_text(config.get("client_cert_file"))
    client_key_file = _normalize_optional_text(config.get("client_key_file"))
    ciphers = _normalize_optional_text(config.get("tls_ciphers"))

    if ca_file is not None and not _is_existing_file(ca_file):
        if not allow_insecure:
            raise FileNotFoundError("proxy_worker_bridge ca_cert_file not found: %s" % ca_file)
        ca_file = None

    if mtls_mode == "disabled":
        client_cert_file = None
        client_key_file = None
    elif mtls_mode == "required":
        if not _is_existing_file(client_cert_file):
            raise FileNotFoundError("proxy_worker_bridge required client_cert_file not found: %s" % client_cert_file)
        if not _is_existing_file(client_key_file):
            raise FileNotFoundError("proxy_worker_bridge required client_key_file not found: %s" % client_key_file)
    elif not (_is_existing_file(client_cert_file) and _is_existing_file(client_key_file)):
        # Optional mTLS: pass cert/key only when both files exist. A missing
        # optional client certificate must never crash one-way TLS operation.
        client_cert_file = None
        client_key_file = None

    tls_args = {
        "ca_certs": _optional_existing_file(ca_file),
        "certfile": _optional_existing_file(client_cert_file),
        "keyfile": _optional_existing_file(client_key_file),
        "cert_reqs": ssl.CERT_NONE if allow_insecure else ssl.CERT_REQUIRED,
        "tls_version": ssl.PROTOCOL_TLS_CLIENT,
        "ciphers": ciphers,
    }

    cleaned = {}
    for key, value in tls_args.items():
        if value is None:
            continue
        cleaned[key] = value
    return cleaned

def configure_client_tls(client_obj: Any, config: dict[str, Any]) -> bool:
    if not _safe_bool(config.get("use_mqtts"), False):
        return False

    cleaned = build_tls_arguments(config)
    client_obj.tls_set(**cleaned)
    if hasattr(client_obj, "tls_insecure_set"):
        client_obj.tls_insecure_set(bool(config.get("tls_allow_insecure") or not config.get("check_hostname", True)))
    return True


def create_mqtt_client(config: dict[str, Any], ctx: dict[str, Any]) -> Any:
    module_obj = _require_mqtt_module()
    protocol = getattr(module_obj, "MQTTv311", 4)
    try:
        client_obj = module_obj.Client(client_id=_safe_str(config.get("client_id"), ""), protocol=protocol)
    except TypeError:
        client_obj = module_obj.Client(_safe_str(config.get("client_id"), ""), protocol=protocol)

    client_obj.user_data_set(ctx)
    username = _normalize_optional_text(config.get("username"))
    password = config.get("password")
    if _safe_bool(config.get("authentication_required"), False) and username is None:
        raise RuntimeError("proxy_worker_bridge authentication_required=true but no username is configured")
    if username:
        client_obj.username_pw_set(username, password=password)

    configure_client_tls(client_obj, config)
    client_obj.on_connect = _on_connect
    client_obj.on_disconnect = _on_disconnect
    client_obj.on_message = _on_message
    return client_obj


# ---------------------------------------------------------------------------
# Runtime ctx
# ---------------------------------------------------------------------------


def build_bridge_ctx(
    node_id: Any = None,
    resources: Any = None,
    runtime_bundle: dict[str, Any] | None = None,
    command_event_queue: Any = None,
    outbound_event_queue: Any = None,
    logger: Any = None,
    shutdown_event: Any = None,
) -> dict[str, Any]:
    runtime_bundle = runtime_bundle if isinstance(runtime_bundle, dict) else {}
    settings = runtime_bundle.get("settings") if isinstance(runtime_bundle.get("settings"), dict) else {}
    cfg = build_bridge_config(settings, runtime_bundle=runtime_bundle)
    if node_id and not cfg.get("worker_id"):
        cfg["worker_id"] = normalize_worker_id(node_id)

    return {
        "cfg": cfg,
        "identity": {
            "node_id": _safe_str(node_id, cfg.get("worker_id")),
            "worker_id": cfg.get("worker_id"),
            "source": BRIDGE_SOURCE,
        },
        "resources": resources if isinstance(resources, dict) else {},
        "queues": {
            "command_events": command_event_queue,
            "outbound_events": outbound_event_queue,
        },
        "runtime": {
            "client_obj": None,
            "connected": threading.Event(),
            "stop_requested": threading.Event(),
            "last_presence_ts": 0.0,
        },
        "metrics": {
            "connects": 0,
            "disconnects": 0,
            "commands_received": 0,
            "commands_rejected": 0,
            "replies_published": 0,
            "snapshots_published": 0,
            "presence_published": 0,
            "events_published": 0,
            "queue_emits": 0,
            "publish_errors": 0,
            "publish_topic_acl_denies": 0,
            "subscribe_topic_acl_denies": 0,
            "last_error": None,
        },
        "shutdown_event": shutdown_event,
        "logger": logger,
        "locks": {
            "metrics": threading.Lock(),
        },
    }


def _metric_inc(ctx: dict[str, Any], key: str, delta: int = 1) -> None:
    lock = ctx.get("locks", {}).get("metrics")
    if lock is None:
        ctx.setdefault("metrics", {})[key] = int(ctx.get("metrics", {}).get(key, 0)) + int(delta)
        return
    with lock:
        ctx.setdefault("metrics", {})[key] = int(ctx.get("metrics", {}).get(key, 0)) + int(delta)


def _metric_set(ctx: dict[str, Any], key: str, value: Any) -> None:
    lock = ctx.get("locks", {}).get("metrics")
    if lock is None:
        ctx.setdefault("metrics", {})[key] = value
        return
    with lock:
        ctx.setdefault("metrics", {})[key] = value


# ---------------------------------------------------------------------------
# Publish / Command Handling
# ---------------------------------------------------------------------------


def publish_proxy_envelope(ctx: dict[str, Any], topic: str, envelope: dict[str, Any], qos: int, retain: bool) -> bool:
    cfg = ctx.get("cfg", {}) if isinstance(ctx.get("cfg"), dict) else {}
    if not topic_acl_allows(cfg, "publish", topic):
        _metric_inc(ctx, "publish_topic_acl_denies")
        _metric_inc(ctx, "publish_errors")
        _metric_set(ctx, "last_error", "publish_topic_acl_denied:%s" % _safe_str(topic, ""))
        return False
    client_obj = ctx.get("runtime", {}).get("client_obj")
    if client_obj is None:
        _metric_inc(ctx, "publish_errors")
        return False
    if not _payload_size_ok(envelope, _safe_int(cfg.get("max_payload_bytes"), DEFAULT_MAX_PAYLOAD_BYTES)):
        _metric_inc(ctx, "publish_errors")
        _metric_set(ctx, "last_error", "publish_payload_exceeds_max_payload_bytes")
        return False
    try:
        info = client_obj.publish(topic, _json_dumps(envelope), qos=int(qos), retain=bool(retain))
    except Exception as exc:
        _metric_inc(ctx, "publish_errors")
        _metric_set(ctx, "last_error", "publish_failed:%s" % exc)
        return False
    rc = getattr(info, "rc", 0)
    try:
        return int(rc) == 0
    except Exception:
        return False

def publish_presence(ctx: dict[str, Any], status: str = "online") -> bool:
    cfg = ctx.get("cfg", {})
    worker_id = cfg.get("worker_id")
    payload = {
        "status": _safe_str(status, "online"),
        "worker_id": worker_id,
        "runtime_version": BRIDGE_SOURCE,
        "capabilities": ["runtime", "events", "commands", "snapshots", "proxy-envelope-v1", "v35_1-runtime-command-binding"],
        "bridge": BRIDGE_SOURCE,
    }
    envelope = build_proxy_envelope(
        kind="presence",
        domain="worker",
        worker_id=worker_id,
        payload=payload,
        request_id=_make_id("presence"),
        mqtt_qos=_safe_int(cfg.get("presence_qos"), DEFAULT_PRESENCE_QOS),
        app_priority=1,
        retain=DEFAULT_PRESENCE_RETAIN,
    )
    ok = publish_proxy_envelope(
        ctx,
        make_proxy_presence_topic(worker_id),
        envelope,
        qos=_safe_int(cfg.get("presence_qos"), DEFAULT_PRESENCE_QOS),
        retain=DEFAULT_PRESENCE_RETAIN,
    )
    if ok:
        ctx["runtime"]["last_presence_ts"] = _now_epoch()
        _metric_inc(ctx, "presence_published")
    return ok


def _emit_command_event(ctx: dict[str, Any], topic_info: dict[str, Any], envelope: dict[str, Any], runtime_reply_payload: dict[str, Any] | None = None) -> bool:
    if not _safe_bool(ctx.get("cfg", {}).get("emit_command_events"), True):
        return False
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
    domain = normalize_topic_part(topic_info.get("domain") or envelope.get("domain"), "runtime")
    operation = normalize_command_operation(envelope, domain)
    item = {
        "event_type": RUNTIME_COMMAND_EVENT_TYPE,
        "legacy_event_type": LEGACY_RUNTIME_COMMAND_EVENT_TYPE,
        "previous_event_type": PREVIOUS_RUNTIME_COMMAND_EVENT_TYPE,
        "accepted_event_types": (
            RUNTIME_COMMAND_EVENT_TYPE,
            LEGACY_RUNTIME_COMMAND_EVENT_TYPE,
            PREVIOUS_RUNTIME_COMMAND_EVENT_TYPE,
            OLDER_RUNTIME_COMMAND_EVENT_TYPE,
        ),
        "target": RUNTIME_COMMAND_TARGET,
        "message_type": RUNTIME_COMMAND_TARGET,
        "_source": BRIDGE_SOURCE,
        "_ts": _now_epoch(),
        "worker_id": topic_info.get("worker_id"),
        "domain": domain,
        "operation": operation,
        "request_id": envelope.get("request_id"),
        "correlation_id": envelope.get("correlation_id"),
        "payload": _deepcopy_safe(payload),
        "envelope": _deepcopy_safe(envelope),
        "runtime_binding": {
            "version": "v35.1",
            "mode": BRIDGE_SOURCE,
            "safe_direct_io_write": False,
        },
    }
    if isinstance(runtime_reply_payload, dict):
        item["runtime_reply_payload"] = _deepcopy_safe(runtime_reply_payload)
        item["runtime_result"] = _deepcopy_safe(runtime_reply_payload.get("result"))
        item["runtime_status"] = runtime_reply_payload.get("status")
    ok = _queue_put(
        ctx.get("queues", {}).get("command_events"),
        item,
        DEFAULT_COMMAND_EVENT_TIMEOUT_S,
    )
    if ok:
        _metric_inc(ctx, "queue_emits")
    return ok


def build_command_reply_payload(ctx: dict[str, Any], envelope: dict[str, Any], topic_info: dict[str, Any]) -> dict[str, Any]:
    return build_runtime_command_reply_payload(ctx, envelope, topic_info)


def normalize_command_operation(envelope: dict[str, Any], domain: str = "runtime") -> str:
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
    for key in ("operation", "command", "action", "method"):
        candidate = normalize_topic_part(payload.get(key), "")
        if candidate:
            return candidate
    if normalize_topic_part(domain, "runtime") == "snapshot":
        return "runtime_snapshot"
    return "runtime_command"


def build_runtime_snapshot_payload(ctx: dict[str, Any], snapshot_domain: str = "runtime", request_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = ctx.get("cfg", {})
    metrics = ctx.get("metrics", {}) if isinstance(ctx.get("metrics"), dict) else {}
    runtime = ctx.get("runtime", {}) if isinstance(ctx.get("runtime"), dict) else {}
    request_payload = request_payload if isinstance(request_payload, dict) else {}
    include_full = _safe_bool(request_payload.get("include_full"), False)
    max_keys = _safe_int(request_payload.get("max_keys"), 40)
    return {
        "status": "ok",
        "worker_id": cfg.get("worker_id"),
        "snapshot_domain": normalize_topic_part(snapshot_domain, "runtime"),
        "created_by": BRIDGE_SOURCE,
        "runtime_view": {
            "bridge_enabled": True,
            "connected": bool(runtime.get("connected") is not None and runtime.get("connected").is_set()),
            "capabilities": [
                "presence",
                "command_reply",
                "runtime_snapshot",
                "state_read",
                "set_control_method_runtime_state",
                "parameter_update_user_config",
                "safe_runtime_command_event_store",
            ],
            "metrics": _deepcopy_safe(metrics),
        },
        "resources": _build_resource_snapshot(
            ctx.get("resources"),
            include_full=include_full,
            max_keys=max_keys,
        ),
    }


def _handle_set_control_method(ctx: dict[str, Any], payload: dict[str, Any], operation: str) -> dict[str, Any]:
    control_method = payload.get("value")
    if control_method is None:
        control_method = payload.get("control_method") or payload.get("method")
    method_text = _safe_str(control_method, "").strip()
    if not method_text:
        return {"status": "rejected", "reason": "missing_control_method"}
    resources = ctx.get("resources")
    write_result = _write_resource_key(resources, "worker_state", "CONTROL_METHOD", method_text)
    _write_resource_key(resources, "worker_state", "CONTROL_METHOD_UPDATED_AT", _now_iso_z())
    event_result = _append_event_store(
        resources,
        "proxy_runtime_commands",
        {
            "operation": operation,
            "control_method": method_text,
            "safe_direct_io_write": False,
        },
    )
    return {
        "status": "applied_to_runtime_state" if write_result.get("ok") else "queued_for_runtime",
        "write_result": write_result,
        "event_store": event_result,
        "safe_direct_io_write": False,
    }


def _handle_parameter_update(ctx: dict[str, Any], payload: dict[str, Any], operation: str) -> dict[str, Any]:
    key = payload.get("parameter_key") or payload.get("key") or payload.get("parameter")
    value = payload.get("value")
    update_result = _update_runtime_parameter(ctx.get("resources"), key, value)
    event_result = _append_event_store(
        ctx.get("resources"),
        "proxy_runtime_commands",
        {
            "operation": operation,
            "parameter_key": _safe_str(key, ""),
            "value": _deepcopy_safe(value),
            "safe_direct_io_write": False,
        },
    )
    return {
        "status": "applied_to_user_config" if update_result.get("ok") else "queued_for_runtime",
        "update_result": update_result,
        "event_store": event_result,
        "safe_direct_io_write": False,
    }


def _handle_safe_runtime_command(ctx: dict[str, Any], payload: dict[str, Any], operation: str) -> dict[str, Any]:
    command_id = _make_id("safe-runtime-command")
    event_result = _append_event_store(
        ctx.get("resources"),
        "proxy_safe_runtime_commands",
        {
            "command_id": command_id,
            "operation": operation,
            "payload": _deepcopy_safe(payload),
            "safe_direct_io_write": False,
        },
    )
    return {
        "status": "queued_for_runtime",
        "command_id": command_id,
        "event_store": event_result,
        "safe_direct_io_write": False,
    }


def build_runtime_command_reply_payload(ctx: dict[str, Any], envelope: dict[str, Any], topic_info: dict[str, Any]) -> dict[str, Any]:
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
    domain = normalize_topic_part(topic_info.get("domain") or envelope.get("domain"), "runtime")
    operation = normalize_command_operation(envelope, domain)
    event = {
        "event_type": RUNTIME_COMMAND_EVENT_TYPE,
        "legacy_event_type": LEGACY_RUNTIME_COMMAND_EVENT_TYPE,
        "previous_event_type": PREVIOUS_RUNTIME_COMMAND_EVENT_TYPE,
        "accepted_event_types": (
            RUNTIME_COMMAND_EVENT_TYPE,
            LEGACY_RUNTIME_COMMAND_EVENT_TYPE,
            PREVIOUS_RUNTIME_COMMAND_EVENT_TYPE,
            OLDER_RUNTIME_COMMAND_EVENT_TYPE,
        ),
        "target": RUNTIME_COMMAND_TARGET,
        "worker_id": topic_info.get("worker_id") or ctx.get("cfg", {}).get("worker_id"),
        "domain": domain,
        "operation": operation,
        "request_id": envelope.get("request_id"),
        "correlation_id": envelope.get("correlation_id") or envelope.get("request_id"),
        "payload": _deepcopy_safe(payload),
        "envelope": _deepcopy_safe(envelope),
    }
    if runtime_command_binding is not None:
        result = runtime_command_binding.apply_runtime_command(
            ctx.get("resources"),
            event,
            apply_side_effects=True,
            actor=BRIDGE_SOURCE,
        )
    else:
        result = _handle_safe_runtime_command(ctx, payload, operation)
        result.setdefault("binding_version", BRIDGE_SOURCE)
    base_payload = {
        "status": result.get("status", "accepted"),
        "worker_id": ctx.get("cfg", {}).get("worker_id"),
        "domain": domain,
        "operation": operation,
        "received_payload": _deepcopy_safe(payload),
        "bridge": BRIDGE_SOURCE,
        "runtime_binding": {
            "version": "v35.1",
            "mode": BRIDGE_SOURCE,
            "event_type": RUNTIME_COMMAND_EVENT_TYPE,
            "legacy_event_type": LEGACY_RUNTIME_COMMAND_EVENT_TYPE,
            "target": RUNTIME_COMMAND_TARGET,
            "safe_direct_io_write": False,
        },
        "result": result,
    }
    return base_payload


def publish_command_reply(ctx: dict[str, Any], command_envelope: dict[str, Any], topic_info: dict[str, Any], payload: dict[str, Any] | None = None) -> bool:
    cfg = ctx.get("cfg", {})
    worker_id = cfg.get("worker_id")
    client_id = extract_client_id_from_command(command_envelope)
    reply_payload = payload if isinstance(payload, dict) else build_command_reply_payload(ctx, command_envelope, topic_info)
    reply_envelope = build_proxy_envelope(
        kind="reply",
        domain=topic_info.get("domain") or command_envelope.get("domain") or "runtime",
        worker_id=worker_id,
        target_kind="client",
        target_ref=client_id,
        request_id=command_envelope.get("request_id"),
        correlation_id=command_envelope.get("correlation_id") or command_envelope.get("request_id"),
        causation_id=command_envelope.get("envelope_id"),
        payload=reply_payload,
        mqtt_qos=_safe_int(cfg.get("reply_qos"), DEFAULT_REPLY_QOS),
        app_priority=2,
        retain=False,
    )
    ok = publish_proxy_envelope(
        ctx,
        make_proxy_reply_topic(worker_id, client_id),
        reply_envelope,
        qos=_safe_int(cfg.get("reply_qos"), DEFAULT_REPLY_QOS),
        retain=False,
    )
    if ok:
        _metric_inc(ctx, "replies_published")
    return ok


def publish_snapshot(ctx: dict[str, Any], command_envelope: dict[str, Any], topic_info: dict[str, Any]) -> bool:
    cfg = ctx.get("cfg", {})
    worker_id = cfg.get("worker_id")
    payload = command_envelope.get("payload") if isinstance(command_envelope.get("payload"), dict) else {}
    snapshot_domain = normalize_topic_part(payload.get("snapshot_domain") or topic_info.get("domain") or "runtime")
    snapshot_payload = {
        "status": "ok",
        "worker_id": worker_id,
        "snapshot_domain": snapshot_domain,
        "created_by": BRIDGE_SOURCE,
        "runtime_view": {
            "bridge_enabled": True,
            "capabilities": ["presence", "command_reply", "snapshot_placeholder"],
        },
    }
    envelope = build_proxy_envelope(
        kind="snapshot",
        domain=snapshot_domain,
        worker_id=worker_id,
        payload=snapshot_payload,
        request_id=command_envelope.get("request_id"),
        correlation_id=command_envelope.get("correlation_id") or command_envelope.get("request_id"),
        causation_id=command_envelope.get("envelope_id"),
        mqtt_qos=_safe_int(cfg.get("reply_qos"), DEFAULT_REPLY_QOS),
        app_priority=2,
        retain=False,
    )
    ok = publish_proxy_envelope(
        ctx,
        make_proxy_snapshot_topic(worker_id, snapshot_domain),
        envelope,
        qos=_safe_int(cfg.get("reply_qos"), DEFAULT_REPLY_QOS),
        retain=False,
    )
    if ok:
        _metric_inc(ctx, "snapshots_published")
    return ok


def handle_proxy_command(ctx: dict[str, Any], topic: str, payload: Any) -> str:
    topic_info = parse_proxy_worker_topic(topic)
    cfg = ctx.get("cfg", {})
    worker_id = cfg.get("worker_id")

    if not topic_info.get("ok") or topic_info.get("channel") != "command":
        _metric_inc(ctx, "commands_rejected")
        return "topic_rejected"
    if normalize_worker_id(topic_info.get("worker_id")) != normalize_worker_id(worker_id):
        _metric_inc(ctx, "commands_rejected")
        return "worker_id_mismatch"

    envelope = normalize_proxy_envelope(payload)
    if envelope is None:
        _metric_inc(ctx, "commands_rejected")
        _metric_set(ctx, "last_error", "invalid_proxy_envelope")
        return "invalid_envelope"

    target = envelope.get("target") if isinstance(envelope.get("target"), dict) else {}
    target_ref = normalize_worker_id(target.get("ref"), worker_id)
    if target_ref and target_ref != normalize_worker_id(worker_id):
        _metric_inc(ctx, "commands_rejected")
        return "target_worker_mismatch"

    if not _payload_size_ok(envelope, _safe_int(cfg.get("max_payload_bytes"), DEFAULT_MAX_PAYLOAD_BYTES)):
        _metric_inc(ctx, "commands_rejected")
        _metric_set(ctx, "last_error", "command_payload_exceeds_max_payload_bytes")
        return "too_large"

    _metric_inc(ctx, "commands_received")

    domain = normalize_topic_part(topic_info.get("domain") or envelope.get("domain"), "runtime")
    reply_payload = build_runtime_command_reply_payload(ctx, envelope, topic_info)
    _emit_command_event(ctx, topic_info, envelope, runtime_reply_payload=reply_payload)
    operation = normalize_command_operation(envelope, domain)

    if (domain == "snapshot" or operation == "runtime_snapshot") and _safe_bool(cfg.get("snapshot_enabled"), True):
        publish_snapshot(ctx, envelope, topic_info)

    if _safe_bool(cfg.get("auto_reply_enabled"), True):
        publish_command_reply(ctx, envelope, topic_info, payload=reply_payload)
    return "handled"


def publish_outbound_event_from_queue(ctx: dict[str, Any], item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    cfg = ctx.get("cfg", {})
    worker_id = cfg.get("worker_id")
    domain = normalize_topic_part(item.get("domain") or item.get("event_domain") or "runtime")
    payload = item.get("payload") if isinstance(item.get("payload"), dict) else item
    envelope = build_proxy_envelope(
        kind="event",
        domain=domain,
        worker_id=worker_id,
        payload=payload,
        request_id=item.get("request_id") or _make_id("event"),
        correlation_id=item.get("correlation_id"),
        causation_id=item.get("causation_id"),
        idempotency_key=item.get("idempotency_key"),
        mqtt_qos=_safe_int(cfg.get("event_qos"), DEFAULT_EVENT_QOS),
        app_priority=2,
        retain=False,
    )
    ok = publish_proxy_envelope(
        ctx,
        make_proxy_event_topic(worker_id, domain),
        envelope,
        qos=_safe_int(cfg.get("event_qos"), DEFAULT_EVENT_QOS),
        retain=False,
    )
    if ok:
        _metric_inc(ctx, "events_published")
    return ok


# ---------------------------------------------------------------------------
# MQTT Callbacks
# ---------------------------------------------------------------------------


def _on_connect(client_obj: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any = None, *extra: Any) -> None:
    ctx = userdata if isinstance(userdata, dict) else {}
    cfg = ctx.get("cfg", {})
    ctx.get("runtime", {}).get("connected", threading.Event()).set()
    _metric_inc(ctx, "connects")
    subscribe_topic = make_proxy_command_wildcard(cfg.get("worker_id"))
    if not topic_acl_allows(cfg, "subscribe", subscribe_topic):
        _metric_inc(ctx, "subscribe_topic_acl_denies")
        _metric_set(ctx, "last_error", "subscribe_topic_acl_denied:%s" % subscribe_topic)
    else:
        try:
            client_obj.subscribe(subscribe_topic, qos=_safe_int(cfg.get("command_qos"), DEFAULT_COMMAND_QOS))
        except Exception as exc:
            _metric_set(ctx, "last_error", "subscribe_failed:%s" % exc)
    publish_presence(ctx, "online")
    logger = ctx.get("logger")
    if logger is not None:
        try:
            logger.info("[proxy_worker_bridge] connected reason=%s worker_id=%s", reason_code, cfg.get("worker_id"))
        except Exception:
            pass


def _on_disconnect(client_obj: Any, userdata: Any, reason_code: Any, properties: Any = None, *extra: Any) -> None:
    ctx = userdata if isinstance(userdata, dict) else {}
    ctx.get("runtime", {}).get("connected", threading.Event()).clear()
    _metric_inc(ctx, "disconnects")
    logger = ctx.get("logger")
    if logger is not None:
        try:
            logger.warning("[proxy_worker_bridge] disconnected reason=%s", reason_code)
        except Exception:
            pass


def _on_message(client_obj: Any, userdata: Any, message: Any) -> None:
    ctx = userdata if isinstance(userdata, dict) else {}
    topic = _safe_str(getattr(message, "topic", ""), "")
    payload = getattr(message, "payload", None)
    result = handle_proxy_command(ctx, topic, payload)
    logger = ctx.get("logger")
    if logger is not None:
        try:
            logger.debug("[proxy_worker_bridge] command topic=%s result=%s", topic, result)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main Runtime
# ---------------------------------------------------------------------------


def _should_stop(ctx: dict[str, Any]) -> bool:
    runtime = ctx.get("runtime", {})
    if runtime.get("stop_requested") is not None and runtime["stop_requested"].is_set():
        return True
    shutdown_event = ctx.get("shutdown_event")
    if shutdown_event is not None:
        try:
            if shutdown_event.is_set():
                return True
        except Exception:
            return False
    return False


def run_proxy_worker_bridge(ctx: dict[str, Any]) -> None:
    cfg = ctx.get("cfg", {})
    if not _safe_bool(cfg.get("enabled"), False):
        logger = ctx.get("logger")
        if logger is not None:
            try:
                logger.info("[proxy_worker_bridge] disabled via config")
            except Exception:
                pass
        return

    try:
        client_obj = create_mqtt_client(cfg, ctx)
    except Exception as exc:
        _metric_set(ctx, "last_error", "mqtt_client_create_failed:%s" % exc)
        logger = ctx.get("logger")
        if logger is not None:
            try:
                logger.error("[proxy_worker_bridge] mqtt client setup failed: %s", exc)
            except Exception:
                pass
        return
    ctx["runtime"]["client_obj"] = client_obj

    host = _safe_str(cfg.get("broker_host"), DEFAULT_BROKER_HOST)
    port = get_effective_broker_port(cfg)
    keepalive = _safe_int(cfg.get("keepalive"), DEFAULT_KEEPALIVE)

    logger = ctx.get("logger")
    if logger is not None:
        try:
            logger.info("[proxy_worker_bridge] connecting broker=%s:%s mqtts=%s worker_id=%s", host, port, cfg.get("use_mqtts"), cfg.get("worker_id"))
        except Exception:
            pass

    client_obj.connect(host, port, keepalive=keepalive)
    client_obj.loop_start()

    try:
        while not _should_stop(ctx):
            now_ts = _now_epoch()
            last_presence = _safe_float(ctx.get("runtime", {}).get("last_presence_ts"), 0.0)
            presence_interval = _safe_float(cfg.get("presence_interval_s"), DEFAULT_PRESENCE_INTERVAL_S)
            if ctx.get("runtime", {}).get("connected").is_set() and now_ts - last_presence >= presence_interval:
                publish_presence(ctx, "online")

            outbound_item = _queue_get(ctx.get("queues", {}).get("outbound_events"), DEFAULT_OUTBOUND_QUEUE_TIMEOUT_S)
            if outbound_item is not None:
                publish_outbound_event_from_queue(ctx, outbound_item)

            time.sleep(min(_safe_float(cfg.get("loop_sleep_s"), DEFAULT_LOOP_SLEEP_S), 0.25))
    finally:
        try:
            if ctx.get("runtime", {}).get("connected").is_set():
                publish_presence(ctx, "offline")
        except Exception:
            pass
        try:
            client_obj.loop_stop()
        except Exception:
            pass
        try:
            client_obj.disconnect()
        except Exception:
            pass


def run_proxy_worker_bridge_thread(
    node_id=None,
    resources=None,
    command_event_queue=None,
    outbound_event_queue=None,
    runtime_bundle=None,
    logger=None,
    shutdown_event=None,
):
    ctx = build_bridge_ctx(
        node_id=node_id,
        resources=resources,
        runtime_bundle=runtime_bundle or {},
        command_event_queue=command_event_queue,
        outbound_event_queue=outbound_event_queue,
        logger=logger,
        shutdown_event=shutdown_event,
    )
    run_proxy_worker_bridge(ctx)
