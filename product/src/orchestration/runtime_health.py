# -*- coding: utf-8 -*-

# src/orchestration/runtime_health.py

"""Runtime health snapshot writer for v35.1.

The worker runtime stays JSON/memory based. PostgreSQL is documented as an
external Gateway/Proxy responsibility and is therefore reported here only as an
external dependency scope marker, not as a local worker database dependency.
"""

import copy
import json
import os
import time


RUNTIME_HEALTH_VERSION = "v35.1"
POSTGRES_SCOPE = "gateway_proxy_external_not_worker_runtime_dependency"


def now_epoch():
    return time.time()


def now_iso_z():
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + (".%03dZ" % int((now_epoch() % 1) * 1000))


def safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled", ""):
            return False
    return bool(default)


def safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def safe_int(value, default=0):
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def deepcopy_safe(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def queue_size(queue_obj):
    if queue_obj is None:
        return None
    try:
        return int(queue_obj.qsize())
    except Exception:
        return None


def summarize_queues(queue_tools):
    tools = queue_tools if isinstance(queue_tools, dict) else {}
    queues = tools.get("queues_dict") if isinstance(tools.get("queues_dict"), dict) else {}
    result = {}
    for name, queue_obj in sorted(queues.items()):
        result[name] = {
            "size": queue_size(queue_obj),
        }
    return result


def summarize_thread_entry(entry):
    if not isinstance(entry, dict):
        return {}
    spec = entry.get("spec") if isinstance(entry.get("spec"), dict) else {}
    thread_obj = entry.get("thread")
    alive = None
    name = spec.get("name")
    if thread_obj is not None:
        try:
            alive = bool(thread_obj.is_alive())
        except Exception:
            alive = None
        try:
            name = thread_obj.name or name
        except Exception:
            pass
    return {
        "name": name,
        "component_name": spec.get("component_name"),
        "state": entry.get("state"),
        "alive": alive,
        "daemon": entry.get("daemon"),
        "started_ts": entry.get("started_ts"),
        "stopped_ts": entry.get("stopped_ts"),
        "error": entry.get("error"),
    }


def summarize_threads(thread_entries):
    result = []
    for entry in tuple(thread_entries or ()):
        item = summarize_thread_entry(entry)
        if item:
            result.append(item)
    return result


def read_resource_data(resources, name):
    if not isinstance(resources, dict):
        return None
    entry = resources.get(name)
    if isinstance(entry, dict) and "data" in entry and "lock" in entry:
        lock = entry.get("lock")
        try:
            with lock:
                return deepcopy_safe(entry.get("data"))
        except Exception:
            return deepcopy_safe(entry.get("data"))
    return deepcopy_safe(entry)


def mapping_summary(value, max_keys=20):
    if isinstance(value, dict):
        keys = sorted([str(key) for key in value.keys()])
        return {
            "type": "dict",
            "count": len(keys),
            "keys": keys[:max(0, int(max_keys))],
            "truncated": len(keys) > max(0, int(max_keys)),
        }
    if isinstance(value, (list, tuple, set)):
        return {
            "type": type(value).__name__,
            "count": len(value),
        }
    if value is None:
        return {"type": "none", "count": 0}
    return {"type": type(value).__name__}


def summarize_resources(resources):
    selected_names = (
        "worker_state",
        "connected",
        "config_data",
        "sensor_values",
        "actuator_values",
        "event_store",
        "proxy_runtime_command_state",
        "cpu_probe_information",
    )
    result = {}
    for name in selected_names:
        value = read_resource_data(resources, name)
        if value is None:
            continue
        result[name] = mapping_summary(value)
    return result


def sanitize_proxy_bridge_settings(settings):
    cfg = settings.get("proxy_worker_bridge") if isinstance(settings.get("proxy_worker_bridge"), dict) else {}
    return {
        "enabled": safe_bool(cfg.get("enabled"), False),
        "worker_id": cfg.get("worker_id"),
        "broker_host": cfg.get("broker_host"),
        "effective_port": cfg.get("effective_port"),
        "use_mqtts": safe_bool(cfg.get("use_mqtts"), False),
        "authentication_required": safe_bool(cfg.get("authentication_required"), False),
        "username_configured": bool(cfg.get("username")),
        "username_env": cfg.get("username_env"),
        "password_configured": bool(cfg.get("password")),
        "mtls_mode": cfg.get("mtls_mode"),
        "certificate_rotation_enabled": safe_bool(cfg.get("certificate_rotation_enabled"), False),
        "certificate_renew_before_days": cfg.get("certificate_renew_before_days"),
        "topic_acl_enforcement": safe_bool(cfg.get("topic_acl_enforcement"), True),
        "topic_acl_default_policy": cfg.get("topic_acl_default_policy"),
        "allowed_publish_topics": deepcopy_safe(cfg.get("allowed_publish_topics")),
        "allowed_subscribe_topics": deepcopy_safe(cfg.get("allowed_subscribe_topics")),
    }


def build_external_dependency_scope(settings):
    bridge = settings.get("proxy_worker_bridge") if isinstance(settings.get("proxy_worker_bridge"), dict) else {}
    monitoring = settings.get("monitoring") if isinstance(settings.get("monitoring"), dict) else {}
    return {
        "emqx_mqtt": {
            "configured": bool(bridge.get("broker_host")),
            "host": bridge.get("broker_host"),
            "port": bridge.get("effective_port"),
            "protocol": "mqtts" if safe_bool(bridge.get("use_mqtts"), False) else "mqtt",
            "runtime_check_mode": "bridge_connectivity_and_presence",
        },
        "proxy_gateway": {
            "configured": safe_bool(bridge.get("enabled"), False),
            "runtime_check_mode": "mqtt_command_reply_presence",
        },
        "postgresql": {
            "worker_local_dependency": False,
            "scope": POSTGRES_SCOPE,
            "backup_restore_owner": "gateway_proxy_instance",
            "worker_action": "no_local_database_patch_required",
        },
        "alerting": deepcopy_safe(monitoring.get("alerting", {})),
    }


def build_runtime_health_snapshot(runtime_bundle, runtime_state, queue_tools, thread_entries, status="running"):
    bundle = runtime_bundle if isinstance(runtime_bundle, dict) else {}
    settings = bundle.get("settings") if isinstance(bundle.get("settings"), dict) else {}
    state = runtime_state if isinstance(runtime_state, dict) else {}
    monitoring = settings.get("monitoring") if isinstance(settings.get("monitoring"), dict) else {}
    fieldbus = settings.get("fieldbus") if isinstance(settings.get("fieldbus"), dict) else {}
    security = settings.get("security") if isinstance(settings.get("security"), dict) else {}
    resources = state.get("resources") if isinstance(state.get("resources"), dict) else {}
    return {
        "schema_version": 1,
        "runtime_health_version": RUNTIME_HEALTH_VERSION,
        "status": str(status or "running"),
        "created_at": now_iso_z(),
        "created_ts": now_epoch(),
        "node_id": settings.get("node_id") or state.get("node_id"),
        "runtime_contract_marker": "v35_1_preproduction_final_runtime",
        "postgres_scope": POSTGRES_SCOPE,
        "monitoring": deepcopy_safe(monitoring),
        "security": deepcopy_safe(security),
        "fieldbus": deepcopy_safe(fieldbus),
        "proxy_worker_bridge": sanitize_proxy_bridge_settings(settings),
        "external_dependencies": build_external_dependency_scope(settings),
        "queues": summarize_queues(queue_tools),
        "threads": summarize_threads(thread_entries),
        "resources": summarize_resources(resources),
    }


def write_json_atomic(path, data):
    path_text = str(path or "").strip()
    if not path_text:
        return False
    directory = os.path.dirname(path_text)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = "%s.tmp.%d" % (path_text, os.getpid())
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
    os.replace(tmp_path, path_text)
    return True


def write_runtime_health_snapshot(runtime_bundle, runtime_state, queue_tools, thread_entries, status="running"):
    settings = runtime_bundle.get("settings") if isinstance(runtime_bundle, dict) and isinstance(runtime_bundle.get("settings"), dict) else {}
    monitoring = settings.get("monitoring") if isinstance(settings.get("monitoring"), dict) else {}
    health = monitoring.get("health") if isinstance(monitoring.get("health"), dict) else {}
    if not safe_bool(health.get("enabled"), True):
        return False
    path = health.get("path")
    snapshot = build_runtime_health_snapshot(runtime_bundle, runtime_state, queue_tools, thread_entries, status=status)
    return write_json_atomic(path, snapshot)


def maybe_write_runtime_health_snapshot(runtime_bundle, runtime_state, queue_tools, thread_entries, health_state, status="running", logger=None):
    state = health_state if isinstance(health_state, dict) else {}
    settings = runtime_bundle.get("settings") if isinstance(runtime_bundle, dict) and isinstance(runtime_bundle.get("settings"), dict) else {}
    monitoring = settings.get("monitoring") if isinstance(settings.get("monitoring"), dict) else {}
    health = monitoring.get("health") if isinstance(monitoring.get("health"), dict) else {}
    if not safe_bool(health.get("enabled"), True):
        return False
    now = now_epoch()
    interval_s = safe_float(health.get("interval_s"), 15.0)
    last_ts = safe_float(state.get("last_ts"), 0.0)
    status_text = str(status or "running")
    force_write = status_text not in ("running", "loop")
    if not force_write and last_ts > 0.0 and now - last_ts < max(0.1, interval_s):
        return False
    try:
        ok = write_runtime_health_snapshot(runtime_bundle, runtime_state, queue_tools, thread_entries, status=status)
        if ok:
            state["last_ts"] = now
        return ok
    except Exception as exc:
        state["last_error"] = str(exc)
        if logger is not None:
            try:
                logger.warning("[runtime_health] snapshot write failed: %s", exc)
            except Exception:
                pass
        return False
