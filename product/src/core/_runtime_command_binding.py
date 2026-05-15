# -*- coding: utf-8 -*-

# src/core/_runtime_command_binding.py
#
# v35.1 pre-production final runtime command binding.
#
# Rolle:
#   * sichere, deterministische Runtime-Anbindung fuer Proxy-Worker-Commands
#   * keine direkten Feldbus-/IO-Schreibzugriffe
#   * zentrale Funktionen fuer Bridge und Thread Management
#
# Stil:
#   * keine Klassen, keine Decorators, keine Lambda-Funktionen
#   * rein funktionale Helper mit dict-Ressourcen

import copy
import time
import uuid
from typing import Any


BINDING_VERSION = "v35_1_preproduction_final_runtime"
COMMAND_EVENT_TYPE = "V35_1_PROXY_RUNTIME_COMMAND_RECEIVED"
LEGACY_COMMAND_EVENT_TYPES = (
    "V34_PROXY_RUNTIME_COMMAND_RECEIVED",
    "V33_PROXY_WORKER_COMMAND_RECEIVED",
    "V32_PROXY_WORKER_COMMAND_RECEIVED",
)
SAFE_DIRECT_IO_WRITE = False
DEFAULT_MAX_KEYS = 40


# ---------------------------------------------------------------------------
# Basis-Helfer
# ---------------------------------------------------------------------------


def now_epoch() -> float:
    return time.time()


def now_iso_z() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + (".%03dZ" % int((now_epoch() % 1) * 1000))


def make_id(prefix: str) -> str:
    text = safe_str(prefix, "id").strip() or "id"
    return "%s-%s" % (text, uuid.uuid4().hex)


def safe_str(value: Any, default: str = "") -> str:
    try:
        if value is None:
            return str(default)
        return str(value)
    except Exception:
        return str(default)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "yes", "on", "enabled", "y"):
            return True
        if text in ("0", "false", "no", "off", "disabled", "n", ""):
            return False
    return bool(default)


def deepcopy_safe(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def normalize_text(value: Any, default: str = "") -> str:
    text = safe_str(value, default).strip()
    if not text:
        return safe_str(default, "")
    return text


def normalize_operation(value: Any, default: str = "runtime_command") -> str:
    text = normalize_text(value, default).strip().lower()
    if not text:
        text = default
    for ch in ("#", "+", "/", "\\"):
        text = text.replace(ch, "_")
    return text


def payload_from_event(event: Any) -> dict[str, Any]:
    if not isinstance(event, dict):
        return {}
    payload = event.get("payload")
    if isinstance(payload, dict):
        return payload
    envelope = event.get("envelope")
    if isinstance(envelope, dict) and isinstance(envelope.get("payload"), dict):
        return envelope.get("payload")
    return {}


def envelope_from_event(event: Any) -> dict[str, Any]:
    if isinstance(event, dict) and isinstance(event.get("envelope"), dict):
        return event.get("envelope")
    return {}


def operation_from_event(event: Any) -> str:
    if not isinstance(event, dict):
        return "runtime_command"
    candidate = event.get("operation")
    if candidate:
        return normalize_operation(candidate)
    payload = payload_from_event(event)
    for key in ("operation", "command", "action", "method"):
        candidate = payload.get(key)
        if candidate:
            return normalize_operation(candidate)
    envelope = envelope_from_event(event)
    domain = normalize_text(event.get("domain") or envelope.get("domain"), "runtime")
    if domain == "snapshot":
        return "runtime_snapshot"
    return "runtime_command"


# ---------------------------------------------------------------------------
# Resource helpers
# ---------------------------------------------------------------------------


def resource_entry(resources: Any, name: str) -> dict[str, Any] | None:
    if not isinstance(resources, dict):
        return None
    value = resources.get(name)
    if isinstance(value, dict) and "data" in value and "lock" in value:
        return value
    if isinstance(value, dict):
        return {"data": value, "lock": None}
    return None


def read_resource(resources: Any, name: str, deep_copy: bool = True) -> Any:
    entry = resource_entry(resources, name)
    if entry is None:
        return None
    data = entry.get("data")
    lock = entry.get("lock")
    try:
        if lock is not None:
            with lock:
                return deepcopy_safe(data) if deep_copy else data
        return deepcopy_safe(data) if deep_copy else data
    except Exception:
        return None


def ensure_mapping_resource(resources: Any, name: str) -> dict[str, Any] | None:
    entry = resource_entry(resources, name)
    if entry is None:
        return None
    data = entry.get("data")
    if not isinstance(data, dict):
        return None
    return data


def write_resource_key(resources: Any, name: str, key: str, value: Any) -> dict[str, Any]:
    entry = resource_entry(resources, name)
    if entry is None:
        return {"ok": False, "reason": "missing_resource", "resource": name}
    data = entry.get("data")
    lock = entry.get("lock")
    if not isinstance(data, dict):
        return {"ok": False, "reason": "resource_not_mapping", "resource": name}
    key_text = normalize_text(key, "")
    if not key_text:
        return {"ok": False, "reason": "empty_key", "resource": name}
    try:
        if lock is not None:
            with lock:
                previous = deepcopy_safe(data.get(key_text))
                data[key_text] = deepcopy_safe(value)
                current = deepcopy_safe(data.get(key_text))
        else:
            previous = deepcopy_safe(data.get(key_text))
            data[key_text] = deepcopy_safe(value)
            current = deepcopy_safe(data.get(key_text))
        return {"ok": True, "resource": name, "key": key_text, "previous": previous, "current": current}
    except Exception as exc:
        return {"ok": False, "reason": "write_failed:%s" % exc, "resource": name, "key": key_text}


def append_event_store(resources: Any, event_name: str, item: dict[str, Any]) -> dict[str, Any]:
    entry = resource_entry(resources, "event_store")
    if entry is None:
        return {"ok": False, "reason": "missing_event_store"}
    data = entry.get("data")
    lock = entry.get("lock")
    if not isinstance(data, dict):
        return {"ok": False, "reason": "event_store_not_mapping"}
    event_key = normalize_text(event_name, "proxy_runtime_commands") or "proxy_runtime_commands"
    try:
        record = deepcopy_safe(item)
        record.setdefault("recorded_at", now_iso_z())
        if lock is not None:
            with lock:
                values = data.setdefault(event_key, [])
                if not isinstance(values, list):
                    values = []
                    data[event_key] = values
                values.append(record)
                index = len(values) - 1
        else:
            values = data.setdefault(event_key, [])
            if not isinstance(values, list):
                values = []
                data[event_key] = values
            values.append(record)
            index = len(values) - 1
        return {"ok": True, "event_key": event_key, "index": index, "record": record}
    except Exception as exc:
        return {"ok": False, "reason": "append_failed:%s" % exc, "event_key": event_key}


def runtime_command_state(resources: Any) -> dict[str, Any] | None:
    data = ensure_mapping_resource(resources, "proxy_runtime_command_state")
    if data is None:
        return None
    data.setdefault("commands", {})
    data.setdefault("control_methods", {})
    data.setdefault("parameters", {})
    data.setdefault("safe_runtime_commands", [])
    data.setdefault("audit", [])
    data.setdefault("last_update", None)
    return data


def update_runtime_command_state(resources: Any, event: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    state = runtime_command_state(resources)
    if state is None:
        return {"ok": False, "reason": "missing_proxy_runtime_command_state"}
    envelope = envelope_from_event(event)
    request_id = normalize_text(event.get("request_id") or envelope.get("request_id") or make_id("request"), "")
    operation = operation_from_event(event)
    record = {
        "request_id": request_id,
        "operation": operation,
        "domain": normalize_text(event.get("domain") or envelope.get("domain"), "runtime"),
        "worker_id": normalize_text(event.get("worker_id"), ""),
        "correlation_id": event.get("correlation_id") or envelope.get("correlation_id"),
        "status": result.get("status"),
        "result": deepcopy_safe(result),
        "updated_at": now_iso_z(),
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
    }
    try:
        state["commands"][request_id] = record
        state["last_update"] = record["updated_at"]
        audit = state.setdefault("audit", [])
        if isinstance(audit, list):
            audit.append(record)
            max_audit = 1000
            if len(audit) > max_audit:
                del audit[:len(audit) - max_audit]
        return {"ok": True, "request_id": request_id, "record": record}
    except Exception as exc:
        return {"ok": False, "reason": "runtime_command_state_update_failed:%s" % exc}


# ---------------------------------------------------------------------------
# Snapshot / state read
# ---------------------------------------------------------------------------


def allowed_runtime_resource_names() -> tuple[str, ...]:
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
        "proxy_runtime_command_state",
        "cpu_probe_information",
    )


def mapping_summary(value: Any, max_keys: int = DEFAULT_MAX_KEYS) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"kind": type(value).__name__, "mapping": False}
    keys = sorted([safe_str(key, "") for key in value.keys()])
    limit = max(0, int(max_keys))
    return {
        "kind": "dict",
        "mapping": True,
        "count": len(keys),
        "keys": keys[:limit],
        "truncated": len(keys) > limit,
    }


def value_by_path(value: Any, key: Any, default: Any = None) -> Any:
    if not isinstance(value, dict):
        return default
    key_text = normalize_text(key, "")
    if not key_text:
        return deepcopy_safe(value)
    current = value
    for part in [item for item in key_text.split(".") if item]:
        if not isinstance(current, dict):
            return default
        current = current.get(part, default)
    return deepcopy_safe(current)


def build_resource_snapshot(resources: Any, include_full: bool = False, max_keys: int = DEFAULT_MAX_KEYS) -> dict[str, Any]:
    snapshot = {}
    for name in allowed_runtime_resource_names():
        value = read_resource(resources, name, deep_copy=True)
        if value is None:
            continue
        if include_full and name != "config_data":
            snapshot[name] = value
        else:
            snapshot[name] = mapping_summary(value, max_keys=max_keys)
    return snapshot


def build_state_read_result(resources: Any, payload: dict[str, Any]) -> dict[str, Any]:
    requested_resource = normalize_text(payload.get("resource") or payload.get("resource_name") or "worker_state", "worker_state")
    if requested_resource not in allowed_runtime_resource_names():
        return {"status": "rejected", "reason": "resource_not_allowed", "resource": requested_resource}
    resource_value = read_resource(resources, requested_resource, deep_copy=True)
    key = payload.get("key") or payload.get("path")
    include_full = safe_bool(payload.get("include_full"), False)
    result = {
        "status": "ok",
        "resource": requested_resource,
        "summary": mapping_summary(resource_value),
        "runtime_view": {
            "binding_version": BINDING_VERSION,
            "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
        },
    }
    if key:
        result["key"] = safe_str(key, "")
        result["value"] = value_by_path(resource_value, key)
    elif include_full and requested_resource != "config_data":
        result["value"] = resource_value
    return result


def build_runtime_snapshot(resources: Any, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    return {
        "status": "ok",
        "snapshot_domain": normalize_text(payload.get("snapshot_domain") or "runtime", "runtime"),
        "created_at": now_iso_z(),
        "binding_version": BINDING_VERSION,
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
        "resources": build_resource_snapshot(
            resources,
            include_full=safe_bool(payload.get("include_full"), False),
            max_keys=safe_int(payload.get("max_keys"), DEFAULT_MAX_KEYS),
        ),
    }


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def allowed_control_methods(resources: Any, payload: dict[str, Any]) -> tuple[str, ...] | None:
    explicit = payload.get("allowed_methods")
    if isinstance(explicit, list):
        values = tuple(normalize_text(item, "") for item in explicit if normalize_text(item, ""))
        return values if values else None
    user_config = read_resource(resources, "user_config", deep_copy=True)
    if isinstance(user_config, dict):
        policy = user_config.get("runtime_command_policy")
        if isinstance(policy, dict) and isinstance(policy.get("allowed_control_methods"), list):
            values = tuple(normalize_text(item, "") for item in policy.get("allowed_control_methods") if normalize_text(item, ""))
            return values if values else None
    return (
        "auto",
        "automatic",
        "manual",
        "off",
        "disabled",
        "maintenance",
        "service",
        "simulation",
        "standby",
        "safe",
    )


def handle_echo(resources: Any, event: dict[str, Any], payload: dict[str, Any], apply_side_effects: bool) -> dict[str, Any]:
    return {
        "status": "accepted",
        "operation": "echo",
        "echo": deepcopy_safe(payload.get("value", payload)),
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
    }


def handle_state_read(resources: Any, event: dict[str, Any], payload: dict[str, Any], apply_side_effects: bool) -> dict[str, Any]:
    result = build_state_read_result(resources, payload)
    return {
        "status": "accepted" if result.get("status") == "ok" else "rejected",
        "operation": "state_read",
        "state": result,
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
    }


def handle_runtime_snapshot(resources: Any, event: dict[str, Any], payload: dict[str, Any], apply_side_effects: bool) -> dict[str, Any]:
    return {
        "status": "accepted",
        "operation": "runtime_snapshot",
        "snapshot": build_runtime_snapshot(resources, payload),
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
    }


def handle_set_control_method(resources: Any, event: dict[str, Any], payload: dict[str, Any], apply_side_effects: bool) -> dict[str, Any]:
    control_method = payload.get("value")
    if control_method is None:
        control_method = payload.get("control_method") or payload.get("method")
    method_text = normalize_text(control_method, "")
    if not method_text:
        return {"status": "rejected", "operation": "set_control_method", "reason": "missing_control_method", "safe_direct_io_write": SAFE_DIRECT_IO_WRITE}
    allowed = allowed_control_methods(resources, payload)
    if allowed is not None and method_text not in allowed:
        return {
            "status": "rejected",
            "operation": "set_control_method",
            "reason": "control_method_not_allowed",
            "control_method": method_text,
            "allowed_methods": list(allowed),
            "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
        }
    write_result = {"ok": True, "skipped": True, "reason": "apply_side_effects_false"}
    if apply_side_effects:
        updated_at = now_iso_z()
        write_result = write_resource_key(resources, "worker_state", "CONTROL_METHOD", method_text)
        write_resource_key(resources, "worker_state", "PROXY_CONTROL_METHOD", method_text)
        write_resource_key(resources, "worker_state", "RUNTIME_CONTROL_METHOD", method_text)
        write_resource_key(resources, "worker_state", "CONTROL_METHOD_UPDATED_AT", updated_at)
        write_resource_key(resources, "worker_state", "RUNTIME_CONTROL_METHOD_UPDATED_AT", updated_at)
        write_resource_key(resources, "worker_state", "RUNTIME_CONTROL_METHOD_SOURCE", BINDING_VERSION)
        state = runtime_command_state(resources)
        if isinstance(state, dict):
            state.setdefault("control_methods", {})["current"] = method_text
            state["control_methods"]["updated_at"] = updated_at
            state["control_methods"]["source"] = BINDING_VERSION
    event_store = append_event_store(
        resources,
        "proxy_runtime_commands",
        {
            "operation": "set_control_method",
            "control_method": method_text,
            "apply_side_effects": bool(apply_side_effects),
            "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
        },
    ) if apply_side_effects else {"ok": True, "skipped": True}
    return {
        "status": "applied_to_validated_runtime_state" if write_result.get("ok") else "queued_for_runtime",
        "operation": "set_control_method",
        "control_method": method_text,
        "write_result": write_result,
        "event_store": event_store,
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
    }


def handle_parameter_update(resources: Any, event: dict[str, Any], payload: dict[str, Any], apply_side_effects: bool) -> dict[str, Any]:
    key = payload.get("parameter_key") or payload.get("key") or payload.get("parameter")
    key_text = normalize_text(key, "")
    if not key_text:
        return {"status": "rejected", "operation": "parameter_update", "reason": "missing_parameter_key", "safe_direct_io_write": SAFE_DIRECT_IO_WRITE}
    value = payload.get("value")
    update_result = {"ok": True, "skipped": True, "reason": "apply_side_effects_false"}
    if apply_side_effects:
        entry = resource_entry(resources, "user_config")
        if entry is None:
            update_result = {"ok": False, "reason": "missing_user_config"}
        else:
            data = entry.get("data")
            lock = entry.get("lock")
            if not isinstance(data, dict):
                update_result = {"ok": False, "reason": "user_config_not_mapping"}
            else:
                try:
                    if lock is not None:
                        with lock:
                            params = data.setdefault("runtime_parameters", {})
                            if not isinstance(params, dict):
                                params = {}
                                data["runtime_parameters"] = params
                            previous = deepcopy_safe(params.get(key_text))
                            params[key_text] = deepcopy_safe(value)
                            current = deepcopy_safe(params.get(key_text))
                    else:
                        params = data.setdefault("runtime_parameters", {})
                        if not isinstance(params, dict):
                            params = {}
                            data["runtime_parameters"] = params
                        previous = deepcopy_safe(params.get(key_text))
                        params[key_text] = deepcopy_safe(value)
                        current = deepcopy_safe(params.get(key_text))
                    updated_at = now_iso_z()
                    update_result = {"ok": True, "parameter_key": key_text, "previous": previous, "current": current, "updated_at": updated_at}
                    worker_state = ensure_mapping_resource(resources, "worker_state")
                    if isinstance(worker_state, dict):
                        runtime_parameters = worker_state.setdefault("RUNTIME_PARAMETERS", {})
                        if not isinstance(runtime_parameters, dict):
                            runtime_parameters = {}
                            worker_state["RUNTIME_PARAMETERS"] = runtime_parameters
                        runtime_parameters[key_text] = {"value": deepcopy_safe(value), "updated_at": updated_at, "source": BINDING_VERSION}
                        worker_state["PROXY_PARAMETERS"] = deepcopy_safe(runtime_parameters)
                        worker_state["RUNTIME_PARAMETER_LAST_UPDATE"] = {"parameter_key": key_text, "value": deepcopy_safe(value), "updated_at": updated_at, "source": BINDING_VERSION}
                    state = runtime_command_state(resources)
                    if isinstance(state, dict):
                        state.setdefault("parameters", {})[key_text] = deepcopy_safe(value)
                        state["parameters_updated_at"] = updated_at
                except Exception as exc:
                    update_result = {"ok": False, "reason": "parameter_update_failed:%s" % exc, "parameter_key": key_text}
    event_store = append_event_store(
        resources,
        "proxy_runtime_commands",
        {
            "operation": "parameter_update",
            "parameter_key": key_text,
            "value": deepcopy_safe(value),
            "apply_side_effects": bool(apply_side_effects),
            "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
        },
    ) if apply_side_effects else {"ok": True, "skipped": True}
    return {
        "status": "applied_to_validated_runtime_state" if update_result.get("ok") else "queued_for_runtime",
        "operation": "parameter_update",
        "parameter_key": key_text,
        "update_result": update_result,
        "event_store": event_store,
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
    }


def handle_safe_runtime_command(resources: Any, event: dict[str, Any], payload: dict[str, Any], operation: str, apply_side_effects: bool) -> dict[str, Any]:
    command_id = make_id("safe-runtime-command")
    record = {
        "command_id": command_id,
        "operation": operation,
        "payload": deepcopy_safe(payload),
        "created_at": now_iso_z(),
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
    }
    state_result = {"ok": True, "skipped": True, "reason": "apply_side_effects_false"}
    if apply_side_effects:
        state = runtime_command_state(resources)
        if isinstance(state, dict):
            values = state.setdefault("safe_runtime_commands", [])
            if isinstance(values, list):
                values.append(record)
                state_result = {"ok": True, "command_id": command_id, "index": len(values) - 1}
            else:
                state_result = {"ok": False, "reason": "safe_runtime_commands_not_list"}
        else:
            state_result = {"ok": False, "reason": "missing_proxy_runtime_command_state"}
        worker_state = ensure_mapping_resource(resources, "worker_state")
        if isinstance(worker_state, dict):
            queue_values = worker_state.setdefault("RUNTIME_SAFE_COMMAND_QUEUE", [])
            if not isinstance(queue_values, list):
                queue_values = []
                worker_state["RUNTIME_SAFE_COMMAND_QUEUE"] = queue_values
            queue_values.append(deepcopy_safe(record))
            worker_state["PROXY_SAFE_RUNTIME_COMMANDS"] = deepcopy_safe(queue_values)
    event_store = append_event_store(resources, "proxy_safe_runtime_commands", record) if apply_side_effects else {"ok": True, "skipped": True}
    return {
        "status": "queued_for_runtime",
        "operation": operation,
        "command_id": command_id,
        "state_result": state_result,
        "event_store": event_store,
        "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
    }


def apply_runtime_command(resources: Any, event: dict[str, Any], apply_side_effects: bool = True, actor: str = "runtime") -> dict[str, Any]:
    payload = payload_from_event(event)
    operation = operation_from_event(event)
    if operation == "echo":
        result = handle_echo(resources, event, payload, apply_side_effects)
    elif operation in ("runtime_snapshot", "snapshot"):
        result = handle_runtime_snapshot(resources, event, payload, apply_side_effects)
    elif operation == "state_read":
        result = handle_state_read(resources, event, payload, apply_side_effects)
    elif operation == "set_control_method":
        result = handle_set_control_method(resources, event, payload, apply_side_effects)
    elif operation == "parameter_update":
        result = handle_parameter_update(resources, event, payload, apply_side_effects)
    elif operation == "safe_runtime_command":
        result = handle_safe_runtime_command(resources, event, payload, operation, apply_side_effects)
    else:
        result = {
            "status": "rejected",
            "operation": operation,
            "reason": "unsupported_operation",
            "safe_direct_io_write": SAFE_DIRECT_IO_WRITE,
        }
    result.setdefault("operation", operation)
    result.setdefault("safe_direct_io_write", SAFE_DIRECT_IO_WRITE)
    result["binding_version"] = BINDING_VERSION
    result["actor"] = normalize_text(actor, "runtime")
    update_runtime_command_state(resources, event, result)
    return result


def record_runtime_command_audit(resources: Any, event: dict[str, Any], runtime_result: dict[str, Any] | None = None, actor: str = "thread_management") -> dict[str, Any]:
    result = runtime_result if isinstance(runtime_result, dict) else None
    if result is None:
        result = apply_runtime_command(resources, event, apply_side_effects=True, actor=actor)
        return {"ok": True, "mode": "applied", "result": result}
    audit_result = update_runtime_command_state(resources, event, result)
    return {"ok": bool(audit_result.get("ok")), "mode": "recorded", "result": result, "audit_result": audit_result}
