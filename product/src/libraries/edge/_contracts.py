# -*- coding: utf-8 -*-

import copy
import time
import uuid

from datetime import datetime, timezone


EDGE_TRANSPORT_VERSION = 1
EDGE_CONTRACT_VERSION = 1

_ALLOWED_KINDS = frozenset(("command", "query", "event", "response", "snapshot"))
_ALLOWED_ACTIONS = frozenset(("read", "write", "delete", "command", "sync", "admin", "event"))


def _now_ts():
    return time.time()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _deepcopy_safe(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _safe_str(value, default=""):
    try:
        if value is None:
            return str(default)
        return str(value)
    except Exception:
        return str(default)


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(sorted(value))
    return [value]


def _normalize_text_list(value):
    items = []
    seen = set()

    for item in _as_list(value):
        text = _safe_str(item, "").strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        items.append(text)

    return items


def _make_id():
    return str(uuid.uuid4())


def build_route_name(resource, operation, suffix):
    res = _safe_str(resource, "worker").strip().lower() or "worker"
    op = _safe_str(operation, "event").strip().lower() or "event"
    sx = _safe_str(suffix, "event").strip().lower() or "event"
    return "worker.{0}.{1}.{2}".format(res, op, sx)


def build_response_route(request_route, default_resource="worker", default_operation="response"):
    route = _safe_str(request_route, "").strip()
    if route.endswith(".request"):
        return route[:-8] + ".response"
    if route.endswith(".query"):
        return route[:-6] + ".response"
    if route.endswith(".command"):
        return route + ".response"
    if route:
        return route + ".response"
    return build_route_name(default_resource, default_operation, "response")


def make_application_contract(
    *,
    kind,
    resource,
    operation,
    body=None,
    request_id=None,
    correlation_id=None,
    actor=None,
    sync=None,
    scope=None,
    meta=None,
    contract_version=EDGE_CONTRACT_VERSION,
):
    normalized_kind = _safe_str(kind, "event").strip().lower() or "event"
    if normalized_kind not in _ALLOWED_KINDS:
        normalized_kind = "event"

    contract = {
        "contract_version": int(contract_version),
        "kind": normalized_kind,
        "resource": _safe_str(resource, "worker").strip().lower() or "worker",
        "operation": _safe_str(operation, "event").strip().lower() or "event",
        "request_id": _safe_str(request_id, "").strip() or _make_id(),
        "correlation_id": _safe_str(correlation_id, "").strip() or _safe_str(request_id, "").strip() or _make_id(),
        "actor": _deepcopy_safe(actor if isinstance(actor, dict) else {}),
        "sync": _deepcopy_safe(sync if isinstance(sync, dict) else {}),
        "scope": _deepcopy_safe(scope if isinstance(scope, dict) else {}),
        "meta": _deepcopy_safe(meta if isinstance(meta, dict) else {}),
        "body": _deepcopy_safe(body if body is not None else {}),
        "ts_utc": _now_iso(),
    }
    return contract


def normalize_application_contract(payload, default_kind="event"):
    if isinstance(payload, dict):
        body = payload.get("body")
        if body is None:
            body = {}

        actor = payload.get("actor") if isinstance(payload.get("actor"), dict) else {}
        sync = payload.get("sync") if isinstance(payload.get("sync"), dict) else {}
        scope = payload.get("scope") if isinstance(payload.get("scope"), dict) else {}
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}

        return make_application_contract(
            kind=payload.get("kind") or default_kind,
            resource=payload.get("resource") or payload.get("target_resource") or "worker",
            operation=payload.get("operation") or payload.get("action") or payload.get("op") or "event",
            body=body,
            request_id=payload.get("request_id") or payload.get("req_id") or payload.get("message_id"),
            correlation_id=payload.get("correlation_id") or payload.get("corr_id") or payload.get("request_id"),
            actor=actor,
            sync=sync,
            scope=scope,
            meta=meta,
            contract_version=payload.get("contract_version") or EDGE_CONTRACT_VERSION,
        )

    return make_application_contract(
        kind=default_kind,
        resource="worker",
        operation="event",
        body={"raw": _deepcopy_safe(payload)},
    )


def validate_application_contract(contract):
    result = {"ok": True, "errors": [], "contract": normalize_application_contract(contract)}
    obj = result["contract"]

    if obj.get("kind") not in _ALLOWED_KINDS:
        result["ok"] = False
        result["errors"].append("invalid kind")

    if not _safe_str(obj.get("resource"), "").strip():
        result["ok"] = False
        result["errors"].append("missing resource")

    if not _safe_str(obj.get("operation"), "").strip():
        result["ok"] = False
        result["errors"].append("missing operation")

    if not _safe_str(obj.get("request_id"), "").strip():
        result["ok"] = False
        result["errors"].append("missing request_id")

    if not _safe_str(obj.get("correlation_id"), "").strip():
        result["ok"] = False
        result["errors"].append("missing correlation_id")

    if not isinstance(obj.get("body"), (dict, list, str, int, float, bool)) and obj.get("body") is not None:
        result["ok"] = False
        result["errors"].append("body must be json-compatible")

    return result


def make_transport_envelope(
    *,
    message_type,
    api_route,
    payload,
    source,
    source_node,
    target_node="",
    target_nodes=None,
    target_roles=None,
    target_capabilities=None,
    broadcast=False,
    destination="",
    message_id=None,
    correlation_id=None,
    meta=None,
    client_id=None,
):
    envelope = {
        "type": _safe_str(message_type, "event").strip().lower() or "event",
        "message_id": _safe_str(message_id, "").strip() or _make_id(),
        "timestamp": _now_ts(),
        "source": _safe_str(source, "worker_edge_interface").strip() or "worker_edge_interface",
        "source_node": _safe_str(source_node, "").strip(),
        "destination": _safe_str(destination, "").strip(),
        "api_route": _safe_str(api_route, "worker.event.event").strip() or "worker.event.event",
        "payload": _deepcopy_safe(payload),
        "meta": _deepcopy_safe(meta if isinstance(meta, dict) else {}),
        "client_id": _safe_str(client_id, "").strip() or None,
        "target_node": _safe_str(target_node, "").strip(),
        "target_nodes": _normalize_text_list(target_nodes),
        "target_roles": _normalize_text_list(target_roles),
        "target_capabilities": _normalize_text_list(target_capabilities),
        "broadcast": bool(broadcast),
    }

    if correlation_id is not None:
        envelope["correlation_id"] = _safe_str(correlation_id, "").strip()

    mqtt_meta = envelope["meta"].get("mqtt") if isinstance(envelope["meta"].get("mqtt"), dict) else {}
    mqtt_meta["standard_version"] = EDGE_TRANSPORT_VERSION
    envelope["meta"]["mqtt"] = mqtt_meta
    return envelope


def normalize_transport_envelope(message, default_source="worker_edge_interface", default_source_node=""):
    if isinstance(message, dict):
        payload = message.get("payload")
        if payload is None:
            reserved = {
                "type",
                "message_id",
                "timestamp",
                "source",
                "source_node",
                "destination",
                "api_route",
                "meta",
                "client_id",
                "target_node",
                "target_nodes",
                "target_role",
                "target_roles",
                "target_capability",
                "target_capabilities",
                "broadcast",
                "exclude_nodes",
            }
            payload = {}
            for key, value in message.items():
                if key in reserved:
                    continue
                payload[key] = _deepcopy_safe(value)
        return make_transport_envelope(
            message_type=message.get("type") or message.get("kind") or "event",
            api_route=message.get("api_route") or message.get("route") or message.get("command") or "worker.event.event",
            payload=payload,
            source=message.get("source") or default_source,
            source_node=message.get("source_node") or default_source_node,
            target_node=message.get("target_node") or "",
            target_nodes=message.get("target_nodes"),
            target_roles=message.get("target_roles") or message.get("target_role"),
            target_capabilities=message.get("target_capabilities") or message.get("target_capability"),
            broadcast=bool(message.get("broadcast", False)),
            destination=message.get("destination") or "",
            message_id=message.get("message_id") or message.get("id"),
            correlation_id=message.get("correlation_id") or message.get("request_id"),
            meta=message.get("meta") if isinstance(message.get("meta"), dict) else {},
            client_id=message.get("client_id"),
        )

    return make_transport_envelope(
        message_type="event",
        api_route="worker.raw.event",
        payload={"raw": _deepcopy_safe(message)},
        source=default_source,
        source_node=default_source_node,
    )


def validate_transport_envelope(envelope):
    result = {"ok": True, "errors": [], "envelope": normalize_transport_envelope(envelope)}
    obj = result["envelope"]

    if not _safe_str(obj.get("message_id"), "").strip():
        result["ok"] = False
        result["errors"].append("missing message_id")

    if not _safe_str(obj.get("api_route"), "").strip():
        result["ok"] = False
        result["errors"].append("missing api_route")

    if obj.get("type") not in _ALLOWED_KINDS:
        result["ok"] = False
        result["errors"].append("invalid transport type")

    if not isinstance(obj.get("payload"), dict):
        result["ok"] = False
        result["errors"].append("payload must be dict")

    return result


def derive_action_from_contract(contract):
    obj = normalize_application_contract(contract)
    operation = _safe_str(obj.get("operation"), "").strip().lower()
    kind = _safe_str(obj.get("kind"), "event").strip().lower()

    if operation in ("patch", "replace", "upsert", "delete", "write", "set", "update"):
        return "write"
    if operation in ("command", "control", "actuate"):
        return "command"
    if operation in ("full_sync", "snapshot", "read", "query", "get"):
        return "read"
    if operation in ("refresh_acl", "acl_refresh", "acl_update", "authz_refresh"):
        return "admin"
    if kind == "query":
        return "read"
    if kind == "command":
        return "write"
    if kind == "snapshot":
        return "sync"
    return "event"


def build_response_contract(request_contract, status, body=None, error=None, meta=None):
    request_obj = normalize_application_contract(request_contract)
    response_body = {}
    if isinstance(body, dict):
        response_body.update(_deepcopy_safe(body))
    elif body is not None:
        response_body["value"] = _deepcopy_safe(body)

    response_body["status"] = _safe_str(status, "ok").strip().lower() or "ok"
    if error is not None:
        response_body["error"] = _deepcopy_safe(error)

    response_meta = _deepcopy_safe(meta if isinstance(meta, dict) else {})
    response_meta["response_to"] = request_obj.get("request_id")

    return make_application_contract(
        kind="response",
        resource=request_obj.get("resource") or "worker",
        operation=request_obj.get("operation") or "response",
        body=response_body,
        request_id=request_obj.get("request_id"),
        correlation_id=request_obj.get("correlation_id") or request_obj.get("request_id"),
        actor={},
        sync=request_obj.get("sync") if isinstance(request_obj.get("sync"), dict) else {},
        scope=request_obj.get("scope") if isinstance(request_obj.get("scope"), dict) else {},
        meta=response_meta,
    )


def build_error_contract(request_contract, code, message, detail=None, meta=None):
    error_body = {
        "status": "error",
        "error": {
            "code": _safe_str(code, "edge.error"),
            "message": _safe_str(message, "unknown error"),
            "detail": _deepcopy_safe(detail),
        },
    }
    return build_response_contract(request_contract, "error", body=error_body, error=error_body["error"], meta=meta)
