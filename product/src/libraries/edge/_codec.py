# -*- coding: utf-8 -*-

import copy

try:
    from src.libraries._heap_sync_final_template import (
        table_pk,
        table_shape,
    )
except Exception:
    table_pk = None
    table_shape = None

from src.libraries.edge._contracts import (
    build_error_contract,
    build_response_contract,
    build_response_route,
    build_route_name,
    make_application_contract,
    normalize_application_contract,
)
from src.libraries.edge._sync import (
    build_config_sync_snapshot,
    build_full_sync_payload,
    build_health_snapshot,
    build_runtime_state_snapshot,
    build_worker_delta_payload,
    read_resource_snapshot,
)


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


def _safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _normalize_json_pointer(path_value):
    path_text = _safe_str(path_value, "").strip()
    if not path_text:
        return "/"
    if not path_text.startswith("/"):
        return "/" + path_text
    return path_text


def _schema_for_table(resources, table_name):
    if not isinstance(resources, dict):
        return None
    schema = resources.get("heap_sync_schema")
    if not isinstance(schema, dict):
        return None
    return schema.get(str(table_name))


def _is_document_table(resources, table_name):
    if table_shape is not None:
        try:
            schema = resources.get("heap_sync_schema") if isinstance(resources, dict) else None
            if isinstance(schema, dict):
                return table_shape(schema, str(table_name)) == "document"
        except Exception:
            pass

    entry = _schema_for_table(resources, table_name)
    if isinstance(entry, dict):
        return _safe_str(entry.get("shape"), "rows") == "document"
    return False


def _table_pk(resources, table_name):
    if table_pk is not None:
        try:
            schema = resources.get("heap_sync_schema") if isinstance(resources, dict) else None
            if isinstance(schema, dict):
                return table_pk(schema, str(table_name))
        except Exception:
            pass

    entry = _schema_for_table(resources, table_name)
    if isinstance(entry, dict):
        pk_name = _safe_str(entry.get("pk"), "").strip()
        if pk_name:
            return pk_name
    return None


def _group_patch_operations(resources, operations):
    grouped = {}

    for item in _as_list(operations):
        if not isinstance(item, dict):
            continue

        table_name = _safe_str(item.get("table"), "").strip()
        if not table_name:
            raise ValueError("typed patch operation requires 'table'")

        op_name = _safe_str(item.get("op") or item.get("kind") or "replace", "replace").strip().lower() or "replace"
        path_value = _normalize_json_pointer(item.get("path") or "/")
        value = _deepcopy_safe(item.get("value"))
        pk_value = item.get("pk")

        document_table = _is_document_table(resources, table_name)
        if document_table:
            key = (table_name, None, "doc_patch")
        else:
            if pk_value is None:
                raise ValueError("rows patch operation requires 'pk' for table {0}".format(table_name))
            key = (table_name, _safe_str(pk_value, "").strip(), "patch")

        grouped.setdefault(key, [])

        op_entry = {"op": op_name, "path": path_value}
        if op_name != "remove":
            op_entry["value"] = value
        grouped[key].append(op_entry)

    commands = []
    for key in sorted(grouped.keys()):
        table_name, pk_value, kind = key
        cmd = {"table": table_name, "kind": kind, "patch": grouped[key]}
        if pk_value is not None:
            cmd["pk"] = pk_value
        commands.append(cmd)
    return commands


def _compile_upsert_commands(resources, body):
    commands = []
    table_name = _safe_str(body.get("table"), "").strip()
    if not table_name:
        raise ValueError("upsert requires body.table")

    document_table = _is_document_table(resources, table_name)
    rows = body.get("rows")
    if rows is None:
        single_row = body.get("row")
        if single_row is not None:
            rows = [single_row]

    if document_table:
        value = body.get("value")
        if value is None:
            value = body.get("document")
        if value is None:
            raise ValueError("document upsert requires body.value or body.document")
        commands.append({"table": table_name, "kind": "doc_replace", "value": _deepcopy_safe(value)})
        return commands

    pk_field = _table_pk(resources, table_name)
    if not pk_field:
        raise ValueError("missing pk field for table {0}".format(table_name))

    for row in _as_list(rows):
        if not isinstance(row, dict):
            continue
        pk_value = row.get(pk_field)
        if pk_value is None:
            pk_value = body.get("pk")
        if pk_value is None:
            raise ValueError("upsert row for table {0} requires pk {1}".format(table_name, pk_field))
        commands.append({
            "table": table_name,
            "pk": _safe_str(pk_value, "").strip(),
            "kind": "upsert",
            "value": _deepcopy_safe(row),
        })

    return commands


def _compile_delete_commands(resources, body):
    commands = []
    table_name = _safe_str(body.get("table"), "").strip()
    if not table_name:
        raise ValueError("delete requires body.table")

    if _is_document_table(resources, table_name):
        commands.append({"table": table_name, "kind": "doc_delete"})
        return commands

    pks = body.get("pks")
    if pks is None:
        pks = [body.get("pk")]

    for pk_value in _as_list(pks):
        if pk_value is None:
            continue
        commands.append({"table": table_name, "pk": _safe_str(pk_value, "").strip(), "kind": "delete"})

    if not commands:
        raise ValueError("delete requires body.pk or body.pks")
    return commands


def _compile_patch_commands(resources, body, allow_raw_admin_mode=False, actor=None):
    actor = actor if isinstance(actor, dict) else {}
    mode = _safe_str(body.get("mode") or body.get("patch_mode") or "typed", "typed").strip().lower() or "typed"

    if mode == "raw":
        if not allow_raw_admin_mode:
            raise PermissionError("raw config mode is disabled")

        roles = set(_safe_str(item, "").strip().lower() for item in _as_list(actor.get("roles")))
        scopes = set(_safe_str(item, "").strip().lower() for item in _as_list(actor.get("scopes")))
        if not roles.intersection(set(("admin", "superuser", "master", "gateway_proxy"))) and "config:raw" not in scopes:
            raise PermissionError("raw config mode requires admin role or config:raw scope")

        commands = body.get("commands")
        if not isinstance(commands, list) or not commands:
            raise ValueError("raw mode requires body.commands")
        return _deepcopy_safe(commands)

    operations = body.get("operations")
    if not isinstance(operations, list) or not operations:
        raise ValueError("typed patch requires body.operations")

    return _group_patch_operations(resources, operations)


def _build_direct_snapshot_response(ctx, request_envelope, contract):
    resource = _safe_str(contract.get("resource"), "worker").strip().lower()
    body = contract.get("body") if isinstance(contract.get("body"), dict) else {}
    sections = body.get("sections")

    if resource == "config":
        response_body = build_config_sync_snapshot(ctx.get("resources"), node_id=ctx.get("node_id"))
    elif resource == "state":
        response_body = build_runtime_state_snapshot(ctx.get("resources"), include_local=True)
    elif resource == "health":
        response_body = build_health_snapshot(ctx.get("resources"))
    else:
        response_body = build_full_sync_payload(ctx.get("resources"), node_id=ctx.get("node_id"), sections=sections)

    response_contract = build_response_contract(contract, "ok", body=response_body)
    response_route = build_response_route(request_envelope.get("api_route"), resource, contract.get("operation"))

    return {
        "kind": "direct_response",
        "response_contract": response_contract,
        "response_route": response_route,
        "response_body": response_body,
    }


def build_internal_dispatch_from_request(ctx, envelope, contract, allow_raw_admin_mode=False):
    resources = ctx.get("resources") if isinstance(ctx, dict) else {}
    actor = contract.get("actor") if isinstance(contract.get("actor"), dict) else {}
    resource = _safe_str(contract.get("resource"), "worker").strip().lower()
    operation = _safe_str(contract.get("operation"), "event").strip().lower()
    body = contract.get("body") if isinstance(contract.get("body"), dict) else {}
    request_id = contract.get("request_id")
    correlation_id = contract.get("correlation_id") or request_id

    if resource in ("config", "state", "health", "worker") and operation in ("snapshot", "full_sync", "read", "query", "get"):
        return _build_direct_snapshot_response(ctx, envelope, contract)

    meta = {
        "source": "worker_edge_interface",
        "api_route": envelope.get("api_route"),
        "source_node": envelope.get("source_node"),
        "client_id": envelope.get("client_id"),
        "request_id": request_id,
        "correlation_id": correlation_id,
    }

    if resource == "config":
        if operation == "replace":
            new_config = body.get("new_config")
            if new_config is None:
                new_config = body.get("config")
            if not isinstance(new_config, dict):
                raise ValueError("config replace requires body.new_config or body.config")
            return {
                "kind": "internal_event",
                "event_type": "CONFIG_REPLACE",
                "target": "state_event_management",
                "payload": {
                    "new_config": _deepcopy_safe(new_config),
                    "request_id": request_id,
                    "correlation_id": correlation_id,
                    "actor": _deepcopy_safe(actor),
                    "meta": meta,
                },
                "pending_kind": "config_replace",
            }

        if operation == "upsert":
            commands = _compile_upsert_commands(resources, body)
        elif operation == "delete":
            commands = _compile_delete_commands(resources, body)
        else:
            commands = _compile_patch_commands(resources, body, allow_raw_admin_mode=allow_raw_admin_mode, actor=actor)

        return {
            "kind": "internal_event",
            "event_type": "CONFIG_PATCH",
            "target": "state_event_management",
            "payload": {
                "commands": _deepcopy_safe(commands),
                "ack_mode": _safe_str(body.get("ack_mode") or "heap_ack", "heap_ack"),
                "request_id": request_id,
                "correlation_id": correlation_id,
                "actor": _deepcopy_safe(actor),
                "meta": meta,
            },
            "pending_kind": "config_patch",
        }

    if resource in ("actuator", "control") and operation in ("command", "write", "set", "actuate"):
        actuator_id = body.get("actuator_id")
        control_value = body.get("control_value")
        if actuator_id is None:
            actuator_id = body.get("id")
        if control_value is None and "value" in body:
            control_value = body.get("value")

        if actuator_id is None:
            raise ValueError("actuator command requires body.actuator_id")

        return {
            "kind": "internal_event",
            "event_type": "EDGE_ACTUATOR_COMMAND",
            "target": "thread_management",
            "payload": {
                "actuator_id": actuator_id,
                "control_value": _deepcopy_safe(control_value),
                "request_id": request_id,
                "correlation_id": correlation_id,
                "actor": _deepcopy_safe(actor),
                "source": "worker_edge_interface",
                "source_node": envelope.get("source_node"),
                "client_id": envelope.get("client_id"),
                "meta": meta,
            },
            "pending_kind": "actuator_command",
            "task_uuid": _safe_str(request_id, "").strip() or _safe_str(correlation_id, "").strip(),
        }

    if resource in ("authz", "acl") and operation in ("refresh", "acl_refresh", "authz_refresh"):
        acl_payload = body.get("acl") if isinstance(body.get("acl"), dict) else body
        return {
            "kind": "local_mutation",
            "resource_name": "client_config",
            "mutation": {
                "edge_acl": _deepcopy_safe(acl_payload),
            },
            "response_contract": build_response_contract(contract, "ok", body={"updated": True}),
            "response_route": build_response_route(envelope.get("api_route"), resource, operation),
        }

    raise ValueError("unsupported resource/operation: {0}/{1}".format(resource, operation))


def build_external_contract_from_internal_event(ctx, event):
    event_type = _safe_str(event.get("event_type") if isinstance(event, dict) else "", "").strip().upper()
    payload = event.get("payload") if isinstance(event, dict) and isinstance(event.get("payload"), dict) else {}
    request_id = payload.get("request_id") or payload.get("task_uuid") or event.get("event_id")
    correlation_id = payload.get("correlation_id") or request_id

    if event_type == "CONFIG_CHANGED":
        body = build_worker_delta_payload(event, ctx.get("resources"), node_id=ctx.get("node_id"))
        return {
            "api_route": build_route_name("config", "changed", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="config",
                operation="changed",
                body=body,
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type == "CONFIG_PATCH_APPLIED":
        body = build_worker_delta_payload(event, ctx.get("resources"), node_id=ctx.get("node_id"))
        return {
            "api_route": build_route_name("config", "patch_applied", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="config",
                operation="patch_applied",
                body=body,
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type == "CONFIG_PATCH_REJECTED":
        return {
            "api_route": build_route_name("config", "patch", "response"),
            "contract": build_error_contract(
                make_application_contract(kind="command", resource="config", operation="patch", request_id=request_id, correlation_id=correlation_id),
                payload.get("code") or "config.patch.rejected",
                payload.get("reason") or "config patch rejected",
                detail=payload,
            ),
            "broadcast": False,
        }

    if event_type == "CONFIG_REPLACE_APPLIED":
        body = build_worker_delta_payload(event, ctx.get("resources"), node_id=ctx.get("node_id"))
        return {
            "api_route": build_route_name("config", "replace_applied", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="config",
                operation="replace_applied",
                body=body,
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type == "CONFIG_REPLACE_REJECTED":
        return {
            "api_route": build_route_name("config", "replace", "response"),
            "contract": build_error_contract(
                make_application_contract(kind="command", resource="config", operation="replace", request_id=request_id, correlation_id=correlation_id),
                payload.get("code") or "config.replace.rejected",
                payload.get("reason") or "config replace rejected",
                detail=payload,
            ),
            "broadcast": False,
        }

    if event_type in ("SENSOR_VALUE_UPDATE", "NEW_SENSOR_DATA", "SENSOR_THRESHOLD"):
        return {
            "api_route": build_route_name("telemetry", "sensor", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="telemetry",
                operation="sensor",
                body=_deepcopy_safe(payload),
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type in ("ACTUATOR_VALUE_UPDATE",):
        return {
            "api_route": build_route_name("telemetry", "actuator", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="telemetry",
                operation="actuator",
                body=_deepcopy_safe(payload),
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type in ("ACTUATOR_FEEDBACK",):
        return {
            "api_route": build_route_name("actuator", "result", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="actuator",
                operation="result",
                body=_deepcopy_safe(payload),
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type in ("PROCESS_STATE_CHANGED",):
        return {
            "api_route": build_route_name("state", "changed", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="state",
                operation="changed",
                body=_deepcopy_safe(payload),
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type in ("PSM_NOTIFICATION",):
        return {
            "api_route": build_route_name("state", "notification", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="state",
                operation="notification",
                body=_deepcopy_safe(payload),
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type in ("THREAD_MANAGER_HEARTBEAT", "STATUS_UPDATE") or event_type.startswith("MQTT_"):
        return {
            "api_route": build_route_name("health", "status", "event"),
            "contract": make_application_contract(
                kind="event",
                resource="health",
                operation="status",
                body=_deepcopy_safe(payload),
                request_id=request_id,
                correlation_id=correlation_id,
            ),
            "broadcast": True,
        }

    if event_type in ("ACTUATOR_COMMAND_ACCEPTED",):
        request_contract = make_application_contract(
            kind="command",
            resource="actuator",
            operation="command",
            request_id=request_id,
            correlation_id=correlation_id,
        )
        return {
            "api_route": build_route_name("actuator", "command", "response"),
            "contract": build_response_contract(request_contract, "accepted", body=_deepcopy_safe(payload)),
            "broadcast": False,
        }

    if event_type in ("ACTUATOR_COMMAND_REJECTED",):
        request_contract = make_application_contract(
            kind="command",
            resource="actuator",
            operation="command",
            request_id=request_id,
            correlation_id=correlation_id,
        )
        return {
            "api_route": build_route_name("actuator", "command", "response"),
            "contract": build_error_contract(request_contract, payload.get("code") or "actuator.command.rejected", payload.get("reason") or "actuator command rejected", detail=payload),
            "broadcast": False,
        }

    return None
