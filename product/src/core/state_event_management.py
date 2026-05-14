# -*- coding: utf-8 -*-

# src/core/state_event_management.py


"""
State Event Management (SEM) — v4.0 FINAL (Synchronous, heap_sync native)
---------------------------------------------------------------------------

Responsibility
~~~~~~~~~~~~~~
Central, event-driven **single-writer** for the master configuration.
Every config mutation in the entire system flows exclusively through SEM.

Incoming events (from IO-Event-Broker via ``queue_event_state``):

    CONFIG_PATCH            heap_sync commands   -> apply on node
    CONFIG_REPLACE          full dataset replace  -> rebuild node
    CONFIG_CHANGED          external notification -> refresh snapshot
    INITIAL_FULL_SYNC       bulk sync request     -> refresh + broadcast
    INITIAL_FULL_SYNC_DONE  telemetry
    ACTUATOR_FEEDBACK       value store write (non-config)
    ACTUATOR_VALUE_UPDATE   value store write (non-config)

Outgoing events (to IO-Event-Broker via ``queue_event_send``):

    CONFIG_CHANGED          target=automation   (PSM, TM, Datastore, UI)
    CONFIG_PATCH_APPLIED    target=thread_management
    CONFIG_REPLACE_APPLIED  target=thread_management
    DATA_LOAD_ERROR         target=datastore    (schema/dataset failure)

Non-Responsibilities
~~~~~~~~~~~~~~~~~~~~
- Thread-replica management              -> TM
- Controller / automation lifecycle      -> TM
- External interface / transport logic   -> Gateway / WebSocket
- JSON file persistence                  -> Datastore
- Process-state mutation logic           -> PSM

Architecture
~~~~~~~~~~~~
- No OOP — dict-based context only.
- ``functools.partial`` instead of ``lambda``.
- All data operations via heap_sync single-node cluster.
- Blocking queue ingress — CPU idle < 1 %.
- Sync event bus (``create_sync_event_bus``).
- C/C11 migration friendly: flat dicts, explicit state, no magic.

Patch Format (the ONLY accepted format)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
::

    payload = {
        "commands": [
            {
                "table":  "process_states",
                "pk":     "5",
                "kind":   "patch",
                "patch":  [
                    {"op": "replace", "path": "/value_id", "value": 2}
                ]
            }
        ],
        "ack_mode": "heap_ack"
    }
"""

import logging
import os
import tempfile
import threading
import time

from functools import partial
from datetime import datetime, timezone

try:
    from src.libraries._evt_interface import (
        create_sync_event_bus,
        sync_queue_put,
        sync_queue_get,
        make_event,
    )
except Exception:
    from _evt_interface import (
        create_sync_event_bus,
        sync_queue_put,
        sync_queue_get,
        make_event,
    )

try:
    from src.libraries._heap_sync_final_template import (
        make_schema_entry,
        make_document_schema_entry,
        infer_schema_from_dataset,
        make_cluster,
        stop_cluster,
        leader_node,
        submit_command,
        next_command_id,
        export_node_dataset,
        export_node_dataset_cached,
        make_snapshot_cache,
        deepcopy_json,
    )
except Exception:
    from _heap_sync_final_template import (
        make_schema_entry,
        make_document_schema_entry,
        infer_schema_from_dataset,
        make_cluster,
        stop_cluster,
        leader_node,
        submit_command,
        next_command_id,
        export_node_dataset,
        export_node_dataset_cached,
        make_snapshot_cache,
        deepcopy_json,
    )


try:
    from src.core._replication_commit_validator import (
        build_config_commit_validation_report,
        format_validation_report,
        preview_candidate_config_from_patch,
    )
except Exception:
    try:
        from _replication_commit_validator import (
            build_config_commit_validation_report,
            format_validation_report,
            preview_candidate_config_from_patch,
        )
    except Exception:
        build_config_commit_validation_report = None
        format_validation_report = None
        preview_candidate_config_from_patch = None


logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

SENTINEL = {"_type": "__STOP__", "_sentinel": True}

_SCHEMA_INIT_MAX_RETRIES = 2
_SCHEMA_INIT_RETRY_DELAY_S = 1.0

_PK_OVERRIDES = {
    "local_node": "node_id", "event_types": "event_id",
    "modbus_functions": "modbus_function_id", "scaling_profiles": "profile_id",
    "controllers": "controller_id", "virtual_controllers": "virt_controller_id",
    "sensors": "sensor_id", "actuators": "actuator_id",
    "sensor_types": "sensor_types_id", "actuator_types": "actuator_types_id",
    "parameter_generic_templates": "schema_id", "schema_mappings": "mapping_id",
    "processes": "p_id", "process_values": "value_id",
    "process_modes": "mode_id", "pid_configs": "pid_config_id",
    "control_method_templates": "template_id", "control_methods": "method_id",
    "process_states": "state_id", "rule_modes": "rule_mode_id",
    "automations": "automation_id", "automation_engines": "engine_id",
    "automation_debug": "engine_id", "automation_filters": "filter_id",
    "automation_rule_sets": "rule_id", "timers": "timer_id",
    "events": "event_id", "triggers": "trigger_id",
    "actions": "action_id", "transitions": "transition_id",
    "dynamic_rule_engine": "rule_id", "rule_controls": "rule_controls_id",
}

_MODE_OVERRIDES = {
    "event_types": "readonly", "modbus_functions": "readonly",
    "scaling_profiles": "readonly", "sensor_types": "readonly",
    "actuator_types": "readonly", "parameter_generic_templates": "readonly",
    "schema_mappings": "readonly", "process_values": "readonly",
    "process_modes": "readonly", "pid_configs": "readonly",
    "control_method_templates": "readonly", "rule_modes": "readonly",
}

_SHAPE_OVERRIDES = {"rule_controls": "rows"}


# =============================================================================
# Pure utility functions
# =============================================================================


def _is_stop_message(obj):
    if not isinstance(obj, dict):
        return False
    return obj.get("_type") == "__STOP__" and obj.get("_sentinel") is True


def _safe_int(val, default=None):
    try:
        return int(val)
    except Exception:
        return default


def _safe_str(val, default=""):
    try:
        return str(val)
    except Exception:
        return str(default)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# Schema construction — deterministic, tolerant
# =============================================================================


def _build_schema_from_dataset(dataset):
    """
    Build heap_sync schema.  Uses hardcoded PK/mode/shape overrides.
    Tolerant: skips tables where PK cannot be resolved rather than crashing.
    """
    schema = {}
    skipped = []

    for table_name, value in dataset.items():
        mode = _MODE_OVERRIDES.get(table_name, "mutable")
        shape_override = _SHAPE_OVERRIDES.get(table_name)

        if shape_override is not None:
            shape = shape_override
        elif isinstance(value, list) and all(isinstance(r, dict) for r in value):
            shape = "rows"
        else:
            shape = "document"

        if shape == "document":
            schema[table_name] = make_document_schema_entry(mode=mode)
            continue

        pk_field = _PK_OVERRIDES.get(table_name)

        # Validate PK exists in data
        if pk_field is not None and isinstance(value, list) and len(value) > 0:
            if isinstance(value[0], dict) and pk_field in value[0]:
                schema[table_name] = make_schema_entry(pk=pk_field, mode=mode, shape="rows")
                continue

        # PK missing or unknown table — attempt inference
        try:
            inferred = infer_schema_from_dataset(
                {table_name: value},
                pk_overrides={},
                mode_overrides={table_name: mode},
                shape_overrides={table_name: shape} if shape_override else {},
                default_mode=mode,
            )
            if table_name in inferred:
                schema[table_name] = inferred[table_name]
                logger.info("SEM: Table %r PK inferred as %r", table_name, inferred[table_name].get("pk"))
                continue
        except Exception as exc:
            logger.warning("SEM: Schema inference failed for %r: %s", table_name, exc)

        skipped.append(table_name)

    if skipped:
        logger.warning("SEM: Skipped tables (no valid PK): %s", skipped)

    return schema


# =============================================================================
# Heap-Sync cluster lifecycle
# =============================================================================


def _init_heap_sync_cluster(dataset, node_id, root_dir=None):
    if root_dir is None:
        root_dir = os.path.join(tempfile.gettempdir(), "heap_sync_sem_{}".format(node_id or "sem"))

    data = deepcopy_json(dataset)
    schema = _build_schema_from_dataset(data)

    if not schema:
        raise RuntimeError("SEM: Empty schema — cannot start cluster")

    cluster = make_cluster(
        root_dir=str(root_dir),
        node_ids=[str(node_id or "sem")],
        leader_id=str(node_id or "sem"),
        schema=schema,
        shard_count=2,
        base_dataset=data,
        snapshot_every=5,
        enable_replication=False,
        prepare_timeout_s=2.0,
        shard_queue_capacity=4096,
        persist_queue_capacity=16384,
        subscriber_capacity=1024,
    )
    logger.info("SEM: Cluster started (node=%s tables=%d shards=2)", node_id or "sem", len(schema))
    return cluster, schema


def _try_init_cluster_with_retry(dataset, node_id, queue_event_send):
    """Init with retry.  On final failure emit DATA_LOAD_ERROR for Datastore resend."""
    last_exc = None
    for attempt in range(_SCHEMA_INIT_MAX_RETRIES + 1):
        try:
            return _init_heap_sync_cluster(dataset, node_id)
        except Exception as exc:
            last_exc = exc
            logger.error("SEM: Cluster init attempt %d/%d failed: %s", attempt + 1, _SCHEMA_INIT_MAX_RETRIES + 1, exc)
            if attempt < _SCHEMA_INIT_MAX_RETRIES:
                time.sleep(_SCHEMA_INIT_RETRY_DELAY_S)

    logger.error("SEM: All cluster init retries exhausted — requesting data resend")
    if queue_event_send is not None:
        evt = make_event(
            "DATA_LOAD_ERROR",
            payload={"reason": "schema_init_failed", "error": str(last_exc), "timestamp": _now_iso()},
            target="datastore",
        )
        sync_queue_put(queue_event_send, evt)

    return None, None


# =============================================================================
# Snapshot management
# =============================================================================


def _get_master_dataset(ctx):
    cluster = ctx.get("heap_sync_cluster")
    if cluster is None:
        return {}
    node = leader_node(cluster)
    cache = ctx.get("snapshot_cache")
    if cache is not None:
        return export_node_dataset_cached(node, cache)
    return export_node_dataset(node)


def _rebuild_indexes(snapshot):
    indexes = {}
    cbi = {}
    for rec in snapshot.get("controllers") or []:
        if isinstance(rec, dict):
            cid = _safe_int(rec.get("controller_id"))
            if cid is not None:
                cbi[cid] = rec
    indexes["controllers_by_id"] = cbi
    return indexes


def _update_snapshot(ctx):
    new_snapshot = _get_master_dataset(ctx)
    ctx["config_snapshot"] = new_snapshot
    ctx["config_snapshot_version"] = int(ctx.get("config_snapshot_version", 0)) + 1
    ctx["config_snapshot_ts"] = time.time()
    ctx["config_indexes"] = _rebuild_indexes(new_snapshot)
    _sync_to_legacy_resource(ctx)
    return new_snapshot


def cfg_snapshot(ctx):
    snap = ctx.get("config_snapshot")
    if snap is None:
        snap = _update_snapshot(ctx)
    return snap


def _sync_to_legacy_resource(ctx):
    resources = ctx.get("resources")
    if not isinstance(resources, dict):
        return
    config_entry = resources.get("config_data")
    if not isinstance(config_entry, dict):
        return
    lock = config_entry.get("lock")
    snapshot = ctx.get("config_snapshot") or {}
    if lock is not None:
        with lock:
            config_entry["data"] = deepcopy_json(snapshot)
    else:
        config_entry["data"] = deepcopy_json(snapshot)


# =============================================================================
# Command execution
# =============================================================================


def _execute_commands(ctx, commands, ack_mode="heap_ack"):
    cluster = ctx.get("heap_sync_cluster")
    if cluster is None:
        return 0, len(commands), []
    applied = 0
    errors = 0
    results = []
    for cmd in commands:
        try:
            if "command_id" not in cmd:
                cmd["command_id"] = next_command_id(cluster, cluster["leader_id"])
            result = submit_command(cluster, cmd, ack_mode=ack_mode)
            applied += 1
            results.append(result)
        except Exception as exc:
            errors += 1
            logger.error("SEM: Command failed (table=%s pk=%s): %s", cmd.get("table"), cmd.get("pk"), exc)
    return applied, errors, results


def _store_commit_validation_report(ctx, report, source):
    if not isinstance(ctx, dict):
        return
    if isinstance(report, dict):
        ctx["last_config_validation_report"] = deepcopy_json(report)
    else:
        ctx["last_config_validation_report"] = None
    ctx["last_config_validation_source"] = source
    ctx["last_config_validation_ts"] = _now_iso()

    if isinstance(report, dict) and report.get("ok"):
        ctx["config_validation_successes"] = int(ctx.get("config_validation_successes", 0)) + 1
        return

    ctx["config_validation_failures"] = int(ctx.get("config_validation_failures", 0)) + 1


def _format_commit_validation_message(report, fallback_message):
    if isinstance(report, dict) and format_validation_report is not None:
        try:
            return format_validation_report(report)
        except Exception:
            pass
    return str(fallback_message)


def _make_commit_validation_error_report(message, source, detail=None):
    report = {
        "ok": False,
        "node_id": None,
        "effective_node_id": None,
        "owned_controller_ids": [],
        "errors": [
            {
                "code": "commit.preview_failed",
                "message": str(message),
                "table": None,
                "pk": None,
                "field": None,
                "ref": detail,
            }
        ],
        "warnings": [],
        "summary": {
            "errors": 1,
            "warnings": 0,
        },
        "source": source,
    }
    return report


def _validate_replace_candidate(ctx, new_config, source):
    if build_config_commit_validation_report is None:
        return True

    report = build_config_commit_validation_report(
        new_config,
        node_id=ctx.get("node_id"),
    )
    report["source"] = source
    _store_commit_validation_report(ctx, report, source)

    if report.get("ok"):
        return True

    logger.error(
        "SEM: Reject %s — commit contract violated:\n%s",
        source,
        _format_commit_validation_message(report, "invalid config"),
    )
    return False


def _validate_patch_candidate(ctx, commands, source):
    if build_config_commit_validation_report is None or preview_candidate_config_from_patch is None:
        return True

    current_snapshot = cfg_snapshot(ctx)
    schema = ctx.get("heap_sync_schema")

    try:
        candidate = preview_candidate_config_from_patch(
            current_snapshot if isinstance(current_snapshot, dict) else {},
            schema,
            deepcopy_json(commands),
        )
    except Exception as exc:
        report = _make_commit_validation_error_report(
            "Patch preview failed before commit: {0}".format(exc),
            source,
            detail=_safe_str(exc, ""),
        )
        _store_commit_validation_report(ctx, report, source)
        logger.error(
            "SEM: Reject %s — preview failed before commit: %s",
            source,
            exc,
        )
        return False

    report = build_config_commit_validation_report(
        candidate,
        node_id=ctx.get("node_id"),
    )
    report["source"] = source
    _store_commit_validation_report(ctx, report, source)

    if report.get("ok"):
        return True

    logger.error(
        "SEM: Reject %s — commit contract violated:\n%s",
        source,
        _format_commit_validation_message(report, "invalid config"),
    )
    return False


# =============================================================================
# Value-store helpers (actuator/sensor — NOT config)
# =============================================================================


def _ensure_value_resource(resources, name):
    if not isinstance(resources, dict):
        return
    entry = resources.get(name)
    if entry is None or not isinstance(entry, dict):
        resources[name] = {"data": {}, "lock": threading.RLock()}
        return
    if "data" not in entry or entry["data"] is None:
        entry["data"] = {}
    if "lock" not in entry or entry["lock"] is None:
        entry["lock"] = threading.RLock()


def _get_value_dict_and_lock(resources, name):
    entry = resources.get(name) or {}
    data = entry.get("data")
    lock = entry.get("lock")
    return data if data is not None else {}, lock if lock is not None else threading.RLock()


# =============================================================================
# Event emission (synchronous)
# =============================================================================


def _emit_config_changed(ctx, kind, version, extra_payload=None):
    payload = {"kind": kind, "version": version, "timestamp": _now_iso()}
    if isinstance(extra_payload, dict):
        payload.update(extra_payload)
    sync_queue_put(ctx.get("queue_event_send"), make_event("CONFIG_CHANGED", payload=payload, target="automation"))


def _emit_patch_applied(ctx, commands, version):
    sync_queue_put(ctx.get("queue_event_send"), make_event(
        "CONFIG_PATCH_APPLIED",
        payload={"commands": deepcopy_json(commands), "version": version, "timestamp": _now_iso()},
        target="thread_management",
    ))


def _emit_replace_applied(ctx, version):
    sync_queue_put(ctx.get("queue_event_send"), make_event(
        "CONFIG_REPLACE_APPLIED",
        payload={"version": version, "timestamp": _now_iso()},
        target="thread_management",
    ))


def _emit_full_sync_events(ctx, scope, affected, version):
    ts = _now_iso()
    affected_list = sorted(list(affected)) if affected else None
    sync_queue_put(ctx.get("queue_event_send"), make_event(
        "CONFIG_CHANGED",
        payload={"kind": "initial_full_sync", "version": version, "timestamp": ts},
        target="automation",
    ))
    sync_queue_put(ctx.get("queue_event_send"), make_event(
        "INITIAL_FULL_SYNC_DONE",
        payload={"scope": scope, "affected": affected_list, "version": version, "timestamp": ts},
        target="automation",
    ))


# =============================================================================
# Event handlers
# =============================================================================


def handle_config_patch(ctx, event):
    """CONFIG_PATCH — ONLY heap_sync command format accepted."""
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    if not bool(ctx.get("master_sync_enabled", True)):
        _update_snapshot(ctx)
        return
    commands = payload.get("commands")
    if not commands or not isinstance(commands, list):
        logger.info("SEM: CONFIG_PATCH without valid commands — ignored")
        return
    if not _validate_patch_candidate(ctx, commands, "CONFIG_PATCH"):
        return
    ack_mode = str(payload.get("ack_mode", "heap_ack"))
    applied, errors, results = _execute_commands(ctx, commands, ack_mode=ack_mode)
    _update_snapshot(ctx)
    if applied == 0:
        return
    version = int(ctx.get("config_snapshot_version", 0))
    _emit_config_changed(ctx, "patch", version, {"applied": applied, "errors": errors})
    _emit_patch_applied(ctx, commands, version)
    logger.info("SEM: PATCH applied (cmds=%d ok=%d err=%d v=%d)", len(commands), applied, errors, version)


def handle_config_replace(ctx, event):
    """CONFIG_REPLACE — full rebuild with retry and resend on failure."""
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    new_cfg = payload.get("new_config")
    if not isinstance(new_cfg, dict):
        logger.info("SEM: CONFIG_REPLACE without valid new_config — ignored")
        return
    if not bool(ctx.get("master_sync_enabled", True)):
        _update_snapshot(ctx)
        return
    if not _validate_replace_candidate(ctx, new_cfg, "CONFIG_REPLACE"):
        return
    old_cluster = ctx.get("heap_sync_cluster")
    cluster, schema = _try_init_cluster_with_retry(new_cfg, ctx.get("node_id", "sem"), ctx.get("queue_event_send"))
    if cluster is None:
        return
    ctx["heap_sync_cluster"] = cluster
    ctx["heap_sync_schema"] = schema
    ctx["snapshot_cache"] = make_snapshot_cache()
    resources = ctx.get("resources")
    if isinstance(resources, dict):
        resources["heap_sync_cluster"] = cluster
        resources["heap_sync_schema"] = schema
    _update_snapshot(ctx)
    if old_cluster is not None:
        try:
            stop_cluster(old_cluster, timeout=1.0)
        except Exception:
            pass
    version = int(ctx.get("config_snapshot_version", 0))
    _emit_config_changed(ctx, "replace", version)
    _emit_replace_applied(ctx, version)
    logger.info("SEM: REPLACE applied (v=%d)", version)


def handle_config_changed(ctx, event):
    _update_snapshot(ctx)


def handle_initial_full_sync(ctx, event):
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    scope = _safe_str(payload.get("scope") or "global", "global")
    raw = payload.get("affected")
    affected = {_safe_int(x) for x in raw if _safe_int(x) is not None} if isinstance(raw, (list, set, tuple)) else None
    _update_snapshot(ctx)
    _emit_full_sync_events(ctx, scope, affected, int(ctx.get("config_snapshot_version", 0)))


def handle_initial_full_sync_done(ctx, event):
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    logger.info("SEM: FULL_SYNC_DONE (scope=%s v=%s)", payload.get("scope"), payload.get("version"))


def _store_actuator_entry(ctx, aid_i, entry):
    lock = ctx.get("actuator_lock")
    store = ctx.get("actuator_values")
    if store is None:
        return
    if lock is not None:
        with lock:
            store[str(aid_i)] = entry
    else:
        store[str(aid_i)] = entry


def handle_actuator_value_update(ctx, event):
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    aid_i = _safe_int(payload.get("actuator_id")) if isinstance(payload, dict) else None
    if aid_i is None:
        return
    _store_actuator_entry(ctx, aid_i, {
        "value": payload.get("value"), "readback": payload.get("readback"),
        "success": bool(payload.get("success", True)), "controller_id": payload.get("controller_id"),
        "address": payload.get("address"), "function_type": payload.get("function_type"),
        "timestamp": payload.get("timestamp") or _now_iso(),
    })


def handle_actuator_feedback(ctx, event):
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    aid_i = _safe_int(payload.get("actuator_id")) if isinstance(payload, dict) else None
    if aid_i is None:
        return
    _store_actuator_entry(ctx, aid_i, {
        "value": payload.get("command_value"), "readback": payload.get("readback"),
        "success": bool(payload.get("success", True)), "controller_id": payload.get("controller_id"),
        "address": payload.get("address"), "function_type": payload.get("function_type"),
        "timestamp": payload.get("timestamp") or _now_iso(),
    })


def handle_generic_event(ctx, event):
    logger.debug("SEM: Unhandled: %s", _safe_str(event.get("event_type") or "", ""))


# =============================================================================
# Handler registration
# =============================================================================


def setup_sem_event_handlers(ctx):
    bus = ctx.get("event_bus")
    register = bus["register_handler"]
    register("CONFIG_PATCH",           partial(handle_config_patch, ctx))
    register("CONFIG_REPLACE",         partial(handle_config_replace, ctx))
    register("CONFIG_CHANGED",         partial(handle_config_changed, ctx))
    register("INITIAL_FULL_SYNC",      partial(handle_initial_full_sync, ctx))
    register("INITIAL_FULL_SYNC_DONE", partial(handle_initial_full_sync_done, ctx))
    register("ACTUATOR_FEEDBACK",      partial(handle_actuator_feedback, ctx))
    register("ACTUATOR_VALUE_UPDATE",  partial(handle_actuator_value_update, ctx))
    bus["register_fallback"](partial(handle_generic_event, ctx))


# =============================================================================
# Context creation
# =============================================================================


def create_sem_context(
    *,
    resources,
    queue_event_send,
    queue_event_state,
    shutdown_event=None,
    node_id=None,
    master_sync_enabled=True,
):
    _ensure_value_resource(resources, "sensor_values")
    _ensure_value_resource(resources, "actuator_values")

    config_entry = resources.get("config_data") or {}
    initial_data = config_entry.get("data") or {}

    startup_validation_report = None
    if initial_data and isinstance(initial_data, dict) and len(initial_data) > 0 and build_config_commit_validation_report is not None:
        try:
            startup_validation_report = build_config_commit_validation_report(initial_data, node_id=node_id or "sem")
            if not startup_validation_report.get("ok"):
                # Strict mode toggle via runtime_config (resources["runtime_config"]["sem_strict_commit_contract"])
                _rt_cfg = resources.get("runtime_config", {}) if isinstance(resources, dict) else {}
                _strict = bool(_rt_cfg.get("sem_strict_commit_contract", False))
                _msg = _format_commit_validation_message(startup_validation_report, "invalid startup config")
                if _strict:
                    logger.error("SEM: Startup config violates commit contract (STRICT, blocking):\n%s", _msg)
                    raise RuntimeError("SEM commit contract violation (strict mode): " + _msg)
                logger.warning(
                    "SEM: Startup config violates commit contract (audit only, not blocked):\n%s",
                    _msg,
                )
        except Exception as exc:
            logger.warning("SEM: Startup config validation failed: %s", exc)

    cluster = None
    schema = None
    if initial_data and isinstance(initial_data, dict) and len(initial_data) > 0:
        cluster, schema = _try_init_cluster_with_retry(initial_data, node_id or "sem", queue_event_send)
        if cluster is not None:
            resources["heap_sync_cluster"] = cluster
            resources["heap_sync_schema"] = schema

    snapshot_cache = make_snapshot_cache()
    initial_snapshot = {}
    if cluster is not None:
        initial_snapshot = export_node_dataset_cached(leader_node(cluster), snapshot_cache)

    event_bus = create_sync_event_bus()
    actuator_data, actuator_lock = _get_value_dict_and_lock(resources, "actuator_values")
    sensor_data, sensor_lock = _get_value_dict_and_lock(resources, "sensor_values")

    ctx = {
        "shutdown_event": shutdown_event or threading.Event(),
        "resources": resources,
        "queue_event_send": queue_event_send,
        "queue_event_state": queue_event_state,
        "node_id": node_id or "sem",
        "heap_sync_cluster": cluster,
        "heap_sync_schema": schema,
        "snapshot_cache": snapshot_cache,
        "master_sync_enabled": bool(master_sync_enabled),
        "event_bus": event_bus,
        "config_snapshot": initial_snapshot,
        "config_snapshot_version": 0,
        "config_snapshot_ts": time.time(),
        "config_indexes": _rebuild_indexes(initial_snapshot),
        "actuator_values": actuator_data,
        "actuator_lock": actuator_lock,
        "sensor_values": sensor_data,
        "sensor_lock": sensor_lock,
        "startup_config_validation_report": startup_validation_report,
        "last_config_validation_report": deepcopy_json(startup_validation_report) if isinstance(startup_validation_report, dict) else None,
        "last_config_validation_source": "startup_audit" if isinstance(startup_validation_report, dict) else None,
        "last_config_validation_ts": _now_iso() if isinstance(startup_validation_report, dict) else None,
        "config_validation_successes": 0,
        "config_validation_failures": 0,
    }
    _sync_to_legacy_resource(ctx)
    return ctx


# =============================================================================
# Ingress loop (blocking)
# =============================================================================


def sem_event_ingress_loop(ctx):
    queue_event_state = ctx.get("queue_event_state")
    bus = ctx.get("event_bus")
    publish = bus["publish"]
    shutdown_event = ctx.get("shutdown_event")

    logger.info("SEM: Ingress started")
    while shutdown_event is None or not shutdown_event.is_set():
        try:
            msg = sync_queue_get(queue_event_state)
        except Exception as exc:
            logger.error("SEM: Queue error: %s", exc)
            time.sleep(0.1)
            continue
        if msg is None:
            continue
        if _is_stop_message(msg):
            break
        if not isinstance(msg, dict):
            continue
        et = _safe_str(msg.get("event_type") or msg.get("type") or "", "").upper()
        if et:
            try:
                publish(msg)
            except Exception as exc:
                logger.error("SEM: Publish error: %s", exc)
    logger.info("SEM: Ingress stopped")


# =============================================================================
# Main loop + thread entry
# =============================================================================


def sem_main_loop(ctx):
    logger.info("SEM: Starting v4.0 (sync, heap_sync native, no legacy)")
    bus = ctx.get("event_bus")
    setup_sem_event_handlers(ctx)
    bus["start_background"]()
    try:
        sem_event_ingress_loop(ctx)
    except Exception as exc:
        logger.error("SEM: Fatal ingress error: %s", exc, exc_info=True)
    finally:
        try:
            bus["stop"]()
        except Exception:
            pass
        try:
            _update_snapshot(ctx)
        except Exception:
            pass
        cluster = ctx.get("heap_sync_cluster")
        if cluster is not None:
            try:
                stop_cluster(cluster, timeout=2.0)
            except Exception:
                pass
        logger.info("SEM: Stopped")


def run_state_event_management(
    *,
    resources,
    queue_event_send,
    queue_event_state,
    shutdown_event=None,
    node_id=None,
    master_sync_enabled=True,
    **unused_kwargs,
):
    """Thread entry for SEM v4.0."""
    try:
        logger.info("Starting State Event Management (SEM v4.0)")
        ctx = create_sem_context(
            resources=resources,
            queue_event_send=queue_event_send,
            queue_event_state=queue_event_state,
            shutdown_event=shutdown_event,
            node_id=node_id,
            master_sync_enabled=master_sync_enabled,
        )
        sem_main_loop(ctx)
    except Exception as exc:
        logger.error("Critical SEM error: %s", exc, exc_info=True)
    finally:
        logger.info("State Event Management stopped")
