# -*- coding: utf-8 -*-

# src/core/thread_management.py

"""
Thread Management (TM) — v5.0 (Synchronous, event-driven, ThreadPool-based)
-----------------------------------------------------------------------------

Responsibility
~~~~~~~~~~~~~~
Central orchestrator for controller threads and automation job dispatch.
TM owns the lifecycle of all controller threads and automation workers.

Architecture
~~~~~~~~~~~~
::

    Controller-Thread (per controller_id)
      -> Sensor-Polling -> SENSOR_VALUE_UPDATE event -> queue_event_send
      -> IO-Broker routes to TM (queue_event_pc)
      -> TM ingress -> event bus -> handle_sensor_event()
      -> 0.5% change filter per sensor_id
      -> ThreadPool job submission (automation_thread placeholder)
      -> Automation job returns actuator_tasks[]
      -> TM dispatches ACTUATOR_TASK to controller event queue (by controller_id)

Event Flow
~~~~~~~~~~
::

    SENSOR_VALUE_UPDATE  -> handle_sensor_event   -> filter -> pool.submit()
    ACTUATOR_TASK_BATCH  -> _dispatch_actuator_tasks_to_controllers()
    CONFIG_CHANGED       -> handle_config_event   -> reload config + reconcile
    CONFIG_PATCH_APPLIED -> handle_config_event   -> replicate to replicas
    TIMER_ELAPSED        -> handle_timer_event    -> pool.submit() (if applicable)

Design
~~~~~~
- No OOP — dict-based context only.
- functools.partial instead of lambda.
- ThreadPool (_thread_pool.py) for automation jobs (default 8 workers).
- Automation threads are job-based, NOT continuously running.
- Actuator tasks sent via event queue (NO shared dicts for runtime data).
- Config replication via heap_sync snapshots.
- Blocking queue ingress — CPU idle < 1%.
- Sync event bus (create_sync_event_bus).
- C/C11 migration friendly.
"""

import copy
import logging
import threading
import time

from functools import partial
from queue import Queue, Empty
from datetime import datetime, timezone

def _mapping_key_as_int(item):
    return int(item[0])


def _controller_sort_id(row):
    return int(row.get("controller_id", 0))


def _virtual_controller_sort_id(row):
    return int(row.get("virt_controller_id", 0))


def _sensor_sort_id(row):
    return int(row.get("sensor_id", 0))


def _actuator_sort_id(row):
    return int(row.get("actuator_id", 0))


# ---------------------------------------------------------------------------
# Event Interface
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Heap Sync (config replication)
# ---------------------------------------------------------------------------

try:
    from src.libraries._heap_sync_final_template import (
        export_node_dataset,
        export_node_dataset_cached,
        make_snapshot_cache,
        leader_node,
        deepcopy_json,
    )
except Exception:
    from _heap_sync_final_template import (
        export_node_dataset,
        export_node_dataset_cached,
        make_snapshot_cache,
        leader_node,
        deepcopy_json,
    )

# ---------------------------------------------------------------------------
# Thread Pool (automation job dispatch)
# ---------------------------------------------------------------------------

try:
    from src.libraries._thread_pool import create_thread_pool
except Exception:
    from _thread_pool import create_thread_pool

# ---------------------------------------------------------------------------
# Controller Thread
# ---------------------------------------------------------------------------

try:
    from src.core._controller_thread import (
        run_control_thread,
        build_controller_context,
    )
except Exception:
    try:
        from _controller_thread import (
            run_control_thread,
            build_controller_context,
        )
    except Exception:
        run_control_thread = None
        build_controller_context = None

# ---------------------------------------------------------------------------
# Connection Layer (optional)
# ---------------------------------------------------------------------------

try:
    from src.core._tm_connection_integration import (
        init_connection_layer,
        get_request_queue_for_controller,
        add_controller_connection,
        remove_controller_connection,
        shutdown_connection_layer,
        connection_layer_health,
    )
except Exception:
    try:
        from _tm_connection_integration import (
            init_connection_layer,
            get_request_queue_for_controller,
            add_controller_connection,
            remove_controller_connection,
            shutdown_connection_layer,
            connection_layer_health,
        )
    except Exception:
        init_connection_layer = None
        get_request_queue_for_controller = None
        add_controller_connection = None
        remove_controller_connection = None
        shutdown_connection_layer = None
        connection_layer_health = None

# ---------------------------------------------------------------------------
# Automation Thread (PLACEHOLDER — will be imported from _automation_thread.py)
# ---------------------------------------------------------------------------

try:
    from src.core._automation_thread import run_automation_cycle
except Exception:
    try:
        from _automation_thread import run_automation_cycle
    except Exception:
        run_automation_cycle = None

# ---------------------------------------------------------------------------
# Runtime Command Binding (Proxy Worker Bridge v35.1)
# ---------------------------------------------------------------------------

try:
    from src.core import _runtime_command_binding as runtime_command_binding
except Exception:
    try:
        import _runtime_command_binding as runtime_command_binding
    except Exception:
        runtime_command_binding = None


logger = logging.getLogger(__name__)

SENTINEL = {"_type": "__STOP__", "_sentinel": True}

# Default automation pool size
DEFAULT_AUTOMATION_POOL_SIZE = 8

# Sensor change threshold (0.5%)
SENSOR_CHANGE_THRESHOLD_PERCENT = 0.5


# =============================================================================
# Pure utility functions
# =============================================================================


def _is_stop_message(obj):
    return (isinstance(obj, dict)
            and obj.get("_type") == "__STOP__"
            and obj.get("_sentinel") is True)


def _safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _safe_bool(value, default=False):
    """Best-effort bool conversion for config and event payload flags."""
    if isinstance(value, bool):
        return bool(value)
    if value is None:
        return bool(default)
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "y", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "n", "off", "disabled"):
            return False
    return bool(default)


def _safe_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default


def _now_ts():
    return time.time()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _queue_put_safe(q, item):
    """Non-blocking queue put with fallback timeout."""
    if q is None:
        return
    try:
        q.put_nowait(item)
    except Exception:
        try:
            q.put(item, timeout=0.2)
        except Exception:
            pass


def _safe_qsize(q, default=None):
    """Best-effort qsize() helper for diagnostics."""
    if q is None:
        return default
    try:
        return int(q.qsize())
    except Exception:
        return default


def _event_payload_dict(event):
    """Return a dict payload for an event or an empty dict."""
    if isinstance(event, dict):
        payload = event.get("payload")
        if isinstance(payload, dict):
            return payload
    return {}


def _config_event_version(event):
    """Extract a monotone config version from an event if present."""
    payload = _event_payload_dict(event)
    return _safe_int(payload.get("version"), None)


def _config_event_timestamp(event):
    """Extract commit/event timestamp from an event if present."""
    payload = _event_payload_dict(event)
    commit_ts = payload.get("commit_ts")
    if commit_ts is not None:
        return commit_ts
    return payload.get("timestamp")


def _is_confirmed_config_event(event):
    """Return True only for post-commit config lifecycle events."""
    et = ""
    if isinstance(event, dict):
        et = str(event.get("event_type") or "").upper()

    version = _config_event_version(event)
    if et in ("CONFIG_PATCH_APPLIED", "CONFIG_REPLACE_APPLIED", "INITIAL_FULL_SYNC_DONE"):
        return version is not None
    if et == "CONFIG_CHANGED":
        return version is not None
    return False


def _should_reload_confirmed_config(ctx, event):
    """Gate TM reloads to confirmed, newer config versions only."""
    if not _is_confirmed_config_event(event):
        return False

    version = _config_event_version(event)
    current = _safe_int(ctx.get("config_version"), 0)
    if version is None:
        return False
    if current is not None and version <= current:
        return False
    return True


def _copy_automation_persistent_state(ctx):
    """Return a deep copy of the shared automation persistent state."""
    lock = ctx.get("automation_persistent_state_lock")
    store = ctx.get("automation_persistent_state") or {}

    if lock is None:
        return copy.deepcopy(store)

    with lock:
        return copy.deepcopy(store)



def _merge_automation_persistent_state(ctx, patch_state):
    """Merge a job-local automation state back into the TM store."""
    if not isinstance(patch_state, dict) or not patch_state:
        return

    def _merge_dict(dst, src):
        for key, value in src.items():
            if isinstance(value, dict):
                cur = dst.get(key)
                if not isinstance(cur, dict):
                    cur = {}
                    dst[key] = cur
                _merge_dict(cur, value)
                continue
            dst[key] = copy.deepcopy(value)

    lock = ctx.get("automation_persistent_state_lock")
    store = ctx.get("automation_persistent_state")
    if not isinstance(store, dict):
        store = {}
        ctx["automation_persistent_state"] = store

    if lock is None:
        _merge_dict(store, patch_state)
        return

    with lock:
        _merge_dict(store, patch_state)


# =============================================================================
# Sensor change filter (0.5% threshold per sensor_id)
# =============================================================================


def _sensor_value_changed(ctx, sensor_id, new_value, force=False):
    """Check if a sensor value should trigger an automation cycle.

    The normal path keeps the historical 0.5 percent change filter.  Forced
    control-loop sensors, especially PID/PI/PD inputs, must be accepted on every
    polling cycle so that stateful filters, PID memory and actuator refreshes do
    not stall during long constant phases.
    """
    if new_value is None:
        return False

    accepted = ctx.get("sensor_accepted_values")
    if accepted is None:
        accepted = {}
        ctx["sensor_accepted_values"] = accepted
    sid_key = str(sensor_id)
    last = accepted.get(sid_key)

    if force:
        accepted[sid_key] = new_value
        return True

    if last is None:
        # First value for this sensor — always accept
        accepted[sid_key] = new_value
        return True

    try:
        last_f = float(last)
        new_f = float(new_value)
    except (TypeError, ValueError):
        # Non-numeric — accept on any change
        if new_value != last:
            accepted[sid_key] = new_value
            return True
        return False

    # Zero guard
    if last_f == 0.0:
        if new_f != 0.0:
            accepted[sid_key] = new_value
            return True
        return False

    # Percentage change check
    pct = abs(new_f - last_f) / abs(last_f) * 100.0
    if pct >= SENSOR_CHANGE_THRESHOLD_PERCENT:
        accepted[sid_key] = new_value
        return True

    return False


# =============================================================================
# Config snapshot helpers
# =============================================================================


def _get_master_snapshot(resources):
    """Get current master config from heap_sync cluster or fallback."""
    cluster = resources.get("heap_sync_cluster")
    if cluster is not None:
        try:
            return export_node_dataset(leader_node(cluster))
        except Exception as exc:
            logger.warning("TM: heap_sync export failed: %s", exc)
    config_entry = resources.get("config_data") or {}
    data = config_entry.get("data")
    if isinstance(data, dict):
        return deepcopy_json(data)
    return {}


def _get_config_snapshot_cached(ctx):
    """Get cached config snapshot from heap_sync or legacy resource."""
    resources = ctx.get("resources") or {}
    cluster = resources.get("heap_sync_cluster")
    if cluster is not None:
        cache = ctx.get("snapshot_cache")
        try:
            node = leader_node(cluster)
            if cache:
                return export_node_dataset_cached(node, cache)
            return export_node_dataset(node)
        except Exception:
            pass
    return _get_master_snapshot(resources)


def _build_automation_settings(master):
    """Build a flat automation_settings dict from config + debug rows.

    The runtime consumes ``automation_settings`` as a flat dict, while the
    current replicated config may additionally store per-engine rows in
    ``automation_debug``.  To keep the integration deterministic, TM projects
    a normalized flat view for automation workers.
    """
    cfg = master if isinstance(master, dict) else {}
    settings = {}

    raw_settings = cfg.get("automation_settings")
    if isinstance(raw_settings, dict):
        settings.update(copy.deepcopy(raw_settings))

    debug_rows = cfg.get("automation_debug") or []
    if not isinstance(debug_rows, list):
        return settings

    debug_pid_enabled = bool(settings.get("debug_pid", False))
    debug_pid_state_ids = []
    debug_pid_interval_s = settings.get("debug_pid_interval_s")
    debug_pid_sat_interval_s = settings.get("debug_pid_saturation_interval_s")
    log_transitions = settings.get("debug_pid_log_transitions")
    log_saturation = settings.get("debug_pid_log_saturation")
    log_clamps = settings.get("debug_pid_log_clamps")

    seen_state_ids = set()
    for row in debug_rows:
        if not isinstance(row, dict):
            continue

        if bool(row.get("pid", False)):
            debug_pid_enabled = True

        state_ids = row.get("pid_state_ids")
        if not isinstance(state_ids, list):
            state_ids = []
        for state_id in state_ids:
            sid = _safe_int(state_id, None)
            if sid is None or sid in seen_state_ids:
                continue
            seen_state_ids.add(sid)
            debug_pid_state_ids.append(sid)

        if debug_pid_interval_s is None and row.get("pid_interval_s") is not None:
            debug_pid_interval_s = row.get("pid_interval_s")

        if debug_pid_sat_interval_s is None and row.get("pid_saturation_interval_s") is not None:
            debug_pid_sat_interval_s = row.get("pid_saturation_interval_s")

        if log_transitions is None and row.get("log_transitions") is not None:
            log_transitions = bool(row.get("log_transitions"))

        if log_saturation is None and row.get("log_saturation") is not None:
            log_saturation = bool(row.get("log_saturation"))

        if log_clamps is None and row.get("log_clamps") is not None:
            log_clamps = bool(row.get("log_clamps"))

    if debug_pid_enabled or debug_pid_state_ids:
        settings.setdefault("debug_pid", debug_pid_enabled)
        if debug_pid_state_ids:
            settings.setdefault("debug_pid_state_ids", list(debug_pid_state_ids))
        if debug_pid_interval_s is not None:
            settings.setdefault("debug_pid_interval_s", debug_pid_interval_s)
        if debug_pid_sat_interval_s is not None:
            settings.setdefault("debug_pid_saturation_interval_s", debug_pid_sat_interval_s)
        if log_transitions is not None:
            settings.setdefault("debug_pid_log_transitions", bool(log_transitions))
        if log_saturation is not None:
            settings.setdefault("debug_pid_log_saturation", bool(log_saturation))
        if log_clamps is not None:
            settings.setdefault("debug_pid_log_clamps", bool(log_clamps))

    return settings


def _build_config_for_automation(master):
    """Build the projected config snapshot used by automation workers.

    The projection must include every table that the automation runtime
    actually consumes.  Extra read-only tables are intentionally carried
    through as a compatibility buffer so new committed snapshots do not get
    semantically truncated before they reach the worker jobs.
    """
    cfg = master or {}
    return {
        "process_states": cfg.get("process_states") or [],
        "process_values": cfg.get("process_values") or [],
        "process_modes": cfg.get("process_modes") or [],
        "processes": cfg.get("processes") or [],
        "sensors": cfg.get("sensors") or [],
        "sensor_types": cfg.get("sensor_types") or [],
        "actuators": cfg.get("actuators") or [],
        "controllers": cfg.get("controllers") or [],
        "virtual_controllers": cfg.get("virtual_controllers") or [],
        "scaling_profiles": cfg.get("scaling_profiles") or [],
        "schema_mappings": cfg.get("schema_mappings") or [],
        "parameter_generic_templates": cfg.get("parameter_generic_templates") or [],
        "pid_configs": cfg.get("pid_configs") or [],
        "control_methods": cfg.get("control_methods") or [],
        "control_method_templates": cfg.get("control_method_templates") or [],
        "automation_rule_sets": cfg.get("automation_rule_sets") or [],
        "automations": cfg.get("automations") or [],
        "automation_engines": cfg.get("automation_engines") or [],
        "automation_filters": cfg.get("automation_filters") or [],
        "automation_debug": cfg.get("automation_debug") or [],
        "automation_settings": _build_automation_settings(cfg),
        "dynamic_rule_engine": cfg.get("dynamic_rule_engine") or [],
        "events": cfg.get("events") or [],
        "triggers": cfg.get("triggers") or [],
        "actions": cfg.get("actions") or [],
        "transitions": cfg.get("transitions") or [],
        "rule_controls": cfg.get("rule_controls") or [],
        "rule_modes": cfg.get("rule_modes") or [],
        "timers": cfg.get("timers") or [],
        "event_types": cfg.get("event_types") or [],
        "local_node": cfg.get("local_node") or [],
    }


# =============================================================================
# Automation job entry (placeholder bridge)
# =============================================================================


def _automation_job_entry(config_snapshot, sensor_data, thread_info, result_queue):
    """Entry point for automation ThreadPool jobs.

    This function runs INSIDE a ThreadPool worker thread.
    It calls the actual automation logic (placeholder) and puts
    resulting actuator_tasks into the result_queue for TM dispatch.

    Args:
        config_snapshot: Frozen config dict (read-only)
        sensor_data:     Dict with sensor_id, value, controller_id, timestamp
        thread_info:     Dict with job_id, pool_worker_id, etc.
        result_queue:    Queue where actuator_tasks are returned to TM
    """
    try:
        actuator_tasks = []

        if run_automation_cycle is not None:
            # Real automation logic (imported from _automation_thread.py)
            actuator_tasks = run_automation_cycle(
                config_snapshot=config_snapshot,
                sensor_data=sensor_data,
                thread_info=thread_info,
            ) or []
        else:
            # PLACEHOLDER — no automation logic available yet
            logger.debug(
                "TM: Automation placeholder (sensor_id=%s, value=%s)",
                sensor_data.get("sensor_id"),
                sensor_data.get("value"),
            )

        if actuator_tasks:
            logger.info(
                "TM: Automation job result: sensor=%s -> %d actuator tasks",
                sensor_data.get("sensor_id"), len(actuator_tasks),
            )
            for _t in actuator_tasks:
                logger.info(
                    "TM:   TASK -> ctrl=%s aid=%s addr=%s f=%s cmd=%s",
                    _t.get("controller_id"), _t.get("actuator_id"),
                    _t.get("address"), _t.get("function_type"),
                    _t.get("control_value"),
                )

        _queue_put_safe(result_queue, {
            "type": "automation_cycle",
            "tasks": list(actuator_tasks),
            "persistent_state": copy.deepcopy(thread_info.get("persistent_state") or {}),
            "sensor_data": sensor_data,
            "job_id": thread_info.get("job_id"),
            "config_version": _safe_int(thread_info.get("config_version"), None),
            "config_commit_ts": thread_info.get("config_commit_ts"),
            "ts": _now_ts(),
        })

    except Exception as exc:
        logger.error("TM: Automation job error: %s", exc, exc_info=True)


# =============================================================================
# Actuator task dispatch (automation result -> controller thread)
# =============================================================================


def _dispatch_actuator_tasks_to_controllers(ctx, tasks, sensor_data):
    """Route actuator tasks to the correct controller thread event queue.

    Each actuator has a controller_id. The task is sent as an ACTUATOR_TASK
    event to that controller's event queue.
    """
    if not tasks:
        return

    config = ctx.get("config_snapshot") or {}
    actuators = config.get("actuators") or []
    ctrl_queues = ctx.get("controller_event_queues") or {}

    # Build actuator_id -> controller_id lookup
    act_to_ctrl = {}
    for act in actuators:
        aid = _safe_int(act.get("actuator_id"))
        cid = _safe_int(act.get("controller_id"))
        if aid is not None and cid is not None:
            act_to_ctrl[aid] = cid

    dispatched = 0
    for task in tasks:
        if not isinstance(task, dict):
            continue

        aid = _safe_int(task.get("actuator_id"))
        if aid is None:
            continue

        cid = act_to_ctrl.get(aid)
        if cid is None:
            logger.warning("TM: No controller for actuator %s", aid)
            continue

        q_key = "controller_event_{}_queue".format(cid)
        target_q = ctrl_queues.get(q_key)
        if target_q is None:
            logger.warning("TM: No event queue for controller %s (actuator %s)", cid, aid)
            continue

        evt = make_event("ACTUATOR_TASK", payload={
            "task": task,
            "source": "automation",
            "ts": _now_iso(),
        })
        _queue_put_safe(target_q, evt)
        dispatched += 1
        logger.info(
            "TM: DISPATCH aid=%s -> C%s (queue=%s) f=%s addr=%s cmd=%s",
            aid, cid, q_key, task.get("function_type"),
            task.get("address"), task.get("control_value"),
        )

    if dispatched > 0:
        logger.info("TM: Dispatched %d actuator tasks total", dispatched)


# =============================================================================
# Automation result consumer (runs in TM worker thread)
# =============================================================================


def _automation_result_consumer(ctx):
    """Consume results from automation ThreadPool and dispatch actuator tasks.

    Blocks on result_queue.get() — CPU idle when no results.
    """
    result_queue = ctx.get("automation_result_queue")
    shutdown = ctx.get("shutdown_event")

    logger.info("TM: Automation result consumer started")

    while not shutdown.is_set():
        try:
            try:
                result = result_queue.get(timeout=1.0)
            except Empty:
                continue

            if result is None or _is_stop_message(result):
                break
            if not isinstance(result, dict):
                continue

            rtype = result.get("type")
            if rtype in ("automation_cycle", "actuator_tasks"):
                result_cfg_ver = _safe_int(result.get("config_version"), None)
                active_cfg_ver = _safe_int(ctx.get("config_version"), 0)

                if (result_cfg_ver is not None
                        and active_cfg_ver is not None
                        and active_cfg_ver > 0
                        and result_cfg_ver < active_cfg_ver):
                    stats = ctx.get("stats")
                    if isinstance(stats, dict):
                        stats["automation_results_stale_dropped"] = int(
                            stats.get("automation_results_stale_dropped", 0)
                        ) + 1
                    logger.info(
                        "TM: DROP stale automation result job=%s result_v=%s active_v=%s",
                        result.get("job_id", "?"),
                        result_cfg_ver,
                        active_cfg_ver,
                    )
                    continue

                _merge_automation_persistent_state(ctx, result.get("persistent_state") or {})
                tasks = result.get("tasks") or []
                sensor_data = result.get("sensor_data") or {}
                if tasks:
                    logger.info(
                        "TM: RESULT_CONSUMER: %d tasks from job=%s (sensor=%s v=%s)",
                        len(tasks), result.get("job_id", "?"),
                        sensor_data.get("sensor_id", "?"),
                        result_cfg_ver,
                    )
                    _dispatch_actuator_tasks_to_controllers(ctx, tasks, sensor_data)

        except Exception as exc:
            logger.error("TM: Result consumer error: %s", exc, exc_info=True)
            time.sleep(0.05)

    logger.info("TM: Automation result consumer stopped")


# =============================================================================
# Controller lifecycle management
# =============================================================================


def _normalize_controller_id_set(value):
    if value is None:
        return set()
    if isinstance(value, dict):
        value = value.get("active_controller_ids") or value.get("controller_ids") or ()
    if isinstance(value, (str, int, float)):
        value = (value,)
    if not isinstance(value, (list, tuple, set)):
        return set()
    result = set()
    for item in value:
        cid = _safe_int(item)
        if cid is not None:
            result.add(cid)
    return result


def _select_fieldbus_runtime_profile(fieldbus_profile):
    if not isinstance(fieldbus_profile, dict):
        return {}
    profile = fieldbus_profile.get("runtime_profile")
    if isinstance(profile, dict):
        return profile
    return fieldbus_profile


def _apply_fieldbus_runtime_profile(controllers, fieldbus_profile=None):
    profile = _select_fieldbus_runtime_profile(fieldbus_profile)
    if not profile or not bool(profile.get("enabled", True)):
        return list(controllers or [])
    active_ids = _normalize_controller_id_set(profile.get("active_controller_ids"))
    if not active_ids:
        return list(controllers or [])
    return [c for c in list(controllers or []) if _safe_int(c.get("controller_id")) in active_ids]


def _resolve_controllers(config, node_id=None, fieldbus_profile=None):
    """Resolve which controllers this node should manage.

    Unterstützt sowohl Legacy-Format
        local_node = {"node_id": "fn-01", "controller_ids": [...]}
    als auch das aktuelle Format
        local_node = [{"node_id": "fn-01", "active_controllers": [...]}]

    v35.1 ergänzt ein optionales Fieldbus-Runtime-Profil. Damit kann ein
    Preproduction-Host nur die real vorhandenen Simulatoren aktiv halten,
    während C1/C3/C4/C5 dokumentiert, aber nicht zwangsweise gestartet werden.
    """
    controllers = config.get("controllers") or []
    if not node_id:
        return _apply_fieldbus_runtime_profile(controllers, fieldbus_profile)

    local_node = config.get("local_node")
    local_nodes = []

    if isinstance(local_node, dict):
        local_nodes = [local_node]
    elif isinstance(local_node, list):
        local_nodes = [ln for ln in local_node if isinstance(ln, dict)]

    for ln in local_nodes:
        ln_id = str(ln.get("node_id") or "")
        if ln_id and ln_id != str(node_id):
            continue

        assigned = ln.get("active_controllers")
        if not isinstance(assigned, list):
            assigned = ln.get("controller_ids")

        if not isinstance(assigned, list):
            continue

        assigned_set = set()
        for c in assigned:
            i = _safe_int(c)
            if i is not None:
                assigned_set.add(i)

        if not assigned_set:
            return []

        selected = [c for c in controllers if _safe_int(c.get("controller_id")) in assigned_set]
        return _apply_fieldbus_runtime_profile(selected, fieldbus_profile)

    return _apply_fieldbus_runtime_profile(controllers, fieldbus_profile)


def _safe_unit_int(unit_raw, default=1):
    """Normalize controller unit values from ints or 0x strings."""
    if isinstance(unit_raw, str):
        try:
            return int(unit_raw.replace("0x", ""), 16)
        except Exception:
            pass
    return _safe_int(unit_raw, default)


def _resolve_controller_runtime_signature(config, controller_data):
    """Build the connection/runtime signature that requires a controller restart.

    Existing controller threads already pick up many config changes live through
    the shared config snapshot.  Connection endpoint and unit changes are
    different: the request queue and the controller ctx are created at startup.
    When this signature changes, the controller thread must be restarted.
    """
    cfg = config if isinstance(config, dict) else {}
    merged = {}
    if isinstance(controller_data, dict):
        merged.update(controller_data)

    cid = _safe_int(merged.get("controller_id"), None)
    if cid is None:
        return None

    for record in cfg.get("controllers") or []:
        if not isinstance(record, dict):
            continue
        if _safe_int(record.get("controller_id"), None) != cid:
            continue
        merged.update(record)
        break

    protocol = str(merged.get("protocol_type") or merged.get("bus_type") or "modbus").lower()
    if protocol == "modbus":
        protocol = "modbus_tcp"

    host = str(merged.get("host") or merged.get("ip_address") or "")
    port = _safe_int(merged.get("port"), 502)
    unit = _safe_unit_int(merged.get("unit", "0x1"), 1)

    for virt in cfg.get("virtual_controllers") or []:
        if not isinstance(virt, dict):
            continue
        if _safe_int(virt.get("controller_id"), None) != cid:
            continue
        if not bool(virt.get("virt_active", False)):
            continue

        virt_host = virt.get("host") or virt.get("ip_address")
        if virt_host:
            host = str(virt_host)

        virt_port = _safe_int(virt.get("port"), None)
        if virt_port is not None:
            port = virt_port
        break

    return (int(cid), str(protocol), str(host), int(port or 0), int(unit or 1))


def _start_controller_thread(ctx, controller_id, controller_data):
    """Start a single controller thread with its own event queue and shutdown event."""
    cid = int(controller_id)

    runtime_signatures = ctx.get("controller_runtime_signatures")
    if isinstance(runtime_signatures, dict):
        runtime_signatures[cid] = _resolve_controller_runtime_signature(
            ctx.get("config_snapshot") or {},
            controller_data,
        )

    # Per-controller event queue
    q_key = "controller_event_{}_queue".format(cid)
    ctrl_queues = ctx.get("controller_event_queues")
    eq = Queue()
    ctrl_queues[q_key] = eq

    # Per-controller shutdown event
    ctrl_shutdown = threading.Event()
    ctx.get("controller_shutdown_events")[cid] = ctrl_shutdown

    # Connection layer request queue
    rq = None
    if get_request_queue_for_controller is not None:
        rq = get_request_queue_for_controller(ctx, cid)
        if rq is None and add_controller_connection is not None:
            add_controller_connection(ctx, cid, controller_data)
            rq = get_request_queue_for_controller(ctx, cid)

    resources = ctx.get("resources") or {}

    # Sensor/actuator value stores (from shared resources)
    sensor_values = resources.get("sensor_values", {}).get("data", {})
    sensor_lock = resources.get("sensor_values", {}).get("lock", threading.RLock())

    # -----------------------------------------------------------------
    # Per-controller config chunking (opt-in via ctx["chunking_enabled"])
    #
    # When enabled, we pass a lean chunk plus a writer-sync emitter
    # into the worker context instead of the full master config. The
    # worker stays fully backward compatible: if chunking is off, the
    # old behavior is preserved byte-for-byte.
    # -----------------------------------------------------------------
    worker_chunk = None
    worker_chunk_flat = None
    if bool(ctx.get("chunking_enabled", False)):
        try:
            from src.orchestration.config_chunking import (
                chunk_controller_config as _chunk_fn,
                materialize_chunk_as_flat_config as _flatten_fn,
            )
            worker_chunk = _chunk_fn(ctx.get("config_snapshot") or {}, cid)
            worker_chunk_flat = _flatten_fn(worker_chunk)
        except Exception as exc:
            logger.warning(
                "TM: chunking failed for controller %d, falling back to full config: %s",
                cid, exc,
            )
            worker_chunk = None
            worker_chunk_flat = None

    # Build a shallow-merged controller_data that carries the chunk and
    # the writer-sync feature flag, so the worker thread can pick them
    # up via its controller_data dict (fully backward compatible: old
    # workers ignore unknown keys).
    enriched_controller_data = dict(controller_data or {})
    if worker_chunk is not None:
        enriched_controller_data["_worker_chunk"] = worker_chunk
    if bool(ctx.get("writer_sync_enabled", False)):
        enriched_controller_data["_writer_sync_enabled"] = True
    if bool(ctx.get("gre_integration_enabled", False)):
        enriched_controller_data["_gre_integration_enabled"] = True

    # If chunking is enabled we pass the flat chunk as config_data so
    # legacy access patterns (config_data[table]) keep working.
    effective_config_data = (
        worker_chunk_flat
        if worker_chunk_flat is not None
        else (ctx.get("config_snapshot") or {})
    )

    th = threading.Thread(
        target=partial(
            run_control_thread,
            controller_id=cid,
            controller_data=enriched_controller_data,
            config_data=effective_config_data,
            config_lock=ctx.get("config_lock"),
            sensor_values=sensor_values,
            sensor_lock=sensor_lock,
            queue_event_send=ctx.get("queue_event_send"),
            event_queue_mbc=eq,
            local_sensor_values={},
            local_sensor_lock=threading.Lock(),
            local_actuator_values={},
            local_actuator_lock=threading.Lock(),
            shutdown_event=ctrl_shutdown,
            request_queue=rq,
        ),
        name="Controller-{}".format(cid),
        daemon=False,
    )
    return th


def _initialize_all_controllers(ctx):
    """Start controller threads for all eligible controllers."""
    config = ctx.get("config_snapshot") or {}
    node_id = ctx.get("node_id")
    controllers = _resolve_controllers(config, node_id, ctx.get("fieldbus_profile"))

    ctrl_threads = ctx.get("controller_threads")
    active_ctrls = ctx.get("active_controllers")

    logger.info("TM: Initializing %d controllers", len(controllers))

    for controller_data in controllers:
        cid = _safe_int(controller_data.get("controller_id"))
        if cid is None or cid in ctrl_threads:
            continue

        th = _start_controller_thread(ctx, cid, controller_data)
        ctrl_threads[cid] = th
        active_ctrls.add(cid)
        th.start()

        _queue_put_safe(ctx.get("queue_event_send"), make_event(
            "CONTROLLER_STARTED", payload={
                "controller_id": cid,
                "device_name": controller_data.get("device_name"),
                "timestamp": _now_iso(),
                "node_id": node_id,
            },
            target="process_manager",
        ))
        logger.info("TM: Controller %d started", cid)


def _stop_controller_thread(ctx, controller_id):
    """Stop a single controller thread gracefully."""
    cid = int(controller_id)
    ctrl_threads = ctx.get("controller_threads", {})
    if cid not in ctrl_threads:
        return

    # Signal shutdown
    shutdown_events = ctx.get("controller_shutdown_events", {})
    ev = shutdown_events.get(cid)
    if ev is not None:
        ev.set()

    # Send sentinel to event queue
    q_key = "controller_event_{}_queue".format(cid)
    ctrl_queues = ctx.get("controller_event_queues", {})
    q = ctrl_queues.get(q_key)
    _queue_put_safe(q, SENTINEL)

    # Join thread
    th = ctrl_threads.get(cid)
    if th is not None and th.is_alive():
        try:
            th.join(timeout=3.0)
        except Exception:
            pass

    # Cleanup
    ctrl_threads.pop(cid, None)
    shutdown_events.pop(cid, None)
    ctrl_queues.pop(q_key, None)
    ctx.get("active_controllers", set()).discard(cid)

    runtime_signatures = ctx.get("controller_runtime_signatures")
    if isinstance(runtime_signatures, dict):
        runtime_signatures.pop(cid, None)

    # Connection cleanup
    if remove_controller_connection is not None:
        try:
            remove_controller_connection(ctx, cid)
        except Exception:
            pass

    logger.info("TM: Controller %d stopped", cid)


def _reconcile_controllers(ctx):
    """Add/remove/restart controller threads to match current config."""
    config = ctx.get("config_snapshot") or {}
    node_id = ctx.get("node_id")
    controllers = _resolve_controllers(config, node_id, ctx.get("fieldbus_profile"))

    new_ids = set()
    ctrl_data_map = {}
    for c in controllers:
        cid = _safe_int(c.get("controller_id"))
        if cid is not None:
            new_ids.add(cid)
            ctrl_data_map[cid] = c

    ctrl_threads = ctx.get("controller_threads", {})
    cur_ids = set(ctrl_threads.keys())
    runtime_signatures = ctx.get("controller_runtime_signatures") or {}

    to_restart = set()
    for cid in sorted(new_ids & cur_ids):
        desired_sig = _resolve_controller_runtime_signature(config, ctrl_data_map.get(cid) or {})
        current_sig = runtime_signatures.get(cid)
        if current_sig is None:
            runtime_signatures[cid] = desired_sig
            continue
        if desired_sig != current_sig:
            to_restart.add(cid)

    to_add = (new_ids - cur_ids) | to_restart
    to_remove = (cur_ids - new_ids) | to_restart

    if not to_add and not to_remove:
        return

    logger.info(
        "TM: Reconcile controllers — add=%s remove=%s restart=%s",
        sorted(new_ids - cur_ids),
        sorted(cur_ids - new_ids),
        sorted(to_restart),
    )

    for cid in sorted(to_remove):
        _stop_controller_thread(ctx, cid)
        _queue_put_safe(ctx.get("queue_event_send"), make_event(
            "CONTROLLER_STOPPED", payload={
                "controller_id": cid, "timestamp": _now_iso(),
            },
            target="process_manager",
        ))

    for cid in sorted(to_add):
        controller_data = ctrl_data_map.get(cid, {})
        th = _start_controller_thread(ctx, cid, controller_data)
        ctrl_threads[cid] = th
        ctx.get("active_controllers", set()).add(cid)
        th.start()
        _queue_put_safe(ctx.get("queue_event_send"), make_event(
            "CONTROLLER_STARTED", payload={
                "controller_id": cid,
                "device_name": controller_data.get("device_name"),
                "timestamp": _now_iso(),
            },
            target="process_manager",
        ))


# =============================================================================
# Automation ThreadPool lifecycle
# =============================================================================


def _create_automation_pool(ctx, num_workers=DEFAULT_AUTOMATION_POOL_SIZE):
    """Create the automation ThreadPool via _thread_pool.py.

    Workers execute automation jobs triggered by sensor events.
    Results flow back through automation_result_queue.
    """
    pool_in = Queue()
    pool_out = Queue()
    pool_event = Queue()

    pool_api = create_thread_pool(
        thread_pool_in=pool_in,
        thread_pool_out=pool_out,
        queue_event_threadpool=pool_event,
        name="automation_pool",
        min_workers=num_workers,
        max_workers=num_workers,
        worker_idle_timeout_s=60.0,
    )

    ctx["automation_pool"] = pool_api
    ctx["automation_pool_in"] = pool_in
    ctx["automation_pool_out"] = pool_out
    ctx["automation_pool_event"] = pool_event

    pool_api["start"]()
    logger.info("TM: Automation ThreadPool started (%d workers)", num_workers)
    return pool_api


def _shutdown_automation_pool(ctx, join_timeout_s=3.0):
    """Shutdown automation ThreadPool gracefully."""
    pool = ctx.get("automation_pool")
    if pool is None:
        return
    try:
        pool["shutdown"](wait=True, join_timeout_s=join_timeout_s)
    except Exception as exc:
        logger.warning("TM: Automation pool shutdown error: %s", exc)
    ctx["automation_pool"] = None
    logger.info("TM: Automation ThreadPool stopped")


# =============================================================================
# Event handlers
# =============================================================================


def handle_sensor_event(ctx, event):
    """Handle SENSOR_VALUE_UPDATE / NEW_SENSOR_DATA from controller threads.

    1. Extract sensor data from event payload
    2. Check 0.5% change threshold
    3. If changed: submit automation job to ThreadPool
    """
    try:
        payload = event.get("payload") or {}
        sensor_id = _safe_int(payload.get("sensor_id"))
        if sensor_id is None:
            return

        value = payload.get("value")
        controller_id = _safe_int(payload.get("controller_id"))

        # Update stats
        stats = ctx.get("stats")
        if isinstance(stats, dict):
            stats["sensor_events"] = int(stats.get("sensor_events", 0)) + 1
            stats["events_total"] = int(stats.get("events_total", 0)) + 1

        force_automation = (
            _safe_bool(payload.get("force_automation"), False)
            or _safe_bool(payload.get("critical"), False)
        )

        # 0.5% change filter. Deterministic control-loop sensors bypass it.
        if not _sensor_value_changed(ctx, sensor_id, value, force=force_automation):
            stats_filtered = ctx.get("stats")
            if isinstance(stats_filtered, dict):
                stats_filtered["sensor_filtered"] = int(stats_filtered.get("sensor_filtered", 0)) + 1
            return

        if force_automation and isinstance(stats, dict):
            stats["sensor_force_automation"] = int(stats.get("sensor_force_automation", 0)) + 1

        # Submit automation job to ThreadPool
        pool = ctx.get("automation_pool")
        if pool is None:
            logger.warning("TM: sensor_event S%s: no automation pool!", sensor_id)
            return

        config_snapshot = ctx.get("config_snapshot_automation")
        if config_snapshot is None:
            config_snapshot = ctx.get("config_snapshot") or {}

        sensor_data = {
            "sensor_id": sensor_id,
            "value": value,
            "controller_id": controller_id,
            "timestamp": payload.get("timestamp") or _now_iso(),
        }

        thread_info = {
            "job_id": "auto_s{}_{}".format(sensor_id, int(_now_ts() * 1000)),
            "sensor_id": sensor_id,
            "node_id": ctx.get("node_id"),
            "ts": _now_ts(),
            "config_version": _safe_int(ctx.get("config_version"), 0),
            "config_commit_ts": ctx.get("config_commit_ts"),
            "persistent_state": _copy_automation_persistent_state(ctx),
        }

        result_queue = ctx.get("automation_result_queue")

        logger.info(
            "TM: SENSOR_EVENT -> pool.submit (S%s=%.2f C%s job=%s force=%s reason=%s)",
            sensor_id, float(value) if value is not None else 0.0,
            controller_id, thread_info["job_id"],
            str(force_automation),
            str(payload.get("automation_reason") or payload.get("force_reason") or "-"),
        )

        pool["submit"](
            _automation_job_entry,
            config_snapshot,
            sensor_data,
            thread_info,
            result_queue,
            call_style="args",
            meta={"sensor_id": sensor_id, "type": "sensor_automation"},
        )

        if isinstance(stats, dict):
            stats["automation_jobs_submitted"] = int(stats.get("automation_jobs_submitted", 0)) + 1

    except Exception as exc:
        logger.error("TM: Sensor event handler error: %s", exc, exc_info=True)


def handle_timer_event(ctx, event):
    """Handle TIMER_ELAPSED / TIMER_DONE events."""
    stats = ctx.get("stats")
    if isinstance(stats, dict):
        stats["events_total"] = int(stats.get("events_total", 0)) + 1
        stats["timer_events"] = int(stats.get("timer_events", 0)) + 1


def handle_config_event(ctx, event):
    """Handle CONFIG_CHANGED / CONFIG_PATCH_APPLIED / CONFIG_REPLACE_APPLIED.

    Reloads config snapshot and reconciles controller threads.
    """
    try:
        stats = ctx.get("stats")
        if isinstance(stats, dict):
            stats["config_events"] = int(stats.get("config_events", 0)) + 1
            stats["events_total"] = int(stats.get("events_total", 0)) + 1

        et = str(event.get("event_type") or "").upper()

        if _should_reload_confirmed_config(ctx, event):
            reload_tm_config(
                ctx,
                reason=et,
                version=_config_event_version(event),
                commit_ts=_config_event_timestamp(event),
            )
            _reconcile_controllers(ctx)

    except Exception as exc:
        logger.error("TM: Config event handler error: %s", exc, exc_info=True)



def handle_proxy_runtime_command_event(ctx, event):
    """Handle Proxy Worker Bridge runtime command events.

    v35.1-Regel:
      - Die Bridge erzeugt die direkte Client-Reply synchron.
      - TM nimmt das Event deterministisch an und schreibt Audit/Command-State.
      - Falls ein Event ohne runtime_result kommt, wird es sicher lokal angewendet.
      - Direkte Feldbus-/IO-Schreibzugriffe bleiben blockiert.
      - Legacy-V34/V33/V32-Events bleiben reine Kompatibilitaetspfade.
    """
    stats = ctx.get("stats")
    if isinstance(stats, dict):
        stats["events_total"] = int(stats.get("events_total", 0)) + 1
        stats["proxy_runtime_command_events"] = int(stats.get("proxy_runtime_command_events", 0)) + 1
    try:
        if runtime_command_binding is None:
            if isinstance(stats, dict):
                stats["proxy_runtime_command_errors"] = int(stats.get("proxy_runtime_command_errors", 0)) + 1
            logger.warning("TM: Runtime command binding module unavailable: %s", event)
            return
        runtime_result = None
        if isinstance(event, dict):
            candidate = event.get("runtime_result")
            if isinstance(candidate, dict):
                runtime_result = candidate
        result = runtime_command_binding.record_runtime_command_audit(
            ctx.get("resources"),
            event,
            runtime_result=runtime_result,
            actor="thread_management",
        )
        if isinstance(stats, dict):
            if result.get("ok"):
                stats["proxy_runtime_command_records"] = int(stats.get("proxy_runtime_command_records", 0)) + 1
            else:
                stats["proxy_runtime_command_errors"] = int(stats.get("proxy_runtime_command_errors", 0)) + 1
        applied_payload = {
            "event_type": event.get("event_type"),
            "operation": event.get("operation"),
            "request_id": event.get("request_id"),
            "correlation_id": event.get("correlation_id"),
            "status": (result.get("result") or {}).get("status"),
            "mode": result.get("mode"),
            "safe_direct_io_write": False,
        }
        _queue_put_safe(ctx.get("queue_event_send"), make_event(
            "V34_RUNTIME_COMMAND_APPLIED",
            payload=applied_payload,
            target="state_event_management",
            source="thread_management",
        ))
        logger.info(
            "TM: Runtime command event handled event_type=%s operation=%s status=%s mode=%s",
            event.get("event_type"),
            event.get("operation"),
            (result.get("result") or {}).get("status"),
            result.get("mode"),
        )
    except Exception as exc:
        if isinstance(stats, dict):
            stats["proxy_runtime_command_errors"] = int(stats.get("proxy_runtime_command_errors", 0)) + 1
        logger.error("TM: Runtime command handler error: %s", exc, exc_info=True)


def handle_generic_event(ctx, event):
    """Fallback handler for unregistered event types."""
    stats = ctx.get("stats")
    if isinstance(stats, dict):
        stats["events_total"] = int(stats.get("events_total", 0)) + 1


# =============================================================================
# Status monitor (heartbeat)
# =============================================================================


def _status_monitor_loop(ctx):
    """Periodic heartbeat emitter. Sleeps via shutdown_event.wait()."""
    shutdown = ctx.get("shutdown_event")

    logger.info("TM: Status monitor started")

    while not shutdown.is_set():
        try:
            status = {
                "node_id": ctx.get("node_id"),
                "active_controllers": sorted(ctx.get("active_controllers", set())),
                "controller_count": len(ctx.get("controller_threads", {})),
                "config_version": _safe_int(ctx.get("config_version"), 0),
                "config_commit_ts": ctx.get("config_commit_ts"),
                "timestamp": _now_iso(),
                "stats": dict(ctx.get("stats") or {}),
            }

            pool = ctx.get("automation_pool")
            if pool is not None:
                try:
                    status["automation_pool"] = pool["get_status_snapshot"]()
                except Exception:
                    pass

            if connection_layer_health is not None:
                try:
                    status["connection_health"] = connection_layer_health(ctx)
                except Exception:
                    pass

            _queue_put_safe(ctx.get("queue_event_send"), make_event(
                "THREAD_MANAGER_HEARTBEAT",
                payload=status,
                target="process_manager",
            ))

            # ====== DIAGNOSTIC: ThreadPool + Pipeline Status ======
            _hb_lines = []
            _hb_lines.append("-" * 72)
            _hb_lines.append("TM HEARTBEAT STATUS")
            _hb_lines.append("-" * 72)
            _hb_stats = dict(ctx.get("stats") or {})
            _hb_lines.append("  Events: total=%d sensor=%d filtered=%d forced=%d jobs_submitted=%d" % (
                _hb_stats.get("events_total", 0),
                _hb_stats.get("sensor_events", 0),
                _hb_stats.get("sensor_filtered", 0),
                _hb_stats.get("sensor_force_automation", 0),
                _hb_stats.get("automation_jobs_submitted", 0),
            ))
            _hb_lines.append("  Controllers: active=%s" % sorted(ctx.get("active_controllers", set())))

            _hb_pool = status.get("automation_pool")
            if isinstance(_hb_pool, dict):
                _hb_lines.append("  ThreadPool: workers_alive=%s busy=%s free=%s queued=%s completed=%s" % (
                    _hb_pool.get("workers_alive", _hb_pool.get("num_workers", "?")),
                    _hb_pool.get("workers_busy", _hb_pool.get("active_jobs", "?")),
                    _hb_pool.get("workers_free", "?"),
                    _hb_pool.get("queue_depth_in", _hb_pool.get("queued_jobs", _hb_pool.get("pending", "?"))),
                    (_hb_pool.get("metrics") or {}).get("jobs_ok", _hb_pool.get("completed_jobs", _hb_pool.get("total_completed", "?"))),
                ))
                _hb_lines.append("              out=%s inflight=%s cfg_ver=%s" % (
                    _hb_pool.get("queue_depth_out", "?"),
                    _hb_pool.get("inflight", "?"),
                    _hb_pool.get("config_version", "?"),
                ))

            _hb_conn = status.get("connection_health")
            if isinstance(_hb_conn, dict):
                _hb_lines.append("  Connections: total=%s connected=%s disconnected=%s" % (
                    _hb_conn.get("total_connections", "?"),
                    _hb_conn.get("connected", "?"),
                    _hb_conn.get("disconnected", "?"),
                ))
                _hb_map = _hb_conn.get("controller_mapping") or {}
                for _mc, _mk in sorted(_hb_map.items(), key=_mapping_key_as_int):
                    _hb_lines.append("    C%s -> %s" % (_mc, _mk))

            _hb_lines.append("  Pipeline Queues:")
            _hb_lines.append("    queue_event_send: size=%s" % _safe_qsize(ctx.get("queue_event_send"), "?"))
            _hb_lines.append("    queue_event_pc:   size=%s" % _safe_qsize(ctx.get("queue_event_pc"), "?"))
            _hb_lines.append("    result_queue:     size=%s" % _safe_qsize(ctx.get("automation_result_queue"), "?"))

            _ctrl_queues = ctx.get("controller_event_queues") or {}
            if _ctrl_queues:
                _hb_lines.append("  Controller Event Queues:")
                for _qk in sorted(_ctrl_queues.keys()):
                    _qq = _ctrl_queues[_qk]
                    _qs = _safe_qsize(_qq, -1)
                    _hb_lines.append("    %s: size=%d" % (_qk, _qs))

            _hb_lines.append("-" * 72)
            logger.info("\n".join(_hb_lines))

            # Dead thread detection
            ctrl_threads = ctx.get("controller_threads", {})
            active = ctx.get("active_controllers", set())
            for cid, th in list(ctrl_threads.items()):
                if not th.is_alive():
                    active.discard(cid)
                    logger.warning("TM: Controller %d thread is dead", cid)

        except Exception as exc:
            logger.error("TM: Status monitor error: %s", exc, exc_info=True)

        shutdown.wait(timeout=30.0)

    logger.info("TM: Status monitor stopped")


# =============================================================================
# Config reload
# =============================================================================


def reload_tm_config(ctx, reason="startup", version=None, commit_ts=None):
    """Reload config snapshot from heap_sync cluster.

    Updates both the TM config_snapshot and the lean automation config.
    Existing controller threads keep a reference to config_snapshot, therefore
    the dict is updated in-place under the shared config_lock.
    """
    master = _get_config_snapshot_cached(ctx)
    config_lock = ctx.get("config_lock")

    if config_lock is None:
        config_lock = threading.RLock()
        ctx["config_lock"] = config_lock

    with config_lock:
        current = ctx.get("config_snapshot")
        if isinstance(current, dict):
            current.clear()
            current.update(master)
            master_ref = current
        else:
            ctx["config_snapshot"] = master
            master_ref = ctx["config_snapshot"]

    ctx["config_snapshot_automation"] = _build_config_for_automation(master_ref)

    if version is not None:
        ctx["config_version"] = int(version)
    if commit_ts is not None:
        ctx["config_commit_ts"] = commit_ts

    # Update automation pool config (frozen copy)
    pool = ctx.get("automation_pool")
    if pool is not None:
        try:
            pool["config_replace"](ctx["config_snapshot_automation"])
        except Exception:
            pass

    logger.info(
        "TM: Config reloaded (%s) controllers=%d sensors=%d actuators=%d states=%d",
        reason,
        len(master.get("controllers") or []),
        len(master.get("sensors") or []),
        len(master.get("actuators") or []),
        len(master.get("process_states") or []),
    )

    # ====== DIAGNOSTIC: Vollständige Geräte-Übersicht bei Startup ======
    if reason == "startup":
        try:
            _lines = []
            _lines.append("=" * 80)
            _lines.append("TM DEVICE REGISTRY OVERVIEW")
            _lines.append("=" * 80)

            _ctrls = master.get("controllers") or []
            _virts = master.get("virtual_controllers") or []
            _sensors = master.get("sensors") or []
            _actuators = master.get("actuators") or []
            _node = master.get("local_node")
            _active_ids = set()
            if isinstance(_node, (list,)):
                for _n in _node:
                    for _ac in (_n.get("active_controllers") or []):
                        _active_ids.add(int(_ac))
            elif isinstance(_node, dict):
                for _ac in (_node.get("active_controllers") or _node.get("controller_ids") or []):
                    _active_ids.add(int(_ac))

            _lines.append("  CONTROLLERS (%d):" % len(_ctrls))
            _lines.append("    %-6s %-16s %-10s %-18s %-6s %-8s" % (
                "CID", "NAME", "BUS", "HOST:PORT", "UNIT", "ACTIVE"))
            for _c in sorted(_ctrls, key=_controller_sort_id):
                _cid = int(_c.get("controller_id", 0))
                _act = "YES" if _cid in _active_ids else "no"
                _lines.append("    %-6s %-16s %-10s %-18s %-6s %-8s" % (
                    _cid, _c.get("device_name", "?"),
                    _c.get("bus_type", "?"),
                    "%s:%s" % (_c.get("host", "?"), _c.get("port", "?")),
                    _c.get("unit", "?"), _act,
                ))

            _lines.append("  VIRTUAL CONTROLLERS (%d):" % len(_virts))
            _lines.append("    %-6s %-6s %-8s %-18s" % ("VID", "CID", "ACTIVE", "HOST:PORT"))
            for _v in sorted(_virts, key=_virtual_controller_sort_id):
                _lines.append("    %-6s %-6s %-8s %-18s" % (
                    _v.get("virt_controller_id"), _v.get("controller_id"),
                    "YES" if _v.get("virt_active") else "no",
                    "%s:%s" % (_v.get("host", "?"), _v.get("port", "?")),
                ))

            _lines.append("  SENSORS (%d):" % len(_sensors))
            _lines.append("    %-6s %-6s %-24s %-8s %-8s" % ("SID", "CID", "FUNCTION_TYPE", "ADDR", "GROUP"))
            for _s in sorted(_sensors, key=_sensor_sort_id):
                _lines.append("    %-6s %-6s %-24s %-8s %-8s" % (
                    _s.get("sensor_id"), _s.get("controller_id"),
                    _s.get("function_type", "?"), _s.get("address", "?"),
                    _s.get("sensor_group", "-"),
                ))

            _lines.append("  ACTUATORS (%d):" % len(_actuators))
            _lines.append("    %-6s %-6s %-24s %-8s %-8s" % ("AID", "CID", "FUNCTION_TYPE", "ADDR", "GROUP"))
            for _a in sorted(_actuators, key=_actuator_sort_id):
                _lines.append("    %-6s %-6s %-24s %-8s %-8s" % (
                    _a.get("actuator_id"), _a.get("controller_id"),
                    _a.get("function_type", "?"), _a.get("address", "?"),
                    _a.get("actuator_group", "-"),
                ))

            _lines.append("=" * 80)
            logger.info("\n".join(_lines))
        except Exception as _diag_exc:
            logger.warning("TM: Diagnostic table error: %s", _diag_exc)


# =============================================================================
# Context creation
# =============================================================================


def create_tm_context(
    *,
    resources,
    queue_event_send,
    queue_event_pc,
    shutdown_event=None,
    node_id=None,
    num_automation=DEFAULT_AUTOMATION_POOL_SIZE,
    chunking_enabled=False,
    writer_sync_enabled=False,
    gre_integration_enabled=False,
    fieldbus_profile=None,
):
    """Create the Thread Management runtime context."""
    shutdown = shutdown_event or threading.Event()
    config_lock = threading.RLock()

    initial_master = _get_master_snapshot(resources)

    return {
        "shutdown_event": shutdown,
        "resources": resources,
        "queue_event_send": queue_event_send,
        "queue_event_pc": queue_event_pc,
        "node_id": node_id or "tm",
        "config_lock": config_lock,
        "config_snapshot": initial_master,
        "config_snapshot_automation": _build_config_for_automation(initial_master),
        "snapshot_cache": make_snapshot_cache(),

        # Lieferung 2/3: per-worker chunking + writer-sync + GRE integration
        "chunking_enabled": bool(chunking_enabled),
        "writer_sync_enabled": bool(writer_sync_enabled),
        "gre_integration_enabled": bool(gre_integration_enabled),
        "fieldbus_profile": fieldbus_profile if isinstance(fieldbus_profile, dict) else {},

        # Event bus (created in main loop)
        "event_bus": None,

        # Controller management
        "controller_threads": {},
        "controller_event_queues": {},
        "controller_shutdown_events": {},
        "controller_runtime_signatures": {},
        "active_controllers": set(),

        # Automation pool (created in main loop)
        "automation_pool": None,
        "automation_result_queue": Queue(),
        "num_automation": int(num_automation),
        "automation_persistent_state": {},
        "automation_persistent_state_lock": threading.RLock(),

        # Config lifecycle state
        "config_version": 0,
        "config_commit_ts": None,

        # Sensor change filter state
        "sensor_accepted_values": {},

        # Telemetry
        "stats": {
            "events_total": 0,
            "sensor_events": 0,
            "sensor_filtered": 0,
            "sensor_force_automation": 0,
            "config_events": 0,
            "timer_events": 0,
            "proxy_runtime_command_events": 0,
            "proxy_runtime_command_records": 0,
            "proxy_runtime_command_errors": 0,
            "automation_jobs_submitted": 0,
            "automation_results_stale_dropped": 0,
        },
    }


# =============================================================================
# Handler registration
# =============================================================================


def setup_tm_event_handlers(ctx):
    """Register all event handlers on the sync event bus."""
    bus = ctx.get("event_bus")
    if bus is None:
        logger.error("TM: No event bus in context")
        return

    register = bus["register_handler"]

    sh = partial(handle_sensor_event, ctx)
    for et in ("NEW_SENSOR_DATA", "SENSOR_VALUE_UPDATE", "SENSOR_THRESHOLD"):
        register(et, sh)

    th = partial(handle_timer_event, ctx)
    for et in ("TIMER_ELAPSED", "TIMER_DONE", "TIMER_FIRED", "SCHEDULE_FIRED"):
        register(et, th)

    ch = partial(handle_config_event, ctx)
    for et in ("CONFIG_CHANGED", "CONFIG_REPLACE", "CONFIG_PATCH_APPLIED",
               "CONFIG_REPLACE_APPLIED", "INITIAL_FULL_SYNC_DONE"):
        register(et, ch)


    rh = partial(handle_proxy_runtime_command_event, ctx)
    for et in ("V35_1_PROXY_RUNTIME_COMMAND_RECEIVED", "V34_PROXY_RUNTIME_COMMAND_RECEIVED", "V33_PROXY_WORKER_COMMAND_RECEIVED", "V32_PROXY_WORKER_COMMAND_RECEIVED"):
        register(et, rh)

    bus["register_fallback"](partial(handle_generic_event, ctx))
    logger.info("TM: All event handlers registered")


# =============================================================================
# Ingress + main loop + thread entry
# =============================================================================


def tm_event_ingress_loop(ctx):
    """Main event ingress loop. Blocks on queue_event_pc.

    Wichtig: queue_event_pc wird vom sync_event_router via sync_queue_put()
    beschrieben und enthält daher JSON-serialisierte Events.
    TM MUSS deshalb sync_queue_get() nutzen, damit dict-Events wieder korrekt
    deserialisiert werden.
    """
    queue_pc = ctx.get("queue_event_pc")
    publish = ctx["event_bus"]["publish"]
    shutdown = ctx.get("shutdown_event")

    ingress_count = 0
    last_log_ts = 0.0

    logger.info("TM: Event ingress started")

    while not shutdown.is_set():
        try:
            msg = sync_queue_get(queue_pc)
            if msg is None:
                continue
            if _is_stop_message(msg):
                break
            if not isinstance(msg, dict):
                logger.warning("TM: Ingress dropped non-dict payload type=%s", type(msg).__name__)
                continue

            et = str(msg.get("event_type") or msg.get("type") or "").upper()
            if not et:
                logger.warning("TM: Ingress dropped event without event_type: %s", msg)
                continue

            ingress_count += 1
            now_ts = _now_ts()
            if ingress_count <= 5 or (now_ts - last_log_ts) >= 5.0:
                logger.info(
                    "TM: INGRESS event=%s target=%s queue_event_pc_depth=%s count=%d",
                    et,
                    msg.get("target", "?"),
                    _safe_qsize(queue_pc, "?"),
                    ingress_count,
                )
                last_log_ts = now_ts

            try:
                publish(msg)
            except Exception as exc:
                logger.error("TM: Publish error: %s", exc)

        except Exception as exc:
            logger.error("TM: Ingress error: %s", exc)
            time.sleep(0.1)

    logger.info("TM: Event ingress stopped")


def tm_main_loop(ctx):
    """Main orchestration loop.

    Startup sequence:
        1. Create sync event bus
        2. Register event handlers
        3. Initialize connection layer
        4. Reload config
        5. Create automation ThreadPool
        6. Initialize controller threads
        7. Start worker threads (ingress, result consumer, status monitor)
        8. Wait for shutdown
        9. Graceful shutdown of all components
    """
    shutdown = ctx.get("shutdown_event")
    workers = []

    try:
        # ---- 1. Sync Event Bus ----
        event_bus = create_sync_event_bus()
        ctx["event_bus"] = event_bus
        event_bus["start_background"]()
        logger.info("TM: Sync event bus started")

        # ---- 2. Event Handlers ----
        setup_tm_event_handlers(ctx)

        # ---- 3. Connection Layer ----
        if init_connection_layer is not None:
            try:
                init_connection_layer(ctx)
                logger.info("TM: Connection layer initialized")
            except Exception as exc:
                logger.warning("TM: Connection layer init failed: %s", exc)

        # ---- 4. Config Reload ----
        reload_tm_config(ctx, reason="startup")

        # ---- 5. Automation ThreadPool ----
        _create_automation_pool(ctx, num_workers=ctx.get("num_automation", DEFAULT_AUTOMATION_POOL_SIZE))

        # ---- 6. Controller Threads ----
        if run_control_thread is not None:
            _initialize_all_controllers(ctx)
        else:
            logger.warning("TM: _controller_thread not available — no controllers started")

        # ---- 7. Worker Threads ----
        worker_specs = [
            ("TM-Ingress",         partial(tm_event_ingress_loop, ctx)),
            ("TM-ResultConsumer",  partial(_automation_result_consumer, ctx)),
            ("TM-StatusMonitor",   partial(_status_monitor_loop, ctx)),
        ]

        for name, target in worker_specs:
            th = threading.Thread(target=target, name=name, daemon=False)
            workers.append(th)
            th.start()
            logger.info("TM: Worker started: %s", name)

        # ---- 8. Wait for Shutdown ----
        logger.info("TM: System ready — v5.0 (event-driven, ThreadPool automation)")
        while not shutdown.is_set():
            shutdown.wait(timeout=1.0)

    except Exception as exc:
        logger.error("TM: Main loop error: %s", exc, exc_info=True)

    finally:
        # ---- 9. Graceful Shutdown ----
        logger.info("TM: Shutdown started...")
        shutdown.set()

        # Stop automation pool
        _shutdown_automation_pool(ctx, join_timeout_s=3.0)

        # Sentinel into result queue
        _queue_put_safe(ctx.get("automation_result_queue"), SENTINEL)

        # Stop all controller threads
        ctrl_threads = ctx.get("controller_threads", {})
        for cid in list(ctrl_threads.keys()):
            _stop_controller_thread(ctx, cid)

        # Stop connection layer
        if shutdown_connection_layer is not None:
            try:
                shutdown_connection_layer(ctx, join_timeout_s=5.0)
            except Exception:
                pass

        # Join worker threads
        for th in workers:
            if th.is_alive():
                try:
                    th.join(timeout=3.0)
                except Exception:
                    pass
            if th.is_alive():
                logger.warning("TM: Worker %s not responding", th.name)

        # Stop event bus
        event_bus = ctx.get("event_bus")
        if event_bus is not None:
            try:
                event_bus["stop"]()
            except Exception:
                pass

        logger.info("TM: Shutdown complete")


def run_thread_management(
    *,
    resources,
    queue_event_send,
    queue_event_pc,
    shutdown_event=None,
    node_id=None,
    num_automation=DEFAULT_AUTOMATION_POOL_SIZE,
    chunking_enabled=False,
    writer_sync_enabled=False,
    gre_integration_enabled=False,
    fieldbus_profile=None,
    **unused_kwargs,
):
    """Thread entry point for TM v5.0.

    Can be called directly or as a threading.Thread target.
    """
    try:
        logger.info(
            "Starting Thread Management (TM v5.0) chunking=%s writer_sync=%s gre=%s",
            bool(chunking_enabled),
            bool(writer_sync_enabled),
            bool(gre_integration_enabled),
        )
        ctx = create_tm_context(
            resources=resources,
            queue_event_send=queue_event_send,
            queue_event_pc=queue_event_pc,
            shutdown_event=shutdown_event,
            node_id=node_id,
            num_automation=num_automation,
            chunking_enabled=chunking_enabled,
            writer_sync_enabled=writer_sync_enabled,
            gre_integration_enabled=gre_integration_enabled,
            fieldbus_profile=fieldbus_profile,
        )
        tm_main_loop(ctx)
    except Exception as exc:
        logger.error("Critical TM error: %s", exc, exc_info=True)
    finally:
        logger.info("Thread Management stopped")
