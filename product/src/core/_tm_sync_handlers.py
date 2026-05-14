# -*- coding: utf-8 -*-

# src/core/_tm_sync_handlers.py

"""Synchrone TM Event-Handler – Phase 4.

Ersetzt ALLE async Handler aus async_thread_management.py durch
rein synchrone Versionen. Eliminiert die _wrap_sync-Brücken aus
_tm_sync_core.py komplett.

Jeder Handler:
    - Empfängt (event) als einziges Argument (vom sync Event-Bus)
    - ctx wird per functools.partial gebunden
    - Kein asyncio, kein await, keine Coroutinen
    - Queue-Operationen über _queue_put_safe() statt async_queue_put()

Kein OOP, kein Decorator, kein Lambda, kein asyncio.
"""


import copy
import time
import logging
import threading

from functools import partial
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


# =============================================================================
# Imports
# =============================================================================

try:
    from src.libraries._evt_interface import make_event
except ImportError:
    from _evt_interface import make_event

try:
    from src.libraries._syncing_generic_library import (
        create_commit,
        normalize_patch_ops,
        replicate_commit_now,
        snapshot_master,
    )
except ImportError:
    create_commit = None
    normalize_patch_ops = None
    replicate_commit_now = None
    snapshot_master = None

try:
    from src.core._tm_connection_integration import (
        get_request_queue_for_controller,
        add_controller_connection,
        remove_controller_connection,
    )
except ImportError:
    get_request_queue_for_controller = None
    add_controller_connection = None
    remove_controller_connection = None


# =============================================================================
# Helper
# =============================================================================

TM_MASTER_SYNC_ENABLED = False


def _ctx_get(ctx, name, default=None):
    if isinstance(ctx, dict):
        return ctx.get(name, default)
    return getattr(ctx, name, default)


def _ctx_set(ctx, name, value):
    if isinstance(ctx, dict):
        ctx[name] = value
    else:
        setattr(ctx, name, value)


def _queue_put_safe(q, item):
    if q is None:
        return
    try:
        q.put_nowait(item)
    except Exception:
        try:
            q.put(item, timeout=0.2)
        except Exception:
            pass


def _refresh_config_ro_cell(ctx):
    """RO-Cell aktualisieren (importiert aus TM falls verfügbar)."""
    try:
        from src.core.async_thread_management import _refresh_config_ro_cell as _refresh
        _refresh(ctx)
    except Exception:
        pass


# =============================================================================
# 1. handle_config_patch (sync)
# =============================================================================

def handle_config_patch_sync(ctx, event):
    """CONFIG_PATCH: Master-Sync im TM ist deaktiviert (SEM ist Single-Writer)."""
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    patch = payload.get("patch") or []
    if not patch:
        return

    if not TM_MASTER_SYNC_ENABLED:
        logger.info("CONFIG_PATCH: Master-Sync deaktiviert – SEM übernimmt")
        return

    logger.warning("TM_MASTER_SYNC_ENABLED ist veraltet")


# =============================================================================
# 2. handle_config_replace (sync)
# =============================================================================

def handle_config_replace_sync(ctx, event):
    """CONFIG_REPLACE: Master-Sync im TM ist deaktiviert."""
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    new_cfg = payload.get("new_config")
    if not isinstance(new_cfg, dict):
        return

    if not TM_MASTER_SYNC_ENABLED:
        logger.info("CONFIG_REPLACE: Master-Sync deaktiviert – SEM übernimmt")
        return


# =============================================================================
# 3. handle_config_patch_applied (sync)
# =============================================================================

def handle_config_patch_applied_sync(ctx, event):
    """CONFIG_PATCH_APPLIED: Patch replizieren auf alle Replicas (sync)."""
    payload = event.get("payload", {}) if isinstance(event, dict) else {}

    patch_ops = payload.get("patch") or payload.get("ops") or []
    if not patch_ops:
        logger.info("TM: CONFIG_PATCH_APPLIED ohne ops – nichts zu tun")
        return

    # Config-Version übernehmen
    try:
        if payload.get("version") is not None:
            _ctx_set(ctx, "config_version", int(payload["version"]))
    except Exception:
        pass

    sync_ctx = _ctx_get(ctx, "sync_ctx")
    if sync_ctx is None:
        logger.warning("TM: CONFIG_PATCH_APPLIED ohne sync_ctx")
        return

    if normalize_patch_ops is None or create_commit is None or replicate_commit_now is None:
        logger.warning("TM: syncing_generic_library nicht verfügbar")
        return

    ops = normalize_patch_ops(patch_ops, coalesce=False)
    commit = create_commit(
        "patch",
        patch_ops=ops,
        scope="global",
        reason="config_patch_applied_from_sem",
        version=payload.get("version"),
        commit_id=payload.get("commit_id"),
    )

    try:
        res = replicate_commit_now(sync_ctx, commit)
        duration_ms = float(res.get("duration_ms", 0.0)) if isinstance(res, dict) else 0.0
        targets = res.get("targets") if isinstance(res, dict) else None
        logger.info(
            "TM: Replikation nach CONFIG_PATCH_APPLIED (ops=%d, %.1fms, targets=%s)",
            len(ops), duration_ms, targets,
        )
    except Exception as exc:
        logger.error("TM: replicate_commit_now fehlgeschlagen: %s", exc, exc_info=True)

    _refresh_config_ro_cell(ctx)


# =============================================================================
# 4. handle_config_replace_applied (sync)
# =============================================================================

def handle_config_replace_applied_sync(ctx, event):
    """CONFIG_REPLACE_APPLIED: Full-Sync auslösen (sync)."""
    payload = event.get("payload", {}) if isinstance(event, dict) else {}

    try:
        if payload.get("version") is not None:
            _ctx_set(ctx, "config_version", int(payload["version"]))
    except Exception:
        pass

    # Full-Sync auslösen
    handle_initial_full_sync_sync(ctx, {
        "event_type": "INITIAL_FULL_SYNC",
        "payload": {"scope": "global", "affected": None},
    })


# =============================================================================
# 5. handle_initial_full_sync (sync)
# =============================================================================

def handle_initial_full_sync_sync(ctx, event):
    """INITIAL_FULL_SYNC: Vollständige Replikation auf alle Replicas (sync)."""
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    scope = str(payload.get("scope") or "global")
    affected = payload.get("affected")

    if isinstance(affected, (list, set, tuple)):
        try:
            affected = {int(x) for x in affected}
        except Exception:
            affected = None
    else:
        affected = None

    sync_ctx = _ctx_get(ctx, "sync_ctx")
    if sync_ctx is None:
        logger.error("TM: INITIAL_FULL_SYNC ohne sync_ctx")
        return

    if create_commit is None or replicate_commit_now is None:
        logger.error("TM: syncing_generic_library nicht verfügbar")
        return

    try:
        commit = create_commit(
            "replace",
            scope=scope,
            affected=affected,
            reason="initial_full_sync",
            force_full_sync=True,
        )
        res = replicate_commit_now(sync_ctx, commit)
        logger.info(
            "INITIAL_FULL_SYNC: targets=%s in %.1fms",
            res.get("targets") if isinstance(res, dict) else None,
            float(res.get("duration_ms", 0.0)) if isinstance(res, dict) else 0.0,
        )
    except Exception as e:
        logger.error("INITIAL_FULL_SYNC fehlgeschlagen: %s", e, exc_info=True)
        return

    _refresh_config_ro_cell(ctx)

    # CONFIG_CHANGED propagieren (synchron über Event-Bus + Queue)
    version = int(sync_ctx.get("version", 0)) if isinstance(sync_ctx, dict) else 0
    ts = datetime.now(timezone.utc).isoformat()

    evt_cfg = make_event(
        "CONFIG_CHANGED",
        payload={"kind": "initial_full_sync", "version": version, "timestamp": ts},
        target="automation",
    )
    event_bus = _ctx_get(ctx, "event_bus")
    if event_bus:
        event_bus["publish"](evt_cfg)
    _queue_put_safe(_ctx_get(ctx, "queue_event_send"), evt_cfg)

    done = make_event(
        "INITIAL_FULL_SYNC_DONE",
        payload={
            "scope": scope,
            "affected": sorted(list(affected)) if affected else None,
            "targets": res.get("targets") if isinstance(res, dict) else None,
            "duration_ms": float(res.get("duration_ms", 0.0)) if isinstance(res, dict) else 0.0,
            "version": version,
            "timestamp": ts,
        },
        target="automation",
    )
    if event_bus:
        event_bus["publish"](done)
    _queue_put_safe(_ctx_get(ctx, "queue_event_send"), done)


# =============================================================================
# 6. handle_config_change (sync) – mit Controller-Reconciliation
# =============================================================================

def handle_config_change_sync(ctx, event):
    """CONFIG_CHANGED: RO-Cell aktualisieren, Controller-Topologie prüfen (sync)."""
    _refresh_config_ro_cell(ctx)

    try:
        from src.core.async_thread_management import (
            resolve_controllers_for_node,
            create_controller_thread,
            remove_pairs_for_controller,
        )
    except ImportError:
        logger.error("TM-Import fehlgeschlagen für CONFIG_CHANGED")
        return

    sync_ctx = _ctx_get(ctx, "sync_ctx")
    if sync_ctx is not None and snapshot_master is not None:
        master_snap = snapshot_master(sync_ctx)
        controllers = master_snap.get("controllers", [])
        eligible_ids = resolve_controllers_for_node(master_snap, _ctx_get(ctx, "node_id"))
    else:
        config_lock = _ctx_get(ctx, "config_lock")
        with config_lock:
            config_data = _ctx_get(ctx, "config_data") or {}
            controllers = config_data.get("controllers", [])
        eligible_ids = resolve_controllers_for_node(config_data, _ctx_get(ctx, "node_id"))

    new_ids = {
        int(c["controller_id"])
        for c in controllers
        if int(c.get("controller_id", -1)) in eligible_ids
    }
    ctrl_threads = _ctx_get(ctx, "controller_threads", {})
    cur_ids = set(ctrl_threads.keys())

    to_add = new_ids - cur_ids
    to_remove = cur_ids - new_ids

    if to_add or to_remove:
        logger.info("CONFIG_CHANGED: add=%s remove=%s",
                     sorted(list(to_add)), sorted(list(to_remove)))

    # Entfernen
    for cid in to_remove:
        # Shutdown-Signal
        shutdown_events = _ctx_get(ctx, "controller_shutdown_events", {})
        ev = shutdown_events.get(cid)
        if ev:
            ev.set()
        q_key = "controller_event_{}_queue".format(cid)
        ctrl_queues = _ctx_get(ctx, "controller_event_queues", {})
        q = ctrl_queues.get(q_key)
        for pill in ({"_type": "__STOP__", "_sentinel": True}, None):
            _queue_put_safe(q, pill)

        th = ctrl_threads.get(cid)
        if th and th.is_alive():
            try:
                th.join(timeout=2.0)
            except Exception:
                pass

        ctrl_threads.pop(cid, None)
        shutdown_events.pop(cid, None)
        ctrl_queues.pop(q_key, None)
        _ctx_get(ctx, "active_controllers", set()).discard(cid)
        remove_pairs_for_controller(ctx, cid)

        if remove_controller_connection is not None:
            try:
                remove_controller_connection(ctx, cid)
            except Exception:
                pass

        _queue_put_safe(
            _ctx_get(ctx, "queue_event_send"),
            make_event("CONTROLLER_STOPPED", payload={
                "controller_id": cid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }),
        )

    # Hinzufügen
    for c in controllers:
        cid = int(c.get("controller_id", -1))
        if cid not in to_add:
            continue

        if add_controller_connection is not None:
            add_controller_connection(ctx, cid, c)

        th = create_controller_thread(ctx, cid, c)
        ctrl_threads[cid] = th
        _ctx_get(ctx, "active_controllers", set()).add(cid)
        th.start()

        _queue_put_safe(
            _ctx_get(ctx, "queue_event_send"),
            make_event("CONTROLLER_STARTED", payload={
                "controller_id": cid,
                "device_name": c.get("device_name"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "node_id": _ctx_get(ctx, "node_id"),
            }),
        )


# =============================================================================
# 7. handle_initial_full_sync_done (sync)
# =============================================================================

def handle_initial_full_sync_done_sync(ctx, event):
    """INITIAL_FULL_SYNC_DONE: Automation-Ingress freigeben (sync)."""
    try:
        _ctx_set(ctx, "automation_ready", True)
        _ctx_set(ctx, "_gate_log_once", False)
        payload = event.get("payload", {}) if isinstance(event, dict) else {}
        logger.info(
            "INITIAL_FULL_SYNC_DONE: scope=%s targets=%s – Automation-Ingress freigegeben",
            payload.get("scope"), payload.get("targets"),
        )
    except Exception as e:
        logger.warning("INITIAL_FULL_SYNC_DONE Fehler: %s", e)


# =============================================================================
# 8. handle_controller_event / handle_automation_event (sync)
# =============================================================================

def handle_controller_event_sync(ctx, event):
    payload = event.get("payload", {})
    logger.debug("Controller-Event: %s", payload)


def handle_automation_event_sync(ctx, event):
    payload = event.get("payload", {})
    logger.debug("Automation-Event: %s", payload)


# =============================================================================
# 9. handle_automation_state_patch (sync)
# =============================================================================

def handle_automation_state_patch_sync(ctx, event):
    """Ingress für State-Patches (queued für Single-Writer, sync)."""
    payload = event.get("payload") or {}
    patches = payload.get("patches")
    if not isinstance(patches, list):
        return

    latest = _ctx_get(ctx, "automation_state_patch_latest")
    if not isinstance(latest, dict):
        latest = {}
        _ctx_set(ctx, "automation_state_patch_latest", latest)

    for p in patches:
        try:
            k = str(p.get("key") or p.get("state_id") or "")
        except Exception:
            k = ""
        latest[k] = p

    # threading.Event signalisieren (für state_patch_worker_sync)
    ev = _ctx_get(ctx, "automation_state_patch_event")
    if ev is not None and hasattr(ev, "set"):
        ev.set()


# =============================================================================
# 10. handle_automation_one_shot_claim (sync)
# =============================================================================

def handle_automation_one_shot_claim_sync(ctx, event):
    """Ingress für One-Shot Claims (queued für Single-Writer, sync)."""
    payload = event.get("payload") or {}
    req_id = payload.get("request_id")
    if not req_id:
        return

    try:
        rule_id = int(payload.get("rule_id"))
    except Exception:
        return

    scope = str(payload.get("scope") or "")
    reply_idx = payload.get("reply_thread_index")
    try:
        reply_thread_index = int(reply_idx) if reply_idx is not None else None
    except Exception:
        reply_thread_index = None

    pids = payload.get("process_ids") or []
    process_ids = []
    if isinstance(pids, list):
        for p in pids:
            try:
                process_ids.append(int(p))
            except Exception:
                continue

    pending = _ctx_get(ctx, "automation_one_shot_claim_pending")
    if not isinstance(pending, dict):
        pending = {}
        _ctx_set(ctx, "automation_one_shot_claim_pending", pending)

    pending[str(req_id)] = {
        "request_id": str(req_id),
        "rule_id": int(rule_id),
        "scope": scope,
        "process_ids": process_ids,
        "reply_thread_index": reply_thread_index,
    }

    ev = _ctx_get(ctx, "automation_state_patch_event")
    if ev is not None and hasattr(ev, "set"):
        ev.set()


# =============================================================================
# 11. run_initial_full_sync (sync convenience)
# =============================================================================

def run_initial_full_sync_sync(ctx, scope="global", affected=None):
    """Direkter Trigger ohne Bus-Latenz (sync)."""
    event = {
        "event_type": "INITIAL_FULL_SYNC",
        "payload": {"scope": scope, "affected": affected},
    }
    handle_initial_full_sync_sync(ctx, event)


# =============================================================================
# Handler-Registrierung (für _tm_sync_core.py)
# =============================================================================

def register_all_sync_handlers(ctx, event_bus):
    """Registriert ALLE Handler als rein synchrone Funktionen.

    Ersetzt setup_event_handlers_sync() aus _tm_sync_core.py.
    Keine _wrap_sync-Brücken mehr nötig!
    """
    register = event_bus["register_handler"]

    register("CONFIG_PATCH",           partial(handle_config_patch_sync, ctx))
    register("CONFIG_REPLACE",         partial(handle_config_replace_sync, ctx))
    register("CONFIG_PATCH_APPLIED",   partial(handle_config_patch_applied_sync, ctx))
    register("CONFIG_REPLACE_APPLIED", partial(handle_config_replace_applied_sync, ctx))
    register("CONFIG_CHANGED",         partial(handle_config_change_sync, ctx))
    register("INITIAL_FULL_SYNC",      partial(handle_initial_full_sync_sync, ctx))
    register("INITIAL_FULL_SYNC_DONE", partial(handle_initial_full_sync_done_sync, ctx))
    register("CONTROLLER_EVENT",       partial(handle_controller_event_sync, ctx))
    register("AUTOMATION_EVENT",       partial(handle_automation_event_sync, ctx))
    register("AUTOMATION_STATE_PATCH", partial(handle_automation_state_patch_sync, ctx))
    register("AUTOMATION_ONE_SHOT_CLAIM", partial(handle_automation_one_shot_claim_sync, ctx))

    logger.info("TM: Alle %d Event-Handler registriert (100%% sync)", 11)
