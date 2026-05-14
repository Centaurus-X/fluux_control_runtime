# -*- coding: utf-8 -*-

# src/core/_tm_sync_core.py

"""Thread-Management Sync Core – Synchrone Worker-Loops.

Ersetzt alle asyncio-basierten Loops aus async_thread_management.py
durch synchrone, blockierende Thread-Loops.

Enthält:
    - event_queue_listener_sync     (ersetzt async event_queue_listener)
    - controller_event_router_sync  (ersetzt async controller_event_router)
    - automation_event_router_sync  (ersetzt async automation_event_router)
    - state_patch_worker_sync       (ersetzt async automation_state_patch_worker)
    - status_monitor_sync           (ersetzt async status_monitor)
    - main_sync                     (ersetzt async main_async)
    - run_thread_management_sync    (ersetzt run_thread_management)

Design:
    - 100% synchron, kein asyncio
    - Blockierende Queue.get(timeout=...) → CPU idle
    - threading.Thread für Worker → sauber terminierbar
    - threading.Event statt asyncio.Event
    - create_sync_event_bus() statt create_event_bus()
    - Kein OOP, kein Decorator, kein Lambda
    - functools.partial statt lambda
"""


import os
import copy
import threading
import time
import logging

from queue import Queue, Empty
from functools import partial
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


# =============================================================================
# Imports (bestehende Module)
# =============================================================================

try:
    from src.libraries._evt_interface import (
        create_sync_event_bus,
        make_event,
    )
except ImportError:
    from _evt_interface import (
        create_sync_event_bus,
        make_event,
    )

try:
    from src.core._tm_connection_integration import (
        init_connection_layer,
        get_request_queue_for_controller,
        add_controller_connection,
        remove_controller_connection,
        shutdown_connection_layer,
        connection_layer_health,
    )
except ImportError:
    try:
        from _tm_connection_integration import (
            init_connection_layer,
            get_request_queue_for_controller,
            add_controller_connection,
            remove_controller_connection,
            shutdown_connection_layer,
            connection_layer_health,
        )
    except ImportError:
        init_connection_layer = None
        logger.warning("_tm_connection_integration nicht verfügbar")

try:
    from src.core._tm_sync_handlers import (
        register_all_sync_handlers,
        run_initial_full_sync_sync,
    )
except ImportError:
    try:
        from _tm_sync_handlers import (
            register_all_sync_handlers,
            run_initial_full_sync_sync,
        )
    except ImportError:
        register_all_sync_handlers = None
        run_initial_full_sync_sync = None
        logger.warning("_tm_sync_handlers nicht verfügbar")


# =============================================================================
# Helper
# =============================================================================

SENTINEL = {"_type": "__STOP__", "_sentinel": True}


def _is_stop_message(obj):
    return (isinstance(obj, dict)
            and obj.get("_type") == "__STOP__"
            and obj.get("_sentinel") is True)


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


def _ctx_get(ctx, name, default=None):
    if isinstance(ctx, dict):
        return ctx.get(name, default)
    return getattr(ctx, name, default)


def _ctx_set(ctx, name, value):
    if isinstance(ctx, dict):
        ctx[name] = value
    else:
        setattr(ctx, name, value)


def _safe_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default


# =============================================================================
# 1. Event-Queue-Listener (sync)
# =============================================================================

def event_queue_listener_sync(ctx):
    """Empfängt Events aus queue_event_pc und dispatcht an Event-Bus.

    Blockiert auf Queue.get(timeout=1.0) → CPU idle.
    Ersetzt: async event_queue_listener()
    """
    q = _ctx_get(ctx, "queue_event_pc")
    shutdown = _ctx_get(ctx, "shutdown_event")
    event_bus = _ctx_get(ctx, "event_bus")

    logger.info("TM: Event-Queue-Listener gestartet (sync)")
    cnt = 0

    while not shutdown.is_set():
        try:
            try:
                ev = q.get(timeout=1.0)
            except Empty:
                continue

            if ev is None or _is_stop_message(ev):
                break
            if not isinstance(ev, dict):
                continue

            et = str(ev.get("event_type") or "").upper()
            cnt += 1

            if cnt <= 50 or cnt % 100 == 0:
                logger.info("TM←Broker #%d: %s", cnt, et)

            # Re-Prime bei Automation-Leerstand
            if et == "AUTOMATION_STATUS":
                try:
                    pl = ev.get("payload") or {}
                    cc = int(pl.get("controllers_cached") or 0)
                    config_lock = _ctx_get(ctx, "config_lock_automation")
                    with config_lock:
                        acfg = copy.deepcopy(_ctx_get(ctx, "config_data_automation") or {})
                    empty_cfg = (not acfg.get("controllers")
                                 or not acfg.get("sensors")
                                 or not acfg.get("process_states"))
                    last_reprime = float(getattr(ctx, "last_reprime_ts", 0.0))
                    if (cc == 0 or empty_cfg) and (time.time() - last_reprime > 5.0):
                        logger.info("Re-Prime ausgelöst: cc=%s empty=%s", cc, empty_cfg)
                        _run_initial_full_sync_local(ctx, scope="global")
                        _ctx_set(ctx, "last_reprime_ts", time.time())
                except Exception as e:
                    logger.debug("Re-Prime-Check ignoriert: %s", e)

            # Event-Bus dispatchen (synchron!)
            if event_bus is not None:
                event_bus["publish"](ev)

        except Exception as e:
            logger.error("Event-Queue-Listener Fehler: %s", e, exc_info=True)
            time.sleep(0.1)

    logger.info("TM: Event-Queue-Listener beendet (#%d Events)", cnt)


# =============================================================================
# 2. Controller-Event-Router (sync)
# =============================================================================

def controller_event_router_sync(ctx):
    """Routet Events an Controller-Event-Queues (sync).

    Blockiert auf Queue.get(timeout=1.0) → CPU idle.
    Ersetzt: async controller_event_router()
    """
    q = _ctx_get(ctx, "queue_event_mbc")
    shutdown = _ctx_get(ctx, "shutdown_event")

    if q is None:
        logger.warning("TM: Controller-Event-Router: queue_event_mbc ist None")
        return

    logger.info("TM: Controller-Event-Router gestartet (sync)")

    try:
        while not shutdown.is_set():
            try:
                msg = q.get(timeout=1.0)
            except Empty:
                continue

            if msg is None or _is_stop_message(msg):
                break
            if not isinstance(msg, dict):
                continue

            et = msg.get("event_type")
            cid = msg.get("controller_id")
            ctrl_queues = _ctx_get(ctx, "controller_event_queues", {})

            if cid is not None:
                q_key = "controller_event_{}_queue".format(cid)
                target_q = ctrl_queues.get(q_key)
                if target_q:
                    _queue_put_safe(target_q, msg)
                else:
                    logger.warning("Keine Event-Queue für Controller %s", cid)
            else:
                # Broadcast
                for q_key, target_q in ctrl_queues.items():
                    _queue_put_safe(target_q, msg)
    finally:
        ctrl_queues = _ctx_get(ctx, "controller_event_queues", {})
        for target_q in ctrl_queues.values():
            _queue_put_safe(target_q, SENTINEL)
        logger.info("TM: Controller-Event-Router beendet")


# =============================================================================
# 3. Automation-Event-Router (sync)
# =============================================================================

def automation_event_router_sync(ctx):
    """Routet Events an Automation-Thread-Queues (sync).

    Broadcast + Round-Robin-Loadbalancing für Sensor-Events.
    Blockiert auf Queue.get(timeout=1.0) → CPU idle.
    Ersetzt: async automation_event_router()
    """
    q = _ctx_get(ctx, "queue_event_mba")
    shutdown = _ctx_get(ctx, "shutdown_event")

    queues = _ctx_get(ctx, "automation_event_queues", None)
    if not queues:
        logger.info("TM: Automation-Event-Router: keine Queues – passiv")
        while not shutdown.is_set():
            shutdown.wait(timeout=1.0)
        return

    logger.info("TM: Automation-Event-Router gestartet (sync, %d Queues)", len(queues))

    rr_index = 0

    try:
        while not shutdown.is_set():
            try:
                msg = q.get(timeout=1.0)
            except Empty:
                continue

            if _is_stop_message(msg):
                break
            if not msg or not isinstance(msg, dict):
                continue

            et = str(msg.get("event_type") or "").upper()

            # Sensor-Events gaten bis System bereit
            if et in ("SENSOR_VALUE_UPDATE", "NEW_SENSOR_DATA"):
                if not bool(getattr(ctx, "automation_ready", False)):
                    continue

            # Expliziter thread_index → gezielter Dispatch
            thread_index = msg.get("thread_index")
            if thread_index is not None:
                idx = _safe_int(thread_index)
                if idx is not None:
                    target_q = queues.get(idx)
                    if target_q is not None:
                        _queue_put_safe(target_q, msg)
                continue

            # Sensor-Events → Round-Robin
            if et in ("SENSOR_VALUE_UPDATE", "NEW_SENSOR_DATA"):
                indices = sorted(queues.keys())
                if indices:
                    target_idx = indices[rr_index % len(indices)]
                    rr_index += 1
                    target_q = queues.get(target_idx)
                    if target_q is not None:
                        _queue_put_safe(target_q, msg)
                continue

            # Default: Broadcast
            for target_q in queues.values():
                _queue_put_safe(target_q, msg)

    finally:
        for target_q in (_ctx_get(ctx, "automation_event_queues", {}) or {}).values():
            _queue_put_safe(target_q, SENTINEL)
        logger.info("TM: Automation-Event-Router beendet")


# =============================================================================
# 4. Automation-State-Patch-Worker (sync)
# =============================================================================

def state_patch_worker_sync(ctx):
    """Single-Writer für shared_automation_states (sync).

    Wartet auf threading.Event + Flush-Intervall.
    Ersetzt: async automation_state_patch_worker()
    """
    shutdown = _ctx_get(ctx, "shutdown_event")

    # threading.Event statt asyncio.Event
    patch_event = threading.Event()
    _ctx_set(ctx, "automation_state_patch_event", patch_event)

    if not hasattr(ctx, "automation_state_patch_latest"):
        _ctx_set(ctx, "automation_state_patch_latest", {})
    if not hasattr(ctx, "automation_one_shot_claim_pending"):
        _ctx_set(ctx, "automation_one_shot_claim_pending", {})

    try:
        flush_interval_s = float(getattr(ctx, "automation_state_patch_flush_interval_s", 0.02))
    except Exception:
        flush_interval_s = 0.02

    logger.info("TM: State-Patch-Worker gestartet (sync, flush=%.3fs)", flush_interval_s)

    while not shutdown.is_set():
        try:
            # Blockierend warten (CPU idle!)
            patch_event.wait(timeout=flush_interval_s)
            patch_event.clear()

            patch_batch = _ctx_get(ctx, "automation_state_patch_latest", {}) or {}
            claim_batch = _ctx_get(ctx, "automation_one_shot_claim_pending", {}) or {}

            if not patch_batch and not claim_batch:
                continue

            _ctx_set(ctx, "automation_state_patch_latest", {})
            _ctx_set(ctx, "automation_one_shot_claim_pending", {})

            patches = sorted(patch_batch.values(), key=_patch_key_safe)

            store = _ctx_get(ctx, "shared_automation_states")
            lock = _ctx_get(ctx, "shared_automation_states_lock")
            if store is None or lock is None:
                continue

            response_events = []
            applied = 0

            with lock:
                # Claims atomar
                for req_id, req in claim_batch.items():
                    if not isinstance(req, dict):
                        continue
                    rid = req.get("rule_id")
                    scope = str(req.get("scope") or "")
                    pids = req.get("process_ids") or []
                    reply_idx = req.get("reply_thread_index")

                    ok = _apply_one_shot_claim_safe(store, rid, scope, pids)

                    try:
                        evt = make_event(
                            "AUTOMATION_ONE_SHOT_CLAIM_RESULT",
                            payload={
                                "request_id": str(req_id),
                                "ok": bool(ok),
                                "rule_id": rid,
                                "scope": scope,
                            },
                            target="automation",
                            thread_index=reply_idx,
                        )
                        response_events.append(evt)
                    except Exception:
                        pass

                # Patches
                for p in patches:
                    if _apply_patch_safe(store, p):
                        applied += 1

            # Antworten nach dem Lock
            q_send = _ctx_get(ctx, "queue_event_send")
            for ev in response_events:
                _queue_put_safe(q_send, ev)

            if applied:
                logger.debug("TM: State-Patches committed: %d", applied)

        except Exception as e:
            logger.error("State-Patch-Worker Fehler: %s", e, exc_info=True)
            time.sleep(0.05)

    logger.info("TM: State-Patch-Worker beendet")


def _patch_key_safe(patch):
    try:
        return str(patch.get("key") or patch.get("state_id") or "")
    except Exception:
        return ""


def _apply_one_shot_claim_safe(store, rule_id, scope, pids):
    """One-Shot-Claim atomar anwenden (unter Lock)."""
    try:
        fired = store.setdefault("one_shot_fired", set())
        fired_pp = store.setdefault("one_shot_fired_per_process", set())

        if scope == "global":
            key = str(rule_id)
            if key in fired:
                return False
            fired.add(key)
            return True
        elif scope == "per_process":
            for pid in (pids if isinstance(pids, list) else []):
                key = "{}:{}".format(rule_id, pid)
                if key in fired_pp:
                    return False
            for pid in (pids if isinstance(pids, list) else []):
                fired_pp.add("{}:{}".format(rule_id, pid))
            return True
        return False
    except Exception:
        return False


def _apply_patch_safe(store, patch):
    """State-Patch atomar anwenden (unter Lock)."""
    try:
        if not isinstance(patch, dict):
            return False
        states = store.setdefault("states", {})
        key = str(patch.get("key") or patch.get("state_id") or "")
        if not key:
            return False
        states[key] = patch.get("value", patch)
        store["version"] = int(store.get("version", 0)) + 1
        store["updated_ts"] = time.time()
        return True
    except Exception:
        return False


# =============================================================================
# 5. Status-Monitor (sync)
# =============================================================================

def status_monitor_sync(ctx):
    """Periodischer Heartbeat (sync).

    Schläft 30s per shutdown_event.wait() → CPU idle.
    Ersetzt: async status_monitor()
    """
    shutdown = _ctx_get(ctx, "shutdown_event")

    logger.info("TM: Status-Monitor gestartet (sync)")

    while not shutdown.is_set():
        try:
            status = {
                "node_id": _ctx_get(ctx, "node_id"),
                "active_controllers": list(_ctx_get(ctx, "active_controllers", set())),
                "controller_count": len(_ctx_get(ctx, "controller_threads", {})),
                "automation_count": len(_ctx_get(ctx, "automation_slot_pools", {}) or {}),
                "config_version": _ctx_get(ctx, "config_version", 0),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            # Connection Layer Health (PHASE15)
            try:
                if connection_layer_health is not None:
                    status["connection_health"] = connection_layer_health(ctx)
            except Exception:
                pass

            evt = make_event(
                "THREAD_MANAGER_HEARTBEAT",
                payload=status,
                target="process_manager",
            )
            _queue_put_safe(_ctx_get(ctx, "queue_event_send"), evt)

            # Dead-Thread Detection
            ctrl_threads = _ctx_get(ctx, "controller_threads", {})
            active_ctrls = _ctx_get(ctx, "active_controllers", set())
            for cid, th in list(ctrl_threads.items()):
                if not th.is_alive():
                    active_ctrls.discard(cid)

        except Exception as e:
            logger.error("Status-Monitor Fehler: %s", e, exc_info=True)

        # 30s schlafen (unterbrechbar durch shutdown)
        shutdown.wait(timeout=30.0)

    logger.info("TM: Status-Monitor beendet")


# =============================================================================
# 6. Event-Handler Setup (sync) – Phase 4: 100% sync, keine Brücken
# =============================================================================

def setup_event_handlers_sync(ctx):
    """Registriert alle Event-Handler als rein synchrone Funktionen.

    Nutzt register_all_sync_handlers() aus _tm_sync_handlers.py.
    Keine _wrap_sync-Brücken, kein asyncio, kein inspect.
    """
    event_bus = _ctx_get(ctx, "event_bus")
    if event_bus is None:
        logger.error("Kein Event-Bus im Context!")
        return

    if register_all_sync_handlers is not None:
        register_all_sync_handlers(ctx, event_bus)
    else:
        logger.error("register_all_sync_handlers nicht verfügbar!")


# =============================================================================
# 7. Initial-Full-Sync (delegiert an _tm_sync_handlers)
# =============================================================================

def _run_initial_full_sync_local(ctx, *, scope="global", affected=None):
    """Synchroner Initial-Full-Sync Trigger.

    Delegiert an run_initial_full_sync_sync() aus _tm_sync_handlers.
    Fallback: direkt aus async TM (legacy).
    """
    if run_initial_full_sync_sync is not None:
        run_initial_full_sync_sync(ctx, scope=scope, affected=affected)
        return

    # Legacy-Fallback (sollte nicht mehr benötigt werden)
    logger.warning("Fallback: run_initial_full_sync_sync nicht verfügbar")
    try:
        from src.core.async_thread_management import run_initial_full_sync
        import asyncio
        import inspect
        if inspect.iscoroutinefunction(run_initial_full_sync):
            loop = asyncio.new_event_loop()
            loop.run_until_complete(run_initial_full_sync(ctx, scope=scope, affected=affected))
            loop.close()
        else:
            run_initial_full_sync(ctx, scope=scope, affected=affected)
    except Exception as e:
        logger.error("Initial-Full-Sync Fehler: %s", e)


# =============================================================================
# 8. main_sync – Ersetzt main_async
# =============================================================================

def main_sync(ctx):
    """Synchroner TM-Haupteinstieg.

    Startet Worker-Threads statt asyncio-Tasks.
    Wartet auf shutdown_event.

    Reihenfolge:
        1. Sync Event-Bus starten
        2. Event-Handler registrieren
        3. Connection Layer initialisieren
        4. Controller initialisieren + starten
        5. Initial-Full-Sync
        6. Automation-Pools starten
        7. Worker-Threads starten
        8. Warten auf Shutdown
        9. Sauberes Shutdown aller Komponenten
    """
    shutdown = _ctx_get(ctx, "shutdown_event")
    workers = []

    try:
        # ---- 1. Sync Event-Bus ----
        event_bus = create_sync_event_bus()
        _ctx_set(ctx, "event_bus", event_bus)
        event_bus["start_background"]()
        logger.info("TM: Sync Event-Bus gestartet")

        # ---- 2. Event-Handler ----
        setup_event_handlers_sync(ctx)

        # State-Patch Init
        _ctx_set(ctx, "automation_state_patch_latest", {})
        _ctx_set(ctx, "automation_one_shot_claim_pending", {})
        _ctx_set(ctx, "automation_state_patch_flush_interval_s", 0.02)

        # ---- 3. Connection Layer ----
        if init_connection_layer is not None:
            init_connection_layer(ctx)
            logger.info("TM: Connection Layer: %s", connection_layer_health(ctx))

        # ---- 4. Controller ----
        _initialize_controllers_sync(ctx)

        # ---- 5. Initial-Full-Sync ----
        _run_initial_full_sync_local(ctx, scope="global")

        # ---- 6. Automation-Pools ----
        try:
            from src.core.async_thread_management import initialize_automation_task_pools
            import inspect
            if inspect.iscoroutinefunction(initialize_automation_task_pools):
                import asyncio
                _loop = asyncio.new_event_loop()
                _loop.run_until_complete(initialize_automation_task_pools(ctx))
                _loop.close()
            else:
                initialize_automation_task_pools(ctx)
        except Exception as e:
            logger.warning("Automation-Pools Init: %s", e)

        # ---- 7. Worker-Threads starten ----
        worker_specs = [
            ("TM-EventListener",      partial(event_queue_listener_sync, ctx)),
            ("TM-CtrlRouter",         partial(controller_event_router_sync, ctx)),
            ("TM-AutoRouter",         partial(automation_event_router_sync, ctx)),
            ("TM-StatePatchWorker",   partial(state_patch_worker_sync, ctx)),
            ("TM-StatusMonitor",      partial(status_monitor_sync, ctx)),
        ]

        for name, target in worker_specs:
            th = threading.Thread(target=target, name=name, daemon=False)
            workers.append(th)
            th.start()
            logger.info("TM-Worker gestartet: %s", name)

        # ---- 8. Warten auf Shutdown ----
        logger.info("TM: System bereit – warte auf Shutdown")
        while not shutdown.is_set():
            shutdown.wait(timeout=1.0)

    except Exception as e:
        logger.error("TM main_sync Fehler: %s", e, exc_info=True)

    finally:
        # ---- 9. Shutdown ----
        logger.info("TM: Shutdown gestartet...")
        shutdown.set()

        # Sentinels in alle Queues
        for q_name in ("queue_event_send", "queue_event_pc",
                        "queue_event_mbc", "queue_event_mba"):
            q = _ctx_get(ctx, q_name)
            _queue_put_safe(q, SENTINEL)

        ctrl_queues = _ctx_get(ctx, "controller_event_queues", {})
        for q in ctrl_queues.values():
            _queue_put_safe(q, SENTINEL)

        # Worker-Threads joinen
        for th in workers:
            if th.is_alive():
                try:
                    th.join(timeout=3.0)
                except Exception:
                    pass
            if th.is_alive():
                logger.warning("TM-Worker %s reagiert nicht", th.name)

        # Event-Bus stoppen
        event_bus = _ctx_get(ctx, "event_bus")
        if event_bus:
            try:
                event_bus["stop"]()
            except Exception:
                pass

        # Controller stoppen
        ctrl_threads = _ctx_get(ctx, "controller_threads", {})
        for cid in list(ctrl_threads.keys()):
            _stop_controller_sync(ctx, cid)

        # Automation stoppen
        try:
            from src.core.async_thread_management import shutdown_automation_task_pools
            shutdown_automation_task_pools(ctx, wait=True)
        except Exception:
            pass

        # Connection Layer stoppen
        if shutdown_connection_layer is not None:
            try:
                shutdown_connection_layer(ctx, join_timeout_s=5.0)
            except Exception:
                pass

        logger.info("TM: Shutdown abgeschlossen")


# =============================================================================
# Sync Controller Initialize / Stop
# =============================================================================

def _initialize_controllers_sync(ctx):
    """Synchrone Controller-Initialisierung."""
    try:
        from src.core.async_thread_management import (
            resolve_controllers_for_node,
            create_controller_thread,
        )
    except ImportError:
        logger.error("TM-Import fehlgeschlagen für Controller-Init")
        return

    try:
        from src.libraries._syncing_generic_library import snapshot_master
        sync_ctx = _ctx_get(ctx, "sync_ctx")
        if sync_ctx is not None:
            master_snap = snapshot_master(sync_ctx)
            controllers_all = master_snap.get("controllers", []) or []
            eligible = resolve_controllers_for_node(master_snap, _ctx_get(ctx, "node_id"))
        else:
            config_lock = _ctx_get(ctx, "config_lock")
            with config_lock:
                config_data = _ctx_get(ctx, "config_data") or {}
                controllers_all = config_data.get("controllers", []) or []
            eligible = resolve_controllers_for_node(config_data, _ctx_get(ctx, "node_id"))
    except Exception:
        config_lock = _ctx_get(ctx, "config_lock")
        with config_lock:
            controllers_all = (_ctx_get(ctx, "config_data") or {}).get("controllers", []) or []
        eligible = set(int(c.get("controller_id", -1)) for c in controllers_all)

    controllers = [c for c in controllers_all if int(c.get("controller_id", -1)) in eligible]

    logger.info("TM: Initialisiere %d Controller (sync)", len(controllers))

    ctrl_threads = _ctx_get(ctx, "controller_threads", {})
    active_ctrls = _ctx_get(ctx, "active_controllers", set())

    for controller_data in controllers:
        cid = int(controller_data["controller_id"])
        if cid in ctrl_threads:
            continue

        # Connection sicherstellen
        if add_controller_connection is not None:
            rq = get_request_queue_for_controller(ctx, cid)
            if rq is None:
                add_controller_connection(ctx, cid, controller_data)

        thread = create_controller_thread(ctx, cid, controller_data)
        ctrl_threads[cid] = thread
        active_ctrls.add(cid)
        thread.start()

        evt = make_event(
            "CONTROLLER_STARTED",
            payload={
                "controller_id": cid,
                "device_name": controller_data.get("device_name"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
        _queue_put_safe(_ctx_get(ctx, "queue_event_send"), evt)


def _stop_controller_sync(ctx, controller_id):
    """Synchrones Stoppen eines Controller-Threads."""
    ctrl_threads = _ctx_get(ctx, "controller_threads", {})
    if controller_id not in ctrl_threads:
        return

    # Shutdown-Signal
    shutdown_events = _ctx_get(ctx, "controller_shutdown_events", {})
    ev = shutdown_events.get(controller_id)
    if ev:
        ev.set()

    # Sentinels
    q_key = "controller_event_{}_queue".format(controller_id)
    ctrl_queues = _ctx_get(ctx, "controller_event_queues", {})
    q = ctrl_queues.get(q_key)
    for pill in (SENTINEL, {"event_type": "__STOP__", "_sentinel": True}, None):
        _queue_put_safe(q, pill)

    # Thread joinen
    th = ctrl_threads.get(controller_id)
    if th and th.is_alive():
        try:
            th.join(timeout=2.0)
        except Exception:
            pass

    # Cleanup
    ctrl_threads.pop(controller_id, None)
    shutdown_events.pop(controller_id, None)
    ctrl_queues.pop(q_key, None)
    _ctx_get(ctx, "active_controllers", set()).discard(controller_id)

    # Connection Cleanup
    if remove_controller_connection is not None:
        try:
            remove_controller_connection(ctx, controller_id)
        except Exception:
            pass

    try:
        from src.core.async_thread_management import remove_pairs_for_controller
        remove_pairs_for_controller(ctx, controller_id)
    except Exception:
        pass


# =============================================================================
# 9. run_thread_management_sync – Neuer Entry-Point
# =============================================================================

def run_thread_management_sync(
    *,
    resources=None,
    config_data,
    config_lock,
    sensor_values,
    sensor_lock,
    queue_event_send,
    queue_event_mba=None,
    queue_event_mbc=None,
    queue_event_pc=None,
    queue_ipc=None,
    num_automation=12,
    node_id=None,
    local_sensor_values=None,
    local_sensor_lock=None,
    local_actuator_values=None,
    local_actuator_lock=None,
    shutdown_event=None,
    **kwargs,
):
    """Synchroner Entry-Point für das Thread-Management.

    Ersetzt run_thread_management() – KEIN asyncio Event-Loop mehr!
    Kann direkt als Thread-Target oder aus main() aufgerufen werden.
    """
    try:
        logger.info("TM: Starte Thread-Management (v4.0 – 100%% sync)")

        # Context erstellen (importiert aus bestehendem TM)
        from src.core.async_thread_management import create_thread_management_context
        ctx = create_thread_management_context(
            resources=resources,
            config_data=config_data,
            config_lock=config_lock,
            sensor_values=sensor_values,
            sensor_lock=sensor_lock,
            queue_event_send=queue_event_send,
            queue_event_mba=queue_event_mba or Queue(),
            queue_event_mbc=queue_event_mbc,
            queue_event_pc=queue_event_pc,
            num_automation=num_automation,
            node_id=node_id,
            local_sensor_values=local_sensor_values,
            local_sensor_lock=local_sensor_lock,
            local_actuator_values=local_actuator_values,
            local_actuator_lock=local_actuator_lock,
            shutdown_event=shutdown_event,
        )

        # Connection Layer Felder ergänzen
        _ctx_set(ctx, "connection_registry", None)
        _ctx_set(ctx, "conn_request_queues", {})
        _ctx_set(ctx, "controller_conn_map", {})
        _ctx_set(ctx, "connection_event_queue", None)

        # Hauptlogik (synchron, blockierend)
        main_sync(ctx)

    except KeyboardInterrupt:
        logger.info("TM: KeyboardInterrupt")
    except Exception as e:
        logger.error("TM: Unerwarteter Fehler: %s", e, exc_info=True)
    finally:
        logger.info("TM: Thread-Management beendet")
