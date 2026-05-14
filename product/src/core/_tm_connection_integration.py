# -*- coding: utf-8 -*-

# src/core/_tm_connection_integration.py

"""TM ↔ Connection Layer Integration.

Verantwortlichkeit:
    - Connection Registry im TM-Context initialisieren
    - Adapter registrieren (Modbus TCP, etc.)
    - Connection Threads für alle Controller starten/stoppen
    - conn_key ↔ controller_id Mapping verwalten
    - Request-Queues für Controller-Threads bereitstellen
    - Live-Rekonfiguration (Controller hinzufügen/entfernen zur Laufzeit)

Wird von async_thread_management.py importiert und aufgerufen.
Kein OOP, kein Decorator, kein Lambda, kein asyncio.
"""


import threading
import time
import logging

from functools import partial
from queue import Queue


logger = logging.getLogger(__name__)


# =============================================================================
# Imports: Connection Layer (Schicht 1)
# =============================================================================

try:
    from src.adapters import register_all_adapters
    from src.adapters._adapter_interface import (
        build_conn_key,
        create_connection_context,
    )
    from src.adapters._connection_registry import (
        create_connection_registry,
        create_adapter_for_protocol,
        registry_register,
        registry_unregister,
        registry_get,
        registry_get_stop_event,
        registry_set_thread,
        registry_list_keys,
        registry_snapshot,
        registry_stop_all,
    )
    from src.adapters._connection_thread import (
        start_connection_thread,
        stop_connection_thread,
        CONNECTION_STOP,
    )
except ImportError:
    from adapters import register_all_adapters
    from adapters._adapter_interface import (
        build_conn_key,
        create_connection_context,
    )
    from adapters._connection_registry import (
        create_connection_registry,
        create_adapter_for_protocol,
        registry_register,
        registry_unregister,
        registry_get,
        registry_get_stop_event,
        registry_set_thread,
        registry_list_keys,
        registry_snapshot,
        registry_stop_all,
    )
    from adapters._connection_thread import (
        start_connection_thread,
        stop_connection_thread,
        CONNECTION_STOP,
    )


# =============================================================================
# Initialisierung (einmalig beim TM-Start)
# =============================================================================

def _safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _resolve_managed_controller_ids(config_data, node_id=None):
    """Resolve controller ownership with the same semantics as TM."""
    cfg = config_data if isinstance(config_data, dict) else {}
    controllers = cfg.get("controllers") or []
    if not node_id:
        return {_safe_int(ctrl.get("controller_id")) for ctrl in controllers if isinstance(ctrl, dict) and _safe_int(ctrl.get("controller_id")) is not None}

    local_node = cfg.get("local_node")
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

        managed = set()
        for controller_id in assigned:
            cid = _safe_int(controller_id, None)
            if cid is not None:
                managed.add(cid)
        return managed

    return {
        _safe_int(ctrl.get("controller_id"))
        for ctrl in controllers
        if isinstance(ctrl, dict) and _safe_int(ctrl.get("controller_id")) is not None
    }


def init_connection_layer(ctx):
    """Initialisiert das Connection Layer im TM-Context.

    Muss VOR initialize_controllers() aufgerufen werden.

    Schritte:
        1. Adapter-Factories registrieren (Modbus TCP, etc.)
        2. Connection Registry erstellen
        3. Connection Threads für alle Controller starten
        4. conn_key → request_queue Mapping aufbauen

    Speichert im ctx:
        - ctx.connection_registry
        - ctx.conn_request_queues    (conn_key → Queue)
        - ctx.controller_conn_map    (controller_id → conn_key)
        - ctx.connection_event_queue (Events von Connection Threads)
    """
    logger.info("Connection Layer wird initialisiert...")

    # 1) Adapter registrieren
    registered = register_all_adapters()
    logger.info("Registrierte Adapter: %s", registered)

    # 2) Connection Registry erstellen
    registry = create_connection_registry(name="tm_connections")
    _ctx_set(ctx, "connection_registry", registry)

    # 3) Event-Queue für Connection-Events
    conn_event_queue = Queue()
    _ctx_set(ctx, "connection_event_queue", conn_event_queue)

    # 4) Mappings initialisieren
    _ctx_set(ctx, "conn_request_queues", {})     # conn_key → Queue
    _ctx_set(ctx, "controller_conn_map", {})     # controller_id → conn_key

    # 5) Connections starten
    _start_all_connections(ctx)

    logger.info("Connection Layer initialisiert: %d Verbindungen",
                len(_ctx_get(ctx, "conn_request_queues", {})))


def _start_all_connections(ctx):
    """Startet Connection Threads für alle Controller in der Config.

    Erkennt Shared Connections (gleiche IP:Port) automatisch.
    """
    registry = _ctx_get(ctx, "connection_registry")
    conn_event_queue = _ctx_get(ctx, "connection_event_queue")
    queues = _ctx_get(ctx, "conn_request_queues")
    conn_map = _ctx_get(ctx, "controller_conn_map")

    # Config lesen (thread-safe, kompatibel mit TM v4 + v5)
    config_data = _ctx_get_config_data(ctx)
    controllers = list(config_data.get("controllers") or [])
    virtual_controllers = list(config_data.get("virtual_controllers") or [])
    node_id = _ctx_get(ctx, "node_id")
    managed_ids = _resolve_managed_controller_ids(config_data, node_id)

    for ctrl in controllers:
        cid = _safe_int(ctrl.get("controller_id"), None)
        if cid is None:
            continue
        if managed_ids is not None and cid not in managed_ids:
            continue

        if not isinstance(ctrl, dict):
            continue

        # Protokoll bestimmen
        protocol = str(ctrl.get("protocol_type") or
                       ctrl.get("bus_type") or "modbus").lower()
        if protocol == "modbus":
            protocol = "modbus_tcp"

        host = str(ctrl.get("host") or ctrl.get("ip_address") or "")
        port = int(ctrl.get("port", 502))
        unit_raw = ctrl.get("unit", "0x1")

        # Virtual Controller prüfen
        for v in virtual_controllers:
            try:
                if (int(v.get("controller_id", -1)) == cid
                        and bool(v.get("virt_active", False))):
                    host = str(v.get("host") or v.get("ip_address") or host)
                    if v.get("port") is not None:
                        port = int(v["port"])
                    break
            except Exception:
                continue

        conn_key = build_conn_key(protocol, host, port)

        # Mapping: controller_id → conn_key
        conn_map[cid] = conn_key

        # Bereits gestartet? → Shared Connection (Multiplexing)
        if conn_key in queues:
            logger.info("Shared Connection: C%d → %s", cid, conn_key)
            continue

        # Unit konvertieren
        if isinstance(unit_raw, str):
            try:
                unit_int = int(unit_raw.replace("0x", ""), 16)
            except Exception:
                unit_int = 1
        else:
            try:
                unit_int = int(unit_raw)
            except Exception:
                unit_int = 1

        # Adapter erstellen
        adapter = create_adapter_for_protocol(protocol)
        if adapter is None:
            logger.error("Kein Adapter für C%d (%s) – übersprungen", cid, protocol)
            continue

        # Connection Context
        conn_ctx = create_connection_context(
            conn_key=conn_key,
            protocol_type=protocol,
            host=host,
            port=port,
            unit=unit_int,
            adapter=adapter,
            config=ctrl.get("comm") or {},
        )

        # Event-Queue setzen
        if conn_event_queue is not None:
            conn_ctx["event_queue"] = conn_event_queue

        # Connection Thread starten
        rq = start_connection_thread(registry, conn_key, conn_ctx)
        queues[conn_key] = rq

        logger.info("Connection gestartet: C%d → %s (%s:%d)",
                     cid, conn_key, host, port)


# =============================================================================
# Request-Queue für einen Controller holen
# =============================================================================

def get_request_queue_for_controller(ctx, controller_id):
    """Gibt die Request-Queue für einen Controller zurück.

    Nutzt das controller_id → conn_key Mapping.
    Bei Shared Connections teilen sich mehrere Controller eine Queue.
    """
    controller_id = int(controller_id)
    conn_map = _ctx_get(ctx, "controller_conn_map", {})
    conn_key = conn_map.get(controller_id)

    if conn_key is None:
        logger.warning("Kein conn_key für Controller %d", controller_id)
        return None

    queues = _ctx_get(ctx, "conn_request_queues", {})
    rq = queues.get(conn_key)

    if rq is None:
        logger.warning("Keine Request-Queue für conn_key %s (C%d)",
                       conn_key, controller_id)
    return rq


# =============================================================================
# Live-Rekonfiguration: Controller hinzufügen
# =============================================================================

def add_controller_connection(ctx, controller_id, controller_data):
    """Fügt eine Verbindung für einen neuen Controller hinzu (Live).

    Falls die Verbindung (IP:Port) bereits existiert → Shared Connection.
    Falls nicht → neuer Connection Thread.
    """
    registry = _ctx_get(ctx, "connection_registry")
    queues = _ctx_get(ctx, "conn_request_queues")
    conn_map = _ctx_get(ctx, "controller_conn_map")
    conn_event_queue = _ctx_get(ctx, "connection_event_queue")

    cid = int(controller_id)
    protocol = str(controller_data.get("protocol_type") or
                   controller_data.get("bus_type") or "modbus").lower()
    if protocol == "modbus":
        protocol = "modbus_tcp"

    host = str(controller_data.get("host") or
               controller_data.get("ip_address") or "")
    port = int(controller_data.get("port", 502))

    # Virtual Check (kompatibel mit TM v4 + v5)
    config_data = _ctx_get_config_data(ctx)
    for v in config_data.get("virtual_controllers") or []:
        try:
            if (int(v.get("controller_id", -1)) == cid
                    and bool(v.get("virt_active", False))):
                host = str(v.get("host") or v.get("ip_address") or host)
                if v.get("port") is not None:
                    port = int(v["port"])
                break
        except Exception:
            continue

    conn_key = build_conn_key(protocol, host, port)
    conn_map[cid] = conn_key

    # Shared?
    if conn_key in queues:
        logger.info("Shared Connection (add): C%d → %s", cid, conn_key)
        return queues[conn_key]

    # Unit
    unit_raw = controller_data.get("unit", "0x1")
    if isinstance(unit_raw, str):
        try:
            unit_int = int(unit_raw.replace("0x", ""), 16)
        except Exception:
            unit_int = 1
    else:
        try:
            unit_int = int(unit_raw)
        except Exception:
            unit_int = 1

    adapter = create_adapter_for_protocol(protocol)
    if adapter is None:
        logger.error("Kein Adapter für C%d (%s)", cid, protocol)
        return None

    conn_ctx = create_connection_context(
        conn_key=conn_key,
        protocol_type=protocol,
        host=host,
        port=port,
        unit=unit_int,
        adapter=adapter,
        config=controller_data.get("comm") or {},
    )
    if conn_event_queue is not None:
        conn_ctx["event_queue"] = conn_event_queue

    rq = start_connection_thread(registry, conn_key, conn_ctx)
    queues[conn_key] = rq

    logger.info("Connection hinzugefügt (live): C%d → %s", cid, conn_key)
    return rq


# =============================================================================
# Live-Rekonfiguration: Controller entfernen
# =============================================================================

def remove_controller_connection(ctx, controller_id):
    """Entfernt die Verbindung eines Controllers (Live).

    Stoppt den Connection Thread NUR wenn kein anderer Controller
    diese Verbindung noch nutzt (Shared Connection Check).
    """
    registry = _ctx_get(ctx, "connection_registry")
    queues = _ctx_get(ctx, "conn_request_queues")
    conn_map = _ctx_get(ctx, "controller_conn_map")

    cid = int(controller_id)
    conn_key = conn_map.pop(cid, None)

    if conn_key is None:
        logger.info("Kein conn_key für C%d (bereits entfernt?)", cid)
        return

    # Shared Check: Nutzt noch ein anderer Controller diesen conn_key?
    still_in_use = any(
        ck == conn_key for cid2, ck in conn_map.items() if cid2 != cid
    )

    if still_in_use:
        logger.info("Connection %s noch in Verwendung (Shared) – nicht gestoppt", conn_key)
        return

    # Kein anderer Controller nutzt die Verbindung → stoppen
    stop_connection_thread(registry, conn_key)
    queues.pop(conn_key, None)

    logger.info("Connection entfernt: C%d → %s", cid, conn_key)


# =============================================================================
# Shutdown (beim TM-Stop)
# =============================================================================

def shutdown_connection_layer(ctx, *, join_timeout_s=5.0):
    """Stoppt alle Connection Threads und räumt auf.

    Muss NACH dem Stoppen der Controller-Threads aufgerufen werden.
    """
    registry = _ctx_get(ctx, "connection_registry")
    if registry is None:
        return

    logger.info("Connection Layer wird heruntergefahren...")

    registry_stop_all(registry, join_timeout_s=join_timeout_s)

    # Mappings aufräumen
    _ctx_set(ctx, "conn_request_queues", {})
    _ctx_set(ctx, "controller_conn_map", {})

    logger.info("Connection Layer heruntergefahren")


# =============================================================================
# Monitoring / Snapshot
# =============================================================================

def connection_layer_snapshot(ctx):
    """Erstellt einen Snapshot aller Verbindungen (für Status-Monitor)."""
    registry = _ctx_get(ctx, "connection_registry")
    if registry is None:
        return {}
    return registry_snapshot(registry)


def connection_layer_health(ctx):
    """Gibt einen Gesundheitsbericht zurück."""
    snapshot = connection_layer_snapshot(ctx)
    total = len(snapshot)
    connected = sum(1 for v in snapshot.values() if v.get("connected"))
    disconnected = total - connected

    conn_map = _ctx_get(ctx, "controller_conn_map", {})

    return {
        "total_connections": total,
        "connected": connected,
        "disconnected": disconnected,
        "controller_mapping": dict(conn_map),
        "connections": snapshot,
    }


# =============================================================================
# Ctx-Helper (attrbag-kompatibel)
# =============================================================================

def _ctx_get(ctx, name, default=None):
    if isinstance(ctx, dict):
        return ctx.get(name, default)
    return getattr(ctx, name, default)


def _ctx_set(ctx, name, value):
    if isinstance(ctx, dict):
        ctx[name] = value
    else:
        setattr(ctx, name, value)


def _ctx_get_config_data(ctx):
    """Liest Config aus dem Context — kompatibel mit TM v4 (config_data) und TM v5 (config_snapshot).

    TM v4/legacy: ctx.config_data (dict, direkt)
    TM v5:        ctx.config_snapshot (dict, frozen snapshot)

    Gibt immer ein dict zurück (oder {}).
    """
    config_lock = _ctx_get(ctx, "config_lock")
    if config_lock is not None:
        try:
            config_lock.acquire()
        except Exception:
            config_lock = None

    try:
        # TM v4 Pfad (direkt)
        cfg = _ctx_get(ctx, "config_data")
        if isinstance(cfg, dict) and cfg:
            return cfg

        # TM v5 Pfad (Snapshot)
        cfg = _ctx_get(ctx, "config_snapshot")
        if isinstance(cfg, dict) and cfg:
            return cfg

        return {}
    finally:
        if config_lock is not None:
            try:
                config_lock.release()
            except Exception:
                pass
