# -*- coding: utf-8 -*-

# src/core/worker_gateway.py
#
# Abstraktionsschicht zwischen internem Event-Router und generic_mqtt_thread.
# Vollendet die bereits halbfertige Telemetry-Pipeline aus sync_event_router.py
# (Routen Z.791-793). Funktionaler Stil, ctx-basiert, stilkonform zu
# generic_mqtt_thread.py und _controller_thread.py.
#
# Konventionen:
#   * Kein OOP, keine Decorators, keine lambdas (functools.partial wo nötig)
#   * Module-level functions, State-Dict als ctx
#   * Thread-safe auf Python 3.14 free-threading
#   * Stop-Marker-Protokoll kompatibel zu generic_mqtt_thread
#   * Envelope-Format via make_transport_envelope aus _contracts.py
#   * ACL-Enforcement via authorize_edge_contract aus _acl.py

import copy
import json
import threading
import time
import uuid

from functools import partial
from queue import Queue, Empty, Full

from src.libraries.edge._contracts import (
    make_transport_envelope,
    normalize_transport_envelope,
    validate_transport_envelope,
    normalize_application_contract,
    build_error_contract,
)
from src.libraries.edge._acl import authorize_edge_contract


# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------

STOP_MARKERS = frozenset(("__STOP__",))

DEFAULT_LOOP_TIMEOUT_S = 0.25
DEFAULT_MQTT_PUT_TIMEOUT_S = 0.5
DEFAULT_MAX_PAYLOAD_BYTES = 262144
DEFAULT_QOS = 1
DEFAULT_RETAIN = False
DEFAULT_TOPIC_ROOT = "xserver/v1"
DEFAULT_METRICS_INTERVAL_S = 60
DEFAULT_DLQ_MAX_SIZE = 1000

GATEWAY_SOURCE_NAME = "worker_gateway"


# ---------------------------------------------------------------------------
# Basis-Helper (dupliziert aus generic_mqtt_thread für Standalone-Kompatibilität)
# ---------------------------------------------------------------------------


def _now_ts():
    return time.time()


def _make_id():
    return str(uuid.uuid4())


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


def _safe_int(value, default):
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value, default):
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "yes", "on", "y"):
            return True
        if text in ("0", "false", "no", "off", "n", ""):
            return False
    return bool(default)


def _safe_json_loads(payload):
    if payload is None:
        return None
    if isinstance(payload, (dict, list)):
        return payload
    try:
        if isinstance(payload, (bytes, bytearray)):
            return json.loads(payload.decode("utf-8", errors="replace"))
        return json.loads(_safe_str(payload, ""))
    except Exception:
        return None


def _json_dumps(payload):
    try:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return json.dumps({"__unserializable__": True}, ensure_ascii=False)


def _queue_get(queue_obj, timeout_s):
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


def _queue_put(queue_obj, item, timeout_s):
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


def _queue_qsize(queue_obj):
    if queue_obj is None:
        return 0
    try:
        return int(queue_obj.qsize())
    except Exception:
        return 0


def _is_stop_message(payload):
    if payload is None:
        return False
    if isinstance(payload, str):
        return payload in STOP_MARKERS
    if isinstance(payload, dict):
        marker_type = _safe_str(payload.get("_type") or payload.get("event_type"), "")
        if marker_type in STOP_MARKERS:
            return True
        if _safe_bool(payload.get("_sentinel"), False) and marker_type in STOP_MARKERS:
            return True
    return False


# ---------------------------------------------------------------------------
# Config-Extraktion
# ---------------------------------------------------------------------------


def _extract_gateway_cfg(settings):
    """Extrahiert den worker_gateway-Block aus dem app_config-Settings-Dict.

    Fällt auf sichere Defaults zurück wenn Keys fehlen. Keine Exceptions nach oben.
    """
    if not isinstance(settings, dict):
        settings = {}

    gw = settings.get("worker_gateway")
    if not isinstance(gw, dict):
        gw = {}

    network = settings.get("network") if isinstance(settings.get("network"), dict) else {}
    mqtt_client = network.get("mqtt_client") if isinstance(network.get("mqtt_client"), dict) else {}

    topic_root_override = gw.get("topic_root_override")
    if topic_root_override in (None, "", "null"):
        topic_root = _safe_str(mqtt_client.get("topic_root"), DEFAULT_TOPIC_ROOT) or DEFAULT_TOPIC_ROOT
    else:
        topic_root = _safe_str(topic_root_override, DEFAULT_TOPIC_ROOT) or DEFAULT_TOPIC_ROOT

    return {
        "enabled": _safe_bool(gw.get("enabled"), False),
        "loop_timeout_s": _safe_float(gw.get("loop_timeout_s"), DEFAULT_LOOP_TIMEOUT_S),
        "mqtt_out_put_timeout_s": _safe_float(gw.get("mqtt_out_put_timeout_s"), DEFAULT_MQTT_PUT_TIMEOUT_S),
        "max_payload_bytes": _safe_int(gw.get("max_payload_bytes"), DEFAULT_MAX_PAYLOAD_BYTES),
        "drop_unmapped_event_types": _safe_bool(gw.get("drop_unmapped_event_types"), True),
        "default_qos": _safe_int(gw.get("default_qos"), DEFAULT_QOS),
        "default_retain": _safe_bool(gw.get("default_retain"), DEFAULT_RETAIN),
        "topic_root": topic_root,
        "acl_enforcement": _safe_bool(gw.get("acl_enforcement"), True),
        "acl_default_policy": _safe_str(gw.get("acl_default_policy"), "deny") or "deny",
        "dlq_enabled": _safe_bool(gw.get("dlq_enabled"), True),
        "dlq_max_size": _safe_int(gw.get("dlq_max_size"), DEFAULT_DLQ_MAX_SIZE),
        "metrics_log_interval_s": _safe_int(gw.get("metrics_log_interval_s"), DEFAULT_METRICS_INTERVAL_S),
    }


# ---------------------------------------------------------------------------
# Topic-Map Build (aus event_types-Block in config_data)
# ---------------------------------------------------------------------------


def _read_config_data(resources):
    """Liest config_data aus dem resources-Dict — zero-copy, lock-aware."""
    if not isinstance(resources, dict):
        return {}

    entry = resources.get("config_data")
    if not isinstance(entry, dict):
        return {}

    data = entry.get("data") if "data" in entry else entry
    lock = entry.get("lock") if isinstance(entry, dict) else None

    if lock is not None:
        try:
            with lock:
                return _deepcopy_safe(data if isinstance(data, dict) else {})
        except Exception:
            return {}

    return _deepcopy_safe(data if isinstance(data, dict) else {})


def _build_topic_map_entries(config_data):
    """Baut topic_map als List-of-Tuples aus dem event_types-Block.

    Format: [(event_name_upper, gateway_block_dict), ...]
    Nur Einträge MIT gateway-Subblock werden aufgenommen.
    """
    entries = []

    event_types = config_data.get("event_types") if isinstance(config_data, dict) else None
    if not isinstance(event_types, list):
        return entries

    for item in event_types:
        if not isinstance(item, dict):
            continue
        gw_block = item.get("gateway")
        if not isinstance(gw_block, dict):
            continue

        name = _safe_str(item.get("name"), "").strip().upper()
        if not name:
            continue

        normalized = {
            "direction": _safe_str(gw_block.get("direction"), "outbound").strip().lower() or "outbound",
            "topic": _safe_str(gw_block.get("topic"), "").strip(),
            "qos": _safe_int(gw_block.get("qos"), DEFAULT_QOS),
            "retain": _safe_bool(gw_block.get("retain"), False),
            "kind": _safe_str(gw_block.get("kind"), "event").strip().lower() or "event",
            "schema_ref": _safe_str(gw_block.get("schema_ref"), "").strip(),
            "acl_action": _safe_str(gw_block.get("acl_action"), "read").strip().lower() or "read",
            "event_id": _safe_int(item.get("event_id"), 0),
            "version": _safe_int(item.get("version"), 1),
            "category": _safe_str(item.get("category"), "").strip(),
            "severity": _safe_str(item.get("severity"), "INFO").strip(),
        }

        if not normalized["topic"]:
            continue

        entries.append((name, normalized))

    return entries


def _build_topic_lookups(topic_map_entries):
    """Erzeugt zwei O(1)-Lookup-Dicts aus der topic_map-Liste:
        - by_event_type: event_name -> gateway_block (für outbound)
        - by_mqtt_topic: relative_topic -> (event_name, gateway_block) (für inbound)
    """
    by_event_type = {}
    by_mqtt_topic = {}

    for name, block in topic_map_entries:
        direction = block.get("direction", "outbound")
        if direction in ("outbound", "bidirectional"):
            by_event_type[name] = block
        if direction in ("inbound", "bidirectional"):
            by_mqtt_topic[block.get("topic", "")] = (name, block)

    return by_event_type, by_mqtt_topic


# ---------------------------------------------------------------------------
# ctx-Builder
# ---------------------------------------------------------------------------


def build_gateway_ctx(runtime_state, queue_tools, runtime_bundle, logger=None):
    """Baut den Gateway-ctx analog zu build_thread_specs-Konventionen.

    Args:
        runtime_state: dict mit node_id, resources, config_data, etc.
        queue_tools: dict mit queues_dict, pick_q, alias_q
        runtime_bundle: dict mit settings (app_config-Snapshot)
        logger: optional Logger-Instanz

    Returns:
        ctx-dict — als erstes Argument in run_worker_gateway(ctx) zu übergeben.
    """
    settings = runtime_bundle.get("settings", {}) if isinstance(runtime_bundle, dict) else {}
    queues_dict = queue_tools.get("queues_dict", {}) if isinstance(queue_tools, dict) else {}

    cfg = _extract_gateway_cfg(settings)
    resources = runtime_state.get("resources") if isinstance(runtime_state, dict) else {}
    node_id = _safe_str(runtime_state.get("node_id") if isinstance(runtime_state, dict) else "", "").strip()

    config_data = _read_config_data(resources)
    topic_map_entries = _build_topic_map_entries(config_data)
    lookup_outbound, lookup_inbound = _build_topic_lookups(topic_map_entries)

    ctx = {
        "cfg": cfg,
        "identity": {
            "node_id": node_id,
            "source": GATEWAY_SOURCE_NAME,
        },
        "queues": {
            "ingress": queues_dict.get("queue_event_ti"),
            "mqtt_out": queues_dict.get("queue_gateway_out"),
            "mqtt_in": queues_dict.get("queue_gateway_in"),
            "event_send": queues_dict.get("queue_event_send"),
        },
        "topic_map": topic_map_entries,
        "lookup_outbound": lookup_outbound,
        "lookup_inbound": lookup_inbound,
        "resources": resources,
        "metrics": {
            "ingress_get": 0,
            "ingress_drops": 0,
            "mqtt_out_put": 0,
            "mqtt_out_drops": 0,
            "mqtt_in_get": 0,
            "event_bus_emit": 0,
            "event_bus_drops": 0,
            "validation_errors": 0,
            "acl_denies": 0,
            "unmapped_event_types_dropped": 0,
            "dlq_count": 0,
            "last_error": None,
            "started_at": None,
        },
        "seen_unmapped_types": set(),
        "locks": {
            "metrics": threading.Lock(),
            "seen": threading.Lock(),
        },
        "shutdown_event": threading.Event(),
        "dlq": [],
        "logger": logger,
    }
    return ctx


# ---------------------------------------------------------------------------
# Metrics-Helpers
# ---------------------------------------------------------------------------


def _metric_inc(ctx, key, delta=1):
    lock = ctx.get("locks", {}).get("metrics")
    if lock is None:
        try:
            ctx["metrics"][key] = int(ctx["metrics"].get(key, 0)) + int(delta)
        except Exception:
            pass
        return
    with lock:
        try:
            ctx["metrics"][key] = int(ctx["metrics"].get(key, 0)) + int(delta)
        except Exception:
            pass


def _metric_set(ctx, key, value):
    lock = ctx.get("locks", {}).get("metrics")
    if lock is None:
        ctx["metrics"][key] = value
        return
    with lock:
        ctx["metrics"][key] = value


def get_gateway_metrics_snapshot(ctx):
    """Thread-safe Snapshot der Metrics — für _metrics_system.py-Integration."""
    lock = ctx.get("locks", {}).get("metrics")
    if lock is None:
        return dict(ctx.get("metrics", {}))
    with lock:
        return dict(ctx.get("metrics", {}))


# ---------------------------------------------------------------------------
# Topic-Resolution (dynamisches Gate)
# ---------------------------------------------------------------------------


def _resolve_topic_mapping(ctx, event_type):
    """Dynamisches Gate für outbound event_type → gateway-block.

    Aktuell: strict lookup, None = drop.
    Erweiterungs-Point für default_topic_fallback.
    """
    if not event_type:
        return None
    key = _safe_str(event_type, "").strip().upper()
    if not key:
        return None
    return ctx.get("lookup_outbound", {}).get(key)


def _log_unmapped_once(ctx, event_type):
    lock = ctx.get("locks", {}).get("seen")
    seen = ctx.get("seen_unmapped_types")
    if seen is None:
        return
    with lock if lock is not None else threading.Lock():
        if event_type in seen:
            return
        seen.add(event_type)
    logger = ctx.get("logger")
    if logger is not None:
        try:
            logger.debug("[worker_gateway] unmapped event_type=%s — drop (first occurrence)", event_type)
        except Exception:
            pass


def _build_full_topic(ctx, relative_topic):
    """Baut das vollständige MQTT-Topic aus topic_root/node_id/relative_topic."""
    topic_root = _safe_str(ctx.get("cfg", {}).get("topic_root"), DEFAULT_TOPIC_ROOT).strip("/")
    node_id = _safe_str(ctx.get("identity", {}).get("node_id"), "").strip("/")
    rel = _safe_str(relative_topic, "").strip("/")
    parts = [p for p in (topic_root, node_id, rel) if p]
    return "/".join(parts)


# ---------------------------------------------------------------------------
# Ingress-Handler (Router → Gateway)
# ---------------------------------------------------------------------------


def gateway_ingress_handler(event_payload, context=None):
    """Router-Handler, 1:1 kompatibel zu make_route_entry.

    Dünn, nicht-blockierend. Der eigentliche Forward nach queue_event_ti
    erfolgt bereits durch die Route-Table im sync_event_router — diese
    Funktion ist nur der Rückfall-Hook falls der Router sie direkt aufruft.
    """
    # Das Event liegt bereits in queue_event_ti (via Route-Dispatch).
    # Diese Funktion ist intentional ein No-Op-Ack, damit der Router
    # einen definierten Handler hat. Der Gateway-Thread drained die Queue.
    return True


# ---------------------------------------------------------------------------
# Outbound: Ingress-Event → MQTT-Envelope → queue_gateway_out
# ---------------------------------------------------------------------------


def _extract_event_type(item):
    if not isinstance(item, dict):
        return ""
    for key in ("event_type", "_type", "type", "name"):
        value = item.get(key)
        if value:
            return _safe_str(value, "").strip()
    return ""


def _check_payload_size(ctx, payload):
    limit = _safe_int(ctx.get("cfg", {}).get("max_payload_bytes"), DEFAULT_MAX_PAYLOAD_BYTES)
    if limit <= 0:
        return True
    try:
        encoded = _json_dumps(payload)
        return len(encoded.encode("utf-8")) <= limit
    except Exception:
        return False


def _process_ingress_item(ctx, item):
    """Verarbeitet ein Item aus queue_event_ti → queue_gateway_out."""
    if _is_stop_message(item):
        return "stop"

    _metric_inc(ctx, "ingress_get")

    event_type = _extract_event_type(item)
    if not event_type:
        _metric_inc(ctx, "ingress_drops")
        return "dropped"

    mapping = _resolve_topic_mapping(ctx, event_type)
    if mapping is None:
        if _safe_bool(ctx.get("cfg", {}).get("drop_unmapped_event_types"), True):
            _metric_inc(ctx, "unmapped_event_types_dropped")
            _log_unmapped_once(ctx, event_type.upper())
            return "unmapped"

    if not _check_payload_size(ctx, item):
        _metric_inc(ctx, "validation_errors")
        _metric_set(ctx, "last_error", "payload_exceeds_max_bytes")
        return "too_large"

    full_topic = _build_full_topic(ctx, mapping.get("topic") if mapping else event_type)
    envelope = make_transport_envelope(
        message_type=mapping.get("kind", "event") if mapping else "event",
        api_route="worker.{0}.{1}".format(
            _safe_str(mapping.get("category", "") if mapping else "", "event"),
            event_type.lower(),
        ),
        payload=item if isinstance(item, dict) else {"raw": item},
        source=GATEWAY_SOURCE_NAME,
        source_node=ctx.get("identity", {}).get("node_id", ""),
        destination=full_topic,
        broadcast=False,
        meta={
            "mqtt": {
                "topic": full_topic,
                "qos": mapping.get("qos", DEFAULT_QOS) if mapping else DEFAULT_QOS,
                "retain": mapping.get("retain", DEFAULT_RETAIN) if mapping else DEFAULT_RETAIN,
            },
            "gateway": {
                "event_type": event_type,
                "mapped": mapping is not None,
            },
        },
    )

    timeout = _safe_float(ctx.get("cfg", {}).get("mqtt_out_put_timeout_s"), DEFAULT_MQTT_PUT_TIMEOUT_S)
    put_ok = _queue_put(ctx.get("queues", {}).get("mqtt_out"), envelope, timeout)
    if put_ok:
        _metric_inc(ctx, "mqtt_out_put")
        return "forwarded"

    _metric_inc(ctx, "mqtt_out_drops")
    _append_to_dlq(ctx, {"kind": "outbound", "envelope": envelope})
    return "dropped_backpressure"


def _append_to_dlq(ctx, entry):
    if not _safe_bool(ctx.get("cfg", {}).get("dlq_enabled"), True):
        return
    dlq = ctx.get("dlq")
    if dlq is None:
        return
    limit = _safe_int(ctx.get("cfg", {}).get("dlq_max_size"), DEFAULT_DLQ_MAX_SIZE)
    dlq.append(entry)
    if len(dlq) > limit:
        del dlq[0:len(dlq) - limit]
    _metric_inc(ctx, "dlq_count")


# ---------------------------------------------------------------------------
# Inbound: MQTT-Message → ACL → intern emit
# ---------------------------------------------------------------------------


def _emit_to_event_bus(ctx, event_type, payload):
    """Schreibt ein Event in queue_event_send — der Router dispatcht es dann weiter."""
    queue_obj = ctx.get("queues", {}).get("event_send")
    if queue_obj is None:
        _metric_inc(ctx, "event_bus_drops")
        return False

    item = {
        "event_type": _safe_str(event_type, "worker_gateway_inbound"),
        "_source": GATEWAY_SOURCE_NAME,
        "_ts": _now_ts(),
        "payload": payload if payload is not None else {},
    }
    ok = _queue_put(queue_obj, item, _safe_float(ctx.get("cfg", {}).get("mqtt_out_put_timeout_s"), 0.5))
    if ok:
        _metric_inc(ctx, "event_bus_emit")
    else:
        _metric_inc(ctx, "event_bus_drops")
    return ok


def gateway_handle_mqtt_inbound(ctx, mqtt_message):
    """Verarbeitet eine Inbound-Message aus queue_gateway_in.

    Ablauf:
      1. Envelope normalisieren + validieren
      2. Contract extrahieren
      3. ACL-Check (falls acl_enforcement aktiv)
      4. Routing nach kind: command/write → state_write, query → state_read, event → mapped
      5. Metrics
    """
    if _is_stop_message(mqtt_message):
        return "stop"

    _metric_inc(ctx, "mqtt_in_get")

    envelope_result = validate_transport_envelope(mqtt_message)
    if not envelope_result.get("ok", False):
        _metric_inc(ctx, "validation_errors")
        _metric_set(ctx, "last_error", "invalid_envelope:" + ",".join(envelope_result.get("errors", [])))
        return "invalid_envelope"

    envelope = envelope_result["envelope"]
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}

    contract = normalize_application_contract(payload)

    if _safe_bool(ctx.get("cfg", {}).get("acl_enforcement"), True):
        acl_report = authorize_edge_contract(
            ctx.get("resources", {}),
            contract,
            envelope,
            node_id=ctx.get("identity", {}).get("node_id", ""),
            default_policy=_safe_str(ctx.get("cfg", {}).get("acl_default_policy"), "deny"),
        )
        if not acl_report.get("ok", True):
            _metric_inc(ctx, "acl_denies")
            logger = ctx.get("logger")
            if logger is not None:
                try:
                    logger.warning(
                        "[worker_gateway] ACL deny: reason=%s actor=%s action=%s",
                        acl_report.get("reason"),
                        acl_report.get("actor"),
                        acl_report.get("context", {}).get("action"),
                    )
                except Exception:
                    pass
            return "acl_denied"

    kind = _safe_str(contract.get("kind"), "event").strip().lower()
    operation = _safe_str(contract.get("operation"), "").strip().lower()

    if kind == "command" or operation in ("write", "patch", "replace", "upsert", "update", "set"):
        _emit_to_event_bus(ctx, "state_write", contract)
        return "routed_write"
    if kind == "query" or operation in ("read", "query", "get"):
        _emit_to_event_bus(ctx, "state_read", contract)
        return "routed_read"

    # Default: event → mapped or generic
    topic = _safe_str(envelope.get("destination"), "")
    inbound_match = ctx.get("lookup_inbound", {}).get(topic)
    event_type = inbound_match[0].lower() if inbound_match else "gateway_inbound_event"
    _emit_to_event_bus(ctx, event_type, payload)
    return "routed_event"


# ---------------------------------------------------------------------------
# Main-Loop (Thread Entry-Point)
# ---------------------------------------------------------------------------


def run_worker_gateway(ctx):
    """Main-Loop des worker_gateway-Threads.

    Ingress-Pfad (queue_event_ti → queue_gateway_out) ist die Hot-Path-Schleife.
    Inbound-Pfad (queue_gateway_in → event_send) wird als non-blocking poll
    in derselben Schleife bedient — kein zweiter Thread nötig.
    """
    if not _safe_bool(ctx.get("cfg", {}).get("enabled"), False):
        logger = ctx.get("logger")
        if logger is not None:
            try:
                logger.info("[worker_gateway] disabled via config — exit immediately")
            except Exception:
                pass
        return

    _metric_set(ctx, "started_at", _now_ts())
    logger = ctx.get("logger")
    if logger is not None:
        try:
            logger.info(
                "[worker_gateway] started node=%s topic_root=%s mapped_outbound=%d mapped_inbound=%d",
                ctx.get("identity", {}).get("node_id"),
                ctx.get("cfg", {}).get("topic_root"),
                len(ctx.get("lookup_outbound", {})),
                len(ctx.get("lookup_inbound", {})),
            )
        except Exception:
            pass

    shutdown_event = ctx.get("shutdown_event")
    cfg = ctx.get("cfg", {})
    loop_timeout = _safe_float(cfg.get("loop_timeout_s"), DEFAULT_LOOP_TIMEOUT_S)
    metrics_interval = _safe_float(cfg.get("metrics_log_interval_s"), DEFAULT_METRICS_INTERVAL_S)
    next_metrics_log = _now_ts() + metrics_interval

    ingress_q = ctx.get("queues", {}).get("ingress")
    mqtt_in_q = ctx.get("queues", {}).get("mqtt_in")

    stop_requested = False

    while not stop_requested:
        if shutdown_event is not None and shutdown_event.is_set():
            break

        # --- Hot-Path: Ingress (blocking mit Timeout) ---
        ingress_item = _queue_get(ingress_q, loop_timeout)
        if ingress_item is not None:
            result = _process_ingress_item(ctx, ingress_item)
            if result == "stop":
                stop_requested = True
                break

        # --- Parallel: Inbound (non-blocking poll, bis Queue leer) ---
        while True:
            inbound_item = _queue_get(mqtt_in_q, 0.01)
            if inbound_item is None:
                break
            result = gateway_handle_mqtt_inbound(ctx, inbound_item)
            if result == "stop":
                stop_requested = True
                break

        # --- Housekeeping: Metrics-Log ---
        now = _now_ts()
        if now >= next_metrics_log:
            _log_metrics_snapshot(ctx)
            next_metrics_log = now + metrics_interval

    # --- Graceful Drain ---
    _drain_remaining(ctx)
    _log_metrics_snapshot(ctx)

    if logger is not None:
        try:
            logger.info("[worker_gateway] stopped gracefully")
        except Exception:
            pass


def _drain_remaining(ctx):
    """Drained verbleibende Ingress-Items bis Queue leer oder Timeout erreicht."""
    ingress_q = ctx.get("queues", {}).get("ingress")
    deadline = _now_ts() + 2.5
    drained = 0
    while _now_ts() < deadline:
        item = _queue_get(ingress_q, 0.05)
        if item is None:
            break
        if _is_stop_message(item):
            continue
        _process_ingress_item(ctx, item)
        drained += 1
    if drained > 0:
        logger = ctx.get("logger")
        if logger is not None:
            try:
                logger.info("[worker_gateway] drained %d items on shutdown", drained)
            except Exception:
                pass


def _log_metrics_snapshot(ctx):
    logger = ctx.get("logger")
    if logger is None:
        return
    snap = get_gateway_metrics_snapshot(ctx)
    try:
        logger.info(
            "[worker_gateway] metrics ingress_get=%d mqtt_out_put=%d mqtt_out_drops=%d "
            "mqtt_in_get=%d event_bus_emit=%d acl_denies=%d unmapped_dropped=%d dlq=%d",
            snap.get("ingress_get", 0),
            snap.get("mqtt_out_put", 0),
            snap.get("mqtt_out_drops", 0),
            snap.get("mqtt_in_get", 0),
            snap.get("event_bus_emit", 0),
            snap.get("acl_denies", 0),
            snap.get("unmapped_event_types_dropped", 0),
            snap.get("dlq_count", 0),
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Public Entry-Points (für thread_specs / component_targets)
# ---------------------------------------------------------------------------


def run_worker_gateway_thread(
    node_id=None,
    resources=None,
    queue_event_ti=None,
    queue_gateway_in=None,
    queue_gateway_out=None,
    queue_event_send=None,
    runtime_bundle=None,
    logger=None,
    shutdown_event=None,
):
    """Public-Entry-Point für thread_specs.py (component_targets['run_worker_gateway']).

    Akzeptiert die vom Thread-Factory injizierten Queues direkt als kwargs —
    kompatibel zum pick_q-Pattern aus build_thread_specs.
    """
    runtime_state = {"node_id": node_id, "resources": resources}
    queue_tools = {
        "queues_dict": {
            "queue_event_ti": queue_event_ti,
            "queue_gateway_in": queue_gateway_in,
            "queue_gateway_out": queue_gateway_out,
            "queue_event_send": queue_event_send,
        },
    }

    ctx = build_gateway_ctx(runtime_state, queue_tools, runtime_bundle or {}, logger=logger)

    if shutdown_event is not None:
        ctx["shutdown_event"] = shutdown_event

    run_worker_gateway(ctx)
