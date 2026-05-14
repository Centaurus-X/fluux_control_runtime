# -*- coding: utf-8 -*-

# src/core/sync_event_router.py

"""
Synchronous event router with ThreadPoolExecutor.

Design goals:
- Fully functional style without OOP
- Standardized queue-dict aware routing
- Sync integration via src.libraries._evt_interface (sync_queue_get/put)
- Backward compatible with the old async_ev_queue_handler signature
- Dynamic routing via function mapping and queue name resolution
- Generic broadcast and mirror rules without long if/else chains
- ThreadPoolExecutor for parallel broadcast dispatch (num_threads=4)
- Zero asyncio dependency
"""

import copy
import inspect
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from src.libraries._evt_interface import sync_queue_get, sync_queue_put

from src.core._event_priority import (
    order_targets_deterministic,
    order_targets_by_priority_domain,
    classify_target,
)
from src.core._cross_domain_resolver import (
    combine_cross_domain_targets,
    normalize_mapping as normalize_cross_domain_mapping,
)

# v18: Optionales Event-Envelope-Enrichment. Default-off. Wenn gesetzt,
# werden Events beim Broker-Forwarding mit Envelope-Feldern angereichert
# (envelope_id, message_kind, sequence_no, vector_clock, etc.).
try:
    from src.libraries._event_envelope import (
        build_broker_enrichment_fn as _eev_build_fn,
    )
except Exception:
    _eev_build_fn = None

_ENVELOPE_ENRICHMENT_FN = None


def bind_envelope_enrichment(enabled, source_ref="broker", node_id=None, trust_level="internal"):
    """v18: Bindet optional eine Envelope-Enrichment-Funktion. Wenn enabled=False
    oder die Library nicht importierbar ist, bleibt das Enrichment inaktiv.
    """
    global _ENVELOPE_ENRICHMENT_FN
    if not enabled:
        _ENVELOPE_ENRICHMENT_FN = None
        return None
    if not callable(_eev_build_fn):
        _ENVELOPE_ENRICHMENT_FN = None
        return None
    try:
        _ENVELOPE_ENRICHMENT_FN = _eev_build_fn(
            source_ref=source_ref,
            node_id=node_id,
            trust_level=trust_level,
        )
        logger.info(
            "[sync_event_router] envelope enrichment bound source_ref=%s node_id=%s",
            source_ref,
            node_id,
        )
        return _ENVELOPE_ENRICHMENT_FN
    except Exception as exc:
        logger.warning("[sync_event_router] envelope enrichment bind failed: %s", exc)
        _ENVELOPE_ENRICHMENT_FN = None
        return None


# ---------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Targets die aus abgebauten Subsystemen stammen (z.B. PSM/process_manager)
# und nicht mehr im Routing existieren. Events an diese Targets werden leise
# verworfen (DEBUG statt ERROR), damit tote Emitter keinen Log-Spam erzeugen
# bis sie an der Quelle entfernt sind.
DEPRECATED_TARGETS = frozenset({"process_manager"})


# ---------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------
CONFIG_EVENT_TYPES = frozenset((
    "CONFIG_CHANGED",
    "CONFIG_PATCH",
    "CONFIG_REPLACE",
    "CONFIG_UPDATED",
    "CONFIG_CHANGE",
))

AUTOMATION_STATUS_EVENT_TYPES = frozenset((
    "AUTOMATION_STATUS",
))

LEGACY_REMOVE_KEYS = ("message_type",)

STOP_MARKERS = frozenset(("__STOP__",))
DISALLOWED_DIRECT_QUEUE_TARGETS = frozenset(("queue_event_send",))

DEFAULT_ROUTER_NAME = "SyncEventRouter"

DEFAULT_GENERIC_QUEUE_SUFFIXES = (
    "ws",
    "bc",
    "io",
    "ipc",
    "state",
    "event",
    "ds",
    "dc",
    "proc",
    "pc",
    "mbh",
    "mba",
    "mbc",
    "redis_api",
    "ti",
    "worker",
)

DEFAULT_EVENT_QUEUE_SUFFIXES = (
    "send",
    "ws",
    "state",
    "ds",
    "dc",
    "proc",
    "pc",
    "mbh",
    "mba",
    "mbc",
    "ti",
    "worker",
)

EXTRA_COMPAT_QUEUE_NAMES = set()

ROUTER_OPTION_DEFAULTS = (
    ("shutdown_event", None),
    ("queues_dict", None),
    ("queue_dict", None),
    ("generic_queue_dict", None),
    ("routing_table", None),
    ("broadcast_rules", None),
    ("router_name", DEFAULT_ROUTER_NAME),
    ("enable_process_broadcast", True),
    ("enable_telemetry_mirror", False),
    ("enable_worker_mirror", True),
    ("enable_config_mirrors", True),
    ("enable_automation_status_mirror", True),
    ("num_threads", 4),
)

RESERVED_ROUTER_PARAMETER_NAMES = frozenset(
    name for name, _default in ROUTER_OPTION_DEFAULTS
)


def build_default_compat_queue_parameter_names():
    """
    Build the compact default list of compatibility queue parameter names.
    """
    try:
        names = []

        for suffix in DEFAULT_GENERIC_QUEUE_SUFFIXES:
            names.append("queue_%s" % suffix)

        for suffix in DEFAULT_EVENT_QUEUE_SUFFIXES:
            names.append("queue_event_%s" % suffix)

        return tuple(dict.fromkeys(names))
    except Exception:
        return tuple()


DEFAULT_COMPAT_QUEUE_PARAMETER_NAMES = build_default_compat_queue_parameter_names()


# =============================================================================
# Basic normalization helpers
# =============================================================================

def safe_deepcopy(data):
    """
    Return a defensive deep copy.
    """
    try:
        return copy.deepcopy(data)
    except Exception:
        return data


def normalize_text(value):
    """
    Normalize a value to a lower-case stripped string.
    """
    try:
        if value is None:
            return ""
        return str(value).strip().lower()
    except Exception:
        return ""


def normalize_upper_text(value):
    """
    Normalize a value to an upper-case stripped string.
    """
    try:
        if value is None:
            return ""
        return str(value).strip().upper()
    except Exception:
        return ""


def normalize_target_collection(value):
    """
    Normalize one target or a collection of targets to a tuple of strings.
    """
    try:
        if value is None:
            return tuple()

        if isinstance(value, (list, tuple, set, frozenset)):
            result = []
            for item in value:
                item_lc = normalize_text(item)
                if item_lc:
                    result.append(item_lc)
            return tuple(result)

        single_target = normalize_text(value)
        if not single_target:
            return tuple()

        return (single_target,)
    except Exception:
        return tuple()


def is_mapping_message(data):
    """
    Return True when the payload is a dict-like event/message object.
    """
    try:
        return isinstance(data, dict)
    except Exception:
        return False


def strip_top_level_keys(data, keys):
    """
    Remove selected top-level keys from a dict payload.
    """
    try:
        if not isinstance(data, dict):
            return data

        payload = dict(data)
        for key in keys:
            try:
                payload.pop(key, None)
            except Exception:
                pass
        return payload
    except Exception:
        return data


def prepare_payload_for_send(data, remove_keys=None, deep_copy=False):
    """
    Prepare a payload snapshot before queue transmission.
    """
    try:
        payload = safe_deepcopy(data) if deep_copy else data

        if remove_keys:
            payload = strip_top_level_keys(payload, remove_keys)

        return payload
    except Exception:
        return data


# =============================================================================
# Event metadata helpers
# =============================================================================

def extract_event_type(data):
    """
    Extract event_type with legacy fallbacks.
    """
    try:
        if not isinstance(data, dict):
            return ""

        event_type = data.get("event_type")
        if event_type is None:
            event_type = data.get("type")
        if event_type is None:
            event_type = data.get("message_type")

        if event_type is None:
            payload = data.get("payload")
            if isinstance(payload, dict):
                event_type = (
                    payload.get("event_type")
                    or payload.get("type")
                    or payload.get("message_type")
                )

        return normalize_upper_text(event_type)
    except Exception:
        return ""


def extract_primary_target_value(data):
    """
    Extract the primary target field with legacy fallbacks.
    """
    try:
        if not isinstance(data, dict):
            return None

        target_value = data.get("target")
        if target_value is not None:
            return target_value

        if data.get("message_type") is not None:
            return data.get("message_type")

        if data.get("type") is not None:
            return data.get("type")

        return None
    except Exception:
        return None


def extract_primary_targets(data):
    """
    Extract normalized primary targets.
    Supports:
    - target="state"
    - target=["state", "telemetry"]
    - message_type="state"
    - type="state"
    """
    try:
        return normalize_target_collection(extract_primary_target_value(data))
    except Exception:
        return tuple()


def extract_explicit_broadcast_targets(data):
    """
    Extract dynamic broadcast targets declared in the message payload.
    Supported keys:
    - broadcast_targets
    - broadcast_to
    - broadcast={"targets": [...]}
    """
    try:
        if not isinstance(data, dict):
            return tuple()

        if data.get("broadcast_targets") is not None:
            return normalize_target_collection(data.get("broadcast_targets"))

        if data.get("broadcast_to") is not None:
            return normalize_target_collection(data.get("broadcast_to"))

        broadcast_value = data.get("broadcast")
        if isinstance(broadcast_value, dict):
            return normalize_target_collection(broadcast_value.get("targets"))

        return tuple()
    except Exception:
        return tuple()


def is_stop_message(data):
    """
    Detect router stop sentinels safely.
    """
    try:
        if data is None:
            return False

        if not isinstance(data, dict):
            return False

        marker_type = normalize_upper_text(data.get("_type"))
        marker_event_type = normalize_upper_text(data.get("event_type"))

        if marker_type in STOP_MARKERS:
            return True

        if marker_event_type in STOP_MARKERS:
            return True

        return False
    except Exception:
        return False


def unwrap_unknown_envelope(data):
    """
    Drop duplicate UNKNOWN envelopes while preserving stability.

    Expected legacy shape:
        {
            "event_type": "UNKNOWN",
            "payload": {"raw": "<json-string>"}
        }

    Return format:
        {
            "data": object,
            "drop": bool,
            "reason": str,
        }
    """
    try:
        if not isinstance(data, dict):
            return {
                "data": data,
                "drop": False,
                "reason": "",
            }

        if extract_event_type(data) != "UNKNOWN":
            return {
                "data": data,
                "drop": False,
                "reason": "",
            }

        payload = data.get("payload")
        raw_candidate = None

        if isinstance(payload, dict):
            raw_candidate = (
                payload.get("raw")
                or payload.get("payload")
                or payload.get("data")
            )
        elif isinstance(payload, str):
            raw_candidate = payload

        if isinstance(raw_candidate, str):
            try:
                decoded = json.loads(raw_candidate)
                if isinstance(decoded, dict):
                    return {
                        "data": decoded,
                        "drop": True,
                        "reason": "dropped_duplicate_unknown_envelope",
                    }
            except Exception:
                pass

        if isinstance(raw_candidate, dict):
            return {
                "data": raw_candidate,
                "drop": True,
                "reason": "dropped_duplicate_unknown_envelope",
            }

        return {
            "data": data,
            "drop": True,
            "reason": "dropped_unknown_envelope_without_payload",
        }
    except Exception:
        return {
            "data": data,
            "drop": True,
            "reason": "dropped_unknown_envelope_error",
        }


def is_config_event(data):
    """
    Detect configuration-related events.
    """
    try:
        if not isinstance(data, dict):
            return False

        if extract_event_type(data) in CONFIG_EVENT_TYPES:
            return True

        payload = data.get("payload") or {}
        if isinstance(payload, dict):
            if "patch" in payload:
                return True
            if "new_config" in payload:
                return True
            if "diff" in payload:
                return True

        return False
    except Exception:
        return False


def is_automation_status_event(data):
    """
    Detect automation status events.
    """
    try:
        return extract_event_type(data) in AUTOMATION_STATUS_EVENT_TYPES
    except Exception:
        return False


# =============================================================================
# Predicate helpers
# =============================================================================

def predicate_true(_data):
    """
    Always-true predicate.
    """
    try:
        return True
    except Exception:
        return False


def predicate_is_mapping_message(data):
    """
    Return True for dict messages.
    """
    try:
        return is_mapping_message(data)
    except Exception:
        return False


def predicate_is_config_event(data):
    """
    Broadcast predicate for config messages.
    """
    try:
        return is_config_event(data)
    except Exception:
        return False


def predicate_is_automation_status_event(data):
    """
    Broadcast predicate for automation status messages.
    """
    try:
        return is_automation_status_event(data)
    except Exception:
        return False


def predicate_all(predicates, data):
    """
    Combine predicates with logical AND.
    """
    try:
        for predicate in predicates:
            try:
                if not predicate(data):
                    return False
            except Exception:
                return False
        return True
    except Exception:
        return False


def predicate_any(predicates, data):
    """
    Combine predicates with logical OR.
    """
    try:
        for predicate in predicates:
            try:
                if predicate(data):
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False


def predicate_event_type_in(event_types, data):
    """
    Return True when extract_event_type(data) is in event_types.
    """
    try:
        return extract_event_type(data) in event_types
    except Exception:
        return False


# =============================================================================
# Queue forwarding — ONE generic handler replaces all 14 duplicates
# =============================================================================

def forward_to_queue(queue, data, handler_name, remove_keys=None, deep_copy=False):
    """
    Send one payload to one queue through the sync evt interface.
    """
    try:
        if queue is None:
            logger.debug("%s: queue is None - skip", handler_name)
            return False

        # v18: Optionales Envelope-Enrichment. Guard: _ENVELOPE_ENRICHMENT_FN
        # ist nur bei aktivem Binding != None. Andernfalls keine Seiteneffekte,
        # kein Overhead für Default-Deployments.
        enrich_fn = _ENVELOPE_ENRICHMENT_FN
        if enrich_fn is not None and isinstance(data, dict):
            try:
                enrich_fn(data)
            except Exception as exc:
                logger.debug(
                    "%s: envelope enrichment failed (non-fatal): %s", handler_name, exc
                )

        payload = prepare_payload_for_send(
            data=data,
            remove_keys=remove_keys,
            deep_copy=deep_copy,
        )

        success = sync_queue_put(queue, payload)
        return bool(success)
    except Exception as exc:
        logger.error("%s failed: %s", handler_name, exc, exc_info=True)
        return False


def heartbeat_handler(data):
    """
    Handle heartbeat messages without queue forwarding.
    """
    try:
        logger.info("Heartbeat received: %s", data)
        return True
    except Exception as exc:
        logger.error("heartbeat_handler failed: %s", exc, exc_info=True)
        return False


def queue_forward_handler(queue, data, handler_name, remove_keys=None):
    """
    Generic unified handler that replaces all 14 domain-specific handler functions.
    Eliminates ~280 lines of duplicated boilerplate.
    """
    try:
        return forward_to_queue(
            queue=queue,
            data=data,
            handler_name=handler_name,
            remove_keys=remove_keys,
            deep_copy=False,
        )
    except Exception as exc:
        logger.error("%s failed: %s", handler_name, exc, exc_info=True)
        return False


# =============================================================================
# Named handler factories via functools.partial
#
# Jeder Name-spezifische Handler wird durch partial erzeugt.
# Das spart ~280 Zeilen duplizierter async-Funktionen.
# =============================================================================

def _make_named_handler(handler_name, remove_keys=None):
    """
    Factory: erzeuge einen benannten queue_forward_handler via partial.
    Signatur des Ergebnisses: (queue, data) -> bool
    """
    handler = partial(queue_forward_handler, handler_name=handler_name, remove_keys=remove_keys)
    handler.__name__ = handler_name
    handler.__qualname__ = handler_name
    return handler


state_handler = _make_named_handler("state_handler", remove_keys=LEGACY_REMOVE_KEYS)
event_handler = _make_named_handler("event_handler", remove_keys=LEGACY_REMOVE_KEYS)
datastore_handler = _make_named_handler("datastore_handler")
data_config_handler = _make_named_handler("data_config_handler")
process_handler = _make_named_handler("process_handler")
automation_handler = _make_named_handler("automation_handler")
modbus_handler = _make_named_handler("modbus_handler")
controller_handler = _make_named_handler("controller_handler")
process_manager_handler = _make_named_handler("process_manager_handler")
thread_management_handler = _make_named_handler("thread_management_handler")
telemetry_handler = _make_named_handler("telemetry_handler")
worker_event_handler = _make_named_handler("worker_event_handler")
websocket_handler = _make_named_handler("websocket_handler")
generic_queue_handler = _make_named_handler("generic_queue_handler")


# =============================================================================
# Route table helpers
# =============================================================================

def make_route_entry(handler, queue_key=None, description=""):
    """
    Create a normalized route entry.
    """
    try:
        return {
            "handler": handler,
            "queue_key": queue_key,
            "description": description,
        }
    except Exception:
        return {
            "handler": handler,
            "queue_key": queue_key,
            "description": "",
        }


def normalize_route_entry(entry):
    """
    Normalize custom routing-table entries.

    Supported forms:
    - "queue_event_state"
    - {"queue_key": "queue_event_state", "handler": state_handler}
    - ("queue_event_state", state_handler)
    - handler_callable
    """
    try:
        if isinstance(entry, str):
            return make_route_entry(
                handler=generic_queue_handler,
                queue_key=entry,
                description="custom_string_queue_route",
            )

        if isinstance(entry, tuple) and len(entry) == 2:
            queue_key = entry[0]
            handler = entry[1]
            return make_route_entry(
                handler=handler,
                queue_key=queue_key,
                description="custom_tuple_route",
            )

        if isinstance(entry, dict):
            queue_key = entry.get("queue_key")
            handler = entry.get("handler") or generic_queue_handler
            description = entry.get("description", "custom_dict_route")
            return make_route_entry(
                handler=handler,
                queue_key=queue_key,
                description=description,
            )

        if callable(entry):
            return make_route_entry(
                handler=entry,
                queue_key=None,
                description="custom_handler_only_route",
            )

        return None
    except Exception:
        return None


def build_default_routing_table():
    """
    Build the standardized default target routing table.
    """
    try:
        return {
            # System
            "heartbeat": make_route_entry(heartbeat_handler, None, "heartbeat"),

            # State / SEM ingress
            "state": make_route_entry(state_handler, "queue_event_state", "state"),
            "state_handler": make_route_entry(state_handler, "queue_event_state", "state_handler"),
            "state_management": make_route_entry(state_handler, "queue_event_state", "state_management"),
            "state_event_management": make_route_entry(state_handler, "queue_event_state", "state_event_management"),
            "sem": make_route_entry(state_handler, "queue_event_state", "sem"),
            "event": make_route_entry(event_handler, "queue_event_state", "event"),
            "event_handler": make_route_entry(event_handler, "queue_event_state", "event_handler"),

            # NOTE: The former "process_state_management" / "psm" targets
            # have been removed. The responsibilities of PSM were absorbed
            # by the per-worker Generic Rule Engine (GRE). The "proc" /
            # "process" ingress remains as a passive archival channel for
            # legacy listeners, but no default routing targets PSM anymore.

            # Controller / automation
            "controller": make_route_entry(controller_handler, "queue_event_mbc", "controller"),
            "mbc": make_route_entry(controller_handler, "queue_event_mbc", "mbc"),
            "automation": make_route_entry(automation_handler, "queue_event_mba", "automation"),
            "mba": make_route_entry(automation_handler, "queue_event_mba", "mba"),

            # Thread management
            "thread_management": make_route_entry(thread_management_handler, "queue_event_pc", "thread_management"),
            "tm": make_route_entry(thread_management_handler, "queue_event_pc", "tm"),
            "pc": make_route_entry(thread_management_handler, "queue_event_pc", "pc"),
            "process_controller": make_route_entry(thread_management_handler, "queue_event_pc", "process_controller"),

            # Datastore / config
            "datastore": make_route_entry(datastore_handler, "queue_event_ds", "datastore"),
            "database": make_route_entry(datastore_handler, "queue_event_ds", "database"),
            "ds": make_route_entry(datastore_handler, "queue_event_ds", "ds"),
            "data_config": make_route_entry(data_config_handler, "queue_event_dc", "data_config"),
            "config_data": make_route_entry(data_config_handler, "queue_event_dc", "config_data"),
            "dc": make_route_entry(data_config_handler, "queue_event_dc", "dc"),

            # Modbus / websocket
            "modbus": make_route_entry(modbus_handler, "queue_event_mbh", "modbus"),
            "mbh": make_route_entry(modbus_handler, "queue_event_mbh", "mbh"),
            "modbus_handler": make_route_entry(modbus_handler, "queue_event_mbh", "modbus_handler"),
            "websocket": make_route_entry(websocket_handler, "queue_event_ws", "websocket"),
            "ws": make_route_entry(websocket_handler, "queue_event_ws", "ws"),

            # Telemetry / worker
            "telemetry": make_route_entry(telemetry_handler, "queue_event_ti", "telemetry"),
            "telemetry_event": make_route_entry(telemetry_handler, "queue_event_ti", "telemetry_event"),
            "ti": make_route_entry(telemetry_handler, "queue_event_ti", "ti"),
            "worker": make_route_entry(worker_event_handler, "queue_event_worker", "worker"),
            "worker_event": make_route_entry(worker_event_handler, "queue_event_worker", "worker_event"),
            "proxy_worker_bridge": make_route_entry(worker_event_handler, "queue_event_worker", "proxy_worker_bridge"),
            "runtime_command_binding": make_route_entry(worker_event_handler, "queue_event_worker", "runtime_command_binding"),
        }
    except Exception:
        return {}


def build_routing_table(custom_routing_table=None):
    """
    Build the final routing table from defaults plus custom overrides.
    """
    try:
        routing_table = build_default_routing_table()

        if not isinstance(custom_routing_table, dict):
            return routing_table

        for target, entry in custom_routing_table.items():
            target_lc = normalize_text(target)
            if not target_lc:
                continue

            normalized_entry = normalize_route_entry(entry)
            if normalized_entry is None:
                continue

            routing_table[target_lc] = normalized_entry

        return routing_table
    except Exception:
        return build_default_routing_table()


# =============================================================================
# Queue registry helpers
# =============================================================================

def merge_queue_dicts(*queue_dicts):
    """
    Merge queue dictionaries and keep only queue_* keys.
    """
    try:
        merged = {}

        for queue_dict in queue_dicts:
            if not isinstance(queue_dict, dict):
                continue

            for key, value in queue_dict.items():
                if not isinstance(key, str):
                    continue

                key_lc = normalize_text(key)
                if not key_lc.startswith("queue_"):
                    continue

                merged[key_lc] = value

        return merged
    except Exception:
        return {}


def is_queue_parameter_name(key):
    """
    Return True for queue-like keyword argument names.
    """
    try:
        key_lc = normalize_text(key)
        if not key_lc:
            return False

        if key_lc in RESERVED_ROUTER_PARAMETER_NAMES:
            return False

        return key_lc.startswith("queue_")
    except Exception:
        return False


def build_known_queue_argument_map(router_kwargs=None):
    """
    Collect explicitly passed queue-like keyword arguments into one normalized dict.
    """
    try:
        source = router_kwargs if isinstance(router_kwargs, dict) else {}
        explicit_map = {}

        for key, value in source.items():
            if not is_queue_parameter_name(key):
                continue

            explicit_map[normalize_text(key)] = value

        return explicit_map
    except Exception:
        return {}


def build_queue_registry(router_kwargs=None):
    """
    Build one normalized queue registry from queue wrapper dicts and direct kwargs.
    """
    try:
        source = router_kwargs if isinstance(router_kwargs, dict) else {}

        explicit_map = build_known_queue_argument_map(source)

        merged = merge_queue_dicts(
            source.get("queues_dict"),
            source.get("queue_dict"),
            source.get("generic_queue_dict"),
            explicit_map,
        )

        compact = {}
        for key, value in merged.items():
            if value is not None:
                compact[key] = value

        return compact
    except Exception:
        return {}


def build_queue_name_index(queue_registry):
    """
    Build a lower-case queue name index.
    """
    try:
        return {
            normalize_text(key): key
            for key in queue_registry.keys()
            if isinstance(key, str)
        }
    except Exception:
        return {}


def resolve_dynamic_queue_key(target, queue_name_index):
    """
    Resolve a queue key dynamically from a target name.

    Resolution order:
    1. exact queue key target
    2. queue_event_<target>
    3. queue_<target>
    """
    try:
        target_lc = normalize_text(target)
        if not target_lc:
            return None

        candidate_keys = (
            target_lc,
            "queue_event_" + target_lc,
            "queue_" + target_lc,
        )

        for candidate_key in candidate_keys:
            actual_key = queue_name_index.get(candidate_key)
            if not actual_key:
                continue

            if actual_key in DISALLOWED_DIRECT_QUEUE_TARGETS:
                continue

            return actual_key

        return None
    except Exception:
        return None


# =============================================================================
# Broadcast rule helpers
# =============================================================================

def make_broadcast_rule(name, predicate, targets, description="", enabled=True):
    """
    Create a normalized broadcast rule.
    """
    try:
        return {
            "name": name,
            "predicate": predicate,
            "targets": normalize_target_collection(targets),
            "description": description,
            "enabled": bool(enabled),
        }
    except Exception:
        return {
            "name": name,
            "predicate": predicate_true,
            "targets": tuple(),
            "description": "",
            "enabled": False,
        }


def build_predicate_from_rule_dict(rule):
    """
    Build a predicate callable from a rule dict.
    """
    try:
        if callable(rule.get("predicate")):
            return rule.get("predicate")

        predicates = []

        if bool(rule.get("require_dict", False)):
            predicates.append(predicate_is_mapping_message)

        event_types = rule.get("event_types")
        if event_types:
            normalized_event_types = frozenset(
                normalize_upper_text(item)
                for item in normalize_target_collection(event_types)
            )
            predicates.append(partial(predicate_event_type_in, normalized_event_types))

        if not predicates:
            return predicate_true

        if len(predicates) == 1:
            return predicates[0]

        return partial(predicate_all, tuple(predicates))
    except Exception:
        return predicate_true


def normalize_broadcast_rule(rule):
    """
    Normalize custom broadcast rules.
    """
    try:
        if not isinstance(rule, dict):
            return None

        targets = normalize_target_collection(rule.get("targets"))
        if not targets:
            return None

        name = str(rule.get("name", "custom_broadcast_rule"))
        description = str(rule.get("description", "custom_broadcast_rule"))
        enabled = bool(rule.get("enabled", True))
        predicate = build_predicate_from_rule_dict(rule)

        return make_broadcast_rule(
            name=name,
            predicate=predicate,
            targets=targets,
            description=description,
            enabled=enabled,
        )
    except Exception:
        return None


def build_default_broadcast_rules(
    queue_registry,
    enable_process_broadcast=True,
    enable_telemetry_mirror=False,
    enable_worker_mirror=True,
    enable_config_mirrors=True,
    enable_automation_status_mirror=True,
):
    """
    Build default broadcast rules that preserve the legacy feature set.
    """
    try:
        rules = []

        # NOTE: The former "broadcast_to_process_state_management" rule has
        # been removed. PSM no longer exists as a separate thread — all
        # process-state logic runs inside the per-worker GRE, driven by the
        # deterministic primary dispatch. The "enable_process_broadcast"
        # option is still accepted for backward compatibility but is a no-op.
        _ = bool(enable_process_broadcast)

        if enable_telemetry_mirror and "queue_event_ti" in queue_registry:
            rules.append(make_broadcast_rule(
                name="mirror_to_telemetry",
                predicate=predicate_is_mapping_message,
                targets=("queue_event_ti",),
                description="Legacy telemetry mirror",
                enabled=True,
            ))

        if enable_worker_mirror and "queue_event_worker" in queue_registry:
            rules.append(make_broadcast_rule(
                name="mirror_to_worker",
                predicate=predicate_is_mapping_message,
                targets=("queue_event_worker",),
                description="Legacy worker mirror",
                enabled=False,
            ))

        if enable_config_mirrors:
            if "queue_event_pc" in queue_registry:
                rules.append(make_broadcast_rule(
                    name="mirror_config_to_thread_management",
                    predicate=predicate_is_config_event,
                    targets=("queue_event_pc",),
                    description="Legacy config mirror to TM",
                    enabled=True,
                ))

            if "queue_event_state" in queue_registry:
                rules.append(make_broadcast_rule(
                    name="mirror_config_to_state_management",
                    predicate=predicate_is_config_event,
                    targets=("queue_event_state",),
                    description="Legacy config mirror to SEM",
                    enabled=True,
                ))

            if "queue_event_ds" in queue_registry:
                rules.append(make_broadcast_rule(
                    name="mirror_config_to_datastore",
                    predicate=predicate_is_config_event,
                    targets=("queue_event_ds",),
                    description="Legacy config mirror to datastore",
                    enabled=True,
                ))

        if enable_automation_status_mirror and "queue_event_pc" in queue_registry:
            rules.append(make_broadcast_rule(
                name="mirror_automation_status_to_thread_management",
                predicate=predicate_is_automation_status_event,
                targets=("queue_event_pc",),
                description="Legacy automation status mirror to TM",
                enabled=True,
            ))

        return tuple(rules)
    except Exception:
        return tuple()


def build_broadcast_rules(
    queue_registry,
    custom_broadcast_rules=None,
    enable_process_broadcast=True,
    enable_telemetry_mirror=False,
    enable_worker_mirror=True,
    enable_config_mirrors=True,
    enable_automation_status_mirror=True,
):
    """
    Merge default and custom broadcast rules.
    """
    try:
        rules = list(build_default_broadcast_rules(
            queue_registry=queue_registry,
            enable_process_broadcast=enable_process_broadcast,
            enable_telemetry_mirror=enable_telemetry_mirror,
            enable_worker_mirror=enable_worker_mirror,
            enable_config_mirrors=enable_config_mirrors,
            enable_automation_status_mirror=enable_automation_status_mirror,
        ))

        if isinstance(custom_broadcast_rules, (list, tuple)):
            for rule in custom_broadcast_rules:
                normalized_rule = normalize_broadcast_rule(rule)
                if normalized_rule is not None:
                    rules.append(normalized_rule)

        return tuple(rules)
    except Exception:
        return tuple()


# =============================================================================
# Router context helpers
# =============================================================================

def create_router_context(
    queue_registry,
    routing_table,
    broadcast_rules,
    router_name="SyncEventRouter",
    cross_domain_mapping=None,
):
    """
    Create a pure dict router context.
    """
    try:
        normalized_mapping = normalize_cross_domain_mapping(cross_domain_mapping or {})
        return {
            "router_name": router_name,
            "queues": queue_registry,
            "queue_name_index": build_queue_name_index(queue_registry),
            "routing_table": routing_table,
            "broadcast_rules": broadcast_rules,
            "cross_domain_mapping": normalized_mapping,
        }
    except Exception:
        return {
            "router_name": "SyncEventRouter",
            "queues": {},
            "queue_name_index": {},
            "routing_table": {},
            "broadcast_rules": tuple(),
            "cross_domain_mapping": {},
        }


# =============================================================================
# Dispatch helpers — fully synchronous
# =============================================================================

def make_dispatch_result(target, queue_key, success, handler_name, reason=""):
    """
    Create a standardized dispatch result.
    """
    try:
        return {
            "target": normalize_text(target),
            "queue_key": queue_key,
            "success": bool(success),
            "handler_name": handler_name,
            "reason": reason,
        }
    except Exception:
        return {
            "target": "",
            "queue_key": queue_key,
            "success": False,
            "handler_name": handler_name,
            "reason": reason,
        }


def resolve_route_entry(target, router_context):
    """
    Resolve one target to one route entry.
    """
    try:
        target_lc = normalize_text(target)
        if not target_lc:
            return None

        routing_table = router_context.get("routing_table", {})
        route_entry = routing_table.get(target_lc)
        if route_entry is not None:
            return route_entry

        dynamic_queue_key = resolve_dynamic_queue_key(
            target=target_lc,
            queue_name_index=router_context.get("queue_name_index", {}),
        )

        if dynamic_queue_key is None:
            return None

        return make_route_entry(
            handler=generic_queue_handler,
            queue_key=dynamic_queue_key,
            description="dynamic_queue_resolution",
        )
    except Exception:
        return None


def invoke_route_handler(route_entry, queue, data):
    """
    Invoke one route handler synchronously.
    """
    try:
        handler = route_entry.get("handler")
        queue_key = route_entry.get("queue_key")

        if queue_key is None:
            result = handler(data)
        else:
            result = handler(queue, data)

        return bool(result)
    except Exception as exc:
        handler_name = getattr(route_entry.get("handler"), "__name__", "unknown_handler")
        logger.error("invoke_route_handler failed for %s: %s", handler_name, exc, exc_info=True)
        return False


def dispatch_target(target, data, router_context, sent_queue_keys=None):
    """
    Dispatch one target by function mapping or dynamic queue resolution.
    Fully synchronous.
    """
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()

        route_entry = resolve_route_entry(
            target=target,
            router_context=router_context,
        )

        if route_entry is None:
            if target in DEPRECATED_TARGETS:
                logger.debug(
                    "%s: dropping event for deprecated target '%s'",
                    router_context.get("router_name", "SyncEventRouter"),
                    target,
                )
                return make_dispatch_result(
                    target=target,
                    queue_key=None,
                    success=True,
                    handler_name="deprecated_target",
                    reason="deprecated_target_dropped",
                )
            logger.error(
                "%s: unknown target '%s'",
                router_context.get("router_name", "SyncEventRouter"),
                target,
            )
            return make_dispatch_result(
                target=target,
                queue_key=None,
                success=False,
                handler_name="unresolved_target",
                reason="unknown_target",
            )

        queue_key = route_entry.get("queue_key")
        handler_name = getattr(route_entry.get("handler"), "__name__", "unknown_handler")

        if queue_key is not None and queue_key in DISALLOWED_DIRECT_QUEUE_TARGETS:
            logger.error(
                "%s: routing to disallowed queue '%s' is blocked",
                router_context.get("router_name", "SyncEventRouter"),
                queue_key,
            )
            return make_dispatch_result(
                target=target,
                queue_key=queue_key,
                success=False,
                handler_name=handler_name,
                reason="disallowed_queue_target",
            )

        if queue_key is not None and queue_key in sent_queue_keys:
            return make_dispatch_result(
                target=target,
                queue_key=queue_key,
                success=False,
                handler_name=handler_name,
                reason="duplicate_queue_skip",
            )

        queue = None
        if queue_key is not None:
            queue = router_context.get("queues", {}).get(queue_key)

            if queue is None:
                return make_dispatch_result(
                    target=target,
                    queue_key=queue_key,
                    success=False,
                    handler_name=handler_name,
                    reason="queue_not_available",
                )

        success = invoke_route_handler(
            route_entry=route_entry,
            queue=queue,
            data=data,
        )

        if success and queue_key is not None:
            sent_queue_keys.add(queue_key)

        return make_dispatch_result(
            target=target,
            queue_key=queue_key,
            success=success,
            handler_name=handler_name,
            reason="",
        )
    except Exception as exc:
        logger.error("dispatch_target failed: %s", exc, exc_info=True)
        return make_dispatch_result(
            target=target,
            queue_key=None,
            success=False,
            handler_name="dispatch_target",
            reason="dispatch_exception",
        )


def dispatch_targets(
    targets,
    data,
    router_context,
    sent_queue_keys=None,
    deep_copy_additional_targets=True,
):
    """
    Dispatch multiple targets sequentially with optional payload cloning.
    Fully synchronous.
    """
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()

        normalized_targets = normalize_target_collection(targets)
        results = []

        for index, target in enumerate(normalized_targets):
            payload = data
            if deep_copy_additional_targets and index > 0:
                payload = safe_deepcopy(data)

            result = dispatch_target(
                target=target,
                data=payload,
                router_context=router_context,
                sent_queue_keys=sent_queue_keys,
            )
            results.append(result)

        return tuple(results)
    except Exception as exc:
        logger.error("dispatch_targets failed: %s", exc, exc_info=True)
        return tuple()


def apply_dynamic_broadcast_targets(data, router_context, sent_queue_keys=None):
    """
    Apply explicit broadcast targets from the message payload.
    Fully synchronous.
    """
    try:
        broadcast_targets = extract_explicit_broadcast_targets(data)
        if not broadcast_targets:
            return tuple()

        return dispatch_targets(
            targets=broadcast_targets,
            data=data,
            router_context=router_context,
            sent_queue_keys=sent_queue_keys,
            deep_copy_additional_targets=True,
        )
    except Exception as exc:
        logger.error("apply_dynamic_broadcast_targets failed: %s", exc, exc_info=True)
        return tuple()


# =============================================================================
# Deterministic, hierarchical, priority-ordered primary dispatch
# =============================================================================

def build_deterministic_primary_targets(data, router_context):
    """
    Build the ordered tuple of primary targets for one event.

    Combines, in this order:
    1. Explicit primary targets from the payload (``target`` / ``type`` / ...).
    2. Cross-domain targets resolved from the router-level mapping or from
       the payload's inline ``cross_domain_targets`` list.

    The final tuple is deterministically sorted by the hierarchical
    ``(priority, domain, target)`` key. Duplicates are removed, preserving
    the first occurrence (which, after sort, is the highest-priority one).

    This function is **pure** — it only reads from ``data`` and
    ``router_context`` and returns a new tuple.
    """
    try:
        primary_targets = extract_primary_targets(data)

        mapping = {}
        if isinstance(router_context, dict):
            mapping = router_context.get("cross_domain_mapping", {}) or {}

        cross_targets = combine_cross_domain_targets(data, mapping)

        all_targets = []
        if primary_targets:
            all_targets.extend(primary_targets)
        if cross_targets:
            all_targets.extend(cross_targets)

        if not all_targets:
            return tuple()

        return order_targets_deterministic(all_targets)
    except Exception as exc:
        logger.error("build_deterministic_primary_targets failed: %s", exc, exc_info=True)
        return tuple()


def dispatch_deterministic_primary(data, router_context, sent_queue_keys=None):
    """
    Deterministic primary dispatch pipeline.

    This function:
    1. Computes the ordered target set via ``build_deterministic_primary_targets``.
    2. Dispatches each target in strict priority order, sequentially.
    3. Returns the tuple of dispatch results.

    The primary dispatch is intentionally **not** parallelized: deterministic
    ordering is the hard requirement for the state-writer path. Broadcast
    mirrors (telemetry / worker / config fan-out) run afterwards and may
    still execute in parallel for throughput.
    """
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()

        ordered_targets = build_deterministic_primary_targets(
            data=data,
            router_context=router_context,
        )

        if not ordered_targets:
            return tuple()

        if logger.isEnabledFor(logging.DEBUG):
            try:
                diag = order_targets_by_priority_domain(ordered_targets)
                logger.debug(
                    "%s: deterministic primary dispatch order: %s",
                    router_context.get("router_name", "SyncEventRouter"),
                    diag,
                )
            except Exception:
                pass

        return dispatch_targets(
            targets=ordered_targets,
            data=data,
            router_context=router_context,
            sent_queue_keys=sent_queue_keys,
            deep_copy_additional_targets=True,
        )
    except Exception as exc:
        logger.error("dispatch_deterministic_primary failed: %s", exc, exc_info=True)
        return tuple()


def apply_broadcast_rules(data, router_context, sent_queue_keys=None):
    """
    Apply configured broadcast rules after the primary dispatch.
    Fully synchronous.
    """
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()

        results = []

        for rule in router_context.get("broadcast_rules", tuple()):
            if not isinstance(rule, dict):
                continue

            if not bool(rule.get("enabled", True)):
                continue

            predicate = rule.get("predicate") or predicate_true
            should_broadcast = False

            try:
                should_broadcast = bool(predicate(data))
            except Exception as exc:
                logger.error(
                    "broadcast rule '%s' predicate failed: %s",
                    rule.get("name", "unknown_rule"),
                    exc,
                    exc_info=True,
                )
                should_broadcast = False

            if not should_broadcast:
                continue

            rule_results = dispatch_targets(
                targets=rule.get("targets", tuple()),
                data=safe_deepcopy(data),
                router_context=router_context,
                sent_queue_keys=sent_queue_keys,
                deep_copy_additional_targets=True,
            )
            results.extend(rule_results)

        return tuple(results)
    except Exception as exc:
        logger.error("apply_broadcast_rules failed: %s", exc, exc_info=True)
        return tuple()


# =============================================================================
# Parallel broadcast dispatch via ThreadPoolExecutor
# =============================================================================

def _dispatch_single_broadcast_rule(rule, data, router_context, sent_queue_keys_snapshot):
    """
    Worker function for parallel broadcast rule dispatch.
    Each rule gets its own sent_queue_keys copy to avoid cross-thread mutation.
    Returns (rule_name, results_tuple).
    """
    try:
        rule_name = rule.get("name", "unknown_rule")
        predicate = rule.get("predicate") or predicate_true

        should_broadcast = False
        try:
            should_broadcast = bool(predicate(data))
        except Exception as exc:
            logger.error(
                "broadcast rule '%s' predicate failed: %s",
                rule_name,
                exc,
                exc_info=True,
            )
            return (rule_name, tuple())

        if not should_broadcast:
            return (rule_name, tuple())

        local_sent_keys = set(sent_queue_keys_snapshot)

        rule_results = dispatch_targets(
            targets=rule.get("targets", tuple()),
            data=safe_deepcopy(data),
            router_context=router_context,
            sent_queue_keys=local_sent_keys,
            deep_copy_additional_targets=True,
        )

        return (rule_name, rule_results)
    except Exception as exc:
        logger.error("_dispatch_single_broadcast_rule failed: %s", exc, exc_info=True)
        return (rule.get("name", "unknown_rule"), tuple())


def apply_broadcast_rules_parallel(data, router_context, sent_queue_keys, executor):
    """
    Apply configured broadcast rules in parallel via ThreadPoolExecutor.
    Each rule is submitted as a separate task for maximum throughput.
    """
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()

        active_rules = []
        for rule in router_context.get("broadcast_rules", tuple()):
            if not isinstance(rule, dict):
                continue
            if not bool(rule.get("enabled", True)):
                continue
            active_rules.append(rule)

        if not active_rules:
            return tuple()

        sent_snapshot = frozenset(sent_queue_keys)

        futures = []
        for rule in active_rules:
            future = executor.submit(
                _dispatch_single_broadcast_rule,
                rule,
                data,
                router_context,
                sent_snapshot,
            )
            futures.append(future)

        results = []
        for future in as_completed(futures):
            try:
                _rule_name, rule_results = future.result(timeout=10.0)
                results.extend(rule_results)
            except Exception as exc:
                logger.error("broadcast future failed: %s", exc, exc_info=True)

        return tuple(results)
    except Exception as exc:
        logger.error("apply_broadcast_rules_parallel failed: %s", exc, exc_info=True)
        return tuple()


# =============================================================================
# Main router loop — fully synchronous
# =============================================================================

def read_router_option(router_kwargs, option_name, default=None):
    """
    Read a router option from kwargs with defensive defaults.
    """
    try:
        if not isinstance(router_kwargs, dict):
            return default

        value = router_kwargs.get(option_name, default)
        if value is None:
            return default

        return value
    except Exception:
        return default


def sync_event_router(**router_kwargs):
    """
    Main synchronous router loop with ThreadPoolExecutor.

    Generic by design:
    - consumes queue_* kwargs dynamically
    - consumes queues_dict / queue_dict / generic_queue_dict wrappers
    - remains compatible with legacy callers via a generated signature
    - uses ThreadPoolExecutor for parallel broadcast dispatch
    """
    try:
        shutdown_event = read_router_option(router_kwargs, "shutdown_event")
        router_name = read_router_option(router_kwargs, "router_name", DEFAULT_ROUTER_NAME)
        routing_table = read_router_option(router_kwargs, "routing_table")
        broadcast_rules = read_router_option(router_kwargs, "broadcast_rules")
        num_threads = read_router_option(router_kwargs, "num_threads", 4)

        enable_process_broadcast = read_router_option(
            router_kwargs,
            "enable_process_broadcast",
            True,
        )
        enable_telemetry_mirror = read_router_option(
            router_kwargs,
            "enable_telemetry_mirror",
            True,
        )
        enable_worker_mirror = read_router_option(
            router_kwargs,
            "enable_worker_mirror",
            True,
        )
        enable_config_mirrors = read_router_option(
            router_kwargs,
            "enable_config_mirrors",
            True,
        )
        enable_automation_status_mirror = read_router_option(
            router_kwargs,
            "enable_automation_status_mirror",
            True,
        )

        queue_registry = build_queue_registry(router_kwargs)

        final_routing_table = build_routing_table(routing_table)

        final_broadcast_rules = build_broadcast_rules(
            queue_registry=queue_registry,
            custom_broadcast_rules=broadcast_rules,
            enable_process_broadcast=enable_process_broadcast,
            enable_telemetry_mirror=enable_telemetry_mirror,
            enable_worker_mirror=enable_worker_mirror,
            enable_config_mirrors=enable_config_mirrors,
            enable_automation_status_mirror=enable_automation_status_mirror,
        )

        router_context = create_router_context(
            queue_registry=queue_registry,
            routing_table=final_routing_table,
            broadcast_rules=final_broadcast_rules,
            router_name=router_name,
        )

        ingress_queue = queue_registry.get("queue_event_send")
        if ingress_queue is None:
            logger.error("%s: queue_event_send is required", router_name)
            return False

        executor = ThreadPoolExecutor(
            max_workers=num_threads,
            thread_name_prefix="%s-pool" % router_name,
        )

        logger.info(
            "%s started with %d queues, %d routes, %d broadcast rules, %d threads",
            router_name,
            len(queue_registry),
            len(final_routing_table),
            len(final_broadcast_rules),
            num_threads,
        )

        try:
            while True:
                if shutdown_event is not None:
                    try:
                        if shutdown_event.is_set():
                            break
                    except Exception:
                        pass

                data = sync_queue_get(ingress_queue)

                if data is None:
                    continue

                if is_stop_message(data):
                    logger.info("%s: stop sentinel received", router_name)
                    break

                unknown_result = unwrap_unknown_envelope(data)
                if bool(unknown_result.get("drop", False)):
                    logger.debug(
                        "%s: dropped unknown envelope (%s)",
                        router_name,
                        unknown_result.get("reason", "unknown"),
                    )
                    continue

                normalized_data = unknown_result.get("data", data)

                sent_queue_keys = set()

                primary_results = dispatch_deterministic_primary(
                    data=normalized_data,
                    router_context=router_context,
                    sent_queue_keys=sent_queue_keys,
                )
                if not primary_results:
                    logger.warning(
                        "%s: message without resolvable target: %s",
                        router_name,
                        normalized_data,
                    )

                apply_dynamic_broadcast_targets(
                    data=normalized_data,
                    router_context=router_context,
                    sent_queue_keys=sent_queue_keys,
                )

                apply_broadcast_rules_parallel(
                    data=normalized_data,
                    router_context=router_context,
                    sent_queue_keys=sent_queue_keys,
                    executor=executor,
                )

        finally:
            executor.shutdown(wait=True, cancel_futures=False)
            logger.info("%s: ThreadPoolExecutor shut down", router_name)

        logger.info("%s stopped", router_name)
        return True
    except Exception as exc:
        logger.error("%s crashed: %s", router_name, exc, exc_info=True)
        return False


# =============================================================================
# Compatibility wrappers
# =============================================================================

def sync_ev_queue_handler(**router_kwargs):
    """
    Compatibility alias for the old broker entrypoint.
    """
    try:
        return sync_event_router(**router_kwargs)
    except Exception as exc:
        logger.error("sync_ev_queue_handler failed: %s", exc, exc_info=True)
        return False


def get_compatibility_queue_parameter_names():
    """
    Return the current compatibility queue parameter names.
    """
    try:
        names = list(DEFAULT_COMPAT_QUEUE_PARAMETER_NAMES)

        if EXTRA_COMPAT_QUEUE_NAMES:
            names.extend(sorted(
                normalize_text(name)
                for name in EXTRA_COMPAT_QUEUE_NAMES
                if is_queue_parameter_name(name)
            ))

        return tuple(dict.fromkeys(name for name in names if name))
    except Exception:
        return tuple(DEFAULT_COMPAT_QUEUE_PARAMETER_NAMES)


def build_router_signature():
    """
    Build a synthetic keyword-only signature for legacy caller compatibility.
    """
    try:
        parameters = []

        for queue_name in get_compatibility_queue_parameter_names():
            parameters.append(
                inspect.Parameter(
                    queue_name,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=None,
                )
            )

        for option_name, default_value in ROUTER_OPTION_DEFAULTS:
            parameters.append(
                inspect.Parameter(
                    option_name,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=default_value,
                )
            )

        return inspect.Signature(parameters=tuple(parameters))
    except Exception:
        return inspect.Signature()


def apply_router_signature(func):
    """
    Attach the generated compatibility signature to a router function.
    """
    try:
        func.__signature__ = build_router_signature()
    except Exception:
        pass

    return func


def refresh_router_signatures():
    """
    Refresh generated compatibility signatures after queue registration updates.
    """
    apply_router_signature(sync_event_router)
    apply_router_signature(sync_ev_queue_handler)
    return True


def register_router_queue_names(*queue_names):
    """
    Register additional queue_* names for compatibility with external
    filter_kwargs() adapters.
    """
    try:
        changed = False

        for queue_name in queue_names:
            if not is_queue_parameter_name(queue_name):
                continue

            normalized_name = normalize_text(queue_name)
            if normalized_name in EXTRA_COMPAT_QUEUE_NAMES:
                continue

            EXTRA_COMPAT_QUEUE_NAMES.add(normalized_name)
            changed = True

        if changed:
            refresh_router_signatures()

        return tuple(sorted(EXTRA_COMPAT_QUEUE_NAMES))
    except Exception:
        return tuple()


refresh_router_signatures()


# =============================================================================
# Diagnostics helpers
# =============================================================================

def event_router_health_snapshot(
    routing_table=None,
    queue_registry=None,
    broadcast_rules=None,
):
    """
    Return a lightweight diagnostic snapshot for debugging.
    """
    try:
        normalized_routing_table = build_routing_table(routing_table)
        normalized_queue_registry = queue_registry if isinstance(queue_registry, dict) else {}
        normalized_broadcast_rules = broadcast_rules if isinstance(broadcast_rules, (list, tuple)) else tuple()

        return {
            "routing_targets": sorted(normalized_routing_table.keys()),
            "queue_keys": sorted(normalized_queue_registry.keys()),
            "broadcast_rule_names": [
                str(rule.get("name", "unknown_rule"))
                for rule in normalized_broadcast_rules
                if isinstance(rule, dict)
            ],
        }
    except Exception:
        return {
            "routing_targets": [],
            "queue_keys": [],
            "broadcast_rule_names": [],
        }
