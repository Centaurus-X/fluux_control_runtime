# -*- coding: utf-8 -*-
#
# src/libraries/edge/_sync.py
#
# Minimal-Implementierung der Sync-Snapshot-Helfer, auf die _codec.py
# verweist. Diese Modul-Datei war in frueheren Lieferungen noch nicht
# vorhanden; in v26 wird sie als funktionaler Basisstub ergaenzt,
# damit die Edge-/Gateway-Pipeline importierbar und einsetzbar ist.
#
# Konventionen:
#   * kein OOP, keine Decorators, keine Dataclasses
#   * kein lambda - wo callables noetig waeren, wird functools.partial verwendet
#   * funktionale Helfer mit expliziten resources-/ctx-Parametern
#
# Die gelieferten Snapshot-Formen sind bewusst minimal und deterministisch.
# Sie liefern genau die Struktur, die _codec.py fuer die Response-Erzeugung
# benoetigt und koennen spaeter ohne Signaturwechsel erweitert werden.

import copy
import time

from functools import partial


# ---------------------------------------------------------------------------
# interne Helfer
# ---------------------------------------------------------------------------


def _now_ms():
    return int(time.time() * 1000)


def _deepcopy_safe(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _section_view(resources, section_name):
    resources_map = _safe_dict(resources)
    section = resources_map.get(section_name)
    return _deepcopy_safe(section) if section is not None else None


def _pick_sections(resources, sections):
    resources_map = _safe_dict(resources)
    if not sections:
        return {k: _deepcopy_safe(v) for k, v in resources_map.items()}

    selected = {}
    for section_name in sections:
        if section_name in resources_map:
            selected[section_name] = _deepcopy_safe(resources_map[section_name])
    return selected


def _envelope(node_id, body, kind):
    return {
        "kind": kind,
        "node_id": node_id,
        "generated_at_ms": _now_ms(),
        "body": body,
    }


# ---------------------------------------------------------------------------
# oeffentliche API - signaturkompatibel zu _codec.py
# ---------------------------------------------------------------------------


def build_config_sync_snapshot(resources, node_id=None):
    body = _pick_sections(
        resources,
        sections=(
            "local_node",
            "event_types",
            "controllers",
            "virtual_controllers",
            "sensors",
            "actuators",
            "processes",
            "process_values",
            "process_modes",
            "process_states",
            "control_methods",
            "control_method_templates",
            "pid_configs",
            "schema_mappings",
        ),
    )
    return _envelope(node_id, body, kind="config_sync_snapshot")


def build_full_sync_payload(resources, node_id=None, sections=None):
    body = _pick_sections(resources, sections=sections)
    return _envelope(node_id, body, kind="full_sync_payload")


def build_health_snapshot(resources, node_id=None):
    resources_map = _safe_dict(resources)
    body = {
        "node_id": node_id,
        "healthy": True,
        "metrics": _deepcopy_safe(resources_map.get("metrics", {})),
        "timestamp_ms": _now_ms(),
    }
    return _envelope(node_id, body, kind="health_snapshot")


def build_runtime_state_snapshot(resources, node_id=None):
    resources_map = _safe_dict(resources)
    body = {
        "node_id": node_id,
        "runtime_state": _deepcopy_safe(resources_map.get("runtime_state", {})),
        "state_version": _deepcopy_safe(resources_map.get("state_version", {})),
        "timestamp_ms": _now_ms(),
    }
    return _envelope(node_id, body, kind="runtime_state_snapshot")


def build_worker_delta_payload(resources, node_id=None, worker_ref=None, since_version=None):
    resources_map = _safe_dict(resources)
    workers_map = _safe_dict(resources_map.get("workers"))
    worker_state = _deepcopy_safe(workers_map.get(worker_ref)) if worker_ref else None

    body = {
        "node_id": node_id,
        "worker_ref": worker_ref,
        "since_version": since_version,
        "worker_state": worker_state,
        "timestamp_ms": _now_ms(),
    }
    return _envelope(node_id, body, kind="worker_delta_payload")


# Lookup-Tabelle fuer read_resource_snapshot - Reihenfolge der Parameter
# ist (resources, node_id). functools.partial statt lambda (Projektregel).
_SNAPSHOT_BUILDERS = {
    "config":        build_config_sync_snapshot,
    "full":          build_full_sync_payload,
    "health":        build_health_snapshot,
    "runtime_state": build_runtime_state_snapshot,
    "state":         build_runtime_state_snapshot,
}


def _build_generic_section_snapshot(section_name, resources, node_id=None):
    section = _section_view(resources, section_name)
    body = {"section": section_name, "content": section}
    return _envelope(node_id, body, kind="section_snapshot")


def read_resource_snapshot(resource, resources, node_id=None, **kwargs):
    builder = _SNAPSHOT_BUILDERS.get(resource)
    if builder is not None:
        return builder(resources, node_id=node_id)

    # Fallback: als generische Sektions-Sicht zurueckliefern,
    # damit unbekannte resource-Namen nicht hart scheitern.
    return partial(_build_generic_section_snapshot, resource)(resources, node_id=node_id)
