# -*- coding: utf-8 -*-
# src/core/_snapshot_freeze.py
#
# Lieferung 2: Snapshot-Freeze fuer die GRE.
# Flag-gated via worker_runtime.snapshot_freeze_enabled. Default false.
# Keine Klassen, keine Decorator, keine lambda. functools.partial wo noetig.
#
# Vertrag:
#   build_snapshot(resources, keys=None) -> frozen_dict
#     Erzeugt einen deepcopy-Snapshot der benannten Ressourcen-Slices.
#     Wenn keys None ist, werden die Default-Lese-Keys verwendet.
#   is_enabled(settings) -> bool
#   wrap_tick_inputs(settings, resources, keys=None) -> dict
#     Einzeiliger Aufruf an der GRE-Aufrufsite in _gre_worker_integration.
#     Wenn Flag aus: gibt das Live-Dict direkt zurueck (Null-Kosten-Pfad).

import copy

DEFAULT_SNAPSHOT_KEYS = (
    "config_data",
    "sensor_values",
    "actuator_values",
    "process_states",
)


def is_enabled(settings):
    wr = (settings or {}).get("worker_runtime", {}) or {}
    return bool(wr.get("snapshot_freeze_enabled", False))


def _select_keys(resources, keys):
    if keys is None:
        keys = DEFAULT_SNAPSHOT_KEYS
    selected = {}
    for key in keys:
        if key in (resources or {}):
            selected[key] = resources[key]
    return selected


def build_snapshot(resources, keys=None):
    selected = _select_keys(resources or {}, keys)
    return {
        "frozen": True,
        "keys": tuple(sorted(selected.keys())),
        "data": copy.deepcopy(selected),
    }


def wrap_tick_inputs(settings, resources, keys=None):
    if not is_enabled(settings):
        return {"frozen": False, "keys": (), "data": resources}
    return build_snapshot(resources, keys=keys)


def read_from(snapshot, key, default=None):
    if not isinstance(snapshot, dict):
        return default
    if snapshot.get("frozen"):
        return (snapshot.get("data") or {}).get(key, default)
    live = snapshot.get("data")
    if isinstance(live, dict):
        return live.get(key, default)
    return default
