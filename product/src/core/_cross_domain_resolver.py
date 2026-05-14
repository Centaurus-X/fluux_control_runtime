# -*- coding: utf-8 -*-

# src/core/_cross_domain_resolver.py

"""
Cross-Domain Resolver
---------------------

Deterministic, priority-ordered resolver for cross-domain event routing.

Context
~~~~~~~
Classical event routing assumes a 1:1 relationship between a message and its
target domain (e.g. a sensor belonging to Ctrl_1 only affects Ctrl_1). In
real-world bus/automation setups this assumption breaks:

    Sensor_1 is wired to Ctrl_1
    Actor_2  is wired to Ctrl_2
    But a rule on Sensor_1 must drive Actor_2 via Ctrl_2.

This module provides a **pure functional** way to express such cross-domain
bindings as a deterministic, priority-ordered target set attached to an event.

Design principles
~~~~~~~~~~~~~~~~~
- No OOP, no classes, no dataclasses.
- All state lives in plain dicts / tuples.
- ``functools.partial`` instead of ``lambda``.
- Fully deterministic ordering via ``(priority, target_name)``.
- Read-only resolver functions — the caller owns state.
- Minimum coupling with the main event router: the router only needs to call
  ``extract_cross_domain_targets(data, mapping)`` and feed the result into
  ``dispatch_targets`` right after the primary targets.

Mapping shape
~~~~~~~~~~~~~
The mapping is a plain dict loaded from the config / heap snapshot::

    {
        "sensor:Sensor_1": [
            {"target": "controller", "scope": "Ctrl_2", "priority": 10},
            {"target": "automation", "scope": "Ctrl_2", "priority": 20},
        ],
        "actor:Actor_2": [
            {"target": "controller", "scope": "Ctrl_2", "priority": 10},
        ],
    }

Keys are ``<domain>:<entity_id>`` tuples. Values are lists of target specs.
A target spec is a dict with:

    - ``target``    : logical router target name (must match routing table)
    - ``scope``     : optional scope tag (e.g. controller id) for the worker
    - ``priority``  : integer, lower value = higher priority (default: 100)
    - ``enabled``   : optional bool, defaults to True

Resolution order
~~~~~~~~~~~~~~~~
1. Extract the source key from the event (explicit ``source`` field or
   inferred from ``sensor_id`` / ``actor_id`` / ``entity_id``).
2. Look up the mapping entry.
3. Filter disabled entries.
4. Sort deterministically by ``(priority, target, scope)``.
5. Return a tuple of target dicts ready for dispatch.
"""

from functools import partial


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
DEFAULT_PRIORITY = 100
SUPPORTED_SOURCE_KINDS = ("sensor", "actor", "entity", "timer", "trigger")


# ---------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------

def _safe_str(value, default=""):
    try:
        if value is None:
            return default
        text = str(value).strip()
        if not text:
            return default
        return text
    except Exception:
        return default


def _safe_int(value, default=DEFAULT_PRIORITY):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _safe_bool(value, default=True):
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
        return default
    except Exception:
        return default


def normalize_source_key(kind, entity_id):
    """
    Build a normalized ``<kind>:<entity_id>`` key.
    """
    kind_text = _safe_str(kind).lower()
    entity_text = _safe_str(entity_id)

    if not kind_text or not entity_text:
        return ""

    if kind_text not in SUPPORTED_SOURCE_KINDS:
        return ""

    return "%s:%s" % (kind_text, entity_text)


def extract_source_key(data):
    """
    Extract a normalized source key from an event payload.

    Detection order:
    1. explicit ``cross_domain_source`` field: ``{"kind": ..., "id": ...}``
    2. ``source`` field: same shape
    3. ``sensor_id`` -> ``sensor:<id>``
    4. ``actor_id``  -> ``actor:<id>``
    5. ``entity_id`` with optional ``entity_kind``
    """
    if not isinstance(data, dict):
        return ""

    explicit = data.get("cross_domain_source")
    if isinstance(explicit, dict):
        key = normalize_source_key(explicit.get("kind"), explicit.get("id"))
        if key:
            return key

    source_value = data.get("source")
    if isinstance(source_value, dict):
        key = normalize_source_key(source_value.get("kind"), source_value.get("id"))
        if key:
            return key

    sensor_id = data.get("sensor_id")
    if sensor_id is not None:
        key = normalize_source_key("sensor", sensor_id)
        if key:
            return key

    actor_id = data.get("actor_id")
    if actor_id is not None:
        key = normalize_source_key("actor", actor_id)
        if key:
            return key

    entity_id = data.get("entity_id")
    if entity_id is not None:
        entity_kind = data.get("entity_kind") or "entity"
        key = normalize_source_key(entity_kind, entity_id)
        if key:
            return key

    return ""


# ---------------------------------------------------------------------
# Mapping build / validation
# ---------------------------------------------------------------------

def normalize_target_spec(entry):
    """
    Normalize one target spec to a plain dict.
    """
    if not isinstance(entry, dict):
        return None

    target_name = _safe_str(entry.get("target")).lower()
    if not target_name:
        return None

    return {
        "target": target_name,
        "scope": _safe_str(entry.get("scope")),
        "priority": _safe_int(entry.get("priority"), DEFAULT_PRIORITY),
        "enabled": _safe_bool(entry.get("enabled"), True),
    }


def normalize_mapping(raw_mapping):
    """
    Normalize a raw cross-domain mapping dict.

    Returns a plain dict::

        {
            "<kind>:<id>": (target_spec_1, target_spec_2, ...),
            ...
        }

    The target specs in each value tuple are deterministically sorted.
    """
    if not isinstance(raw_mapping, dict):
        return {}

    normalized = {}

    for raw_key, raw_targets in raw_mapping.items():
        raw_key_text = _safe_str(raw_key)
        if ":" not in raw_key_text:
            continue

        # Only the kind prefix is case-insensitive. Entity IDs must
        # stay case-preserved because "Sensor_1" != "sensor_1" in
        # real configs.
        kind_part, _, entity_part = raw_key_text.partition(":")
        kind_text = kind_part.strip().lower()
        entity_text = entity_part.strip()
        if not kind_text or not entity_text:
            continue
        if kind_text not in SUPPORTED_SOURCE_KINDS:
            continue
        key_text = "%s:%s" % (kind_text, entity_text)

        if not isinstance(raw_targets, (list, tuple)):
            continue

        specs = []
        for entry in raw_targets:
            spec = normalize_target_spec(entry)
            if spec is None:
                continue
            if not spec["enabled"]:
                continue
            specs.append(spec)

        if not specs:
            continue

        specs.sort(key=partial(_spec_sort_key))
        normalized[key_text] = tuple(specs)

    return normalized


def _spec_sort_key(spec):
    return (
        int(spec.get("priority", DEFAULT_PRIORITY)),
        str(spec.get("target", "")),
        str(spec.get("scope", "")),
    )


# ---------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------

def resolve_targets(data, mapping):
    """
    Resolve the ordered target list for one event payload.

    Returns an empty tuple if:
    - the mapping is empty
    - no source key can be inferred
    - the source key is not present in the mapping
    """
    if not isinstance(mapping, dict) or not mapping:
        return tuple()

    source_key = extract_source_key(data)
    if not source_key:
        return tuple()

    specs = mapping.get(source_key)
    if not specs:
        return tuple()

    return tuple(specs)


def extract_cross_domain_target_names(data, mapping):
    """
    Convenience: return only the ordered ``target`` names (no scopes).

    This is the form the event router actually needs for ``dispatch_targets``.
    """
    resolved = resolve_targets(data, mapping)
    if not resolved:
        return tuple()

    seen = set()
    ordered = []
    for spec in resolved:
        target = spec.get("target")
        if not target or target in seen:
            continue
        seen.add(target)
        ordered.append(target)

    return tuple(ordered)


# ---------------------------------------------------------------------
# Inline mapping extraction
# ---------------------------------------------------------------------

def extract_inline_mapping(data):
    """
    Allow events to carry their own inline cross-domain mapping.

    Event shape::

        {
            "cross_domain_targets": [
                {"target": "controller", "scope": "Ctrl_2", "priority": 10},
                {"target": "automation", "scope": "Ctrl_2", "priority": 20},
            ]
        }

    This is useful for one-shot rule engine outputs where the engine
    has already computed the target set for this specific event.
    """
    if not isinstance(data, dict):
        return tuple()

    entries = data.get("cross_domain_targets")
    if not isinstance(entries, (list, tuple)):
        return tuple()

    specs = []
    for entry in entries:
        spec = normalize_target_spec(entry)
        if spec is None or not spec["enabled"]:
            continue
        specs.append(spec)

    specs.sort(key=partial(_spec_sort_key))

    seen = set()
    ordered = []
    for spec in specs:
        target = spec.get("target")
        if not target or target in seen:
            continue
        seen.add(target)
        ordered.append(target)

    return tuple(ordered)


def combine_cross_domain_targets(data, mapping):
    """
    Combine inline + mapping-based cross-domain targets into one
    deterministic ordered tuple.

    Inline targets win when priorities are equal, but the resulting
    sequence is still deterministic.
    """
    inline = extract_inline_mapping(data)
    mapped = extract_cross_domain_target_names(data, mapping)

    if not inline and not mapped:
        return tuple()

    seen = set()
    ordered = []

    for target in inline:
        if target in seen:
            continue
        seen.add(target)
        ordered.append(target)

    for target in mapped:
        if target in seen:
            continue
        seen.add(target)
        ordered.append(target)

    return tuple(ordered)
