# -*- coding: utf-8 -*-

# src/core/_event_priority.py

"""
Hierarchical, deterministic event priority for the sync event router.
---------------------------------------------------------------------

Goals
~~~~~
- Targets resolve to a **priority class** (small integer, lower = higher).
- Routing is deterministic: same input -> same dispatch order.
- No OOP, pure functions, pure dicts.
- ``functools.partial`` instead of ``lambda``.
- The module is stand-alone and importable without side effects.

Priority classes
~~~~~~~~~~~~~~~~
0   SYSTEM_CRITICAL     heartbeat, stop, shutdown, watchdog
10  STATE_WRITER        SEM / state event management (single writer)
20  DATASTORE           worker datastore / config patches
30  CONTROLLER          controller / automation tasks (per-worker)
40  AUTOMATION          automation threads
50  THREAD_MANAGEMENT   process controller / tm
60  FIELDBUS            modbus / fieldbus / hardware bus
70  TELEMETRY           telemetry / worker mirror
80  UI                  websocket / mqtt bridge
100 DEFAULT             unclassified

Events are primary-dispatched strictly in ascending priority order.
Broadcast mirrors run **after** the deterministic primary pipeline.
"""

from functools import partial


# ---------------------------------------------------------------------
# Priority class constants
# ---------------------------------------------------------------------
PRIO_SYSTEM_CRITICAL = 0
PRIO_STATE_WRITER = 10
PRIO_DATASTORE = 20
PRIO_CONTROLLER = 30
PRIO_AUTOMATION = 40
PRIO_THREAD_MANAGEMENT = 50
PRIO_FIELDBUS = 60
PRIO_TELEMETRY = 70
PRIO_UI = 80
PRIO_DEFAULT = 100


# ---------------------------------------------------------------------
# Domain classification
# ---------------------------------------------------------------------
DOMAIN_SYSTEM = "system"
DOMAIN_STATE = "state"
DOMAIN_DATA = "data"
DOMAIN_CONTROLLER = "controller"
DOMAIN_AUTOMATION = "automation"
DOMAIN_THREAD_MGMT = "thread_management"
DOMAIN_FIELDBUS = "fieldbus"
DOMAIN_TELEMETRY = "telemetry"
DOMAIN_UI = "ui"
DOMAIN_UNKNOWN = "unknown"


# ---------------------------------------------------------------------
# Target -> (priority, domain) mapping
#
# This is the single authoritative classification table. It stays in sync
# with build_default_routing_table() in sync_event_router.py.
# ---------------------------------------------------------------------
TARGET_PRIORITY_TABLE = {
    # System
    "heartbeat": (PRIO_SYSTEM_CRITICAL, DOMAIN_SYSTEM),

    # State writer (single source of truth sink)
    "state":                   (PRIO_STATE_WRITER, DOMAIN_STATE),
    "state_handler":           (PRIO_STATE_WRITER, DOMAIN_STATE),
    "state_management":        (PRIO_STATE_WRITER, DOMAIN_STATE),
    "state_event_management":  (PRIO_STATE_WRITER, DOMAIN_STATE),
    "sem":                     (PRIO_STATE_WRITER, DOMAIN_STATE),
    "event":                   (PRIO_STATE_WRITER, DOMAIN_STATE),
    "event_handler":           (PRIO_STATE_WRITER, DOMAIN_STATE),

    # Datastore / config data
    "datastore":    (PRIO_DATASTORE, DOMAIN_DATA),
    "database":     (PRIO_DATASTORE, DOMAIN_DATA),
    "ds":           (PRIO_DATASTORE, DOMAIN_DATA),
    "data_config":  (PRIO_DATASTORE, DOMAIN_DATA),
    "config_data":  (PRIO_DATASTORE, DOMAIN_DATA),
    "dc":           (PRIO_DATASTORE, DOMAIN_DATA),

    # Controller / worker runtime (GRE-driven)
    "controller": (PRIO_CONTROLLER, DOMAIN_CONTROLLER),
    "mbc":        (PRIO_CONTROLLER, DOMAIN_CONTROLLER),

    # Automation
    "automation": (PRIO_AUTOMATION, DOMAIN_AUTOMATION),
    "mba":        (PRIO_AUTOMATION, DOMAIN_AUTOMATION),

    # Thread management
    "thread_management":  (PRIO_THREAD_MANAGEMENT, DOMAIN_THREAD_MGMT),
    "tm":                 (PRIO_THREAD_MANAGEMENT, DOMAIN_THREAD_MGMT),
    "pc":                 (PRIO_THREAD_MANAGEMENT, DOMAIN_THREAD_MGMT),
    "process_controller": (PRIO_THREAD_MANAGEMENT, DOMAIN_THREAD_MGMT),

    # Fieldbus / hardware
    "modbus":         (PRIO_FIELDBUS, DOMAIN_FIELDBUS),
    "mbh":            (PRIO_FIELDBUS, DOMAIN_FIELDBUS),
    "modbus_handler": (PRIO_FIELDBUS, DOMAIN_FIELDBUS),

    # Telemetry / worker mirror
    "telemetry":       (PRIO_TELEMETRY, DOMAIN_TELEMETRY),
    "telemetry_event": (PRIO_TELEMETRY, DOMAIN_TELEMETRY),
    "ti":              (PRIO_TELEMETRY, DOMAIN_TELEMETRY),
    "worker":          (PRIO_TELEMETRY, DOMAIN_TELEMETRY),
    "worker_event":    (PRIO_TELEMETRY, DOMAIN_TELEMETRY),

    # UI transport
    "websocket": (PRIO_UI, DOMAIN_UI),
    "ws":        (PRIO_UI, DOMAIN_UI),
}


# ---------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------

def classify_target(target_name):
    """
    Return (priority, domain) for a target name.

    Unknown targets map to (PRIO_DEFAULT, DOMAIN_UNKNOWN) — that still
    preserves determinism because the sort is by (priority, target).
    """
    try:
        if target_name is None:
            return (PRIO_DEFAULT, DOMAIN_UNKNOWN)
        key = str(target_name).strip().lower()
        if not key:
            return (PRIO_DEFAULT, DOMAIN_UNKNOWN)
        return TARGET_PRIORITY_TABLE.get(key, (PRIO_DEFAULT, DOMAIN_UNKNOWN))
    except Exception:
        return (PRIO_DEFAULT, DOMAIN_UNKNOWN)


def get_target_priority(target_name):
    return classify_target(target_name)[0]


def get_target_domain(target_name):
    return classify_target(target_name)[1]


def _priority_sort_key(target_name):
    priority, domain = classify_target(target_name)
    return (priority, str(domain), str(target_name or ""))


def order_targets_deterministic(targets):
    """
    Return a deterministic, priority-sorted tuple of targets.

    - Stable even when priorities tie (secondary sort by domain, then
      by target name).
    - Duplicates removed, preserving the first occurrence.
    """
    try:
        if not targets:
            return tuple()

        seen = set()
        unique = []
        for target in targets:
            if target is None:
                continue
            key = str(target).strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            unique.append(key)

        if not unique:
            return tuple()

        unique.sort(key=partial(_priority_sort_key))
        return tuple(unique)
    except Exception:
        return tuple()


def order_targets_by_priority_domain(targets):
    """
    Same as ``order_targets_deterministic`` but also returns the
    ``(priority, domain, target)`` tuples for diagnostic use.
    """
    try:
        ordered_names = order_targets_deterministic(targets)
        result = []
        for target in ordered_names:
            priority, domain = classify_target(target)
            result.append((priority, domain, target))
        return tuple(result)
    except Exception:
        return tuple()
