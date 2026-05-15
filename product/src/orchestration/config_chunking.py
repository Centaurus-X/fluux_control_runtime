# -*- coding: utf-8 -*-

# src/orchestration/config_chunking.py

"""
Per-controller config chunking for the runtime worker threads.
--------------------------------------------------------------

Goal
~~~~
Instead of passing the entire master config dict to every controller worker
thread, this module extracts a lean, self-contained **chunk** that contains
only the rows relevant to one controller. A worker then operates on its
chunk as a read-only local snapshot and only interacts with the central
single-writer via the worker-writer-sync protocol (see
``core/_worker_writer_sync.py``).

Design
~~~~~~
- No OOP, no classes, no dataclasses.
- Pure functions over plain dicts.
- ``functools.partial`` is used internally (no lambdas).
- The chunk is deterministically ordered so that two calls with the same
  input return byte-identical output (important for cache/signature use).
- Unknown tables pass through untouched — callers can add future tables
  without editing this module.

Chunking rules
~~~~~~~~~~~~~~
A row belongs to a controller's chunk when **any** of its configured
foreign-key columns matches the controller id. Columns checked (in this
order, first hit wins):

    controller_id, ctrl_id, target_controller_id, device_id

Cross-domain rows (rule-engine side outputs that target *other*
controllers) are detected via the ``cross_domain_targets`` or
``cross_domain_scope`` columns and are included in the chunk of the
*source* controller. This is the local mirror of the router-level
cross-domain resolver and keeps the worker able to evaluate its rules
without fetching the full master.

Tables that carry **no foreign key** to a controller (e.g. global
``unit_defaults`` / ``application_settings``) are copied by reference
— the worker must treat them as read-only.
"""

from functools import partial


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
CONTROLLER_FK_COLUMNS = (
    "controller_id",
    "ctrl_id",
    "target_controller_id",
    "device_id",
)

CROSS_DOMAIN_COLUMNS = (
    "cross_domain_targets",
    "cross_domain_scope",
)

# Tables whose rows are ALWAYS chunked per controller. For each entry,
# the chunker enforces that at least one foreign-key column is present.
CONTROLLER_SCOPED_TABLES = (
    "controllers",
    "sensors",
    "actuators",
    "controller_tasks",
    "automation_tasks",
    "process_states",
    "automation_rule_sets",
    "triggers",
    "transitions",
    "actions",
    "events",
    "timers",
    "dynamic_rule_engine",
)

# Tables that are kept as-is (shared across all workers).
GLOBAL_PASSTHROUGH_TABLES = (
    "unit_defaults",
    "application_settings",
    "sensor_types",
    "actuator_types",
    "bus_timing_profiles",
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _safe_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _row_matches_controller(row, controller_id):
    """
    Return True when a config row belongs to the given controller.
    """
    if not isinstance(row, dict):
        return False

    cid_int = _safe_int(controller_id)
    if cid_int is None:
        return False

    for column in CONTROLLER_FK_COLUMNS:
        value = row.get(column)
        if value is None:
            continue
        value_int = _safe_int(value)
        if value_int is None:
            continue
        if value_int == cid_int:
            return True

    return False


def _row_cross_domain_source(row, controller_id):
    """
    Return True when the row is a cross-domain producer that is hosted on
    ``controller_id`` but fans out to other controllers. This keeps the
    rule engine able to evaluate it locally.
    """
    if not isinstance(row, dict):
        return False

    cid_int = _safe_int(controller_id)
    if cid_int is None:
        return False

    # Must originate at this controller
    originated_here = False
    for column in CONTROLLER_FK_COLUMNS:
        value_int = _safe_int(row.get(column))
        if value_int is not None and value_int == cid_int:
            originated_here = True
            break

    if not originated_here:
        return False

    for column in CROSS_DOMAIN_COLUMNS:
        value = row.get(column)
        if value is None:
            continue
        if isinstance(value, (list, tuple, dict)) and value:
            return True
        if isinstance(value, str) and value.strip():
            return True

    return False


def _filter_table_rows(rows, controller_id):
    """
    Return a new list containing only rows that match the controller.
    Order is preserved so that signatures are stable.
    """
    if not isinstance(rows, (list, tuple)):
        return []

    match_fn = partial(_row_matches_controller, controller_id=controller_id)
    cross_fn = partial(_row_cross_domain_source, controller_id=controller_id)

    result = []
    for row in rows:
        if match_fn(row) or cross_fn(row):
            result.append(row)
    return result


<<<<<<< HEAD
=======
def _collect_int_values_from_rows(rows, column_name):
    """Return a deterministic set of int values from list rows."""
    result = set()
    if not isinstance(rows, (list, tuple)):
        return result

    for row in rows:
        if not isinstance(row, dict):
            continue
        value_int = _safe_int(row.get(column_name))
        if value_int is not None:
            result.add(value_int)
    return result


def _normalize_row_id_list(value):
    """Normalize scalar/list id columns to a list of unique ints."""
    if value is None:
        return []
    if not isinstance(value, (list, tuple, set)):
        value = [value]

    result = []
    seen = set()
    for item in value:
        item_int = _safe_int(item)
        if item_int is None or item_int in seen:
            continue
        seen.add(item_int)
        result.append(item_int)
    return result


def _row_references_any_id(row, column_names, allowed_ids):
    """Return True if a row references one of ``allowed_ids`` in any id column."""
    if not isinstance(row, dict) or not allowed_ids:
        return False

    for column_name in column_names:
        for item_int in _normalize_row_id_list(row.get(column_name)):
            if item_int in allowed_ids:
                return True
    return False


def _filter_process_state_rows(rows, controller_id, controller_sensor_ids, controller_actuator_ids):
    """Filter process states by controller ownership and state dependencies.

    Process-state rows often do not carry a direct ``controller_id``.  They are
    nevertheless controller-relevant when their ``sensor_ids`` or
    ``actuator_ids`` reference devices owned by the controller.  This is
    important for deterministic PID/PI/PD liveness because controller workers
    derive forced automation sensors from ``process_states``.
    """
    if not isinstance(rows, (list, tuple)):
        return []

    match_fn = partial(_row_matches_controller, controller_id=controller_id)
    cross_fn = partial(_row_cross_domain_source, controller_id=controller_id)

    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if match_fn(row) or cross_fn(row):
            result.append(row)
            continue
        if _row_references_any_id(row, ("sensor_ids", "sensor_id"), controller_sensor_ids):
            result.append(row)
            continue
        if _row_references_any_id(row, ("actuator_ids", "actuator_id"), controller_actuator_ids):
            result.append(row)
            continue
    return result


>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def chunk_controller_config(master_config, controller_id):
    """
    Build the lean per-controller config chunk.

    Parameters
    ----------
    master_config : dict
        The full master config dict (as produced by the DataStore /
        compiler system, or loaded from YAML).
    controller_id : int-like
        The numeric controller id this chunk is for.

    Returns
    -------
    dict
        A new dict containing:
        - ``controller_id``          : int
        - ``controller_scoped``      : dict of filtered tables
        - ``global_passthrough``     : dict of shared tables (by reference)
        - ``unscoped_tables``        : dict of any remaining tables we did
                                       not classify, passed through untouched
        - ``meta``                   : {"row_count": int, "table_counts": {}}
    """
    if not isinstance(master_config, dict):
        return {
            "controller_id": _safe_int(controller_id),
            "controller_scoped": {},
            "global_passthrough": {},
            "unscoped_tables": {},
            "meta": {"row_count": 0, "table_counts": {}},
        }

    cid_int = _safe_int(controller_id)
    controller_scoped = {}
    table_counts = {}
    total_rows = 0

    for table_name in CONTROLLER_SCOPED_TABLES:
        rows = master_config.get(table_name)
<<<<<<< HEAD
        filtered = _filter_table_rows(rows, cid_int)
=======
        if table_name == "process_states":
            controller_sensor_ids = _collect_int_values_from_rows(
                controller_scoped.get("sensors"), "sensor_id"
            )
            controller_actuator_ids = _collect_int_values_from_rows(
                controller_scoped.get("actuators"), "actuator_id"
            )
            filtered = _filter_process_state_rows(
                rows, cid_int, controller_sensor_ids, controller_actuator_ids
            )
        else:
            filtered = _filter_table_rows(rows, cid_int)
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
        controller_scoped[table_name] = filtered
        table_counts[table_name] = len(filtered)
        total_rows += len(filtered)

    global_passthrough = {}
    for table_name in GLOBAL_PASSTHROUGH_TABLES:
        value = master_config.get(table_name)
        if value is not None:
            global_passthrough[table_name] = value

    known = set(CONTROLLER_SCOPED_TABLES) | set(GLOBAL_PASSTHROUGH_TABLES)
    unscoped_tables = {}
    for key, value in master_config.items():
        if key in known:
            continue
        unscoped_tables[key] = value

    return {
        "controller_id": cid_int,
        "controller_scoped": controller_scoped,
        "global_passthrough": global_passthrough,
        "unscoped_tables": unscoped_tables,
        "meta": {
            "row_count": total_rows,
            "table_counts": table_counts,
        },
    }


def materialize_chunk_as_flat_config(chunk):
    """
    Convert a chunk back into a flat dict in the same shape as the master
    config — useful when legacy code expects a single ``config_data`` dict.

    The flat config contains:
    - all rows from ``controller_scoped`` under their original table names
    - all tables from ``global_passthrough``
    - all tables from ``unscoped_tables``

    Row lists are new list objects (shallow copies), so mutating them in
    the worker cannot race the master. Table values from the passthrough
    sections are shared by reference — they are expected to be read-only.
    """
    if not isinstance(chunk, dict):
        return {}

    flat = {}

    scoped = chunk.get("controller_scoped", {}) or {}
    for table_name, rows in scoped.items():
        if isinstance(rows, (list, tuple)):
            flat[table_name] = list(rows)
        else:
            flat[table_name] = rows

    passthrough = chunk.get("global_passthrough", {}) or {}
    for table_name, value in passthrough.items():
        flat[table_name] = value

    unscoped = chunk.get("unscoped_tables", {}) or {}
    for table_name, value in unscoped.items():
        if table_name not in flat:
            flat[table_name] = value

    return flat


def chunk_signature(chunk):
    """
    Return a cheap deterministic signature for a chunk.

    The signature is based on ``controller_id`` and the per-table row
    counts. It is **not** a content hash — it is meant for cheap
    change-detection in the worker main loop, not for deduplication.
    """
    if not isinstance(chunk, dict):
        return (None, ())

    cid = chunk.get("controller_id")
    table_counts = chunk.get("meta", {}).get("table_counts", {}) or {}
    sorted_counts = tuple(sorted(
        (str(k), int(v)) for k, v in table_counts.items()
    ))
    return (cid, sorted_counts)


def build_chunk_map(master_config, controller_ids):
    """
    Build a dict mapping ``controller_id -> chunk`` for a list of controllers.
    """
    result = {}
    for controller_id in controller_ids or ():
        cid = _safe_int(controller_id)
        if cid is None:
            continue
        result[cid] = chunk_controller_config(master_config, cid)
    return result
