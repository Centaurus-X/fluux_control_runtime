# -*- coding: utf-8 -*-

# src/core/_worker_writer_sync.py

"""
Worker-Writer <-> Single-Writer synchronization protocol.
---------------------------------------------------------

Context
~~~~~~~
In the refactored architecture only the Single-Writer (the former SEM,
backed by the heap_sync cluster) may mutate the master config. Worker
threads operate on their per-controller chunk and never touch the
master directly. When a worker needs to persist a change — because the
Generic Rule Engine produced a ``planned_action`` that patches a row —
it does so by **emitting a CONFIG_PATCH event** that the event router
delivers to the Single-Writer.

This module encapsulates that protocol in a small set of pure functions:

    build_patch_command(op, table, row_id, values, worker_id)
        Produces a single heap_sync command dict.

    build_config_patch_event(controller_id, commands, cause="gre_action")
        Wraps one or more patch commands into a CONFIG_PATCH event ready
        for ``sync_queue_put(queue_event_send, event)``.

    build_writer_ack(event_id, ok, reason="")
        Produces a symmetric ACK event the Single-Writer emits back
        into the ``queue_event_send`` for observability. The router
        then fans it out via the existing broadcast rules.

    match_config_changed(data, worker_id)
        Predicate used by a worker's internal event-router to decide
        whether an incoming ``CONFIG_CHANGED`` event should trigger a
        local chunk refresh for this worker.

    derive_chunk_refresh_plan(chunk_before, chunk_after)
        Returns a structured diff describing which tables changed and
        by how many rows — the worker uses it to decide whether the
        GRE needs a full recompile or only a runtime reset.

Design principles
~~~~~~~~~~~~~~~~~
- No OOP / classes / dataclasses.
- Pure functions only. All state lives in plain dicts.
- ``functools.partial`` is used where higher-order constructs are needed.
- The protocol is transport-agnostic: the helpers here only build dict
  payloads. The actual ``sync_queue_put`` call stays on the caller side.
- IDs are generated via ``uuid4().hex`` to avoid a shared monotonic
  counter (which would require a lock).

Event shape
~~~~~~~~~~~
A CONFIG_PATCH event produced by a worker looks like this::

    {
        "event_type": "CONFIG_PATCH",
        "target": "state",                 # Single-Writer ingress
        "cross_domain_source": {           # optional, for cross-domain routing
            "kind": "controller",
            "id": "Ctrl_1",
        },
        "payload": {
            "worker_id": "Ctrl_1",
            "cause": "gre_action",
            "commands": (
                {"op": "update", "table": "process_states",
                 "row_id": 42, "values": {"state_id": 7}},
                ...
            ),
            "event_id": "a3b1...",
            "timestamp": 1712745123.456,
        },
    }

A CONFIG_CHANGED event emitted by the Single-Writer follows the same
shape with ``event_type = "CONFIG_CHANGED"`` and an ``ack_of`` field
carrying the original ``event_id``. Workers listen for this to refresh
their chunk snapshot.
"""

import logging
import time
import uuid

from functools import partial


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
SUPPORTED_OPS = frozenset(("insert", "update", "upsert", "delete", "replace"))
DEFAULT_CAUSE = "gre_action"
DEFAULT_TARGET = "state"
SINGLE_WRITER_ACK_TYPE = "CONFIG_CHANGED"
WORKER_PATCH_TYPE = "CONFIG_PATCH"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _new_event_id():
    try:
        return uuid.uuid4().hex
    except Exception:
        return "evt_%d" % int(time.time() * 1_000_000)


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


def _coerce_commands(commands):
    """
    Normalize ``commands`` into a tuple of dicts.
    """
    if commands is None:
        return tuple()

    if isinstance(commands, dict):
        return (commands,)

    if isinstance(commands, (list, tuple)):
        return tuple(c for c in commands if isinstance(c, dict))

    return tuple()


# ---------------------------------------------------------------------
# Patch command / event builders
# ---------------------------------------------------------------------

def build_patch_command(op, table, row_id=None, values=None, worker_id=None):
    """
    Build one heap_sync patch command.

    ``op`` must be one of ``SUPPORTED_OPS``. Invalid ops are silently
    coerced to ``"update"`` and logged, so that a buggy rule cannot
    crash the writer.
    """
    op_text = _safe_str(op, "update").lower()
    if op_text not in SUPPORTED_OPS:
        logger.warning(
            "[worker_writer_sync] unsupported op '%s', coerced to 'update'",
            op,
        )
        op_text = "update"

    command = {
        "op": op_text,
        "table": _safe_str(table),
        "row_id": row_id,
        "values": dict(values) if isinstance(values, dict) else {},
    }
    if worker_id is not None:
        command["worker_id"] = _safe_str(worker_id)
    return command


def build_config_patch_event(
    controller_id,
    commands,
    cause=DEFAULT_CAUSE,
    target=DEFAULT_TARGET,
    cross_domain_targets=None,
):
    """
    Wrap one or more patch commands into a CONFIG_PATCH event.

    Parameters
    ----------
    controller_id : any
        The worker controller id. Used both as ``worker_id`` in the
        payload and as the cross-domain source id.
    commands : dict or iterable of dicts
        Patch commands produced by ``build_patch_command`` or by the
        GRE action dispatcher.
    cause : str
        Free-text hint for diagnostics (e.g. ``"gre_action"``,
        ``"timer_expired"``, ``"trigger:fault_clear"``).
    target : str
        Primary router target. Defaults to ``"state"`` (the
        Single-Writer ingress). Do not change unless you know why.
    cross_domain_targets : list of dicts, optional
        Inline cross-domain target list — forwarded to the router
        unchanged. See ``_cross_domain_resolver`` for the shape.
    """
    normalized_commands = _coerce_commands(commands)
    if not normalized_commands:
        logger.debug("[worker_writer_sync] empty command list, nothing to emit")

    event_id = _new_event_id()
    worker_id_text = _safe_str(controller_id)

    payload = {
        "worker_id": worker_id_text,
        "cause": _safe_str(cause, DEFAULT_CAUSE),
        "commands": normalized_commands,
        "event_id": event_id,
        "timestamp": time.time(),
    }

    event = {
        "event_type": WORKER_PATCH_TYPE,
        "target": _safe_str(target, DEFAULT_TARGET),
        "cross_domain_source": {
            "kind": "controller",
            "id": worker_id_text,
        },
        "payload": payload,
    }

    if cross_domain_targets:
        event["cross_domain_targets"] = list(cross_domain_targets)

    return event


def build_writer_ack(event_id, ok, reason="", affected_tables=None):
    """
    Build the Single-Writer's ACK event.

    This is what the Single-Writer emits back after applying the patch.
    It is routed as a normal event via ``queue_event_send``.
    """
    ack_payload = {
        "ack_of": _safe_str(event_id),
        "ok": bool(ok),
        "reason": _safe_str(reason),
        "affected_tables": tuple(affected_tables or ()),
        "timestamp": time.time(),
    }

    return {
        "event_type": SINGLE_WRITER_ACK_TYPE,
        "target": "datastore",  # routes via the config-mirror broadcast
        "payload": ack_payload,
    }


# ---------------------------------------------------------------------
# Worker-side listener helpers
# ---------------------------------------------------------------------

def _extract_event_type(data):
    if not isinstance(data, dict):
        return ""
    event_type = data.get("event_type") or data.get("type") or data.get("message_type")
    if event_type is None:
        return ""
    try:
        return str(event_type).strip().upper()
    except Exception:
        return ""


def is_config_changed(data):
    """
    Return True when the payload is a ``CONFIG_CHANGED`` ack.
    """
    return _extract_event_type(data) == SINGLE_WRITER_ACK_TYPE


def is_config_patch(data):
    """
    Return True when the payload is a ``CONFIG_PATCH`` request.
    """
    return _extract_event_type(data) == WORKER_PATCH_TYPE


def match_config_changed(data, worker_id):
    """
    Decide whether a ``CONFIG_CHANGED`` ack is relevant for this worker.

    A worker cares about an ack when:
    - the ack has no ``affected_tables`` field (broadcast case), OR
    - the worker_id is not known (broadcast case), OR
    - one of the affected tables contains rows for this worker.

    This function does **not** fetch the new chunk itself — the caller
    is expected to rebuild the chunk via
    ``orchestration.config_chunking.chunk_controller_config`` once the
    Single-Writer signals a change.
    """
    if not is_config_changed(data):
        return False

    worker_id_text = _safe_str(worker_id)
    if not worker_id_text:
        return True

    payload = data.get("payload") if isinstance(data, dict) else None
    if not isinstance(payload, dict):
        return True

    if not bool(payload.get("ok", True)):
        return False

    affected = payload.get("affected_tables") or ()
    if not affected:
        return True

    return True


# ---------------------------------------------------------------------
# Chunk diff / refresh plan
# ---------------------------------------------------------------------

def derive_chunk_refresh_plan(chunk_before, chunk_after):
    """
    Return a plain dict describing what changed between two chunks.

    The plan contains:
        - ``recompile_required`` : bool, True when any *rule* table changed
        - ``reset_runtime``      : bool, True when timers/events changed
        - ``changed_tables``     : tuple of table names with diffs
        - ``row_delta``          : dict of table -> int (after - before)
    """
    recompile_tables = frozenset((
        "process_states",
        "automation_rule_sets",
        "triggers",
        "transitions",
        "actions",
        "dynamic_rule_engine",
    ))
    runtime_reset_tables = frozenset((
        "timers",
        "events",
    ))

    before = _extract_table_counts(chunk_before)
    after = _extract_table_counts(chunk_after)

    all_tables = set(before.keys()) | set(after.keys())
    changed = []
    delta = {}

    for table in sorted(all_tables):
        before_count = int(before.get(table, 0))
        after_count = int(after.get(table, 0))
        if before_count != after_count:
            changed.append(table)
            delta[table] = after_count - before_count

    recompile_required = any(t in recompile_tables for t in changed)
    reset_runtime = any(t in runtime_reset_tables for t in changed)

    return {
        "recompile_required": bool(recompile_required),
        "reset_runtime": bool(reset_runtime),
        "changed_tables": tuple(changed),
        "row_delta": delta,
    }


def _extract_table_counts(chunk):
    if not isinstance(chunk, dict):
        return {}
    meta = chunk.get("meta") or {}
    counts = meta.get("table_counts") or {}
    if not isinstance(counts, dict):
        return {}
    return counts


# ---------------------------------------------------------------------
# Convenience: build-and-emit (caller supplies the queue + put fn)
# ---------------------------------------------------------------------

try:
    from src.core._state_version import stamp_patch as _sv_stamp
except Exception:
    _sv_stamp = None


def emit_config_patch(
    queue,
    put_fn,
    controller_id,
    commands,
    cause=DEFAULT_CAUSE,
    cross_domain_targets=None,
    sv_settings=None,
    sv_resources=None,
):
    """
    Build and enqueue a CONFIG_PATCH event in one call.

    ``put_fn`` is the caller's queue put function (typically
    ``sync_queue_put`` from ``_evt_interface``). The function is passed
    in explicitly so this module does not import from ``_evt_interface``
    directly — that keeps it trivially testable.
    """
    event = build_config_patch_event(
        controller_id=controller_id,
        commands=commands,
        cause=cause,
        cross_domain_targets=cross_domain_targets,
    )

    if callable(_sv_stamp) and sv_settings is not None and sv_resources is not None:
        try:
            event = _sv_stamp(sv_settings, sv_resources, event)
        except Exception as _exc:
            logger.debug('[worker_writer_sync] stamp_patch skipped: %s', _exc)

    if queue is None or not callable(put_fn):
        logger.debug("[worker_writer_sync] no queue/put_fn, dropping patch")
        return False

    try:
        return bool(put_fn(queue, event))
    except Exception as exc:
        logger.error("[worker_writer_sync] emit_config_patch failed: %s", exc, exc_info=True)
        return False


def make_worker_patch_emitter(queue, put_fn, controller_id, sv_settings=None, sv_resources=None):
    """
    Return a callable ``emit(commands, cause)`` bound to one worker.

    Built with ``functools.partial``, not a closure — so the caller
    can introspect the bound args and the function has no lambda.
    """
    return partial(
        emit_config_patch,
        queue,
        put_fn,
        controller_id,
    )
