# -*- coding: utf-8 -*-

# src/core/_gre_action_dispatcher.py

"""
Generic Rule Engine action dispatcher.
--------------------------------------

Translates ``planned_actions`` produced by ``_generic_rule_engine.evaluate()``
into heap_sync-compatible patch commands that can be emitted via
``_worker_writer_sync.build_config_patch_event``.

This is the pure, stateless replacement of the former PSM action dispatcher.

Design
~~~~~~
- No OOP, no classes, no dataclasses.
- Pure functions over plain dicts.
- ``functools.partial`` instead of ``lambda``.
- Mode-gating (auto / manual) remains a per-worker concern and is expressed
  here as a simple predicate ``is_automatic_mode_active(controller_data)``.
- Action kinds supported out of the box:
    * ``set_state``       -> update row in ``process_states``
    * ``set_actuator``    -> update row in ``actuators``
    * ``raise_event``     -> insert row in ``events``
    * ``arm_timer``       -> update row in ``timers``
    * ``patch_row``       -> direct raw patch (table + row_id + values)
- Unknown action kinds are logged and dropped — never raised. A broken
  rule must not crash the worker.

Typical caller
~~~~~~~~~~~~~~
``_gre_worker_integration.run_gre_tick()`` collects GRE actions, passes
them through ``dispatch_planned_actions(...)`` to get the command list,
and emits the commands via the worker-writer-sync protocol.
"""

import logging

from functools import partial

from src.core._worker_writer_sync import build_patch_command


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
MODE_AUTO = 2  # must match the legacy PSM mode gate

ACTION_KIND_HANDLERS = (
    "set_state",
    "set_actuator",
    "raise_event",
    "arm_timer",
    "patch_row",
)


# ---------------------------------------------------------------------
# Mode gate
# ---------------------------------------------------------------------

def is_automatic_mode_active(controller_data):
    """
    Return True when the controller is in automatic mode.

    The legacy PSM gated all writes behind ``mode_id == 2``. We preserve
    that contract verbatim so that existing YAML configs keep working.
    """
    if not isinstance(controller_data, dict):
        return False
    try:
        mode_value = controller_data.get("mode_id")
        if mode_value is None:
            return False
        return int(mode_value) == MODE_AUTO
    except Exception:
        return False


# ---------------------------------------------------------------------
# Action -> patch command translators
# ---------------------------------------------------------------------

def _translate_set_state(action, worker_id):
    row_id = action.get("row_id") or action.get("process_state_id")
    values = action.get("values")
    if not isinstance(values, dict):
        # Accept the common shortcut {"state_id": 7}
        state_id = action.get("state_id")
        if state_id is not None:
            values = {"state_id": state_id}
        else:
            values = {}
    return build_patch_command(
        op="update",
        table="process_states",
        row_id=row_id,
        values=values,
        worker_id=worker_id,
    )


def _translate_set_actuator(action, worker_id):
    row_id = action.get("row_id") or action.get("actuator_id")
    values = action.get("values")
    if not isinstance(values, dict):
        value = action.get("value")
        values = {"value": value} if value is not None else {}
    return build_patch_command(
        op="update",
        table="actuators",
        row_id=row_id,
        values=values,
        worker_id=worker_id,
    )


def _translate_raise_event(action, worker_id):
    values = dict(action.get("values") or {})
    for key in ("event_type", "event_id", "severity", "message"):
        if key in action and key not in values:
            values[key] = action.get(key)
    return build_patch_command(
        op="insert",
        table="events",
        row_id=action.get("row_id"),
        values=values,
        worker_id=worker_id,
    )


def _translate_arm_timer(action, worker_id):
    values = dict(action.get("values") or {})
    if "duration_s" in action and "duration_s" not in values:
        values["duration_s"] = action.get("duration_s")
    if "armed" not in values:
        values["armed"] = True
    return build_patch_command(
        op="update",
        table="timers",
        row_id=action.get("row_id") or action.get("timer_id"),
        values=values,
        worker_id=worker_id,
    )


def _translate_patch_row(action, worker_id):
    return build_patch_command(
        op=action.get("op", "update"),
        table=action.get("table"),
        row_id=action.get("row_id"),
        values=action.get("values") or {},
        worker_id=worker_id,
    )


# Registry: action kind -> translator
_TRANSLATORS = {
    "set_state": _translate_set_state,
    "set_actuator": _translate_set_actuator,
    "raise_event": _translate_raise_event,
    "arm_timer": _translate_arm_timer,
    "patch_row": _translate_patch_row,
}


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def translate_action(action, worker_id):
    """
    Translate one GRE planned action into a patch command.

    Returns None for unknown kinds. The caller must filter Nones out.
    """
    if not isinstance(action, dict):
        return None

    kind = action.get("kind") or action.get("action") or action.get("type")
    if kind is None:
        logger.debug("[gre_action] action without kind: %s", action)
        return None

    kind_text = str(kind).strip().lower()
    translator = _TRANSLATORS.get(kind_text)
    if translator is None:
        logger.warning("[gre_action] unsupported action kind '%s'", kind_text)
        return None

    try:
        return translator(action, worker_id)
    except Exception as exc:
        logger.error(
            "[gre_action] translator for '%s' failed: %s",
            kind_text,
            exc,
            exc_info=True,
        )
        return None


def dispatch_planned_actions(
    planned_actions,
    controller_data,
    worker_id,
    force_dispatch=False,
):
    """
    Translate a list of GRE planned actions into patch commands.

    Parameters
    ----------
    planned_actions : iterable of dicts
        The ``planned_actions`` list from ``gre.evaluate()`` output.
    controller_data : dict
        Per-worker controller row. Used for the automatic-mode gate.
    worker_id : any
        The worker's controller id (stamped into each command).
    force_dispatch : bool
        When True, the automatic-mode gate is bypassed. Intended for
        unit tests and for one-shot writer bootstraps; **not** for
        production rule eval.

    Returns
    -------
    tuple of dict
        The command tuple ready to feed into
        ``build_config_patch_event(controller_id, commands)``.
    """
    if not planned_actions:
        return tuple()

    if not force_dispatch and not is_automatic_mode_active(controller_data):
        logger.debug(
            "[gre_action] controller not in auto mode, dropping %d actions",
            len(list(planned_actions)),
        )
        return tuple()

    translator_fn = partial(translate_action, worker_id=worker_id)

    commands = []
    for action in planned_actions:
        command = translator_fn(action)
        if command is None:
            continue
        commands.append(command)

    return tuple(commands)
