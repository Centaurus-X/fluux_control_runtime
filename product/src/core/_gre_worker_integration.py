# -*- coding: utf-8 -*-

# src/core/_gre_worker_integration.py

"""
Per-worker Generic Rule Engine integration.
-------------------------------------------

This module is the thin glue between a runtime worker thread and the
pure Generic Rule Engine (``libraries/_generic_rule_engine``). It
replaces the old centralized PSM thread — each worker now owns its
own compiled rule set derived from its controller config chunk.

Design
~~~~~~
- No OOP, no classes.
- Pure functions + plain dicts for runtime state.
- ``functools.partial`` for any bound helpers.
- Opt-in: a worker only uses this module when it calls
  ``create_gre_worker_runtime(...)``. Legacy worker code paths are
  unaffected if they never construct the runtime.
- The runtime is a plain dict with the following keys::

      {
          "worker_id": <controller_id>,
          "compiled_config": <GRE compile result>,
          "runtime": <GRE runtime dict for edge detection / timers>,
          "emitter": partial(emit_config_patch, queue, put_fn, ctrl_id),
          "controller_data": <dict>,
          "last_chunk_signature": tuple,
          "stats": {
              "ticks": int,
              "commands_emitted": int,
              "actions_dropped": int,
          },
      }

- Config refresh: when a ``CONFIG_CHANGED`` ack for this worker arrives,
  ``refresh_runtime_chunk(runtime, new_chunk)`` rebuilds the compiled
  config and clears the GRE runtime as required by the diff plan.

Usage sketch
~~~~~~~~~~~~
::

    from src.core._gre_worker_integration import (
        create_gre_worker_runtime, run_gre_tick, refresh_runtime_chunk
    )
    from src.core._worker_writer_sync import make_worker_patch_emitter
    from src.orchestration.config_chunking import chunk_controller_config
    from src.libraries._evt_interface import sync_queue_put

    chunk = chunk_controller_config(master_config, controller_id=1)
    emitter = make_worker_patch_emitter(
        queue=queue_event_send, put_fn=sync_queue_put, controller_id=1
    )
    gre_runtime = create_gre_worker_runtime(
        worker_id=1,
        chunk=chunk,
        controller_data=ctrl_row,
        emitter=emitter,
    )

    # In the worker main loop, after every sensor/event update:
    run_gre_tick(
        gre_runtime,
        sensor_values=sensor_cache,
        state_values=state_cache,
        timers=timer_cache,
        now_s=time.time(),
    )

    # When a CONFIG_CHANGED ack matches this worker:
    new_chunk = chunk_controller_config(master_config, controller_id=1)
    refresh_runtime_chunk(gre_runtime, new_chunk)
"""

import logging
import time

from functools import partial

from src.core._gre_action_dispatcher import dispatch_planned_actions
from src.core._worker_writer_sync import (
    build_config_patch_event,
    emit_config_patch,
    make_worker_patch_emitter,
)
from src.orchestration.config_chunking import (
    chunk_signature,
    materialize_chunk_as_flat_config,
)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Lazy GRE import
#
# The GRE lives in src/libraries/_generic_rule_engine.py and has a large
# import footprint. We import it lazily so the worker can keep booting
# even if the GRE is temporarily unavailable (e.g. in a stripped-down
# test environment).
# ---------------------------------------------------------------------

def _load_gre_module():
    try:
        from src.libraries import _generic_rule_engine as gre_module
        return gre_module
    except Exception as exc:
        logger.warning("[gre_worker] GRE module unavailable: %s", exc)
        return None


# ---------------------------------------------------------------------
# Runtime construction
# ---------------------------------------------------------------------

def _empty_stats():
    return {
        "ticks": 0,
        "commands_emitted": 0,
        "actions_dropped": 0,
        "last_tick_s": 0.0,
        "last_error": "",
    }


def _compile_chunk(chunk):
    """
    Compile a chunk into a GRE-ready config.

    Returns (compiled_config, runtime) where runtime is a fresh GRE
    runtime dict produced by the engine. Returns (None, None) when the
    GRE is unavailable.
    """
    gre_module = _load_gre_module()
    if gre_module is None:
        return (None, None)

    flat_config = materialize_chunk_as_flat_config(chunk)

    try:
        compile_cached = getattr(gre_module, "compile_config_cached", None)
        if callable(compile_cached):
            compiled, _cached = compile_cached(flat_config)
        else:
            compile_fn = getattr(gre_module, "compile_config", None)
            if not callable(compile_fn):
                logger.error("[gre_worker] GRE has no compile function")
                return (None, None)
            compiled = compile_fn(flat_config)
    except Exception as exc:
        logger.error("[gre_worker] compile failed: %s", exc, exc_info=True)
        return (None, None)

    try:
        ensure_runtime = getattr(gre_module, "_ensure_runtime", None)
        if callable(ensure_runtime):
            runtime = ensure_runtime(None)
        else:
            runtime = {}
    except Exception:
        runtime = {}

    return (compiled, runtime)


def create_gre_worker_runtime(
    worker_id,
    chunk,
    controller_data,
    emitter=None,
    queue=None,
    put_fn=None,
):
    """
    Build a fresh GRE worker runtime dict.

    Either pass a prebuilt ``emitter`` (typically from
    ``make_worker_patch_emitter``) or pass the raw ``queue`` + ``put_fn``
    pair so this function can build the emitter on its own.
    """
    if emitter is None and queue is not None and callable(put_fn):
        emitter = make_worker_patch_emitter(
            queue=queue,
            put_fn=put_fn,
            controller_id=worker_id,
        )

    compiled_config, gre_runtime = _compile_chunk(chunk)

    return {
        "worker_id": worker_id,
        "compiled_config": compiled_config,
        "runtime": gre_runtime,
        "emitter": emitter,
        "controller_data": dict(controller_data or {}),
        "last_chunk_signature": chunk_signature(chunk),
        "stats": _empty_stats(),
    }


# ---------------------------------------------------------------------
# Runtime refresh
# ---------------------------------------------------------------------

def refresh_runtime_chunk(gre_worker_runtime, new_chunk):
    """
    Recompile the GRE config for a new chunk and reset the GRE runtime.

    Returns True when recompile succeeded, False otherwise. The old
    compiled config is kept intact on failure so the worker can still
    evaluate rules.
    """
    # --- Lieferung 2b: snapshot-freeze (no-op when flag disabled) ---
    try:
        if callable(_sf_wrap) and isinstance(gre_worker_runtime, dict):
            _settings = gre_worker_runtime.get('settings') or {}
            _live = {
                'sensor_values': sensor_values or {},
                'actuator_values': state_values or {},
                'process_states': facts or {},
            }
            _snap = _sf_wrap(_settings, _live)
            if _snap.get('frozen'):
                _d = _snap.get('data') or {}
                sensor_values = _d.get('sensor_values', sensor_values)
                state_values = _d.get('actuator_values', state_values)
                facts = _d.get('process_states', facts)
    except Exception:
        pass

    if not isinstance(gre_worker_runtime, dict):
        return False

    new_signature = chunk_signature(new_chunk)
    if new_signature == gre_worker_runtime.get("last_chunk_signature"):
        logger.debug("[gre_worker] chunk signature unchanged, skip recompile")
        return True

    compiled_config, runtime = _compile_chunk(new_chunk)
    if compiled_config is None:
        return False

    gre_worker_runtime["compiled_config"] = compiled_config
    gre_worker_runtime["runtime"] = runtime
    gre_worker_runtime["last_chunk_signature"] = new_signature
    logger.info(
        "[gre_worker] worker %s chunk refreshed, signature=%s",
        gre_worker_runtime.get("worker_id"),
        new_signature,
    )
    return True


def update_controller_data(gre_worker_runtime, controller_data):
    """
    Update the controller row used for the automatic-mode gate.
    """
    if isinstance(gre_worker_runtime, dict):
        gre_worker_runtime["controller_data"] = dict(controller_data or {})


# ---------------------------------------------------------------------
# Per-tick evaluation
# ---------------------------------------------------------------------

try:
    from src.core._snapshot_freeze import wrap_tick_inputs as _sf_wrap
except Exception:
    _sf_wrap = None


def run_gre_tick(
    gre_worker_runtime,
    sensor_values=None,
    sensor_metadata=None,
    state_values=None,
    timers=None,
    facts=None,
    now_s=None,
    profile_id=None,
    force_dispatch=False,
):
    """
    Evaluate the GRE once and emit any resulting CONFIG_PATCH commands.

    Returns a dict with ``tick_number``, ``actions_planned``,
    ``commands_emitted`` and ``success``.
    """
    # --- Lieferung 2b: snapshot-freeze (no-op when flag disabled) ---
    try:
        if callable(_sf_wrap) and isinstance(gre_worker_runtime, dict):
            _settings = gre_worker_runtime.get('settings') or {}
            _live = {
                'sensor_values': sensor_values or {},
                'actuator_values': state_values or {},
                'process_states': facts or {},
            }
            _snap = _sf_wrap(_settings, _live)
            if _snap.get('frozen'):
                _d = _snap.get('data') or {}
                sensor_values = _d.get('sensor_values', sensor_values)
                state_values = _d.get('actuator_values', state_values)
                facts = _d.get('process_states', facts)
    except Exception:
        pass

    if not isinstance(gre_worker_runtime, dict):
        return {
            "tick_number": 0,
            "actions_planned": 0,
            "commands_emitted": 0,
            "success": False,
        }

    stats = gre_worker_runtime.setdefault("stats", _empty_stats())
    stats["ticks"] = int(stats.get("ticks", 0)) + 1

    compiled_config = gre_worker_runtime.get("compiled_config")
    if compiled_config is None:
        stats["last_error"] = "compiled_config is None"
        return {
            "tick_number": stats["ticks"],
            "actions_planned": 0,
            "commands_emitted": 0,
            "success": False,
        }

    gre_module = _load_gre_module()
    if gre_module is None:
        stats["last_error"] = "gre_module unavailable"
        return {
            "tick_number": stats["ticks"],
            "actions_planned": 0,
            "commands_emitted": 0,
            "success": False,
        }

    create_context = getattr(gre_module, "create_engine_context", None)
    evaluate_fn = getattr(gre_module, "evaluate", None)
    if not callable(create_context) or not callable(evaluate_fn):
        stats["last_error"] = "gre api incomplete"
        return {
            "tick_number": stats["ticks"],
            "actions_planned": 0,
            "commands_emitted": 0,
            "success": False,
        }

    now_value = float(now_s) if now_s is not None else time.time()

    try:
        context = create_context(
            now_s=now_value,
            facts=facts or {},
            sensor_values=sensor_values or {},
            sensor_metadata=sensor_metadata or {},
            state_values=state_values or {},
            timers=timers or {},
            runtime=gre_worker_runtime.get("runtime"),
        )
    except Exception as exc:
        stats["last_error"] = "context_build_error: %s" % exc
        logger.error("[gre_worker] context build failed: %s", exc, exc_info=True)
        return {
            "tick_number": stats["ticks"],
            "actions_planned": 0,
            "commands_emitted": 0,
            "success": False,
        }

    try:
        result = evaluate_fn(compiled_config, context, profile_id=profile_id)
    except Exception as exc:
        stats["last_error"] = "evaluate_error: %s" % exc
        logger.error("[gre_worker] evaluate failed: %s", exc, exc_info=True)
        return {
            "tick_number": stats["ticks"],
            "actions_planned": 0,
            "commands_emitted": 0,
            "success": False,
        }

    if isinstance(result, dict):
        planned_actions = result.get("planned_actions") or ()
        # Persist runtime so edge-detection / debounce / cooldown survive.
        new_runtime = result.get("runtime")
        if new_runtime is not None:
            gre_worker_runtime["runtime"] = new_runtime
    else:
        planned_actions = ()

    actions_count = len(planned_actions) if planned_actions else 0

    if actions_count == 0:
        stats["last_tick_s"] = now_value
        return {
            "tick_number": stats["ticks"],
            "actions_planned": 0,
            "commands_emitted": 0,
            "success": True,
        }

    commands = dispatch_planned_actions(
        planned_actions=planned_actions,
        controller_data=gre_worker_runtime.get("controller_data") or {},
        worker_id=gre_worker_runtime.get("worker_id"),
        force_dispatch=force_dispatch,
    )

    if not commands:
        stats["actions_dropped"] = int(stats.get("actions_dropped", 0)) + actions_count
        stats["last_tick_s"] = now_value
        return {
            "tick_number": stats["ticks"],
            "actions_planned": actions_count,
            "commands_emitted": 0,
            "success": True,
        }

    emitter = gre_worker_runtime.get("emitter")
    ok = False
    if callable(emitter):
        try:
            ok = bool(emitter(commands=commands, cause="gre_action"))
        except TypeError:
            # Some emitters accept positional commands only.
            try:
                ok = bool(emitter(commands))
            except Exception as exc:
                stats["last_error"] = "emitter_error: %s" % exc
                logger.error("[gre_worker] emitter failed: %s", exc, exc_info=True)
                ok = False
        except Exception as exc:
            stats["last_error"] = "emitter_error: %s" % exc
            logger.error("[gre_worker] emitter failed: %s", exc, exc_info=True)
            ok = False
    else:
        logger.debug("[gre_worker] no emitter bound, dropping %d commands", len(commands))

    if ok:
        stats["commands_emitted"] = int(stats.get("commands_emitted", 0)) + len(commands)

    stats["last_tick_s"] = now_value

    return {
        "tick_number": stats["ticks"],
        "actions_planned": actions_count,
        "commands_emitted": len(commands) if ok else 0,
        "success": bool(ok),
    }


# ---------------------------------------------------------------------
# Direct emit (bypass evaluate) — for bootstrap / recovery
# ---------------------------------------------------------------------

def emit_direct_patch(gre_worker_runtime, commands, cause="bootstrap"):
    """
    Emit a hand-built command list through the runtime's emitter.

    Bypasses the mode gate and the GRE evaluation pipeline. Use this
    only for bootstrap or recovery paths where the worker needs to
    reconcile the master with its own view.
    """
    # --- Lieferung 2b: snapshot-freeze (no-op when flag disabled) ---
    try:
        if callable(_sf_wrap) and isinstance(gre_worker_runtime, dict):
            _settings = gre_worker_runtime.get('settings') or {}
            _live = {
                'sensor_values': sensor_values or {},
                'actuator_values': state_values or {},
                'process_states': facts or {},
            }
            _snap = _sf_wrap(_settings, _live)
            if _snap.get('frozen'):
                _d = _snap.get('data') or {}
                sensor_values = _d.get('sensor_values', sensor_values)
                state_values = _d.get('actuator_values', state_values)
                facts = _d.get('process_states', facts)
    except Exception:
        pass

    if not isinstance(gre_worker_runtime, dict):
        return False

    emitter = gre_worker_runtime.get("emitter")
    if not callable(emitter):
        return False

    try:
        return bool(emitter(commands=tuple(commands), cause=cause))
    except TypeError:
        try:
            return bool(emitter(tuple(commands)))
        except Exception as exc:
            logger.error("[gre_worker] direct emit failed: %s", exc, exc_info=True)
            return False
    except Exception as exc:
        logger.error("[gre_worker] direct emit failed: %s", exc, exc_info=True)
        return False
