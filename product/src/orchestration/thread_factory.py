# -*- coding: utf-8 -*-

# src/orchestration/thread_factory.py

import logging
import signal
import threading
import time

from functools import partial

from src.orchestration.thread_adapters import run_target


def _logger_or_default(logger=None):
    if logger is not None:
        return logger
    return logging.getLogger(__name__)


def _ensure_entry_lock(entry):
    lock = entry.get("state_lock")
    if lock is None:
        lock = threading.RLock()
        entry["state_lock"] = lock
    return lock


def _update_entry_state(entry, **updates):
    lock = _ensure_entry_lock(entry)

    with lock:
        for key, value in updates.items():
            entry[key] = value


def _clone_sequence_as_tuple(value):
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, set):
        return tuple(sorted(value))
    return value


def _clone_ownership_metadata(ownership):
    if not isinstance(ownership, dict):
        return {}

    cloned = {}

    for key, value in ownership.items():
        if isinstance(value, dict):
            cloned[key] = dict(value)
            continue

        cloned[key] = _clone_sequence_as_tuple(value)

    return cloned


def clone_thread_spec(spec):
    """
    Clone one thread spec without deep-copying runtime resources.

    The thread spec may contain queues, locks and shared resource registries inside
    kwargs. Those objects must keep their original identity and ownership. A deep
    copy would try to pickle thread locks and break determinism.
    """
    if not isinstance(spec, dict):
        return {}

    cloned = {}

    for key, value in spec.items():
        if key == "kwargs":
            if isinstance(value, dict):
                cloned[key] = dict(value)
            else:
                cloned[key] = {}
            continue

        if key == "ownership":
            cloned[key] = _clone_ownership_metadata(value)
            continue

        if isinstance(value, list):
            cloned[key] = list(value)
            continue

        if isinstance(value, tuple):
            cloned[key] = tuple(value)
            continue

        if isinstance(value, set):
            cloned[key] = set(value)
            continue

        if isinstance(value, dict):
            cloned[key] = dict(value)
            continue

        cloned[key] = value

    return cloned


def build_thread_ctx(spec, runtime_state=None, config_store=None):
    """
    Build the per-thread ownership context passed into the thread wrapper.
    """
    runtime_state = runtime_state or {}
    config_store = config_store or {}
    ownership = _clone_ownership_metadata(spec.get("ownership", {}))

    return {
        "thread_name": spec.get("name"),
        "component_name": spec.get("component_name", spec.get("name")),
        "shutdown_event": runtime_state.get("shutdown_event"),
        "node_id": runtime_state.get("node_id"),
        "ownership": ownership,
        "resource_names": tuple(ownership.get("resource_names", ())),
        "read_resources": tuple(ownership.get("read_resources", ())),
        "write_resources": tuple(ownership.get("write_resources", ())),
        "ingress_queues": tuple(ownership.get("ingress_queues", ())),
        "egress_queues": tuple(ownership.get("egress_queues", ())),
        "config_revision": config_store.get("revision"),
        "created_ts": time.time(),
    }


def thread_entry_runner(entry, runtime_state=None, config_store=None, logger=None):
    """
    Wrap one thread target with lifecycle tracking and explicit thread context.
    """
    logger_obj = _logger_or_default(logger)
    spec = entry.get("spec", {})
    target = spec.get("target")

    if not callable(target):
        raise ValueError("thread target is not callable for %s" % spec.get("name"))

    thread_ctx = build_thread_ctx(
        spec=spec,
        runtime_state=runtime_state,
        config_store=config_store,
    )

    kwargs = dict(spec.get("kwargs", {}))

    shutdown_event = runtime_state.get("shutdown_event") if isinstance(runtime_state, dict) else None
    if shutdown_event is not None and "shutdown_event" not in kwargs:
        kwargs["shutdown_event"] = shutdown_event

    kwargs.setdefault("thread_ctx", thread_ctx)
    kwargs.setdefault("runtime_config_store", config_store)

    _update_entry_state(
        entry,
        state="running",
        thread_ctx=thread_ctx,
        started_ts=time.time(),
    )

    try:
        result = run_target(
            target=target,
            target_kind=spec.get("target_kind", "auto"),
            logger=logger_obj,
            **kwargs,
        )
        _update_entry_state(
            entry,
            state="stopped",
            stopped_ts=time.time(),
            result=result,
        )
        return result
    except Exception as exc:
        _update_entry_state(
            entry,
            state="failed",
            stopped_ts=time.time(),
            error=str(exc),
        )
        logger_obj.error(
            "[thread_factory] Thread '%s' crashed: %s",
            spec.get("name"),
            exc,
            exc_info=True,
        )
        raise


def create_thread(entry, runtime_state=None, config_store=None, logger=None, default_daemon=False):
    """
    Create one Thread instance from one normalized entry.
    """
    spec = entry.get("spec", {})
    daemon = bool(spec.get("daemon", default_daemon))

    runner = partial(
        thread_entry_runner,
        entry,
        runtime_state=runtime_state,
        config_store=config_store,
        logger=logger,
    )

    thread_obj = threading.Thread(
        target=runner,
        name=spec.get("name"),
        daemon=daemon,
    )

    entry["thread"] = thread_obj
    entry["daemon"] = daemon
    return thread_obj


def create_threads_from_specs(
    thread_specs,
    runtime_state=None,
    config_store=None,
    logger=None,
    default_daemon=False,
):
    """
    Materialize thread specs into thread entries with lifecycle state.
    """
    entries = []

    for spec in tuple(thread_specs or ()):
        if not isinstance(spec, dict):
            continue

        if not bool(spec.get("enabled", True)):
            continue

        entry = {
            "spec": clone_thread_spec(spec),
            "state": "created",
            "created_ts": time.time(),
            "started_ts": None,
            "stopped_ts": None,
            "result": None,
            "error": None,
            "thread_ctx": None,
            "state_lock": threading.RLock(),
        }

        create_thread(
            entry=entry,
            runtime_state=runtime_state,
            config_store=config_store,
            logger=logger,
            default_daemon=default_daemon,
        )

        entries.append(entry)

    return entries


def install_signal_handlers(shutdown_event, logger=None):
    """
    Install SIGINT and SIGTERM handlers that set the shared shutdown event.
    """
    logger_obj = _logger_or_default(logger)

    def handle_signal(signum, frame):
        logger_obj.info(
            "[thread_factory] Signal %s received - setting shutdown event",
            signum,
        )
        try:
            shutdown_event.set()
        except Exception:
            pass

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


def start_threads(thread_entries, logger=None, sleep_fn=None):
    """
    Start thread entries deterministically in the provided order.
    """
    logger_obj = _logger_or_default(logger)

    if sleep_fn is None:
        sleep_fn = time.sleep

    for entry in thread_entries:
        thread_obj = entry.get("thread")
        spec = entry.get("spec", {})
        if thread_obj is None:
            continue

        thread_obj.start()
        _update_entry_state(entry, state="started")
        logger_obj.info("[thread_factory] Started thread: %s", thread_obj.name)

        start_delay_s = spec.get("start_delay_s", 0.0)
        try:
            start_delay_s = float(start_delay_s)
        except Exception:
            start_delay_s = 0.0

        if start_delay_s > 0.0:
            sleep_fn(start_delay_s)


def join_threads(thread_entries, timeout_s=5.0, logger=None):
    """
    Join thread entries deterministically and report alive threads afterwards.
    """
    logger_obj = _logger_or_default(logger)
    alive_after_join = []

    for entry in thread_entries:
        thread_obj = entry.get("thread")

        if thread_obj is None:
            continue

        if not thread_obj.is_alive():
            logger_obj.info("[thread_factory] Thread already stopped: %s", thread_obj.name)
            continue

        logger_obj.info("[thread_factory] Joining thread: %s", thread_obj.name)

        try:
            thread_obj.join(timeout=float(timeout_s))
        except Exception as exc:
            logger_obj.error(
                "[thread_factory] Join failed for %s: %s",
                thread_obj.name,
                exc,
                exc_info=True,
            )
            alive_after_join.append(entry)
            continue

        if thread_obj.is_alive():
            logger_obj.warning(
                "[thread_factory] Join timeout reached for %s",
                thread_obj.name,
            )
            alive_after_join.append(entry)
        else:
            logger_obj.info("[thread_factory] Thread stopped: %s", thread_obj.name)

    return alive_after_join
