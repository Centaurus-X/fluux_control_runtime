# -*- coding: utf-8 -*-

# src/orchestration/shutdown.py

import logging
import os
import signal
import threading
import time

from src.orchestration.thread_factory import join_threads


DEFAULT_STOP_PAYLOADS = (
    {"_type": "__STOP__", "_sentinel": True},
    {"event_type": "__STOP__", "_sentinel": True},
    None,
)


def _logger_or_default(logger=None):
    if logger is not None:
        return logger
    return logging.getLogger(__name__)


def nudge_queues_for_shutdown(queues, repeats=2, put_timeout_s=0.1, stop_payloads=None):
    """
    Nudge queue consumers so blocking get-loops can wake up quickly.
    """
    payloads = stop_payloads if stop_payloads is not None else DEFAULT_STOP_PAYLOADS

    def put_payload(queue_obj, payload):
        if queue_obj is None:
            return

        try:
            queue_obj.put_nowait(payload)
            return
        except Exception:
            pass

        try:
            queue_obj.put(payload, timeout=put_timeout_s)
        except Exception:
            pass

    for _ in range(int(repeats)):
        for queue_obj in tuple(queues or ()):
            for payload in payloads:
                put_payload(queue_obj, payload)


def shutdown_executor(main_executor=None, automation_shutdown_fn=None, wait=True, logger=None):
    """
    Shut down the main executor and the automation executor if available.
    """
    logger_obj = _logger_or_default(logger)

    if main_executor is not None:
        try:
            main_executor.shutdown(wait=bool(wait))
            logger_obj.info("[shutdown] Main executor stopped")
        except Exception as exc:
            logger_obj.warning("[shutdown] Main executor shutdown failed: %s", exc)

    if callable(automation_shutdown_fn):
        try:
            automation_shutdown_fn(wait=bool(wait))
            logger_obj.info("[shutdown] Automation executor stopped")
        except Exception as exc:
            logger_obj.warning("[shutdown] Automation executor shutdown failed: %s", exc)


def join_extra_threads(extra_threads, timeout_s=5.0, logger=None):
    """
    Join additional thread objects that are not part of the managed thread entries.
    """
    logger_obj = _logger_or_default(logger)
    alive_after_join = []

    for thread_obj in tuple(extra_threads or ()):
        if thread_obj is None:
            continue

        try:
            if not thread_obj.is_alive():
                continue
        except Exception:
            continue

        try:
            thread_obj.join(timeout=float(timeout_s))
        except Exception as exc:
            logger_obj.warning("[shutdown] Extra thread join failed: %s", exc)
            alive_after_join.append(thread_obj)
            continue

        try:
            if thread_obj.is_alive():
                alive_after_join.append(thread_obj)
        except Exception:
            alive_after_join.append(thread_obj)

    return tuple(alive_after_join)


def enforce_sigkill_if_threads_stuck(timeout=20.0, logger=None):
    """
    Force SIGKILL when non-daemon threads are still alive after cooperative waiting.
    """
    logger_obj = _logger_or_default(logger)
    end_ts = time.time() + float(timeout)

    def alive_non_daemon_threads():
        current = threading.current_thread()
        return [
            thread_obj
            for thread_obj in threading.enumerate()
            if thread_obj.is_alive() and not thread_obj.daemon and thread_obj is not current
        ]

    while time.time() < end_ts:
        alive_threads = alive_non_daemon_threads()
        if not alive_threads:
            return False

        for thread_obj in alive_threads:
            try:
                thread_obj.join(timeout=0.2)
            except Exception:
                pass

    alive_threads = alive_non_daemon_threads()
    if not alive_threads:
        return False

    logger_obj.error(
        "[shutdown] Force-killing process because threads are still alive: %s",
        [thread_obj.name for thread_obj in alive_threads],
    )

    try:
        for handler in logging.getLogger().handlers:
            try:
                handler.flush()
            except Exception:
                pass
    except Exception:
        pass

    os.kill(os.getpid(), signal.SIGKILL)
    return True


def shutdown_runtime(
    shutdown_event,
    thread_entries,
    queue_tools,
    queues_to_nudge,
    logger=None,
    join_timeout_s=5.0,
    nudge_enabled=True,
    nudge_repeats=2,
    nudge_put_timeout_s=0.1,
    nudge_stop_payloads=None,
    main_executor=None,
    automation_shutdown_fn=None,
    executor_shutdown_wait=True,
    system_cleanup_fn=None,
    enforce_sigkill_timeout_s=10.0,
    extra_threads=None,
):
    """
    Run the coordinated shutdown sequence.
    """
    logger_obj = _logger_or_default(logger)

    try:
        shutdown_event.set()
    except Exception:
        pass

    if nudge_enabled:
        queues = queue_tools["queue_list"](*tuple(queues_to_nudge or ()))
        nudge_queues_for_shutdown(
            queues=queues,
            repeats=nudge_repeats,
            put_timeout_s=nudge_put_timeout_s,
            stop_payloads=nudge_stop_payloads,
        )

    alive_entries = join_threads(
        thread_entries=thread_entries,
        timeout_s=join_timeout_s,
        logger=logger_obj,
    )

    alive_extra_threads = join_extra_threads(
        extra_threads=extra_threads,
        timeout_s=join_timeout_s,
        logger=logger_obj,
    )

    shutdown_executor(
        main_executor=main_executor,
        automation_shutdown_fn=automation_shutdown_fn,
        wait=executor_shutdown_wait,
        logger=logger_obj,
    )

    if callable(system_cleanup_fn):
        try:
            system_cleanup_fn()
        except Exception as exc:
            logger_obj.debug("[shutdown] System cleanup failed: %s", exc)

    try:
        enforce_sigkill_if_threads_stuck(
            timeout=enforce_sigkill_timeout_s,
            logger=logger_obj,
        )
    except Exception as exc:
        logger_obj.debug("[shutdown] Force-kill check failed: %s", exc)

    return {
        "alive_thread_entries": alive_entries,
        "alive_extra_threads": alive_extra_threads,
    }
