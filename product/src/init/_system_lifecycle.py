# -*- coding: utf-8 -*-

"""Lifecycle helpers built on top of the split startup modules."""

import asyncio
import importlib.metadata
import logging
import os
import queue as queue_module
import sys
import threading
from datetime import datetime
from functools import partial

from ._system_logging import (
    LOGGING_CONFIG,
    LOGGING_LOCK,
    LOGGING_RUNTIME_STATE,
    THREAD_LOGGING_SELECTION,
    _log_startup_cleanup_result,
    apply_thread_selection_config,
    configure_logging_for_production,
    set_allowed_modules,
    set_module_level,
    setup_logging,
)
from ._system_paths import cleanup_logs_on_startup, cleanup_old_logs, resolve_logging_target_paths
from ._system_state import get_active_threads_safe, register_system_state

logger = logging.getLogger(__name__)

LIFECYCLE_STATE = {
    'start_time': None,
    'cleanup_done': False,
}



def run_in_thread(target, name=None, daemon=True, *args, **kwargs):
    """Start a callable in a new thread and return the thread object."""
    thread_obj = threading.Thread(
        target=target,
        name=name,
        args=args,
        kwargs=kwargs,
        daemon=bool(daemon),
    )
    thread_obj.start()
    logger.info("Thread started: %s", thread_obj.name)
    return thread_obj



def run_coroutine(coro, loop=None):
    """Run or schedule a coroutine against the selected event loop."""
    selected_loop = loop

    if selected_loop is None:
        try:
            selected_loop = asyncio.get_event_loop()
        except RuntimeError:
            selected_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(selected_loop)

    if selected_loop.is_running():
        return asyncio.ensure_future(coro, loop=selected_loop)

    return selected_loop.run_until_complete(coro)



def create_queue_broker_thread(queue, name, handler_func, stop_event=None):
    """Create a queue broker thread with explicit shutdown handling."""

    def broker_loop(local_queue, local_name, local_handler, local_stop_event):
        logger.info("Queue broker started: %s", local_name)

        while True:
            try:
                item = local_queue.get(timeout=0.5)
            except queue_module.Empty:
                if local_stop_event is not None and local_stop_event.is_set():
                    logger.info("Queue broker stop event received: %s", local_name)
                    break
                continue
            except Exception as exc:
                logger.error("Queue broker read failure: name=%s error=%s", local_name, exc, exc_info=True)
                continue

            if item is None:
                logger.info("Queue broker shutdown sentinel received: %s", local_name)
                break

            try:
                local_handler(item)
            except Exception as exc:
                logger.error("Queue broker handler failure: name=%s error=%s", local_name, exc, exc_info=True)

        logger.info("Queue broker stopped: %s", local_name)

    return threading.Thread(
        target=partial(broker_loop, queue, name, handler_func, stop_event),
        name=name,
        daemon=True,
    )



def log_imported_modules(limit=20):
    """Log a deterministic subset of imported modules."""
    modules = sorted([name for name in list(sys.modules.keys()) if not name.startswith('_')])
    logger.debug("Imported modules (%d total)", len(modules))
    for module_name in modules[:int(limit)]:
        logger.debug("  %s", module_name)
    if len(modules) > int(limit):
        logger.debug("  ... and %d more", len(modules) - int(limit))



def log_python_pip_versions():
    """Log Python and selected package versions when available."""
    logger.info('=' * 60)
    logger.info('SYSTEM VERSIONS')
    logger.info('=' * 60)
    logger.info('Python: %s', sys.version)

    packages = (
        'numpy',
        'pandas',
        'asyncio',
        'websockets',
        'requests',
        'pytest',
        'colorama',
        'setuptools',
    )

    for package_name in packages:
        try:
            version = importlib.metadata.version(package_name)
            logger.info('Package: %s==%s', package_name, version)
        except Exception:
            logger.info('Package: %s (not installed)', package_name)

    logger.info('=' * 60)



def check_system_processes():
    """Placeholder for external service checks."""
    logger.info('System process checks completed')



def initialize_system():
    """Initialize logging, cleanup policy and startup diagnostics."""
    LIFECYCLE_STATE['start_time'] = datetime.now()
    log_profile = os.environ.get('LOG_PROFILE', 'default')

    try:
        if log_profile == 'debug_controller':
            set_allowed_modules('_controller_thread', '__main__')
            set_module_level('_controller_thread', logging.DEBUG)
        elif log_profile == 'production':
            configure_logging_for_production()
        elif log_profile == 'silent':
            with LOGGING_LOCK:
                LOGGING_CONFIG['console_level'] = logging.ERROR

        apply_thread_selection_config(THREAD_LOGGING_SELECTION)

        log_dir, _, log_path = resolve_logging_target_paths(logging_config=LOGGING_CONFIG)
        startup_cleanup_result = cleanup_logs_on_startup(
            logging_config=LOGGING_CONFIG,
            runtime_state=LOGGING_RUNTIME_STATE,
            log_dir=log_dir,
        )

        setup_logging(log_file=log_path)
        _log_startup_cleanup_result(startup_cleanup_result)

        retention_cfg = LOGGING_CONFIG.get('retention_cleanup', {}) or {}
        if bool(retention_cfg.get('enabled', False)) and not startup_cleanup_result.get('performed', False):
            cleanup_old_logs(
                base_pattern=retention_cfg.get('patterns', ['*.log', '*.log.*', '*.gz', '*.zip']),
                keep_days=int(retention_cfg.get('keep_days', 30)),
                log_dir=log_dir,
                logging_config=LOGGING_CONFIG,
                recursive=bool(retention_cfg.get('recursive', False)),
            )

        check_system_processes()
        register_system_state(logger_instance=logger)
        log_python_pip_versions()
        logger.info('System initialization completed')
        LIFECYCLE_STATE['cleanup_done'] = False

    except Exception as exc:
        logging.basicConfig(level=logging.DEBUG)
        logger.error('System initialization failed: %s', exc, exc_info=True)
        raise



def cleanup_system():
    """Perform shutdown cleanup and log remaining thread state."""
    if LIFECYCLE_STATE.get('cleanup_done'):
        return False

    LIFECYCLE_STATE['cleanup_done'] = True
    logger.info('Starting system cleanup')

    start_time = LIFECYCLE_STATE.get('start_time')
    if start_time is not None:
        logger.info('Total runtime: %s', datetime.now() - start_time)

    try:
        active_threads = get_active_threads_safe(logger_instance=logger)
        if len(active_threads) > 1:
            logger.info('Active threads during cleanup: %d', len(active_threads))
            for thread_obj in active_threads:
                logger.info(
                    'Thread status: name=%s daemon=%s alive=%s',
                    thread_obj.name,
                    thread_obj.daemon,
                    thread_obj.is_alive(),
                )
    except Exception as exc:
        logger.warning('Failed to inspect active threads during cleanup: %s', exc)

    thread_executor = LOGGING_RUNTIME_STATE.get('thread_executor')
    if thread_executor is not None:
        try:
            logger.info('Shutting down logging thread executor')
            thread_executor.shutdown(wait=True, cancel_futures=True)
        except Exception as exc:
            logger.error('Failed to shut down logging thread executor: %s', exc, exc_info=True)

    active_threads = get_active_threads_safe(logger_instance=logger)
    main_thread = threading.main_thread()
    for thread_obj in active_threads:
        if thread_obj is main_thread:
            continue
        if not thread_obj.is_alive():
            continue
        if thread_obj.daemon:
            continue
        if 'Queue_Broker' not in thread_obj.name:
            continue

        try:
            logger.info('Joining queue broker thread: %s', thread_obj.name)
            thread_obj.join(timeout=5.0)
        except Exception as exc:
            logger.error('Failed to join thread %s: %s', thread_obj.name, exc, exc_info=True)

    current_log_file = LOGGING_RUNTIME_STATE.get('current_log_file')
    if current_log_file:
        logger.info('Log file stored at: %s', current_log_file)

    logger.info('System cleanup completed')
    return True
