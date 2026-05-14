# -*- coding: utf-8 -*-

"""State, process and free-threading helpers for startup diagnostics."""

import logging
import os
import platform
import sys
import threading
from datetime import datetime
from queue import Queue

logger = logging.getLogger(__name__)


def create_async_queue():
    """Create a thread-safe queue for async-compatible handoff."""
    return Queue()



def get_active_threads_safe(logger_instance=None):
    """Return active threads and fall back to an empty list on failure."""
    logger_obj = logger_instance or logger
    try:
        return list(threading.enumerate())
    except Exception as exc:
        logger_obj.warning('Failed to enumerate active threads: %s', exc)
        return []



def is_main_process():
    """Return True when running in the main process context."""
    try:
        import multiprocessing
        return multiprocessing.current_process().name == 'MainProcess'
    except Exception:
        return True



def get_process_info():
    """Return basic process information for diagnostics."""
    return {
        'pid': os.getpid(),
        'ppid': os.getppid(),
        'cwd': os.getcwd(),
        'executable': sys.executable,
        'argv': list(sys.argv),
    }



def get_memory_usage():
    """Return current process memory usage in MiB when available."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return float(process.memory_info().rss) / 1024.0 / 1024.0
    except Exception:
        return 0.0



def get_thread_info(logger_instance=None):
    """Return a stable snapshot of active thread metadata."""
    thread_entries = []
    for thread in get_active_threads_safe(logger_instance=logger_instance):
        thread_entries.append(
            {
                'name': thread.name,
                'ident': thread.ident,
                'daemon': bool(thread.daemon),
                'alive': bool(thread.is_alive()),
            }
        )
    return thread_entries



def register_system_state(logger_instance=None):
    """Log and return a startup snapshot of the current runtime state."""
    logger_obj = logger_instance or logger

    try:
        state = {
            'timestamp': datetime.now().isoformat(),
            'pid': os.getpid(),
            'ppid': os.getppid(),
            'cwd': os.getcwd(),
            'argv': list(sys.argv),
            'active_threads': len(get_active_threads_safe(logger_instance=logger_obj)),
            'threads': get_thread_info(logger_instance=logger_obj),
            'memory_usage_mb': get_memory_usage(),
        }

        logger_obj.info('=' * 60)
        logger_obj.info('SYSTEM STATUS')
        logger_obj.info('=' * 60)
        logger_obj.info('PID: %s', state['pid'])
        logger_obj.info('Working directory: %s', state['cwd'])
        logger_obj.info('Active threads: %s', state['active_threads'])
        logger_obj.info('Memory usage: %.2f MiB', state['memory_usage_mb'])
        logger_obj.info('System state registered')
        return state

    except Exception as exc:
        logger_obj.error('Failed to register system state: %s', exc, exc_info=True)
        return {}



def get_system_info():
    """Collect a system information snapshot for diagnostics."""
    info = {
        'python_version': sys.version,
        'platform': platform.platform(),
        'processor': platform.processor(),
        'cpu_count': os.cpu_count(),
        'memory_total': None,
        'memory_available': None,
        'disk_usage': None,
        'active_threads': threading.active_count(),
        'process_id': os.getpid(),
    }

    try:
        import psutil
        info['cpu_count'] = psutil.cpu_count()
        info['memory_total'] = psutil.virtual_memory().total
        info['memory_available'] = psutil.virtual_memory().available
        info['disk_usage'] = psutil.disk_usage('/').percent
    except Exception:
        pass

    return info



def log_system_info(logger_instance=None):
    """Log the current system information snapshot."""
    logger_obj = logger_instance or logger

    try:
        info = get_system_info()
        logger_obj.info('SYSTEM INFORMATION')
        logger_obj.info('Python version: %s', info.get('python_version'))
        logger_obj.info('Platform: %s', info.get('platform'))
        logger_obj.info('Processor: %s', info.get('processor'))
        logger_obj.info('CPU count: %s', info.get('cpu_count'))
        if info.get('memory_total') is not None:
            logger_obj.info('Memory total: %.2f MiB', float(info['memory_total']) / 1024.0 / 1024.0)
        if info.get('memory_available') is not None:
            logger_obj.info('Memory available: %.2f MiB', float(info['memory_available']) / 1024.0 / 1024.0)
        if info.get('disk_usage') is not None:
            logger_obj.info('Disk usage: %s%%', info.get('disk_usage'))
        logger_obj.info('Active threads: %s', info.get('active_threads'))
        logger_obj.info('Process ID: %s', info.get('process_id'))
    except Exception as exc:
        logger_obj.error('Failed to log system information: %s', exc, exc_info=True)



def is_free_threading_enabled(logger_instance=None):
    """Heuristically determine whether Python free-threading is active."""
    logger_obj = logger_instance or logger

    try:
        free_threading_fn = getattr(sys, 'is_free_threading', None)
        if callable(free_threading_fn):
            return bool(free_threading_fn())

        xoptions = getattr(sys, '_xoptions', {})
        if isinstance(xoptions, dict) and 'free-threading' in xoptions:
            return True

        gil_disabled = getattr(sys, '_is_gil_enabled', None)
        if callable(gil_disabled):
            return not bool(gil_disabled())

    except Exception as exc:
        logger_obj.warning('Failed to detect free-threading status: %s', exc)

    return True



def assert_free_threading_or_exit(logger_instance=None, hard=False):
    """Assert free-threading support and exit when hard enforcement is requested."""
    logger_obj = logger_instance or logger

    if is_free_threading_enabled(logger_instance=logger_obj):
        logger_obj.info('Free-threading support is active')
        return True

    message = 'Free-threading support does not appear to be active'
    if hard:
        logger_obj.critical(message)
        logger_obj.critical('Exiting because hard free-threading enforcement is enabled')
        raise SystemExit(1)

    logger_obj.warning(message)
    return False
