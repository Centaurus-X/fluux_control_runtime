# -*- coding: utf-8 -*-

"""Deterministic logging helpers without custom classes or decorators."""

import copy
import gzip
import logging
import os
import shutil
import sys
import threading
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

from ._system_paths import (
    DEFAULT_SYSTEM_LOG_FILE,
    PROJECT_ROOT,
    cleanup_logs_on_startup,
    cleanup_old_logs,
    resolve_logging_target_paths,
)

logger = logging.getLogger(__name__)
custom_loggers = {}
LOGGING_LOCK = threading.RLock()

try:
    import colorama
    from colorama import Back, Fore, Style
    colorama.init(autoreset=True)
    _COLOR_ENABLED = True
except Exception:
    colorama = None
    Back = None
    Fore = None
    Style = None
    _COLOR_ENABLED = False


LOGGING_CONFIG = {
    'enabled': True,
    'console_level': logging.INFO,
    'file_level': logging.DEBUG,
    'paths': {
        'log_dir': os.path.join('logs', 'system_logs'),
        'log_file': DEFAULT_SYSTEM_LOG_FILE,
    },
    'allowed_modules': None,
    'module_levels': {},
    'console_only_modules': set(),
    'disabled_modules': set(),
    'allowed_threads': None,
    'thread_levels': {},
    'disabled_threads': set(),
    'startup_cleanup': {
        'enabled': True,
        'patterns': ['*.log', '*.log.*', '*.gz', '*.zip'],
        'recursive': False,
    },
    'retention_cleanup': {
        'enabled': False,
        'keep_days': 30,
        'patterns': ['*.log', '*.log.*', '*.gz', '*.zip'],
        'recursive': False,
    },
    'rotation_config': {
        'type': 'size',
        'max_bytes': 1 * 1024 * 1024,
        'when': 'midnight',
        'interval': 1,
        'backup_count': 5,
        'compress': True,
        'utc': False,
    },
}

THREAD_LOGGING_SELECTION = {}

LOGGING_RUNTIME_STATE = {
    'current_log_file': None,
    'log_handlers': {},
    'thread_executor': None,
}

_LEVEL_COLORS = {
    'DEBUG': (Fore.CYAN + Style.DIM) if _COLOR_ENABLED else '',
    'INFO': (Fore.GREEN + Style.BRIGHT) if _COLOR_ENABLED else '',
    'WARNING': (Fore.YELLOW + Style.BRIGHT) if _COLOR_ENABLED else '',
    'ERROR': (Fore.RED + Style.BRIGHT) if _COLOR_ENABLED else '',
    'CRITICAL': (Fore.WHITE + Back.RED + Style.BRIGHT) if _COLOR_ENABLED else '',
}

_LEVEL_EMOJIS = {
    'DEBUG': 'DBG',
    'INFO': 'INF',
    'WARNING': 'WRN',
    'ERROR': 'ERR',
    'CRITICAL': 'CRT',
}

_RESET = Style.RESET_ALL if _COLOR_ENABLED else ''



def _cfg_bool(value, default):
    if value is None:
        return bool(default)

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ('1', 'true', 'yes', 'on', 'enabled'):
            return True
        if normalized in ('0', 'false', 'no', 'off', 'disabled'):
            return False

    return bool(default)



def _cfg_int(value, default):
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)



def _resolve_log_level(value, default=logging.INFO):
    """Resolve a logging level from int or string input."""
    if isinstance(value, int):
        return value

    if isinstance(value, str):
        level = getattr(logging, value.strip().upper(), None)
        if isinstance(level, int):
            return level

    return default



def _config_dict_get(mapping, dotted_key, default=None):
    """Read a nested dotted-path value from a mapping safely."""
    current = mapping

    if not isinstance(dotted_key, str) or not dotted_key:
        return default

    for key in dotted_key.split('.'):
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]

    return current



def _normalize_rotation_config(rotation_cfg):
    normalized = {}
    raw_cfg = rotation_cfg if isinstance(rotation_cfg, dict) else {}

    enabled = _cfg_bool(raw_cfg.get('enabled', True), True)

    rotation_type = raw_cfg.get('type')
    if isinstance(rotation_type, str) and rotation_type.strip():
        normalized['type'] = rotation_type.strip().lower()
    elif enabled:
        if raw_cfg.get('when') is not None or raw_cfg.get('utc') is not None:
            normalized['type'] = 'time'
        elif raw_cfg.get('max_bytes') is not None or raw_cfg.get('maxBytes') is not None:
            normalized['type'] = 'size'
        else:
            normalized['type'] = 'time'
    else:
        normalized['type'] = 'none'

    normalized['max_bytes'] = _cfg_int(raw_cfg.get('max_bytes', raw_cfg.get('maxBytes', 1 * 1024 * 1024)), 1 * 1024 * 1024)
    normalized['backup_count'] = _cfg_int(raw_cfg.get('backup_count', raw_cfg.get('backupCount', 5)), 5)
    normalized['compress'] = _cfg_bool(raw_cfg.get('compress', True), True)
    normalized['interval'] = _cfg_int(raw_cfg.get('interval', 1), 1)
    normalized['utc'] = _cfg_bool(raw_cfg.get('utc', False), False)
    normalized['when'] = str(raw_cfg.get('when', 'midnight') or 'midnight')
    return normalized



def _copy_set(value):
    if value is None:
        return None
    return set(value)



def _record_matches_module(record_name, candidate):
    if not candidate:
        return False
    if record_name == candidate:
        return True
    if record_name.startswith(candidate + '.'):
        return True
    if candidate in record_name:
        return True
    return False



def _resolve_module_filter_match(record_name, candidates):
    if candidates is None:
        return True

    for candidate in candidates:
        if _record_matches_module(record_name, candidate):
            return True

    return False



def _shorten_logger_name(logger_name, max_length=42):
    name_text = str(logger_name or __name__)
    if len(name_text) <= max_length:
        return name_text

    parts = [part for part in name_text.split('.') if part]
    if len(parts) >= 2:
        return '%s...%s' % (parts[0], parts[-1])

    return name_text[:max_length - 3] + '...'



def _module_color(logger_name):
    if not _COLOR_ENABLED:
        return ''

    name_text = str(logger_name or '').lower()
    if 'thread_management' in name_text:
        return Fore.YELLOW + Style.BRIGHT
    if 'controller_thread' in name_text or '_controller' in name_text:
        return Fore.BLUE + Style.BRIGHT
    if 'automation' in name_text:
        return Fore.CYAN + Style.BRIGHT
    if 'connection' in name_text or 'adapter' in name_text:
        return Fore.GREEN + Style.BRIGHT
    if 'event_router' in name_text or 'sync_event' in name_text:
        return Fore.WHITE + Style.BRIGHT
    if 'process_state' in name_text:
        return Fore.RED + Style.NORMAL
    if 'state_event' in name_text:
        return Fore.MAGENTA + Style.BRIGHT
    if 'worker' in name_text or 'datastore' in name_text:
        return Fore.GREEN + Style.NORMAL
    if 'runtime' in name_text or 'system' in name_text:
        return Fore.CYAN + Style.NORMAL
    return Fore.MAGENTA + Style.NORMAL


def _thread_color(thread_name):
    """Farbzuordnung basierend auf Thread-Name."""
    if not _COLOR_ENABLED:
        return ''

    name_text = str(thread_name or '').lower()
    if name_text.startswith('c') and 'sensor' in name_text:
        return Fore.BLUE + Style.BRIGHT
    if name_text.startswith('c') and 'event' in name_text:
        return Fore.BLUE + Style.NORMAL
    if 'tm-' in name_text or 'thread_management' in name_text:
        return Fore.YELLOW + Style.BRIGHT
    if 'conn-' in name_text:
        return Fore.GREEN + Style.BRIGHT
    if 'controller-' in name_text:
        return Fore.BLUE + Style.BRIGHT
    if 'pool' in name_text or 'worker' in name_text:
        return Fore.CYAN + Style.NORMAL
    if 'event_broker' in name_text or 'broker' in name_text:
        return Fore.WHITE + Style.NORMAL
    return ''



def _colorize(text, color_code):
    if not _COLOR_ENABLED or not color_code:
        return text
    return '%s%s%s' % (color_code, text, _RESET)



def _apply_record_attributes(record):
    level_name = str(record.levelname or 'INFO').upper()
    short_name = _shorten_logger_name(record.name)
    emoji = _LEVEL_EMOJIS.get(level_name, level_name[:3])
    level_text = '%s %-8s' % (emoji, level_name)
    record.short_name = short_name
    record.level_display = _colorize(level_text, _LEVEL_COLORS.get(level_name, ''))
    record.module_display = _colorize(short_name, _module_color(record.name))
    record.thread_display = _colorize(
        str(record.threadName or 'Main')[:22],
        _thread_color(record.threadName),
    )
    return True



def _filter_log_record(record):
    with LOGGING_LOCK:
        disabled_threads = _copy_set(LOGGING_CONFIG.get('disabled_threads', set())) or set()
        allowed_threads = _copy_set(LOGGING_CONFIG.get('allowed_threads', None))
        thread_levels = dict(LOGGING_CONFIG.get('thread_levels', {}))

        disabled_modules = _copy_set(LOGGING_CONFIG.get('disabled_modules', set())) or set()
        allowed_modules = _copy_set(LOGGING_CONFIG.get('allowed_modules', None))
        module_levels = dict(LOGGING_CONFIG.get('module_levels', {}))

    if record.threadName in disabled_threads:
        return False

    if allowed_threads is not None and record.threadName not in allowed_threads:
        return False

    for candidate in disabled_modules:
        if _record_matches_module(record.name, candidate):
            return False

    if not _resolve_module_filter_match(record.name, allowed_modules):
        return False

    thread_level = thread_levels.get(record.threadName)
    if isinstance(thread_level, int) and record.levelno < thread_level:
        return False

    for module_name, module_level in module_levels.items():
        if _record_matches_module(record.name, module_name):
            if isinstance(module_level, int) and record.levelno < module_level:
                return False
            break

    return _apply_record_attributes(record)



def _gzip_namer(default_name):
    return '%s.gz' % default_name



def _gzip_rotator(source, dest):
    try:
        with open(source, 'rb') as source_handle:
            with gzip.open(dest, 'wb') as dest_handle:
                shutil.copyfileobj(source_handle, dest_handle)
        os.remove(source)
    except Exception:
        try:
            if os.path.exists(dest):
                os.remove(dest)
        except Exception:
            pass



def _close_root_handlers(root_logger):
    for handler in list(root_logger.handlers):
        try:
            handler.flush()
        except Exception:
            pass
        try:
            handler.close()
        except Exception:
            pass
        try:
            root_logger.removeHandler(handler)
        except Exception:
            pass



def _timestamp_log_path(log_path):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    directory = os.path.dirname(log_path)
    file_name = os.path.basename(log_path)

    if '.' in file_name:
        base_name, extension = file_name.rsplit('.', 1)
        stamped_name = '%s_%s.%s' % (base_name, timestamp, extension)
    else:
        stamped_name = '%s_%s.log' % (file_name, timestamp)

    return os.path.normpath(os.path.join(directory, stamped_name))



def _build_console_formatter():
    return logging.Formatter(
        '%(asctime)s | %(level_display)s | %(thread_display)-22s | %(module_display)s | %(message)s',
        datefmt='%H:%M:%S',
    )



def _build_file_formatter():
    return logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(threadName)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )



def _create_file_handler(timestamped_log_file):
    rotation_cfg = copy.deepcopy(LOGGING_CONFIG.get('rotation_config', {}) or {})
    rotation_type = str(rotation_cfg.get('type', 'none')).strip().lower()

    if rotation_type == 'size':
        file_handler = RotatingFileHandler(
            timestamped_log_file,
            maxBytes=_cfg_int(rotation_cfg.get('max_bytes', 0), 0),
            backupCount=_cfg_int(rotation_cfg.get('backup_count', 0), 0),
            encoding='utf-8',
        )
    elif rotation_type == 'time':
        file_handler = TimedRotatingFileHandler(
            timestamped_log_file,
            when=str(rotation_cfg.get('when', 'midnight') or 'midnight'),
            interval=_cfg_int(rotation_cfg.get('interval', 1), 1),
            backupCount=_cfg_int(rotation_cfg.get('backup_count', 0), 0),
            encoding='utf-8',
            utc=_cfg_bool(rotation_cfg.get('utc', False), False),
        )
    else:
        file_handler = logging.FileHandler(timestamped_log_file, mode='w', encoding='utf-8')

    if hasattr(file_handler, 'namer') and _cfg_bool(rotation_cfg.get('compress', False), False):
        file_handler.namer = _gzip_namer
        file_handler.rotator = _gzip_rotator

    return file_handler



def set_allowed_modules(*modules):
    """Set the allowed module whitelist for logging."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['allowed_modules'] = set(modules) if modules else None



def allow_all_modules():
    """Allow all modules to emit log records."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['allowed_modules'] = None



def set_module_level(module_name, level):
    """Set a specific level threshold for one module namespace."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['module_levels'][str(module_name)] = _resolve_log_level(level, default=logging.INFO)



def disable_module_logging(module_name):
    """Disable logging for one module namespace."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['disabled_modules'].add(str(module_name))



def enable_module_logging(module_name, level=None):
    """Re-enable logging for one module namespace."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['disabled_modules'].discard(str(module_name))
    if level is not None:
        set_module_level(module_name, level)



def set_rotation_config(rotation_type='size', **kwargs):
    """Update the active log rotation configuration."""
    with LOGGING_LOCK:
        rotation_cfg = LOGGING_CONFIG.setdefault('rotation_config', {})
        rotation_cfg.clear()
        rotation_cfg.update(_normalize_rotation_config(dict(kwargs, type=rotation_type)))



def apply_thread_selection_config(selection):
    """Apply a user-facing thread selection mapping to the whitelist."""
    if not selection:
        with LOGGING_LOCK:
            LOGGING_CONFIG['allowed_threads'] = None
        return

    allowed = set()
    for thread_name, enabled in dict(selection).items():
        if enabled:
            allowed.add(str(thread_name))

    with LOGGING_LOCK:
        LOGGING_CONFIG['allowed_threads'] = allowed or None



def set_allowed_threads(*thread_names):
    """Set the allowed thread whitelist for logging."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['allowed_threads'] = set(thread_names) if thread_names else None



def allow_all_threads():
    """Allow all threads to emit log records."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['allowed_threads'] = None



def set_thread_level(thread_name, level):
    """Set a specific level threshold for one thread name."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['thread_levels'][str(thread_name)] = _resolve_log_level(level, default=logging.INFO)



def disable_thread_logging(thread_name):
    """Disable logging for one thread name."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['disabled_threads'].add(str(thread_name))



def enable_thread_logging(thread_name, level=None):
    """Re-enable logging for one thread name."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['disabled_threads'].discard(str(thread_name))
    if level is not None:
        set_thread_level(thread_name, level)



def set_log_path_config(log_dir=None, log_file=None):
    """Update log path settings."""
    with LOGGING_LOCK:
        paths_cfg = LOGGING_CONFIG.setdefault('paths', {})
        if log_dir:
            paths_cfg['log_dir'] = str(log_dir)
        if log_file:
            paths_cfg['log_file'] = str(log_file)



def set_startup_cleanup_config(enabled=True, patterns=None, recursive=False):
    """Update startup cleanup behavior for system logs."""
    with LOGGING_LOCK:
        cleanup_cfg = LOGGING_CONFIG.setdefault('startup_cleanup', {})
        cleanup_cfg['enabled'] = bool(enabled)
        cleanup_cfg['recursive'] = bool(recursive)
        if patterns is not None:
            cleanup_cfg['patterns'] = [str(pattern) for pattern in patterns if pattern]



def set_retention_cleanup_config(enabled=False, keep_days=30, patterns=None, recursive=False):
    """Update retention cleanup behavior for system logs."""
    with LOGGING_LOCK:
        cleanup_cfg = LOGGING_CONFIG.setdefault('retention_cleanup', {})
        cleanup_cfg['enabled'] = bool(enabled)
        cleanup_cfg['keep_days'] = int(keep_days)
        cleanup_cfg['recursive'] = bool(recursive)
        if patterns is not None:
            cleanup_cfg['patterns'] = [str(pattern) for pattern in patterns if pattern]



def _log_startup_cleanup_result(cleanup_result):
    """Log the startup cleanup result after the logging system is active."""
    if not isinstance(cleanup_result, dict):
        return

    if not cleanup_result.get('enabled', False):
        logger.info('Startup log cleanup is disabled')
        return

    if not cleanup_result.get('performed', False):
        logger.info('Startup log cleanup was not executed')
        return

    logger.info(
        'Startup log cleanup completed: deleted=%d directory=%s',
        cleanup_result.get('deleted_count', 0),
        cleanup_result.get('log_dir', ''),
    )

    for error_entry in cleanup_result.get('errors', []):
        logger.warning(
            'Startup cleanup error: path=%s error=%s',
            error_entry.get('path', 'unknown'),
            error_entry.get('error', 'unknown'),
        )



def configure_logging_from_app_config(app_config, logger_instance=None):
    """Apply logging settings from the loaded runtime configuration."""
    logger_obj = logger_instance or logger
    if not isinstance(app_config, dict):
        return False

    system_logs_cfg = _config_dict_get(app_config, 'logging.system_logs', None)
    if not isinstance(system_logs_cfg, dict):
        return False

    with LOGGING_LOCK:
        LOGGING_CONFIG['enabled'] = _cfg_bool(system_logs_cfg.get('enabled', True), True)

    set_log_path_config(
        log_dir=system_logs_cfg.get('directory'),
        log_file=system_logs_cfg.get('file_name'),
    )

    levels_cfg = system_logs_cfg.get('levels')
    single_level = system_logs_cfg.get('level')
    if isinstance(levels_cfg, dict):
        with LOGGING_LOCK:
            if 'console' in levels_cfg:
                LOGGING_CONFIG['console_level'] = _resolve_log_level(levels_cfg.get('console'), default=LOGGING_CONFIG.get('console_level', logging.INFO))
            if 'file' in levels_cfg:
                LOGGING_CONFIG['file_level'] = _resolve_log_level(levels_cfg.get('file'), default=LOGGING_CONFIG.get('file_level', logging.DEBUG))
    elif single_level is not None:
        resolved_level = _resolve_log_level(single_level, default=logging.INFO)
        with LOGGING_LOCK:
            LOGGING_CONFIG['console_level'] = resolved_level
            LOGGING_CONFIG['file_level'] = resolved_level

    rotation_cfg = system_logs_cfg.get('rotation')
    if isinstance(rotation_cfg, dict):
        normalized_rotation = _normalize_rotation_config(rotation_cfg)
        set_rotation_config(normalized_rotation.get('type', 'none'), **normalized_rotation)

    startup_cleanup_cfg = system_logs_cfg.get('startup_cleanup')
    if isinstance(startup_cleanup_cfg, dict):
        set_startup_cleanup_config(
            enabled=startup_cleanup_cfg.get('enabled', True),
            patterns=startup_cleanup_cfg.get('patterns'),
            recursive=startup_cleanup_cfg.get('recursive', False),
        )

    retention_cleanup_cfg = system_logs_cfg.get('retention_cleanup')
    if isinstance(retention_cleanup_cfg, dict):
        set_retention_cleanup_config(
            enabled=retention_cleanup_cfg.get('enabled', False),
            keep_days=retention_cleanup_cfg.get('keep_days', 30),
            patterns=retention_cleanup_cfg.get('patterns'),
            recursive=retention_cleanup_cfg.get('recursive', False),
        )

    logger_obj.debug('Logging configuration loaded from app config')
    return True



def setup_logging(log_file='system.log', log_level=logging.INFO, project_root=None):
    """Initialize root logging with deterministic console and file handlers."""
    del log_level
    root = project_root or PROJECT_ROOT
    root_logger = logging.getLogger()

    resolved_log_dir, _, resolved_log_path = resolve_logging_target_paths(
        logging_config=LOGGING_CONFIG,
        project_root=root,
        log_file=log_file,
    )
    os.makedirs(resolved_log_dir, exist_ok=True)
    timestamped_log_file = _timestamp_log_path(resolved_log_path)

    with LOGGING_LOCK:
        LOGGING_RUNTIME_STATE['current_log_file'] = timestamped_log_file

    _close_root_handlers(root_logger)
    root_logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(_resolve_log_level(LOGGING_CONFIG.get('console_level', logging.INFO), default=logging.INFO))
    console_handler.setFormatter(_build_console_formatter())
    console_handler.addFilter(_filter_log_record)
    root_logger.addHandler(console_handler)

    file_handler = _create_file_handler(timestamped_log_file)
    file_handler.setLevel(_resolve_log_level(LOGGING_CONFIG.get('file_level', logging.DEBUG), default=logging.DEBUG))
    file_handler.setFormatter(_build_file_formatter())
    file_handler.addFilter(_filter_log_record)
    root_logger.addHandler(file_handler)

    with LOGGING_LOCK:
        LOGGING_RUNTIME_STATE['log_handlers'] = {
            'console': console_handler,
            'file': file_handler,
        }

    logger.info('=' * 60)
    logger.info('NEW LOG STARTED')
    logger.info('Log file: %s', timestamped_log_file)
    logger.info('Start time: %s', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info('=' * 60)

    return timestamped_log_file



def get_logger(name=None):
    """Return a cached logger instance by name."""
    if name is None:
        return logger

    logger_name = str(name)
    with LOGGING_LOCK:
        if logger_name in custom_loggers:
            return custom_loggers[logger_name]
        new_logger = logging.getLogger(logger_name)
        custom_loggers[logger_name] = new_logger
        return new_logger



def log_level_test():
    """Emit one message for each standard log level."""
    logger.debug('DEBUG log level test')
    logger.info('INFO log level test')
    logger.warning('WARNING log level test')
    logger.error('ERROR log level test')
    logger.critical('CRITICAL log level test')



def configure_logging_for_production():
    """Apply a pragmatic production logging profile."""
    with LOGGING_LOCK:
        LOGGING_CONFIG['console_level'] = logging.INFO
        LOGGING_CONFIG['file_level'] = logging.DEBUG
        LOGGING_CONFIG['rotation_config'] = {
            'type': 'size',
            'max_bytes': 10 * 1024 * 1024,
            'backup_count': 10,
            'compress': True,
            'when': 'midnight',
            'interval': 1,
            'utc': False,
        }
