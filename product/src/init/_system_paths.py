# -*- coding: utf-8 -*-

"""Path and cleanup helpers for startup and logging initialization."""

import glob
import logging
import os
import time

logger = logging.getLogger(__name__)


def get_project_root(anchor_file=None):
    """Resolve the project root from a file inside the src tree."""
    selected_anchor = anchor_file or __file__
    current_file = os.path.abspath(selected_anchor)
    return os.path.dirname(os.path.dirname(os.path.dirname(current_file)))


PROJECT_ROOT = get_project_root()
DEFAULT_LOG_ROOT_DIR = os.path.join(PROJECT_ROOT, 'logs')
DEFAULT_SYSTEM_LOG_DIR = os.path.join(DEFAULT_LOG_ROOT_DIR, 'system_logs')
DEFAULT_SYSTEM_LOG_FILE = 'system.log'


def _normalize_project_relative_path(path_value, default_value, project_root=None):
    """Normalize a path against the project root."""
    root = project_root or PROJECT_ROOT
    selected_value = path_value if path_value else default_value

    if selected_value is None:
        return None

    selected_text = str(selected_value).strip()
    if not selected_text:
        return None

    expanded = os.path.expanduser(os.path.expandvars(selected_text))
    if os.path.isabs(expanded):
        return os.path.normpath(expanded)

    return os.path.normpath(os.path.join(root, expanded))


def get_logs_root_directory(project_root=None):
    """Return the log root directory and ensure that it exists."""
    root = project_root or PROJECT_ROOT
    logs_root = os.path.join(root, 'logs')
    os.makedirs(logs_root, exist_ok=True)
    return logs_root



def get_logs_directory(logging_config=None, project_root=None):
    """Return the system log directory and ensure that it exists."""
    config = logging_config if isinstance(logging_config, dict) else {}
    path_cfg = config.get('paths', {}) if isinstance(config.get('paths', {}), dict) else {}
    configured_dir = path_cfg.get('log_dir')
    logs_dir = _normalize_project_relative_path(
        configured_dir,
        os.path.join('logs', 'system_logs'),
        project_root=project_root,
    )
    os.makedirs(logs_dir, exist_ok=True)
    return logs_dir



def get_default_log_file(logging_config=None, project_root=None):
    """Return the default system log file path."""
    config = logging_config if isinstance(logging_config, dict) else {}
    path_cfg = config.get('paths', {}) if isinstance(config.get('paths', {}), dict) else {}
    configured_file = path_cfg.get('log_file', DEFAULT_SYSTEM_LOG_FILE) or DEFAULT_SYSTEM_LOG_FILE

    if os.path.isabs(str(configured_file)):
        return os.path.normpath(str(configured_file))

    logs_dir = get_logs_directory(logging_config=config, project_root=project_root)
    return os.path.join(logs_dir, os.path.basename(str(configured_file)))



def _resolve_cleanup_patterns(patterns, fallback_patterns=None):
    """Normalize cleanup patterns and preserve deterministic order."""
    resolved_patterns = []

    if isinstance(patterns, str):
        resolved_patterns.append(patterns)
    elif isinstance(patterns, (list, tuple, set)):
        for pattern in patterns:
            if isinstance(pattern, str) and pattern:
                resolved_patterns.append(pattern)

    if not resolved_patterns and fallback_patterns is not None:
        return _resolve_cleanup_patterns(fallback_patterns, fallback_patterns=None)

    if not resolved_patterns:
        resolved_patterns = ['*.log', '*.log.*', '*.gz', '*.zip']

    unique_patterns = []
    seen = set()
    for pattern in resolved_patterns:
        if pattern in seen:
            continue
        seen.add(pattern)
        unique_patterns.append(pattern)

    return tuple(unique_patterns)



def resolve_logging_target_paths(logging_config=None, project_root=None, log_dir=None, log_file=None):
    """Resolve the effective log directory, file name and full path."""
    config = logging_config if isinstance(logging_config, dict) else {}
    root = project_root or PROJECT_ROOT
    path_cfg = config.get('paths', {}) if isinstance(config.get('paths', {}), dict) else {}

    resolved_log_dir = log_dir
    if not resolved_log_dir:
        resolved_log_dir = os.environ.get('LOG_DIR')
    if not resolved_log_dir:
        resolved_log_dir = path_cfg.get('log_dir')
    resolved_log_dir = _normalize_project_relative_path(
        resolved_log_dir,
        os.path.join('logs', 'system_logs'),
        project_root=root,
    )

    resolved_log_file = log_file
    if not resolved_log_file:
        resolved_log_file = os.environ.get('LOG_FILE')
    if not resolved_log_file:
        resolved_log_file = path_cfg.get('log_file')
    if not resolved_log_file:
        resolved_log_file = DEFAULT_SYSTEM_LOG_FILE

    resolved_log_file = str(resolved_log_file)
    if os.path.isabs(resolved_log_file):
        resolved_log_path = os.path.normpath(resolved_log_file)
        resolved_log_dir = os.path.dirname(resolved_log_path) or resolved_log_dir
        resolved_log_file = os.path.basename(resolved_log_path)
    else:
        resolved_log_file = os.path.basename(resolved_log_file) or DEFAULT_SYSTEM_LOG_FILE
        resolved_log_path = os.path.normpath(os.path.join(resolved_log_dir, resolved_log_file))

    os.makedirs(resolved_log_dir, exist_ok=True)
    return resolved_log_dir, resolved_log_file, resolved_log_path



def _collect_matching_files(log_dir, patterns=None, recursive=False, project_root=None):
    """Collect regular files that match one or more glob patterns."""
    resolved_dir = _normalize_project_relative_path(
        log_dir,
        os.path.join('logs', 'system_logs'),
        project_root=project_root,
    )
    resolved_patterns = _resolve_cleanup_patterns(patterns)
    os.makedirs(resolved_dir, exist_ok=True)

    collected = []
    seen = set()

    for pattern in resolved_patterns:
        if recursive:
            search_pattern = os.path.join(resolved_dir, '**', pattern)
        else:
            search_pattern = os.path.join(resolved_dir, pattern)

        for candidate in glob.glob(search_pattern, recursive=bool(recursive)):
            normalized_candidate = os.path.normpath(candidate)
            if normalized_candidate in seen:
                continue
            if not (os.path.isfile(normalized_candidate) or os.path.islink(normalized_candidate)):
                continue
            seen.add(normalized_candidate)
            collected.append(normalized_candidate)

    collected.sort()
    return tuple(collected)



def cleanup_log_directory(
    logging_config=None,
    runtime_state=None,
    project_root=None,
    log_dir=None,
    patterns=None,
    recursive=False,
):
    """Delete matching log files from the resolved log directory."""
    config = logging_config if isinstance(logging_config, dict) else {}
    state = runtime_state if isinstance(runtime_state, dict) else {}

    resolved_log_dir, _, _ = resolve_logging_target_paths(
        logging_config=config,
        project_root=project_root,
        log_dir=log_dir,
    )

    cleanup_patterns = _resolve_cleanup_patterns(
        patterns,
        fallback_patterns=(config.get('startup_cleanup', {}) or {}).get('patterns'),
    )

    current_log_file = state.get('current_log_file')
    current_log_path = os.path.normpath(current_log_file) if current_log_file else None

    result = {
        'log_dir': resolved_log_dir,
        'patterns': cleanup_patterns,
        'recursive': bool(recursive),
        'deleted_files': [],
        'errors': [],
    }

    candidates = _collect_matching_files(
        resolved_log_dir,
        patterns=cleanup_patterns,
        recursive=recursive,
        project_root=project_root,
    )

    for candidate in candidates:
        normalized_candidate = os.path.normpath(candidate)
        if current_log_path and normalized_candidate == current_log_path:
            continue

        try:
            os.remove(normalized_candidate)
            result['deleted_files'].append(normalized_candidate)
        except FileNotFoundError:
            continue
        except Exception as exc:
            result['errors'].append({'path': normalized_candidate, 'error': str(exc)})

    result['deleted_count'] = len(result['deleted_files'])
    result['error_count'] = len(result['errors'])
    return result



def cleanup_logs_on_startup(logging_config=None, runtime_state=None, project_root=None, log_dir=None):
    """Run startup cleanup according to the active logging configuration."""
    config = logging_config if isinstance(logging_config, dict) else {}
    cleanup_cfg = config.get('startup_cleanup', {}) or {}

    resolved_log_dir, _, _ = resolve_logging_target_paths(
        logging_config=config,
        project_root=project_root,
        log_dir=log_dir,
    )

    cleanup_enabled = bool(cleanup_cfg.get('enabled', True))
    cleanup_recursive = bool(cleanup_cfg.get('recursive', False))
    cleanup_patterns = _resolve_cleanup_patterns(cleanup_cfg.get('patterns'))

    result = {
        'enabled': cleanup_enabled,
        'performed': False,
        'log_dir': resolved_log_dir,
        'patterns': cleanup_patterns,
        'recursive': cleanup_recursive,
        'deleted_files': [],
        'errors': [],
        'deleted_count': 0,
        'error_count': 0,
    }

    if not cleanup_enabled:
        return result

    cleanup_result = cleanup_log_directory(
        logging_config=config,
        runtime_state=runtime_state,
        project_root=project_root,
        log_dir=resolved_log_dir,
        patterns=cleanup_patterns,
        recursive=cleanup_recursive,
    )

    result.update(cleanup_result)
    result['performed'] = True
    return result



def cleanup_old_logs(
    base_pattern=None,
    keep_days=30,
    log_dir=None,
    logging_config=None,
    project_root=None,
    recursive=False,
):
    """Delete old log files based on age retention."""
    config = logging_config if isinstance(logging_config, dict) else {}
    resolved_log_dir, _, _ = resolve_logging_target_paths(
        logging_config=config,
        project_root=project_root,
        log_dir=log_dir,
    )

    patterns = _resolve_cleanup_patterns(base_pattern)
    now_ts = time.time()
    max_age_s = float(int(keep_days) * 86400)

    deleted_files = []
    error_entries = []

    for candidate in _collect_matching_files(
        resolved_log_dir,
        patterns=patterns,
        recursive=recursive,
        project_root=project_root,
    ):
        try:
            age_s = now_ts - os.path.getmtime(candidate)
            if age_s < max_age_s:
                continue
            os.remove(candidate)
            deleted_files.append(candidate)
        except FileNotFoundError:
            continue
        except Exception as exc:
            error_entries.append({'path': candidate, 'error': str(exc)})

    return {
        'log_dir': resolved_log_dir,
        'patterns': patterns,
        'recursive': bool(recursive),
        'deleted_files': deleted_files,
        'errors': error_entries,
        'deleted_count': len(deleted_files),
        'error_count': len(error_entries),
    }



def _iter_project_pycache_dirs(project_root=None, include_root_names=None):
    """Yield project-local __pycache__ directories in deterministic order."""
    root = os.path.abspath(project_root or PROJECT_ROOT)
    allowed_roots = []

    for item in tuple(include_root_names or ("src",)):
        if not isinstance(item, str):
            continue
        item_text = item.strip()
        if not item_text:
            continue
        allowed_roots.append(os.path.normpath(os.path.join(root, item_text)))

    if not allowed_roots:
        allowed_roots.append(os.path.join(root, 'src'))

    collected = []
    seen = set()

    for scan_root in allowed_roots:
        if not os.path.isdir(scan_root):
            continue
        for current_root, dir_names, _file_names in os.walk(scan_root):
            dir_names.sort()
            if os.path.basename(current_root) != '__pycache__':
                continue
            normalized = os.path.normpath(current_root)
            if normalized in seen:
                continue
            seen.add(normalized)
            collected.append(normalized)

    collected.sort()
    return tuple(collected)



def cleanup_project_pycache_dirs(project_root=None, include_root_names=None):
    """Delete project-local __pycache__ directories.

    Important limitation:
    Python can still create early cache files before runtime code executes.
    This cleanup keeps the project tree clean after startup even when the
    interpreter had already created local cache files before the bootstrap
    could redirect later bytecode files into /tmp.
    """
    root = os.path.abspath(project_root or PROJECT_ROOT)
    deleted_dirs = []
    deleted_files = []
    errors = []

    for pycache_dir in _iter_project_pycache_dirs(
        project_root=root,
        include_root_names=include_root_names,
    ):
        try:
            children = []
            for child_name in sorted(os.listdir(pycache_dir)):
                children.append(os.path.join(pycache_dir, child_name))

            for child_path in children:
                try:
                    if os.path.isdir(child_path) and not os.path.islink(child_path):
                        continue
                    os.remove(child_path)
                    deleted_files.append(os.path.normpath(child_path))
                except FileNotFoundError:
                    continue
                except Exception as exc:
                    errors.append({'path': os.path.normpath(child_path), 'error': str(exc)})

            try:
                os.rmdir(pycache_dir)
                deleted_dirs.append(os.path.normpath(pycache_dir))
            except OSError:
                pass
        except FileNotFoundError:
            continue
        except Exception as exc:
            errors.append({'path': os.path.normpath(pycache_dir), 'error': str(exc)})

    return {
        'project_root': root,
        'deleted_dirs': deleted_dirs,
        'deleted_files': deleted_files,
        'deleted_dir_count': len(deleted_dirs),
        'deleted_file_count': len(deleted_files),
        'errors': errors,
        'error_count': len(errors),
    }
