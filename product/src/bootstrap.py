# -*- coding: utf-8 -*-

import os
import sys
import logging

try:
    import yaml
except Exception:
    yaml = None

DEFAULT_APP_CONFIG_PATH = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'application', 'app_config.yaml')
)


def _resolve_project_root(anchor_file=None):
    resolved_anchor = anchor_file or __file__
    anchor_dir = os.path.dirname(os.path.abspath(resolved_anchor))
    return os.path.normpath(os.path.join(anchor_dir, '..'))


def _ensure_project_root_on_sys_path(anchor_file=None):
    project_root = _resolve_project_root(anchor_file)

    try:
        already_present = project_root in sys.path
    except Exception:
        already_present = False

    if not already_present:
        sys.path.insert(0, project_root)

    return project_root


def _deep_merge(base, override):
    if not isinstance(base, dict):
        base = {}
    if not isinstance(override, dict):
        override = {}

    result = {}
    for key, value in base.items():
        if isinstance(value, dict):
            result[key] = _deep_merge(value, {})
        else:
            result[key] = value

    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result.get(key), value)
        elif isinstance(value, dict):
            result[key] = _deep_merge({}, value)
        else:
            result[key] = value

    return result


def _cfg_get(cfg, dotted_path, default=None):
    current = cfg
    for part in str(dotted_path).split('.'):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def _load_effective_config(config_path):
    if yaml is None:
        return {}
    if not os.path.exists(config_path):
        return {}

    with open(config_path, 'r', encoding='utf-8') as handle:
        raw_cfg = yaml.safe_load(handle) or {}

    if not isinstance(raw_cfg, dict):
        return {}

    base_cfg = raw_cfg.get('base', {}) if isinstance(raw_cfg.get('base'), dict) else {}
    profiles = raw_cfg.get('profiles', {}) if isinstance(raw_cfg.get('profiles'), dict) else {}
    active_profile = str(raw_cfg.get('active_profile', 'base') or 'base').strip() or 'base'
    profile_cfg = profiles.get(active_profile, {}) if isinstance(profiles.get(active_profile), dict) else {}

    merged_cfg = _deep_merge(base_cfg, profile_cfg)
    for top_key in ('logging', 'python'):
        if top_key in raw_cfg:
            merged_cfg[top_key] = _deep_merge(merged_cfg.get(top_key, {}), raw_cfg.get(top_key, {}))
    return merged_cfg


def _build_pycache_prefix(config_path):
    cfg = _load_effective_config(config_path)
    enabled = bool(_cfg_get(cfg, 'python.pycache.enabled', True))
    if not enabled:
        return None

    root_dir = str(_cfg_get(cfg, 'python.pycache.root_dir', '/tmp') or '/tmp').strip() or '/tmp'
    cache_dir_name = str(_cfg_get(cfg, 'python.pycache.dir_name', 'sync_xserver_pycache') or 'sync_xserver_pycache').strip()
    strategy = str(_cfg_get(cfg, 'python.pycache.strategy', 'prefix') or 'prefix').strip().lower()

    if strategy != 'prefix':
        return None

    if not cache_dir_name:
        cache_dir_name = 'sync_xserver_pycache'

    return os.path.normpath(os.path.join(root_dir, cache_dir_name))


def apply(config_path=None, logger=None, anchor_file=None):
    logger_obj = logger or logging.getLogger(__name__)
    _ensure_project_root_on_sys_path(anchor_file=anchor_file or __file__)
    resolved_config_path = config_path or DEFAULT_APP_CONFIG_PATH
    pycache_prefix = _build_pycache_prefix(resolved_config_path)

    if not pycache_prefix:
        return None

    try:
        os.makedirs(pycache_prefix, exist_ok=True)
    except Exception as exc:
        logger_obj.debug('[bootstrap] Failed to create pycache prefix %s: %s', pycache_prefix, exc)
        return None

    os.environ['PYTHONPYCACHEPREFIX'] = pycache_prefix

    try:
        sys.pycache_prefix = pycache_prefix
    except Exception:
        pass

    return pycache_prefix
