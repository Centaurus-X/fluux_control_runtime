# -*- coding: utf-8 -*-

"""Compatibility facade for legacy app-config helpers.

The functional runtime configuration logic now lives in src.orchestration.runtime_config.
This module preserves explicit compatibility names without reintroducing global magic.
"""

import logging
import os

from src.orchestration.runtime_config import (
    DEFAULT_APP_CONFIG_PATH,
    deep_merge_dicts,
    app_cfg_get,
    cfg_bool,
    cfg_int,
    cfg_float,
    cfg_str,
    resolve_project_root,
    resolve_relative_path,
    build_master_uri,
    load_effective_app_config,
    build_runtime_settings,
    runtime_cfg_sleep,
    _make_print_startup_ascii_fn,
)


def resolve_path(project_root, path_value):
    return resolve_relative_path(project_root, path_value)


def app_cfg_sleep(cfg, dotted_path, default_seconds):
    runtime_cfg_sleep(cfg, dotted_path, default_seconds)


def load_yaml_app_config(
    load_enabled=True,
    config_path=DEFAULT_APP_CONFIG_PATH,
    active_profile=None,
    anchor_file=None,
    logger=None,
):
    return load_effective_app_config(
        load_enabled=load_enabled,
        config_path=config_path,
        active_profile=active_profile,
        anchor_file=anchor_file,
        logger=logger,
    )


def _apply_namespace_exports(namespace, runtime_meta):
    app_config = runtime_meta.get('config', {})
    project_root = runtime_meta.get('project_root') or resolve_project_root(namespace.get('__file__', __file__))
    anchor_file = namespace.get('__file__', __file__)
    base_dir = os.path.dirname(os.path.abspath(anchor_file))
    settings = build_runtime_settings(app_config, project_root, base_dir=base_dir)

    namespace['APP_CONFIG'] = app_config
    namespace['APP_CONFIG_RUNTIME'] = runtime_meta
    namespace['node_id'] = settings['node_id']
    namespace['show_ascii'] = settings['show_ascii']
    namespace['MASTER_PORT'] = settings['network']['master_port']
    namespace['MQTT_BROKER_HOST'] = settings['mqtt_client']['broker_host']
    namespace['MQTT_BROKER_PORT'] = settings['mqtt_client']['broker_port']
    namespace['WEBSOCKET_PORT_WORKER'] = settings['network']['master_port']
    namespace['WEBSOCKET_PORT_CLIENT'] = settings['network']['master_port']
    namespace['LOCAL_MODE'] = settings['network']['local_mode']
    namespace['MASTER_URI'] = settings['network']['master_uri']
    namespace['CONFIG_PATH'] = settings['datastore']['main_json_path']
    namespace['CONFIG_PATH_'] = settings['datastore']['test_json_path']
    namespace['APP_RUNTIME_SETTINGS'] = settings

    current_cfg = namespace.get('cfg', {})
    if not isinstance(current_cfg, dict):
        current_cfg = {}

    namespace['cfg'] = {
        'ip': settings['network']['master_ip'],
        'master_port': settings['network']['master_port'],
        'broker_host': settings['mqtt_client']['broker_host'],
        'broker_port': settings['mqtt_client']['broker_port'],
        'protocol': settings['network']['protocol'],
        'use_ipv6': settings['network']['use_ipv6'],
        'local_mode': settings['network']['local_mode'],
    }

    worker_state = namespace.get('worker_state')
    worker_state_lock = namespace.get('worker_state_lock')
    if isinstance(worker_state, dict):
        def apply_worker_state():
            worker_state['NODE_ID'] = settings['node_id']
            worker_state['RUN_TIME'] = settings['worker_state']['run_time_mode']
            worker_state['LOCAL_MODE'] = settings['network']['local_mode']
            worker_state['MASTER'] = settings['network']['master_uri']
            worker_state['PERSIST_DATA'] = settings['worker_state']['persist_data']

        if worker_state_lock is None:
            apply_worker_state()
        else:
            try:
                with worker_state_lock:
                    apply_worker_state()
            except Exception:
                apply_worker_state()

    loader_fn = namespace.get('load_device_state_data')
    config_data = namespace.get('config_data')
    config_lock = namespace.get('config_lock')
    selected_json_path = settings['datastore']['selected_json_path']
    if callable(loader_fn) and config_data is not None and config_lock is not None and selected_json_path:
        def load_device_state_data_partial():
            return loader_fn(selected_json_path, config_data, config_lock)
        namespace['load_device_state_data_partial'] = load_device_state_data_partial

    return app_config


def apply_runtime_config(namespace, runtime_meta, logger=None):
    return _apply_namespace_exports(namespace, runtime_meta)


def load_and_apply_app_config(
    namespace,
    load_enabled=True,
    config_path=DEFAULT_APP_CONFIG_PATH,
    active_profile=None,
    logger=None,
):
    runtime_meta = load_yaml_app_config(
        load_enabled=load_enabled,
        config_path=config_path,
        active_profile=active_profile,
        anchor_file=namespace.get('__file__', __file__),
        logger=logger,
    )
    return apply_runtime_config(namespace, runtime_meta, logger=logger)


def log_runtime_config(namespace, logger=None):
    logger_obj = logger or logging.getLogger(__name__)
    logger_obj.info(
        '[app_config] node_id=%s LOCAL_MODE=%s MASTER_URI=%s BROKER=%s:%s',
        namespace.get('node_id'),
        namespace.get('LOCAL_MODE'),
        namespace.get('MASTER_URI'),
        namespace.get('MQTT_BROKER_HOST'),
        namespace.get('MQTT_BROKER_PORT'),
    )


def print_startup_ascii_from_config(namespace, ascii_module, logger=None):
    printer = _make_print_startup_ascii_fn(ascii_module)
    return printer(namespace.get('APP_CONFIG', {}), logger=logger)


__all__ = (
    'DEFAULT_APP_CONFIG_PATH',
    'deep_merge_dicts',
    'app_cfg_get',
    'cfg_bool',
    'cfg_int',
    'cfg_float',
    'cfg_str',
    'app_cfg_sleep',
    'resolve_project_root',
    'resolve_relative_path',
    'resolve_path',
    'build_master_uri',
    'load_yaml_app_config',
    'apply_runtime_config',
    'load_and_apply_app_config',
    'log_runtime_config',
    'print_startup_ascii_from_config',
)
