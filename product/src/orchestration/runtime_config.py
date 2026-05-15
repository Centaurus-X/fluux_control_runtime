# -*- coding: utf-8 -*-

# src/orchestration/runtime_config.py

import copy
import importlib
import logging
import os
import threading
import time

try:
    import yaml
except Exception:
    yaml = None


DEFAULT_APP_CONFIG_PATH = "config/application/app_config.yaml"
DEFAULT_CONTRACT_REGISTRY_PATH = "contracts/contract_registry.yaml"
DEFAULT_PROCESS_STATES_MAPPING_CONTRACT_ID = "C08"
DEFAULT_CONTROL_METHODS_MAPPING_CONTRACT_ID = "C17"
# Legacy defaults remain only as non-strict compatibility fallbacks. The v31
# runtime resolves operative contract paths through contract_registry.yaml.
DEFAULT_PROCESS_STATES_MAPPING_CONTRACT_PATH = "contracts/runtime/PROCESS_STATES_MAPPING_CONTRACT_V1.yaml"
DEFAULT_CONTROL_METHODS_MAPPING_CONTRACT_PATH = "contracts/runtime/CONTROL_METHODS_MAPPING_CONTRACT_V1.yaml"
DEFAULT_HOT_RELOAD_POLL_INTERVAL_S = 1.0
DEFAULT_HOT_RELOAD_MUTABLE_PATHS = (
    "logging.system_logs.enabled",
    "logging.system_logs.level",
    "logging.system_logs.levels",
    "timing.main_loop_sleep_s",
    "shutdown.join_timeout_s",
    "shutdown.enforce_sigkill_timeout_s",
    "shutdown.nudge_queues.enabled",
    "shutdown.nudge_queues.repeats",
    "shutdown.nudge_queues.put_timeout_s",
    "shutdown.nudge_queues.stop_payloads",
    "runtime.hot_reload.enabled",
    "runtime.hot_reload.poll_interval_s",
    "runtime.hot_reload.mutable_paths",
)

_MISSING = object()


def _logger_or_default(logger=None):
    if logger is not None:
        return logger
    return logging.getLogger(__name__)


def is_mapping(value):
    return isinstance(value, dict)


def deep_merge_dicts(base, override):
    """
    Merge two mapping trees recursively.
    """
    if not is_mapping(base):
        base = {}
    if not is_mapping(override):
        override = {}

    merged = copy.deepcopy(base)

    for key, value in override.items():
        if is_mapping(value) and is_mapping(merged.get(key)):
            merged[key] = deep_merge_dicts(merged.get(key), value)
        else:
            merged[key] = copy.deepcopy(value)

    return merged


def cfg_bool(value, default):
    if value is None:
        return bool(default)

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False
        return bool(default)

    return bool(value)


def cfg_int(value, default):
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def cfg_float(value, default):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def cfg_str(value, default):
    try:
        if value is None:
            return str(default)
        return str(value)
    except Exception:
        return str(default)


def app_cfg_get(cfg, dotted_path, default=None):
    """
    Read a dotted-path value from a mapping safely.
    """
    if not is_mapping(cfg):
        return default

    if not dotted_path:
        return default

    current = cfg

    for part in str(dotted_path).split("."):
        if not is_mapping(current) or part not in current:
            return default
        current = current[part]

    return current


def set_dotted_value(cfg, dotted_path, value):
    """
    Set or replace a dotted-path value in-place.
    """
    if not is_mapping(cfg):
        raise ValueError("cfg must be a mapping")

    parts = [part for part in str(dotted_path).split(".") if part]
    if not parts:
        raise ValueError("dotted_path must not be empty")

    current = cfg

    for part in parts[:-1]:
        if part not in current or not is_mapping(current.get(part)):
            current[part] = {}
        current = current[part]

    current[parts[-1]] = copy.deepcopy(value)


def delete_dotted_value(cfg, dotted_path):
    """
    Delete a dotted-path value if present.
    """
    if not is_mapping(cfg):
        return False

    parts = [part for part in str(dotted_path).split(".") if part]
    if not parts:
        return False

    current = cfg

    for part in parts[:-1]:
        if not is_mapping(current) or part not in current:
            return False
        current = current[part]

    if not is_mapping(current) or parts[-1] not in current:
        return False

    del current[parts[-1]]
    return True


def resolve_project_root(anchor_file):
    """
    Resolve the delivered product root from an anchor file.

    The search walks upwards until it finds a directory that contains the
    delivered application config or, at minimum, the sibling directories
    ``src`` and ``config``. This makes the low-level helpers self-contained even
    when no explicit anchor file is supplied.
    """
    selected_anchor = anchor_file or __file__
    current_dir = os.path.dirname(os.path.abspath(selected_anchor))

    probe_dir = current_dir
    while True:
        config_candidate = os.path.join(probe_dir, DEFAULT_APP_CONFIG_PATH)
        if os.path.isfile(config_candidate):
            return os.path.normpath(probe_dir)

        if os.path.isdir(os.path.join(probe_dir, 'src')) and os.path.isdir(os.path.join(probe_dir, 'config')):
            return os.path.normpath(probe_dir)

        parent_dir = os.path.dirname(probe_dir)
        if parent_dir == probe_dir:
            break
        probe_dir = parent_dir

    return os.path.normpath(os.path.join(current_dir, '..'))


def resolve_relative_path(project_root, path_value):
    """
    Resolve a relative path against project_root.
    """
    if path_value is None:
        return None

    path_text = str(path_value).strip()
    if not path_text:
        return None

    expanded = os.path.expanduser(os.path.expandvars(path_text))

    if os.path.isabs(expanded):
        return os.path.normpath(expanded)

    return os.path.normpath(os.path.join(project_root, expanded))


def normalize_optional_config_value(value):
    """
    Normalize optional config values.

    YAML null, empty strings and textual null markers must not become file
    paths. This is important for optional TLS client certificates: the bridge
    must not inherit non-existing generic MQTT client cert/key defaults.
    """
    if value is None:
        return None
    text = cfg_str(value, "").strip()
    if not text:
        return None
    if text.lower() in ("none", "null", "nil", "false", "off", "disabled"):
        return None
    return text


def resolve_optional_path(project_root, path_value):
    normalized = normalize_optional_config_value(path_value)
    if normalized is None:
        return None
    return resolve_relative_path(project_root, normalized)



def normalize_config_text_sequence(value):
    if value is None:
        return ()
    if isinstance(value, str):
        items = [item.strip() for item in value.replace(";", ",").split(",")]
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]
    result = []
    for item in items:
        normalized = normalize_optional_config_value(item)
        if normalized is not None:
            result.append(normalized)
    return tuple(result)


def normalize_acl_policy(value, default="deny"):
    text = cfg_str(value, default).strip().lower()
    if text in ("allow", "permit", "accept"):
        return "allow"
    return "deny"


def normalize_mtls_mode(value, default="optional"):
    text = cfg_str(value, default).strip().lower().replace("-", "_")
    if text in ("required", "require", "strict", "mutual_tls", "mtls"):
        return "required"
    if text in ("disabled", "disable", "off", "false", "none", "no"):
        return "disabled"
    return "optional"


def build_security_runtime_settings(app_config, project_root, proxy_worker_bridge=None):
    bridge = proxy_worker_bridge if isinstance(proxy_worker_bridge, dict) else {}
    return {
        "runtime_security_version": "v35.1",
        "postgres_scope": "gateway_proxy_external_not_worker_runtime_dependency",
        "mqtt": {
            "authentication_required": cfg_bool(
                app_cfg_get(app_config, "security.mqtt.authentication_required", bridge.get("authentication_required", False)),
                False,
            ),
            "topic_acl_enforcement": cfg_bool(
                app_cfg_get(app_config, "security.mqtt.topic_acl_enforcement", bridge.get("topic_acl_enforcement", True)),
                True,
            ),
            "topic_acl_default_policy": normalize_acl_policy(
                app_cfg_get(app_config, "security.mqtt.topic_acl_default_policy", bridge.get("topic_acl_default_policy", "deny")),
                "deny",
            ),
        },
        "pki": {
            "mtls_mode": normalize_mtls_mode(
                app_cfg_get(app_config, "security.pki.mtls_mode", bridge.get("mtls_mode", "optional")),
                "optional",
            ),
            "certificate_rotation_enabled": cfg_bool(
                app_cfg_get(app_config, "security.pki.certificate_rotation_enabled", bridge.get("certificate_rotation_enabled", False)),
                False,
            ),
            "certificate_renew_before_days": cfg_int(
                app_cfg_get(app_config, "security.pki.certificate_renew_before_days", bridge.get("certificate_renew_before_days", 30)),
                30,
            ),
            "ca_cert_file": bridge.get("ca_cert_file"),
            "client_cert_file": bridge.get("client_cert_file"),
            "client_key_file_configured": bridge.get("client_key_file") is not None,
        },
        "secrets": {
            "username_env": app_cfg_get(app_config, "proxy_worker_bridge.username_env", None),
            "password_env": app_cfg_get(app_config, "proxy_worker_bridge.password_env", None),
            "password_inline_configured": normalize_optional_config_value(app_cfg_get(app_config, "proxy_worker_bridge.password", None)) is not None,
        },
    }


def build_monitoring_runtime_settings(app_config, project_root):
    health_path = app_cfg_get(app_config, "monitoring.health.path", "logs/system_logs/runtime_health.json")
    return {
        "version": "v35.1",
        "health": {
            "enabled": cfg_bool(app_cfg_get(app_config, "monitoring.health.enabled", True), True),
            "path": resolve_relative_path(project_root, health_path),
            "interval_s": cfg_float(app_cfg_get(app_config, "monitoring.health.interval_s", 15.0), 15.0),
            "max_age_s": cfg_float(app_cfg_get(app_config, "monitoring.health.max_age_s", 120.0), 120.0),
            "include_resource_summary": cfg_bool(app_cfg_get(app_config, "monitoring.health.include_resource_summary", True), True),
            "include_thread_summary": cfg_bool(app_cfg_get(app_config, "monitoring.health.include_thread_summary", True), True),
        },
        "alerting": {
            "enabled": cfg_bool(app_cfg_get(app_config, "monitoring.alerting.enabled", False), False),
            "log_only": cfg_bool(app_cfg_get(app_config, "monitoring.alerting.log_only", True), True),
            "worker_presence_max_age_s": cfg_float(app_cfg_get(app_config, "monitoring.alerting.worker_presence_max_age_s", 120.0), 120.0),
            "queue_backlog_warn": cfg_int(app_cfg_get(app_config, "monitoring.alerting.queue_backlog_warn", 1000), 1000),
            "restart_observation_window_s": cfg_float(app_cfg_get(app_config, "monitoring.alerting.restart_observation_window_s", 300.0), 300.0),
        },
    }


def build_fieldbus_runtime_settings(app_config, project_root):
    active_controller_ids = normalize_config_text_sequence(
        app_cfg_get(app_config, "fieldbus.runtime_profile.active_controller_ids", (2,))
    )
    optional_controller_ids = normalize_config_text_sequence(
        app_cfg_get(app_config, "fieldbus.runtime_profile.optional_controller_ids", (1, 3, 4, 5))
    )
    expected_simulators = app_cfg_get(app_config, "fieldbus.runtime_profile.expected_simulators", [])
    if not isinstance(expected_simulators, list):
        expected_simulators = []
    return {
        "runtime_profile": {
            "enabled": cfg_bool(app_cfg_get(app_config, "fieldbus.runtime_profile.enabled", True), True),
            "mode": cfg_str(app_cfg_get(app_config, "fieldbus.runtime_profile.mode", "simulation_subset"), "simulation_subset"),
            "active_controller_ids": tuple(cfg_int(item, -1) for item in active_controller_ids if cfg_int(item, -1) >= 0),
            "optional_controller_ids": tuple(cfg_int(item, -1) for item in optional_controller_ids if cfg_int(item, -1) >= 0),
            "strict_missing_simulators": cfg_bool(
                app_cfg_get(app_config, "fieldbus.runtime_profile.strict_missing_simulators", False),
                False,
            ),
            "expected_simulators": copy.deepcopy(expected_simulators),
        }
    }


def format_host_for_uri(host, use_ipv6):
    """
    Format a host for URI usage and wrap IPv6 literals.
    """
    host_value = cfg_str(host, "localhost").strip()
    if not host_value:
        return "localhost"

    if cfg_bool(use_ipv6, False) and ":" in host_value:
        if not (host_value.startswith("[") and host_value.endswith("]")):
            return "[%s]" % host_value

    return host_value


def build_master_uri(protocol, host, port, use_ipv6):
    """
    Build the master URI from normalized network settings.
    """
    scheme = cfg_str(protocol, "ws").strip() or "ws"
    normalized_host = format_host_for_uri(host, use_ipv6)
    normalized_port = cfg_int(port, 8000)
    return "%s://%s:%d" % (scheme, normalized_host, normalized_port)


def _load_yaml_file(config_file, logger=None):
    logger_obj = _logger_or_default(logger)

    if yaml is None:
        logger_obj.warning("[runtime_config] PyYAML is not available")
        return {}

    if not os.path.exists(config_file):
        logger_obj.warning("[runtime_config] Config file not found: %s", config_file)
        return {}

    try:
        with open(config_file, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except Exception as exc:
        logger_obj.error("[runtime_config] Failed to load YAML: %s", exc, exc_info=True)
        return {}

    if not is_mapping(data):
        logger_obj.warning("[runtime_config] YAML root is not a mapping")
        return {}

    return data




def _relative_registry_path_candidates(project_root, path_value):
    """
    Build deterministic candidate paths for registry entries.

    Registry paths are stored in repository notation, for example
    ``product/contracts/runtime/...``. Runtime helpers operate from the delivered
    product root. Therefore the resolver first honors absolute paths, then tries
    product-root notation, stripped ``product/`` notation and repository-root
    notation. No path is guessed outside these deterministic candidates.
    """
    if path_value is None:
        return []

    text = str(path_value).strip()
    if not text:
        return []

    expanded = os.path.expanduser(os.path.expandvars(text))
    if os.path.isabs(expanded):
        return [os.path.normpath(expanded)]

    normalized_text = expanded.replace("\\", "/")
    candidates = []

    def _add(candidate):
        if not candidate:
            return
        normalized = os.path.normpath(candidate)
        if normalized in candidates:
            return
        candidates.append(normalized)

    if project_root:
        root = os.path.normpath(project_root)
        _add(os.path.join(root, expanded))

        if normalized_text.startswith("product/"):
            stripped = normalized_text[len("product/"):]
            _add(os.path.join(root, stripped))

        repo_root = os.path.dirname(root)
        _add(os.path.join(repo_root, expanded))
    else:
        _add(expanded)

    return candidates


def resolve_registry_artifact_path(project_root, path_value):
    """
    Resolve a path value from contract_registry.yaml to an absolute path.
    """
    candidates = _relative_registry_path_candidates(project_root, path_value)
    if not candidates:
        return None

    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate

    return candidates[0]


def _registry_entries_by_id(registry_document):
    if not is_mapping(registry_document):
        return {}

    entries = registry_document.get("entries", [])
    if not isinstance(entries, list):
        return {}

    result = {}
    for entry in entries:
        if not is_mapping(entry):
            continue
        contract_id = str(entry.get("contract_id", "")).strip()
        if not contract_id:
            continue
        result[contract_id] = entry

    return result


def _registry_path_value_from_entry(entry, path_kind):
    if not is_mapping(entry):
        return None

    for group_name in ("current_paths", "planned_paths"):
        group = entry.get(group_name, {})
        if not is_mapping(group):
            continue
        value = group.get(path_kind)
        if value:
            return value

    return None


def resolve_contract_registry_entry(contract_registry, contract_id, path_kind="runtime_instance"):
    """
    Resolve one contract ID to a concrete artifact path through the registry.
    """
    registry = contract_registry if is_mapping(contract_registry) else {}
    entries_by_id = registry.get("entries_by_id", {})
    if not is_mapping(entries_by_id):
        entries_by_id = {}

    normalized_contract_id = cfg_str(contract_id, "").strip()
    entry = entries_by_id.get(normalized_contract_id)
    path_value = _registry_path_value_from_entry(entry, path_kind)
    resolved_path = resolve_registry_artifact_path(registry.get("project_root"), path_value)

    return {
        "contract_id": normalized_contract_id,
        "path_kind": path_kind,
        "found": is_mapping(entry),
        "title": entry.get("title") if is_mapping(entry) else None,
        "status": entry.get("status") if is_mapping(entry) else None,
        "classification": entry.get("classification") if is_mapping(entry) else None,
        "source_of_truth": entry.get("source_of_truth") if is_mapping(entry) else None,
        "registry_path_value": path_value,
        "resolved_path": resolved_path,
        "exists": bool(resolved_path and os.path.exists(resolved_path)),
    }


def build_contract_registry_runtime_settings(app_config, project_root, logger=None):
    """
    Load and normalize the runtime contract registry.

    v31 makes this the runtime single source of truth for operative contract
    artifact paths. Application config selects contract IDs; the registry owns
    the concrete file locations.
    """
    logger_obj = _logger_or_default(logger)
    enabled = cfg_bool(app_cfg_get(app_config, "contract_registry.enabled", True), True)
    strict = cfg_bool(app_cfg_get(app_config, "contract_registry.strict", True), True)
    registry_path_value = app_cfg_get(
        app_config,
        "contract_registry.registry_path",
        app_cfg_get(app_config, "contract_registry.path", DEFAULT_CONTRACT_REGISTRY_PATH),
    )
    registry_path = resolve_relative_path(project_root, registry_path_value)
    registry_document = {}
    errors = []

    if enabled:
        registry_document = _load_yaml_file(registry_path, logger=logger_obj)
        if not registry_document:
            errors.append("registry_not_loaded")
    else:
        errors.append("registry_disabled")

    entries_by_id = _registry_entries_by_id(registry_document)
    if enabled and not entries_by_id:
        errors.append("registry_entries_missing")

    settings = {
        "enabled": enabled,
        "strict": strict,
        "project_root": project_root,
        "registry_path": registry_path,
        "loaded": bool(registry_document),
        "schema_version": registry_document.get("schema_version") if is_mapping(registry_document) else None,
        "registry_id": registry_document.get("registry_id") if is_mapping(registry_document) else None,
        "status": registry_document.get("status") if is_mapping(registry_document) else None,
        "product_release_target": registry_document.get("product_release_target") if is_mapping(registry_document) else None,
        "canonical_contract_root": registry_document.get("canonical_contract_root") if is_mapping(registry_document) else None,
        "entry_count": len(entries_by_id),
        "entries_by_id": entries_by_id,
        "resolved_contracts": {},
        "errors": tuple(errors),
    }

    if errors and strict:
        logger_obj.warning(
            "[runtime_config] Contract registry strict mode has unresolved state: path=%s errors=%s",
            registry_path,
            errors,
        )

    return settings


def _resolve_runtime_contract_path_from_registry(
    contract_registry,
    contract_id,
    path_kind="runtime_instance",
):
    return resolve_contract_registry_entry(contract_registry, contract_id, path_kind=path_kind)

def _resolve_active_profile(raw_cfg, active_profile):
    if active_profile is not None and str(active_profile).strip():
        return str(active_profile).strip()

    yaml_profile = raw_cfg.get("active_profile", "base")
    profile = cfg_str(yaml_profile, "base").strip()

    if not profile:
        return "base"

    return profile


def load_effective_app_config(
    load_enabled=True,
    config_path=DEFAULT_APP_CONFIG_PATH,
    active_profile=None,
    anchor_file=None,
    logger=None,
):
    """
    Load YAML config and merge base + active profile.
    """
    logger_obj = _logger_or_default(logger)
    project_root = resolve_project_root(anchor_file or __file__)
    config_file = resolve_relative_path(project_root, config_path)

    result = {
        "config": {},
        "profile": "base",
        "config_file": config_file,
        "project_root": project_root,
        "anchor_file": anchor_file or __file__,
    }

    if not load_enabled:
        logger_obj.info("[runtime_config] Config load disabled")
        return result

    raw_cfg = _load_yaml_file(config_file, logger=logger_obj)
    if not raw_cfg:
        return result

    profile = _resolve_active_profile(raw_cfg, active_profile)
    base_cfg = raw_cfg.get("base", {})
    profiles_cfg = raw_cfg.get("profiles", {})
    profile_cfg = {}

    shared_cfg = {}
    for key, value in raw_cfg.items():
        if key in ("base", "profiles", "active_profile", "config_version"):
            continue
        shared_cfg[key] = copy.deepcopy(value)

    if is_mapping(profiles_cfg):
        profile_cfg = profiles_cfg.get(profile, {})

    effective_cfg = deep_merge_dicts(shared_cfg, base_cfg)
    effective_cfg = deep_merge_dicts(effective_cfg, profile_cfg)
    result["config"] = effective_cfg if is_mapping(effective_cfg) else {}
    result["profile"] = profile

    logger_obj.info(
        "[runtime_config] Loaded config: file=%s profile=%s",
        config_file,
        profile,
    )

    return result


def _normalize_logging_shape(app_config):
    """
    Normalize the logging section to a shape that supports both
    legacy helpers and the refactored runtime.
    """
    cfg = copy.deepcopy(app_config) if is_mapping(app_config) else {}

    system_logs = app_cfg_get(cfg, "logging.system_logs", None)
    if not is_mapping(system_logs):
        return cfg

    levels = system_logs.get("levels")
    single_level = system_logs.get("level")

    if not is_mapping(levels) and single_level is not None:
        system_logs["levels"] = {
            "console": single_level,
            "file": single_level,
        }

    return cfg


def select_device_state_json_path(app_config, project_root):
    """Resolve the selected device-state JSON path from config.

    The selection is deterministic and tolerant:
    - prefer the requested source
    - fall back to the existing alternative path when the requested file is missing
    - keep both requested and effective source labels explicit
    """
    requested_source = cfg_str(
        app_cfg_get(app_config, "datastore.device_state_source", "main"),
        "main",
    ).strip().lower()

    if requested_source not in ("main", "test"):
        requested_source = "main"

    main_rel = app_cfg_get(app_config, "paths.device_state_json", "config/___device_state_data.json")
    test_rel = app_cfg_get(app_config, "paths.device_state_json_test", "config/___device_state_data.variant_b.json")

    main_abs = resolve_relative_path(project_root, main_rel)
    test_abs = resolve_relative_path(project_root, test_rel)

    candidate_map = {
        "main": main_abs,
        "test": test_abs,
    }

    selected_source = requested_source
    selected_abs = candidate_map.get(requested_source)

    if not selected_abs or not os.path.exists(selected_abs):
        fallback_source = "test" if requested_source == "main" else "main"
        fallback_abs = candidate_map.get(fallback_source)
        if fallback_abs and os.path.exists(fallback_abs):
            selected_source = "%s_fallback_to_%s" % (requested_source, fallback_source)
            selected_abs = fallback_abs

    return {
        "requested_source": requested_source,
        "selected_source": selected_source,
        "main_json_path": main_abs,
        "test_json_path": test_abs,
        "selected_json_path": selected_abs,
    }

def build_mqtt_client_runtime_settings(app_config, project_root):
    """Build MQTT client runtime settings.

    This transport service replaces the local WebSocket server in the refactored
    orchestration layer. All path handling stays explicit and deterministic.
    """
    broker_host = cfg_str(
        app_cfg_get(
            app_config,
            "network.mqtt_client.broker_ip",
            app_cfg_get(app_config, "network.master.ip", "127.0.0.1"),
        ),
        "127.0.0.1",
    )
    ssl_enabled = cfg_bool(app_cfg_get(app_config, "network.mqtt_client.ssl.enabled", False), False)
    default_port = 8883 if ssl_enabled else 1883
    broker_port = cfg_int(
        app_cfg_get(
            app_config,
            "network.mqtt_client.broker_port",
            app_cfg_get(app_config, "network.master.port", default_port),
        ),
        default_port,
    )

    return {
        "enabled": cfg_bool(app_cfg_get(app_config, "network.mqtt_client.enabled", True), True),
        "broker_host": broker_host,
        "broker_port": broker_port,
        "topic_root": cfg_str(app_cfg_get(app_config, "network.mqtt_client.topic_root", "xserver/v1"), "xserver/v1"),
        "interface_role": cfg_str(app_cfg_get(app_config, "network.mqtt_client.interface_role", "worker"), "worker"),
        "username": app_cfg_get(app_config, "network.mqtt_client.username", None),
        "password": app_cfg_get(app_config, "network.mqtt_client.password", None),
        "keepalive": cfg_int(app_cfg_get(app_config, "network.mqtt_client.keepalive", 60), 60),
        "qos": cfg_int(app_cfg_get(app_config, "network.mqtt_client.qos", 1), 1),
        "protocol_version": cfg_str(app_cfg_get(app_config, "network.mqtt_client.protocol_version", "MQTTv311"), "MQTTv311"),
        "transport": cfg_str(app_cfg_get(app_config, "network.mqtt_client.transport", "tcp"), "tcp"),
        "clean_session": cfg_bool(app_cfg_get(app_config, "network.mqtt_client.clean_session", True), True),
        "reconnect_on_failure": cfg_bool(app_cfg_get(app_config, "network.mqtt_client.reconnect_on_failure", True), True),
        "reconnect_min_delay": cfg_int(app_cfg_get(app_config, "network.mqtt_client.reconnect_min_delay", 1), 1),
        "reconnect_max_delay": cfg_int(app_cfg_get(app_config, "network.mqtt_client.reconnect_max_delay", 30), 30),
        "recv_system_events": cfg_bool(app_cfg_get(app_config, "network.mqtt_client.recv_system_events", False), False),
        "auto_probe_on_connect": cfg_bool(app_cfg_get(app_config, "network.mqtt_client.auto_probe_on_connect", True), True),
        "auto_select_single_peer": cfg_bool(app_cfg_get(app_config, "network.mqtt_client.auto_select_single_peer", True), True),
        "connect_wait_timeout_s": cfg_float(app_cfg_get(app_config, "network.mqtt_client.connect_wait_timeout_s", 10.0), 10.0),
        "shutdown_join_timeout_s": cfg_float(app_cfg_get(app_config, "network.mqtt_client.shutdown_join_timeout_s", 2.0), 2.0),
        "ssl_enabled": ssl_enabled,
        "ssl_cafile": resolve_relative_path(project_root, app_cfg_get(app_config, "network.mqtt_client.ssl.cafile", None)),
        "ssl_capath": resolve_relative_path(project_root, app_cfg_get(app_config, "network.mqtt_client.ssl.capath", None)),
        "ssl_cadata": app_cfg_get(app_config, "network.mqtt_client.ssl.cadata", None),
        "ssl_certfile": resolve_relative_path(project_root, app_cfg_get(app_config, "network.mqtt_client.ssl.certfile", None)),
        "ssl_keyfile": resolve_relative_path(project_root, app_cfg_get(app_config, "network.mqtt_client.ssl.keyfile", None)),
        "ssl_password": app_cfg_get(app_config, "network.mqtt_client.ssl.password", None),
        "ssl_minimum_tls_version": cfg_str(app_cfg_get(app_config, "network.mqtt_client.ssl.minimum_tls_version", "TLSv1_2"), "TLSv1_2"),
        "ssl_ciphers": app_cfg_get(app_config, "network.mqtt_client.ssl.ciphers", None),
        "ssl_allow_unverified": cfg_bool(app_cfg_get(app_config, "network.mqtt_client.ssl.allow_unverified", False), False),
        "ssl_auto_allow_unverified_if_no_ca": cfg_bool(
            app_cfg_get(app_config, "network.mqtt_client.ssl.auto_allow_unverified_if_no_ca", False),
            False,
        ),
    }


def _normalize_path_list(value):
    if value is None:
        return []

    if isinstance(value, str):
        items = []
        for part in value.split(os.pathsep):
            text = str(part).strip()
            if text:
                items.append(text)
        return items

    if isinstance(value, (list, tuple)):
        items = []
        for item in value:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                items.append(text)
        return items

    text = str(value).strip()
    if not text:
        return []
    return [text]


def _collect_compiler_python_paths(app_config, project_root):
    candidates = []
    seen = set()

    def _add(path_value):
        if not path_value:
            return
        normalized = os.path.normpath(path_value)
        if normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    configured = _normalize_path_list(app_cfg_get(app_config, "config_source.compiler_python_paths", ()))
    env_paths = _normalize_path_list(os.environ.get("CUD_COMPILER_PYTHONPATH"))

    for raw_path in configured:
        resolved = resolve_relative_path(project_root, raw_path)
        if resolved:
            _add(resolved)

    for raw_path in env_paths:
        resolved = resolve_relative_path(project_root, raw_path)
        if resolved:
            _add(resolved)

    conventional = (
        os.path.join(project_root, "vendor", "current_compiler_engine", "code_compiler_engine"),
        os.path.join(project_root, "..", "source_inputs", "extracted", "_compiler", "current_compiler_engine", "code_compiler_engine"),
        os.path.join(project_root, "current_compiler_engine", "code_compiler_engine"),
        os.path.join(project_root, "..", "current_compiler_engine", "code_compiler_engine"),
        # Historische Paketlayouts bleiben als Rueckfallpfade erhalten.
        os.path.join(project_root, "..", "..", "_compiler", "current_compiler_engine", "code_compiler_engine"),
        os.path.join(project_root, "..", "..", "..", "_compiler", "current_compiler_engine", "code_compiler_engine"),
    )

    for candidate in conventional:
        if os.path.isdir(candidate):
            _add(candidate)

    return tuple(candidates)


def build_config_source_runtime_settings(app_config, project_root, contract_registry=None):
    registry_settings = contract_registry if is_mapping(contract_registry) else build_contract_registry_runtime_settings(
        app_config=app_config,
        project_root=project_root,
    )
    process_states_contract_id = cfg_str(
        app_cfg_get(
            app_config,
            "config_source.process_states_mapping_contract_id",
            DEFAULT_PROCESS_STATES_MAPPING_CONTRACT_ID,
        ),
        DEFAULT_PROCESS_STATES_MAPPING_CONTRACT_ID,
    ).strip() or DEFAULT_PROCESS_STATES_MAPPING_CONTRACT_ID
    control_methods_contract_id = cfg_str(
        app_cfg_get(
            app_config,
            "config_source.control_methods_mapping_contract_id",
            DEFAULT_CONTROL_METHODS_MAPPING_CONTRACT_ID,
        ),
        DEFAULT_CONTROL_METHODS_MAPPING_CONTRACT_ID,
    ).strip() or DEFAULT_CONTROL_METHODS_MAPPING_CONTRACT_ID

    process_states_contract = _resolve_runtime_contract_path_from_registry(
        registry_settings,
        process_states_contract_id,
        path_kind="runtime_instance",
    )
    control_methods_contract = _resolve_runtime_contract_path_from_registry(
        registry_settings,
        control_methods_contract_id,
        path_kind="runtime_instance",
    )

    if is_mapping(registry_settings):
        resolved_contracts = registry_settings.get("resolved_contracts")
        if is_mapping(resolved_contracts):
            resolved_contracts[process_states_contract_id] = copy.deepcopy(process_states_contract)
            resolved_contracts[control_methods_contract_id] = copy.deepcopy(control_methods_contract)

    return {
        "mode": cfg_str(app_cfg_get(app_config, "config_source.mode", "legacy_json"), "legacy_json"),
        "contract_registry_path": registry_settings.get("registry_path"),
        "contract_registry_loaded": registry_settings.get("loaded"),
        "legacy_json_path": resolve_relative_path(
            project_root,
            app_cfg_get(app_config, "config_source.legacy_json_path", "config/___device_state_data.json"),
        ),
        "cud_source_dir": resolve_relative_path(
            project_root,
            app_cfg_get(app_config, "config_source.cud_source_dir", "config/cud/"),
        ),
        "compiled_cache_dir": resolve_relative_path(
            project_root,
            app_cfg_get(app_config, "config_source.compiled_cache_dir", "config/compiled/"),
        ),
        "runtime_compile_enabled": cfg_bool(
            app_cfg_get(app_config, "config_source.runtime_compile_enabled", False),
            False,
        ),
        "reverse_compile_enabled": cfg_bool(
            app_cfg_get(app_config, "config_source.reverse_compile_enabled", False),
            False,
        ),
        "reverse_compile_output_dir": resolve_relative_path(
            project_root,
            app_cfg_get(app_config, "config_source.reverse_compile_output_dir", "config/decompiled/"),
        ),
        "runtime_change_capture_enabled": cfg_bool(
            app_cfg_get(app_config, "config_source.runtime_change_capture_enabled", False),
            False,
        ),
        "runtime_change_capture_dir": resolve_relative_path(
            project_root,
            app_cfg_get(app_config, "config_source.runtime_change_capture_dir", "config/runtime_change_sets/"),
        ),
        "next_legacy_projection_candidate": cfg_str(
            app_cfg_get(app_config, "config_source.next_legacy_projection_candidate", "process_states"),
            "process_states",
        ),
        "process_states_mapping_contract_id": process_states_contract_id,
        "process_states_mapping_contract_path": process_states_contract.get("resolved_path"),
        "process_states_mapping_contract": process_states_contract,
        "process_states_live_projection_enabled": cfg_bool(
            app_cfg_get(app_config, "config_source.process_states_live_projection_enabled", False),
            False,
        ),
        "process_states_live_projection_mode": cfg_str(
            app_cfg_get(app_config, "config_source.process_states_live_projection_mode", "disabled"),
            "disabled",
        ),
        "process_states_live_projection_apply_limit": cfg_int(
            app_cfg_get(app_config, "config_source.process_states_live_projection_apply_limit", 0),
            0,
        ),
        "process_states_live_projection_allow_review_required": cfg_bool(
            app_cfg_get(app_config, "config_source.process_states_live_projection_allow_review_required", False),
            False,
        ),
        # v16/v31: control_methods als nächster Legacy-Sektor; Pfad aus contract_registry.yaml.
        "control_methods_mapping_contract_id": control_methods_contract_id,
        "control_methods_mapping_contract_path": control_methods_contract.get("resolved_path"),
        "control_methods_mapping_contract": control_methods_contract,
        "control_methods_live_projection_enabled": cfg_bool(
            app_cfg_get(app_config, "config_source.control_methods_live_projection_enabled", False),
            False,
        ),
        "control_methods_live_projection_mode": cfg_str(
            app_cfg_get(app_config, "config_source.control_methods_live_projection_mode", "disabled"),
            "disabled",
        ),
        "control_methods_live_projection_apply_limit": cfg_int(
            app_cfg_get(app_config, "config_source.control_methods_live_projection_apply_limit", 0),
            0,
        ),
        "control_methods_live_projection_allow_review_required": cfg_bool(
            app_cfg_get(app_config, "config_source.control_methods_live_projection_allow_review_required", False),
            False,
        ),
        # v18: Event-Envelope-Enrichment im Broker (default disabled)
        "event_envelope_enrichment_enabled": cfg_bool(
            app_cfg_get(app_config, "config_source.event_envelope_enrichment_enabled", False),
            False,
        ),
        "event_envelope_source_ref": cfg_str(
            app_cfg_get(app_config, "config_source.event_envelope_source_ref", "sync_event_router"),
            "sync_event_router",
        ),
        "event_envelope_node_id": cfg_str(
            app_cfg_get(app_config, "config_source.event_envelope_node_id", "node_main"),
            "node_main",
        ),
        "compiler_python_paths": _collect_compiler_python_paths(app_config, project_root),
        "project_root": project_root,
    }


def build_worker_gateway_runtime_settings(app_config, project_root, mqtt_client=None):
    if not isinstance(mqtt_client, dict):
        mqtt_client = {}

    topic_root_override = app_cfg_get(app_config, "worker_gateway.topic_root_override", None)
    topic_root_override = cfg_str(topic_root_override, "").strip()
    if not topic_root_override:
        topic_root_override = None

    topic_root_effective = topic_root_override
    if topic_root_effective is None:
        topic_root_effective = mqtt_client.get("topic_root") or cfg_str(
            app_cfg_get(app_config, "network.mqtt_client.topic_root", "xserver/v1"),
            "xserver/v1",
        )

    return {
        "enabled": cfg_bool(app_cfg_get(app_config, "worker_gateway.enabled", False), False),
        "loop_timeout_s": cfg_float(app_cfg_get(app_config, "worker_gateway.loop_timeout_s", 0.25), 0.25),
        "mqtt_out_put_timeout_s": cfg_float(app_cfg_get(app_config, "worker_gateway.mqtt_out_put_timeout_s", 0.5), 0.5),
        "max_payload_bytes": cfg_int(app_cfg_get(app_config, "worker_gateway.max_payload_bytes", 262144), 262144),
        "drop_unmapped_event_types": cfg_bool(app_cfg_get(app_config, "worker_gateway.drop_unmapped_event_types", True), True),
        "default_qos": cfg_int(app_cfg_get(app_config, "worker_gateway.default_qos", 1), 1),
        "default_retain": cfg_bool(app_cfg_get(app_config, "worker_gateway.default_retain", False), False),
        "topic_root_override": topic_root_override,
        "topic_root_effective": topic_root_effective,
        "acl_enforcement": cfg_bool(app_cfg_get(app_config, "worker_gateway.acl_enforcement", True), True),
        "acl_default_policy": cfg_str(app_cfg_get(app_config, "worker_gateway.acl_default_policy", "deny"), "deny"),
        "dlq_enabled": cfg_bool(app_cfg_get(app_config, "worker_gateway.dlq_enabled", True), True),
        "dlq_max_size": cfg_int(app_cfg_get(app_config, "worker_gateway.dlq_max_size", 1000), 1000),
        "metrics_log_interval_s": cfg_float(app_cfg_get(app_config, "worker_gateway.metrics_log_interval_s", 60.0), 60.0),
    }


def build_proxy_worker_bridge_runtime_settings(app_config, project_root, mqtt_client=None):
    """Build normalized settings for the v35.1 Proxy Worker Bridge.

    The bridge is intentionally optional. It uses the already configured
    network.mqtt_client defaults unless explicit proxy_worker_bridge values are
    set. TLS paths are resolved relative to the product root.
    """
    if not isinstance(mqtt_client, dict):
        mqtt_client = {}

    enabled = cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.enabled", False), False)
    worker_id = cfg_str(
        app_cfg_get(app_config, "proxy_worker_bridge.worker_id", app_cfg_get(app_config, "app.node_id", "worker_fn_01")),
        "worker_fn_01",
    )
    broker_host = cfg_str(
        app_cfg_get(app_config, "proxy_worker_bridge.broker_host", mqtt_client.get("broker_host") or app_cfg_get(app_config, "network.mqtt_client.broker_ip", "127.0.0.1")),
        "127.0.0.1",
    )
    broker_port = cfg_int(
        app_cfg_get(app_config, "proxy_worker_bridge.broker_port", mqtt_client.get("broker_port") or app_cfg_get(app_config, "network.mqtt_client.broker_port", 1883)),
        1883,
    )
    secure_port = cfg_int(app_cfg_get(app_config, "proxy_worker_bridge.secure_port", 8883), 8883)
    use_mqtts = cfg_bool(
        app_cfg_get(app_config, "proxy_worker_bridge.use_mqtts", mqtt_client.get("ssl_enabled", False)),
        False,
    )

    bridge_ca_value = app_cfg_get(app_config, "proxy_worker_bridge.ca_cert_file", _MISSING)
    if bridge_ca_value is _MISSING:
        if cfg_bool(mqtt_client.get("ssl_enabled", False), False):
            bridge_ca_value = mqtt_client.get("ssl_cafile") or app_cfg_get(app_config, "network.mqtt_client.ssl.cafile", None)
        else:
            bridge_ca_value = None

    # mTLS is intentionally optional. Do not inherit the generic MQTT client
    # cert/key defaults automatically because they often point to placeholder
    # files. The Proxy Worker Bridge only uses a client certificate when it is
    # explicitly configured under proxy_worker_bridge.*.
    bridge_client_cert_value = app_cfg_get(app_config, "proxy_worker_bridge.client_cert_file", None)
    bridge_client_key_value = app_cfg_get(app_config, "proxy_worker_bridge.client_key_file", None)

    ca_cert_file = resolve_optional_path(project_root, bridge_ca_value)
    client_cert_file = resolve_optional_path(project_root, bridge_client_cert_value)
    client_key_file = resolve_optional_path(project_root, bridge_client_key_value)

    tls_ciphers = cfg_str(app_cfg_get(app_config, "proxy_worker_bridge.tls_ciphers", None), "").strip()
    if tls_ciphers.lower() in ("", "none", "null", "nil", "false"):
        tls_ciphers = None

    return {
        "enabled": enabled,
        "contract_id": cfg_str(app_cfg_get(app_config, "proxy_worker_bridge.contract_id", "C27"), "C27"),
        "worker_id": worker_id,
        "broker_host": broker_host,
        "broker_port": broker_port,
        "secure_port": secure_port,
        "effective_port": secure_port if use_mqtts else broker_port,
        "use_mqtts": use_mqtts,
        "ca_cert_file": ca_cert_file,
        "client_cert_file": client_cert_file,
        "client_key_file": client_key_file,
        "tls_ciphers": tls_ciphers,
        "tls_allow_insecure": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.tls_allow_insecure", False), False),
        "check_hostname": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.check_hostname", True), True),
        "authentication_required": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.authentication_required", False), False),
        "username": app_cfg_get(app_config, "proxy_worker_bridge.username", mqtt_client.get("username")),
        "password": app_cfg_get(app_config, "proxy_worker_bridge.password", mqtt_client.get("password")),
        "username_env": app_cfg_get(app_config, "proxy_worker_bridge.username_env", None),
        "password_env": app_cfg_get(app_config, "proxy_worker_bridge.password_env", None),
        "client_id": cfg_str(app_cfg_get(app_config, "proxy_worker_bridge.client_id", "v35-1-worker-%s" % worker_id), "v35-1-worker-%s" % worker_id),
        "mtls_mode": normalize_mtls_mode(app_cfg_get(app_config, "proxy_worker_bridge.mtls_mode", "optional"), "optional"),
        "certificate_rotation_enabled": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.certificate_rotation_enabled", False), False),
        "certificate_renew_before_days": cfg_int(app_cfg_get(app_config, "proxy_worker_bridge.certificate_renew_before_days", 30), 30),
        "topic_acl_enforcement": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.topic_acl_enforcement", True), True),
        "topic_acl_default_policy": normalize_acl_policy(app_cfg_get(app_config, "proxy_worker_bridge.topic_acl_default_policy", "deny"), "deny"),
        "allowed_publish_topics": normalize_config_text_sequence(app_cfg_get(app_config, "proxy_worker_bridge.allowed_publish_topics", (
            "worker/{worker_id}/reply/#",
            "worker/{worker_id}/presence",
            "worker/{worker_id}/snapshot/#",
            "worker/{worker_id}/event/#",
        ))),
        "allowed_subscribe_topics": normalize_config_text_sequence(app_cfg_get(app_config, "proxy_worker_bridge.allowed_subscribe_topics", (
            "worker/{worker_id}/command/+",
        ))),
        "keepalive": cfg_int(app_cfg_get(app_config, "proxy_worker_bridge.keepalive", 60), 60),
        "command_qos": cfg_int(app_cfg_get(app_config, "proxy_worker_bridge.command_qos", 2), 2),
        "reply_qos": cfg_int(app_cfg_get(app_config, "proxy_worker_bridge.reply_qos", 1), 1),
        "presence_qos": cfg_int(app_cfg_get(app_config, "proxy_worker_bridge.presence_qos", 1), 1),
        "event_qos": cfg_int(app_cfg_get(app_config, "proxy_worker_bridge.event_qos", 1), 1),
        "presence_interval_s": cfg_float(app_cfg_get(app_config, "proxy_worker_bridge.presence_interval_s", 30.0), 30.0),
        "loop_sleep_s": cfg_float(app_cfg_get(app_config, "proxy_worker_bridge.loop_sleep_s", 0.1), 0.1),
        "auto_reply_enabled": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.auto_reply_enabled", True), True),
        "emit_command_events": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.emit_command_events", True), True),
        "snapshot_enabled": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.snapshot_enabled", True), True),
        "runtime_binding_enabled": cfg_bool(app_cfg_get(app_config, "proxy_worker_bridge.runtime_binding_enabled", True), True),
        "runtime_event_target": cfg_str(app_cfg_get(app_config, "proxy_worker_bridge.runtime_event_target", "thread_management"), "thread_management"),
        "runtime_state_resource": cfg_str(app_cfg_get(app_config, "proxy_worker_bridge.runtime_state_resource", "proxy_runtime_command_state"), "proxy_runtime_command_state"),
        "runtime_command_binding_version": cfg_str(app_cfg_get(app_config, "proxy_worker_bridge.runtime_command_binding_version", "v35_1_preproduction_final_runtime"), "v35_1_preproduction_final_runtime"),
        "max_payload_bytes": cfg_int(app_cfg_get(app_config, "proxy_worker_bridge.max_payload_bytes", 262144), 262144),
    }

def _read_master_sync_enabled(app_config):
    direct_value = app_cfg_get(app_config, "state_event_management.master_sync_enabled", _MISSING)
    if direct_value is not _MISSING:
        return cfg_bool(direct_value, True)

    env_value = app_cfg_get(app_config, "env_overrides.values.SEM_MASTER_SYNC_ENABLED", _MISSING)
    if env_value is not _MISSING:
        return cfg_bool(env_value, True)

    return True


def build_runtime_settings(app_config, project_root, base_dir=None):
    """Build a normalized runtime settings bundle from the effective config."""
    if base_dir is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    node_id = cfg_str(app_cfg_get(app_config, "app.node_id", "fn-01"), "fn-01")
    local_mode = cfg_bool(app_cfg_get(app_config, "network.local_mode", False), False)
    protocol = cfg_str(app_cfg_get(app_config, "network.master.protocol", "mqtt"), "mqtt")
    master_ip = cfg_str(app_cfg_get(app_config, "network.master.ip", "127.0.0.1"), "127.0.0.1")
    use_ipv6 = cfg_bool(app_cfg_get(app_config, "network.master.use_ipv6", False), False)
    master_port = cfg_int(
        app_cfg_get(
            app_config,
            "network.master.port",
            app_cfg_get(app_config, "network.mqtt_client.broker_port", 1883),
        ),
        1883,
    )
    local_ip_override = cfg_str(app_cfg_get(app_config, "network.master.local_ip_override", "localhost"), "localhost")
    host_for_master_uri = local_ip_override if local_mode else master_ip

    device_state_paths = select_device_state_json_path(app_config, project_root)
    hot_reload_enabled = cfg_bool(app_cfg_get(app_config, "runtime.hot_reload.enabled", False), False)
    hot_reload_poll_interval_s = cfg_float(
        app_cfg_get(app_config, "runtime.hot_reload.poll_interval_s", DEFAULT_HOT_RELOAD_POLL_INTERVAL_S),
        DEFAULT_HOT_RELOAD_POLL_INTERVAL_S,
    )
    hot_reload_mutable_paths = app_cfg_get(
        app_config,
        "runtime.hot_reload.mutable_paths",
        DEFAULT_HOT_RELOAD_MUTABLE_PATHS,
    )
    if not isinstance(hot_reload_mutable_paths, (list, tuple)):
        hot_reload_mutable_paths = DEFAULT_HOT_RELOAD_MUTABLE_PATHS

    pycache_root_dir = cfg_str(app_cfg_get(app_config, "python.pycache.root_dir", "/tmp"), "/tmp")
    pycache_dir_name = cfg_str(app_cfg_get(app_config, "python.pycache.dir_name", "sync_xserver_pycache"), "sync_xserver_pycache")
    pycache_prefix_path = resolve_relative_path(project_root, os.path.join(pycache_root_dir, pycache_dir_name)) if not os.path.isabs(pycache_root_dir) else os.path.normpath(os.path.join(pycache_root_dir, pycache_dir_name))

    mqtt_client = build_mqtt_client_runtime_settings(app_config, project_root)
    contract_registry = build_contract_registry_runtime_settings(app_config, project_root)
    config_source = build_config_source_runtime_settings(
        app_config,
        project_root,
        contract_registry=contract_registry,
    )
    worker_gateway = build_worker_gateway_runtime_settings(app_config, project_root, mqtt_client=mqtt_client)
    proxy_worker_bridge = build_proxy_worker_bridge_runtime_settings(app_config, project_root, mqtt_client=mqtt_client)
    security = build_security_runtime_settings(app_config, project_root, proxy_worker_bridge=proxy_worker_bridge)
    monitoring = build_monitoring_runtime_settings(app_config, project_root)
    fieldbus = build_fieldbus_runtime_settings(app_config, project_root)
    effective_scheme = "mqtts" if mqtt_client.get("ssl_enabled") else protocol
    master_uri = build_master_uri(
        protocol=effective_scheme,
        host=host_for_master_uri,
        port=master_port,
        use_ipv6=use_ipv6,
    )

    cleanup_roots = app_cfg_get(app_config, "python.pycache.cleanup_roots", ("src",))
    if not isinstance(cleanup_roots, (list, tuple)):
        cleanup_roots = ("src",)

    return {
        "node_id": node_id,
        "show_ascii": cfg_bool(app_cfg_get(app_config, "app.show_ascii", True), True),
        "executor": {
            "max_workers": cfg_int(app_cfg_get(app_config, "executor.max_workers", 40), 40),
        },
        "network": {
            "local_mode": local_mode,
            "master_ip": master_ip,
            "protocol": protocol,
            "use_ipv6": use_ipv6,
            "master_port": master_port,
            "local_ip_override": local_ip_override,
            "master_uri": master_uri,
        },
        "mqtt_client": mqtt_client,
        "contract_registry": contract_registry,
        "config_source": config_source,
        "worker_gateway": worker_gateway,
        "proxy_worker_bridge": proxy_worker_bridge,
        "security": security,
        "monitoring": monitoring,
        "fieldbus": fieldbus,
        "worker_state": {
            "run_time_mode": cfg_str(app_cfg_get(app_config, "worker_state.run_time_mode", ""), ""),
            "persist_data": cfg_bool(app_cfg_get(app_config, "worker_state.persist_data", True), True),
            "master_sync_enabled": _read_master_sync_enabled(app_config),
        },
        "datastore": {
            "bypass_device_state_load": cfg_bool(app_cfg_get(app_config, "datastore.bypass_device_state_load", True), True),
            "device_state_load_enabled": cfg_bool(app_cfg_get(app_config, "datastore.device_state_load_enabled", False), False),
            "device_state_source": device_state_paths["selected_source"],
            "requested_device_state_source": device_state_paths["requested_source"],
            "selected_json_path": device_state_paths["selected_json_path"],
            "main_json_path": device_state_paths["main_json_path"],
            "test_json_path": device_state_paths["test_json_path"],
            "start_datastore_runtime_thread_in_local_mode": cfg_bool(
                app_cfg_get(app_config, "datastore.start_datastore_runtime_thread_in_local_mode", True),
                True,
            ),
        },
        "thread_management": {
            "num_automation": cfg_int(app_cfg_get(app_config, "thread_management.num_automation", 2), 2),
        },
        "cpu_probe": {
            "enabled": cfg_bool(app_cfg_get(app_config, "cpu_probe.enabled", True), True),
            "interval_s": cfg_float(app_cfg_get(app_config, "cpu_probe.interval_s", 1.0), 1.0),
            "top_n": cfg_int(app_cfg_get(app_config, "cpu_probe.top_n", 12), 12),
            "min_pct": cfg_float(app_cfg_get(app_config, "cpu_probe.min_pct", 0.1), 0.1),
            "history_max": cfg_int(app_cfg_get(app_config, "cpu_probe.history_max", 300), 300),
            "log_top": cfg_bool(app_cfg_get(app_config, "cpu_probe.log_top", True), True),
        },
        "system": {
            "init_sleep_s": cfg_float(app_cfg_get(app_config, "system.init_sleep_s", 1.0), 1.0),
            "log_level_test": cfg_bool(app_cfg_get(app_config, "system.log_level_test", False), False),
        },
        "timing": {
            "main_loop_sleep_s": cfg_float(app_cfg_get(app_config, "timing.main_loop_sleep_s", 5.0), 5.0),
            "start_delays_s": {
                "after_datastore": cfg_float(app_cfg_get(app_config, "timing.start_delays_s.after_datastore", 2.4), 2.4),
                "after_event_broker_1": cfg_float(app_cfg_get(app_config, "timing.start_delays_s.after_event_broker_1", 0.0), 0.0),
                "after_event_broker_2": cfg_float(app_cfg_get(app_config, "timing.start_delays_s.after_event_broker_2", 0.0), 0.0),
                "after_thread_management": cfg_float(app_cfg_get(app_config, "timing.start_delays_s.after_thread_management", 1.0), 1.0),
                "after_state_event_management": cfg_float(app_cfg_get(app_config, "timing.start_delays_s.after_state_event_management", 1.0), 1.0),
                "after_transport_service": cfg_float(app_cfg_get(app_config, "timing.start_delays_s.after_transport_service", 1.0), 1.0),
            },
        },
        "worker_runtime": {
            "config_chunking_enabled": cfg_bool(
                app_cfg_get(app_config, "worker_runtime.config_chunking_enabled", False),
                False,
            ),
            "gre_integration_enabled": cfg_bool(
                app_cfg_get(app_config, "worker_runtime.gre_integration_enabled", False),
                False,
            ),
            "writer_sync_enabled": cfg_bool(
                app_cfg_get(app_config, "worker_runtime.writer_sync_enabled", False),
                False,
            ),
        },
        "shutdown": {
            "nudge_queues": {
                "enabled": cfg_bool(app_cfg_get(app_config, "shutdown.nudge_queues.enabled", True), True),
                "repeats": cfg_int(app_cfg_get(app_config, "shutdown.nudge_queues.repeats", 2), 2),
                "put_timeout_s": cfg_float(app_cfg_get(app_config, "shutdown.nudge_queues.put_timeout_s", 0.1), 0.1),
                "stop_payloads": app_cfg_get(
                    app_config,
                    "shutdown.nudge_queues.stop_payloads",
                    [{"_type": "__STOP__", "_sentinel": True}, {"event_type": "__STOP__", "_sentinel": True}, None],
                ),
            },
            "join_timeout_s": cfg_float(app_cfg_get(app_config, "shutdown.join_timeout_s", 5.0), 5.0),
            "enforce_sigkill_timeout_s": cfg_float(app_cfg_get(app_config, "shutdown.enforce_sigkill_timeout_s", 10.0), 10.0),
            "executor_shutdown_wait": cfg_bool(app_cfg_get(app_config, "shutdown.executor_shutdown_wait", True), True),
        },
        "event_router": {
            "broker_threads": cfg_int(app_cfg_get(app_config, "event_router.broker_threads", 2), 2),
            "router_threads_per_broker": cfg_int(app_cfg_get(app_config, "event_router.router_threads_per_broker", 4), 4),
        },
        "hot_reload": {
            "enabled": hot_reload_enabled,
            "poll_interval_s": hot_reload_poll_interval_s,
            "mutable_paths": tuple(str(path) for path in hot_reload_mutable_paths if str(path).strip()),
        },
        "python": {
            "pycache": {
                "enabled": cfg_bool(app_cfg_get(app_config, "python.pycache.enabled", True), True),
                "strategy": cfg_str(app_cfg_get(app_config, "python.pycache.strategy", "prefix"), "prefix"),
                "root_dir": pycache_root_dir,
                "dir_name": pycache_dir_name,
                "prefix_path": pycache_prefix_path,
                "cleanup_project_cache_dirs": cfg_bool(
                    app_cfg_get(app_config, "python.pycache.cleanup_project_cache_dirs", True),
                    True,
                ),
                "cleanup_roots": tuple(str(item) for item in cleanup_roots if str(item).strip()),
            },
        },
    }

def _resolve_module_from_candidates(module_names, logger=None):
    logger_obj = _logger_or_default(logger)

    for module_name in module_names:
        try:
            module_obj = importlib.import_module(module_name)
            return module_obj, module_name
        except ModuleNotFoundError:
            continue
        except Exception as exc:
            logger_obj.debug(
                "[runtime_config] Failed to import %s: %s",
                module_name,
                exc,
            )

    return None, None


def _resolve_attr(module_obj, attr_name, default=None):
    if module_obj is None:
        return default

    try:
        return getattr(module_obj, attr_name)
    except Exception:
        return default


def _no_op(*args, **kwargs):
    return None


def _make_print_startup_ascii_fn(ascii_module):
    if ascii_module is None:
        return _no_op

    print_ascii_art = _resolve_attr(ascii_module, "print_ascii_art", None)
    if not callable(print_ascii_art):
        return _no_op

    def _printer(app_config, logger=None):
        logger_obj = _logger_or_default(logger)

        if not cfg_bool(app_cfg_get(app_config, "app.show_ascii", True), True):
            return

        blank_before = cfg_int(app_cfg_get(app_config, "app.ascii.blank_lines_before_each_art", 3), 3)
        blank_after = cfg_int(app_cfg_get(app_config, "app.ascii.blank_lines_after_each_art", 3), 3)
        sleep_s_default = cfg_float(app_cfg_get(app_config, "app.ascii.sleep_s_default", 2.0), 2.0)

        arts = app_cfg_get(app_config, "app.ascii.arts", None)
        if not isinstance(arts, list) or not arts:
            arts = [
                {"name": "ASCII_TEXT", "indent": 3, "palette": "CYBER_TEXT_PALETTE"},
                {"name": "ASCII_CYBER_PUNK", "indent": 3, "palette": "CYBERPALM_PALETTE"},
                {"name": "ASCII_CTRL", "indent": 3, "palette": None},
                {"name": "ASCII_PENGUIN", "indent": 9, "palette": None},
                {"name": "METRICS_LEGENDE", "indent": 3, "palette": None},
            ]

        for art_cfg in arts:
            if not is_mapping(art_cfg):
                continue

            art_name = cfg_str(art_cfg.get("name", ""), "").strip()
            if not art_name:
                continue

            ascii_string = _resolve_attr(ascii_module, art_name, None)
            if ascii_string is None:
                logger_obj.warning(
                    "[runtime_config] ASCII art not found: %s",
                    art_name,
                )
                continue

            indent = cfg_int(art_cfg.get("indent", 3), 3)
            palette_name = art_cfg.get("palette", None)
            palette_value = _resolve_attr(ascii_module, palette_name, None) if palette_name else None

            print("\n" * max(blank_before, 0), end="")

            if palette_value is not None:
                print_ascii_art(ascii_string, indent=indent, rgb=palette_value)
            else:
                print_ascii_art(ascii_string, indent=indent)

            print("\n" * max(blank_after, 0), end="")
            time.sleep(sleep_s_default)

    return _printer


def resolve_runtime_interfaces(logger=None):
    """Resolve startup/runtime helper interfaces from their canonical homes."""
    logger_obj = _logger_or_default(logger)

    system_init_module, system_init_source = _resolve_module_from_candidates(
        ("src.init._system_init",),
        logger=logger_obj,
    )
    ascii_module, ascii_source = _resolve_module_from_candidates(
        ("src.init._ascii_plotter",),
        logger=logger_obj,
    )
    metrics_module, metrics_source = _resolve_module_from_candidates(
        ("src.libraries._metrics_system",),
        logger=logger_obj,
    )

    return {
        "sources": {
            "system_init": system_init_source,
            "ascii_plotter": ascii_source,
            "metrics_system": metrics_source,
        },
        "configure_logging_from_app_config": _resolve_attr(system_init_module, "configure_logging_from_app_config", _no_op),
        "initialize_system": _resolve_attr(system_init_module, "initialize_system", _no_op),
        "cleanup_system": _resolve_attr(system_init_module, "cleanup_system", _no_op),
        "register_system_state": _resolve_attr(system_init_module, "register_system_state", _no_op),
        "assert_free_threading_or_exit": _resolve_attr(system_init_module, "assert_free_threading_or_exit", _no_op),
        "cleanup_project_pycache_dirs": _resolve_attr(system_init_module, "cleanup_project_pycache_dirs", _no_op),
        "log_level_test": _resolve_attr(system_init_module, "log_level_test", _no_op),
        "queue_register": _resolve_attr(metrics_module, "queue_register", None),
        "print_startup_ascii": _make_print_startup_ascii_fn(ascii_module),
    }

def _resolve_log_level_name(value, default=logging.INFO):
    if isinstance(value, int):
        return value

    if isinstance(value, str):
        level = getattr(logging, value.strip().upper(), None)
        if isinstance(level, int):
            return level

    return default


def apply_live_logging_update(bundle, logger=None):
    """
    Apply simple live level updates to already-created handlers.
    """
    logger_obj = _logger_or_default(logger)
    app_config = bundle.get("app_config", {})

    system_logs = app_cfg_get(app_config, "logging.system_logs", {})
    if not is_mapping(system_logs):
        return False

    levels_cfg = system_logs.get("levels")
    if is_mapping(levels_cfg):
        console_level = _resolve_log_level_name(levels_cfg.get("console"), logging.INFO)
        file_level = _resolve_log_level_name(levels_cfg.get("file"), logging.DEBUG)
    else:
        single_level = system_logs.get("level", None)
        console_level = _resolve_log_level_name(single_level, logging.INFO)
        file_level = _resolve_log_level_name(single_level, logging.DEBUG)

    root_logger = logging.getLogger()
    try:
        root_logger.setLevel(min(console_level, file_level))
    except Exception:
        pass

    for handler in list(root_logger.handlers):
        try:
            if isinstance(handler, logging.FileHandler):
                handler.setLevel(file_level)
            else:
                handler.setLevel(console_level)
        except Exception:
            continue

    logger_obj.info(
        "[runtime_config] Applied live logging levels: console=%s file=%s",
        logging.getLevelName(console_level),
        logging.getLevelName(file_level),
    )
    return True


def apply_env_exports(app_config, project_root, logger=None):
    """Export selected runtime values to environment variables for legacy-compatible components."""
    logger_obj = _logger_or_default(logger)

    try:
        os.environ["PROJECT_ROOT"] = str(project_root)
    except Exception:
        pass

    env_export_cfg = app_cfg_get(app_config, "network.env_export", {})
    if is_mapping(env_export_cfg) and cfg_bool(env_export_cfg.get("enabled", False), False):
        override_existing = cfg_bool(env_export_cfg.get("override_existing_env", True), True)
        mapping = env_export_cfg.get("mapping", {})
        if not is_mapping(mapping):
            mapping = {}

        mqtt_ssl_enabled = cfg_bool(app_cfg_get(app_config, "network.mqtt_client.ssl.enabled", False), False)
        value_map = {
            "local_mode": "1" if cfg_bool(app_cfg_get(app_config, "network.local_mode", False), False) else "0",
            "master_ip": cfg_str(app_cfg_get(app_config, "network.master.ip", "127.0.0.1"), "127.0.0.1"),
            "master_port": str(cfg_int(app_cfg_get(app_config, "network.master.port", 1883), 1883)),
            "broker_ip": cfg_str(app_cfg_get(app_config, "network.mqtt_client.broker_ip", "127.0.0.1"), "127.0.0.1"),
            "broker_port": str(cfg_int(app_cfg_get(app_config, "network.mqtt_client.broker_port", 8883 if mqtt_ssl_enabled else 1883), 1883)),
            "protocol": cfg_str(app_cfg_get(app_config, "network.master.protocol", "mqtt"), "mqtt"),
            "ipv6": "1" if cfg_bool(app_cfg_get(app_config, "network.master.use_ipv6", False), False) else "0",
        }

        for logical_name, env_name in mapping.items():
            if logical_name not in value_map:
                continue
            if not env_name:
                continue
            if not override_existing and env_name in os.environ:
                continue
            os.environ[str(env_name)] = value_map[logical_name]

    env_overrides_cfg = app_cfg_get(app_config, "env_overrides", {})
    if is_mapping(env_overrides_cfg) and cfg_bool(env_overrides_cfg.get("enabled", False), False):
        override_existing = cfg_bool(env_overrides_cfg.get("override_existing_env", False), False)
        values = env_overrides_cfg.get("values", {})
        if not is_mapping(values):
            values = {}

        for env_name, env_value in values.items():
            if not env_name:
                continue
            if not override_existing and env_name in os.environ:
                continue
            os.environ[str(env_name)] = "" if env_value is None else str(env_value)

    device_state_info = select_device_state_json_path(app_config, project_root)
    selected_json_path = device_state_info.get("selected_json_path")
    if selected_json_path:
        os.environ["CONFIG_JSON_PATH"] = str(selected_json_path)

    logger_obj.debug("[runtime_config] Environment export applied")

def create_runtime_config_store(
    config_meta,
    app_config,
    settings,
    mutable_paths=None,
):
    """
    Create a thread-safe config store for runtime-owned configuration.
    """
    if mutable_paths is None:
        mutable_paths = settings.get("hot_reload", {}).get("mutable_paths", DEFAULT_HOT_RELOAD_MUTABLE_PATHS)

    config_file = config_meta.get("config_file")
    last_mtime_ns = None

    if config_file and os.path.exists(config_file):
        try:
            last_mtime_ns = os.stat(config_file).st_mtime_ns
        except Exception:
            last_mtime_ns = None

    return {
        "lock": threading.RLock(),
        "config_file": config_file,
        "project_root": config_meta.get("project_root"),
        "profile": config_meta.get("profile", "base"),
        "anchor_file": config_meta.get("anchor_file"),
        "effective_config": copy.deepcopy(app_config),
        "settings": copy.deepcopy(settings),
        "revision": 1,
        "last_mtime_ns": last_mtime_ns,
        "last_reload_check_ts": 0.0,
        "hot_reload_enabled": cfg_bool(settings.get("hot_reload", {}).get("enabled", False), False),
        "hot_reload_poll_interval_s": cfg_float(
            settings.get("hot_reload", {}).get("poll_interval_s", DEFAULT_HOT_RELOAD_POLL_INTERVAL_S),
            DEFAULT_HOT_RELOAD_POLL_INTERVAL_S,
        ),
        "mutable_paths": tuple(str(path) for path in mutable_paths if str(path).strip()),
    }


def runtime_cfg_get(bundle_or_store_or_cfg, dotted_path, default=None):
    """
    Read a dotted-path value from bundle, config store or raw config mapping.
    """
    if is_mapping(bundle_or_store_or_cfg):
        if "effective_config" in bundle_or_store_or_cfg and "lock" in bundle_or_store_or_cfg:
            with bundle_or_store_or_cfg["lock"]:
                return app_cfg_get(bundle_or_store_or_cfg.get("effective_config", {}), dotted_path, default)

        if "config_store" in bundle_or_store_or_cfg:
            return runtime_cfg_get(bundle_or_store_or_cfg.get("config_store"), dotted_path, default)

    return app_cfg_get(bundle_or_store_or_cfg, dotted_path, default)


def runtime_cfg_sleep(bundle_or_store_or_cfg, dotted_path, default_seconds):
    """
    Sleep using a runtime-configured dotted path.
    """
    seconds = cfg_float(runtime_cfg_get(bundle_or_store_or_cfg, dotted_path, default_seconds), default_seconds)
    time.sleep(seconds)


def build_runtime_bundle(
    load_enabled=True,
    config_path=DEFAULT_APP_CONFIG_PATH,
    active_profile=None,
    anchor_file=None,
    logger=None,
):
    """
    Build the full runtime bundle consumed by the refactored main entrypoint.
    """
    logger_obj = _logger_or_default(logger)
    anchor_value = anchor_file or __file__
    base_dir = os.path.dirname(os.path.abspath(anchor_value))

    config_meta = load_effective_app_config(
        load_enabled=load_enabled,
        config_path=config_path,
        active_profile=active_profile,
        anchor_file=anchor_value,
        logger=logger_obj,
    )

    normalized_app_config = _normalize_logging_shape(config_meta.get("config", {}))
    project_root = config_meta.get("project_root") or resolve_project_root(anchor_value)
    settings = build_runtime_settings(
        app_config=normalized_app_config,
        project_root=project_root,
        base_dir=base_dir,
    )
    interfaces = resolve_runtime_interfaces(logger=logger_obj)

    apply_env_exports(
        app_config=normalized_app_config,
        project_root=project_root,
        logger=logger_obj,
    )

    config_store = create_runtime_config_store(
        config_meta=config_meta,
        app_config=normalized_app_config,
        settings=settings,
        mutable_paths=settings.get("hot_reload", {}).get("mutable_paths", DEFAULT_HOT_RELOAD_MUTABLE_PATHS),
    )

    return {
        "config_meta": config_meta,
        "app_config": normalized_app_config,
        "settings": settings,
        "interfaces": interfaces,
        "config_store": config_store,
        "base_dir": base_dir,
        "project_root": project_root,
    }


def initialize_runtime_system(bundle, logger=None, hard_free_threading=True):
    """Initialize logging/system services through resolved runtime interfaces."""
    logger_obj = _logger_or_default(logger)
    interfaces = bundle.get("interfaces", {})
    app_config = bundle.get("app_config", {})
    settings = bundle.get("settings", {})

    configure_logging_from_app_config = interfaces.get("configure_logging_from_app_config", _no_op)
    initialize_system = interfaces.get("initialize_system", _no_op)
    assert_free_threading_or_exit = interfaces.get("assert_free_threading_or_exit", _no_op)
    cleanup_project_pycache_dirs = interfaces.get("cleanup_project_pycache_dirs", _no_op)

    try:
        configure_logging_from_app_config(app_config, logger_instance=logger_obj)
    except Exception as exc:
        logger_obj.debug("[runtime_config] Logging pre-configuration failed: %s", exc)

    initialize_system()

    pycache_cfg = settings.get("python", {}).get("pycache", {})
    if cfg_bool(pycache_cfg.get("cleanup_project_cache_dirs", True), True):
        try:
            cleanup_result = cleanup_project_pycache_dirs(
                project_root=bundle.get("project_root"),
                include_root_names=pycache_cfg.get("cleanup_roots", ("src",)),
            )
            logger_obj.info(
                "[runtime_config] Project __pycache__ cleanup: dirs=%d files=%d errors=%d",
                cleanup_result.get("deleted_dir_count", 0),
                cleanup_result.get("deleted_file_count", 0),
                cleanup_result.get("error_count", 0),
            )
        except Exception as exc:
            logger_obj.warning("[runtime_config] Project __pycache__ cleanup failed: %s", exc)

    assert_free_threading_or_exit(logger_obj, hard=bool(hard_free_threading))

def emit_startup_ascii(bundle, logger=None):
    """
    Emit startup ASCII art using the resolved runtime interface.
    """
    logger_obj = _logger_or_default(logger)
    printer = bundle.get("interfaces", {}).get("print_startup_ascii", _no_op)
    app_config = bundle.get("app_config", {})
    try:
        printer(app_config, logger=logger_obj)
    except Exception as exc:
        logger_obj.warning("[runtime_config] Startup ASCII failed: %s", exc)


def log_runtime_summary(bundle, logger=None):
    """Log a concise runtime summary."""
    logger_obj = _logger_or_default(logger)
    settings = bundle.get("settings", {})
    network = settings.get("network", {})
    datastore = settings.get("datastore", {})
    hot_reload = settings.get("hot_reload", {})
    mqtt_client = settings.get("mqtt_client", {})

    logger_obj.info(
        "[runtime] node_id=%s local_mode=%s master_uri=%s broker=%s:%s ssl=%s mqtt_service_enabled=%s",
        settings.get("node_id"),
        network.get("local_mode"),
        network.get("master_uri"),
        mqtt_client.get("broker_host"),
        mqtt_client.get("broker_port"),
        mqtt_client.get("ssl_enabled"),
        mqtt_client.get("enabled"),
    )
    logger_obj.info(
        "[runtime] datastore_source=%s selected_json=%s hot_reload=%s",
        datastore.get("device_state_source"),
        datastore.get("selected_json_path"),
        hot_reload.get("enabled"),
    )
    logger_obj.info(
        "[runtime] config_source=%s runtime_compile=%s reverse_compile=%s next_legacy=%s pstate_live=%s pstate_mode=%s worker_gateway=%s topic_root=%s proxy_worker_bridge=%s",
        settings.get("config_source", {}).get("mode"),
        settings.get("config_source", {}).get("runtime_compile_enabled"),
        settings.get("config_source", {}).get("reverse_compile_enabled"),
        settings.get("config_source", {}).get("next_legacy_projection_candidate"),
        settings.get("config_source", {}).get("process_states_live_projection_enabled"),
        settings.get("config_source", {}).get("process_states_live_projection_mode"),
        settings.get("worker_gateway", {}).get("enabled"),
        settings.get("worker_gateway", {}).get("topic_root_effective"),
        settings.get("proxy_worker_bridge", {}).get("enabled"),
    )
    logger_obj.info(
        "[runtime] contract_registry=%s loaded=%s process_states=%s control_methods=%s",
        settings.get("contract_registry", {}).get("registry_path"),
        settings.get("contract_registry", {}).get("loaded"),
        settings.get("config_source", {}).get("process_states_mapping_contract_id"),
        settings.get("config_source", {}).get("control_methods_mapping_contract_id"),
    )

def _config_value_changed(old_cfg, new_cfg, dotted_path):
    old_value = app_cfg_get(old_cfg, dotted_path, _MISSING)
    new_value = app_cfg_get(new_cfg, dotted_path, _MISSING)
    return old_value != new_value, old_value, new_value


def _store_stat_mtime_ns(config_file):
    if not config_file or not os.path.exists(config_file):
        return None

    try:
        return os.stat(config_file).st_mtime_ns
    except Exception:
        return None


def _reload_raw_effective_config(store, logger=None):
    logger_obj = _logger_or_default(logger)

    meta = load_effective_app_config(
        load_enabled=True,
        config_path=store.get("config_file"),
        active_profile=store.get("profile"),
        anchor_file=store.get("anchor_file"),
        logger=logger_obj,
    )

    return meta


def _apply_mutable_paths_to_current_config(current_cfg, new_cfg, mutable_paths):
    changed_paths = []

    for dotted_path in mutable_paths:
        changed, old_value, new_value = _config_value_changed(current_cfg, new_cfg, dotted_path)
        if not changed:
            continue

        if new_value is _MISSING:
            delete_dotted_value(current_cfg, dotted_path)
        else:
            set_dotted_value(current_cfg, dotted_path, new_value)

        changed_paths.append({
            "path": dotted_path,
            "old": copy.deepcopy(old_value) if old_value is not _MISSING else None,
            "new": copy.deepcopy(new_value) if new_value is not _MISSING else None,
        })

    return changed_paths


def should_reload_runtime_bundle(bundle, now_ts=None):
    store = bundle.get("config_store", {})

    if now_ts is None:
        now_ts = time.time()

    if not cfg_bool(store.get("hot_reload_enabled", False), False):
        return False

    interval_s = cfg_float(
        store.get("hot_reload_poll_interval_s", DEFAULT_HOT_RELOAD_POLL_INTERVAL_S),
        DEFAULT_HOT_RELOAD_POLL_INTERVAL_S,
    )
    last_check_ts = cfg_float(store.get("last_reload_check_ts", 0.0), 0.0)

    if now_ts - last_check_ts < interval_s:
        return False

    return True


def reload_runtime_bundle(bundle, logger=None):
    """
    Reload only explicitly mutable config paths from the config file.

    This keeps deterministic ownership:
    - coordinator-owned runtime parameters can change
    - bootstrap-sensitive parameters remain start-time only
    """
    logger_obj = _logger_or_default(logger)
    store = bundle.get("config_store", {})

    result = {
        "checked": False,
        "reloaded": False,
        "applied": False,
        "changed_paths": [],
        "reason": "",
    }

    if not should_reload_runtime_bundle(bundle):
        result["reason"] = "poll_interval_not_reached_or_disabled"
        return result

    with store.get("lock", threading.RLock()):
        store["last_reload_check_ts"] = time.time()

        current_mtime_ns = _store_stat_mtime_ns(store.get("config_file"))
        if current_mtime_ns is None:
            result["checked"] = True
            result["reason"] = "config_file_missing"
            return result

        if store.get("last_mtime_ns") == current_mtime_ns:
            result["checked"] = True
            result["reason"] = "mtime_unchanged"
            return result

        reloaded_meta = load_effective_app_config(
            load_enabled=True,
            config_path=store.get("config_file"),
            active_profile=store.get("profile"),
            anchor_file=store.get("anchor_file"),
            logger=logger_obj,
        )

        new_cfg = _normalize_logging_shape(reloaded_meta.get("config", {}))
        mutable_paths = tuple(store.get("mutable_paths", DEFAULT_HOT_RELOAD_MUTABLE_PATHS))
        current_cfg = copy.deepcopy(store.get("effective_config", {}))

        changed_paths = _apply_mutable_paths_to_current_config(
            current_cfg=current_cfg,
            new_cfg=new_cfg,
            mutable_paths=mutable_paths,
        )

        store["last_mtime_ns"] = current_mtime_ns
        result["checked"] = True
        result["reloaded"] = True

        if not changed_paths:
            result["reason"] = "reload_without_mutable_changes"
            return result

        project_root = store.get("project_root")
        new_settings = build_runtime_settings(
            app_config=current_cfg,
            project_root=project_root,
            base_dir=bundle.get("base_dir"),
        )

        store["effective_config"] = current_cfg
        store["settings"] = copy.deepcopy(new_settings)
        store["revision"] = int(store.get("revision", 0)) + 1
        store["hot_reload_enabled"] = cfg_bool(new_settings.get("hot_reload", {}).get("enabled", False), False)
        store["hot_reload_poll_interval_s"] = cfg_float(
            new_settings.get("hot_reload", {}).get("poll_interval_s", DEFAULT_HOT_RELOAD_POLL_INTERVAL_S),
            DEFAULT_HOT_RELOAD_POLL_INTERVAL_S,
        )
        store["mutable_paths"] = tuple(new_settings.get("hot_reload", {}).get("mutable_paths", DEFAULT_HOT_RELOAD_MUTABLE_PATHS))

        bundle["app_config"] = copy.deepcopy(current_cfg)
        bundle["settings"] = copy.deepcopy(new_settings)

        apply_env_exports(
            app_config=current_cfg,
            project_root=project_root,
            logger=logger_obj,
        )

        logging_changed = False
        for entry in changed_paths:
            if entry["path"].startswith("logging.system_logs"):
                logging_changed = True
                break

        if logging_changed:
            apply_live_logging_update(bundle, logger=logger_obj)

        result["applied"] = True
        result["changed_paths"] = changed_paths
        result["reason"] = "mutable_changes_applied"

        if changed_paths:
            logger_obj.info(
                "[runtime_config] Applied live config reload: revision=%d changed=%s",
                store.get("revision"),
                [entry.get("path") for entry in changed_paths],
            )

        return result
