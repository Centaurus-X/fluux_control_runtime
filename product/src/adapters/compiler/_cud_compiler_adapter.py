# -*- coding: utf-8 -*-
# src/adapters/compiler/_cud_compiler_adapter.py
#
# Ruft current_compiler_engine als separate Library auf, projiziert das rohe
# Compile-Ergebnis in einen Runtime-Projection-Bridge-Vertrag und liefert daraus
# einen runtime_json_kernel-kompatiblen Dict-Overlay. Keine Klassen.

import copy
import hashlib
import json
import os
import sys

from src.adapters.compiler._projection_bridge import (
    build_runtime_projection_bundle,
    merge_projection_into_runtime_json_kernel,
)


_BUNDLE_CACHE = {
    "hash": None,
    "projection_bundle": None,
}

_SKIP_NAME_MARKERS = (
    "compile_request",
    "decompile_request",
    "runtime_change_set",
)



def _scan_cud_dir(cud_dir):
    paths = []
    if not os.path.isdir(cud_dir):
        return paths

    for root, _dirs, files in os.walk(cud_dir):
        for name in files:
            if not name.endswith((".yaml", ".yml", ".json")):
                continue
            paths.append(os.path.join(root, name))

    paths.sort()
    return paths



def _hash_inputs(paths):
    hasher = hashlib.sha256()
    for path in paths:
        try:
            with open(path, "rb") as handle:
                hasher.update(path.encode("utf-8"))
                hasher.update(b"\0")
                hasher.update(handle.read())
                hasher.update(b"\0")
        except Exception:
            continue
    return hasher.hexdigest()



def _normalize_path_sequence(value):
    if value is None:
        return []

    if isinstance(value, str):
        items = []
        for part in value.split(os.pathsep):
            text = part.strip()
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



def _resolve_path(base_dir, value):
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    expanded = os.path.expanduser(os.path.expandvars(text))
    if os.path.isabs(expanded):
        return os.path.normpath(expanded)

    if base_dir:
        return os.path.normpath(os.path.join(base_dir, expanded))

    return os.path.normpath(expanded)



def _iter_compiler_python_paths(source_settings):
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

    project_root = source_settings.get("project_root")
    configured = _normalize_path_sequence(source_settings.get("compiler_python_paths"))
    env_paths = _normalize_path_sequence(os.environ.get("CUD_COMPILER_PYTHONPATH"))

    for entry in configured:
        _add(_resolve_path(project_root, entry))

    for entry in env_paths:
        _add(_resolve_path(project_root, entry))

    conventional = []
    if project_root:
        conventional.extend((
            os.path.join(project_root, "vendor", "current_compiler_engine", "code_compiler_engine"),
            os.path.join(project_root, "..", "source_inputs", "extracted", "_compiler", "current_compiler_engine", "code_compiler_engine"),
            os.path.join(project_root, "current_compiler_engine", "code_compiler_engine"),
            os.path.join(project_root, "..", "current_compiler_engine", "code_compiler_engine"),
            # Historische Paketlayouts bleiben als Rueckfallpfade erhalten.
            os.path.join(project_root, "..", "..", "_compiler", "current_compiler_engine", "code_compiler_engine"),
            os.path.join(project_root, "..", "..", "..", "_compiler", "current_compiler_engine", "code_compiler_engine"),
        ))

    try:
        cwd = os.getcwd()
    except Exception:
        cwd = project_root or None

    if cwd:
        conventional.extend((
            os.path.join(cwd, "current_compiler_engine", "code_compiler_engine"),
            os.path.join(cwd, "vendor", "current_compiler_engine", "code_compiler_engine"),
        ))

    for candidate in conventional:
        _add(candidate)

    return candidates



def _ensure_compiler_on_syspath(source_settings, logger):
    # v27: Reihenfolge-erhaltendes Einhaengen in sys.path plus Isolation
    # gegen sys.modules-Leaks zwischen Test-Laeufen bzw. Umgebungen.
    #
    # Vorher (v26): sys.path.insert(0, path) in aufsteigender Schleife kehrte
    # die Prioritaet der Pfadliste um - der letzte (= konventionelle)
    # Pfad landete dadurch VOR den explizit konfigurierten.
    #
    # Jetzt (v27):
    #   1. Rueckwaerts iterieren, damit der erste Pfad aus
    #      _iter_compiler_python_paths (= hoechste Prioritaet) auch am
    #      Anfang von sys.path steht.
    #   2. Wenn neue Pfade eingefuegt werden, die bereits geladenen
    #      compiler_engine.*-Module aus sys.modules invalidieren, damit
    #      der naechste Import tatsaechlich aus dem neu konfigurierten
    #      Pfad aufloest und nicht aus einem vorherigen Lauf kommt.
    inserted = []
    candidates = _iter_compiler_python_paths(source_settings)

    for path_value in reversed(candidates):
        if not os.path.isdir(path_value):
            continue
        if path_value in sys.path:
            continue
        sys.path.insert(0, path_value)
        inserted.append(path_value)

    if inserted:
        _invalidate_compiler_engine_module_cache(logger)
        # Log in Original-Prioritaetsreihenfolge fuer bessere Lesbarkeit
        logger.info(
            "[cud_compiler_adapter] added compiler search paths (in priority order): %s",
            list(reversed(inserted)),
        )


def _invalidate_compiler_engine_module_cache(logger):
    stale = [name for name in sys.modules if name == "compiler_engine" or name.startswith("compiler_engine.")]
    for name in stale:
        sys.modules.pop(name, None)
    if stale:
        logger.debug(
            "[cud_compiler_adapter] invalidated %d stale compiler_engine module(s) from sys.modules",
            len(stale),
        )



def _import_compile_from_paths(source_settings, logger):
    _ensure_compiler_on_syspath(source_settings, logger)

    try:
        from compiler_engine.application.compile_service import compile_from_paths
        return compile_from_paths
    except Exception as exc:
        logger.error("[cud_compiler_adapter] compile_service import failed: %s", exc)
        return None



def _import_decompile_data(source_settings, logger):
    _ensure_compiler_on_syspath(source_settings, logger)

    try:
        from compiler_engine.application.decompile_service import decompile_data
        return decompile_data
    except Exception as exc:
        logger.error("[cud_compiler_adapter] decompile_service import failed: %s", exc)
        return None



def _is_primary_cud_candidate(path):
    name = os.path.basename(path).lower()
    for marker in _SKIP_NAME_MARKERS:
        if marker in name:
            return False
    return True



def _primary_sort_key(path):
    name = os.path.basename(path).lower()
    ext_rank = 9
    if name.endswith(".yaml"):
        ext_rank = 0
    elif name.endswith(".yml"):
        ext_rank = 1
    elif name.endswith(".json"):
        ext_rank = 2

    preferred_rank = 1
    if "main" in name:
        preferred_rank = 0
    elif name.startswith("01_"):
        preferred_rank = 1
    elif "cud" in name:
        preferred_rank = 2
    else:
        preferred_rank = 3

    return (preferred_rank, ext_rank, name)



def _select_primary_cud_source(source_settings, logger):
    explicit_path = _resolve_path(
        source_settings.get("project_root"),
        source_settings.get("cud_source_path"),
    )
    if explicit_path and os.path.isfile(explicit_path):
        return explicit_path

    cud_dir = _resolve_path(
        source_settings.get("project_root"),
        source_settings.get("cud_source_dir", "config/cud/"),
    )
    paths = _scan_cud_dir(cud_dir)
    candidates = []

    for path in paths:
        if _is_primary_cud_candidate(path):
            candidates.append(path)

    if not candidates:
        logger.warning("[cud_compiler_adapter] no primary CUD source found in %s", cud_dir)
        return None

    candidates.sort(key=_primary_sort_key)
    return candidates[0]



def _invoke_compiler(source_settings, logger):
    compile_from_paths = _import_compile_from_paths(source_settings, logger)
    if not callable(compile_from_paths):
        return None

    primary_source = _select_primary_cud_source(source_settings, logger)
    if not primary_source:
        return None

    cache_dir = _resolve_path(
        source_settings.get("project_root"),
        source_settings.get("compiled_cache_dir", "config/compiled/"),
    )
    if cache_dir is None:
        cache_dir = os.path.normpath("config/compiled/")

    try:
        result = compile_from_paths(primary_source, cache_dir)
    except Exception as exc:
        logger.error("[cud_compiler_adapter] compile_from_paths failed: %s", exc)
        return None

    if not isinstance(result, dict):
        logger.error(
            "[cud_compiler_adapter] compiler returned %s, expected dict",
            type(result).__name__,
        )
        return None

    return result



def _validate_projection_bundle(projection_bundle, logger):
    from src.adapters.compiler._compatibility_validator import validate_compiled_bundle

    ok, reasons = validate_compiled_bundle(projection_bundle)
    if not ok:
        logger.error("[cud_compiler_adapter] projection bundle rejected: %s", reasons)
    return ok



def _load_legacy_base_config(source_settings, logger):
    try:
        from src.adapters.compiler import _legacy_json_adapter
    except Exception as exc:
        logger.error("[cud_compiler_adapter] legacy adapter import failed: %s", exc)
        return {}

    data = _legacy_json_adapter.load_config(source_settings, logger)
    if isinstance(data, dict):
        return data
    return {}



def _bool_setting(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default



def _int_setting(value, default=0):
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return default
    try:
        return int(text)
    except Exception:
        return default



def _load_mapping_document(path_value, logger):
    _, ext = os.path.splitext(path_value)
    ext = ext.lower()

    try:
        with open(path_value, "r", encoding="utf-8") as handle:
            if ext == ".json":
                return json.load(handle)
    except Exception as exc:
        logger.error("[cud_compiler_adapter] failed to parse JSON mapping contract: %s", exc)
        return None

    try:
        import yaml
    except Exception:
        yaml = None

    if yaml is None:
        logger.error("[cud_compiler_adapter] YAML mapping contract requires PyYAML: %s", path_value)
        return None

    try:
        with open(path_value, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except Exception as exc:
        logger.error("[cud_compiler_adapter] failed to parse YAML mapping contract: %s", exc)
        return None



def _load_process_states_mapping_contract(source_settings, logger):
    mapping_contract_path = _resolve_path(
        source_settings.get("project_root"),
        source_settings.get("process_states_mapping_contract_path"),
    )
    if not mapping_contract_path:
        return None

    if not os.path.isfile(mapping_contract_path):
        logger.warning(
            "[cud_compiler_adapter] process_states mapping contract not found: %s",
            mapping_contract_path,
        )
        return None

    document = _load_mapping_document(mapping_contract_path, logger)
    if document is None:
        return None

    if not isinstance(document, (dict, list)):
        logger.error(
            "[cud_compiler_adapter] process_states mapping contract has unsupported type: %s",
            type(document).__name__,
        )
        return None

    return document



def _build_process_states_live_projection_settings(source_settings):
    return {
        "process_states_live_projection_enabled": _bool_setting(
            source_settings.get("process_states_live_projection_enabled"),
            False,
        ),
        "process_states_live_projection_mode": str(
            source_settings.get("process_states_live_projection_mode", "disabled")
        ).strip() or "disabled",
        "process_states_live_projection_apply_limit": _int_setting(
            source_settings.get("process_states_live_projection_apply_limit"),
            0,
        ),
        "process_states_live_projection_allow_review_required": _bool_setting(
            source_settings.get("process_states_live_projection_allow_review_required"),
            False,
        ),
    }


# v19: control_methods — analog zu process_states
def _load_control_methods_mapping_contract(source_settings, logger):
    mapping_contract_path = _resolve_path(
        source_settings.get("project_root"),
        source_settings.get("control_methods_mapping_contract_path"),
    )
    if not mapping_contract_path:
        return None
    if not os.path.isfile(mapping_contract_path):
        logger.warning(
            "[cud_compiler_adapter] control_methods mapping contract not found: %s",
            mapping_contract_path,
        )
        return None
    document = _load_mapping_document(mapping_contract_path, logger)
    if document is None:
        return None
    if not isinstance(document, (dict, list)):
        logger.error(
            "[cud_compiler_adapter] control_methods mapping contract has unsupported type: %s",
            type(document).__name__,
        )
        return None
    return document


def _build_control_methods_live_projection_settings(source_settings):
    return {
        "control_methods_live_projection_enabled": _bool_setting(
            source_settings.get("control_methods_live_projection_enabled"),
            False,
        ),
        "control_methods_live_projection_mode": str(
            source_settings.get("control_methods_live_projection_mode", "disabled")
        ).strip() or "disabled",
        "control_methods_live_projection_apply_limit": _int_setting(
            source_settings.get("control_methods_live_projection_apply_limit"),
            0,
        ),
        "control_methods_live_projection_allow_review_required": _bool_setting(
            source_settings.get("control_methods_live_projection_allow_review_required"),
            False,
        ),
    }



def compile_if_needed(source_settings, logger):
    resolved_settings = dict(source_settings or {})
    cud_dir = _resolve_path(
        resolved_settings.get("project_root"),
        resolved_settings.get("cud_source_dir", "config/cud/"),
    )
    paths = _scan_cud_dir(cud_dir)
    if not paths:
        logger.warning("[cud_compiler_adapter] no CUD files under %s", cud_dir)
        return _BUNDLE_CACHE.get("projection_bundle")

    digest = _hash_inputs(paths)
    cached_digest = _BUNDLE_CACHE.get("hash")
    cached_projection = _BUNDLE_CACHE.get("projection_bundle")
    runtime_compile_enabled = bool(resolved_settings.get("runtime_compile_enabled", False))

    if digest == cached_digest and cached_projection is not None:
        return cached_projection

    if cached_projection is not None and not runtime_compile_enabled:
        logger.info(
            "[cud_compiler_adapter] source digest changed but runtime compile is disabled -> reusing cached projection",
        )
        return cached_projection

    compiler_result = _invoke_compiler(resolved_settings, logger)
    if compiler_result is None:
        return cached_projection

    try:
        projection_bundle = build_runtime_projection_bundle(
            compiler_result,
            previous_projection=cached_projection,
        )
    except Exception as exc:
        logger.error("[cud_compiler_adapter] projection build failed: %s", exc)
        return cached_projection

    if not _validate_projection_bundle(projection_bundle, logger):
        return cached_projection

    _BUNDLE_CACHE["hash"] = digest
    _BUNDLE_CACHE["projection_bundle"] = projection_bundle
    logger.info(
        "[cud_compiler_adapter] projection bundle compiled and cached (digest=%s revision=%s)",
        digest[:12],
        projection_bundle.get("compile_revision"),
    )
    return projection_bundle



def load_config(source_settings, logger):
    resolved_settings = dict(source_settings or {})
    projection_bundle = compile_if_needed(resolved_settings, logger)
    base_config = _load_legacy_base_config(resolved_settings, logger)

    if projection_bundle is None:
        if base_config:
            logger.warning("[cud_compiler_adapter] no projection bundle available -> legacy baseline only")
            return base_config
        logger.error("[cud_compiler_adapter] no projection bundle and no legacy baseline -> empty dict")
        return {}

    process_states_mapping_contract = _load_process_states_mapping_contract(resolved_settings, logger)
    process_states_live_projection_settings = _build_process_states_live_projection_settings(resolved_settings)
    # v19: control_methods
    control_methods_mapping_contract = _load_control_methods_mapping_contract(resolved_settings, logger)
    control_methods_live_projection_settings = _build_control_methods_live_projection_settings(resolved_settings)

    try:
        runtime_config = merge_projection_into_runtime_json_kernel(
            base_config,
            projection_bundle,
            process_states_mapping_contract=process_states_mapping_contract,
            process_states_live_projection_settings=process_states_live_projection_settings,
            control_methods_mapping_contract=control_methods_mapping_contract,
            control_methods_live_projection_settings=control_methods_live_projection_settings,
        )
    except Exception as exc:
        logger.error("[cud_compiler_adapter] projection merge failed: %s", exc)
        if base_config:
            return base_config
        return {}

    return runtime_config



def _normalize_runtime_change_set_payload(runtime_change_set, projection_bundle):
    if not isinstance(runtime_change_set, dict):
        return None

    payload = copy.deepcopy(runtime_change_set)
    if not payload.get("bundle_ref"):
        payload["bundle_ref"] = projection_bundle.get("bundle_ref")
    if not payload.get("compile_revision"):
        payload["compile_revision"] = projection_bundle.get("compile_revision")
    return payload



def reverse_compile_change_set(source_settings, logger, runtime_change_set):
    projection_bundle = compile_if_needed(source_settings, logger)
    if projection_bundle is None:
        logger.error("[cud_compiler_adapter] reverse compile unavailable -> no projection bundle")
        return None

    normalized_change_set = _normalize_runtime_change_set_payload(runtime_change_set, projection_bundle)
    if normalized_change_set is None:
        logger.error("[cud_compiler_adapter] reverse compile requires runtime_change_set dict")
        return None

    sidecars = projection_bundle.get("sidecars")
    if not isinstance(sidecars, dict):
        logger.error("[cud_compiler_adapter] reverse compile unavailable -> sidecars missing")
        return None

    runtime_contract_bundle = sidecars.get("runtime_contract_bundle")
    if not isinstance(runtime_contract_bundle, dict):
        logger.error("[cud_compiler_adapter] reverse compile unavailable -> runtime_contract_bundle missing")
        return None

    decompile_data = _import_decompile_data(source_settings, logger)
    if not callable(decompile_data):
        return None

    try:
        result = decompile_data(runtime_contract_bundle, runtime_change_set=normalized_change_set)
    except Exception as exc:
        logger.error("[cud_compiler_adapter] reverse compile failed: %s", exc)
        return None

    if not isinstance(result, dict):
        logger.error(
            "[cud_compiler_adapter] reverse compile returned %s, expected dict",
            type(result).__name__,
        )
        return None

    logger.info(
        "[cud_compiler_adapter] reverse compile created result for revision=%s change_set=%s",
        projection_bundle.get("compile_revision"),
        normalized_change_set.get("change_set_id"),
    )
    return result



def is_compiled(source_settings, logger):
    return _BUNDLE_CACHE.get("projection_bundle") is not None



def watch_targets(source_settings, logger):
    settings = source_settings or {}
    project_root = settings.get("project_root")
    cud_dir = _resolve_path(project_root, settings.get("cud_source_dir", "config/cud/"))
    legacy_json_path = _resolve_path(project_root, settings.get("legacy_json_path", "config/___device_state_data.json"))
    registry_path = _resolve_path(project_root, settings.get("contract_registry_path"))
    process_mapping_contract_path = _resolve_path(project_root, settings.get("process_states_mapping_contract_path"))
    control_mapping_contract_path = _resolve_path(project_root, settings.get("control_methods_mapping_contract_path"))

    targets = []
    seen = set()

    for path_value in (
        cud_dir,
        legacy_json_path,
        registry_path,
        process_mapping_contract_path,
        control_mapping_contract_path,
    ):
        if not path_value:
            continue
        normalized = os.path.normpath(path_value)
        if normalized in seen:
            continue
        seen.add(normalized)
        targets.append(normalized)

    return tuple(targets)
