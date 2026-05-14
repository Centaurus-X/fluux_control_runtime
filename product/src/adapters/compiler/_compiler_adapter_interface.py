# -*- coding: utf-8 -*-

# src/adapters/compiler/_compiler_adapter_interface.py
#
# Duennes Interface fuer Config-Source-Adapter.
# Analog zu src/adapters/_adapter_interface.py aus dem Fieldbus-Zweig:
# gibt es keine Klassen, nur freie Funktionen und ein Dict als Handle.
#
# Vertrag:
#   build_adapter(source_settings, logger=None) -> adapter_handle (dict)
#   load_config(adapter_handle)                  -> config_dict
#   is_compiled(adapter_handle)                  -> bool
#   compile_if_needed(adapter_handle)            -> projection_bundle | None
#   reverse_compile(adapter_handle, change_set)  -> dict | None
#   describe(adapter_handle)                     -> dict (fuer Logs)

from functools import partial
import logging


SUPPORTED_MODES = ("legacy_json", "cud_compiled")


def _logger_or_default(logger):
    if logger is not None:
        return logger
    return logging.getLogger(__name__)


def build_adapter(source_settings, logger=None):
    logger_obj = _logger_or_default(logger)
    mode = str((source_settings or {}).get("mode", "legacy_json")).strip()
    if mode not in SUPPORTED_MODES:
        logger_obj.warning(
            "[compiler_adapter] Unknown mode '%s' -> fallback to legacy_json",
            mode,
        )
        mode = "legacy_json"

    # Spaete Imports: die konkreten Adapter-Module sollen nur geladen werden
    # wenn sie auch gebraucht werden. Legacy-Betrieb darf keine pyyaml-
    # Abhaengigkeit aus dem compile_engine ziehen.
    if mode == "legacy_json":
        from src.adapters.compiler import _legacy_json_adapter as impl
    else:
        from src.adapters.compiler import _cud_compiler_adapter as impl

    reverse_compile_fn = None
    if mode == "cud_compiled" and hasattr(impl, "reverse_compile_change_set"):
        reverse_compile_fn = partial(impl.reverse_compile_change_set, source_settings or {}, logger_obj)

    handle = {
        "mode": mode,
        "settings": dict(source_settings or {}),
        "impl_module": impl.__name__,
        "load_fn": partial(impl.load_config, source_settings or {}, logger_obj),
        "compile_fn": partial(impl.compile_if_needed, source_settings or {}, logger_obj),
        "is_compiled_fn": partial(impl.is_compiled, source_settings or {}, logger_obj),
        "watch_targets_fn": partial(impl.watch_targets, source_settings or {}, logger_obj),
        "reverse_compile_fn": reverse_compile_fn,
        "logger": logger_obj,
    }
    logger_obj.info("[compiler_adapter] built adapter mode=%s impl=%s", mode, impl.__name__)
    return handle


def load_config(adapter_handle):
    return adapter_handle["load_fn"]()


def compile_if_needed(adapter_handle):
    return adapter_handle["compile_fn"]()


def is_compiled(adapter_handle):
    return adapter_handle["is_compiled_fn"]()


def watch_targets(adapter_handle):
    # Liste der Pfade, die der bestehende inotify-Watcher im worker_datastore
    # ueberwachen soll. Legacy = JSON-Datei. CUD = CUD-Verzeichnis.
    return adapter_handle["watch_targets_fn"]()


def reverse_compile(adapter_handle, runtime_change_set):
    reverse_compile_fn = adapter_handle.get("reverse_compile_fn")
    if not callable(reverse_compile_fn):
        return None
    return reverse_compile_fn(runtime_change_set)


def describe(adapter_handle):
    return {
        "mode": adapter_handle.get("mode"),
        "impl_module": adapter_handle.get("impl_module"),
        "settings_keys": sorted(list((adapter_handle.get("settings") or {}).keys())),
        "supports_reverse_compile": callable(adapter_handle.get("reverse_compile_fn")),
    }
