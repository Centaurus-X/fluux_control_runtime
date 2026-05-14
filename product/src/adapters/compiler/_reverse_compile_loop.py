# -*- coding: utf-8 -*-
# src/adapters/compiler/_reverse_compile_loop.py
#
# Reverse-Compile-Consumer.
#
# Zweck:
#   Liest Runtime-Change-Sets aus dem Capture-Verzeichnis, ruft den
#   Adapter-seitig vorhandenen `reverse_compile`-Pfad auf und legt das
#   Ergebnis deterministisch in einem Ausgabe-Verzeichnis ab. Verarbeitete
#   Eingaben werden in ein Archivunterverzeichnis verschoben (mark_processed).
#
# Entwurfslinie:
#   - Keine Klassen, keine Decorator.
#   - functools.partial statt Lambda.
#   - Einziger Einsprung: `drain_reverse_compile_once(handle, adapter_handle)`.
#     Runtime-Code ruft diese Funktion periodisch (oder ereignisgesteuert).
#   - Kein Scheduler, kein eigener Thread: die Aufruffrequenz bestimmt die
#     Runtime. Das hält das Modul klein und testbar.

import json
import os
import time
from functools import partial

from src.adapters.compiler._runtime_change_capture import (
    CHANGE_SET_FILE_PREFIX,
    CHANGE_SET_FILE_SUFFIX,
    list_pending_change_sets,
    load_change_set,
    mark_processed,
)


REVERSE_COMPILE_OUTPUT_SCHEMA_VERSION = 1
REVERSE_COMPILE_OUTPUT_PREFIX = "rc_"
REVERSE_COMPILE_OUTPUT_SUFFIX = ".json"
DEFAULT_OUTPUT_DIR = "config/decompiled/"


def _default_logger():
    import logging
    return logging.getLogger(__name__)


def _resolve_logger(logger):
    if logger is not None:
        return logger
    return _default_logger()


def _bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("true", "1", "yes", "on"):
        return True
    if text in ("false", "0", "no", "off"):
        return False
    return default


def _ensure_dir(path, logger):
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except Exception as exc:
        logger.error("[reverse_compile_loop] cannot create '%s': %s", path, exc)
        return False


def _atomic_write(file_path, content, logger):
    tmp = file_path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(content)
        os.replace(tmp, file_path)
        return True
    except Exception as exc:
        logger.error("[reverse_compile_loop] atomic write failed for %s: %s", file_path, exc)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False


def _build_output_envelope(change_set, reverse_result):
    return {
        "schema_version": REVERSE_COMPILE_OUTPUT_SCHEMA_VERSION,
        "decompiled_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input_envelope_id": change_set.get("envelope_id"),
        "input_revision_ref": change_set.get("revision_ref"),
        "input_source_ref": change_set.get("source_ref"),
        "reverse_compile_result": reverse_result,
    }


def _build_output_file_name(envelope_id):
    safe = (envelope_id or "noeid").replace("/", "_").replace("\\", "_")
    return "{}{}{}".format(REVERSE_COMPILE_OUTPUT_PREFIX, safe[:16], REVERSE_COMPILE_OUTPUT_SUFFIX)


def build_loop_handle(source_settings, logger=None):
    """Baut ein Loop-Handle aus Konfigurationseinstellungen.

    Erwartete Felder in source_settings:
      - reverse_compile_enabled: bool
      - reverse_compile_output_dir: str
      - runtime_change_capture_dir: str (Eingangsverzeichnis)
      - project_root: str (optional)
    """
    logger_obj = _resolve_logger(logger)
    cfg = source_settings or {}
    enabled = _bool(cfg.get("reverse_compile_enabled"), default=False)

    project_root = cfg.get("project_root")
    raw_out = cfg.get("reverse_compile_output_dir") or DEFAULT_OUTPUT_DIR
    if project_root and not os.path.isabs(raw_out):
        out_dir = os.path.join(project_root, raw_out)
    else:
        out_dir = raw_out
    out_dir = os.path.normpath(out_dir)

    raw_in = cfg.get("runtime_change_capture_dir") or "config/runtime_change_sets/"
    if project_root and not os.path.isabs(raw_in):
        in_dir = os.path.join(project_root, raw_in)
    else:
        in_dir = raw_in
    in_dir = os.path.normpath(in_dir)

    if enabled:
        _ensure_dir(out_dir, logger_obj)

    handle = {
        "enabled": enabled,
        "input_dir": in_dir,
        "output_dir": out_dir,
        "logger": logger_obj,
        "processed_count": 0,
        "failed_count": 0,
    }
    logger_obj.info(
        "[reverse_compile_loop] handle built enabled=%s input=%s output=%s",
        enabled,
        in_dir,
        out_dir,
    )
    return handle


def is_enabled(handle):
    if not isinstance(handle, dict):
        return False
    return bool(handle.get("enabled"))


def _simulate_capture_handle(input_dir):
    return {"capture_dir": input_dir}


def _process_one(handle, adapter_handle, reverse_compile_fn, file_path):
    logger = handle.get("logger") or _default_logger()
    try:
        change_set = load_change_set(file_path)
    except Exception as exc:
        logger.error("[reverse_compile_loop] cannot read %s: %s", file_path, exc)
        handle["failed_count"] = handle.get("failed_count", 0) + 1
        return False

    try:
        reverse_result = reverse_compile_fn(change_set)
    except Exception as exc:
        logger.error(
            "[reverse_compile_loop] reverse_compile raised for %s: %s", file_path, exc
        )
        handle["failed_count"] = handle.get("failed_count", 0) + 1
        return False

    if reverse_result is None:
        logger.warning(
            "[reverse_compile_loop] reverse_compile returned None for %s", file_path
        )
        handle["failed_count"] = handle.get("failed_count", 0) + 1
        return False

    envelope = _build_output_envelope(change_set, reverse_result)
    out_name = _build_output_file_name(change_set.get("envelope_id"))
    out_path = os.path.join(handle["output_dir"], out_name)
    ok = _atomic_write(out_path, json.dumps(envelope, sort_keys=True, ensure_ascii=False, indent=2), logger)
    if not ok:
        handle["failed_count"] = handle.get("failed_count", 0) + 1
        return False

    archived = mark_processed(file_path)
    if not archived:
        logger.warning(
            "[reverse_compile_loop] could not archive %s after processing", file_path
        )

    handle["processed_count"] = handle.get("processed_count", 0) + 1
    return True


def drain_reverse_compile_once(handle, adapter_handle, max_items=50):
    """Verarbeitet bis zu `max_items` Change-Sets in einer Runde.

    Diese Funktion ist sicher mehrmals aufrufbar: sie verarbeitet nur die
    aktuell noch nicht archivierten Dateien. `adapter_handle` muss das
    Ergebnis von `build_adapter(..., mode='cud_compiled')` sein, also ein
    dict mit `reverse_compile_fn`.
    """
    if not isinstance(handle, dict) or not handle.get("enabled"):
        return {"processed": 0, "skipped": 0, "failed": 0, "reason": "disabled"}
    if not isinstance(adapter_handle, dict):
        return {"processed": 0, "skipped": 0, "failed": 0, "reason": "no_adapter"}

    reverse_compile_fn = adapter_handle.get("reverse_compile_fn")
    if not callable(reverse_compile_fn):
        return {
            "processed": 0,
            "skipped": 0,
            "failed": 0,
            "reason": "adapter_has_no_reverse_compile_fn",
        }

    logger = handle.get("logger") or _default_logger()
    pending = list_pending_change_sets(_simulate_capture_handle(handle["input_dir"]))
    if not pending:
        return {"processed": 0, "skipped": 0, "failed": 0, "reason": "empty"}

    processed = 0
    failed = 0
    skipped = 0
    for file_path in pending[:max_items]:
        ok = _process_one(handle, adapter_handle, reverse_compile_fn, file_path)
        if ok:
            processed += 1
        else:
            failed += 1
    if len(pending) > max_items:
        skipped = len(pending) - max_items
        logger.info(
            "[reverse_compile_loop] %d items deferred to next drain (max_items=%d)",
            skipped,
            max_items,
        )
    return {"processed": processed, "skipped": skipped, "failed": failed, "reason": "ok"}


def describe(handle):
    if not isinstance(handle, dict):
        return {"enabled": False}
    return {
        "enabled": bool(handle.get("enabled")),
        "input_dir": handle.get("input_dir"),
        "output_dir": handle.get("output_dir"),
        "processed_count": handle.get("processed_count", 0),
        "failed_count": handle.get("failed_count", 0),
    }
