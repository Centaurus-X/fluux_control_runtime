# -*- coding: utf-8 -*-
# src/adapters/compiler/_runtime_change_capture.py
#
# Runtime-Change-Capture-Producer.
#
# Zweck:
#   Fängt deterministisch beschriebene Mutations-Events aus der laufenden
#   Runtime auf und persistiert sie als Change-Set-JSON-Dateien in einem
#   konfigurierten Zielverzeichnis. Diese Change-Sets sind die Eingabe
#   für den Reverse-Compile-Consumer (siehe _reverse_compile_loop.py).
#
# Entwurfslinie:
#   - Keine Klassen, keine Decorator.
#   - functools.partial statt Lambdas, wo möglich.
#   - Keine Eingriffe in Runtime-Core-Dateien. Das Modul ist eine reine
#     Adapter-Bibliothek. Der Runtime-Code erhält einen opt-in Callback
#     (`build_capture_callback`), den er an geeigneten Mutationspunkten
#     aufrufen kann. Wenn das Capture-Feature abgeschaltet ist, liefert
#     der Callback einen No-Op zurück.
#   - Atomares Schreiben über tmp-Datei + rename.
#   - Deterministische Dateinamen (monotone Sequenz + Envelope-ID).
#   - Idempotenz über `envelope_id` in den Change-Sets.

import json
import os
import threading
import time
import uuid
from functools import partial


CHANGE_SET_SCHEMA_VERSION = 1
CHANGE_SET_FILE_PREFIX = "rcs_"
CHANGE_SET_FILE_SUFFIX = ".json"
DEFAULT_CAPTURE_DIR = "config/runtime_change_sets/"


def _default_logger():
    import logging
    return logging.getLogger(__name__)


def _resolve_logger(logger):
    if logger is not None:
        return logger
    return _default_logger()


def _ensure_dir(path, logger):
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except Exception as exc:
        logger.error("[runtime_change_capture] could not create dir '%s': %s", path, exc)
        return False


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


def _new_envelope_id():
    return str(uuid.uuid4())


def _build_change_envelope(payload, source_ref, envelope_id=None, revision=None):
    """Deterministische Change-Set-Hülle gemäß Runtime-Change-Set-Vertrag v1."""
    return {
        "schema_version": CHANGE_SET_SCHEMA_VERSION,
        "envelope_id": envelope_id or _new_envelope_id(),
        "captured_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_ref": source_ref,
        "revision_ref": revision,
        "payload": payload,
    }


def _stable_json(data):
    return json.dumps(data, sort_keys=True, ensure_ascii=False, indent=2)


def _atomic_write(file_path, content, logger):
    tmp_path = file_path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(content)
        os.replace(tmp_path, file_path)
        return True
    except Exception as exc:
        logger.error("[runtime_change_capture] atomic write failed for %s: %s", file_path, exc)
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return False


def _build_file_name(sequence_no, envelope_id):
    return "{}{:010d}_{}{}".format(
        CHANGE_SET_FILE_PREFIX,
        int(sequence_no),
        envelope_id[:8],
        CHANGE_SET_FILE_SUFFIX,
    )


def _init_state(capture_dir, enabled, source_ref, logger):
    return {
        "capture_dir": capture_dir,
        "enabled": enabled,
        "source_ref": source_ref,
        "logger": logger,
        "sequence_no": 0,
        "lock": threading.Lock(),
        "written_envelope_ids": set(),
    }


def build_capture_handle(source_settings, source_ref="worker_runtime", logger=None):
    """Baut ein Capture-Handle aus Konfigurationseinstellungen.

    Erwartete Felder in source_settings:
      - runtime_change_capture_enabled: bool
      - runtime_change_capture_dir: str (absolut oder projekt-relativ)
      - project_root: str (optional, wird für Pfadauflösung genutzt)
    """
    logger_obj = _resolve_logger(logger)
    cfg = source_settings or {}
    enabled = _bool(cfg.get("runtime_change_capture_enabled"), default=False)
    raw_dir = cfg.get("runtime_change_capture_dir") or DEFAULT_CAPTURE_DIR
    project_root = cfg.get("project_root")
    if project_root and not os.path.isabs(raw_dir):
        capture_dir = os.path.join(project_root, raw_dir)
    else:
        capture_dir = raw_dir
    capture_dir = os.path.normpath(capture_dir)

    if enabled:
        _ensure_dir(capture_dir, logger_obj)

    state = _init_state(capture_dir, enabled, source_ref, logger_obj)
    logger_obj.info(
        "[runtime_change_capture] handle built enabled=%s dir=%s source=%s",
        enabled,
        capture_dir,
        source_ref,
    )
    return state


def is_enabled(handle):
    if not isinstance(handle, dict):
        return False
    return bool(handle.get("enabled"))


def _noop_capture(payload, envelope_id=None, revision=None):
    return None


def capture_change(handle, payload, envelope_id=None, revision=None):
    """Schreibt einen Change-Set atomar in das Capture-Verzeichnis.

    Wenn das Feature abgeschaltet ist, ist dies ein No-Op und liefert None.
    Bei Erfolg wird der absolute Pfad der geschriebenen Datei zurückgegeben.
    Idempotent: Ein bereits geschriebener `envelope_id` wird nicht erneut
    geschrieben.
    """
    if not isinstance(handle, dict) or not handle.get("enabled"):
        return None
    if payload is None:
        return None

    logger = handle.get("logger") or _default_logger()
    lock = handle.get("lock")
    if lock is None:
        return None

    envelope = _build_change_envelope(
        payload=payload,
        source_ref=handle.get("source_ref"),
        envelope_id=envelope_id,
        revision=revision,
    )
    eid = envelope["envelope_id"]

    with lock:
        if eid in handle["written_envelope_ids"]:
            logger.debug("[runtime_change_capture] envelope_id=%s already written", eid)
            return None
        handle["sequence_no"] += 1
        sequence_no = handle["sequence_no"]
        file_name = _build_file_name(sequence_no, eid)
        file_path = os.path.join(handle["capture_dir"], file_name)
        content = _stable_json(envelope)
        ok = _atomic_write(file_path, content, logger)
        if not ok:
            handle["sequence_no"] -= 1
            return None
        handle["written_envelope_ids"].add(eid)
        logger.debug(
            "[runtime_change_capture] captured envelope_id=%s seq=%d path=%s",
            eid,
            sequence_no,
            file_path,
        )
        return file_path


def build_capture_callback(handle):
    """Liefert einen Callback, den Runtime-Komponenten aufrufen können.

    Dies ist die empfohlene Integration für Runtime-Core-Module: sie erhalten
    per Bootstrap einen Callback (ohne zu wissen, ob Capture aktiv ist oder
    nicht). Wenn Capture deaktiviert ist, liefert der Callback einen echten
    No-Op, der keinerlei Seiteneffekt hat.
    """
    if not isinstance(handle, dict) or not handle.get("enabled"):
        return _noop_capture
    return partial(capture_change, handle)


def list_pending_change_sets(handle):
    """Listet alle aktuell im Capture-Verzeichnis liegenden Change-Set-Dateien."""
    if not isinstance(handle, dict):
        return []
    capture_dir = handle.get("capture_dir")
    if not capture_dir or not os.path.isdir(capture_dir):
        return []
    files = []
    for name in sorted(os.listdir(capture_dir)):
        if name.startswith(CHANGE_SET_FILE_PREFIX) and name.endswith(CHANGE_SET_FILE_SUFFIX):
            files.append(os.path.join(capture_dir, name))
    return files


def load_change_set(file_path):
    with open(file_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def mark_processed(file_path, processed_subdir="_processed"):
    """Verschiebt verarbeitete Change-Sets in ein Archivverzeichnis.

    So bleibt das Capture-Verzeichnis übersichtlich, und der Reverse-Compile-
    Consumer sieht nur noch offene Einträge.
    """
    if not os.path.isfile(file_path):
        return None
    base = os.path.dirname(file_path)
    target_dir = os.path.join(base, processed_subdir)
    try:
        os.makedirs(target_dir, exist_ok=True)
    except Exception:
        return None
    target_path = os.path.join(target_dir, os.path.basename(file_path))
    try:
        os.replace(file_path, target_path)
        return target_path
    except Exception:
        return None


def describe(handle):
    """Diagnose-Handle für Logging / Runtime-Reporting."""
    if not isinstance(handle, dict):
        return {"enabled": False}
    return {
        "enabled": bool(handle.get("enabled")),
        "capture_dir": handle.get("capture_dir"),
        "source_ref": handle.get("source_ref"),
        "sequence_no": handle.get("sequence_no", 0),
        "written_count": len(handle.get("written_envelope_ids", ())),
    }
