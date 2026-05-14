# -*- coding: utf-8 -*-
# src/adapters/compiler/_legacy_json_adapter.py
#
# Wrapper um den bestehenden JSON-Pfad (___device_state_data.json).
# Aendert NICHTS am Leseverhalten - liest und parst die Datei exakt so
# wie der heutige worker_datastore es bereits tut.
# Rueckgabe ist das rohe Dict; der Aufrufer behaelt sein Verhalten bei.

import json
import os


def _resolve_path(source_settings):
    path = (source_settings or {}).get("legacy_json_path")
    if not path:
        path = "config/___device_state_data.json"
    return os.path.normpath(path)


def load_config(source_settings, logger):
    path = _resolve_path(source_settings)
    if not os.path.exists(path):
        logger.warning("[legacy_json_adapter] file not found: %s -> empty dict", path)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        logger.error("[legacy_json_adapter] read failed: %s: %s", path, exc)
        return {}
    if not isinstance(data, dict):
        logger.error("[legacy_json_adapter] unexpected top-level type: %s", type(data).__name__)
        return {}
    logger.info("[legacy_json_adapter] loaded %d top-level keys from %s", len(data), path)
    return data


def compile_if_needed(source_settings, logger):
    # Legacy kennt keinen Compile. Wir liefern None zurueck, damit der
    # worker_datastore weiss: es gibt nichts neu zu laden ausser bei echter
    # Dateiaenderung (die bereits der bestehende inotify-Pfad triggert).
    return None


def is_compiled(source_settings, logger):
    return False


def watch_targets(source_settings, logger):
    # Derselbe Pfad, den der heutige inotify-Watcher auch schon benutzt.
    return (_resolve_path(source_settings),)


def reverse_compile_change_set(source_settings, logger, runtime_change_set):
    # Legacy-JSON kennt keinen Rueckweg in CUD. Wir liefern None zurueck, damit
    # der Aufrufer sauber unterscheiden kann zwischen vorbereitetem CUD-Pfad
    # und rein legacy-basiertem Laufzeitbetrieb.
    return None
