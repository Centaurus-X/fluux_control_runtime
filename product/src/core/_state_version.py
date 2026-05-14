# -*- coding: utf-8 -*-
# src/core/_state_version.py
#
# Lieferung 3: state_version als First-Class Konzept.
# Flag-gated via worker_runtime.state_version_enabled. Default false.
# Integriert sich in _worker_writer_sync, ohne dessen Code zu aendern:
# _worker_writer_sync ruft beim Emittieren eines CONFIG_PATCH die
# Funktion stamp_patch(settings, resources, patch_envelope) auf.
# Wenn das Flag aus ist, passiert nichts (Null-Kosten-Pfad).
#
# Keine Klassen, keine Decorator, keine lambda. threading.Lock statt OOP-State.

import threading

_VERSIONS = {}
_LOCK = threading.Lock()
_VERSION_BUCKET_KEY = "__versions__"


def is_enabled(settings):
    wr = (settings or {}).get("worker_runtime", {}) or {}
    return bool(wr.get("state_version_enabled", False))


def _ensure_bucket(resources):
    if not isinstance(resources, dict):
        return None
    bucket = resources.get(_VERSION_BUCKET_KEY)
    if not isinstance(bucket, dict):
        bucket = {}
        resources[_VERSION_BUCKET_KEY] = bucket
    return bucket


def current_version(resources, resource_key):
    bucket = None
    if isinstance(resources, dict):
        bucket = resources.get(_VERSION_BUCKET_KEY)
    if not isinstance(bucket, dict):
        return 0
    return int(bucket.get(resource_key, 0))


def bump_version(resources, resource_key):
    with _LOCK:
        bucket = _ensure_bucket(resources)
        if bucket is None:
            return 0
        next_value = int(bucket.get(resource_key, 0)) + 1
        bucket[resource_key] = next_value
        return next_value


def stamp_patch(settings, resources, patch_envelope):
    # Einzige Aufrufsite: _worker_writer_sync nach erfolgreichem Apply.
    # Wenn Flag aus oder Envelope kein Dict: Envelope unveraendert zurueck.
    if not isinstance(patch_envelope, dict):
        return patch_envelope
    if not is_enabled(settings):
        return patch_envelope
    resource_key = patch_envelope.get("resource_key") or patch_envelope.get("target") or "config_data"
    version = bump_version(resources, resource_key)
    stamped = dict(patch_envelope)
    stamped["state_version"] = version
    stamped["based_on_state_version"] = int(patch_envelope.get("based_on_state_version", version - 1))
    return stamped


def validate_based_on(settings, resources, envelope):
    # Optionales optimistisches Locking fuer CONFIG_CHANGED-Konsumenten.
    if not is_enabled(settings) or not isinstance(envelope, dict):
        return True, "disabled"
    expected = envelope.get("based_on_state_version")
    if expected is None:
        return True, "no_based_on"
    resource_key = envelope.get("resource_key") or envelope.get("target") or "config_data"
    actual = current_version(resources, resource_key)
    if int(expected) >= actual - 1:
        return True, "ok"
    return False, "stale_based_on:%d<%d" % (int(expected), actual)


def reset_for_tests():
    with _LOCK:
        _VERSIONS.clear()
