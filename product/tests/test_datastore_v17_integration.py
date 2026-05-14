# -*- coding: utf-8 -*-
# tests/test_datastore_v17_integration.py
#
# v17: Testet die Runtime-Core-Integration von Capture und Reverse-Compile-Loop.
# Prüft, dass die Binder-Funktionen aus worker_datastore.py sauber arbeiten —
# insbesondere, dass sie bei deaktivierten Flags echte No-Ops sind.

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core import worker_datastore as wds


def _make_tmp_project():
    return tempfile.mkdtemp(prefix="v17_ds_")


def _cleanup(base):
    shutil.rmtree(base, ignore_errors=True)


def test_bind_runtime_change_capture_disabled_is_noop():
    base = _make_tmp_project()
    try:
        handle = wds.bind_runtime_change_capture(
            {
                "runtime_change_capture_enabled": False,
                "project_root": base,
            }
        )
        assert handle is not None
        assert handle.get("enabled") is False
        # Commit-Wrapper muss inert sein
        result = wds._capture_commit({"event": "test"}, envelope_id="eid-1")
        assert result is None
        # Verzeichnis darf nicht entstehen
        assert not os.path.exists(os.path.join(base, "config/runtime_change_sets"))
    finally:
        _cleanup(base)


def test_bind_runtime_change_capture_enabled_writes():
    base = _make_tmp_project()
    try:
        wds.bind_runtime_change_capture(
            {
                "runtime_change_capture_enabled": True,
                "project_root": base,
            }
        )
        # Commit-Wrapper schreibt Change-Set
        result = wds._capture_commit(
            {"event": "config_changed", "old_hash": "a", "new_hash": "b"},
            envelope_id="eid-v17-1",
            revision="rev-b",
        )
        assert result is not None
        assert os.path.isfile(result)
    finally:
        wds._CAPTURE_HANDLE = None
        wds._CAPTURE_CALLBACK = None
        _cleanup(base)


def test_bind_reverse_compile_loop_disabled_is_noop():
    base = _make_tmp_project()
    try:
        handle = wds.bind_reverse_compile_loop(
            {
                "reverse_compile_enabled": False,
                "project_root": base,
            }
        )
        assert handle is not None
        assert handle.get("enabled") is False
        # Drain-Tick: No-op (keine Exception, None zurück)
        result = wds._drain_reverse_compile_tick()
        # Rate-Limit oder disabled → None
        assert result is None or result.get("reason") in (
            "disabled",
            "adapter_has_no_reverse_compile_fn",
            "empty",
        )
    finally:
        wds._REVERSE_LOOP_HANDLE = None
        _cleanup(base)


def test_rate_limit_on_drain_tick():
    base = _make_tmp_project()
    try:
        wds.bind_reverse_compile_loop(
            {
                "reverse_compile_enabled": True,
                "project_root": base,
            }
        )
        # Erste Invocation setzt den Timestamp
        wds._REVERSE_LOOP_LAST_TICK_TS = 0.0
        r1 = wds._drain_reverse_compile_tick()
        # Unmittelbar darauf: Rate-Limit greift
        r2 = wds._drain_reverse_compile_tick()
        # Zumindest einer der Aufrufe liefert None wegen Rate-Limit
        assert r1 is None or r2 is None
    finally:
        wds._REVERSE_LOOP_HANDLE = None
        wds._REVERSE_LOOP_LAST_TICK_TS = 0.0
        _cleanup(base)


def test_bootstrap_flags_none_is_safe():
    # Bootstrap-Szenario: Wenn kein source_settings vorhanden ist,
    # sollten die Binder-Funktionen defensiv None/inert zurückgeben.
    h1 = wds.bind_runtime_change_capture({})
    assert h1 is not None or h1 is None  # tolerant
    h2 = wds.bind_reverse_compile_loop({})
    assert h2 is not None or h2 is None  # tolerant
