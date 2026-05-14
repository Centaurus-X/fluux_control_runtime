# -*- coding: utf-8 -*-
# tests/test_reverse_compile_loop.py
#
# Testet den Reverse-Compile-Consumer (v16).

import json
import os
import shutil
import sys
import tempfile
from functools import partial

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.adapters.compiler import _runtime_change_capture as rcc
from src.adapters.compiler import _reverse_compile_loop as rcl


def _make_tmp_project():
    return tempfile.mkdtemp(prefix="v16_rcl_")


def _cleanup(base):
    shutil.rmtree(base, ignore_errors=True)


def _fake_reverse_compile(captured_state, change_set):
    """Minimaler Fake-Reverse-Compile für Tests. Echo-artig."""
    captured_state["calls"] = captured_state.get("calls", 0) + 1
    return {
        "decompiled": True,
        "from_envelope_id": change_set.get("envelope_id"),
        "from_payload": change_set.get("payload"),
    }


def _make_adapter_with_fake_reverse(state):
    """Baut ein minimalistisches Adapter-Handle, das reverse_compile_fn anbietet."""
    return {
        "mode": "cud_compiled",
        "reverse_compile_fn": partial(_fake_reverse_compile, state),
    }


def test_disabled_loop_is_noop():
    base = _make_tmp_project()
    try:
        handle = rcl.build_loop_handle(
            {"reverse_compile_enabled": False, "project_root": base}
        )
        assert not rcl.is_enabled(handle)
        state = {}
        adapter = _make_adapter_with_fake_reverse(state)
        result = rcl.drain_reverse_compile_once(handle, adapter)
        assert result["processed"] == 0
        assert result["reason"] == "disabled"
        assert state.get("calls", 0) == 0
    finally:
        _cleanup(base)


def test_missing_reverse_compile_fn():
    base = _make_tmp_project()
    try:
        handle = rcl.build_loop_handle(
            {"reverse_compile_enabled": True, "project_root": base}
        )
        adapter = {"mode": "legacy_json"}  # Kein reverse_compile_fn
        result = rcl.drain_reverse_compile_once(handle, adapter)
        assert result["processed"] == 0
        assert result["reason"] == "adapter_has_no_reverse_compile_fn"
    finally:
        _cleanup(base)


def test_empty_input_dir():
    base = _make_tmp_project()
    try:
        os.makedirs(os.path.join(base, "config/runtime_change_sets"), exist_ok=True)
        handle = rcl.build_loop_handle(
            {"reverse_compile_enabled": True, "project_root": base}
        )
        state = {}
        adapter = _make_adapter_with_fake_reverse(state)
        result = rcl.drain_reverse_compile_once(handle, adapter)
        assert result["processed"] == 0
        assert result["reason"] == "empty"
    finally:
        _cleanup(base)


def test_drain_processes_pending_and_writes_output():
    base = _make_tmp_project()
    try:
        # 1. Producer schreibt drei Change-Sets
        capture_handle = rcc.build_capture_handle(
            {"runtime_change_capture_enabled": True, "project_root": base}
        )
        rcc.capture_change(capture_handle, {"op": "set", "value": 10})
        rcc.capture_change(capture_handle, {"op": "set", "value": 20})
        rcc.capture_change(capture_handle, {"op": "set", "value": 30})
        assert len(rcc.list_pending_change_sets(capture_handle)) == 3

        # 2. Consumer verarbeitet
        loop_handle = rcl.build_loop_handle(
            {"reverse_compile_enabled": True, "project_root": base}
        )
        state = {}
        adapter = _make_adapter_with_fake_reverse(state)
        result = rcl.drain_reverse_compile_once(loop_handle, adapter)
        assert result["processed"] == 3
        assert result["failed"] == 0
        assert state.get("calls") == 3

        # 3. Output-Verzeichnis enthält drei Dateien
        out_dir = os.path.join(base, "config/decompiled")
        out_files = [f for f in os.listdir(out_dir) if f.startswith("rc_")]
        assert len(out_files) == 3

        # 4. Pending ist leer (Dateien wurden archiviert)
        assert rcc.list_pending_change_sets(capture_handle) == []

        # 5. Inhalt der Output-Datei prüfen
        first_path = os.path.join(out_dir, out_files[0])
        with open(first_path, "r", encoding="utf-8") as fh:
            envelope = json.load(fh)
        assert envelope["schema_version"] == rcl.REVERSE_COMPILE_OUTPUT_SCHEMA_VERSION
        assert envelope["reverse_compile_result"]["decompiled"] is True
    finally:
        _cleanup(base)


def test_max_items_defers_rest():
    base = _make_tmp_project()
    try:
        capture_handle = rcc.build_capture_handle(
            {"runtime_change_capture_enabled": True, "project_root": base}
        )
        for i in range(5):
            rcc.capture_change(capture_handle, {"i": i})

        loop_handle = rcl.build_loop_handle(
            {"reverse_compile_enabled": True, "project_root": base}
        )
        adapter = _make_adapter_with_fake_reverse({})
        result = rcl.drain_reverse_compile_once(loop_handle, adapter, max_items=2)
        assert result["processed"] == 2
        assert result["skipped"] == 3
    finally:
        _cleanup(base)


def test_describe():
    base = _make_tmp_project()
    try:
        handle = rcl.build_loop_handle(
            {"reverse_compile_enabled": True, "project_root": base}
        )
        info = rcl.describe(handle)
        assert info["enabled"] is True
        assert info["processed_count"] == 0
        assert info["failed_count"] == 0
    finally:
        _cleanup(base)
