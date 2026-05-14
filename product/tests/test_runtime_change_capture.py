# -*- coding: utf-8 -*-
# tests/test_runtime_change_capture.py
#
# Testet den Runtime-Change-Capture-Producer (v16).
# Stil: keine Klassen, keine Decorator, functools.partial statt Lambda.

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.adapters.compiler import _runtime_change_capture as rcc


def _make_tmp_project():
    base = tempfile.mkdtemp(prefix="v16_rcc_")
    return base


def _cleanup(base):
    shutil.rmtree(base, ignore_errors=True)


def test_disabled_handle_is_noop():
    base = _make_tmp_project()
    try:
        handle = rcc.build_capture_handle(
            {"runtime_change_capture_enabled": False, "project_root": base}
        )
        assert not rcc.is_enabled(handle)
        callback = rcc.build_capture_callback(handle)
        result = callback({"mutation": "anything"})
        assert result is None
        # Verzeichnis darf nicht entstehen, wenn disabled
        expected_dir = os.path.join(base, "config/runtime_change_sets")
        assert not os.path.exists(expected_dir)
    finally:
        _cleanup(base)


def test_enabled_handle_creates_dir_and_writes():
    base = _make_tmp_project()
    try:
        handle = rcc.build_capture_handle(
            {
                "runtime_change_capture_enabled": True,
                "runtime_change_capture_dir": "config/runtime_change_sets/",
                "project_root": base,
            },
            source_ref="test_worker",
        )
        assert rcc.is_enabled(handle)
        capture_dir = os.path.join(base, "config/runtime_change_sets")
        assert os.path.isdir(capture_dir)

        callback = rcc.build_capture_callback(handle)
        path = callback({"mutation": "set_process_state", "id": 42})
        assert path is not None
        assert os.path.isfile(path)

        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        assert data["schema_version"] == rcc.CHANGE_SET_SCHEMA_VERSION
        assert data["source_ref"] == "test_worker"
        assert data["payload"]["id"] == 42
        assert "envelope_id" in data
        assert "captured_at_utc" in data
    finally:
        _cleanup(base)


def test_idempotency_by_envelope_id():
    base = _make_tmp_project()
    try:
        handle = rcc.build_capture_handle(
            {
                "runtime_change_capture_enabled": True,
                "project_root": base,
            }
        )
        eid = "fixed-envelope-id-0001"
        p1 = rcc.capture_change(handle, {"x": 1}, envelope_id=eid)
        p2 = rcc.capture_change(handle, {"x": 2}, envelope_id=eid)
        assert p1 is not None
        # Zweiter Aufruf mit gleicher envelope_id ist ein No-Op
        assert p2 is None

        pending = rcc.list_pending_change_sets(handle)
        assert len(pending) == 1
    finally:
        _cleanup(base)


def test_mark_processed_moves_file():
    base = _make_tmp_project()
    try:
        handle = rcc.build_capture_handle(
            {"runtime_change_capture_enabled": True, "project_root": base}
        )
        path = rcc.capture_change(handle, {"mutation": "ok"})
        assert path is not None
        archived = rcc.mark_processed(path)
        assert archived is not None
        assert os.path.isfile(archived)
        assert not os.path.exists(path)
        # Nach Archivierung keine Pending mehr
        assert rcc.list_pending_change_sets(handle) == []
    finally:
        _cleanup(base)


def test_describe_handle():
    base = _make_tmp_project()
    try:
        handle = rcc.build_capture_handle(
            {"runtime_change_capture_enabled": True, "project_root": base}
        )
        info = rcc.describe(handle)
        assert info["enabled"] is True
        assert info["sequence_no"] == 0
        assert info["written_count"] == 0

        rcc.capture_change(handle, {"a": 1})
        info2 = rcc.describe(handle)
        assert info2["sequence_no"] == 1
        assert info2["written_count"] == 1
    finally:
        _cleanup(base)
