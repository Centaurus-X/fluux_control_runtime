# -*- coding: utf-8 -*-
# tests/test_broker_envelope_v18.py
#
# v18: Testet die Broker-Integration des Event-Envelope-Enrichment.
# Prüft insbesondere die Default-off-Garantie.

import os
import sys
from functools import partial

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core import sync_event_router as ser


def _reset_binding():
    """Hilfsfunktion: setzt die globale Enrichment-Funktion zurück."""
    ser._ENVELOPE_ENRICHMENT_FN = None


def _build_mock_queue():
    """Minimale mock queue als dict mit list-semantik."""
    return {"items": []}


def _mock_sync_queue_put(queue, payload):
    queue["items"].append(payload)
    return True


def test_default_forward_does_not_enrich():
    """v18: Bei fehlender Bindung darf das Payload *nicht* angereichert werden."""
    _reset_binding()

    # Backup und Monkey-Patch sync_queue_put
    original = ser.sync_queue_put
    ser.sync_queue_put = _mock_sync_queue_put
    try:
        q = _build_mock_queue()
        data = {"event_type": "CONFIG_CHANGED", "payload": {}}
        ok = ser.forward_to_queue(q, data, "test_handler")
        assert ok is True
        assert len(q["items"]) == 1
        forwarded = q["items"][0]
        # Kein envelope_id, keine message_kind, kein vector_clock
        assert "envelope_id" not in forwarded
        assert "message_kind" not in forwarded
        assert "vector_clock" not in forwarded
    finally:
        ser.sync_queue_put = original
        _reset_binding()


def test_enabled_forward_enriches_payload():
    """v18: Bei aktivem Binding wird das Payload mit Envelope-Feldern angereichert."""
    _reset_binding()
    ser.bind_envelope_enrichment(
        enabled=True,
        source_ref="broker_test",
        node_id="node_test",
        trust_level="internal",
    )
    assert ser._ENVELOPE_ENRICHMENT_FN is not None

    original = ser.sync_queue_put
    ser.sync_queue_put = _mock_sync_queue_put
    try:
        q = _build_mock_queue()
        data = {"event_type": "CONFIG_CHANGED", "payload": {}}
        ok = ser.forward_to_queue(q, data, "test_handler")
        assert ok is True
        assert len(q["items"]) == 1
        forwarded = q["items"][0]
        assert "envelope_id" in forwarded
        assert forwarded.get("message_kind") == "state"
        assert forwarded.get("source_ref") == "broker_test"
        assert "vector_clock" in forwarded
        assert forwarded["vector_clock"].get("node_test") == 1
    finally:
        ser.sync_queue_put = original
        _reset_binding()


def test_disable_clears_binding():
    """v18: bind_envelope_enrichment(enabled=False) leert das globale Handle."""
    _reset_binding()
    ser.bind_envelope_enrichment(enabled=True, source_ref="x")
    assert ser._ENVELOPE_ENRICHMENT_FN is not None
    ser.bind_envelope_enrichment(enabled=False)
    assert ser._ENVELOPE_ENRICHMENT_FN is None


def test_enrichment_errors_do_not_fail_forward():
    """v18: Ein Fehler im Enrichment darf das Routing nicht brechen."""
    _reset_binding()

    def _broken_enrich_fn(data):
        raise ValueError("intentional test failure")

    ser._ENVELOPE_ENRICHMENT_FN = _broken_enrich_fn
    original = ser.sync_queue_put
    ser.sync_queue_put = _mock_sync_queue_put
    try:
        q = _build_mock_queue()
        data = {"event_type": "CONFIG_CHANGED"}
        ok = ser.forward_to_queue(q, data, "test_handler")
        # Trotz Enrichment-Fehler muss das Forwarding erfolgreich sein.
        assert ok is True
        assert len(q["items"]) == 1
    finally:
        ser.sync_queue_put = original
        _reset_binding()


def test_non_dict_data_is_passed_through():
    """v18: Kein Enrichment für Nicht-Dict-Daten (z.B. Stop-Sentinels)."""
    _reset_binding()
    ser.bind_envelope_enrichment(enabled=True, source_ref="x")

    original = ser.sync_queue_put
    ser.sync_queue_put = _mock_sync_queue_put
    try:
        q = _build_mock_queue()
        data = "STOP"  # kein dict
        ok = ser.forward_to_queue(q, data, "test_handler")
        assert ok is True
        assert q["items"] == ["STOP"]
    finally:
        ser.sync_queue_put = original
        _reset_binding()
