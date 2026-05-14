# -*- coding: utf-8 -*-
# tests/test_event_envelope_v18.py
#
# Tests für v18 Event-Envelope-Contract + Vector Clock + Enrichment.

import os
import sys
from functools import partial

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.libraries import _event_envelope as env


# =============================================================================
# 1. Enrichment
# =============================================================================

def test_enrichment_adds_required_fields():
    event = {"event_type": "CONFIG_CHANGED", "payload": {}}
    seq = env.build_sequence_state()
    enriched = env.enrich_envelope(
        event,
        source_ref="worker_datastore",
        sequence_state=seq,
        node_id="node_main",
    )
    assert enriched["envelope_schema_version"] == env.ENVELOPE_SCHEMA_VERSION
    assert enriched["message_kind"] == "state"
    assert enriched["source_ref"] == "worker_datastore"
    assert enriched["sequence_no"] == 1
    assert "envelope_id" in enriched
    assert "vector_clock" in enriched
    assert enriched["vector_clock"]["node_main"] == 1


def test_enrichment_is_idempotent_without_overwrite():
    event = {
        "event_type": "CONFIG_CHANGED",
        "envelope_id": "fixed-eid",
        "message_kind": "state",
        "sequence_no": 42,
    }
    seq = env.build_sequence_state()
    enriched = env.enrich_envelope(event, source_ref="x", sequence_state=seq)
    # Bereits gesetzte Felder bleiben erhalten
    assert enriched["envelope_id"] == "fixed-eid"
    assert enriched["message_kind"] == "state"
    assert enriched["sequence_no"] == 42


def test_enrichment_overwrite_resets_all():
    event = {
        "event_type": "CONFIG_CHANGED",
        "envelope_id": "old-eid",
        "sequence_no": 99,
    }
    seq = env.build_sequence_state()
    enriched = env.enrich_envelope(
        event, source_ref="x", sequence_state=seq, overwrite=True
    )
    assert enriched["envelope_id"] != "old-eid"
    assert enriched["sequence_no"] == 1


def test_message_kind_derivation_heuristics():
    for etype, expected in [
        ("CONFIG_CHANGED", "state"),
        ("SET_VALUE_REQUEST", "intent"),
        ("WRITE_RESULT", "result"),
        ("START_COMMAND", "command"),
        ("SENSOR_TELEMETRY", "telemetry"),
        ("USER_AUDIT", "audit"),
        ("PAUSE_CONTROL", "control"),
        ("NOTHING_MATCHING", "unknown"),
    ]:
        derived = env._derive_message_kind_from_event({"event_type": etype})
        assert derived == expected, "for %s expected %s, got %s" % (etype, expected, derived)


def test_explicit_message_kind_is_kept():
    derived = env._derive_message_kind_from_event(
        {"event_type": "XXX_UNUSED", "message_kind": "intent"}
    )
    assert derived == "intent"


# =============================================================================
# 2. Sequence Counter
# =============================================================================

def test_sequence_is_monotonic_per_source():
    seq = env.build_sequence_state()
    assert env.next_sequence_no(seq, "a") == 1
    assert env.next_sequence_no(seq, "a") == 2
    assert env.next_sequence_no(seq, "b") == 1
    assert env.next_sequence_no(seq, "a") == 3


# =============================================================================
# 3. Vector Clocks
# =============================================================================

def test_empty_vector_clock():
    assert env.empty_vector_clock() == {}


def test_advance_vector_clock_does_not_mutate_input():
    vc = {"n1": 1}
    advanced = env.advance_vector_clock(vc, "n1")
    assert advanced == {"n1": 2}
    assert vc == {"n1": 1}  # Original unverändert


def test_advance_new_node():
    advanced = env.advance_vector_clock({"n1": 3}, "n2")
    assert advanced == {"n1": 3, "n2": 1}


def test_merge_vector_clocks():
    merged = env.merge_vector_clocks({"n1": 2, "n2": 3}, {"n1": 5, "n3": 1})
    assert merged == {"n1": 5, "n2": 3, "n3": 1}


def test_happens_before_true():
    assert env.happens_before({"n1": 1}, {"n1": 2}) is True
    assert env.happens_before({"n1": 1}, {"n1": 1, "n2": 1}) is True


def test_happens_before_false_when_concurrent():
    assert env.happens_before({"n1": 2}, {"n1": 1}) is False
    assert env.happens_before({"n1": 1, "n2": 2}, {"n1": 2, "n2": 1}) is False


def test_happens_before_false_when_equal():
    assert env.happens_before({"n1": 1}, {"n1": 1}) is False


def test_concurrent_detection():
    assert env.concurrent({"n1": 1, "n2": 2}, {"n1": 2, "n2": 1}) is True
    assert env.concurrent({"n1": 1}, {"n1": 2}) is False


# =============================================================================
# 4. Validation
# =============================================================================

def test_validate_envelope_lenient():
    event = {"event_type": "X", "message_kind": "intent"}
    ok, reasons = env.validate_envelope(event, strict=False)
    assert ok is True
    assert reasons == []


def test_validate_envelope_strict_missing():
    event = {"event_type": "X"}
    ok, reasons = env.validate_envelope(event, strict=True)
    assert ok is False
    assert len(reasons) > 0


def test_validate_envelope_strict_complete():
    event = {
        "envelope_schema_version": 1,
        "envelope_id": "eid",
        "message_kind": "intent",
        "source_ref": "x",
        "sequence_no": 1,
    }
    ok, reasons = env.validate_envelope(event, strict=True)
    assert ok is True, "reasons: %s" % reasons


def test_validate_envelope_bad_message_kind():
    event = {"message_kind": "nonsense_kind"}
    ok, reasons = env.validate_envelope(event, strict=False)
    assert ok is False
    assert any("message_kind" in r for r in reasons)


def test_validate_envelope_bad_trust_level():
    event = {"trust_level": "galactic"}
    ok, reasons = env.validate_envelope(event, strict=False)
    assert ok is False
    assert any("trust_level" in r for r in reasons)


def test_validate_envelope_wrong_version():
    event = {"envelope_schema_version": 99}
    ok, reasons = env.validate_envelope(event, strict=False)
    assert ok is False


# =============================================================================
# 5. Broker Enrichment Factory
# =============================================================================

def test_build_broker_enrichment_fn():
    fn = env.build_broker_enrichment_fn(
        source_ref="broker_1",
        node_id="node_main",
        trust_level="internal",
    )
    event1 = {"event_type": "CONFIG_CHANGED"}
    event2 = {"event_type": "SET_REQUEST"}
    out1 = fn(event1)
    out2 = fn(event2)
    # Beide Events tragen source_ref="broker_1" → Sequenzzähler für
    # broker_1 wird pro Enrichment erhöht
    assert out1["sequence_no"] == 1
    assert out2["sequence_no"] == 2
    assert out1["source_ref"] == "broker_1"
    assert out2["source_ref"] == "broker_1"


def test_describe_envelope_extracts_fields():
    event = {
        "event_type": "X",
        "envelope_id": "eid",
        "message_kind": "state",
        "source_ref": "src",
        "sequence_no": 7,
        "vector_clock": {"n1": 1},
    }
    desc = env.describe_envelope(event)
    assert desc["envelope_id"] == "eid"
    assert desc["message_kind"] == "state"
    assert desc["sequence_no"] == 7
    assert desc["vector_clock"] == {"n1": 1}
