# -*- coding: utf-8 -*-
# tests/test_v21_consistency_zones_and_causation_tracker.py
#
# v21: Tests für:
#   - Konsistenzzonen-Feld in der Type-Mapping-Registry
#   - Automatischer causation_id-Tracker
#   - process_states-Umstellung auf die generische Engine

import os
import sys
from functools import partial

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.libraries import _type_mapping_registry as tmr
from src.libraries import _event_envelope as env
from src.adapters.compiler import _projection_bridge as pb


# =============================================================================
# 1. Type-Mapping-Registry: Konsistenzzonen
# =============================================================================

def test_contract_version_bumped_to_2():
    assert tmr.TYPE_MAPPING_CONTRACT_VERSION == 2


def test_all_consistency_zones_defined():
    assert tmr.CONSISTENCY_ZONE_STRICT == "strict"
    assert tmr.CONSISTENCY_ZONE_CAUSAL == "causal"
    assert tmr.CONSISTENCY_ZONE_EVENTUAL == "eventual"
    assert tmr.CONSISTENCY_ZONE_READ_ONLY == "read_only"
    assert len(tmr.ALL_CONSISTENCY_ZONES) == 4


def test_registry_validates_with_consistency_zones():
    """Nach v21-Erweiterung muss validate_registry leer bleiben."""
    reasons = tmr.validate_registry()
    assert reasons == [], "registry violations: %s" % reasons


def test_lookup_consistency_zone_known_types():
    # Config-Datei strikt
    assert tmr.lookup_consistency_zone("device_state_data_json") == tmr.CONSISTENCY_ZONE_STRICT
    # Projection-Patches strikt
    assert tmr.lookup_consistency_zone("projection_bundle") == tmr.CONSISTENCY_ZONE_STRICT
    assert tmr.lookup_consistency_zone("runtime_json_kernel_patch") == tmr.CONSISTENCY_ZONE_STRICT
    # Events kausal
    assert tmr.lookup_consistency_zone("cross_worker_event") == tmr.CONSISTENCY_ZONE_CAUSAL
    assert tmr.lookup_consistency_zone("local_worker_event") == tmr.CONSISTENCY_ZONE_CAUSAL
    # Runtime-Change-Set kausal (über envelope_id gereiht)
    assert tmr.lookup_consistency_zone("runtime_change_set") == tmr.CONSISTENCY_ZONE_CAUSAL
    # Reverse-Compile-Output eventual
    assert tmr.lookup_consistency_zone("reverse_compile_output") == tmr.CONSISTENCY_ZONE_EVENTUAL
    # Edge-Telemetry eventual
    assert tmr.lookup_consistency_zone("edge_inbound_telemetry") == tmr.CONSISTENCY_ZONE_EVENTUAL


def test_lookup_consistency_zone_unknown_returns_none():
    assert tmr.lookup_consistency_zone("nonexistent_type") is None


def test_list_types_by_consistency_zone():
    strict_types = tmr.list_types_by_consistency_zone(tmr.CONSISTENCY_ZONE_STRICT)
    assert "device_state_data_json" in strict_types
    assert "projection_bundle" in strict_types
    assert "worker_hot_state" in strict_types

    causal_types = tmr.list_types_by_consistency_zone(tmr.CONSISTENCY_ZONE_CAUSAL)
    assert "cross_worker_event" in causal_types
    assert "local_worker_event" in causal_types
    assert "runtime_change_set" in causal_types

    eventual_types = tmr.list_types_by_consistency_zone(tmr.CONSISTENCY_ZONE_EVENTUAL)
    assert "reverse_compile_output" in eventual_types
    assert "edge_inbound_telemetry" in eventual_types


def test_describe_registry_includes_zones():
    desc = tmr.describe_registry()
    assert desc["contract_version"] == 2
    assert "consistency_zones" in desc
    assert "types_by_consistency_zone" in desc
    assert tmr.CONSISTENCY_ZONE_STRICT in desc["types_by_consistency_zone"]
    assert len(desc["types_by_consistency_zone"][tmr.CONSISTENCY_ZONE_STRICT]) > 0


# =============================================================================
# 2. Automatischer causation_id-Tracker
# =============================================================================

def test_causation_tracker_build():
    tracker = env.build_causation_tracker()
    assert tracker is not None
    assert isinstance(tracker, dict)
    assert "lock" in tracker
    assert "last_envelope_id" in tracker


def test_record_and_lookup_envelope():
    tracker = env.build_causation_tracker()
    env.record_envelope(tracker, "src_a", "eid_1")
    env.record_envelope(tracker, "src_b", "eid_2")
    env.record_envelope(tracker, "src_a", "eid_3")  # überschreibt

    assert env.lookup_last_envelope_id(tracker, "src_a") == "eid_3"
    assert env.lookup_last_envelope_id(tracker, "src_b") == "eid_2"
    assert env.lookup_last_envelope_id(tracker, "src_c") is None


def test_lookup_on_none_tracker_is_safe():
    assert env.lookup_last_envelope_id(None, "any") is None
    # record_envelope mit None soll kein Fehler werfen
    env.record_envelope(None, "any", "eid_x")  # No-Op


def test_broker_enrichment_without_tracker_sets_no_causation():
    """v18-Verhalten bleibt: ohne Tracker keine automatische causation_id."""
    fn = env.build_broker_enrichment_fn_with_tracker(
        source_ref="broker_test",
        node_id="node_test",
        trust_level="internal",
        causation_tracker=None,
    )
    evt1 = {"event_type": "CONFIG_CHANGED"}
    evt2 = {"event_type": "CONFIG_CHANGED"}
    out1 = fn(evt1)
    out2 = fn(evt2)
    assert "causation_id" not in out1
    assert "causation_id" not in out2


def test_broker_enrichment_with_tracker_chains_events():
    """v21-Kerntest: Events einer source erhalten automatisch die envelope_id
    des direkten Vorgängers als causation_id."""
    tracker = env.build_causation_tracker()
    fn = env.build_broker_enrichment_fn_with_tracker(
        source_ref="src_chain",
        node_id="node_test",
        trust_level="internal",
        causation_tracker=tracker,
    )

    evt1 = {"event_type": "CONFIG_CHANGED"}
    evt2 = {"event_type": "CONFIG_CHANGED"}
    evt3 = {"event_type": "CONFIG_CHANGED"}

    out1 = fn(evt1)
    out2 = fn(evt2)
    out3 = fn(evt3)

    # Erstes Event hat keine causation_id (kein Vorgänger)
    assert "causation_id" not in out1

    # Zweites und drittes Event werden verkettet
    assert out2.get("causation_id") == out1["envelope_id"]
    assert out3.get("causation_id") == out2["envelope_id"]


def test_broker_enrichment_different_sources_isolated():
    """Events verschiedener sources werden NICHT verkettet."""
    tracker = env.build_causation_tracker()

    fn_a = env.build_broker_enrichment_fn_with_tracker(
        source_ref="src_a",
        causation_tracker=tracker,
    )
    fn_b = env.build_broker_enrichment_fn_with_tracker(
        source_ref="src_b",
        causation_tracker=tracker,
    )

    # zwei Events aus src_a, dann eins aus src_b
    a1 = fn_a({"event_type": "X"})
    a2 = fn_a({"event_type": "X"})
    b1 = fn_b({"event_type": "Y"})

    # a2 verkettet mit a1
    assert a2.get("causation_id") == a1["envelope_id"]
    # b1 hat KEINE causation_id (erstes Event aus src_b)
    assert "causation_id" not in b1


def test_explicit_causation_id_is_preserved():
    """Wenn der Producer explizit eine causation_id setzt, bleibt sie erhalten."""
    tracker = env.build_causation_tracker()
    fn = env.build_broker_enrichment_fn_with_tracker(
        source_ref="src_x",
        causation_tracker=tracker,
    )

    fn({"event_type": "X"})  # legt last-envelope ab
    # Zweites Event mit explizit gesetzter causation_id
    evt = {"event_type": "X", "causation_id": "explicit_custom_id"}
    out = fn(evt)
    assert out["causation_id"] == "explicit_custom_id"


def test_tracker_thread_safety_basic():
    """Basischeck: mehrere record-Operationen verlieren keine Envelope-IDs."""
    tracker = env.build_causation_tracker()
    for i in range(100):
        env.record_envelope(tracker, "bulk_src", "eid_%d" % i)
    # Letzte sollte die Nummer 99 sein
    assert env.lookup_last_envelope_id(tracker, "bulk_src") == "eid_99"


# =============================================================================
# 3. process_states via generische Engine
# =============================================================================

def _minimal_process_state_decision(
    candidate_ref="ps_v21_1",
    state_id=5001,
    mapping_id=6001,
    p_id=7001,
    value_id=8001,
    mode_id=9001,
    mapping_mode="create_new_legacy_state",
):
    return {
        "candidate_ref": candidate_ref,
        "mapping_mode": mapping_mode,
        "merge_policy": "explicit_field_merge",
        "rollback_policy": "restore_previous_row",
        "mapping_decision_ref": "md_ref_1",
        "state_id_strategy": "explicit",
        "resolved_state_id": state_id,
        "resolved_p_id": p_id,
        "resolved_value_id": value_id,
        "resolved_mode_id": mode_id,
        "resolved_mapping_id": mapping_id,
        "resolved_sensor_ids": [],
        "resolved_actuator_ids": [],
        "parameters_strategy": "explicit",
        "control_strategy_strategy": "explicit",
        "setpoint_source_strategy": "explicit",
        "owner_context_ref": "v21_owner",
        "worker_refs": [],
        "requires_review": False,
    }


def _minimal_activation_process_states(enabled=True, mode="apply_resolved", apply_limit=10):
    return {
        "process_states_live_projection_enabled": enabled,
        "process_states_live_projection_mode": mode,
        "process_states_live_projection_apply_limit": apply_limit,
        "process_states_live_projection_allow_review_required": False,
    }


def test_process_states_disabled_default():
    patch, report = pb.build_process_states_live_projection_patch(
        {"process_states": []},
        {},
        mapping_contract={"mapping_decisions": [_minimal_process_state_decision()]},
        activation_settings=_minimal_activation_process_states(enabled=False),
    )
    assert patch is None
    assert report["status"] == "disabled"


def test_process_states_requires_sidecar_registration():
    """v14-Logik bleibt: Decision ohne Sidecar-Eintrag wird als invalid_mapping_decision
    geskipt (Sidecar-Check ist process_states-spezifisch)."""
    patch, report = pb.build_process_states_live_projection_patch(
        {"process_states": []},
        {"sidecars": {"process_state_projection_sidecar": {"records": []}}},
        mapping_contract={"mapping_decisions": [_minimal_process_state_decision(candidate_ref="not_in_sidecar")]},
        activation_settings=_minimal_activation_process_states(),
    )
    assert patch is None
    # Alle Decisions werden geskipt, weil candidate_ref nicht im Sidecar ist
    assert any(
        entry.get("reason") == "invalid_mapping_decision" for entry in report["skipped"]
    )


def test_process_states_applies_when_sidecar_matches():
    """Happy-Path: Sidecar hat den Candidate-Ref, Decision wird appliziert."""
    decision = _minimal_process_state_decision(candidate_ref="ps_ok_1")
    projection = {
        "sidecars": {
            "process_state_projection_sidecar": {
                "records": [{"candidate_ref": "ps_ok_1"}],
            },
        },
    }
    patch, report = pb.build_process_states_live_projection_patch(
        {"process_states": []},
        projection,
        mapping_contract={"mapping_decisions": [decision]},
        activation_settings=_minimal_activation_process_states(),
    )
    assert patch is not None, "report: %s" % report
    assert report["status"] == "applied"
    assert "ps_ok_1" in report["applied_candidate_refs"]
    assert patch["process_states"][0]["state_id"] == 5001


def test_process_states_apply_limit_applies_via_engine():
    """apply_limit-Guardrail greift identisch zur Engine-Version."""
    decisions = [_minimal_process_state_decision(
        candidate_ref="ps_limit_%d" % i,
        state_id=5000 + i,
        mapping_id=6000 + i,
    ) for i in range(1, 6)]
    sidecar_records = [{"candidate_ref": "ps_limit_%d" % i} for i in range(1, 6)]

    patch, report = pb.build_process_states_live_projection_patch(
        {"process_states": []},
        {"sidecars": {"process_state_projection_sidecar": {"records": sidecar_records}}},
        mapping_contract={"mapping_decisions": decisions},
        activation_settings=_minimal_activation_process_states(apply_limit=2),
    )
    assert patch is not None
    assert len(patch["process_states"]) == 2
    assert any(
        entry.get("reason") == "apply_limit_reached" for entry in report["skipped"]
    )
