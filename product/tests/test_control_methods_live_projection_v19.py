# -*- coding: utf-8 -*-
# tests/test_control_methods_live_projection_v19.py
#
# v19: Tests für build_control_methods_live_projection_patch.
# Deckt alle Guardrail-Zustände analog zu den process_states-Tests ab.

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.adapters.compiler import _projection_bridge as pb


def _minimal_mapping_decision(
    candidate_ref="cm_test_1",
    method_id=2001,
    algorithm_type="pid",
    parameters=None,
    mapping_mode="create_new_legacy_method",
    merge_policy="explicit_field_merge",
    rollback_policy="restore_previous_row",
    requires_review=False,
):
    if parameters is None:
        parameters = {"kp": 1.0, "ki": 0.1, "kd": 0.0}
    return {
        "candidate_ref": candidate_ref,
        "mapping_mode": mapping_mode,
        "merge_policy": merge_policy,
        "rollback_policy": rollback_policy,
        "resolved_method_id": method_id,
        "resolved_algorithm_type": algorithm_type,
        "resolved_parameters": parameters,
        "resolved_owner_context_ref": "test_owner",
        "resolved_worker_refs": [],
        "requires_review": requires_review,
    }


def _minimal_contract(decisions):
    return {
        "schema_version": 1,
        "contract_mode": "mapping_contract_guarded_live_projection_v1",
        "section": "control_methods",
        "mapping_contract_ref": "cm_contract_test_v1",
        "mapping_decisions": decisions,
    }


def _minimal_activation(
    enabled=True,
    mode="apply_resolved",
    apply_limit=10,
    allow_review_required=False,
):
    return {
        "control_methods_live_projection_enabled": enabled,
        "control_methods_live_projection_mode": mode,
        "control_methods_live_projection_apply_limit": apply_limit,
        "control_methods_live_projection_allow_review_required": allow_review_required,
    }


# =============================================================================
# Guardrail-Zustände
# =============================================================================

def test_disabled_when_enabled_flag_false():
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([_minimal_mapping_decision()]),
        activation_settings=_minimal_activation(enabled=False),
    )
    assert patch is None
    assert report["status"] == "disabled"
    assert report["enabled"] is False


def test_disabled_when_mode_is_disabled():
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([_minimal_mapping_decision()]),
        activation_settings=_minimal_activation(mode="disabled"),
    )
    assert patch is None
    assert report["status"] == "disabled"


def test_mapping_contract_missing():
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=None,
        activation_settings=_minimal_activation(),
    )
    assert patch is None
    assert report["status"] == "mapping_contract_missing"


def test_review_only_mode():
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([_minimal_mapping_decision()]),
        activation_settings=_minimal_activation(mode="review_only"),
    )
    assert patch is None
    assert report["status"] == "review_only"


def test_apply_limit_zero_blocks_everything():
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([_minimal_mapping_decision()]),
        activation_settings=_minimal_activation(apply_limit=0),
    )
    assert patch is None
    assert report["status"] == "apply_limit_zero"


# =============================================================================
# Apply-Flow
# =============================================================================

def test_new_method_gets_applied():
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([_minimal_mapping_decision(
            candidate_ref="cm_pid_1",
            method_id=2001,
        )]),
        activation_settings=_minimal_activation(),
    )
    assert patch is not None
    assert "control_methods" in patch
    assert len(patch["control_methods"]) == 1
    row = patch["control_methods"][0]
    assert row["method_id"] == 2001
    assert row["algorithm_type"] == "pid"
    assert row["parameters"]["kp"] == 1.0
    assert report["status"] == "applied"
    assert "cm_pid_1" in report["applied_candidate_refs"]


def test_apply_limit_restricts_count():
    decisions = [
        _minimal_mapping_decision(candidate_ref="cm_1", method_id=2001),
        _minimal_mapping_decision(candidate_ref="cm_2", method_id=2002),
        _minimal_mapping_decision(candidate_ref="cm_3", method_id=2003),
    ]
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract(decisions),
        activation_settings=_minimal_activation(apply_limit=2),
    )
    assert patch is not None
    assert len(patch["control_methods"]) == 2
    assert len(report["applied_candidate_refs"]) == 2
    # Dritter Eintrag als "apply_limit_reached" skipped
    assert any(
        entry.get("reason") == "apply_limit_reached" for entry in report["skipped"]
    )


def test_review_required_is_skipped_by_default():
    decision = _minimal_mapping_decision(requires_review=True)
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(allow_review_required=False),
    )
    assert patch is None
    assert any(
        entry.get("reason") == "review_required" for entry in report["skipped"]
    )


def test_review_required_is_applied_when_allowed():
    decision = _minimal_mapping_decision(requires_review=True)
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(allow_review_required=True),
    )
    assert patch is not None
    assert report["status"] == "applied"


def test_merge_policy_review_only_skips():
    decision = _minimal_mapping_decision(merge_policy="review_only")
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is None
    assert any(
        entry.get("reason") == "merge_policy_review_only" for entry in report["skipped"]
    )


# =============================================================================
# Merge-Flow
# =============================================================================

def test_baseline_wins_does_not_merge():
    existing = [{
        "method_id": 2001,
        "algorithm_type": "pid",
        "parameters": {"kp": 5.0},
        "owner_context_ref": "original_owner",
        "worker_refs": [],
    }]
    decision = _minimal_mapping_decision(
        method_id=2001,
        merge_policy="baseline_wins",
        mapping_mode="attach_to_existing_legacy_method",
    )
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": existing},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is None
    assert any(
        entry.get("reason") == "baseline_wins_existing_row" for entry in report["skipped"]
    )


def test_projection_wins_on_empty_only_merges_only_empty_fields():
    existing = [{
        "method_id": 2001,
        "algorithm_type": "pid",
        "parameters": {"kp": 5.0},
        "owner_context_ref": "",
        "worker_refs": [],
    }]
    decision = _minimal_mapping_decision(
        method_id=2001,
        merge_policy="projection_wins_on_empty_only",
        mapping_mode="attach_to_existing_legacy_method",
        parameters={"kp": 99.0, "ki": 0.1},
    )
    decision["resolved_owner_context_ref"] = "new_owner"
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": existing},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is not None
    merged = patch["control_methods"][0]
    # existing parameters bleiben erhalten, da nicht empty
    assert merged["parameters"]["kp"] == 5.0
    # owner_context_ref war empty → projection gewinnt
    assert merged["owner_context_ref"] == "new_owner"


def test_explicit_field_merge_overwrites_params():
    existing = [{
        "method_id": 2001,
        "algorithm_type": "pid",
        "parameters": {"kp": 5.0, "ki": 0.2},
        "owner_context_ref": "original",
        "worker_refs": [],
    }]
    decision = _minimal_mapping_decision(
        method_id=2001,
        merge_policy="explicit_field_merge",
        mapping_mode="attach_to_existing_legacy_method",
        parameters={"kp": 99.0, "kd": 0.5},
    )
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": existing},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is not None
    merged = patch["control_methods"][0]
    # kp wird überschrieben, ki bleibt, kd wird ergänzt
    assert merged["parameters"]["kp"] == 99.0
    assert merged["parameters"]["ki"] == 0.2
    assert merged["parameters"]["kd"] == 0.5


def test_attach_target_missing_is_conflict():
    decision = _minimal_mapping_decision(
        method_id=2999,
        mapping_mode="attach_to_existing_legacy_method",
    )
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},  # kein Row mit method_id=2999
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is None
    assert any(
        entry.get("reason") == "attach_target_missing" for entry in report["conflicts"]
    )


def test_conflicting_identity_blocks_merge():
    """v19: Wenn method_id übereinstimmt, aber algorithm_type abweicht,
    ist das ein unauflösbarer Identity-Konflikt."""
    existing = [{
        "method_id": 2001,
        "algorithm_type": "pid",
        "parameters": {"kp": 5.0},
        "owner_context_ref": "",
        "worker_refs": [],
    }]
    decision = _minimal_mapping_decision(
        method_id=2001,
        algorithm_type="step_response",  # abweichende Identity
        merge_policy="explicit_field_merge",
        mapping_mode="attach_to_existing_legacy_method",
    )
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": existing},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is None
    assert any(
        entry.get("reason") == "conflicting_control_method_identity"
        for entry in report["conflicts"]
    )


# =============================================================================
# Rollback-Preview
# =============================================================================

def test_rollback_preview_for_new_row():
    decision = _minimal_mapping_decision(method_id=2001)
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is not None
    assert len(report["rollback_preview"]) == 1
    preview = report["rollback_preview"][0]
    assert preview["rollback_policy"] == "restore_previous_row"
    assert preview["target_method_id"] == 2001
    assert preview["previous_row"] is None


# =============================================================================
# Ungültige Mapping-Entscheidungen
# =============================================================================

def test_invalid_mapping_decision_missing_required_fields():
    decision = {
        "candidate_ref": "cm_broken",
        "mapping_mode": "create_new_legacy_method",
        # merge_policy / rollback_policy / resolved_* fehlen
    }
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is None
    assert any(
        entry.get("reason") == "invalid_mapping_decision" for entry in report["skipped"]
    )


def test_invalid_mapping_mode_is_reported():
    decision = _minimal_mapping_decision(mapping_mode="nonsense_mode")
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=_minimal_contract([decision]),
        activation_settings=_minimal_activation(),
    )
    assert patch is None
    assert any(
        entry.get("reason") == "invalid_mapping_decision" for entry in report["skipped"]
    )
