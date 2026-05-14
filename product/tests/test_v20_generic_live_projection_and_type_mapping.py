# -*- coding: utf-8 -*-
# tests/test_v20_generic_live_projection_and_type_mapping.py
#
# v20: Tests für die generische Live-Projection-Engine und die Type-Mapping-
#      Registry.

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.adapters.compiler import _live_projection_engine as eng
from src.libraries import _type_mapping_registry as tmr


def _row_id_sort_value(row):
    return row.get("id") or 0


# =============================================================================
# 1. Generische Live-Projection-Engine — direkt aufrufen
# =============================================================================

def _make_minimal_callbacks():
    """Minimale Callbacks, die Tests ohne echte Sektor-Semantik ermöglichen."""

    def _sort_rows(rows):
        sorted_rows = [r for r in rows if isinstance(r, dict)]
        sorted_rows.sort(key=_row_id_sort_value)
        return sorted_rows

    def _index_rows(rows):
        idx = {"id": {}}
        for pos, r in enumerate(rows):
            if isinstance(r, dict) and "id" in r:
                idx["id"].setdefault(r["id"], pos)
        return idx

    def _find_existing(index, candidate):
        rid = candidate.get("id")
        if rid in index.get("id", {}):
            return index["id"][rid], None
        return None, None

    def _rows_conflict(existing, candidate):
        if existing.get("id") != candidate.get("id"):
            return True, "id_mismatch"
        if existing.get("kind") and candidate.get("kind") and existing["kind"] != candidate["kind"]:
            return True, "kind_mismatch"
        return False, None

    def _validate_decision(decision):
        reasons = []
        required = ("candidate_ref", "mapping_mode", "merge_policy", "rollback_policy", "resolved_id")
        for k in required:
            if k not in decision:
                reasons.append("missing: %s" % k)
        return reasons

    def _build_row(decision):
        return {
            "id": decision.get("resolved_id"),
            "kind": decision.get("resolved_kind", "default"),
            "data": dict(decision.get("resolved_data") or {}),
        }

    def _merge_on_empty(existing, candidate):
        merged = dict(existing)
        for k, v in (candidate.get("data") or {}).items():
            if k not in (merged.get("data") or {}):
                if "data" not in merged:
                    merged["data"] = {}
                merged["data"][k] = v
        return merged

    def _merge_explicit(existing, candidate):
        merged = dict(existing)
        existing_data = dict(existing.get("data") or {})
        existing_data.update(candidate.get("data") or {})
        merged["data"] = existing_data
        return merged

    def _sort_key_record(record):
        return (record.get("candidate_ref") or "", record.get("resolved_id") or 0)

    return {
        "section_key": "test_section",
        "target_id_field": "id",
        "allowed_merge_policies": ("baseline_wins", "projection_wins_on_empty_only", "explicit_field_merge", "review_only"),
        "allowed_rollback_policies": ("restore_previous_row", "remove_new_row"),
        "allowed_live_projection_modes": ("disabled", "review_only", "apply_resolved"),
        "default_live_projection_mode": "disabled",
        "attach_mode": "attach_to_existing",
        "sort_rows_fn": _sort_rows,
        "index_rows_fn": _index_rows,
        "find_existing_fn": _find_existing,
        "rows_conflict_fn": _rows_conflict,
        "validate_decision_fn": _validate_decision,
        "build_row_fn": _build_row,
        "merge_on_empty_fn": _merge_on_empty,
        "merge_explicit_fn": _merge_explicit,
        "sort_key_mapping_record_fn": _sort_key_record,
    }


def _minimal_decision(ref="d1", rid=1):
    return {
        "candidate_ref": ref,
        "mapping_mode": "create_new",
        "merge_policy": "explicit_field_merge",
        "rollback_policy": "restore_previous_row",
        "resolved_id": rid,
        "resolved_kind": "widget",
        "resolved_data": {"color": "red"},
    }


def test_engine_disabled_returns_none():
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=[],
        contract_meta={},
        mapping_records=[_minimal_decision()],
        enabled=False,
        mode="apply_resolved",
        apply_limit=10,
        allow_review_required=False,
        section_callbacks=_make_minimal_callbacks(),
    )
    assert rows is None
    assert report["status"] == eng.REPORT_STATUS_DISABLED


def test_engine_mapping_missing():
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=[],
        contract_meta={},
        mapping_records=[],
        enabled=True,
        mode="apply_resolved",
        apply_limit=10,
        allow_review_required=False,
        section_callbacks=_make_minimal_callbacks(),
    )
    assert rows is None
    assert report["status"] == eng.REPORT_STATUS_MAPPING_MISSING


def test_engine_review_only_mode():
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=[],
        contract_meta={},
        mapping_records=[_minimal_decision()],
        enabled=True,
        mode="review_only",
        apply_limit=10,
        allow_review_required=False,
        section_callbacks=_make_minimal_callbacks(),
    )
    assert rows is None
    assert report["status"] == eng.REPORT_STATUS_REVIEW_ONLY


def test_engine_apply_limit_zero():
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=[],
        contract_meta={},
        mapping_records=[_minimal_decision()],
        enabled=True,
        mode="apply_resolved",
        apply_limit=0,
        allow_review_required=False,
        section_callbacks=_make_minimal_callbacks(),
    )
    assert rows is None
    assert report["status"] == eng.REPORT_STATUS_APPLY_LIMIT_ZERO


def test_engine_applies_new_row():
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=[],
        contract_meta={"mapping_contract_ref": "test_v1"},
        mapping_records=[_minimal_decision(ref="new_1", rid=42)],
        enabled=True,
        mode="apply_resolved",
        apply_limit=10,
        allow_review_required=False,
        section_callbacks=_make_minimal_callbacks(),
    )
    assert rows is not None
    assert len(rows) == 1
    assert rows[0]["id"] == 42
    assert report["status"] == eng.REPORT_STATUS_APPLIED
    assert "new_1" in report["applied_candidate_refs"]
    assert report["mapping_contract_ref"] == "test_v1"


def test_engine_identity_conflict_blocks():
    existing = [{"id": 1, "kind": "widget", "data": {"color": "red"}}]
    # Candidate mit gleicher id aber anderem kind → Conflict
    decision = _minimal_decision(rid=1)
    decision["resolved_kind"] = "gadget"
    decision["mapping_mode"] = "attach_to_existing"
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=existing,
        contract_meta={},
        mapping_records=[decision],
        enabled=True,
        mode="apply_resolved",
        apply_limit=10,
        allow_review_required=False,
        section_callbacks=_make_minimal_callbacks(),
    )
    assert rows is None
    assert report["status"] == eng.REPORT_STATUS_BLOCKED
    assert any(c.get("reason") == "kind_mismatch" for c in report["conflicts"])


def test_engine_attach_target_missing():
    decision = _minimal_decision(rid=99)
    decision["mapping_mode"] = "attach_to_existing"
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=[],
        contract_meta={},
        mapping_records=[decision],
        enabled=True,
        mode="apply_resolved",
        apply_limit=10,
        allow_review_required=False,
        section_callbacks=_make_minimal_callbacks(),
    )
    assert rows is None
    assert any(c.get("reason") == "attach_target_missing" for c in report["conflicts"])


def test_engine_apply_limit_partial():
    decisions = [_minimal_decision(ref="d%d" % i, rid=i) for i in range(1, 6)]
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=[],
        contract_meta={},
        mapping_records=decisions,
        enabled=True,
        mode="apply_resolved",
        apply_limit=3,
        allow_review_required=False,
        section_callbacks=_make_minimal_callbacks(),
    )
    assert rows is not None
    assert len(rows) == 3
    assert len(report["applied_candidate_refs"]) == 3
    assert any(s.get("reason") == "apply_limit_reached" for s in report["skipped"])


def test_engine_invalid_callbacks():
    rows, report = eng.apply_live_projection_for_section(
        existing_rows=[],
        contract_meta={},
        mapping_records=[_minimal_decision()],
        enabled=True,
        mode="apply_resolved",
        apply_limit=10,
        allow_review_required=False,
        section_callbacks={},  # leer!
    )
    assert rows is None
    assert report["status"] == eng.REPORT_STATUS_DISABLED
    assert any(
        s.get("reason") == "invalid_section_callbacks" for s in report["skipped"]
    )


def test_engine_validate_callbacks_utility():
    errors = eng.validate_section_callbacks({})
    assert len(errors) > 0
    errors_ok = eng.validate_section_callbacks(_make_minimal_callbacks())
    assert errors_ok == []


# =============================================================================
# 2. Type-Mapping-Registry
# =============================================================================

def test_type_mapping_registry_self_validates():
    reasons = tmr.validate_registry()
    assert reasons == [], "registry violations: %s" % reasons


def test_known_types_have_writers():
    assert tmr.lookup_primary_writer("device_state_data_json") == tmr.WRITER_DATASTORE
    assert tmr.lookup_primary_writer("projection_bundle") == tmr.WRITER_CUD_COMPILER
    assert tmr.lookup_primary_writer("cross_worker_event") == tmr.WRITER_SEM
    assert tmr.lookup_primary_writer("worker_hot_state") == tmr.WRITER_WORKER_LOCAL
    assert tmr.lookup_primary_writer("runtime_change_set") == tmr.WRITER_RUNTIME_CAPTURE


def test_unknown_type_returns_none():
    assert tmr.lookup_primary_writer("nonexistent_type") is None
    assert tmr.lookup_entry("nonexistent_type") is None


def test_lookup_entry_contains_scope():
    entry = tmr.lookup_entry("device_state_data_json")
    assert entry is not None
    assert entry["scope"] == tmr.SCOPE_CONFIG_FILE
    assert entry["primary_writer"] == tmr.WRITER_DATASTORE


def test_list_types_by_writer():
    compiler_types = tmr.list_types_by_writer(tmr.WRITER_CUD_COMPILER)
    assert "projection_bundle" in compiler_types
    assert "process_states_live_projection_patch" in compiler_types
    assert "control_methods_live_projection_patch" in compiler_types


def test_list_types_by_scope():
    projection_types = tmr.list_types_by_scope(tmr.SCOPE_PROJECTION_PATCH)
    assert "projection_bundle" in projection_types
    assert "runtime_json_kernel_patch" in projection_types


def test_all_types_nonempty_and_sorted():
    types = tmr.all_types()
    assert len(types) > 5
    assert types == sorted(types)


def test_describe_registry_output():
    desc = tmr.describe_registry()
    assert desc["contract_version"] == tmr.TYPE_MAPPING_CONTRACT_VERSION
    assert desc["total_types"] > 0
    assert tmr.WRITER_DATASTORE in desc["types_by_writer"]
    assert tmr.WRITER_CUD_COMPILER in desc["types_by_writer"]


# =============================================================================
# 3. Backward-Compatibility: bestehende control_methods-Tests laufen weiter
#    (diese Tests sind in test_control_methods_live_projection_v19.py;
#     hier nur ein Spot-Check, dass die Delegation ankommt)
# =============================================================================

def test_control_methods_still_works_via_generic_engine():
    """v20: build_control_methods_live_projection_patch delegiert an die
    generische Engine. Bei enabled=True, mode=apply_resolved, mit einem
    minimalen Mapping muss ein Patch entstehen."""
    from src.adapters.compiler import _projection_bridge as pb

    decision = {
        "candidate_ref": "cm_v20_1",
        "mapping_mode": "create_new_legacy_method",
        "merge_policy": "explicit_field_merge",
        "rollback_policy": "restore_previous_row",
        "resolved_method_id": 3001,
        "resolved_algorithm_type": "pid",
        "resolved_parameters": {"kp": 2.0},
        "resolved_owner_context_ref": "v20_owner",
        "resolved_worker_refs": [],
        "requires_review": False,
    }
    contract = {
        "schema_version": 1,
        "contract_mode": "mapping_contract_guarded_live_projection_v1",
        "section": "control_methods",
        "mapping_contract_ref": "cm_v20_ref",
        "mapping_decisions": [decision],
    }
    activation = {
        "control_methods_live_projection_enabled": True,
        "control_methods_live_projection_mode": "apply_resolved",
        "control_methods_live_projection_apply_limit": 5,
        "control_methods_live_projection_allow_review_required": False,
    }
    patch, report = pb.build_control_methods_live_projection_patch(
        {"control_methods": []},
        {},
        mapping_contract=contract,
        activation_settings=activation,
    )
    assert patch is not None
    assert report["status"] == "applied"
    assert patch["control_methods"][0]["method_id"] == 3001
    assert report["mapping_contract_ref"] == "cm_v20_ref"
