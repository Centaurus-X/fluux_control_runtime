# -*- coding: utf-8 -*-
# src/adapters/compiler/_live_projection_engine.py
#
# v20: Generische Live-Projection-Engine.
#
# Zweck:
#   Konsolidiert die strukturell identische Apply/Rollback-Logik aus
#   `process_states` (v14) und `control_methods` (v19) in eine einzige,
#   sektor-agnostische Funktion.
#
# Entwurfslinien:
#   - Keine Klassen, keine Decorator.
#   - Sektor-spezifische Logik wird als Callbacks in einem dict übergeben.
#   - Identische Guardrail-Kaskade, Merge-Policy-Verzweigung und
#     Rollback-Preview-Struktur wie bei process_states/control_methods.
#   - Die bestehenden Section-spezifischen Funktionen
#     build_process_states_live_projection_patch(...) und
#     build_control_methods_live_projection_patch(...) rufen diese
#     generische Engine auf und bleiben damit API-stabil.
#
# Report-Status (unverändert):
#   disabled | mapping_contract_missing | review_only | apply_limit_zero |
#   applied | blocked_by_conflicts | no_changes_applied

import copy


REPORT_STATUS_DISABLED = "disabled"
REPORT_STATUS_MAPPING_MISSING = "mapping_contract_missing"
REPORT_STATUS_REVIEW_ONLY = "review_only"
REPORT_STATUS_APPLY_LIMIT_ZERO = "apply_limit_zero"
REPORT_STATUS_APPLIED = "applied"
REPORT_STATUS_BLOCKED = "blocked_by_conflicts"
REPORT_STATUS_NO_CHANGES = "no_changes_applied"

ALL_REPORT_STATUSES = (
    REPORT_STATUS_DISABLED,
    REPORT_STATUS_MAPPING_MISSING,
    REPORT_STATUS_REVIEW_ONLY,
    REPORT_STATUS_APPLY_LIMIT_ZERO,
    REPORT_STATUS_APPLIED,
    REPORT_STATUS_BLOCKED,
    REPORT_STATUS_NO_CHANGES,
)


# ---------------------------------------------------------------------------
# Callback-Dict: Erwartete Schlüssel
# ---------------------------------------------------------------------------
#
# section_callbacks = {
#     "section_key":              str,    # z.B. "process_states" / "control_methods"
#     "target_id_field":          str,    # z.B. "state_id" / "method_id"
#     "allowed_merge_policies":   tuple,
#     "allowed_rollback_policies": tuple,
#     "allowed_live_projection_modes": tuple,
#     "default_live_projection_mode":  str,
#     "allowed_mapping_modes":    tuple,  # z.B. ("attach_to_existing_legacy_state",
#                                         #       "create_new_legacy_state",
#                                         #       "review_only")
#     "attach_mode":              str,    # "attach_to_existing_legacy_*"
#     "sort_rows_fn":             callable(rows) -> sorted list
#     "index_rows_fn":            callable(rows) -> index dict
#     "find_existing_fn":         callable(index, candidate_row) -> (idx_or_None, reason_or_None)
#     "rows_conflict_fn":         callable(existing, candidate) -> (bool, reason_or_None)
#     "validate_decision_fn":     callable(decision) -> list[str]
#     "build_row_fn":             callable(decision) -> row dict
#     "merge_on_empty_fn":        callable(existing, candidate) -> merged row
#     "merge_explicit_fn":        callable(existing, candidate) -> merged row
#     "sort_key_mapping_record_fn": callable(record) -> sort-key tuple
# }


_REQUIRED_CALLBACK_KEYS = (
    "section_key",
    "target_id_field",
    "allowed_merge_policies",
    "allowed_rollback_policies",
    "allowed_live_projection_modes",
    "default_live_projection_mode",
    "attach_mode",
    "sort_rows_fn",
    "index_rows_fn",
    "find_existing_fn",
    "rows_conflict_fn",
    "validate_decision_fn",
    "build_row_fn",
    "merge_on_empty_fn",
    "merge_explicit_fn",
    "sort_key_mapping_record_fn",
)


def validate_section_callbacks(section_callbacks):
    """Stellt sicher, dass ein Callback-Dict alle Pflichtschlüssel hat.
    Rückgabe: Liste von Fehlermeldungen (leer = ok).
    """
    reasons = []
    if not isinstance(section_callbacks, dict):
        return ["section_callbacks must be a dict"]
    for key in _REQUIRED_CALLBACK_KEYS:
        if key not in section_callbacks:
            reasons.append("section_callbacks missing required key: %s" % key)
            continue
        value = section_callbacks[key]
        if key.endswith("_fn") and not callable(value):
            reasons.append("section_callbacks[%s] must be callable" % key)
    return reasons


def _empty_report(enabled, mode, apply_limit, allow_review, contract_meta, mapping_records):
    return {
        "enabled": enabled,
        "mode": mode,
        "apply_limit": apply_limit,
        "allow_review_required": allow_review,
        "mapping_contract_present": bool(mapping_records),
        "mapping_contract_ref": contract_meta.get("mapping_contract_ref") if isinstance(contract_meta, dict) else None,
        "applied_candidate_refs": [],
        "skipped": [],
        "conflicts": [],
        "rollback_preview": [],
        "status": REPORT_STATUS_DISABLED,
    }


def apply_live_projection_for_section(
    existing_rows,
    contract_meta,
    mapping_records,
    enabled,
    mode,
    apply_limit,
    allow_review_required,
    section_callbacks,
):
    """Generische Live-Projection-Apply-Funktion.

    Parameter:
      existing_rows       : list — bestehende Rows der Sektion (aus base_config)
      contract_meta       : dict — Contract-Metadaten (ohne mapping_decisions)
      mapping_records     : list — Mapping-Entscheidungen aus dem Contract
      enabled             : bool — Activation-Flag
      mode                : str — disabled | review_only | apply_resolved
      apply_limit         : int — Obergrenze applizierter Entscheidungen
      allow_review_required: bool
      section_callbacks   : dict — siehe Format oben

    Rückgabe:
      (rows_or_None, report_dict)

    Garantien:
      - Identity-Konflikte werden aus section_callbacks["rows_conflict_fn"]
        erkannt und als "conflicts" gemeldet, nicht als Exception geworfen.
      - Report hat immer status ∈ ALL_REPORT_STATUSES.
      - Bei disabled/mapping_missing/review_only/apply_limit_zero wird
        rows_or_None = None zurückgegeben.
    """
    # Callbacks validieren
    cb_errors = validate_section_callbacks(section_callbacks)
    if cb_errors:
        report = _empty_report(enabled, mode, apply_limit, allow_review_required, contract_meta or {}, mapping_records)
        report["status"] = REPORT_STATUS_DISABLED
        report["skipped"].append({
            "reason": "invalid_section_callbacks",
            "details": cb_errors,
        })
        return None, report

    target_id_field = section_callbacks["target_id_field"]
    attach_mode = section_callbacks["attach_mode"]
    allowed_merge_policies = section_callbacks["allowed_merge_policies"]
    sort_rows_fn = section_callbacks["sort_rows_fn"]
    index_rows_fn = section_callbacks["index_rows_fn"]
    find_existing_fn = section_callbacks["find_existing_fn"]
    rows_conflict_fn = section_callbacks["rows_conflict_fn"]
    validate_decision_fn = section_callbacks["validate_decision_fn"]
    build_row_fn = section_callbacks["build_row_fn"]
    merge_on_empty_fn = section_callbacks["merge_on_empty_fn"]
    merge_explicit_fn = section_callbacks["merge_explicit_fn"]
    sort_key_mapping_record_fn = section_callbacks["sort_key_mapping_record_fn"]

    report = _empty_report(
        enabled, mode, apply_limit, allow_review_required, contract_meta or {}, mapping_records
    )

    if not enabled or mode == "disabled":
        report["status"] = REPORT_STATUS_DISABLED
        return None, report

    if not mapping_records:
        report["status"] = REPORT_STATUS_MAPPING_MISSING
        return None, report

    if mode != "apply_resolved":
        report["status"] = REPORT_STATUS_REVIEW_ONLY
        return None, report

    if apply_limit == 0:
        report["status"] = REPORT_STATUS_APPLY_LIMIT_ZERO
        return None, report

    existing_sorted = sort_rows_fn(existing_rows or [])
    rows = copy.deepcopy(existing_sorted)
    index = index_rows_fn(rows)

    sorted_records = []
    for record in mapping_records:
        if not isinstance(record, dict):
            continue
        sorted_records.append(copy.deepcopy(record))
    sorted_records.sort(key=sort_key_mapping_record_fn)

    applied = []

    for record in sorted_records:
        if len(applied) >= apply_limit:
            report["skipped"].append({
                "candidate_ref": record.get("candidate_ref"),
                "reason": "apply_limit_reached",
            })
            continue

        candidate_ref = record.get("candidate_ref")
        reasons = validate_decision_fn(record)
        if not candidate_ref:
            reasons.append("mapping decision requires non-empty candidate_ref")
        if reasons:
            report["skipped"].append({
                "candidate_ref": candidate_ref,
                "reason": "invalid_mapping_decision",
                "details": reasons,
            })
            continue

        if bool(record.get("requires_review", False)) and not allow_review_required:
            report["skipped"].append({
                "candidate_ref": candidate_ref,
                "reason": "review_required",
            })
            continue

        merge_policy = str(record.get("merge_policy"))
        if merge_policy not in allowed_merge_policies:
            report["skipped"].append({
                "candidate_ref": candidate_ref,
                "reason": "unsupported_merge_policy",
                "details": [merge_policy],
            })
            continue

        if merge_policy == "review_only":
            report["skipped"].append({
                "candidate_ref": candidate_ref,
                "reason": "merge_policy_review_only",
            })
            continue

        candidate_row = build_row_fn(record)
        existing_index, index_reason = find_existing_fn(index, candidate_row)
        if index_reason is not None:
            report["conflicts"].append({
                "candidate_ref": candidate_ref,
                "reason": index_reason,
            })
            continue

        mapping_mode = str(record.get("mapping_mode"))
        rollback_policy = str(record.get("rollback_policy"))

        if existing_index is None and mapping_mode == attach_mode:
            report["conflicts"].append({
                "candidate_ref": candidate_ref,
                "reason": "attach_target_missing",
            })
            continue

        if existing_index is not None:
            existing_row = rows[existing_index] if isinstance(rows[existing_index], dict) else {}
            has_conflict, conflict_reason = rows_conflict_fn(existing_row, candidate_row)
            if has_conflict:
                report["conflicts"].append({
                    "candidate_ref": candidate_ref,
                    "reason": conflict_reason,
                })
                continue

            if merge_policy == "baseline_wins":
                report["skipped"].append({
                    "candidate_ref": candidate_ref,
                    "reason": "baseline_wins_existing_row",
                })
                continue

            if merge_policy == "projection_wins_on_empty_only":
                merged_row = merge_on_empty_fn(existing_row, candidate_row)
            else:
                merged_row = merge_explicit_fn(existing_row, candidate_row)

            # Feststellen, ob sich etwas geändert hat — JSON-Sortier-Text-Vergleich
            try:
                import json as _json
                if _json.dumps(existing_row, sort_keys=True) == _json.dumps(merged_row, sort_keys=True):
                    report["skipped"].append({
                        "candidate_ref": candidate_ref,
                        "reason": "no_change_after_merge",
                    })
                    continue
            except Exception:
                pass  # im Zweifel weiter

            rows[existing_index] = merged_row
            report["rollback_preview"].append({
                "candidate_ref": candidate_ref,
                "rollback_policy": rollback_policy,
                "target_%s" % target_id_field: existing_row.get(target_id_field),
                "previous_row": copy.deepcopy(existing_row),
            })
        else:
            rows.append(candidate_row)
            report["rollback_preview"].append({
                "candidate_ref": candidate_ref,
                "rollback_policy": rollback_policy,
                "target_%s" % target_id_field: candidate_row.get(target_id_field),
                "previous_row": None,
            })

        rows = sort_rows_fn(rows)
        index = index_rows_fn(rows)
        applied.append(candidate_ref)

    report["applied_candidate_refs"] = applied

    if applied:
        report["status"] = REPORT_STATUS_APPLIED
    elif report["conflicts"]:
        report["status"] = REPORT_STATUS_BLOCKED
    else:
        report["status"] = REPORT_STATUS_NO_CHANGES

    return (rows if applied else None), report
