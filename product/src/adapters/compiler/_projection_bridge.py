# -*- coding: utf-8 -*-
# src/adapters/compiler/_projection_bridge.py
#
# Feste Projection-Schicht zwischen current_compiler_engine und runtime_json_kernel.
# Keine Klassen, keine Decorator. Sort-Keys als benannte Hilfsfunktionen
# (statt inline lambdas), passend zur Projekthygiene-Linie ab v16.

import copy
import hashlib
import json


PROJECTION_VERSION = 1
CONFIG_SCHEMA_VERSION = 2
PROCESS_STATE_SIDECAR_VERSION = 1
NEXT_LEGACY_PROJECTION_CANDIDATE = "process_states"
REVERSE_COMPILE_CONTRACT_MODE = "runtime_contract_sidecar_change_set_v1"
PROCESS_STATES_ACTIVATION_CONTRACT_VERSION = 1
PROCESS_STATES_ACTIVATION_CONTRACT_MODE = "mapping_contract_guarded_live_projection_v1"
PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE = "disabled"
PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES = (
    "disabled",
    "review_only",
    "apply_resolved",
)
PROCESS_STATES_ALLOWED_MERGE_POLICIES = (
    "baseline_wins",
    "projection_wins_on_empty_only",
    "explicit_field_merge",
    "review_only",
)
PROCESS_STATES_ALLOWED_ROLLBACK_POLICIES = (
    "restore_previous_row",
    "remove_new_row",
    "rebind_previous_mapping",
    "manual_rollback_required",
)
REQUIRED_PROCESS_STATES_MAPPING_DECISION_KEYS = (
    "candidate_ref",
    "mapping_decision_ref",
    "mapping_mode",
    "state_id_strategy",
    "resolved_state_id",
    "resolved_p_id",
    "resolved_value_id",
    "resolved_mode_id",
    "resolved_mapping_id",
    "resolved_sensor_ids",
    "resolved_actuator_ids",
    "parameters_strategy",
    "control_strategy_strategy",
    "setpoint_source_strategy",
    "merge_policy",
    "rollback_policy",
    "owner_context_ref",
    "worker_refs",
    "requires_review",
)

# ---------------------------------------------------------------------------
# control_methods — v16-Vorbereitung
#
# Analog zu process_states (v12..v15) wird control_methods als nächster
# Legacy-Sektor vertraglich vorbereitet, aber nicht aktiviert.
# Default: disabled. Live-Projektion erfordert in Zukunft einen expliziten
# Mapping-Vertrag unter contracts/runtime/CONTROL_METHODS_MAPPING_CONTRACT_V1.yaml.
# ---------------------------------------------------------------------------

CONTROL_METHODS_ACTIVATION_CONTRACT_VERSION = 1
CONTROL_METHODS_ACTIVATION_CONTRACT_MODE = "mapping_contract_guarded_live_projection_v1"
CONTROL_METHODS_DEFAULT_LIVE_PROJECTION_MODE = "disabled"
CONTROL_METHODS_ALLOWED_LIVE_PROJECTION_MODES = (
    "disabled",
    "review_only",
    "apply_resolved",
)
CONTROL_METHODS_ALLOWED_MERGE_POLICIES = (
    "baseline_wins",
    "projection_wins_on_empty_only",
    "explicit_field_merge",
    "review_only",
)
CONTROL_METHODS_ALLOWED_ROLLBACK_POLICIES = (
    "restore_previous_row",
    "remove_new_row",
    "rebind_previous_mapping",
    "manual_rollback_required",
)

# v19: Erforderliche Felder einer control_methods-Mapping-Entscheidung.
# Bewusst schlanker als REQUIRED_PROCESS_STATES_MAPPING_DECISION_KEYS — eine
# Kontrollmethode ist typischerweise eine Algorithmus-Definition mit
# Parametern, keine vollständige Prozess-Zustandszeile.
REQUIRED_CONTROL_METHODS_MAPPING_DECISION_KEYS = (
    "candidate_ref",
    "mapping_mode",
    "merge_policy",
    "rollback_policy",
    "resolved_method_id",
    "resolved_algorithm_type",
    "resolved_parameters",
)

PROJECTED_RUNTIME_JSON_KERNEL_KEYS = (
    "config_schema_version",
    "compile_projection_meta",
    "runtime_topology",
    "chunk_registry",
    "event_routes",
    "event_payload_contracts",
    "runtime_control_state_boot",
)

DEFERRED_LEGACY_SECTION_KEYS = (
    "controllers",
    "virtual_controllers",
    "sensors",
    "actuators",
    "processes",
    "process_values",
    "process_modes",
    "pid_configs",
    "control_methods",
    "process_states",
    "timers",
    "events",
    "triggers",
    "actions",
    "transitions",
    "dynamic_rule_engine",
    "comm_profiles",
)

PREPARED_LEGACY_SECTION_KEYS = (
    "process_states",
)


def _is_mapping(value):
    return isinstance(value, dict)


def _deep_merge_dicts(base, override):
    if not _is_mapping(base):
        base = {}
    if not _is_mapping(override):
        override = {}

    merged = copy.deepcopy(base)

    for key, value in override.items():
        if _is_mapping(value) and _is_mapping(merged.get(key)):
            merged[key] = _deep_merge_dicts(merged.get(key), value)
        else:
            merged[key] = copy.deepcopy(value)

    return merged


def _stable_json_text(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _content_ref(prefix, payload):
    stable = _stable_json_text(payload).encode("utf-8")
    digest = hashlib.sha256(stable).hexdigest()
    return "%s_%s" % (prefix, digest[:16])


def _as_list(value):
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return []


def _as_mapping(value):
    if _is_mapping(value):
        return copy.deepcopy(value)
    return {}


def _string_or_none(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _sorted_unique_texts(values):
    result = []
    seen = set()

    for value in values:
        text = _string_or_none(value)
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        result.append(text)

    result.sort()
    return result

def _safe_int_or_none(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


_LARGE_SORT_FALLBACK = 10 ** 9


def _sort_key_domain_state(row):
    return (
        _string_or_none(row.get("domain_ref")) or "",
        _string_or_none(row.get("logical_owner_context_ref")) or "",
        _stable_json_text(row),
    )


def _sort_key_owner_context(row):
    return (
        _string_or_none(row.get("owner_context_ref")) or "",
        _stable_json_text(row),
    )


def _sort_key_process_state_row(row):
    state_id = _safe_int_or_none(row.get("state_id"))
    mapping_id = _safe_int_or_none(row.get("mapping_id"))
    return (
        state_id if state_id is not None else _LARGE_SORT_FALLBACK,
        mapping_id if mapping_id is not None else _LARGE_SORT_FALLBACK,
        _stable_json_text(row),
    )


def _sort_key_mapping_record(row):
    resolved_state_id = _safe_int_or_none(row.get("resolved_state_id"))
    return (
        _string_or_none(row.get("candidate_ref")) or "",
        resolved_state_id if resolved_state_id is not None else _LARGE_SORT_FALLBACK,
        _stable_json_text(row),
    )


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


def _sorted_unique_ints(values):
    result = []
    seen = set()

    for value in _as_list(values):
        number = _safe_int_or_none(value)
        if number is None:
            continue
        if number in seen:
            continue
        seen.add(number)
        result.append(number)

    result.sort()
    return result


def _build_chunk_registry(runtime_shard_layout):
    layout = _as_mapping(runtime_shard_layout)
    logical_shards = _as_list(layout.get("logical_shards"))
    physical_shards = _as_list(layout.get("physical_shards"))
    chunks = []

    for shard in logical_shards:
        if not _is_mapping(shard):
            continue
        chunks.append({
            "chunk_id": shard.get("id"),
            "chunk_class": "logical_shard",
            "layout_ref": layout.get("layout_id"),
            "owner_context_ref": shard.get("owner_context_ref"),
            "domain_ref": shard.get("domain_ref"),
            "worker_ref": shard.get("worker_ref"),
            "mode": shard.get("mode"),
            "signal_refs": _as_list(shard.get("signal_refs")),
            "rule_refs": _as_list(shard.get("rule_refs")),
        })

    for shard in physical_shards:
        if not _is_mapping(shard):
            continue
        chunks.append({
            "chunk_id": shard.get("id"),
            "chunk_class": "physical_shard",
            "layout_ref": layout.get("layout_id"),
            "owner_context_ref": shard.get("owner_context_ref"),
            "domain_ref": shard.get("domain_ref"),
            "worker_ref": shard.get("worker_ref"),
            "mode": shard.get("mode"),
            "point_refs": _as_list(shard.get("point_refs")),
            "asset_refs": _as_list(shard.get("asset_refs")),
        })

    return {
        "schema_version": 1,
        "layout_ref": layout.get("layout_id"),
        "chunks": chunks,
    }


def _build_event_contract_patch(compiled_rule_ir):
    rule_ir = _as_mapping(compiled_rule_ir)
    rules = _as_list(rule_ir.get("rules"))
    event_routes = {}
    event_payload_contracts = {}

    for rule in rules:
        if not _is_mapping(rule):
            continue

        emits = _as_list(_as_mapping(rule.get("normalized_then")).get("emit"))
        for emit in emits:
            if not _is_mapping(emit):
                continue

            code = _string_or_none(emit.get("code"))
            if not code:
                continue

            kind = _string_or_none(emit.get("kind")) or "event"
            route_name = "compiled.%s.%s" % (kind.lower(), code.lower())
            event_routes[route_name] = {
                "kind": kind,
                "code": code,
                "rule_ref": rule.get("id"),
                "domain_ref": rule.get("domain_ref"),
                "owner_context_ref": rule.get("owner_context_ref"),
                "consistency": "strong",
                "authority_plane": "control_plane",
            }
            event_payload_contracts[route_name] = {
                "required_fields": ["code", "rule_ref", "domain_ref"],
                "optional_fields": ["correlation_id", "causation_id", "revision_ref"],
            }

    return {
        "event_routes": event_routes,
        "event_payload_contracts": event_payload_contracts,
    }


def _collect_affected_workers(runtime_shard_layout):
    layout = _as_mapping(runtime_shard_layout)
    logical_shards = _as_list(layout.get("logical_shards"))
    physical_shards = _as_list(layout.get("physical_shards"))
    worker_refs = []

    for shard in logical_shards + physical_shards:
        if not _is_mapping(shard):
            continue
        worker_refs.append(shard.get("worker_ref"))

    return _sorted_unique_texts(worker_refs)


def _build_projection_refs(compiler_result, previous_projection):
    manifest = _as_mapping(compiler_result.get("runtime_manifest"))
    bundle = _as_mapping(compiler_result.get("runtime_contract_bundle"))
    bundle_ref = (
        _string_or_none(bundle.get("bundle_id"))
        or _string_or_none(manifest.get("manifest_id"))
        or _content_ref("BUNDLE", bundle)
    )
    manifest_ref = (
        _string_or_none(manifest.get("manifest_id"))
        or _content_ref("MANIFEST", manifest)
    )
    compile_revision = (
        _string_or_none(manifest.get("bundle_hash"))
        or _string_or_none(bundle.get("bundle_id"))
        or _content_ref("REV", compiler_result)
    )
    compile_revision = "REV_%s" % str(compile_revision).replace("-", "_")[:48]

    rollback_ref = None
    if _is_mapping(previous_projection):
        rollback_ref = _string_or_none(previous_projection.get("compile_revision"))

    return {
        "compile_revision": compile_revision,
        "bundle_ref": bundle_ref,
        "manifest_ref": manifest_ref,
        "rollback_ref": rollback_ref,
        "mutation_class": "compile_incremental" if rollback_ref else "compile_full",
    }


def _extract_signal_refs(value):
    signal_refs = []

    if isinstance(value, list):
        for item in value:
            signal_refs.extend(_extract_signal_refs(item))
        return signal_refs

    if not _is_mapping(value):
        return signal_refs

    signal_ref = _string_or_none(value.get("signal_ref"))
    if signal_ref:
        signal_refs.append(signal_ref)

    for nested_value in value.values():
        if isinstance(nested_value, (list, dict)):
            signal_refs.extend(_extract_signal_refs(nested_value))

    return signal_refs


def _build_rule_index(compiled_rule_ir):
    rule_ir = _as_mapping(compiled_rule_ir)
    rule_index = {}

    for rule in _as_list(rule_ir.get("rules")):
        if not _is_mapping(rule):
            continue
        rule_ref = _string_or_none(rule.get("id"))
        if not rule_ref:
            continue
        rule_index[rule_ref] = rule

    return rule_index


def _build_logical_shard_index(runtime_shard_layout):
    layout = _as_mapping(runtime_shard_layout)
    domain_index = {}
    owner_index = {}

    for shard in _as_list(layout.get("logical_shards")):
        if not _is_mapping(shard):
            continue
        worker_ref = _string_or_none(shard.get("worker_ref"))
        if not worker_ref:
            continue

        domain_ref = _string_or_none(shard.get("domain_ref"))
        owner_context_ref = _string_or_none(shard.get("owner_context_ref"))
        if domain_ref:
            domain_index.setdefault(domain_ref, []).append(worker_ref)
        if owner_context_ref:
            owner_index.setdefault(owner_context_ref, []).append(worker_ref)

    return {
        "domain": domain_index,
        "owner": owner_index,
    }


def _build_physical_shard_index(runtime_shard_layout):
    layout = _as_mapping(runtime_shard_layout)
    owner_index = {}

    for shard in _as_list(layout.get("physical_shards")):
        if not _is_mapping(shard):
            continue
        worker_ref = _string_or_none(shard.get("worker_ref"))
        owner_context_ref = _string_or_none(shard.get("owner_context_ref"))
        if not worker_ref or not owner_context_ref:
            continue
        owner_index.setdefault(owner_context_ref, []).append(worker_ref)

    return owner_index


def _match_physical_owner_context_refs(active_binding_refs, physical_owner_states):
    active_binding_set = set(_sorted_unique_texts(active_binding_refs))
    if not active_binding_set:
        return []

    owner_context_refs = []
    for owner_state in physical_owner_states:
        if not _is_mapping(owner_state):
            continue
        state_bindings = set(_sorted_unique_texts(owner_state.get("active_bindings")))
        if not state_bindings:
            continue
        if active_binding_set.intersection(state_bindings):
            owner_context_refs.append(owner_state.get("owner_context_ref"))

    return _sorted_unique_texts(owner_context_refs)


def _infer_mode_hint(domain_state):
    state = _as_mapping(domain_state)
    if _as_list(state.get("active_fsms")):
        return "automatic"
    if _as_list(state.get("active_rules")):
        return "automatic"
    if _as_list(state.get("active_bindings")):
        return "bound"
    return "passive"


def _infer_value_hint(domain_state):
    state = _as_mapping(domain_state)
    if _as_list(state.get("active_signals")):
        return "activated"
    if _as_list(state.get("active_bindings")):
        return "bound"
    return "idle"


def _build_control_strategy_hints(rule_index, active_rule_refs):
    hints = []

    for rule_ref in _sorted_unique_texts(active_rule_refs):
        rule = _as_mapping(rule_index.get(rule_ref))
        if not rule:
            continue

        normalized_when = _as_mapping(rule.get("normalized_when"))
        normalized_then = _as_mapping(rule.get("normalized_then"))
        input_refs = _sorted_unique_texts(_extract_signal_refs(normalized_when))
        output_refs = _sorted_unique_texts(_extract_signal_refs(normalized_then))

        rule_ref_lower = rule_ref.lower()
        stage = _string_or_none(rule.get("stage")) or "runtime_rule"
        strategy = "rule_effect"
        pid_profile_ref = None

        if "pid" in rule_ref_lower or "pid" in stage.lower():
            strategy = "placeholder_pid"
            pid_profile_ref = "PID_PROFILE_PLACEHOLDER"
        elif _as_list(normalized_then.get("set_signals")):
            strategy = "signal_set"
        elif _as_list(normalized_then.get("force_fsm_state")):
            strategy = "fsm_force_state"
        elif _as_list(normalized_then.get("deny_commands")):
            strategy = "guard_command_deny"
        elif _as_list(normalized_then.get("emit")):
            strategy = "event_emit"

        hint_payload = {
            "rule_ref": rule_ref,
            "strategy": strategy,
            "output_refs": output_refs,
            "input_refs": input_refs,
            "stage": stage,
        }
        hint = {
            "hint_ref": _content_ref("CMHINT", hint_payload),
            "rule_ref": rule_ref,
            "strategy": strategy,
            "output_signal_ref": output_refs[0] if output_refs else None,
            "inputs": input_refs,
            "stage": stage,
        }
        if pid_profile_ref is not None:
            hint["pid_profile_ref"] = pid_profile_ref
        hints.append(hint)

    return hints


def _build_process_state_projection_sidecar(runtime_control_state_boot, compiled_rule_ir, runtime_shard_layout):
    boot_state = _as_mapping(runtime_control_state_boot)
    rule_index = _build_rule_index(compiled_rule_ir)
    logical_shards = _build_logical_shard_index(runtime_shard_layout)
    physical_shards = _build_physical_shard_index(runtime_shard_layout)
    logical_domain_states = _as_list(boot_state.get("logical_domain_states"))
    physical_owner_states = _as_list(boot_state.get("physical_owner_states"))
    records = []

    sorted_domain_states = []
    for domain_state in logical_domain_states:
        if not _is_mapping(domain_state):
            continue
        sorted_domain_states.append(domain_state)

    sorted_domain_states.sort(key=_sort_key_domain_state)

    for domain_state in sorted_domain_states:
        domain_ref = _string_or_none(domain_state.get("domain_ref"))
        logical_owner_context_ref = (
            _string_or_none(domain_state.get("logical_owner_context_ref"))
            or domain_ref
        )
        active_rule_refs = _sorted_unique_texts(domain_state.get("active_rules"))
        active_signal_refs = _sorted_unique_texts(domain_state.get("active_signals"))
        active_timer_refs = _sorted_unique_texts(domain_state.get("active_timers"))
        active_binding_refs = _sorted_unique_texts(domain_state.get("active_bindings"))
        physical_owner_context_refs = _match_physical_owner_context_refs(
            active_binding_refs,
            physical_owner_states,
        )

        worker_refs = []
        worker_refs.extend(logical_shards.get("domain", {}).get(domain_ref, []))
        worker_refs.extend(logical_shards.get("owner", {}).get(logical_owner_context_ref, []))
        for owner_context_ref in physical_owner_context_refs:
            worker_refs.extend(physical_shards.get(owner_context_ref, []))
        worker_refs = _sorted_unique_texts(worker_refs)

        source_domain_state_payload = {
            "boot_state_id": boot_state.get("boot_state_id"),
            "domain_ref": domain_ref,
            "logical_owner_context_ref": logical_owner_context_ref,
            "active_rule_refs": active_rule_refs,
            "active_signal_refs": active_signal_refs,
            "active_timer_refs": active_timer_refs,
            "active_binding_refs": active_binding_refs,
        }
        source_domain_state_ref = _content_ref("LDSTATE", source_domain_state_payload)
        control_strategy_hints = _build_control_strategy_hints(rule_index, active_rule_refs)
        candidate_payload = {
            "source_domain_state_ref": source_domain_state_ref,
            "domain_ref": domain_ref,
            "logical_owner_context_ref": logical_owner_context_ref,
            "worker_refs": worker_refs,
            "control_strategy_hints": control_strategy_hints,
        }

        records.append({
            "candidate_ref": _content_ref("PSTATECAND", candidate_payload),
            "source_domain_state_ref": source_domain_state_ref,
            "target_section": "process_states",
            "projection_status": "planned_not_applied",
            "projection_strategy": "sidecar_preprojection_v1",
            "apply_policy": "requires_legacy_mapping_contract",
            "legacy_mapping_status": "unresolved",
            "domain_ref": domain_ref,
            "logical_owner_context_ref": logical_owner_context_ref,
            "physical_owner_context_refs": physical_owner_context_refs,
            "worker_refs": worker_refs,
            "mode_hint": _infer_mode_hint(domain_state),
            "value_hint": _infer_value_hint(domain_state),
            "active_rule_refs": active_rule_refs,
            "active_signal_refs": active_signal_refs,
            "active_timer_refs": active_timer_refs,
            "active_binding_refs": active_binding_refs,
            "control_strategy_hints": control_strategy_hints,
            "legacy_mapping_hints": {
                "state_id_hint": None,
                "p_id_hint": None,
                "value_id_hint": None,
                "mode_id_hint": None,
                "mapping_id_hint": None,
                "sensor_ids_hint": [],
                "actuator_ids_hint": [],
            },
        })

    sorted_physical_owner_states = []
    for owner_state in physical_owner_states:
        if not _is_mapping(owner_state):
            continue
        sorted_physical_owner_states.append(owner_state)

    sorted_physical_owner_states.sort(key=_sort_key_owner_context)

    physical_owner_records = []
    for owner_state in sorted_physical_owner_states:
        owner_context_ref = _string_or_none(owner_state.get("owner_context_ref"))
        physical_owner_records.append({
            "owner_context_ref": owner_context_ref,
            "active_binding_refs": _sorted_unique_texts(owner_state.get("active_bindings")),
            "active_point_refs": _sorted_unique_texts(owner_state.get("active_points")),
            "source_boot_state_ref": boot_state.get("boot_state_id"),
            "worker_refs": _sorted_unique_texts(physical_shards.get(owner_context_ref, [])),
        })

    return {
        "version": PROCESS_STATE_SIDECAR_VERSION,
        "target_section": "process_states",
        "projection_strategy": "sidecar_preprojection_v1",
        "projection_status": "planned_not_applied",
        "requires_legacy_mapping_contract": True,
        "reverse_compile_contract_mode": REVERSE_COMPILE_CONTRACT_MODE,
        "next_activation_step": "mapping_contract_review",
        "source_boot_state_ref": boot_state.get("boot_state_id"),
        "records": records,
        "physical_owner_states": physical_owner_records,
    }


def _build_process_states_activation_contract(process_state_projection_sidecar):
    sidecar = _as_mapping(process_state_projection_sidecar)
    records = _as_list(sidecar.get("records"))
    candidate_refs = []

    for record in records:
        if not _is_mapping(record):
            continue
        candidate_refs.append(record.get("candidate_ref"))

    candidate_refs = _sorted_unique_texts(candidate_refs)

    return {
        "version": PROCESS_STATES_ACTIVATION_CONTRACT_VERSION,
        "contract_mode": PROCESS_STATES_ACTIVATION_CONTRACT_MODE,
        "target_section": "process_states",
        "default_live_projection_mode": PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE,
        "allowed_live_projection_modes": list(PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES),
        "allowed_merge_policies": list(PROCESS_STATES_ALLOWED_MERGE_POLICIES),
        "allowed_rollback_policies": list(PROCESS_STATES_ALLOWED_ROLLBACK_POLICIES),
        "mapping_contract_required": True,
        "requires_review_by_default": True,
        "identity_fields": ["state_id", "p_id", "value_id", "mode_id", "mapping_id"],
        "guardrails": [
            "no_implicit_foreign_key_guessing",
            "no_live_projection_without_mapping_contract",
            "no_identity_conflict_overwrite",
            "no_io_conflict_overwrite",
            "rollback_preview_must_exist_before_apply",
        ],
        "rollback_strategy": "row_snapshot_by_state_id_v1",
        "candidate_count": len(candidate_refs),
        "candidate_refs": candidate_refs,
        "next_activation_step": "provide_mapping_contract_and_enable_apply_resolved",
    }


def _build_projection_meta(compiler_result, refs):
    manifest = _as_mapping(compiler_result.get("runtime_manifest"))
    bundle = _as_mapping(compiler_result.get("runtime_contract_bundle"))
    topology = _as_mapping(compiler_result.get("runtime_shard_layout"))
    control_state_boot = _as_mapping(compiler_result.get("runtime_control_state_boot"))

    return {
        "projection_version": PROJECTION_VERSION,
        "config_schema_version": CONFIG_SCHEMA_VERSION,
        "compile_revision": refs.get("compile_revision"),
        "bundle_ref": refs.get("bundle_ref"),
        "manifest_ref": refs.get("manifest_ref"),
        "rollback_ref": refs.get("rollback_ref"),
        "source_cud_hash": (
            manifest.get("source_cud_hash")
            or bundle.get("source_cud_hash")
            or topology.get("source_cud_hash")
        ),
        "manifest": {
            "canonical_id": manifest.get("canonical_id"),
            "partition_id": manifest.get("partition_id"),
            "bundle_hash": manifest.get("bundle_hash"),
            "inventory_hash": manifest.get("inventory_hash"),
            "signal_catalog_hash": manifest.get("signal_catalog_hash"),
            "io_bindings_hash": manifest.get("io_bindings_hash"),
            "rule_ir_hash": manifest.get("rule_ir_hash"),
            "layout_hash": manifest.get("layout_hash"),
        },
        "control_state_boot_ref": control_state_boot.get("boot_state_id"),
        "projection_mode": "overlay_onto_legacy_runtime_json_kernel",
        "projection_scope_class": "minimal_kernel_overlay_v1",
        "projection_determinism": "stable_hash_sorted_json_merge",
        "projected_kernel_sections": list(PROJECTED_RUNTIME_JSON_KERNEL_KEYS),
        "deferred_legacy_sections": list(DEFERRED_LEGACY_SECTION_KEYS),
        "prepared_legacy_sections": list(PREPARED_LEGACY_SECTION_KEYS),
        "next_legacy_projection_candidate": NEXT_LEGACY_PROJECTION_CANDIDATE,
        "reverse_compile_contract_mode": REVERSE_COMPILE_CONTRACT_MODE,
        "process_states_activation_contract_mode": PROCESS_STATES_ACTIVATION_CONTRACT_MODE,
        "process_states_default_live_projection_mode": PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE,
    }


def build_runtime_projection_bundle(compiler_result, previous_projection=None):
    if not _is_mapping(compiler_result):
        raise ValueError("compiler_result must be a mapping")

    runtime_contract_bundle = _as_mapping(compiler_result.get("runtime_contract_bundle"))
    runtime_manifest = _as_mapping(compiler_result.get("runtime_manifest"))
    runtime_shard_layout = _as_mapping(compiler_result.get("runtime_shard_layout"))
    runtime_control_state_boot = _as_mapping(compiler_result.get("runtime_control_state_boot"))
    compiled_rule_ir = _as_mapping(compiler_result.get("compiled_rule_ir"))

    refs = _build_projection_refs(compiler_result, previous_projection)
    topology_patch = {
        "runtime_topology": runtime_shard_layout,
    }
    chunk_registry_patch = {
        "chunk_registry": _build_chunk_registry(runtime_shard_layout),
    }
    event_contract_patch = _build_event_contract_patch(compiled_rule_ir)
    projection_meta = _build_projection_meta(compiler_result, refs)
    affected_workers = _collect_affected_workers(runtime_shard_layout)
    process_state_projection_sidecar = _build_process_state_projection_sidecar(
        runtime_control_state_boot,
        compiled_rule_ir,
        runtime_shard_layout,
    )
    process_states_activation_contract = _build_process_states_activation_contract(
        process_state_projection_sidecar,
    )

    runtime_json_kernel_patch = {
        "config_schema_version": CONFIG_SCHEMA_VERSION,
        "compile_projection_meta": projection_meta,
        "runtime_topology": copy.deepcopy(topology_patch.get("runtime_topology")),
        "chunk_registry": copy.deepcopy(chunk_registry_patch.get("chunk_registry")),
        "event_routes": copy.deepcopy(event_contract_patch.get("event_routes")),
        "event_payload_contracts": copy.deepcopy(event_contract_patch.get("event_payload_contracts")),
        "runtime_control_state_boot": runtime_control_state_boot,
    }

    return {
        "projection_version": PROJECTION_VERSION,
        "compile_revision": refs.get("compile_revision"),
        "bundle_ref": refs.get("bundle_ref"),
        "manifest_ref": refs.get("manifest_ref"),
        "rollback_ref": refs.get("rollback_ref"),
        "runtime_json_kernel_patch": runtime_json_kernel_patch,
        "runtime_topology_patch": topology_patch,
        "chunk_registry_patch": chunk_registry_patch,
        "event_contract_patch": event_contract_patch,
        "affected_workers": affected_workers,
        "activation": {
            "mutation_class": refs.get("mutation_class"),
            "activation_class": "affected_workers" if affected_workers else "global",
            "requires_restart": False,
            "requires_review": False,
            "process_states_live_projection": {
                "contract_mode": PROCESS_STATES_ACTIVATION_CONTRACT_MODE,
                "default_mode": PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE,
                "candidate_count": len(_as_list(process_state_projection_sidecar.get("records"))),
                "requires_review": True,
            },
        },
        "sidecars": {
            "runtime_manifest": runtime_manifest,
            "runtime_contract_bundle": runtime_contract_bundle,
            "process_state_projection_sidecar": process_state_projection_sidecar,
            "process_states_activation_contract": process_states_activation_contract,
        },
    }


def _normalize_mapping_contract_records(mapping_contract):
    contract = copy.deepcopy(mapping_contract)

    if isinstance(contract, list):
        return {}, contract

    if not _is_mapping(contract):
        return {}, []

    records = contract.get("records")
    if isinstance(records, list):
        contract_meta = copy.deepcopy(contract)
        contract_meta.pop("records", None)
        return contract_meta, copy.deepcopy(records)

    # v19: YAML-Contracts verwenden "mapping_decisions" als Schlüssel.
    mapping_decisions = contract.get("mapping_decisions")
    if isinstance(mapping_decisions, list):
        contract_meta = copy.deepcopy(contract)
        contract_meta.pop("mapping_decisions", None)
        return contract_meta, copy.deepcopy(mapping_decisions)

    if "candidate_ref" in contract:
        return {}, [copy.deepcopy(contract)]

    return copy.deepcopy(contract), []


def _validate_process_states_mapping_decision(decision):
    reasons = []
    row = _as_mapping(decision)

    for key in REQUIRED_PROCESS_STATES_MAPPING_DECISION_KEYS:
        if key not in row:
            reasons.append("mapping decision missing required key: %s" % key)

    if str(row.get("mapping_mode")) not in (
        "attach_to_existing_legacy_state",
        "create_new_legacy_state",
        "review_only",
    ):
        reasons.append("mapping decision has unsupported mapping_mode: %s" % row.get("mapping_mode"))

    if str(row.get("merge_policy")) not in PROCESS_STATES_ALLOWED_MERGE_POLICIES:
        reasons.append("mapping decision has unsupported merge_policy: %s" % row.get("merge_policy"))

    if str(row.get("rollback_policy")) not in PROCESS_STATES_ALLOWED_ROLLBACK_POLICIES:
        reasons.append("mapping decision has unsupported rollback_policy: %s" % row.get("rollback_policy"))

    for key in (
        "resolved_state_id",
        "resolved_p_id",
        "resolved_value_id",
        "resolved_mode_id",
        "resolved_mapping_id",
    ):
        if _safe_int_or_none(row.get(key)) is None:
            reasons.append("mapping decision requires integer value for: %s" % key)

    for key in ("resolved_sensor_ids", "resolved_actuator_ids", "worker_refs"):
        if not isinstance(row.get(key), list):
            reasons.append("mapping decision requires list for: %s" % key)

    return reasons


def _build_process_state_row_from_mapping_decision(decision):
    row = _as_mapping(decision)
    return {
        "state_id": _safe_int_or_none(row.get("resolved_state_id")),
        "p_id": _safe_int_or_none(row.get("resolved_p_id")),
        "value_id": _safe_int_or_none(row.get("resolved_value_id")),
        "mode_id": _safe_int_or_none(row.get("resolved_mode_id")),
        "mapping_id": _safe_int_or_none(row.get("resolved_mapping_id")),
        "actuator_ids": _sorted_unique_ints(row.get("resolved_actuator_ids")),
        "sensor_ids": _sorted_unique_ints(row.get("resolved_sensor_ids")),
        "parameters": _as_mapping(row.get("resolved_parameters")),
        "control_strategy": copy.deepcopy(row.get("resolved_control_strategy")),
        "setpoint_source": copy.deepcopy(row.get("resolved_setpoint_source")),
    }


def _build_process_state_identity_signature(row):
    state = _as_mapping(row)
    return (
        _safe_int_or_none(state.get("state_id")),
        _safe_int_or_none(state.get("p_id")),
        _safe_int_or_none(state.get("value_id")),
        _safe_int_or_none(state.get("mode_id")),
        _safe_int_or_none(state.get("mapping_id")),
    )


def _index_process_states(rows):
    index = {
        "state_id": {},
        "mapping_id": {},
    }

    for position, row in enumerate(rows):
        if not _is_mapping(row):
            continue
        state_id = _safe_int_or_none(row.get("state_id"))
        mapping_id = _safe_int_or_none(row.get("mapping_id"))
        if state_id is not None and state_id not in index["state_id"]:
            index["state_id"][state_id] = position
        if mapping_id is not None and mapping_id not in index["mapping_id"]:
            index["mapping_id"][mapping_id] = position

    return index


def _find_existing_process_state_index(index, row):
    state_id = _safe_int_or_none(row.get("state_id"))
    mapping_id = _safe_int_or_none(row.get("mapping_id"))
    state_index = index.get("state_id", {}).get(state_id)
    mapping_index = index.get("mapping_id", {}).get(mapping_id)

    if state_index is not None and mapping_index is not None and state_index != mapping_index:
        return None, "conflicting_state_id_and_mapping_id"
    if state_index is not None:
        return state_index, None
    if mapping_index is not None:
        return mapping_index, None
    return None, None


def _process_state_rows_conflict(existing_row, new_row):
    existing = _as_mapping(existing_row)
    candidate = _as_mapping(new_row)

    if _build_process_state_identity_signature(existing) != _build_process_state_identity_signature(candidate):
        return True, "conflicting_process_identity"

    if existing.get("sensor_ids") and candidate.get("sensor_ids"):
        if _sorted_unique_ints(existing.get("sensor_ids")) != _sorted_unique_ints(candidate.get("sensor_ids")):
            return True, "conflicting_sensor_binding"

    if existing.get("actuator_ids") and candidate.get("actuator_ids"):
        if _sorted_unique_ints(existing.get("actuator_ids")) != _sorted_unique_ints(candidate.get("actuator_ids")):
            return True, "conflicting_actuator_binding"

    existing_strategy = existing.get("control_strategy")
    candidate_strategy = candidate.get("control_strategy")
    if existing_strategy not in (None, {}, []) and candidate_strategy not in (None, {}, []):
        if _stable_json_text(existing_strategy) != _stable_json_text(candidate_strategy):
            return True, "conflicting_control_strategy"

    return False, None


def _merge_process_state_row_on_empty(existing_row, new_row):
    existing = copy.deepcopy(_as_mapping(existing_row))
    candidate = _as_mapping(new_row)

    for field_name in ("sensor_ids", "actuator_ids"):
        if not _as_list(existing.get(field_name)):
            existing[field_name] = _sorted_unique_ints(candidate.get(field_name))

    if not _as_mapping(existing.get("parameters")):
        existing["parameters"] = _as_mapping(candidate.get("parameters"))

    if existing.get("control_strategy") in (None, {}, []):
        existing["control_strategy"] = copy.deepcopy(candidate.get("control_strategy"))

    if existing.get("setpoint_source") in (None, ""):
        existing["setpoint_source"] = copy.deepcopy(candidate.get("setpoint_source"))

    return existing


def _merge_process_state_row_explicit(existing_row, new_row):
    existing = copy.deepcopy(_as_mapping(existing_row))
    candidate = _as_mapping(new_row)

    existing["parameters"] = _deep_merge_dicts(existing.get("parameters"), candidate.get("parameters"))

    for field_name in ("sensor_ids", "actuator_ids"):
        if candidate.get(field_name) is not None:
            existing[field_name] = _sorted_unique_ints(candidate.get(field_name))

    if candidate.get("control_strategy") not in (None, {}, []):
        existing["control_strategy"] = copy.deepcopy(candidate.get("control_strategy"))

    if "setpoint_source" in candidate:
        existing["setpoint_source"] = copy.deepcopy(candidate.get("setpoint_source"))

    return existing


def _sort_process_states_rows(rows):
    normalized_rows = []
    for row in rows:
        if not _is_mapping(row):
            continue
        normalized = copy.deepcopy(row)
        if "sensor_ids" in normalized:
            normalized["sensor_ids"] = _sorted_unique_ints(normalized.get("sensor_ids"))
        if "actuator_ids" in normalized:
            normalized["actuator_ids"] = _sorted_unique_ints(normalized.get("actuator_ids"))
        normalized_rows.append(normalized)

    normalized_rows.sort(key=_sort_key_process_state_row)
    return normalized_rows


def build_process_states_live_projection_patch(base_config, projection_bundle, mapping_contract=None, activation_settings=None):
    """v14: Live-Projection für process_states.
    Ab v21: Delegiert an die generische Engine (wie control_methods seit v20).

    Rückgabewerte: (patch_or_None, report_dict)
    """
    from functools import partial
    from src.adapters.compiler._live_projection_engine import (
        apply_live_projection_for_section,
    )

    base = _as_mapping(base_config)
    projection = _as_mapping(projection_bundle)
    activation = _as_mapping(activation_settings)

    enabled = _as_bool(activation.get("process_states_live_projection_enabled"), False)
    mode = (
        _string_or_none(activation.get("process_states_live_projection_mode"))
        or PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE
    )
    if mode not in PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES:
        mode = PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE

    apply_limit = _safe_int_or_none(activation.get("process_states_live_projection_apply_limit"))
    if apply_limit is None or apply_limit < 0:
        apply_limit = 0

    allow_requires_review = _as_bool(
        activation.get("process_states_live_projection_allow_review_required"),
        False,
    )

    # Sidecar-Candidate-Index aufbauen — der ist process_states-spezifisch
    # und wird als zusätzlicher Check in validate_decision_fn hineingereicht.
    sidecars = _as_mapping(projection.get("sidecars"))
    process_state_sidecar = _as_mapping(sidecars.get("process_state_projection_sidecar"))
    candidate_index = {}
    for record in _as_list(process_state_sidecar.get("records")):
        if not _is_mapping(record):
            continue
        candidate_ref = _string_or_none(record.get("candidate_ref"))
        if not candidate_ref:
            continue
        candidate_index[candidate_ref] = copy.deepcopy(record)

    contract_meta, mapping_records = _normalize_mapping_contract_records(mapping_contract)

    # Validator-Closure: schneidet standard-Validator + Sidecar-Check zusammen.
    # Wird per functools.partial gebunden, damit kein Lambda nötig ist.
    def _validate_process_states_decision_with_sidecar(candidate_index_ref, decision):
        reasons = _validate_process_states_mapping_decision(decision)
        cref = _string_or_none(decision.get("candidate_ref"))
        if cref and cref not in candidate_index_ref:
            reasons.append("mapping decision candidate_ref not found in process_state_projection_sidecar")
        return reasons

    validate_fn = partial(_validate_process_states_decision_with_sidecar, candidate_index)

    section_callbacks = {
        "section_key": "process_states",
        "target_id_field": "state_id",
        "allowed_merge_policies": PROCESS_STATES_ALLOWED_MERGE_POLICIES,
        "allowed_rollback_policies": PROCESS_STATES_ALLOWED_ROLLBACK_POLICIES,
        "allowed_live_projection_modes": PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES,
        "default_live_projection_mode": PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE,
        "attach_mode": "attach_to_existing_legacy_state",
        "sort_rows_fn": _sort_process_states_rows,
        "index_rows_fn": _index_process_states,
        "find_existing_fn": _find_existing_process_state_index,
        "rows_conflict_fn": _process_state_rows_conflict,
        "validate_decision_fn": validate_fn,
        "build_row_fn": _build_process_state_row_from_mapping_decision,
        "merge_on_empty_fn": _merge_process_state_row_on_empty,
        "merge_explicit_fn": _merge_process_state_row_explicit,
        "sort_key_mapping_record_fn": _sort_key_mapping_record,
    }

    rows, report = apply_live_projection_for_section(
        existing_rows=_as_list(base.get("process_states")),
        contract_meta=contract_meta,
        mapping_records=mapping_records,
        enabled=enabled,
        mode=mode,
        apply_limit=apply_limit,
        allow_review_required=allow_requires_review,
        section_callbacks=section_callbacks,
    )

    patch = None
    if rows is not None:
        patch = {"process_states": rows}

    return patch, report


def merge_projection_into_runtime_json_kernel(
    base_config,
    projection_bundle,
    process_states_mapping_contract=None,
    process_states_live_projection_settings=None,
    control_methods_mapping_contract=None,
    control_methods_live_projection_settings=None,
):
    base = _as_mapping(base_config)
    projection = _as_mapping(projection_bundle)
    kernel_patch = _as_mapping(projection.get("runtime_json_kernel_patch"))

    merged = _deep_merge_dicts(base, kernel_patch)
    merged = _deep_merge_dicts(merged, _as_mapping(projection.get("runtime_topology_patch")))
    merged = _deep_merge_dicts(merged, _as_mapping(projection.get("chunk_registry_patch")))
    merged = _deep_merge_dicts(merged, _as_mapping(projection.get("event_contract_patch")))

    process_states_patch, activation_report = build_process_states_live_projection_patch(
        merged,
        projection,
        mapping_contract=process_states_mapping_contract,
        activation_settings=process_states_live_projection_settings,
    )
    if _is_mapping(process_states_patch):
        merged = _deep_merge_dicts(merged, process_states_patch)

    compile_projection_meta = _as_mapping(merged.get("compile_projection_meta"))
    compile_projection_meta["process_states_live_projection"] = activation_report

    # v19: control_methods live projection (Default deaktiviert; gleiche
    # Guardrails-Logik wie process_states).
    control_methods_patch, control_methods_report = build_control_methods_live_projection_patch(
        merged,
        projection,
        mapping_contract=control_methods_mapping_contract,
        activation_settings=control_methods_live_projection_settings,
    )
    if _is_mapping(control_methods_patch):
        merged = _deep_merge_dicts(merged, control_methods_patch)
    compile_projection_meta["control_methods_live_projection"] = control_methods_report

    merged["compile_projection_meta"] = compile_projection_meta
    return merged


# ===========================================================================
# v19: control_methods — Live-Projection analog zu process_states (v14/v15)
# ===========================================================================

def _sort_key_control_method_row(row):
    method_id = _safe_int_or_none(row.get("method_id"))
    return (
        method_id if method_id is not None else _LARGE_SORT_FALLBACK,
        _string_or_none(row.get("algorithm_type")) or "",
        _stable_json_text(row),
    )


def _sort_control_methods_rows(rows):
    sorted_rows = []
    for row in rows:
        if not _is_mapping(row):
            continue
        sorted_rows.append(copy.deepcopy(row))
    sorted_rows.sort(key=_sort_key_control_method_row)
    return sorted_rows


def _validate_control_methods_mapping_decision(decision):
    reasons = []
    row = _as_mapping(decision)

    for key in REQUIRED_CONTROL_METHODS_MAPPING_DECISION_KEYS:
        if key not in row:
            reasons.append("control_methods mapping decision missing required key: %s" % key)

    if str(row.get("mapping_mode")) not in (
        "attach_to_existing_legacy_method",
        "create_new_legacy_method",
        "review_only",
    ):
        reasons.append(
            "control_methods mapping decision has unsupported mapping_mode: %s" % row.get("mapping_mode")
        )

    if str(row.get("merge_policy")) not in CONTROL_METHODS_ALLOWED_MERGE_POLICIES:
        reasons.append(
            "control_methods mapping decision has unsupported merge_policy: %s" % row.get("merge_policy")
        )

    if str(row.get("rollback_policy")) not in CONTROL_METHODS_ALLOWED_ROLLBACK_POLICIES:
        reasons.append(
            "control_methods mapping decision has unsupported rollback_policy: %s" % row.get("rollback_policy")
        )

    if _safe_int_or_none(row.get("resolved_method_id")) is None:
        reasons.append("control_methods mapping decision requires integer value for: resolved_method_id")

    algorithm = _string_or_none(row.get("resolved_algorithm_type"))
    if not algorithm:
        reasons.append("control_methods mapping decision requires non-empty resolved_algorithm_type")

    if not isinstance(row.get("resolved_parameters"), dict):
        reasons.append("control_methods mapping decision requires dict for: resolved_parameters")

    return reasons


def _build_control_method_row_from_mapping_decision(decision):
    row = _as_mapping(decision)
    return {
        "method_id": _safe_int_or_none(row.get("resolved_method_id")),
        "algorithm_type": _string_or_none(row.get("resolved_algorithm_type")),
        "parameters": _as_mapping(row.get("resolved_parameters")),
        "owner_context_ref": _string_or_none(row.get("resolved_owner_context_ref")),
        "worker_refs": _sorted_unique_texts(row.get("resolved_worker_refs")),
        "requires_review": _as_bool(row.get("requires_review"), False),
    }


def _build_control_method_identity_signature(row):
    method = _as_mapping(row)
    return (
        _safe_int_or_none(method.get("method_id")),
        _string_or_none(method.get("algorithm_type")),
    )


def _index_control_methods(rows):
    index = {"method_id": {}}
    for position, row in enumerate(rows):
        if not _is_mapping(row):
            continue
        method_id = _safe_int_or_none(row.get("method_id"))
        if method_id is not None and method_id not in index["method_id"]:
            index["method_id"][method_id] = position
    return index


def _find_existing_control_method_index(index, row):
    method_id = _safe_int_or_none(row.get("method_id"))
    method_index = index.get("method_id", {}).get(method_id)
    if method_index is not None:
        return method_index, None
    return None, None


def _control_method_rows_conflict(existing_row, new_row):
    existing = _as_mapping(existing_row)
    candidate = _as_mapping(new_row)

    # Identity-Konflikt: method_id + algorithm_type müssen übereinstimmen,
    # wenn beide Rows auf dieselbe method_id zeigen.
    if _build_control_method_identity_signature(existing) != _build_control_method_identity_signature(candidate):
        return True, "conflicting_control_method_identity"

    # Parameter-Konflikte sind KEIN row_conflict — sie werden durch die
    # merge_policy aufgelöst (baseline_wins, projection_wins_on_empty_only,
    # explicit_field_merge). Nur inkonsistente Identity blockiert die Projektion.
    return False, None


def _merge_control_method_row_on_empty(existing_row, new_row):
    existing = copy.deepcopy(_as_mapping(existing_row))
    candidate = _as_mapping(new_row)

    if not _as_mapping(existing.get("parameters")):
        existing["parameters"] = _as_mapping(candidate.get("parameters"))

    if not existing.get("owner_context_ref"):
        existing["owner_context_ref"] = _string_or_none(candidate.get("owner_context_ref"))

    if not _as_list(existing.get("worker_refs")):
        existing["worker_refs"] = _sorted_unique_texts(candidate.get("worker_refs"))

    return existing


def _merge_control_method_row_explicit(existing_row, new_row):
    existing = copy.deepcopy(_as_mapping(existing_row))
    candidate = _as_mapping(new_row)

    existing["parameters"] = _deep_merge_dicts(existing.get("parameters"), candidate.get("parameters"))

    if candidate.get("owner_context_ref"):
        existing["owner_context_ref"] = _string_or_none(candidate.get("owner_context_ref"))

    if candidate.get("worker_refs") is not None:
        existing["worker_refs"] = _sorted_unique_texts(candidate.get("worker_refs"))

    return existing


def build_control_methods_live_projection_patch(
    base_config,
    projection_bundle,
    mapping_contract=None,
    activation_settings=None,
):
    """v19: Baut einen control_methods-Live-Projection-Patch analog zu
    build_process_states_live_projection_patch. Default-off.

    Ab v20: Delegiert an die generische Live-Projection-Engine mit
    control_methods-spezifischen Callbacks.

    Rückgabewerte: (patch_or_None, report_dict)
    """
    from src.adapters.compiler._live_projection_engine import (
        apply_live_projection_for_section,
    )

    base = _as_mapping(base_config)
    activation = _as_mapping(activation_settings)

    enabled = _as_bool(activation.get("control_methods_live_projection_enabled"), False)
    mode = (
        _string_or_none(activation.get("control_methods_live_projection_mode"))
        or CONTROL_METHODS_DEFAULT_LIVE_PROJECTION_MODE
    )
    if mode not in CONTROL_METHODS_ALLOWED_LIVE_PROJECTION_MODES:
        mode = CONTROL_METHODS_DEFAULT_LIVE_PROJECTION_MODE

    apply_limit = _safe_int_or_none(activation.get("control_methods_live_projection_apply_limit"))
    if apply_limit is None or apply_limit < 0:
        apply_limit = 0

    allow_requires_review = _as_bool(
        activation.get("control_methods_live_projection_allow_review_required"),
        False,
    )

    contract_meta, mapping_records = _normalize_mapping_contract_records(mapping_contract)

    section_callbacks = {
        "section_key": "control_methods",
        "target_id_field": "method_id",
        "allowed_merge_policies": CONTROL_METHODS_ALLOWED_MERGE_POLICIES,
        "allowed_rollback_policies": CONTROL_METHODS_ALLOWED_ROLLBACK_POLICIES,
        "allowed_live_projection_modes": CONTROL_METHODS_ALLOWED_LIVE_PROJECTION_MODES,
        "default_live_projection_mode": CONTROL_METHODS_DEFAULT_LIVE_PROJECTION_MODE,
        "attach_mode": "attach_to_existing_legacy_method",
        "sort_rows_fn": _sort_control_methods_rows,
        "index_rows_fn": _index_control_methods,
        "find_existing_fn": _find_existing_control_method_index,
        "rows_conflict_fn": _control_method_rows_conflict,
        "validate_decision_fn": _validate_control_methods_mapping_decision,
        "build_row_fn": _build_control_method_row_from_mapping_decision,
        "merge_on_empty_fn": _merge_control_method_row_on_empty,
        "merge_explicit_fn": _merge_control_method_row_explicit,
        "sort_key_mapping_record_fn": _sort_key_mapping_record,
    }

    rows, report = apply_live_projection_for_section(
        existing_rows=_as_list(base.get("control_methods")),
        contract_meta=contract_meta,
        mapping_records=mapping_records,
        enabled=enabled,
        mode=mode,
        apply_limit=apply_limit,
        allow_review_required=allow_requires_review,
        section_callbacks=section_callbacks,
    )

    patch = None
    if rows is not None:
        patch = {"control_methods": rows}

    return patch, report
