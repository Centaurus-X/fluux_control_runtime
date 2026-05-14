# -*- coding: utf-8 -*-
# src/adapters/compiler/_compatibility_validator.py
#
# Validiert den Projection-Bridge-Vertrag zwischen Compiler und Runtime.
# Fuer einen sanften Uebergang wird ein Legacy-Direktformat weiterhin erkannt,
# aber nicht mehr als primaeres Zielmodell behandelt.

from src.adapters.compiler._projection_bridge import (
    CONTROL_METHODS_ACTIVATION_CONTRACT_MODE,
    CONTROL_METHODS_ALLOWED_LIVE_PROJECTION_MODES,
    CONTROL_METHODS_ALLOWED_MERGE_POLICIES,
    CONTROL_METHODS_ALLOWED_ROLLBACK_POLICIES,
    PROCESS_STATES_ACTIVATION_CONTRACT_MODE,
    PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES,
    PROCESS_STATES_ALLOWED_MERGE_POLICIES,
    PROCESS_STATES_ALLOWED_ROLLBACK_POLICIES,
    PROJECTED_RUNTIME_JSON_KERNEL_KEYS,
)


REQUIRED_PROJECTION_KEYS = (
    "projection_version",
    "compile_revision",
    "bundle_ref",
    "runtime_json_kernel_patch",
    "activation",
)

OPTIONAL_PROJECTION_KEYS = (
    "manifest_ref",
    "rollback_ref",
    "runtime_topology_patch",
    "chunk_registry_patch",
    "event_contract_patch",
    "affected_workers",
    "sidecars",
)

LEGACY_REQUIRED_TOP_LEVEL_KEYS = ("devices", "controllers")

REQUIRED_RUNTIME_JSON_KERNEL_PATCH_KEYS = (
    "config_schema_version",
    "compile_projection_meta",
)

REQUIRED_COMPILE_PROJECTION_META_KEYS = (
    "projection_version",
    "compile_revision",
    "bundle_ref",
    "projection_mode",
    "projected_kernel_sections",
)

REQUIRED_ACTIVATION_KEYS = (
    "mutation_class",
    "activation_class",
    "requires_restart",
    "requires_review",
)

ALLOWED_MUTATION_CLASSES = (
    "compile_full",
    "compile_incremental",
    "overlay_update",
    "event_contract_update",
    "topology_update",
)

ALLOWED_ACTIVATION_CLASSES = (
    "global",
    "affected_workers",
    "local_worker",
    "deferred",
)

ALLOWED_NEXT_LEGACY_PROJECTION_CANDIDATES = (
    "process_states",
    "control_methods",
)

REQUIRED_PROCESS_STATE_SIDECAR_KEYS = (
    "version",
    "target_section",
    "projection_strategy",
    "projection_status",
    "requires_legacy_mapping_contract",
    "source_boot_state_ref",
    "records",
)

REQUIRED_PROCESS_STATE_RECORD_KEYS = (
    "candidate_ref",
    "target_section",
    "projection_status",
    "projection_strategy",
    "apply_policy",
    "legacy_mapping_status",
    "domain_ref",
    "logical_owner_context_ref",
    "active_rule_refs",
    "active_signal_refs",
    "active_timer_refs",
    "active_binding_refs",
    "control_strategy_hints",
    "legacy_mapping_hints",
)

REQUIRED_CONTROL_STRATEGY_HINT_KEYS = (
    "hint_ref",
    "rule_ref",
    "strategy",
)

REQUIRED_LEGACY_MAPPING_HINT_KEYS = (
    "state_id_hint",
    "p_id_hint",
    "value_id_hint",
    "mode_id_hint",
    "mapping_id_hint",
    "sensor_ids_hint",
    "actuator_ids_hint",
)

REQUIRED_PROCESS_STATES_ACTIVATION_CONTRACT_KEYS = (
    "version",
    "contract_mode",
    "target_section",
    "default_live_projection_mode",
    "allowed_live_projection_modes",
    "allowed_merge_policies",
    "allowed_rollback_policies",
    "mapping_contract_required",
    "candidate_count",
    "candidate_refs",
)

REQUIRED_ACTIVATION_PROCESS_STATES_KEYS = (
    "contract_mode",
    "default_mode",
    "candidate_count",
    "requires_review",
)



def _is_mapping(value):
    return isinstance(value, dict)



def _validate_compile_projection_meta(compile_projection_meta):
    reasons = []

    if not _is_mapping(compile_projection_meta):
        return [
            "compile_projection_meta has unexpected type: %s"
            % type(compile_projection_meta).__name__,
        ]

    for key in REQUIRED_COMPILE_PROJECTION_META_KEYS:
        if key not in compile_projection_meta:
            reasons.append("compile_projection_meta missing required key: %s" % key)

    projected_kernel_sections = compile_projection_meta.get("projected_kernel_sections")
    if projected_kernel_sections is not None and not isinstance(projected_kernel_sections, (list, tuple)):
        reasons.append(
            "compile_projection_meta.projected_kernel_sections has unexpected type: %s"
            % type(projected_kernel_sections).__name__,
        )
    elif isinstance(projected_kernel_sections, (list, tuple)):
        unsupported_projected_sections = []
        for key in projected_kernel_sections:
            if key not in PROJECTED_RUNTIME_JSON_KERNEL_KEYS:
                unsupported_projected_sections.append(str(key))
        if unsupported_projected_sections:
            unsupported_projected_sections.sort()
            reasons.append(
                "compile_projection_meta.projected_kernel_sections contains unsupported values: %s"
                % ", ".join(unsupported_projected_sections),
            )

    prepared_legacy_sections = compile_projection_meta.get("prepared_legacy_sections")
    if prepared_legacy_sections is not None and not isinstance(prepared_legacy_sections, (list, tuple)):
        reasons.append(
            "compile_projection_meta.prepared_legacy_sections has unexpected type: %s"
            % type(prepared_legacy_sections).__name__,
        )

    next_candidate = compile_projection_meta.get("next_legacy_projection_candidate")
    if next_candidate is not None and str(next_candidate) not in ALLOWED_NEXT_LEGACY_PROJECTION_CANDIDATES:
        reasons.append(
            "compile_projection_meta.next_legacy_projection_candidate has unsupported value: %s"
            % next_candidate,
        )

    reverse_compile_contract_mode = compile_projection_meta.get("reverse_compile_contract_mode")
    if reverse_compile_contract_mode is not None and not str(reverse_compile_contract_mode).strip():
        reasons.append("compile_projection_meta.reverse_compile_contract_mode must not be empty")

    activation_contract_mode = compile_projection_meta.get("process_states_activation_contract_mode")
    if activation_contract_mode is not None and str(activation_contract_mode) != PROCESS_STATES_ACTIVATION_CONTRACT_MODE:
        reasons.append(
            "compile_projection_meta.process_states_activation_contract_mode has unsupported value: %s"
            % activation_contract_mode,
        )

    default_mode = compile_projection_meta.get("process_states_default_live_projection_mode")
    if default_mode is not None and str(default_mode) not in PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES:
        reasons.append(
            "compile_projection_meta.process_states_default_live_projection_mode has unsupported value: %s"
            % default_mode,
        )

    # v16: control_methods als nächster Legacy-Sektor vertraglich vorbereitet.
    cm_contract_mode = compile_projection_meta.get("control_methods_activation_contract_mode")
    if cm_contract_mode is not None and str(cm_contract_mode) != CONTROL_METHODS_ACTIVATION_CONTRACT_MODE:
        reasons.append(
            "compile_projection_meta.control_methods_activation_contract_mode has unsupported value: %s"
            % cm_contract_mode,
        )

    cm_default_mode = compile_projection_meta.get("control_methods_default_live_projection_mode")
    if cm_default_mode is not None and str(cm_default_mode) not in CONTROL_METHODS_ALLOWED_LIVE_PROJECTION_MODES:
        reasons.append(
            "compile_projection_meta.control_methods_default_live_projection_mode has unsupported value: %s"
            % cm_default_mode,
        )

    return reasons



def _validate_runtime_json_kernel_patch(runtime_json_kernel_patch):
    reasons = []

    if not _is_mapping(runtime_json_kernel_patch):
        return [
            "runtime_json_kernel_patch has unexpected type: %s"
            % type(runtime_json_kernel_patch).__name__,
        ]

    for key in REQUIRED_RUNTIME_JSON_KERNEL_PATCH_KEYS:
        if key not in runtime_json_kernel_patch:
            reasons.append("runtime_json_kernel_patch missing required key: %s" % key)

    unknown_keys = []
    for key in runtime_json_kernel_patch.keys():
        if key not in PROJECTED_RUNTIME_JSON_KERNEL_KEYS:
            unknown_keys.append(key)

    if unknown_keys:
        unknown_keys.sort()
        reasons.append(
            "runtime_json_kernel_patch contains unsupported top-level sections: %s"
            % ", ".join(unknown_keys),
        )

    compile_projection_meta = runtime_json_kernel_patch.get("compile_projection_meta")
    if compile_projection_meta is not None:
        reasons.extend(_validate_compile_projection_meta(compile_projection_meta))

    return reasons



def _validate_activation_process_states(payload):
    reasons = []

    if not _is_mapping(payload):
        return [
            "activation.process_states_live_projection has unexpected type: %s"
            % type(payload).__name__,
        ]

    for key in REQUIRED_ACTIVATION_PROCESS_STATES_KEYS:
        if key not in payload:
            reasons.append("activation.process_states_live_projection missing required key: %s" % key)

    contract_mode = payload.get("contract_mode")
    if contract_mode is not None and str(contract_mode) != PROCESS_STATES_ACTIVATION_CONTRACT_MODE:
        reasons.append(
            "activation.process_states_live_projection.contract_mode has unsupported value: %s"
            % contract_mode,
        )

    default_mode = payload.get("default_mode")
    if default_mode is not None and str(default_mode) not in PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES:
        reasons.append(
            "activation.process_states_live_projection.default_mode has unsupported value: %s"
            % default_mode,
        )

    candidate_count = payload.get("candidate_count")
    if candidate_count is not None and not isinstance(candidate_count, int):
        reasons.append(
            "activation.process_states_live_projection.candidate_count has unexpected type: %s"
            % type(candidate_count).__name__,
        )

    return reasons


def _validate_activation_control_methods(payload):
    """v19: Analog zu process_states, aber default-tolerant — fehlende Felder
    im Activation-Report sind nicht fatal, solange vorhandene Felder plausibel
    sind. Damit bleiben Default-off-Deployments validation-sauber."""
    reasons = []

    if not _is_mapping(payload):
        return [
            "activation.control_methods_live_projection has unexpected type: %s"
            % type(payload).__name__,
        ]

    contract_mode = payload.get("contract_mode")
    if contract_mode is not None and str(contract_mode) != CONTROL_METHODS_ACTIVATION_CONTRACT_MODE:
        reasons.append(
            "activation.control_methods_live_projection.contract_mode has unsupported value: %s"
            % contract_mode,
        )

    mode = payload.get("mode")
    if mode is not None and str(mode) not in CONTROL_METHODS_ALLOWED_LIVE_PROJECTION_MODES:
        reasons.append(
            "activation.control_methods_live_projection.mode has unsupported value: %s"
            % mode,
        )

    apply_limit = payload.get("apply_limit")
    if apply_limit is not None and not isinstance(apply_limit, int):
        reasons.append(
            "activation.control_methods_live_projection.apply_limit has unexpected type: %s"
            % type(apply_limit).__name__,
        )

    status = payload.get("status")
    allowed_status = (
        "disabled",
        "mapping_contract_missing",
        "review_only",
        "apply_limit_zero",
        "applied",
        "blocked_by_conflicts",
        "no_changes_applied",
    )
    if status is not None and str(status) not in allowed_status:
        reasons.append(
            "activation.control_methods_live_projection.status has unsupported value: %s"
            % status,
        )

    return reasons



def _validate_activation(activation):
    reasons = []

    if not _is_mapping(activation):
        return ["activation has unexpected type: %s" % type(activation).__name__]

    for key in REQUIRED_ACTIVATION_KEYS:
        if key not in activation:
            reasons.append("activation missing required key: %s" % key)

    mutation_class = activation.get("mutation_class")
    if mutation_class is not None and str(mutation_class) not in ALLOWED_MUTATION_CLASSES:
        reasons.append("activation.mutation_class has unsupported value: %s" % mutation_class)

    activation_class = activation.get("activation_class")
    if activation_class is not None and str(activation_class) not in ALLOWED_ACTIVATION_CLASSES:
        reasons.append("activation.activation_class has unsupported value: %s" % activation_class)

    process_states_live_projection = activation.get("process_states_live_projection")
    if process_states_live_projection is not None:
        reasons.extend(_validate_activation_process_states(process_states_live_projection))

    # v19: control_methods Activation-Report (nur validiert, wenn gesetzt).
    control_methods_live_projection = activation.get("control_methods_live_projection")
    if control_methods_live_projection is not None:
        reasons.extend(_validate_activation_control_methods(control_methods_live_projection))

    return reasons



def _validate_control_strategy_hints(control_strategy_hints):
    reasons = []

    if not isinstance(control_strategy_hints, list):
        return [
            "process_state_projection_sidecar.control_strategy_hints has unexpected type: %s"
            % type(control_strategy_hints).__name__,
        ]

    for index, hint in enumerate(control_strategy_hints):
        if not _is_mapping(hint):
            reasons.append(
                "process_state_projection_sidecar.control_strategy_hints[%d] has unexpected type: %s"
                % (index, type(hint).__name__),
            )
            continue
        for key in REQUIRED_CONTROL_STRATEGY_HINT_KEYS:
            if key not in hint:
                reasons.append(
                    "process_state_projection_sidecar.control_strategy_hints[%d] missing required key: %s"
                    % (index, key),
                )

    return reasons



def _validate_legacy_mapping_hints(legacy_mapping_hints):
    reasons = []

    if not _is_mapping(legacy_mapping_hints):
        return [
            "process_state_projection_sidecar.legacy_mapping_hints has unexpected type: %s"
            % type(legacy_mapping_hints).__name__,
        ]

    for key in REQUIRED_LEGACY_MAPPING_HINT_KEYS:
        if key not in legacy_mapping_hints:
            reasons.append(
                "process_state_projection_sidecar.legacy_mapping_hints missing required key: %s"
                % key,
            )

    for list_key in ("sensor_ids_hint", "actuator_ids_hint"):
        value = legacy_mapping_hints.get(list_key)
        if value is not None and not isinstance(value, list):
            reasons.append(
                "process_state_projection_sidecar.legacy_mapping_hints.%s has unexpected type: %s"
                % (list_key, type(value).__name__),
            )

    return reasons



def _validate_process_state_record(record, index):
    reasons = []

    if not _is_mapping(record):
        return [
            "process_state_projection_sidecar.records[%d] has unexpected type: %s"
            % (index, type(record).__name__),
        ]

    for key in REQUIRED_PROCESS_STATE_RECORD_KEYS:
        if key not in record:
            reasons.append(
                "process_state_projection_sidecar.records[%d] missing required key: %s"
                % (index, key),
            )

    for list_key in (
        "physical_owner_context_refs",
        "worker_refs",
        "active_rule_refs",
        "active_signal_refs",
        "active_timer_refs",
        "active_binding_refs",
    ):
        value = record.get(list_key)
        if value is not None and not isinstance(value, list):
            reasons.append(
                "process_state_projection_sidecar.records[%d].%s has unexpected type: %s"
                % (index, list_key, type(value).__name__),
            )

    control_strategy_hints = record.get("control_strategy_hints")
    if control_strategy_hints is not None:
        reasons.extend(_validate_control_strategy_hints(control_strategy_hints))

    legacy_mapping_hints = record.get("legacy_mapping_hints")
    if legacy_mapping_hints is not None:
        reasons.extend(_validate_legacy_mapping_hints(legacy_mapping_hints))

    return reasons



def _validate_process_state_projection_sidecar(sidecar):
    reasons = []

    if not _is_mapping(sidecar):
        return [
            "process_state_projection_sidecar has unexpected type: %s"
            % type(sidecar).__name__,
        ]

    for key in REQUIRED_PROCESS_STATE_SIDECAR_KEYS:
        if key not in sidecar:
            reasons.append("process_state_projection_sidecar missing required key: %s" % key)

    records = sidecar.get("records")
    if records is not None and not isinstance(records, list):
        reasons.append(
            "process_state_projection_sidecar.records has unexpected type: %s"
            % type(records).__name__,
        )
    elif isinstance(records, list):
        for index, record in enumerate(records):
            reasons.extend(_validate_process_state_record(record, index))

    physical_owner_states = sidecar.get("physical_owner_states")
    if physical_owner_states is not None and not isinstance(physical_owner_states, list):
        reasons.append(
            "process_state_projection_sidecar.physical_owner_states has unexpected type: %s"
            % type(physical_owner_states).__name__,
        )

    return reasons



def _validate_process_states_activation_contract(sidecar, process_state_sidecar):
    reasons = []

    if not _is_mapping(sidecar):
        return [
            "process_states_activation_contract has unexpected type: %s"
            % type(sidecar).__name__,
        ]

    for key in REQUIRED_PROCESS_STATES_ACTIVATION_CONTRACT_KEYS:
        if key not in sidecar:
            reasons.append("process_states_activation_contract missing required key: %s" % key)

    contract_mode = sidecar.get("contract_mode")
    if contract_mode is not None and str(contract_mode) != PROCESS_STATES_ACTIVATION_CONTRACT_MODE:
        reasons.append(
            "process_states_activation_contract.contract_mode has unsupported value: %s"
            % contract_mode,
        )

    target_section = sidecar.get("target_section")
    if target_section is not None and str(target_section) != "process_states":
        reasons.append(
            "process_states_activation_contract.target_section has unsupported value: %s"
            % target_section,
        )

    default_mode = sidecar.get("default_live_projection_mode")
    if default_mode is not None and str(default_mode) not in PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES:
        reasons.append(
            "process_states_activation_contract.default_live_projection_mode has unsupported value: %s"
            % default_mode,
        )

    for key, allowed_values in (
        ("allowed_live_projection_modes", PROCESS_STATES_ALLOWED_LIVE_PROJECTION_MODES),
        ("allowed_merge_policies", PROCESS_STATES_ALLOWED_MERGE_POLICIES),
        ("allowed_rollback_policies", PROCESS_STATES_ALLOWED_ROLLBACK_POLICIES),
    ):
        value = sidecar.get(key)
        if value is None:
            continue
        if not isinstance(value, list):
            reasons.append(
                "process_states_activation_contract.%s has unexpected type: %s"
                % (key, type(value).__name__),
            )
            continue
        for item in value:
            if str(item) not in allowed_values:
                reasons.append(
                    "process_states_activation_contract.%s contains unsupported value: %s"
                    % (key, item),
                )

    candidate_refs = sidecar.get("candidate_refs")
    if candidate_refs is not None and not isinstance(candidate_refs, list):
        reasons.append(
            "process_states_activation_contract.candidate_refs has unexpected type: %s"
            % type(candidate_refs).__name__,
        )

    candidate_count = sidecar.get("candidate_count")
    if candidate_count is not None and not isinstance(candidate_count, int):
        reasons.append(
            "process_states_activation_contract.candidate_count has unexpected type: %s"
            % type(candidate_count).__name__,
        )

    if isinstance(candidate_refs, list) and isinstance(candidate_count, int):
        if candidate_count != len(candidate_refs):
            reasons.append(
                "process_states_activation_contract.candidate_count does not match candidate_refs length"
            )

    if _is_mapping(process_state_sidecar):
        records = process_state_sidecar.get("records")
        record_count = len(records) if isinstance(records, list) else 0
        if isinstance(candidate_count, int) and candidate_count != record_count:
            reasons.append(
                "process_states_activation_contract.candidate_count does not match process_state_projection_sidecar records length"
            )

    guardrails = sidecar.get("guardrails")
    if guardrails is not None and not isinstance(guardrails, list):
        reasons.append(
            "process_states_activation_contract.guardrails has unexpected type: %s"
            % type(guardrails).__name__,
        )

    return reasons



def _validate_sidecars(sidecars):
    reasons = []

    if not _is_mapping(sidecars):
        return ["sidecars has unexpected type: %s" % type(sidecars).__name__]

    process_state_sidecar = sidecars.get("process_state_projection_sidecar")
    if process_state_sidecar is not None:
        reasons.extend(_validate_process_state_projection_sidecar(process_state_sidecar))

    process_states_activation_contract = sidecars.get("process_states_activation_contract")
    if process_states_activation_contract is not None:
        reasons.extend(
            _validate_process_states_activation_contract(
                process_states_activation_contract,
                process_state_sidecar,
            )
        )

    if process_state_sidecar is not None and process_states_activation_contract is None:
        reasons.append(
            "sidecars.process_states_activation_contract missing while process_state_projection_sidecar is present"
        )

    for key in ("runtime_manifest", "runtime_contract_bundle"):
        value = sidecars.get(key)
        if value is not None and not _is_mapping(value):
            reasons.append("sidecars.%s has unexpected type: %s" % (key, type(value).__name__))

    return reasons



def _validate_projection_bundle(bundle):
    reasons = []

    for key in REQUIRED_PROJECTION_KEYS:
        if key not in bundle:
            reasons.append("missing required projection key: %s" % key)

    runtime_json_kernel_patch = bundle.get("runtime_json_kernel_patch")
    if runtime_json_kernel_patch is not None:
        reasons.extend(_validate_runtime_json_kernel_patch(runtime_json_kernel_patch))

    activation = bundle.get("activation")
    if activation is not None:
        reasons.extend(_validate_activation(activation))

    for key in ("runtime_topology_patch", "chunk_registry_patch", "event_contract_patch"):
        value = bundle.get(key)
        if value is not None and not _is_mapping(value):
            reasons.append("%s has unexpected type: %s" % (key, type(value).__name__))

    affected_workers = bundle.get("affected_workers")
    if affected_workers is not None and not isinstance(affected_workers, (list, tuple)):
        reasons.append("affected_workers has unexpected type: %s" % type(affected_workers).__name__)

    sidecars = bundle.get("sidecars")
    if sidecars is not None:
        reasons.extend(_validate_sidecars(sidecars))

    if reasons:
        return False, reasons

    warnings = []
    for key in OPTIONAL_PROJECTION_KEYS:
        if key not in bundle:
            warnings.append("optional projection key missing: %s" % key)

    return True, warnings



def _validate_legacy_direct_bundle(bundle):
    reasons = []

    for key in LEGACY_REQUIRED_TOP_LEVEL_KEYS:
        if key not in bundle:
            reasons.append("missing required legacy key: %s" % key)

    controllers = bundle.get("controllers")
    if controllers is not None and not isinstance(controllers, (list, dict)):
        reasons.append("controllers has unexpected type: %s" % type(controllers).__name__)

    devices = bundle.get("devices")
    if devices is not None and not isinstance(devices, (list, dict)):
        reasons.append("devices has unexpected type: %s" % type(devices).__name__)

    if reasons:
        return False, reasons

    return True, ["legacy_direct_runtime_config_detected"]



def validate_compiled_bundle(bundle):
    if not _is_mapping(bundle):
        return False, ["bundle is not a dict"]

    if "runtime_json_kernel_patch" in bundle or "projection_version" in bundle:
        return _validate_projection_bundle(bundle)

    if all(key in bundle for key in LEGACY_REQUIRED_TOP_LEVEL_KEYS):
        return _validate_legacy_direct_bundle(bundle)

    return False, [
        "bundle does not match projection bridge contract",
        "projection keys expected: %s" % ", ".join(REQUIRED_PROJECTION_KEYS),
    ]
