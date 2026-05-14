from typing import Any, Dict, List

from compiler_engine.domain.common import stable_hash, stable_sort_data


RULE_STAGE_ORDER = {
    "safety": 0,
    "guard": 1,
    "fsm": 2,
    "control": 3,
    "sequence": 4,
    "projection": 5,
    "notification": 6,
}


def _index_by_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result = {}
    for item in items:
        result[item["id"]] = item
    return result


def compile_inventory(cud: Dict[str, Any], partition: Dict[str, Any]) -> Dict[str, Any]:
    assets = []
    for asset in cud.get("assets", []):
        owner_context_ref = asset.get("owner_context_hint")
        assets.append({
            "id": asset["id"],
            "kind": asset.get("kind"),
            "name": asset.get("name"),
            "asset_type": asset.get("asset_type"),
            "parent_ref": asset.get("parent_ref"),
            "owner_context_ref": owner_context_ref,
            "source_ref": asset["id"],
        })
    return stable_sort_data({
        "inventory_id": "INV_" + stable_hash({"assets": assets})[:16],
        "source_cud_hash": stable_hash(cud),
        "assets": assets,
        "physical_owner_contexts": partition.get("physical_owner_contexts", []),
    })


def compile_signal_catalog(cud: Dict[str, Any], partition: Dict[str, Any]) -> Dict[str, Any]:
    ownership_index = partition.get("ownership_index", {})
    points_by_id = _index_by_id(cud.get("points", []))
    items = []
    for signal in cud.get("signals", []):
        point_ref = signal.get("point_ref")
        point = points_by_id.get(point_ref) if point_ref else None
        items.append({
            "id": signal["id"],
            "signal_type": signal.get("signal_type"),
            "direction": signal.get("direction"),
            "domain_ref": signal.get("domain_ref"),
            "point_ref": point_ref,
            "owner_context_ref": point.get("owner_context_ref") if point else None,
            "logical_owner_context_ref": ownership_index.get("signals", {}).get(signal["id"]),
            "qos": signal.get("qos", point.get("qos") if point else {}),
            "initial_value": signal.get("initial_value"),
            "source_ref": signal["id"],
        })
    return stable_sort_data({
        "signal_catalog_id": "SIGCAT_" + stable_hash(items)[:16],
        "source_cud_hash": stable_hash(cud),
        "signals": items,
    })


def compile_io_bindings(cud: Dict[str, Any], partition: Dict[str, Any], signal_catalog: Dict[str, Any]) -> Dict[str, Any]:
    points_by_id = _index_by_id(cud.get("points", []))
    bindings = []
    for signal in signal_catalog.get("signals", []):
        point_ref = signal.get("point_ref")
        binding_id = "BND_" + signal["id"]
        if point_ref:
            point = points_by_id[point_ref]
            bindings.append({
                "id": binding_id,
                "binding_kind": "io",
                "signal_ref": signal["id"],
                "point_ref": point_ref,
                "direction": signal.get("direction"),
                "domain_ref": signal.get("domain_ref"),
                "owner_context_ref": point.get("owner_context_ref"),
                "address": point.get("address", {}),
                "codec": point.get("codec", {}),
                "qos": signal.get("qos", point.get("qos", {})),
                "source_ref": signal.get("source_ref"),
            })
        else:
            bindings.append({
                "id": binding_id,
                "binding_kind": "internal",
                "signal_ref": signal["id"],
                "point_ref": None,
                "direction": signal.get("direction"),
                "domain_ref": signal.get("domain_ref"),
                "owner_context_ref": signal.get("logical_owner_context_ref"),
                "address": {},
                "codec": {},
                "qos": signal.get("qos", {}),
                "source_ref": signal.get("source_ref"),
            })
    return stable_sort_data({
        "io_bindings_id": "IOB_" + stable_hash(bindings)[:16],
        "source_cud_hash": stable_hash(cud),
        "bindings": bindings,
    })


def compile_rule_ir(cud: Dict[str, Any], partition: Dict[str, Any]) -> Dict[str, Any]:
    ownership_index = partition.get("ownership_index", {})
    rules = []
    for rule in cud.get("rule_sources", []):
        normalized_rule = {
            "id": rule["id"],
            "stage": rule.get("stage"),
            "priority": rule.get("priority", 0),
            "domain_ref": rule.get("domain_ref"),
            "owner_context_ref": ownership_index.get("rules", {}).get(rule["id"]),
            "normalized_when": rule.get("when", {}),
            "normalized_then": rule.get("then", {}),
            "normalized_else": rule.get("else", {}),
            "source_ref": rule["id"],
            "placeholders": rule.get("placeholders", {}),
        }
        normalized_rule["rule_hash"] = stable_hash(normalized_rule)
        rules.append(normalized_rule)

    def _rule_sort_key(item: Dict[str, Any]) -> tuple[int, int, str]:
        stage_order = RULE_STAGE_ORDER.get(item.get("stage"), 999)
        priority = int(item.get("priority", 0))
        return (stage_order, -priority, item.get("id", ""))

    ordered_rules = sorted(rules, key=_rule_sort_key)
    return stable_sort_data({
        "ir_id": "RULEIR_" + stable_hash(ordered_rules)[:16],
        "source_cud_hash": stable_hash(cud),
        "hierarchy": [
            "safety",
            "guard",
            "fsm",
            "control",
            "sequence",
            "projection",
            "notification",
        ],
        "rules": ordered_rules,
    })


def compile_shard_layout(cud: Dict[str, Any], partition: Dict[str, Any], signal_catalog: Dict[str, Any]) -> Dict[str, Any]:
    physical_shards = []
    for context in partition.get("physical_owner_contexts", []):
        physical_shards.append({
            "id": "PSHARD_" + context["id"],
            "owner_context_ref": context["id"],
            "asset_refs": context.get("asset_refs", []),
            "point_refs": context.get("point_refs", []),
            "mode": "physical_adapter",
        })

    logical_shards = []
    for context in partition.get("logical_domain_contexts", []):
        logical_shards.append({
            "id": "LSHARD_" + context["id"],
            "domain_ref": context.get("domain_ref"),
            "owner_context_ref": context.get("id"),
            "signal_refs": context.get("signal_refs", []),
            "rule_refs": context.get("rule_refs", []),
            "mode": "single_writer_commit",
        })

    return stable_sort_data({
        "layout_id": "LAYOUT_" + stable_hash({"physical": physical_shards, "logical": logical_shards})[:16],
        "source_cud_hash": stable_hash(cud),
        "physical_shards": physical_shards,
        "logical_shards": logical_shards,
        "coordination_edges": partition.get("coordination_edges", []),
    })


def compile_manifest(cud: Dict[str, Any], partition: Dict[str, Any], compiled_inventory: Dict[str, Any], signal_catalog: Dict[str, Any], io_bindings: Dict[str, Any], rule_ir: Dict[str, Any], shard_layout: Dict[str, Any]) -> Dict[str, Any]:
    manifest = {
        "manifest_id": "MANIFEST_" + stable_hash({
            "canonical_id": cud.get("canonical_id"),
            "inventory": compiled_inventory.get("inventory_id"),
            "signals": signal_catalog.get("signal_catalog_id"),
            "bindings": io_bindings.get("io_bindings_id"),
            "rule_ir": rule_ir.get("ir_id"),
            "layout": shard_layout.get("layout_id"),
        })[:16],
        "canonical_id": cud.get("canonical_id"),
        "source_cud_hash": stable_hash(cud),
        "partition_id": partition.get("partition_id"),
        "inventory_hash": stable_hash(compiled_inventory),
        "signal_catalog_hash": stable_hash(signal_catalog),
        "io_bindings_hash": stable_hash(io_bindings),
        "rule_ir_hash": stable_hash(rule_ir),
        "layout_hash": stable_hash(shard_layout),
    }
    manifest["bundle_hash"] = stable_hash(manifest)
    return stable_sort_data(manifest)


def compile_boot_control_state(cud: Dict[str, Any], partition: Dict[str, Any], signal_catalog: Dict[str, Any], io_bindings: Dict[str, Any], rule_ir: Dict[str, Any]) -> Dict[str, Any]:
    bindings = io_bindings.get("bindings", [])
    domain_states = []
    for domain in cud.get("domains", []):
        active_bindings = []
        for signal in signal_catalog.get("signals", []):
            if signal.get("domain_ref") != domain["id"]:
                continue
            binding_id = "BND_" + signal["id"]
            active_bindings.append(binding_id)
        domain_states.append({
            "domain_ref": domain["id"],
            "logical_owner_context_ref": domain["id"],
            "active_signals": list(domain.get("signal_refs", [])),
            "active_rules": list(domain.get("rule_refs", [])),
            "active_parameter_sets": list(domain.get("parameter_set_refs", [])),
            "active_timers": list(domain.get("timer_refs", [])),
            "active_fsms": list(domain.get("fsm_refs", [])),
            "active_bindings": sorted(active_bindings),
        })

    physical_states = []
    for context in partition.get("physical_owner_contexts", []):
        active_bindings = []
        for binding in bindings:
            if binding.get("owner_context_ref") == context["id"]:
                active_bindings.append(binding["id"])
        physical_states.append({
            "owner_context_ref": context["id"],
            "active_bindings": sorted(active_bindings),
            "active_points": list(context.get("point_refs", [])),
        })

    return stable_sort_data({
        "boot_state_id": "BOOT_" + stable_hash({"logical": domain_states, "physical": physical_states})[:16],
        "logical_domain_states": domain_states,
        "physical_owner_states": physical_states,
    })


def build_runtime_contract_bundle(cud: Dict[str, Any], partition: Dict[str, Any], compiled_inventory: Dict[str, Any], signal_catalog: Dict[str, Any], io_bindings: Dict[str, Any], rule_ir: Dict[str, Any], shard_layout: Dict[str, Any], manifest: Dict[str, Any], boot_control_state: Dict[str, Any]) -> Dict[str, Any]:
    bundle = {
        "bundle_id": "BUNDLE_" + stable_hash({
            "manifest": manifest,
            "inventory": compiled_inventory,
            "signals": signal_catalog,
            "bindings": io_bindings,
            "rules": rule_ir,
            "layout": shard_layout,
            "boot": boot_control_state,
        })[:16],
        "manifest": manifest,
        "compiled_inventory": compiled_inventory,
        "compiled_signal_catalog": signal_catalog,
        "compiled_io_bindings": io_bindings,
        "compiled_rule_ir": rule_ir,
        "runtime_shard_layout": shard_layout,
        "runtime_control_state_boot": boot_control_state,
        "decompile_backrefs": {
            "schema_version": cud.get("schema_version"),
            "canonical_id": cud.get("canonical_id"),
            "metadata": cud.get("metadata", {}),
            "assets": cud.get("assets", []),
            "relations": cud.get("relations", []),
            "points": cud.get("points", []),
            "signals": cud.get("signals", []),
            "parameter_sets": cud.get("parameter_sets", []),
            "timer_sources": cud.get("timer_sources", []),
            "fsm_sources": cud.get("fsm_sources", []),
            "rule_sources": cud.get("rule_sources", []),
            "domains": cud.get("domains", []),
            "owner_context_hints": cud.get("owner_context_hints", {}),
            "placeholders": cud.get("placeholders", {}),
        },
    }
    return stable_sort_data(bundle)
