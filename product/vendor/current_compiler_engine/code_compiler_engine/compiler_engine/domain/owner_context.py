from typing import Any, Dict, List

from compiler_engine.domain.common import stable_hash, stable_sort_data


def _build_ownership_index(partition: Dict[str, Any]) -> Dict[str, Any]:
    ownership_index = {"points": {}, "signals": {}, "rules": {}, "domains": {}}
    for context in partition.get("physical_owner_contexts", []):
        for point_ref in context.get("point_refs", []):
            ownership_index["points"][point_ref] = context["id"]
    for context in partition.get("logical_domain_contexts", []):
        ownership_index["domains"][context.get("domain_ref")] = context["id"]
        for signal_ref in context.get("signal_refs", []):
            ownership_index["signals"][signal_ref] = context["id"]
        for rule_ref in context.get("rule_refs", []):
            ownership_index["rules"][rule_ref] = context["id"]
    return ownership_index


def build_owner_context_partition(cud: Dict[str, Any]) -> Dict[str, Any]:
    hints = cud.get("owner_context_hints", {})
    physical_owner_contexts = list(hints.get("physical", []))
    logical_hints = {item["id"]: item for item in hints.get("logical", [])}

    logical_domain_contexts: List[Dict[str, Any]] = []
    for domain in cud.get("domains", []):
        logical_item = {
            "id": domain["id"],
            "domain_ref": domain["id"],
            "signal_refs": list(domain.get("signal_refs", [])),
            "rule_refs": list(domain.get("rule_refs", [])),
            "parameter_set_refs": list(domain.get("parameter_set_refs", [])),
            "timer_refs": list(domain.get("timer_refs", [])),
            "fsm_refs": list(domain.get("fsm_refs", [])),
            "physical_owner_context_refs": list(domain.get("physical_owner_context_refs", [])),
        }
        if domain["id"] in logical_hints:
            logical_item.update(logical_hints[domain["id"]])
        logical_domain_contexts.append(logical_item)

    partition = {
        "partition_id": "PARTITION_" + stable_hash({
            "canonical_id": cud.get("canonical_id"),
            "physical": physical_owner_contexts,
            "logical": logical_domain_contexts,
        })[:16],
        "source_cud_hash": stable_hash(cud),
        "physical_owner_contexts": stable_sort_data(physical_owner_contexts),
        "logical_domain_contexts": stable_sort_data(logical_domain_contexts),
        "coordination_edges": stable_sort_data(hints.get("coordination_edges", [])),
    }
    partition["ownership_index"] = _build_ownership_index(partition)
    return stable_sort_data(partition)
