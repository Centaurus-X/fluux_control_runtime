from typing import Any, Dict, List


def determine_rule_scope(rule: Dict[str, Any], point_to_physical_context: Dict[str, str], signal_to_domain: Dict[str, str], signal_to_point: Dict[str, str]) -> str:
    read_set = list(rule.get("read_set", []))
    write_set = list(rule.get("write_set", []))
    domain_refs: List[str] = []
    physical_context_refs: List[str] = []

    signal_ref: str
    for signal_ref in read_set + write_set:
        domain_ref = signal_to_domain.get(signal_ref)
        if domain_ref is not None and domain_ref not in domain_refs:
            domain_refs.append(domain_ref)
        point_ref = signal_to_point.get(signal_ref)
        if point_ref is not None:
            context_ref = point_to_physical_context.get(point_ref)
            if context_ref is not None and context_ref not in physical_context_refs:
                physical_context_refs.append(context_ref)

    if len(domain_refs) > 1:
        return "cross_domain_placeholder"
    if len(physical_context_refs) > 1:
        return "cross_physical_same_domain"
    if len(physical_context_refs) == 1 and len(write_set) > 0:
        return "physical_context_local"
    return "logical_domain_local"
