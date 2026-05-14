from typing import Any, Dict, Iterable, Set


REQUIRED_CUD_KEYS = [
    "schema_version",
    "canonical_id",
    "metadata",
    "assets",
    "points",
    "signals",
    "rule_sources",
    "domains",
]


SECTION_KEYS = [
    "assets",
    "relations",
    "points",
    "signals",
    "parameter_sets",
    "timer_sources",
    "fsm_sources",
    "rule_sources",
    "domains",
]


def _collect_ids(items: Iterable[Dict[str, Any]], section_name: str) -> Set[str]:
    seen = set()
    for item in items:
        item_id = item.get("id")
        if not item_id:
            raise ValueError(f"Section {section_name} contains an element without id")
        if item_id in seen:
            raise ValueError(f"Duplicate id in section {section_name}: {item_id}")
        seen.add(item_id)
    return seen


def validate_cud(cud: Dict[str, Any]) -> None:
    for key in REQUIRED_CUD_KEYS:
        if key not in cud:
            raise ValueError(f"Missing required CUD key: {key}")

    section_ids = {}
    for section_name in SECTION_KEYS:
        items = cud.get(section_name, [])
        section_ids[section_name] = _collect_ids(items, section_name)

    point_ids = section_ids.get("points", set())
    signal_ids = section_ids.get("signals", set())
    timer_ids = section_ids.get("timer_sources", set())
    fsm_ids = section_ids.get("fsm_sources", set())
    rule_ids = section_ids.get("rule_sources", set())
    parameter_set_ids = section_ids.get("parameter_sets", set())

    for signal in cud.get("signals", []):
        point_ref = signal.get("point_ref")
        direction = signal.get("direction")
        if direction != "internal" and point_ref and point_ref not in point_ids:
            raise ValueError(f"Signal {signal['id']} references unknown point {point_ref}")

    for domain in cud.get("domains", []):
        for signal_ref in domain.get("signal_refs", []):
            if signal_ref not in signal_ids:
                raise ValueError(f"Domain {domain['id']} references unknown signal {signal_ref}")
        for timer_ref in domain.get("timer_refs", []):
            if timer_ref not in timer_ids:
                raise ValueError(f"Domain {domain['id']} references unknown timer {timer_ref}")
        for fsm_ref in domain.get("fsm_refs", []):
            if fsm_ref not in fsm_ids:
                raise ValueError(f"Domain {domain['id']} references unknown fsm {fsm_ref}")
        for rule_ref in domain.get("rule_refs", []):
            if rule_ref not in rule_ids:
                raise ValueError(f"Domain {domain['id']} references unknown rule {rule_ref}")
        for parameter_set_ref in domain.get("parameter_set_refs", []):
            if parameter_set_ref not in parameter_set_ids:
                raise ValueError(f"Domain {domain['id']} references unknown parameter set {parameter_set_ref}")
