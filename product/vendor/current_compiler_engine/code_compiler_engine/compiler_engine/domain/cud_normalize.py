from typing import Any, Dict

from compiler_engine.domain.common import stable_sort_data


DEFAULT_LIST_KEYS = [
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


DEFAULT_DICT_KEYS = [
    "metadata",
    "owner_context_hints",
    "placeholders",
]


def normalize_cud(cud: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(cud)
    for key in DEFAULT_LIST_KEYS:
        normalized.setdefault(key, [])
    for key in DEFAULT_DICT_KEYS:
        normalized.setdefault(key, {})
    owner_context_hints = dict(normalized.get("owner_context_hints", {}))
    owner_context_hints.setdefault("physical", [])
    owner_context_hints.setdefault("logical", [])
    owner_context_hints.setdefault("coordination_edges", [])
    normalized["owner_context_hints"] = owner_context_hints
    normalized.setdefault("schema_version", "1.0")
    normalized.setdefault("canonical_id", "UNSPECIFIED_CUD")
    return stable_sort_data(normalized)
