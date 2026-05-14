from typing import Any, Dict

from compiler_engine.domain.common import stable_hash, stable_sort_data


def decompile_runtime_contract_bundle(bundle: Dict[str, Any]) -> Dict[str, Any]:
    backrefs = dict(bundle.get("decompile_backrefs", {}))
    result = {
        "schema_version": backrefs.get("schema_version", "1.0"),
        "canonical_id": backrefs.get("canonical_id", "UNSPECIFIED_CUD"),
        "metadata": backrefs.get("metadata", {}),
        "assets": backrefs.get("assets", []),
        "relations": backrefs.get("relations", []),
        "points": backrefs.get("points", []),
        "signals": backrefs.get("signals", []),
        "parameter_sets": backrefs.get("parameter_sets", []),
        "timer_sources": backrefs.get("timer_sources", []),
        "fsm_sources": backrefs.get("fsm_sources", []),
        "rule_sources": backrefs.get("rule_sources", []),
        "domains": backrefs.get("domains", []),
        "owner_context_hints": backrefs.get("owner_context_hints", {}),
        "placeholders": backrefs.get("placeholders", {}),
    }
    result["decompiled_from_bundle_id"] = bundle.get("bundle_id")
    result["decompiled_hash"] = stable_hash(result)
    return stable_sort_data(result)


def build_cud_delta_from_runtime_change_set(bundle: Dict[str, Any], change_set: Dict[str, Any]) -> Dict[str, Any]:
    changes = []
    for item in change_set.get("changes", []):
        changes.append({
            "id": item.get("id"),
            "classification": item.get("classification"),
            "target_ref": item.get("target_ref"),
            "source_ref": item.get("source_ref"),
            "payload": item.get("payload", {}),
        })
    result = {
        "cud_delta_id": "CUDDELTA_" + stable_hash(changes)[:16],
        "bundle_ref": bundle.get("bundle_id"),
        "changes": changes,
    }
    return stable_sort_data(result)
