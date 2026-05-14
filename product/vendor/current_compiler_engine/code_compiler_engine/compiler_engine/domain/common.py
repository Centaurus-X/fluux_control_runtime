import hashlib
import json
from typing import Any, Tuple


PRESERVE_LIST_KEYS = {
    "hierarchy",
    "rules",
    "states",
    "transitions",
    "inputs",
    "set_signals",
    "force_fsm_state",
    "emit",
    "changes",
}


def stable_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(data: Any) -> str:
    payload = stable_json_dumps(data).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _sort_key(value: Any) -> str:
    if isinstance(value, dict) and "id" in value:
        return str(value["id"])
    return stable_json_dumps(value)


def stable_sort_data(data: Any, path: Tuple[str, ...] = ()) -> Any:
    if isinstance(data, dict):
        result = {}
        keys = sorted(data.keys())
        for key in keys:
            result[key] = stable_sort_data(data[key], path + (key,))
        return result
    if isinstance(data, list):
        normalized = []
        for item in data:
            normalized.append(stable_sort_data(item, path))
        if path and path[-1] in PRESERVE_LIST_KEYS:
            return normalized
        return sorted(normalized, key=_sort_key)
    return data
