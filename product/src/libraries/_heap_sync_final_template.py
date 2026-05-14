# -*- coding: utf-8 -*-

# src/libraries/_heap_sync_final_template.py


import copy
import json
import os
import threading
import time
from functools import partial
from pathlib import Path
from typing import Any, Callable


_STOP = object()
_RING_CLOSED = object()
_RING_TIMEOUT = object()
_REPLICATION_ENQUEUE_TIMEOUT_S = 0.5


# ============================================================
# JSON helpers
# ============================================================

def deepcopy_json(value: Any) -> Any:
    return copy.deepcopy(value)


def fnv1a_64(text: str) -> int:
    value = 0xCBF29CE484222325

    for byte in text.encode("utf-8"):
        value ^= int(byte)
        value = (value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF

    return value


def json_pointer_escape(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def json_pointer_unescape(token: str) -> str:
    return token.replace("~1", "/").replace("~0", "~")


def split_json_pointer(pointer: str) -> list[str]:
    if pointer == "":
        return []

    if not pointer.startswith("/"):
        raise ValueError("JSON Pointer muss leer oder mit '/' beginnen")

    return [json_pointer_unescape(part) for part in pointer[1:].split("/")]


def get_container_and_key(document: Any, pointer: str) -> tuple[Any, str | int | None]:
    parts = split_json_pointer(pointer)

    if not parts:
        return None, None

    current = document

    for part in parts[:-1]:
        if isinstance(current, list):
            current = current[int(part)]
            continue

        if isinstance(current, dict):
            current = current[part]
            continue

        raise TypeError(f"Ungültiger JSON-Pointer-Pfad: {pointer!r}")

    last = parts[-1]

    if isinstance(current, list):
        if last == "-":
            return current, last

        return current, int(last)

    return current, last


def json_get(document: Any, pointer: str) -> Any:
    parts = split_json_pointer(pointer)
    current = document

    for part in parts:
        if isinstance(current, list):
            current = current[int(part)]
            continue

        if isinstance(current, dict):
            current = current[part]
            continue

        raise TypeError(f"Ungültiger JSON-Pointer-Pfad: {pointer!r}")

    return current


def json_add(document: Any, pointer: str, value: Any) -> Any:
    if pointer == "":
        return deepcopy_json(value)

    container, key = get_container_and_key(document, pointer)

    if isinstance(container, list):
        if key == "-":
            container.append(deepcopy_json(value))
            return document

        container.insert(int(key), deepcopy_json(value))
        return document

    container[key] = deepcopy_json(value)
    return document


def json_replace(document: Any, pointer: str, value: Any) -> Any:
    if pointer == "":
        return deepcopy_json(value)

    container, key = get_container_and_key(document, pointer)

    if isinstance(container, list):
        container[int(key)] = deepcopy_json(value)
        return document

    if key not in container:
        raise KeyError(pointer)

    container[key] = deepcopy_json(value)
    return document


def json_remove(document: Any, pointer: str) -> Any:
    if pointer == "":
        raise ValueError("Root kann nicht mit remove entfernt werden")

    container, key = get_container_and_key(document, pointer)

    if isinstance(container, list):
        del container[int(key)]
        return document

    del container[key]
    return document


def apply_json_patch(document: Any, patch_ops: list[dict[str, Any]]) -> Any:
    current = deepcopy_json(document)

    for op in patch_ops:
        kind = op["op"]

        if kind == "test":
            actual = json_get(current, op["path"])

            if actual != op["value"]:
                raise ValueError(
                    f"JSON-Patch test fehlgeschlagen auf {op['path']!r}: "
                    f"{actual!r} != {op['value']!r}"
                )

            continue

        if kind == "add":
            current = json_add(current, op["path"], op["value"])
            continue

        if kind == "replace":
            current = json_replace(current, op["path"], op["value"])
            continue

        if kind == "remove":
            current = json_remove(current, op["path"])
            continue

        if kind == "move":
            moved = json_get(current, op["from"])
            current = json_remove(current, op["from"])
            current = json_add(current, op["path"], moved)
            continue

        if kind == "copy":
            copied = json_get(current, op["from"])
            current = json_add(current, op["path"], copied)
            continue

        raise ValueError(f"Nicht unterstützte JSON-Patch-Operation: {kind!r}")

    return current


def make_json_patch(before: Any, after: Any, base_pointer: str = "") -> list[dict[str, Any]]:
    ops: list[dict[str, Any]] = []

    if before is None and after is None:
        return ops

    if before is None:
        ops.append({"op": "add", "path": base_pointer or "", "value": deepcopy_json(after)})
        return ops

    if after is None:
        ops.append({"op": "remove", "path": base_pointer or ""})
        return ops

    if type(before) is not type(after):
        ops.append({"op": "replace", "path": base_pointer or "", "value": deepcopy_json(after)})
        return ops

    if isinstance(before, dict):
        before_keys = set(before)
        after_keys = set(after)

        for key in sorted(before_keys - after_keys):
            path = f"{base_pointer}/{json_pointer_escape(str(key))}" if base_pointer else f"/{json_pointer_escape(str(key))}"
            ops.append({"op": "remove", "path": path})

        for key in sorted(after_keys - before_keys):
            path = f"{base_pointer}/{json_pointer_escape(str(key))}" if base_pointer else f"/{json_pointer_escape(str(key))}"
            ops.append({"op": "add", "path": path, "value": deepcopy_json(after[key])})

        for key in sorted(before_keys & after_keys):
            path = f"{base_pointer}/{json_pointer_escape(str(key))}" if base_pointer else f"/{json_pointer_escape(str(key))}"
            ops.extend(make_json_patch(before[key], after[key], path))

        return ops

    if isinstance(before, list):
        if before != after:
            ops.append({"op": "replace", "path": base_pointer or "", "value": deepcopy_json(after)})

        return ops

    if before != after:
        ops.append({"op": "replace", "path": base_pointer or "", "value": deepcopy_json(after)})

    return ops


# ============================================================
# Ringbuffer Queue (C11-nah)
# ============================================================

def make_ring_queue(capacity: int) -> dict[str, Any]:
    if capacity <= 0:
        raise ValueError("capacity muss > 0 sein")

    lock = threading.Lock()

    return {
        "capacity": int(capacity),
        "buffer": [None] * int(capacity),
        "head": 0,
        "tail": 0,
        "size": 0,
        "closed": False,
        "lock": lock,
        "not_empty": threading.Condition(lock),
        "not_full": threading.Condition(lock),
    }


def ring_queue_close(ring: dict[str, Any]) -> None:
    with ring["lock"]:
        ring["closed"] = True
        ring["not_empty"].notify_all()
        ring["not_full"].notify_all()


def _remaining_timeout(deadline: float | None) -> float | None:
    if deadline is None:
        return None

    remaining = float(deadline) - time.monotonic()

    if remaining <= 0:
        return 0.0

    return remaining


def ring_queue_put(ring: dict[str, Any], item: Any, timeout: float | None = None) -> bool:
    deadline = None if timeout is None else time.monotonic() + float(timeout)

    with ring["lock"]:
        while int(ring["size"]) >= int(ring["capacity"]):
            if ring["closed"]:
                return False

            remaining = _remaining_timeout(deadline)

            if remaining == 0.0:
                return False

            ring["not_full"].wait(timeout=remaining)

        if ring["closed"]:
            return False

        ring["buffer"][ring["tail"]] = item
        ring["tail"] = (int(ring["tail"]) + 1) % int(ring["capacity"])
        ring["size"] = int(ring["size"]) + 1
        ring["not_empty"].notify()
        return True


def ring_queue_try_put(ring: dict[str, Any], item: Any) -> bool:
    with ring["lock"]:
        if ring["closed"]:
            return False

        if int(ring["size"]) >= int(ring["capacity"]):
            return False

        ring["buffer"][ring["tail"]] = item
        ring["tail"] = (int(ring["tail"]) + 1) % int(ring["capacity"])
        ring["size"] = int(ring["size"]) + 1
        ring["not_empty"].notify()
        return True


def ring_queue_get(ring: dict[str, Any], timeout: float | None = None) -> Any:
    deadline = None if timeout is None else time.monotonic() + float(timeout)

    with ring["lock"]:
        while int(ring["size"]) == 0:
            if ring["closed"]:
                return _RING_CLOSED

            remaining = _remaining_timeout(deadline)

            if remaining == 0.0:
                return _RING_TIMEOUT

            ring["not_empty"].wait(timeout=remaining)

        item = ring["buffer"][ring["head"]]
        ring["buffer"][ring["head"]] = None
        ring["head"] = (int(ring["head"]) + 1) % int(ring["capacity"])
        ring["size"] = int(ring["size"]) - 1
        ring["not_full"].notify()
        return item


def make_reply_ring() -> dict[str, Any]:
    return make_ring_queue(1)


def wait_reply(reply_ring: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
    result = ring_queue_get(reply_ring, timeout=timeout)

    if result is _RING_TIMEOUT:
        raise TimeoutError("Antwort-Timeout")

    if result is _RING_CLOSED:
        raise RuntimeError("Antwortkanal geschlossen")

    return result


# ============================================================
# Persistenz
# ============================================================

def atomic_write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)

    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(tmp, path)


def append_json_line(path: Path, payload: dict[str, Any], do_fsync: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        handle.write("\n")
        handle.flush()

        if do_fsync:
            os.fsync(handle.fileno())


def rewrite_json_lines(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")

    with open(tmp, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(tmp, path)


def read_json_lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    items: list[dict[str, Any]] = []

    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()

            if not line:
                continue

            items.append(json.loads(line))

    return items


def wal_path(node_dir: Path, shard_id: int) -> Path:
    return node_dir / f"shard_{int(shard_id)}.wal.jsonl"


def snapshot_path(node_dir: Path, shard_id: int) -> Path:
    return node_dir / f"shard_{int(shard_id)}.snapshot.json"


def compact_wal_after_index(node_dir: Path, shard_id: int, keep_from_index: int) -> None:
    keep_from = int(keep_from_index)
    filtered: list[dict[str, Any]] = []

    for record in read_json_lines(wal_path(node_dir, shard_id)):
        record_type = record["record_type"]

        if record_type == "entry":
            if int(record["index"]) >= keep_from:
                filtered.append(record)
            continue

        if record_type == "commit":
            if int(record["commit_index"]) >= keep_from:
                filtered.append(record)
            continue

        raise ValueError(f"Unbekannter WAL record_type: {record_type!r}")

    rewrite_json_lines(wal_path(node_dir, shard_id), filtered)


def reset_wal_file(node_dir: Path, shard_id: int) -> None:
    rewrite_json_lines(wal_path(node_dir, shard_id), [])


def make_persistence_worker(node_dir: Path, capacity: int = 32768) -> tuple[dict[str, Any], threading.Thread]:
    ring = make_ring_queue(capacity)

    def reply_ok(reply_ring: dict[str, Any] | None) -> None:
        if reply_ring is not None:
            ring_queue_put(reply_ring, {"ok": True})

    def reply_error(reply_ring: dict[str, Any] | None, exc: Exception) -> None:
        if reply_ring is not None:
            ring_queue_put(reply_ring, {"ok": False, "error": repr(exc)})

    def worker() -> None:
        while True:
            item = ring_queue_get(ring)

            if item is _RING_CLOSED or item is _STOP:
                return

            reply_ring = item.get("reply")

            try:
                kind = item["kind"]

                if kind == "wal_append":
                    append_json_line(
                        wal_path(node_dir, int(item["shard_id"])),
                        item["record"],
                        bool(item["fsync"]),
                    )
                    reply_ok(reply_ring)
                    continue

                if kind == "snapshot_compact":
                    atomic_write_json_file(
                        snapshot_path(node_dir, int(item["shard_id"])),
                        item["payload"],
                    )
                    compact_wal_after_index(
                        node_dir=node_dir,
                        shard_id=int(item["shard_id"]),
                        keep_from_index=int(item["keep_from_index"]),
                    )
                    reply_ok(reply_ring)
                    continue

                if kind == "snapshot_reset_wal":
                    atomic_write_json_file(
                        snapshot_path(node_dir, int(item["shard_id"])),
                        item["payload"],
                    )
                    reset_wal_file(node_dir=node_dir, shard_id=int(item["shard_id"]))
                    reply_ok(reply_ring)
                    continue

                raise ValueError(f"Unbekannter persistence kind: {kind!r}")
            except Exception as exc:
                reply_error(reply_ring, exc)

    thread = threading.Thread(target=worker, daemon=True, name=f"persist-{node_dir.name}")
    thread.start()

    return ring, thread


def wait_persist_ok(reply_ring: dict[str, Any]) -> None:
    result = wait_reply(reply_ring)

    if not result["ok"]:
        raise RuntimeError(result["error"])


def persist_wal_sync(persist_ring: dict[str, Any], shard_id: int, record: dict[str, Any], fsync: bool) -> None:
    reply_ring = make_reply_ring()
    ok = ring_queue_put(
        persist_ring,
        {
            "kind": "wal_append",
            "shard_id": int(shard_id),
            "record": deepcopy_json(record),
            "fsync": bool(fsync),
            "reply": reply_ring,
        },
    )

    if not ok:
        raise RuntimeError("Persistenzqueue geschlossen")

    wait_persist_ok(reply_ring)
    ring_queue_close(reply_ring)


def persist_wal_async(persist_ring: dict[str, Any], shard_id: int, record: dict[str, Any], fsync: bool) -> None:
    ok = ring_queue_put(
        persist_ring,
        {
            "kind": "wal_append",
            "shard_id": int(shard_id),
            "record": deepcopy_json(record),
            "fsync": bool(fsync),
            "reply": None,
        },
    )

    if not ok:
        raise RuntimeError("Persistenzqueue geschlossen")


def persist_snapshot_compact_async(
    persist_ring: dict[str, Any],
    shard_id: int,
    payload: dict[str, Any],
    keep_from_index: int,
) -> None:
    ok = ring_queue_put(
        persist_ring,
        {
            "kind": "snapshot_compact",
            "shard_id": int(shard_id),
            "payload": deepcopy_json(payload),
            "keep_from_index": int(keep_from_index),
            "reply": None,
        },
    )

    if not ok:
        raise RuntimeError("Persistenzqueue geschlossen")


def persist_snapshot_reset_wal_sync(
    persist_ring: dict[str, Any],
    shard_id: int,
    payload: dict[str, Any],
) -> None:
    reply_ring = make_reply_ring()
    ok = ring_queue_put(
        persist_ring,
        {
            "kind": "snapshot_reset_wal",
            "shard_id": int(shard_id),
            "payload": deepcopy_json(payload),
            "reply": reply_ring,
        },
    )

    if not ok:
        raise RuntimeError("Persistenzqueue geschlossen")

    wait_persist_ok(reply_ring)
    ring_queue_close(reply_ring)


# ============================================================
# Schema / Dataset Shape
# ============================================================

def make_schema_entry(pk: str | None = None, mode: str = "mutable", shape: str = "rows") -> dict[str, Any]:
    if mode not in {"mutable", "readonly"}:
        raise ValueError("mode muss 'mutable' oder 'readonly' sein")

    if shape not in {"rows", "document"}:
        raise ValueError("shape muss 'rows' oder 'document' sein")

    if shape == "rows" and (pk is None or str(pk) == ""):
        raise ValueError("rows-Schema benötigt einen Primärschlüssel")

    if shape == "document":
        pk = None

    return {"pk": pk, "mode": mode, "shape": shape}


def make_document_schema_entry(mode: str = "mutable") -> dict[str, Any]:
    return make_schema_entry(pk=None, mode=mode, shape="document")


def table_exists_in_schema(schema: dict[str, dict[str, Any]], table_name: str) -> None:
    if table_name not in schema:
        raise KeyError(f"Tabelle {table_name!r} fehlt im Schema")


def table_shape(schema: dict[str, dict[str, Any]], table_name: str) -> str:
    table_exists_in_schema(schema, table_name)
    return str(schema[table_name]["shape"])


def table_pk(schema: dict[str, dict[str, Any]], table_name: str) -> str:
    table_exists_in_schema(schema, table_name)
    pk_value = schema[table_name].get("pk")

    if not pk_value:
        raise ValueError(f"Tabelle {table_name!r} hat keinen Primärschlüssel")

    return str(pk_value)


def ensure_table_mutable(schema: dict[str, dict[str, Any]], table_name: str) -> None:
    table_exists_in_schema(schema, table_name)

    if schema[table_name]["mode"] != "mutable":
        raise PermissionError(f"Tabelle {table_name!r} ist readonly")


def is_scalar_json_value(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def route_row_to_shard(table_name: str, pk_value: str, shard_count: int) -> int:
    return fnv1a_64(f"row:{table_name}:{pk_value}") % int(shard_count)


def route_document_to_shard(table_name: str, shard_count: int) -> int:
    return fnv1a_64(f"doc:{table_name}") % int(shard_count)


def route_command_to_shard(schema: dict[str, dict[str, Any]], command: dict[str, Any], shard_count: int) -> int:
    table_name = str(command["table"])
    shape = table_shape(schema, table_name)

    if shape == "rows":
        return route_row_to_shard(table_name, str(command["pk"]), shard_count)

    return route_document_to_shard(table_name, shard_count)


def normalize_dataset(
    dataset: dict[str, Any],
    schema: dict[str, dict[str, Any]],
    shard_count: int,
) -> list[dict[str, Any]]:
    shards: list[dict[str, Any]] = []

    for _ in range(int(shard_count)):
        shards.append({"tables": {}, "versions": {}, "global_version": 0})

    for table_name, value in dataset.items():
        shape = table_shape(schema, table_name)

        if shape == "rows":
            if not isinstance(value, list):
                raise TypeError(f"Tabelle {table_name!r} erwartet list[dict], bekam {type(value).__name__}")

            pk_field = table_pk(schema, table_name)

            for row in value:
                if not isinstance(row, dict):
                    raise TypeError(f"Tabelle {table_name!r} erwartet dict-Zeilen")

                if pk_field not in row:
                    raise KeyError(f"Tabelle {table_name!r}: PK {pk_field!r} fehlt in {row!r}")

                pk_value = str(row[pk_field])
                shard_id = route_row_to_shard(table_name, pk_value, shard_count)
                shard = shards[shard_id]
                shard["tables"].setdefault(table_name, {})
                shard["versions"].setdefault(table_name, 0)
                shard["tables"][table_name][pk_value] = deepcopy_json(row)

            continue

        shard_id = route_document_to_shard(table_name, shard_count)
        shard = shards[shard_id]
        shard["tables"][table_name] = deepcopy_json(value)
        shard["versions"].setdefault(table_name, 0)

    return shards


def row_sort_key(row: dict[str, Any], pk_field: str) -> tuple[str]:
    return (str(row[pk_field]),)


def export_table_value_for_snapshot(
    table_value: Any,
    schema: dict[str, dict[str, Any]],
    table_name: str,
) -> Any:
    shape = table_shape(schema, table_name)

    if shape == "rows":
        pk_field = table_pk(schema, table_name)
        rows = deepcopy_json(list(table_value.values()))
        rows.sort(key=partial(row_sort_key, pk_field=pk_field))
        return rows

    return deepcopy_json(table_value)


def export_shard_snapshot_file(
    snapshot: dict[str, Any],
    schema: dict[str, dict[str, Any]],
    node_id: str,
    shard_id: int,
    last_applied_index: int,
) -> dict[str, Any]:
    dataset: dict[str, Any] = {}

    for table_name in sorted(snapshot["tables"]):
        dataset[table_name] = export_table_value_for_snapshot(
            snapshot["tables"][table_name],
            schema,
            table_name,
        )

    return {
        "meta": {
            "node_id": node_id,
            "shard_id": int(shard_id),
            "last_applied_index": int(last_applied_index),
            "global_version": int(snapshot["global_version"]),
            "versions": dict(snapshot["versions"]),
            "format_version": 4,
        },
        "data": dataset,
    }


def import_shard_snapshot_file(payload: dict[str, Any], schema: dict[str, dict[str, Any]]) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    dataset = payload.get("data", {})

    for table_name, value in dataset.items():
        shape = table_shape(schema, table_name)

        if shape == "rows":
            pk_field = table_pk(schema, table_name)
            tables[table_name] = {}

            if not isinstance(value, list):
                raise TypeError(f"Snapshot-Tabelle {table_name!r} erwartet list[dict]")

            for row in value:
                if pk_field not in row:
                    raise KeyError(f"Snapshot-Tabelle {table_name!r}: PK {pk_field!r} fehlt")
                tables[table_name][str(row[pk_field])] = deepcopy_json(row)

            continue

        tables[table_name] = deepcopy_json(value)

    return {
        "tables": tables,
        "versions": dict(payload.get("meta", {}).get("versions", {})),
        "global_version": int(payload.get("meta", {}).get("global_version", 0)),
        "last_applied_index": int(payload.get("meta", {}).get("last_applied_index", 0)),
    }


def make_published_snapshot(state: dict[str, Any], last_applied_index: int) -> dict[str, Any]:
    return {
        "tables": deepcopy_json(state["tables"]),
        "versions": dict(state["versions"]),
        "global_version": int(state["global_version"]),
        "last_applied_index": int(last_applied_index),
    }


def make_incremental_published_snapshot(
    previous_published: dict[str, Any],
    state: dict[str, Any],
    last_applied_index: int,
    changed_table_name: str,
) -> dict[str, Any]:
    new_tables = dict(previous_published["tables"])

    if changed_table_name in state["tables"]:
        new_tables[changed_table_name] = deepcopy_json(state["tables"][changed_table_name])
    else:
        new_tables.pop(changed_table_name, None)

    return {
        "tables": new_tables,
        "versions": dict(state["versions"]),
        "global_version": int(state["global_version"]),
        "last_applied_index": int(last_applied_index),
    }


def export_node_dataset(node: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for table_name in sorted(node["schema"]):
        shape = table_shape(node["schema"], table_name)

        if shape == "rows":
            merged_rows: dict[str, Any] = {}

            for shard in node["shards"]:
                snapshot = read_shard_snapshot_table(shard, table_name)

                if snapshot["present"]:
                    merged_rows.update(snapshot["table"])

            result[table_name] = export_table_value_for_snapshot(
                merged_rows,
                node["schema"],
                table_name,
            )
            continue

        seen = False
        doc_value = None

        for shard in node["shards"]:
            snapshot = read_shard_snapshot_table(shard, table_name)

            if not snapshot["present"]:
                continue

            current = snapshot["table"]

            if not seen:
                doc_value = current
                seen = True
                continue

            if current != doc_value:
                raise ValueError(f"Dokument-Tabelle {table_name!r} liegt inkonsistent auf mehreren Shards")

        if seen:
            result[table_name] = doc_value

    return result


def export_node_dataset_v2(node: dict[str, Any]) -> dict[str, Any]:
    return export_node_dataset(node)


def read_node_published_versions(node: dict[str, Any]) -> dict[int, int]:
    versions: dict[int, int] = {}

    for shard_id, shard in enumerate(node["shards"]):
        meta = read_shard_snapshot_meta(shard)
        versions[shard_id] = int(meta["global_version"])

    return versions


def make_snapshot_cache() -> dict[str, Any]:
    return {
        "lock": threading.Lock(),
        "last_versions": {},
        "cached_result": None,
    }


def export_node_dataset_cached(
    node: dict[str, Any],
    cache: dict[str, Any],
    max_retries: int = 2,
) -> dict[str, Any]:
    retries = max(1, int(max_retries))
    fresh: dict[str, Any] = {}
    versions_after: dict[int, int] = {}

    for _ in range(retries):
        versions_before = read_node_published_versions(node)

        with cache["lock"]:
            if cache["cached_result"] is not None and cache["last_versions"] == versions_before:
                return deepcopy_json(cache["cached_result"])

        fresh = export_node_dataset(node)
        versions_after = read_node_published_versions(node)

        if versions_before == versions_after:
            with cache["lock"]:
                cache["last_versions"] = dict(versions_after)
                cache["cached_result"] = deepcopy_json(fresh)

            return fresh

    with cache["lock"]:
        cache["last_versions"] = dict(versions_after)
        cache["cached_result"] = deepcopy_json(fresh)

    return fresh


# ============================================================
# Dataset Diagnose / Schema Inferenz
# ============================================================

def tokenize_name(value: str) -> list[str]:
    tokens = [token for token in str(value).replace("-", "_").split("_") if token]
    normalized = []

    for token in tokens:
        normalized.append(token)
        if token.endswith("s") and len(token) > 1:
            normalized.append(token[:-1])

    return normalized


def score_pk_candidate(table_name: str, key_name: str) -> tuple[int, int, int]:
    table_tokens = set(tokenize_name(table_name))
    key_tokens = set(tokenize_name(key_name))
    overlap = len(table_tokens & key_tokens)

    score = 0

    if key_name == "id":
        score += 100

    singular = table_name[:-1] if table_name.endswith("s") else table_name

    if key_name == f"{singular}_id":
        score += 90

    if key_name == "key":
        score += 80

    if key_name.endswith("_id"):
        score += 70

    if key_name.endswith("_key"):
        score += 60

    if key_name.endswith("id"):
        score += 50

    score += overlap * 5

    return score, overlap, len(key_name)


def find_unique_scalar_keys(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []

    common_keys = set.intersection(*(set(row.keys()) for row in rows))
    candidates: list[str] = []

    for key in sorted(common_keys):
        values = [row[key] for row in rows]

        if not all(is_scalar_json_value(value) for value in values):
            continue

        frozen_values = [json.dumps(value, sort_keys=True, ensure_ascii=False) for value in values]

        if len(set(frozen_values)) == len(frozen_values):
            candidates.append(key)

    return candidates


def infer_pk_for_rows(table_name: str, rows: list[dict[str, Any]]) -> str:
    candidates = find_unique_scalar_keys(rows)

    if not candidates:
        raise ValueError(f"Keine geeignete PK-Inferenz für Tabelle {table_name!r}")

    ranked = sorted(
        candidates,
        key=partial(score_pk_candidate, table_name),
        reverse=True,
    )
    return ranked[0]


def infer_schema_from_dataset(
    dataset: dict[str, Any],
    pk_overrides: dict[str, str] | None = None,
    mode_overrides: dict[str, str] | None = None,
    shape_overrides: dict[str, str] | None = None,
    default_mode: str = "mutable",
) -> dict[str, dict[str, Any]]:
    pk_overrides = dict(pk_overrides or {})
    mode_overrides = dict(mode_overrides or {})
    shape_overrides = dict(shape_overrides or {})
    schema: dict[str, dict[str, Any]] = {}

    for table_name, value in dataset.items():
        if table_name in shape_overrides:
            shape = str(shape_overrides[table_name])
        elif isinstance(value, list) and all(isinstance(item, dict) for item in value):
            shape = "rows"
        else:
            shape = "document"

        mode = str(mode_overrides.get(table_name, default_mode))

        if shape == "document":
            schema[table_name] = make_document_schema_entry(mode=mode)
            continue

        pk_field = pk_overrides.get(table_name)

        if pk_field is None:
            pk_field = infer_pk_for_rows(table_name, value)

        schema[table_name] = make_schema_entry(pk=pk_field, mode=mode, shape="rows")

    return schema


def analyze_dataset_against_schema(dataset: dict[str, Any], schema: dict[str, dict[str, Any]]) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    tables: dict[str, dict[str, Any]] = {}

    for table_name, value in dataset.items():
        table_info = {
            "shape": None,
            "status": "ok",
            "rows": None,
            "pk": None,
            "notes": [],
        }

        try:
            shape = table_shape(schema, table_name)
            table_info["shape"] = shape
        except Exception as exc:
            issues.append(f"{table_name}: Schema fehlt oder ist ungültig: {exc!r}")
            table_info["status"] = "error"
            table_info["notes"].append(repr(exc))
            tables[table_name] = table_info
            continue

        if shape == "rows":
            table_info["pk"] = table_pk(schema, table_name)

            if not isinstance(value, list):
                issues.append(f"{table_name}: erwartet list[dict], bekam {type(value).__name__}")
                table_info["status"] = "error"
                tables[table_name] = table_info
                continue

            seen = set()

            for index, row in enumerate(value):
                if not isinstance(row, dict):
                    issues.append(f"{table_name}[{index}]: erwartet dict-Zeile")
                    table_info["status"] = "error"
                    continue

                pk_field = table_pk(schema, table_name)

                if pk_field not in row:
                    issues.append(f"{table_name}[{index}]: PK {pk_field!r} fehlt")
                    table_info["status"] = "error"
                    continue

                pk_value = str(row[pk_field])

                if pk_value in seen:
                    issues.append(f"{table_name}: doppelte PK {pk_value!r}")
                    table_info["status"] = "error"
                else:
                    seen.add(pk_value)

            table_info["rows"] = len(value)
            tables[table_name] = table_info
            continue

        table_info["notes"].append("document table")
        tables[table_name] = table_info

    for table_name in schema:
        if table_name not in dataset:
            warnings.append(f"{table_name}: im Schema vorhanden, aber nicht im Dataset")

    return {"ok": len(issues) == 0, "issues": issues, "warnings": warnings, "tables": tables}


# ============================================================
# Event Bus
# ============================================================

def make_bus(subscriber_capacity: int = 2048) -> dict[str, Any]:
    return {
        "lock": threading.Lock(),
        "subscriber_capacity": int(subscriber_capacity),
        "subscribers": [],
    }


def subscribe(
    bus: dict[str, Any],
    handler: Callable[[dict[str, Any]], None],
    event_filter: Callable[[dict[str, Any]], bool] | None = None,
) -> dict[str, Any]:
    ring = make_ring_queue(int(bus["subscriber_capacity"]))

    def worker() -> None:
        while True:
            item = ring_queue_get(ring)

            if item is _RING_CLOSED or item is _STOP:
                return

            handler(item)

    thread = threading.Thread(target=worker, daemon=True, name="event-subscriber")
    thread.start()

    subscriber = {
        "ring": ring,
        "filter": event_filter,
        "thread": thread,
        "dropped": 0,
        "stats_lock": threading.Lock(),
    }

    with bus["lock"]:
        bus["subscribers"].append(subscriber)

    return subscriber


def publish(bus: dict[str, Any], event: dict[str, Any]) -> None:
    with bus["lock"]:
        subscribers = list(bus["subscribers"])

    for subscriber in subscribers:
        predicate = subscriber["filter"]

        if predicate is not None and not predicate(event):
            continue

        ok = ring_queue_try_put(subscriber["ring"], deepcopy_json(event))

        if not ok:
            with subscriber["stats_lock"]:
                subscriber["dropped"] = int(subscriber["dropped"]) + 1


def stop_bus(bus: dict[str, Any], timeout: float = 1.0) -> None:
    with bus["lock"]:
        subscribers = list(bus["subscribers"])
        bus["subscribers"].clear()

    for subscriber in subscribers:
        ring_queue_put(subscriber["ring"], _STOP)
        ring_queue_close(subscriber["ring"])

    for subscriber in subscribers:
        subscriber["thread"].join(timeout=timeout)


# ============================================================
# WAL / Replay
# ============================================================

def make_entry_record(index: int, command: dict[str, Any], source_node_id: str) -> dict[str, Any]:
    return {
        "record_type": "entry",
        "index": int(index),
        "command": deepcopy_json(command),
        "source_node_id": source_node_id,
        "ts_ns": time.time_ns(),
    }


def make_commit_record(commit_index: int, source_node_id: str) -> dict[str, Any]:
    return {
        "record_type": "commit",
        "commit_index": int(commit_index),
        "source_node_id": source_node_id,
        "ts_ns": time.time_ns(),
    }


def preview_command(state: dict[str, Any], schema: dict[str, dict[str, Any]], command: dict[str, Any]) -> dict[str, Any]:
    preview_state = {
        "tables": deepcopy_json(state["tables"]),
        "versions": dict(state["versions"]),
        "global_version": int(state["global_version"]),
    }
    return apply_command_to_state(preview_state, schema, command)


def apply_row_table_command(
    state: dict[str, Any],
    schema: dict[str, dict[str, Any]],
    command: dict[str, Any],
) -> dict[str, Any]:
    table_name = str(command["table"])
    pk_value = str(command["pk"])
    kind = str(command["kind"])
    table = state["tables"].setdefault(table_name, {})
    state["versions"].setdefault(table_name, 0)
    before_row = deepcopy_json(table.get(pk_value))

    if kind == "patch":
        if pk_value not in table:
            raise KeyError(f"{table_name!r}/{pk_value!r} existiert nicht für patch")

        after_row = apply_json_patch(table[pk_value], command["patch"])
        pk_field = table_pk(schema, table_name)

        if pk_field not in after_row:
            after_row[pk_field] = pk_value

        if str(after_row[pk_field]) != pk_value:
            raise ValueError("Primärschlüssel im Patch darf nicht geändert werden")

        table[pk_value] = after_row

    elif kind == "upsert":
        after_row = deepcopy_json(command["value"])
        pk_field = table_pk(schema, table_name)

        if pk_field not in after_row:
            after_row[pk_field] = pk_value

        if str(after_row[pk_field]) != pk_value:
            raise ValueError("pk in value passt nicht zu command.pk")

        table[pk_value] = after_row

    elif kind == "delete":
        if pk_value not in table:
            raise KeyError(f"{table_name!r}/{pk_value!r} existiert nicht für delete")

        del table[pk_value]
        after_row = None

    else:
        raise ValueError(f"Unbekannter rows-command kind: {kind!r}")

    state["versions"][table_name] = int(state["versions"][table_name]) + 1
    state["global_version"] = int(state["global_version"]) + 1

    return {
        "scope": "row",
        "table": table_name,
        "pk": pk_value,
        "before": before_row,
        "after": deepcopy_json(after_row),
        "table_version": int(state["versions"][table_name]),
        "global_version": int(state["global_version"]),
    }


def apply_document_table_command(
    state: dict[str, Any],
    schema: dict[str, dict[str, Any]],
    command: dict[str, Any],
) -> dict[str, Any]:
    table_name = str(command["table"])
    kind = str(command["kind"])
    before_doc = deepcopy_json(state["tables"].get(table_name))
    state["versions"].setdefault(table_name, 0)

    if kind == "doc_patch":
        if table_name not in state["tables"]:
            raise KeyError(f"Dokument-Tabelle {table_name!r} existiert nicht für doc_patch")

        after_doc = apply_json_patch(state["tables"][table_name], command["patch"])
        state["tables"][table_name] = after_doc

    elif kind == "doc_replace":
        after_doc = deepcopy_json(command["value"])
        state["tables"][table_name] = after_doc

    elif kind == "doc_delete":
        if table_name not in state["tables"]:
            raise KeyError(f"Dokument-Tabelle {table_name!r} existiert nicht für doc_delete")

        del state["tables"][table_name]
        after_doc = None

    else:
        raise ValueError(f"Unbekannter document-command kind: {kind!r}")

    state["versions"][table_name] = int(state["versions"][table_name]) + 1
    state["global_version"] = int(state["global_version"]) + 1

    return {
        "scope": "document",
        "table": table_name,
        "pk": None,
        "before": before_doc,
        "after": deepcopy_json(after_doc),
        "table_version": int(state["versions"][table_name]),
        "global_version": int(state["global_version"]),
    }


def apply_command_to_state(
    state: dict[str, Any],
    schema: dict[str, dict[str, Any]],
    command: dict[str, Any],
) -> dict[str, Any]:
    table_name = str(command["table"])
    ensure_table_mutable(schema, table_name)

    if table_shape(schema, table_name) == "rows":
        return apply_row_table_command(state, schema, command)

    return apply_document_table_command(state, schema, command)


def replay_shard_from_disk(
    node_dir: Path,
    shard_id: int,
    schema: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    initial = {
        "tables": {},
        "versions": {},
        "global_version": 0,
        "last_applied_index": 0,
    }

    if snapshot_path(node_dir, shard_id).exists():
        with open(snapshot_path(node_dir, shard_id), "r", encoding="utf-8") as handle:
            initial = import_shard_snapshot_file(json.load(handle), schema)

    records = read_json_lines(wal_path(node_dir, shard_id))
    entries: dict[int, dict[str, Any]] = {}
    highest_commit = int(initial["last_applied_index"])
    max_entry_index = int(initial["last_applied_index"])

    for record in records:
        record_type = record["record_type"]

        if record_type == "entry":
            index = int(record["index"])
            entries[index] = record
            max_entry_index = max(max_entry_index, index)
            continue

        if record_type == "commit":
            highest_commit = max(highest_commit, int(record["commit_index"]))
            continue

        raise ValueError(f"Unbekannter WAL record_type: {record_type!r}")

    state = {
        "tables": deepcopy_json(initial["tables"]),
        "versions": dict(initial["versions"]),
        "global_version": int(initial["global_version"]),
    }

    last_applied = int(initial["last_applied_index"])

    for index in range(last_applied + 1, highest_commit + 1):
        if index not in entries:
            raise ValueError(
                f"WAL Lücke in shard {shard_id}: commit_index={highest_commit}, fehlender entry={index}"
            )

        apply_command_to_state(state, schema, entries[index]["command"])
        last_applied = index

    had_uncommitted_tail = max_entry_index > highest_commit

    return {
        "state": state,
        "last_applied_index": int(last_applied),
        "commit_index": int(highest_commit),
        "last_log_index": int(highest_commit),
        "had_uncommitted_tail": bool(had_uncommitted_tail),
    }


# ============================================================
# Change Events
# ============================================================

def change_event_from_transition(
    node_id: str,
    shard_id: int,
    command_id: str,
    index: int,
    transition: dict[str, Any],
) -> dict[str, Any]:
    before_value = transition["before"]
    after_value = transition["after"]

    if before_value is None and after_value is not None:
        change_kind = "insert"
    elif before_value is not None and after_value is None:
        change_kind = "delete"
    else:
        change_kind = "patch"

    event = {
        "kind": "table_change",
        "node_id": node_id,
        "shard_id": int(shard_id),
        "command_id": command_id,
        "log_index": int(index),
        "scope": transition["scope"],
        "table": transition["table"],
        "pk": transition["pk"],
        "change_kind": change_kind,
        "patch": make_json_patch(before_value, after_value, ""),
        "before": deepcopy_json(before_value),
        "after": deepcopy_json(after_value),
        "table_version": int(transition["table_version"]),
        "global_version": int(transition["global_version"]),
        "ts_ns": time.time_ns(),
    }

    return event


def majority_count(cluster_size: int) -> int:
    return (int(cluster_size) // 2) + 1


# ============================================================
# Shards / Nodes / Cluster
# ============================================================

def make_shard(node: dict[str, Any], shard_id: int, restored: dict[str, Any]) -> dict[str, Any]:
    work_ring = make_ring_queue(int(node["shard_queue_capacity"]))
    publication_lock = threading.Lock()

    shard = {
        "node": node,
        "shard_id": int(shard_id),
        "queue": work_ring,
        "publication_lock": publication_lock,
        "state": restored["state"],
        "last_applied_index": int(restored["last_applied_index"]),
        "commit_index": int(restored["commit_index"]),
        "last_log_index": int(restored["last_log_index"]),
        "published": make_published_snapshot(restored["state"], int(restored["last_applied_index"])),
        "pending_entries": {},
        "commit_count_since_snapshot": 0,
    }

    def publish_snapshot(changed_table_name: str | None = None) -> None:
        with publication_lock:
            if changed_table_name is None:
                shard["published"] = make_published_snapshot(
                    shard["state"],
                    int(shard["last_applied_index"]),
                )
                return

            shard["published"] = make_incremental_published_snapshot(
                previous_published=shard["published"],
                state=shard["state"],
                last_applied_index=int(shard["last_applied_index"]),
                changed_table_name=str(changed_table_name),
            )

    def schedule_snapshot_compaction_if_needed() -> None:
        if int(node["snapshot_every"]) <= 0:
            return

        if int(shard["commit_count_since_snapshot"]) < int(node["snapshot_every"]):
            return

        payload = export_shard_snapshot_file(
            snapshot=read_shard_snapshot(shard),
            schema=node["schema"],
            node_id=node["node_id"],
            shard_id=shard_id,
            last_applied_index=int(shard["last_applied_index"]),
        )

        persist_snapshot_compact_async(
            persist_ring=node["persist_ring"],
            shard_id=shard_id,
            payload=payload,
            keep_from_index=int(shard["last_applied_index"]) + 1,
        )
        shard["commit_count_since_snapshot"] = 0

    def apply_committed_entry(index: int, command: dict[str, Any], source_node_id: str) -> dict[str, Any]:
        expected = int(shard["last_applied_index"]) + 1

        if int(index) != expected:
            raise ValueError(
                f"Shard {shard_id}: commit out of order, erwartet {expected}, bekam {index}"
            )

        transition = apply_command_to_state(shard["state"], node["schema"], command)
        shard["last_applied_index"] = int(index)
        shard["commit_index"] = max(int(shard["commit_index"]), int(index))
        shard["commit_count_since_snapshot"] = int(shard["commit_count_since_snapshot"]) + 1
        publish_snapshot(str(transition["table"]))

        event = change_event_from_transition(
            node_id=node["node_id"],
            shard_id=shard_id,
            command_id=command["command_id"],
            index=index,
            transition=transition,
        )
        event["source_node_id"] = source_node_id
        event["role"] = node["role"]
        publish(node["bus"], event)
        schedule_snapshot_compaction_if_needed()

        return {
            "scope": transition["scope"],
            "table": transition["table"],
            "pk": transition["pk"],
            "patch": deepcopy_json(event["patch"]),
            "after": deepcopy_json(transition["after"]),
            "log_index": int(index),
            "table_version": int(transition["table_version"]),
            "global_version": int(transition["global_version"]),
        }

    def reply_if_needed(reply_ring: dict[str, Any] | None, payload: dict[str, Any]) -> None:
        if reply_ring is not None:
            ring_queue_put(reply_ring, payload)

    def handle_client_command(message: dict[str, Any]) -> None:
        reply_ring = message["reply"]

        if node["role"] != "leader":
            reply_if_needed(reply_ring, {"ok": False, "error": "client commands nur auf leader erlaubt"})
            return

        command = deepcopy_json(message["command"])
        ack_mode = str(message["ack_mode"])

        try:
            preview_command(shard["state"], node["schema"], command)
            index = int(shard["last_log_index"]) + 1
            entry_record = make_entry_record(index=index, command=command, source_node_id=node["node_id"])
            commit_record = make_commit_record(commit_index=index, source_node_id=node["node_id"])

            if ack_mode == "heap_ack":
                shard["last_log_index"] = index
                result = apply_committed_entry(index=index, command=command, source_node_id=node["node_id"])
                persist_wal_async(node["persist_ring"], shard_id, entry_record, fsync=False)
                persist_wal_async(node["persist_ring"], shard_id, commit_record, fsync=False)

                if bool(node["replication"]["enabled"]):
                    enqueue_async_replication(node["cluster"], shard_id, entry_record)

                reply_if_needed(reply_ring, {"ok": True, "result": result})
                return

            if ack_mode == "local_durable_ack":
                persist_wal_sync(node["persist_ring"], shard_id, entry_record, fsync=True)
                persist_wal_sync(node["persist_ring"], shard_id, commit_record, fsync=True)
                shard["last_log_index"] = index
                result = apply_committed_entry(index=index, command=command, source_node_id=node["node_id"])

                if bool(node["replication"]["enabled"]):
                    enqueue_async_replication(node["cluster"], shard_id, entry_record)

                reply_if_needed(reply_ring, {"ok": True, "result": result})
                return

            if ack_mode == "quorum_ack":
                if not bool(node["replication"]["enabled"]):
                    persist_wal_sync(node["persist_ring"], shard_id, entry_record, fsync=True)
                    persist_wal_sync(node["persist_ring"], shard_id, commit_record, fsync=True)
                    shard["last_log_index"] = index
                    result = apply_committed_entry(index=index, command=command, source_node_id=node["node_id"])
                    reply_if_needed(reply_ring, {"ok": True, "result": result})
                    return

                persist_wal_sync(node["persist_ring"], shard_id, entry_record, fsync=True)
                shard["last_log_index"] = index

                follower_acks = replicate_prepare_and_wait_majority(
                    cluster=node["cluster"],
                    leader_node_id=node["node_id"],
                    shard_id=shard_id,
                    entry_record=entry_record,
                    timeout_s=float(node["replication"]["prepare_timeout_s"]),
                )
                needed = majority_count(len(node["cluster"]["node_ids"]))

                if 1 + follower_acks < needed:
                    raise TimeoutError(
                        f"Quorum nicht erreicht: local=1, follower_acks={follower_acks}, benötigt={needed}"
                    )

                persist_wal_sync(node["persist_ring"], shard_id, commit_record, fsync=True)
                result = apply_committed_entry(index=index, command=command, source_node_id=node["node_id"])
                broadcast_commit(node["cluster"], node["node_id"], shard_id, index)
                reply_if_needed(reply_ring, {"ok": True, "result": result})
                return

            raise ValueError(f"Unbekannter ack_mode: {ack_mode!r}")
        except Exception as exc:
            reply_if_needed(reply_ring, {"ok": False, "error": repr(exc)})

    def handle_replica_prepare(message: dict[str, Any]) -> None:
        reply_ring = message.get("reply")
        entry_record = deepcopy_json(message["entry_record"])
        index = int(entry_record["index"])

        try:
            if index <= int(shard["last_log_index"]):
                reply_if_needed(reply_ring, {"ok": True, "already_present": True})
                return

            expected = int(shard["last_log_index"]) + 1

            if index != expected:
                raise ValueError(
                    f"Follower shard {shard_id}: out-of-order entry {index}, erwartet {expected}"
                )

            persist_wal_sync(node["persist_ring"], shard_id, entry_record, fsync=True)
            shard["pending_entries"][index] = entry_record
            shard["last_log_index"] = index
            reply_if_needed(reply_ring, {"ok": True, "already_present": False})
        except Exception as exc:
            reply_if_needed(reply_ring, {"ok": False, "error": repr(exc)})

    def handle_replica_commit(message: dict[str, Any]) -> None:
        reply_ring = message.get("reply")
        commit_index = int(message["commit_index"])
        source_node_id = str(message["source_node_id"])

        try:
            if commit_index <= int(shard["commit_index"]):
                reply_if_needed(reply_ring, {"ok": True, "already_committed": True})
                return

            persist_wal_sync(
                node["persist_ring"],
                shard_id,
                make_commit_record(commit_index=commit_index, source_node_id=source_node_id),
                fsync=True,
            )

            while int(shard["last_applied_index"]) < commit_index:
                next_index = int(shard["last_applied_index"]) + 1

                if next_index not in shard["pending_entries"]:
                    raise ValueError(f"Follower shard {shard_id}: fehlender pending entry {next_index}")

                entry_record = shard["pending_entries"].pop(next_index)
                apply_committed_entry(
                    index=next_index,
                    command=entry_record["command"],
                    source_node_id=entry_record["source_node_id"],
                )

            shard["commit_index"] = max(int(shard["commit_index"]), commit_index)
            reply_if_needed(reply_ring, {"ok": True, "already_committed": False})
        except Exception as exc:
            reply_if_needed(reply_ring, {"ok": False, "error": repr(exc)})

    def handle_async_replication(message: dict[str, Any]) -> None:
        entry_record = deepcopy_json(message["entry_record"])
        index = int(entry_record["index"])
        source_node_id = str(message["source_node_id"])

        try:
            if index <= int(shard["commit_index"]):
                return

            expected = int(shard["commit_index"]) + 1

            if index != expected:
                raise ValueError(
                    f"Follower shard {shard_id}: async replication out-of-order {index}, erwartet {expected}"
                )

            persist_wal_sync(node["persist_ring"], shard_id, entry_record, fsync=True)
            persist_wal_sync(
                node["persist_ring"],
                shard_id,
                make_commit_record(commit_index=index, source_node_id=source_node_id),
                fsync=True,
            )
            shard["last_log_index"] = index
            apply_committed_entry(index=index, command=entry_record["command"], source_node_id=source_node_id)
        except Exception as exc:
            publish(
                node["bus"],
                {
                    "kind": "replication_error",
                    "node_id": node["node_id"],
                    "role": node["role"],
                    "shard_id": shard_id,
                    "error": repr(exc),
                    "entry_record": deepcopy_json(entry_record),
                },
            )

    def handle_seed_from_snapshot(message: dict[str, Any]) -> None:
        reply_ring = message["reply"]
        snapshot_payload = deepcopy_json(message["snapshot_payload"])

        try:
            imported = import_shard_snapshot_file(snapshot_payload, node["schema"])
            shard["state"]["tables"] = imported["tables"]
            shard["state"]["versions"] = imported["versions"]
            shard["state"]["global_version"] = int(imported["global_version"])
            shard["last_applied_index"] = int(imported["last_applied_index"])
            shard["commit_index"] = int(imported["last_applied_index"])
            shard["last_log_index"] = int(imported["last_applied_index"])
            shard["pending_entries"].clear()
            shard["commit_count_since_snapshot"] = 0
            publish_snapshot()

            persist_snapshot_reset_wal_sync(
                persist_ring=node["persist_ring"],
                shard_id=shard_id,
                payload=snapshot_payload,
            )

            reply_if_needed(reply_ring, {"ok": True})
        except Exception as exc:
            reply_if_needed(reply_ring, {"ok": False, "error": repr(exc)})

    def worker() -> None:
        while True:
            item = ring_queue_get(work_ring)

            if item is _RING_CLOSED or item is _STOP:
                return

            kind = item["kind"]

            if kind == "client_command":
                handle_client_command(item)
                continue

            if kind == "replica_prepare":
                handle_replica_prepare(item)
                continue

            if kind == "replica_commit":
                handle_replica_commit(item)
                continue

            if kind == "replicate_committed_async":
                handle_async_replication(item)
                continue

            if kind == "seed_from_snapshot":
                handle_seed_from_snapshot(item)
                continue

            raise ValueError(f"Unbekannter shard queue item kind: {kind!r}")

    thread = threading.Thread(target=worker, daemon=True, name=f"{node['node_id']}-shard-{shard_id}")
    thread.start()
    shard["thread"] = thread

    return shard


def read_shard_snapshot_meta(shard: dict[str, Any]) -> dict[str, Any]:
    with shard["publication_lock"]:
        return {
            "versions": dict(shard["published"]["versions"]),
            "global_version": int(shard["published"]["global_version"]),
            "last_applied_index": int(shard["published"]["last_applied_index"]),
        }


def read_shard_snapshot_table(shard: dict[str, Any], table_name: str) -> dict[str, Any]:
    with shard["publication_lock"]:
        present = table_name in shard["published"]["tables"]

        return {
            "present": bool(present),
            "table": deepcopy_json(shard["published"]["tables"][table_name]) if present else None,
            "version": int(shard["published"]["versions"].get(table_name, 0)),
            "global_version": int(shard["published"]["global_version"]),
            "last_applied_index": int(shard["published"]["last_applied_index"]),
        }


def read_shard_snapshot(shard: dict[str, Any]) -> dict[str, Any]:
    with shard["publication_lock"]:
        return {
            "tables": deepcopy_json(shard["published"]["tables"]),
            "versions": dict(shard["published"]["versions"]),
            "global_version": int(shard["published"]["global_version"]),
            "last_applied_index": int(shard["published"]["last_applied_index"]),
        }


def make_node(
    root_dir: str,
    node_id: str,
    role: str,
    schema: dict[str, dict[str, Any]],
    shard_count: int,
    base_dataset: dict[str, Any] | None,
    snapshot_every: int,
    shard_queue_capacity: int = 8192,
    persist_queue_capacity: int = 32768,
    subscriber_capacity: int = 2048,
) -> dict[str, Any]:
    node_dir = Path(root_dir) / node_id
    node_dir.mkdir(parents=True, exist_ok=True)

    persist_ring, persist_thread = make_persistence_worker(node_dir, capacity=persist_queue_capacity)
    bus = make_bus(subscriber_capacity=subscriber_capacity)

    node = {
        "node_id": node_id,
        "role": role,
        "node_dir": node_dir,
        "schema": schema,
        "shard_count": int(shard_count),
        "persist_ring": persist_ring,
        "persist_thread": persist_thread,
        "bus": bus,
        "snapshot_every": int(snapshot_every),
        "shard_queue_capacity": int(shard_queue_capacity),
        "replication": {
            "enabled": False,
            "prepare_timeout_s": 2.0,
        },
        "cluster": None,
        "shards": [],
    }

    loaded_shards: list[dict[str, Any] | None] = []
    has_any_persisted = False

    for shard_id in range(int(shard_count)):
        if snapshot_path(node_dir, shard_id).exists() or wal_path(node_dir, shard_id).exists():
            has_any_persisted = True
            loaded_shards.append(replay_shard_from_disk(node_dir, shard_id, schema))
        else:
            loaded_shards.append(None)

    normalized = normalize_dataset(base_dataset or {}, schema, shard_count) if not has_any_persisted else None

    for shard_id in range(int(shard_count)):
        if loaded_shards[shard_id] is None:
            initial_state = {
                "tables": {} if normalized is None else deepcopy_json(normalized[shard_id]["tables"]),
                "versions": {} if normalized is None else dict(normalized[shard_id]["versions"]),
                "global_version": 0 if normalized is None else int(normalized[shard_id]["global_version"]),
            }
            loaded_shards[shard_id] = {
                "state": initial_state,
                "last_applied_index": 0,
                "commit_index": 0,
                "last_log_index": 0,
                "had_uncommitted_tail": False,
            }

    for shard_id in range(int(shard_count)):
        node["shards"].append(make_shard(node, shard_id, loaded_shards[shard_id]))

    if not has_any_persisted and base_dataset is not None:
        for shard_id, shard in enumerate(node["shards"]):
            payload = export_shard_snapshot_file(
                snapshot=read_shard_snapshot(shard),
                schema=node["schema"],
                node_id=node["node_id"],
                shard_id=shard_id,
                last_applied_index=int(shard["last_applied_index"]),
            )
            persist_snapshot_reset_wal_sync(node["persist_ring"], shard_id, payload)

    for shard_id, restored in enumerate(loaded_shards):
        if not restored["had_uncommitted_tail"]:
            continue

        shard = node["shards"][shard_id]
        payload = export_shard_snapshot_file(
            snapshot=read_shard_snapshot(shard),
            schema=node["schema"],
            node_id=node["node_id"],
            shard_id=shard_id,
            last_applied_index=int(shard["last_applied_index"]),
        )
        persist_snapshot_reset_wal_sync(node["persist_ring"], shard_id, payload)

    return node


def stop_node(node: dict[str, Any], timeout: float = 1.0) -> None:
    for shard in node["shards"]:
        ring_queue_put(shard["queue"], _STOP)
        ring_queue_close(shard["queue"])

    for shard in node["shards"]:
        shard["thread"].join(timeout=timeout)

    stop_bus(node["bus"], timeout=timeout)

    ring_queue_put(node["persist_ring"], _STOP)
    ring_queue_close(node["persist_ring"])
    node["persist_thread"].join(timeout=timeout)


def attach_cluster(
    nodes: list[dict[str, Any]],
    leader_id: str,
    enable_replication: bool = True,
    prepare_timeout_s: float = 2.0,
) -> dict[str, Any]:
    by_id = {node["node_id"]: node for node in nodes}

    cluster = {
        "node_ids": [node["node_id"] for node in nodes],
        "nodes": by_id,
        "leader_id": leader_id,
        "command_seq_lock": threading.Lock(),
        "command_seq": 0,
    }

    for node in nodes:
        node["cluster"] = cluster
        node["replication"]["enabled"] = bool(enable_replication)
        node["replication"]["prepare_timeout_s"] = float(prepare_timeout_s)
        node["role"] = "leader" if node["node_id"] == leader_id else "follower"

    return cluster


def make_cluster(
    root_dir: str,
    node_ids: list[str],
    leader_id: str,
    schema: dict[str, dict[str, Any]],
    shard_count: int,
    base_dataset: dict[str, Any] | None,
    snapshot_every: int = 10,
    enable_replication: bool = True,
    prepare_timeout_s: float = 2.0,
    shard_queue_capacity: int = 8192,
    persist_queue_capacity: int = 32768,
    subscriber_capacity: int = 2048,
) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []

    for node_id in node_ids:
        dataset = base_dataset if node_id == leader_id else None
        nodes.append(
            make_node(
                root_dir=root_dir,
                node_id=node_id,
                role="leader" if node_id == leader_id else "follower",
                schema=schema,
                shard_count=shard_count,
                base_dataset=dataset,
                snapshot_every=snapshot_every,
                shard_queue_capacity=shard_queue_capacity,
                persist_queue_capacity=persist_queue_capacity,
                subscriber_capacity=subscriber_capacity,
            )
        )

    cluster = attach_cluster(
        nodes=nodes,
        leader_id=leader_id,
        enable_replication=enable_replication,
        prepare_timeout_s=prepare_timeout_s,
    )

    if enable_replication:
        seed_followers_from_leader(cluster)

    return cluster


def stop_cluster(cluster: dict[str, Any], timeout: float = 1.0) -> None:
    for node_id in cluster["node_ids"]:
        stop_node(cluster["nodes"][node_id], timeout=timeout)


def next_command_id(cluster: dict[str, Any], leader_id: str) -> str:
    with cluster["command_seq_lock"]:
        cluster["command_seq"] = int(cluster["command_seq"]) + 1
        return f"{leader_id}-cmd-{cluster['command_seq']}"


def leader_node(cluster: dict[str, Any]) -> dict[str, Any]:
    return cluster["nodes"][cluster["leader_id"]]


def seed_follower_shard_from_leader(
    cluster: dict[str, Any],
    follower_id: str,
    shard_id: int,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    leader = leader_node(cluster)
    leader_shard = leader["shards"][shard_id]
    snapshot_payload = export_shard_snapshot_file(
        snapshot=read_shard_snapshot(leader_shard),
        schema=leader["schema"],
        node_id=leader["node_id"],
        shard_id=shard_id,
        last_applied_index=int(leader_shard["last_applied_index"]),
    )
    reply_ring = make_reply_ring()

    try:
        ok = ring_queue_put(
            cluster["nodes"][follower_id]["shards"][shard_id]["queue"],
            {
                "kind": "seed_from_snapshot",
                "snapshot_payload": deepcopy_json(snapshot_payload),
                "reply": reply_ring,
            },
            timeout=float(timeout_s),
        )

        if not ok:
            raise RuntimeError(f"Follower {follower_id} shard {shard_id} Queue blockiert oder geschlossen")

        return wait_reply(reply_ring, timeout=float(timeout_s))
    finally:
        ring_queue_close(reply_ring)


def seed_followers_from_leader(cluster: dict[str, Any]) -> None:
    leader = leader_node(cluster)

    for shard_id, _leader_shard in enumerate(leader["shards"]):
        for node_id in cluster["node_ids"]:
            if node_id == cluster["leader_id"]:
                continue

            result = seed_follower_shard_from_leader(cluster, node_id, shard_id, timeout_s=10.0)

            if not result["ok"]:
                raise RuntimeError(result["error"])


def publish_replication_enqueue_failed(
    cluster: dict[str, Any],
    leader_id: str,
    follower_id: str,
    shard_id: int,
    message_kind: str,
    reason: str,
    entry_index: int | None = None,
    commit_index: int | None = None,
) -> None:
    publish(
        cluster["nodes"][leader_id]["bus"],
        {
            "kind": "replication_enqueue_failed",
            "leader_id": leader_id,
            "follower_id": follower_id,
            "shard_id": int(shard_id),
            "message_kind": str(message_kind),
            "reason": str(reason),
            "entry_index": None if entry_index is None else int(entry_index),
            "commit_index": None if commit_index is None else int(commit_index),
            "ts_ns": time.time_ns(),
        },
    )


def enqueue_replication_message(
    cluster: dict[str, Any],
    leader_id: str,
    follower_id: str,
    shard_id: int,
    message: dict[str, Any],
    message_kind: str,
    timeout_s: float = _REPLICATION_ENQUEUE_TIMEOUT_S,
    entry_index: int | None = None,
    commit_index: int | None = None,
) -> bool:
    ok = ring_queue_put(
        cluster["nodes"][follower_id]["shards"][shard_id]["queue"],
        message,
        timeout=float(timeout_s),
    )

    if ok:
        return True

    publish_replication_enqueue_failed(
        cluster=cluster,
        leader_id=leader_id,
        follower_id=follower_id,
        shard_id=shard_id,
        message_kind=message_kind,
        reason="timeout_or_closed",
        entry_index=entry_index,
        commit_index=commit_index,
    )
    return False


def enqueue_async_replication(
    cluster: dict[str, Any],
    shard_id: int,
    entry_record: dict[str, Any],
    enqueue_timeout_s: float = _REPLICATION_ENQUEUE_TIMEOUT_S,
) -> None:
    leader_id = str(cluster["leader_id"])
    entry_index = int(entry_record["index"])

    for node_id in cluster["node_ids"]:
        if node_id == leader_id:
            continue

        enqueue_replication_message(
            cluster=cluster,
            leader_id=leader_id,
            follower_id=node_id,
            shard_id=shard_id,
            message={
                "kind": "replicate_committed_async",
                "entry_record": deepcopy_json(entry_record),
                "source_node_id": leader_id,
            },
            message_kind="replicate_committed_async",
            timeout_s=float(enqueue_timeout_s),
            entry_index=entry_index,
        )


def replicate_prepare_and_wait_majority(
    cluster: dict[str, Any],
    leader_node_id: str,
    shard_id: int,
    entry_record: dict[str, Any],
    timeout_s: float,
) -> int:
    pending_replies: list[tuple[str, dict[str, Any]]] = []
    entry_index = int(entry_record["index"])
    needed_follower_acks = max(0, majority_count(len(cluster["node_ids"])) - 1)

    for node_id in cluster["node_ids"]:
        if node_id == leader_node_id:
            continue

        reply_ring = make_reply_ring()
        ok = enqueue_replication_message(
            cluster=cluster,
            leader_id=leader_node_id,
            follower_id=node_id,
            shard_id=shard_id,
            message={
                "kind": "replica_prepare",
                "entry_record": deepcopy_json(entry_record),
                "reply": reply_ring,
            },
            message_kind="replica_prepare",
            timeout_s=min(float(timeout_s), float(_REPLICATION_ENQUEUE_TIMEOUT_S)),
            entry_index=entry_index,
        )

        if not ok:
            ring_queue_close(reply_ring)
            continue

        pending_replies.append((node_id, reply_ring))

    deadline = time.monotonic() + float(timeout_s)
    ack_count = 0

    for node_id, reply_ring in pending_replies:
        if ack_count >= needed_follower_acks:
            ring_queue_close(reply_ring)
            continue

        remaining = deadline - time.monotonic()

        if remaining <= 0:
            ring_queue_close(reply_ring)
            continue

        try:
            result = wait_reply(reply_ring, timeout=remaining)
        except TimeoutError:
            publish(
                cluster["nodes"][leader_node_id]["bus"],
                {
                    "kind": "replication_prepare_reply_timeout",
                    "leader_id": leader_node_id,
                    "follower_id": node_id,
                    "shard_id": int(shard_id),
                    "entry_index": int(entry_index),
                    "ts_ns": time.time_ns(),
                },
            )
            ring_queue_close(reply_ring)
            continue

        ring_queue_close(reply_ring)

        if result.get("ok"):
            ack_count += 1
            continue

        publish(
            cluster["nodes"][leader_node_id]["bus"],
            {
                "kind": "replication_prepare_failed",
                "leader_id": leader_node_id,
                "follower_id": node_id,
                "shard_id": int(shard_id),
                "entry_index": int(entry_index),
                "error": result.get("error"),
                "ts_ns": time.time_ns(),
            },
        )

    return ack_count


def broadcast_commit(
    cluster: dict[str, Any],
    leader_node_id: str,
    shard_id: int,
    commit_index: int,
    enqueue_timeout_s: float = _REPLICATION_ENQUEUE_TIMEOUT_S,
) -> None:
    for node_id in cluster["node_ids"]:
        if node_id == leader_node_id:
            continue

        enqueue_replication_message(
            cluster=cluster,
            leader_id=leader_node_id,
            follower_id=node_id,
            shard_id=shard_id,
            message={
                "kind": "replica_commit",
                "commit_index": int(commit_index),
                "reply": None,
                "source_node_id": leader_node_id,
            },
            message_kind="replica_commit",
            timeout_s=float(enqueue_timeout_s),
            commit_index=int(commit_index),
        )


def replication_lag(cluster: dict[str, Any], shard_id: int) -> dict[str, Any]:
    leader = leader_node(cluster)
    leader_meta = read_shard_snapshot_meta(leader["shards"][shard_id])
    leader_index = int(leader_meta["last_applied_index"])
    lag_info: dict[str, Any] = {}

    for node_id in cluster["node_ids"]:
        if node_id == cluster["leader_id"]:
            continue

        follower_meta = read_shard_snapshot_meta(cluster["nodes"][node_id]["shards"][shard_id])
        follower_index = int(follower_meta["last_applied_index"])
        lag_info[node_id] = {
            "leader_index": leader_index,
            "follower_index": follower_index,
            "lag": leader_index - follower_index,
        }

    return lag_info


def reseed_lagging_followers(
    cluster: dict[str, Any],
    max_lag: int = 500,
    timeout_s: float = 10.0,
) -> list[str]:
    reseeded: list[str] = []
    leader = leader_node(cluster)

    for shard_id in range(int(leader["shard_count"])):
        lag_info = replication_lag(cluster, shard_id)

        for node_id, info in lag_info.items():
            if int(info["lag"]) <= int(max_lag):
                continue

            result = seed_follower_shard_from_leader(
                cluster=cluster,
                follower_id=node_id,
                shard_id=shard_id,
                timeout_s=float(timeout_s),
            )

            if result.get("ok"):
                reseeded.append(node_id)
                continue

            publish(
                leader["bus"],
                {
                    "kind": "replication_reseed_failed",
                    "leader_id": leader["node_id"],
                    "follower_id": node_id,
                    "shard_id": int(shard_id),
                    "error": result.get("error"),
                    "ts_ns": time.time_ns(),
                },
            )

    return reseeded


def cluster_health(cluster: dict[str, Any], lag_threshold: int = 100) -> dict[str, Any]:
    leader = leader_node(cluster)
    leader_indices = {}

    for shard_id in range(int(leader["shard_count"])):
        meta = read_shard_snapshot_meta(leader["shards"][shard_id])
        leader_indices[shard_id] = int(meta["last_applied_index"])

    health: dict[str, Any] = {
        "leader_id": cluster["leader_id"],
        "node_count": len(cluster["node_ids"]),
        "shard_count": int(leader["shard_count"]),
        "nodes": {},
        "replication_lag": {},
        "bus_dropped": {},
        "queue_fill": {},
    }

    for node_id in cluster["node_ids"]:
        node = cluster["nodes"][node_id]
        node_info: dict[str, Any] = {
            "role": node["role"],
            "shards": [],
        }
        lag_per_shard: dict[int, int] = {}
        queue_per_shard: dict[int, float] = {}

        for shard_id, shard in enumerate(node["shards"]):
            shard_meta = read_shard_snapshot_meta(shard)
            node_index = int(shard_meta["last_applied_index"])
            lag = int(leader_indices[shard_id]) - int(node_index)
            lag_per_shard[shard_id] = lag

            with shard["queue"]["lock"]:
                queue_size = int(shard["queue"]["size"])
                queue_capacity = int(shard["queue"]["capacity"])

            queue_fill_ratio = 0.0 if queue_capacity <= 0 else round(queue_size / queue_capacity, 3)
            queue_per_shard[shard_id] = queue_fill_ratio

            node_info["shards"].append(
                {
                    "shard_id": shard_id,
                    "last_applied_index": node_index,
                    "commit_index": int(shard["commit_index"]),
                    "lag": lag,
                    "queue_fill_ratio": queue_fill_ratio,
                }
            )

        with node["bus"]["lock"]:
            subscribers = list(node["bus"]["subscribers"])

        total_dropped = 0

        for subscriber in subscribers:
            with subscriber["stats_lock"]:
                total_dropped += int(subscriber["dropped"])

        health["nodes"][node_id] = node_info
        health["replication_lag"][node_id] = lag_per_shard
        health["queue_fill"][node_id] = queue_per_shard
        health["bus_dropped"][node_id] = total_dropped

    all_lags = []

    for node_id, lags in health["replication_lag"].items():
        if node_id == cluster["leader_id"]:
            continue
        all_lags.extend(int(lag) for lag in lags.values())

    health["max_replication_lag"] = max(all_lags) if all_lags else 0
    health["cluster_ok"] = int(health["max_replication_lag"]) <= int(lag_threshold)
    return health


def submit_command(
    cluster: dict[str, Any],
    command: dict[str, Any],
    ack_mode: str = "local_durable_ack",
) -> dict[str, Any]:
    leader = leader_node(cluster)
    shard_index = route_command_to_shard(leader["schema"], command, leader["shard_count"])
    shard = leader["shards"][shard_index]
    reply_ring = make_reply_ring()
    command_envelope = deepcopy_json(command)

    if "command_id" not in command_envelope:
        command_envelope["command_id"] = next_command_id(cluster, leader["node_id"])

    ok = ring_queue_put(
        shard["queue"],
        {
            "kind": "client_command",
            "command": command_envelope,
            "ack_mode": ack_mode,
            "reply": reply_ring,
        },
    )

    if not ok:
        ring_queue_close(reply_ring)
        raise RuntimeError("Leader-Queue geschlossen")

    result = wait_reply(reply_ring)
    ring_queue_close(reply_ring)

    if not result["ok"]:
        raise RuntimeError(result["error"])

    return result["result"]


def read_table_snapshot(node: dict[str, Any], table_name: str) -> dict[str, Any]:
    shape = table_shape(node["schema"], table_name)
    version = 0
    last_applied_index = 0

    if shape == "rows":
        merged_rows: dict[str, Any] = {}

        for shard in node["shards"]:
            snapshot = read_shard_snapshot_table(shard, table_name)

            if snapshot["present"]:
                merged_rows.update(snapshot["table"])

            version = max(version, int(snapshot["version"]))
            last_applied_index = max(last_applied_index, int(snapshot["last_applied_index"]))

        return {
            "table": table_name,
            "shape": "rows",
            "version": int(version),
            "last_applied_index": int(last_applied_index),
            "rows": merged_rows,
        }

    seen = False
    doc_value = None

    for shard in node["shards"]:
        snapshot = read_shard_snapshot_table(shard, table_name)
        version = max(version, int(snapshot["version"]))
        last_applied_index = max(last_applied_index, int(snapshot["last_applied_index"]))

        if not snapshot["present"]:
            continue

        current = snapshot["table"]

        if not seen:
            doc_value = current
            seen = True
            continue

        if current != doc_value:
            raise ValueError(f"Dokument-Tabelle {table_name!r} liegt inkonsistent auf mehreren Shards")

    return {
        "table": table_name,
        "shape": "document",
        "version": int(version),
        "last_applied_index": int(last_applied_index),
        "document": doc_value,
    }


# ============================================================
# Demo
# ============================================================

def demo() -> None:
    import shutil

    root = Path("./cluster_demo_final_v2")

    if root.exists():
        shutil.rmtree(root)

    dataset = {
        "orders": [
            {"id": "o1", "status": "new", "qty": 2, "attrs": {"prio": 1}},
            {"id": "o2", "status": "new", "qty": 1, "attrs": {"prio": 2}},
        ],
        "rule_controls": {
            "timers": [],
            "actions": [1, 2],
        },
        "lookup": [
            {"key": "country_de", "value": "Deutschland"},
        ],
    }

    schema = {
        "orders": make_schema_entry(pk="id", mode="mutable", shape="rows"),
        "lookup": make_schema_entry(pk="key", mode="readonly", shape="rows"),
        "rule_controls": make_document_schema_entry(mode="mutable"),
    }

    cluster = make_cluster(
        root_dir=str(root),
        node_ids=["n1", "n2", "n3"],
        leader_id="n1",
        schema=schema,
        shard_count=2,
        base_dataset=dataset,
        snapshot_every=2,
        enable_replication=True,
        prepare_timeout_s=2.0,
    )

    submit_command(
        cluster,
        {
            "kind": "patch",
            "table": "orders",
            "pk": "o1",
            "patch": [
                {"op": "replace", "path": "/status", "value": "processing"},
                {"op": "replace", "path": "/attrs/prio", "value": 5},
            ],
        },
        ack_mode="quorum_ack",
    )

    submit_command(
        cluster,
        {
            "kind": "doc_patch",
            "table": "rule_controls",
            "patch": [
                {"op": "add", "path": "/timers/-", "value": 100},
            ],
        },
        ack_mode="local_durable_ack",
    )

    time.sleep(0.3)

    before_restart = export_node_dataset(leader_node(cluster))
    print("LEADER BEFORE RESTART", json.dumps(before_restart, ensure_ascii=False, sort_keys=True))

    stop_cluster(cluster)

    cluster_rebuilt = make_cluster(
        root_dir=str(root),
        node_ids=["n1", "n2", "n3"],
        leader_id="n1",
        schema=schema,
        shard_count=2,
        base_dataset=None,
        snapshot_every=2,
        enable_replication=True,
        prepare_timeout_s=2.0,
    )

    after_restart = export_node_dataset(leader_node(cluster_rebuilt))
    print("LEADER AFTER RESTART", json.dumps(after_restart, ensure_ascii=False, sort_keys=True))

    assert before_restart == after_restart

    stop_cluster(cluster_rebuilt)


if __name__ == "__main__":
    demo()
