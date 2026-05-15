# -*- coding: utf-8 -*-

# src/orchestration/runtime_state_factory.py

import copy
import threading

from functools import partial


DEFAULT_RUN_TIME_LABEL = "SYNC_CTX"


def create_resource(data=None, lock=None):
    """
    Create one resource cell with explicit data ownership and lock ownership.
    """
    if data is None:
        data = {}

    if lock is None:
        lock = threading.RLock()

    return {
        "data": data,
        "lock": lock,
    }


def add_resource(resources, name, data=None, lock=None, replace=False):
    """
    Add a named resource to the shared resource registry.
    """
    if not isinstance(resources, dict):
        raise ValueError("resources must be a dict")

    if not name:
        raise ValueError("name must not be empty")

    if name in resources and not replace:
        raise ValueError("resource already exists: %s" % name)

    resources[name] = create_resource(data=data, lock=lock)
    return resources[name]


def get_resource_entry(resources, name):
    """
    Resolve a resource entry by name.
    """
    if not isinstance(resources, dict) or name not in resources:
        raise KeyError("unknown resource: %s" % name)

    entry = resources[name]

    if not isinstance(entry, dict) or "data" not in entry or "lock" not in entry:
        raise ValueError("invalid resource entry: %s" % name)

    return entry


def safe_read(resources, resource_name, key=None, default=None, deep_copy=False):
    """
    Read a resource value safely.
    """
    entry = get_resource_entry(resources, resource_name)

    with entry["lock"]:
        if key is None:
            value = entry["data"]
        else:
            value = entry["data"].get(key, default)

        if deep_copy:
            return copy.deepcopy(value)

        return value


def safe_write(resources, resource_name, key, value):
    """
    Write one key/value into a resource mapping safely.
    """
    entry = get_resource_entry(resources, resource_name)

    with entry["lock"]:
        entry["data"][key] = value
        return entry["data"][key]


def safe_merge(resources, resource_name, mapping, replace=False):
    """
    Merge a mapping into a named resource safely.
    """
    if not isinstance(mapping, dict):
        raise ValueError("mapping must be a dict")

    entry = get_resource_entry(resources, resource_name)

    with entry["lock"]:
        if replace:
            entry["data"].clear()
        entry["data"].update(mapping)
        return copy.deepcopy(entry["data"])


def safe_replace(resources, resource_name, new_data):
    """
    Replace the full content of a resource mapping safely.
    """
    if not isinstance(new_data, dict):
        raise ValueError("new_data must be a dict")

    entry = get_resource_entry(resources, resource_name)

    with entry["lock"]:
        entry["data"].clear()
        entry["data"].update(new_data)
        return copy.deepcopy(entry["data"])


def snapshot_resources(resources, resource_names=None):
    """
    Create a deterministic deep snapshot of selected resources.
    """
    if resource_names is None:
        resource_names = tuple(resources.keys())

    snapshot = {}

    for name in resource_names:
        entry = get_resource_entry(resources, name)
        with entry["lock"]:
            snapshot[name] = copy.deepcopy(entry["data"])

    return snapshot


def _build_default_resource_registry(settings):
    node_id = settings.get("node_id", "fn-01")
    local_mode = bool(settings.get("network", {}).get("local_mode", False))
    master_uri = str(settings.get("network", {}).get("master_uri", ""))
    persist_data = bool(settings.get("worker_state", {}).get("persist_data", True))
    run_time_mode = str(settings.get("worker_state", {}).get("run_time_mode", DEFAULT_RUN_TIME_LABEL))

    if run_time_mode == "":
        run_time_mode = DEFAULT_RUN_TIME_LABEL

    resources = {}

    add_resource(
        resources,
        "worker_state",
        data={
            "NODE_ID": node_id,
            "RUN_TIME": run_time_mode,
            "LOCAL_MODE": local_mode,
            "MASTER": master_uri,
            "PERSIST_DATA": persist_data,
        },
    )
    add_resource(resources, "connected", data={})
    add_resource(resources, "sem_local_registry_sem", data={"thread_name": ""})
    add_resource(resources, "client_config", data={})
    add_resource(resources, "user_config", data={})
    add_resource(resources, "config_data", data={})
    add_resource(resources, "sensor_values", data={})
    add_resource(resources, "actuator_values", data={})
    add_resource(resources, "event_store", data={})
    add_resource(resources, "proxy_runtime_command_state", data={
        "binding_version": "v35_1_preproduction_final_runtime",
        "commands": {},
        "control_methods": {},
        "parameters": {},
        "safe_runtime_commands": [],
        "audit": [],
        "last_update": None,
    })
    add_resource(resources, "local_sensor_values", data={})
    add_resource(resources, "local_actuator_values", data={})
    add_resource(resources, "local_controller_tasks", data={})
    add_resource(resources, "cpu_probe_information", data={})

    return resources


def _bind_runtime_views(resources):
    """
    Create direct runtime views for commonly accessed shared structures.
    """
    bound = {}

    for name, entry in resources.items():
        bound[name] = entry["data"]
        bound["%s_lock" % name] = entry["lock"]

    return bound


def build_runtime_state(settings, config_store=None, extra_resources=None):
    """
    Build the runtime state bundle consumed by main and worker threads.

    The runtime state keeps ownership explicit:
    - global shared resources live in the resource registry
    - each thread receives its own thread_ctx later through the thread factory
    """
    resources = _build_default_resource_registry(settings)

    if isinstance(extra_resources, dict):
        for name, resource_data in extra_resources.items():
            if isinstance(resource_data, dict) and "data" in resource_data and "lock" in resource_data:
                resources[name] = {
                    "data": resource_data["data"],
                    "lock": resource_data["lock"],
                }
            else:
                add_resource(resources, name, data=resource_data, replace=True)

    shutdown_event = threading.Event()

    runtime_state = {
        "shutdown_event": shutdown_event,
        "resources": resources,
        "settings": settings,
        "config_store": config_store,
        "node_id": settings.get("node_id", "fn-01"),
        "project_root": config_store.get("project_root") if isinstance(config_store, dict) else None,
    }

    runtime_state.update(_bind_runtime_views(resources))

    runtime_state["safe_read"] = partial(safe_read, resources)
    runtime_state["safe_write"] = partial(safe_write, resources)
    runtime_state["safe_merge"] = partial(safe_merge, resources)
    runtime_state["safe_replace"] = partial(safe_replace, resources)
    runtime_state["snapshot_resources"] = partial(snapshot_resources, resources)
    runtime_state["add_resource"] = partial(add_resource, resources)

    return runtime_state
