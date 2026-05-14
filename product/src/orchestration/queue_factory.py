# -*- coding: utf-8 -*-

# src/orchestration/queue_factory.py

from functools import partial
from queue import Queue


DEFAULT_QUEUE_GROUP_SPECS = {
    "system": (
        "queue_ws",
        "queue_bc",
        "queue_io",
        "queue_ipc",
        "queue_state",
        "queue_event",
        "queue_ds",
        "queue_dc",
        "queue_proc",
        "queue_pc",
        "queue_mbh",
        "queue_mba",
        "queue_mbc",
        "queue_redis_api",
        "queue_ti",
        "queue_worker",
    ),
    "event": (
        "queue_event_send",
        "queue_event_ws",
        "queue_event_state",
        "queue_event_ds",
        "queue_event_dc",
        "queue_event_proc",
        "queue_event_pc",
        "queue_event_mbh",
        "queue_event_mba",
        "queue_event_mbc",
        "queue_event_ti",
        "queue_event_worker",
    ),
}


def validate_queue_group_specs(queue_group_specs):
    """
    Validate queue group specs and raise deterministic errors early.
    """
    if not isinstance(queue_group_specs, dict):
        raise ValueError("queue_group_specs must be a dict")

    for group_name, group_entries in queue_group_specs.items():
        if not isinstance(group_name, str) or not group_name:
            raise ValueError("invalid queue group name")
        if not isinstance(group_entries, (list, tuple)):
            raise ValueError("queue group '%s' must be a list or tuple" % group_name)

        for queue_name in group_entries:
            if not isinstance(queue_name, str) or not queue_name.startswith("queue_"):
                raise ValueError("invalid queue name in group '%s': %r" % (group_name, queue_name))

    return True


def flatten_queue_group_specs(queue_group_specs):
    """
    Flatten grouped queue specs into one deterministic ordered tuple.
    """
    validate_queue_group_specs(queue_group_specs)

    names = []
    seen = set()

    for group_entries in queue_group_specs.values():
        for queue_name in group_entries:
            if queue_name in seen:
                continue
            seen.add(queue_name)
            names.append(queue_name)

    return tuple(names)


def merge_queue_group_specs(*group_specs):
    """
    Merge multiple queue group spec mappings while keeping order deterministic.
    """
    merged = {}

    for spec in group_specs:
        if spec is None:
            continue
        validate_queue_group_specs(spec)

        for group_name, group_entries in spec.items():
            current_entries = list(merged.get(group_name, ()))
            seen = set(current_entries)

            for queue_name in group_entries:
                if queue_name in seen:
                    continue
                seen.add(queue_name)
                current_entries.append(queue_name)

            merged[group_name] = tuple(current_entries)

    return merged


def create_named_queues(queue_group_specs=None, queue_factory=None, queue_register_fn=None):
    """
    Create all queues defined by queue_group_specs and optionally register them.
    """
    if queue_group_specs is None:
        queue_group_specs = DEFAULT_QUEUE_GROUP_SPECS

    if queue_factory is None:
        queue_factory = Queue

    queue_names = flatten_queue_group_specs(queue_group_specs)
    queues = {}

    for queue_name in queue_names:
        queue_obj = queue_factory()

        try:
            setattr(queue_obj, "metrics_name", queue_name)
        except Exception:
            pass

        if callable(queue_register_fn):
            try:
                queue_register_fn(queue_obj, name=queue_name)
            except Exception:
                pass

        queues[queue_name] = queue_obj

    return queues


def select_named_mapping_entries(mapping, *names):
    """
    Resolve named entries from a mapping and fail fast on missing names.
    """
    resolved = {}

    for name in names:
        if name not in mapping:
            raise KeyError("missing mapping key: %s" % name)
        resolved[name] = mapping[name]

    return resolved


def map_queue_aliases(queues_dict, alias_map):
    """
    Resolve alias parameter names to concrete queue objects.
    """
    if not isinstance(alias_map, dict):
        raise ValueError("alias_map must be a dict")

    resolved = {}

    for parameter_name, queue_name in alias_map.items():
        if queue_name not in queues_dict:
            raise KeyError("missing queue: %s" % queue_name)
        resolved[parameter_name] = queues_dict[queue_name]

    return resolved


def resolve_queue_objects(mapping, *names):
    """
    Resolve queue objects in the given order and skip missing entries.
    """
    queue_objects = []

    for name in names:
        queue_obj = mapping.get(name)
        if queue_obj is not None:
            queue_objects.append(queue_obj)

    return tuple(queue_objects)


def build_queue_tools(queue_group_specs=None, queue_factory=None, queue_register_fn=None):
    """
    Build the queue registry and helper accessors used by main and thread specs.
    """
    if queue_group_specs is None:
        queue_group_specs = DEFAULT_QUEUE_GROUP_SPECS

    queues_dict = create_named_queues(
        queue_group_specs=queue_group_specs,
        queue_factory=queue_factory,
        queue_register_fn=queue_register_fn,
    )

    return {
        "queue_group_specs": queue_group_specs,
        "queue_names": tuple(queues_dict.keys()),
        "queues_dict": queues_dict,
        "pick_q": partial(select_named_mapping_entries, queues_dict),
        "alias_q": partial(map_queue_aliases, queues_dict),
        "queue_list": partial(resolve_queue_objects, queues_dict),
    }
