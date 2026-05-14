# -*- coding: utf-8 -*-

# src/libraries/_generic_finite_state_machine.py

"""
Generic, functional, deterministic finite-state machine library.

This module implements a generic FSM with dynamic registries for states,
transitions, actions, timers, and events. The design is fully functional,
fully synchronous, and uses only Python standard library modules.

Key design goals:
- No OOP
- Deterministic transition resolution
- Dynamic dependency injection via CTX build function
- Registry-based extensibility
- Minimal invasive logging support
- Robust validation, tracing, and selftesting
- Testable, readable, and maintainable code
"""

from copy import deepcopy
from functools import partial
from pprint import pprint, pformat

import json
import inspect
import logging


# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------

_log = logging.getLogger("generic_rule_engine")


# ---------------------------------------------------------------------------
#  Generic helpers
# ---------------------------------------------------------------------------

def _deep_copy(value):
    if callable(value):
        return value

    if isinstance(value, dict):
        copied = {}
        for key, item in value.items():
            copied[_deep_copy(key)] = _deep_copy(item)
        return copied

    if isinstance(value, list):
        return [_deep_copy(item) for item in value]

    if isinstance(value, tuple):
        return tuple(_deep_copy(item) for item in value)

    if isinstance(value, set):
        return {_deep_copy(item) for item in value}

    return deepcopy(value)

def _is_mapping(value):
    return isinstance(value, dict)

def _is_sequence(value):
    return isinstance(value, (list, tuple))

def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    return [value]

def _copy_mapping(value):
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Expected a dict-compatible value.")
    return _deep_copy(value)

def _merge_mapping(base, patch):
    if patch is None:
        return _deep_copy(base)
    if base is None:
        base = {}
    if not isinstance(base, dict):
        raise ValueError("Base value must be a dict for merge operations.")
    if not isinstance(patch, dict):
        raise ValueError("Patch value must be a dict for merge operations.")
    merged = _deep_copy(base)
    for key, value in patch.items():
        merged[key] = _deep_copy(value)
    return merged

def _join_error_messages(errors):
    if not errors:
        return ""
    return "; ".join(str(item) for item in errors)

def _normalize_name(name, label):
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"{label} must be a non-empty string.")
    return name.strip()

def _event_sort_key(event):
    meta = event.get("meta", {})
    return (
        event.get("clock", 0),
        meta.get("sequence", 0),
        event.get("name", ""),
    )

def _history_sort_key(entry):
    return (
        entry.get("clock", 0),
        entry.get("sequence", 0),
        entry.get("kind", ""),
    )

def _timer_sort_key(timer_instance):
    return (
        timer_instance.get("due_at", 0),
        timer_instance.get("sequence", 0),
        timer_instance.get("id", 0),
        timer_instance.get("name", ""),
    )

def _has_var_keyword_parameter(signature):
    for parameter in signature.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
    return False

def _invoke_callable(func, scope):
    # The injector keeps call sites flexible and deterministic.
    # A callable may request only the names it needs, or receive the
    # entire scope when it exposes exactly one positional argument.
    if func is None:
        return None

    if not callable(func):
        raise ValueError("Referenced value is not callable.")

    signature = inspect.signature(func)
    parameters = list(signature.parameters.values())

    if not parameters:
        return func()

    has_var_keywords = _has_var_keyword_parameter(signature)
    kwargs = {}
    args = []

    for parameter in parameters:
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            continue

        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            continue

        if parameter.kind == inspect.Parameter.POSITIONAL_ONLY:
            if parameter.name in scope:
                args.append(scope[parameter.name])
                continue
            if len(parameters) == 1:
                args.append(scope)
                continue
            if parameter.default is inspect._empty:
                raise ValueError(
                    f"Cannot inject required positional-only parameter '{parameter.name}'."
                )
            continue

        if parameter.name in scope:
            kwargs[parameter.name] = scope[parameter.name]
            continue

        if len(parameters) == 1:
            args.append(scope)
            continue

        if parameter.default is inspect._empty:
            raise ValueError(
                f"Cannot inject required parameter '{parameter.name}' into callable '{getattr(func, '__name__', repr(func))}'."
            )

    if has_var_keywords:
        for key, value in scope.items():
            if key not in kwargs:
                kwargs[key] = value

    return func(*args, **kwargs)

# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------

def _normalize_logger_name(name):
    if not isinstance(name, str) or not name.strip():
        return "generic_fsm"
    return name.strip()

def _new_logging_config(enabled=False, log_func=None, logger_name="generic_fsm"):
    return {
        "enabled": bool(enabled),
        "log_func": log_func,
        "logger_name": _normalize_logger_name(logger_name),
    }

def _merge_logging_config(base, patch):
    merged = _new_logging_config()

    if isinstance(base, dict):
        if "enabled" in base:
            merged["enabled"] = bool(base.get("enabled"))
        if "log_func" in base:
            merged["log_func"] = base.get("log_func")
        if "logger_name" in base:
            merged["logger_name"] = _normalize_logger_name(base.get("logger_name"))

    if isinstance(patch, dict):
        if "enabled" in patch:
            merged["enabled"] = bool(patch.get("enabled"))
        if "log_func" in patch:
            merged["log_func"] = patch.get("log_func")
        if "logger_name" in patch:
            merged["logger_name"] = _normalize_logger_name(patch.get("logger_name"))

    return merged

def _extract_spec(target):
    if not isinstance(target, dict):
        raise ValueError("Target must be a spec or machine dict.")
    if "runtime" in target and "spec" in target:
        return target["spec"]
    return target

def _resolve_target_argument(target=None, spec=None, machine=None):
    matches = [item for item in [target, spec, machine] if item is not None]
    if len(matches) != 1:
        raise ValueError("Provide exactly one of target, spec, or machine.")
    return matches[0]

def _resolve_logging_config(target=None, log_func=None, logger_name=None):
    logging_config = _new_logging_config()

    if isinstance(target, dict):
        try:
            spec = _extract_spec(target)
        except ValueError:
            spec = None

        if isinstance(spec, dict):
            options = spec.get("options", {})
            if isinstance(options, dict):
                logging_config = _merge_logging_config(
                    logging_config,
                    options.get("logging"),
                )

    if log_func is not None:
        logging_config["log_func"] = log_func

    if logger_name is not None:
        logging_config["logger_name"] = _normalize_logger_name(logger_name)

    return logging_config

def fsm_default_log_func(level=None, message=None, fields=None, logger_name="generic_fsm"):
    level_map = {
        "info": logging.INFO,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "err": logging.ERROR,
        "error": logging.ERROR,
    }

    normalized_level = str(level or "info").lower()
    logging_level = level_map.get(normalized_level, logging.INFO)
    logger = logging.getLogger(_normalize_logger_name(logger_name))

    rendered_message = str(message or "fsm_log")
    if fields:
        rendered_message = f"{rendered_message} | {pformat(fields, sort_dicts=True, compact=True)}"

    logger.log(logging_level, rendered_message)

    return {
        "level": normalized_level,
        "message": rendered_message,
        "logger_name": _normalize_logger_name(logger_name),
    }

def fsm_make_log_func(logger_name="generic_fsm"):
    return partial(fsm_default_log_func, logger_name=_normalize_logger_name(logger_name))

def _fsm_emit_log(target, level, message, fields=None, log_func=None, logger_name=None, force=False):
    config = _resolve_logging_config(
        target=target,
        log_func=log_func,
        logger_name=logger_name,
    )

    if not force and not config.get("enabled"):
        return {
            "emitted": False,
            "level": level,
            "message": message,
            "fields": _copy_mapping(fields),
            "logger_name": config.get("logger_name"),
        }

    emitter = config.get("log_func") or fsm_default_log_func
    scope = {
        "level": str(level or "info").lower(),
        "message": str(message or "fsm_log"),
        "fields": _copy_mapping(fields),
        "logger_name": config.get("logger_name"),
    }
    _invoke_callable(emitter, scope)

    return {
        "emitted": True,
        "level": scope["level"],
        "message": scope["message"],
        "fields": scope["fields"],
        "logger_name": scope["logger_name"],
    }

# ---------------------------------------------------------------------------
#  Declarative builders
# ---------------------------------------------------------------------------

def fsm_result(
    data_patch=None,
    data_replace=None,
    ctx_patch=None,
    ctx_replace=None,
    emit=None,
    schedule=None,
    cancel_timers=None,
    halt=None,
    trace=None,
    meta_patch=None,
):
    result = {}

    if data_patch is not None:
        result["data_patch"] = _copy_mapping(data_patch)

    if data_replace is not None:
        result["data_replace"] = _deep_copy(data_replace)

    if ctx_patch is not None:
        result["ctx_patch"] = _copy_mapping(ctx_patch)

    if ctx_replace is not None:
        result["ctx_replace"] = _deep_copy(ctx_replace)

    if emit is not None:
        result["emit"] = _deep_copy(_as_list(emit))

    if schedule is not None:
        result["schedule"] = _deep_copy(_as_list(schedule))

    if cancel_timers is not None:
        result["cancel_timers"] = _deep_copy(_as_list(cancel_timers))

    if halt is not None:
        result["halt"] = bool(halt)

    if trace is not None:
        result["trace"] = _deep_copy(_as_list(trace))

    if meta_patch is not None:
        result["meta_patch"] = _copy_mapping(meta_patch)

    return result

def fsm_make_event(name, payload=None, headers=None, meta=None):
    return {
        "name": _normalize_name(name, "Event name"),
        "payload": _deep_copy(payload),
        "headers": _copy_mapping(headers),
        "meta": _copy_mapping(meta),
    }

def fsm_schedule_request(
    timer=None,
    event=None,
    delay=None,
    interval=None,
    repeat=None,
    payload=None,
    key=None,
    state_scoped=None,
    meta=None,
):
    if timer is None and event is None:
        raise ValueError("Schedule request requires a timer name or an inline event.")

    request = {
        "timer": timer,
        "event": _deep_copy(event),
        "delay": delay,
        "interval": interval,
        "repeat": repeat,
        "payload": _deep_copy(payload),
        "key": key,
        "state_scoped": state_scoped,
        "meta": _copy_mapping(meta),
    }
    return request

def fsm_cancel_request(timer=None, timer_id=None, key=None, owner_state=None):
    if timer is None and timer_id is None and key is None and owner_state is None:
        raise ValueError("Cancel request requires at least one selector.")
    return {
        "timer": timer,
        "timer_id": timer_id,
        "key": key,
        "owner_state": owner_state,
    }

def fsm_state(name, on_enter=None, on_exit=None, timers=None, meta=None):
    return {
        "name": _normalize_name(name, "State name"),
        "on_enter": _deep_copy(_as_list(on_enter)),
        "on_exit": _deep_copy(_as_list(on_exit)),
        "timers": _deep_copy(_as_list(timers)),
        "meta": _copy_mapping(meta),
    }

def fsm_action(name, func=None, meta=None):
    return {
        "name": _normalize_name(name, "Action name"),
        "func": func,
        "meta": _copy_mapping(meta),
    }

def fsm_event(name, normalizer=None, meta=None):
    return {
        "name": _normalize_name(name, "Event name"),
        "normalizer": normalizer,
        "meta": _copy_mapping(meta),
    }

def fsm_timer(
    name,
    event,
    delay=0,
    interval=None,
    repeat=None,
    payload=None,
    condition=None,
    meta=None,
):
    event_value = _deep_copy(event)
    if isinstance(event_value, str):
        event_value = fsm_make_event(event_value)

    if not isinstance(event_value, dict):
        raise ValueError("Timer event must be an event name or an event dict.")

    return {
        "name": _normalize_name(name, "Timer name"),
        "event": event_value,
        "delay": delay,
        "interval": interval,
        "repeat": repeat,
        "payload": _deep_copy(payload),
        "condition": condition,
        "meta": _copy_mapping(meta),
    }

def fsm_transition(
    name,
    source,
    event,
    target=None,
    guard=None,
    actions=None,
    priority=0,
    internal=False,
    meta=None,
):
    return {
        "name": _normalize_name(name, "Transition name"),
        "source": _deep_copy(source),
        "event": _deep_copy(event),
        "target": target,
        "guard": guard,
        "actions": _deep_copy(_as_list(actions)),
        "priority": int(priority),
        "internal": bool(internal),
        "meta": _copy_mapping(meta),
    }

# ---------------------------------------------------------------------------
#  Registry and spec construction
# ---------------------------------------------------------------------------

def _new_registry():
    return {
        "states": {},
        "actions": {},
        "events": {},
        "timers": {},
        "transitions": [],
        "transitions_by_name": {},
    }

def _new_options(
    strict=True,
    auto_register=False,
    allow_unhandled_events=True,
    logging_enabled=False,
    log_func=None,
    logger_name="generic_fsm",
):
    effective_logging_enabled = bool(logging_enabled)
    if log_func is not None:
        effective_logging_enabled = True

    return {
        "strict": bool(strict),
        "auto_register": bool(auto_register),
        "allow_unhandled_events": bool(allow_unhandled_events),
        "logging": _new_logging_config(
            enabled=effective_logging_enabled,
            log_func=log_func,
            logger_name=logger_name,
        ),
    }

def _new_spec(
    name,
    initial_state,
    strict=True,
    auto_register=False,
    meta=None,
    options=None,
    allow_unhandled_events=True,
    logging_enabled=False,
    log_func=None,
    logger_name="generic_fsm",
):
    merged_options = _new_options(
        strict=strict,
        auto_register=auto_register,
        allow_unhandled_events=allow_unhandled_events,
        logging_enabled=logging_enabled,
        log_func=log_func,
        logger_name=logger_name,
    )

    if isinstance(options, dict):
        for key, value in options.items():
            if key == "logging" and isinstance(value, dict):
                merged_options["logging"] = _merge_logging_config(
                    merged_options.get("logging"),
                    value,
                )
                continue
            merged_options[key] = _deep_copy(value)

    return {
        "name": _normalize_name(name, "FSM name"),
        "initial_state": _normalize_name(initial_state, "Initial state"),
        "registry": _new_registry(),
        "options": merged_options,
        "meta": _copy_mapping(meta),
    }

def _normalize_action_reference(action_reference):
    if action_reference is None:
        return None

    if isinstance(action_reference, str):
        return action_reference

    if callable(action_reference):
        return action_reference

    if isinstance(action_reference, dict):
        if "func" in action_reference:
            return action_reference
        raise ValueError("Inline action dict must contain a 'func' field.")

    raise ValueError("Unsupported action reference.")

def _normalize_guard_reference(guard_reference):
    if guard_reference is None:
        return []

    if isinstance(guard_reference, (list, tuple)):
        return [_normalize_action_reference(item) for item in guard_reference]

    return [_normalize_action_reference(guard_reference)]

def _normalize_action_list(actions):
    return [_normalize_action_reference(item) for item in _as_list(actions)]

def _transition_order(spec):
    return len(spec["registry"]["transitions"])

def _register_state(spec, state_definition):
    state = _deep_copy(state_definition)
    name = _normalize_name(state["name"], "State name")
    state["on_enter"] = _normalize_action_list(state.get("on_enter"))
    state["on_exit"] = _normalize_action_list(state.get("on_exit"))
    state["timers"] = _deep_copy(_as_list(state.get("timers")))
    state["meta"] = _copy_mapping(state.get("meta"))
    spec["registry"]["states"][name] = state

    _fsm_emit_log(
        spec,
        "info",
        "fsm_register_state",
        fields={
            "fsm_name": spec.get("name"),
            "state": name,
            "state_count": len(spec["registry"]["states"]),
        },
    )
    return spec

def _register_action(spec, action_definition):
    action = _deep_copy(action_definition)
    name = _normalize_name(action["name"], "Action name")
    action["meta"] = _copy_mapping(action.get("meta"))
    spec["registry"]["actions"][name] = action

    _fsm_emit_log(
        spec,
        "info",
        "fsm_register_action",
        fields={
            "fsm_name": spec.get("name"),
            "action": name,
            "action_count": len(spec["registry"]["actions"]),
        },
    )
    return spec

def _register_event(spec, event_definition):
    event = _deep_copy(event_definition)
    name = _normalize_name(event["name"], "Event name")
    event["meta"] = _copy_mapping(event.get("meta"))
    spec["registry"]["events"][name] = event

    _fsm_emit_log(
        spec,
        "info",
        "fsm_register_event",
        fields={
            "fsm_name": spec.get("name"),
            "event": name,
            "event_count": len(spec["registry"]["events"]),
        },
    )
    return spec

def _register_timer(spec, timer_definition):
    timer = _deep_copy(timer_definition)
    name = _normalize_name(timer["name"], "Timer name")
    timer["meta"] = _copy_mapping(timer.get("meta"))
    spec["registry"]["timers"][name] = timer

    _fsm_emit_log(
        spec,
        "info",
        "fsm_register_timer",
        fields={
            "fsm_name": spec.get("name"),
            "timer": name,
            "timer_count": len(spec["registry"]["timers"]),
        },
    )
    return spec

def _register_transition(spec, transition_definition):
    transition = _deep_copy(transition_definition)
    name = _normalize_name(transition["name"], "Transition name")
    transition["actions"] = _normalize_action_list(transition.get("actions"))
    transition["guard"] = _normalize_guard_reference(transition.get("guard"))
    transition["meta"] = _copy_mapping(transition.get("meta"))
    transition["order"] = _transition_order(spec)
    spec["registry"]["transitions"].append(transition)
    spec["registry"]["transitions_by_name"][name] = transition

    _fsm_emit_log(
        spec,
        "info",
        "fsm_register_transition",
        fields={
            "fsm_name": spec.get("name"),
            "transition": name,
            "transition_count": len(spec["registry"]["transitions"]),
        },
    )
    return spec

def _reference_is_wildcard(reference):
    return reference == "*"

def _iter_names(reference):
    if reference is None:
        return []
    if isinstance(reference, str):
        return [reference]
    if isinstance(reference, (list, tuple, set)):
        names = []
        for item in reference:
            if isinstance(item, str):
                names.append(item)
        return names
    return []

def _auto_register_missing(spec):
    if not spec["options"].get("auto_register"):
        return spec

    updated = _deep_copy(spec)

    if updated["initial_state"] not in updated["registry"]["states"]:
        _register_state(updated, fsm_state(updated["initial_state"]))

    for transition in list(updated["registry"]["transitions"]):
        for source_name in _iter_names(transition.get("source")):
            if source_name != "*" and source_name not in updated["registry"]["states"]:
                _register_state(updated, fsm_state(source_name))

        for event_name in _iter_names(transition.get("event")):
            if event_name != "*" and event_name not in updated["registry"]["events"]:
                _register_event(updated, fsm_event(event_name))

        target = transition.get("target")
        if isinstance(target, str) and target != "*" and target not in updated["registry"]["states"]:
            _register_state(updated, fsm_state(target))

        for guard_item in transition.get("guard", []):
            if isinstance(guard_item, str) and guard_item not in updated["registry"]["actions"]:
                _register_action(updated, fsm_action(guard_item))

        for action_item in transition.get("actions", []):
            if isinstance(action_item, str) and action_item not in updated["registry"]["actions"]:
                _register_action(updated, fsm_action(action_item))

    for state_name, state in list(updated["registry"]["states"].items()):
        for action_item in state.get("on_enter", []):
            if isinstance(action_item, str) and action_item not in updated["registry"]["actions"]:
                _register_action(updated, fsm_action(action_item))

        for action_item in state.get("on_exit", []):
            if isinstance(action_item, str) and action_item not in updated["registry"]["actions"]:
                _register_action(updated, fsm_action(action_item))

        for timer_item in state.get("timers", []):
            if isinstance(timer_item, str) and timer_item not in updated["registry"]["timers"]:
                raise ValueError(
                    f"Auto-register cannot infer timer '{timer_item}' because no timer definition exists."
                )

            if isinstance(timer_item, dict):
                timer_name = timer_item.get("timer")
                inline_event = timer_item.get("event")
                if timer_name is None and inline_event is None:
                    raise ValueError("State timer request requires 'timer' or 'event'.")

    for timer_name, timer_definition in list(updated["registry"]["timers"].items()):
        event_name = timer_definition.get("event", {}).get("name")
        if event_name and event_name not in updated["registry"]["events"]:
            _register_event(updated, fsm_event(event_name))

        condition = timer_definition.get("condition")
        if isinstance(condition, str) and condition not in updated["registry"]["actions"]:
            _register_action(updated, fsm_action(condition))

    return updated

def fsm_register_state(spec, state_definition):
    updated = _deep_copy(spec)
    return _register_state(updated, state_definition)

def fsm_register_action(spec, action_definition):
    updated = _deep_copy(spec)
    return _register_action(updated, action_definition)

def fsm_register_event(spec, event_definition):
    updated = _deep_copy(spec)
    return _register_event(updated, event_definition)

def fsm_register_timer(spec, timer_definition):
    updated = _deep_copy(spec)
    return _register_timer(updated, timer_definition)

def fsm_register_transition(spec, transition_definition):
    updated = _deep_copy(spec)
    return _register_transition(updated, transition_definition)

def fsm_build_spec(
    name,
    initial_state,
    states=None,
    transitions=None,
    actions=None,
    events=None,
    timers=None,
    strict=True,
    auto_register=False,
    allow_unhandled_events=True,
    meta=None,
    options=None,
    logging_enabled=False,
    log_func=None,
    logger_name="generic_fsm",
):
    # A spec may be built from any subset of registries.
    # Validation keeps cross references deterministic, while auto_register
    # can infer a controlled subset of missing names.
    spec = _new_spec(
        name=name,
        initial_state=initial_state,
        strict=strict,
        auto_register=auto_register,
        meta=meta,
        options=options,
        allow_unhandled_events=allow_unhandled_events,
        logging_enabled=logging_enabled,
        log_func=log_func,
        logger_name=logger_name,
    )
    spec["options"]["allow_unhandled_events"] = bool(allow_unhandled_events)

    for state_definition in _as_list(states):
        _register_state(spec, state_definition)

    for action_definition in _as_list(actions):
        _register_action(spec, action_definition)

    for event_definition in _as_list(events):
        _register_event(spec, event_definition)

    for timer_definition in _as_list(timers):
        _register_timer(spec, timer_definition)

    for transition_definition in _as_list(transitions):
        _register_transition(spec, transition_definition)

    spec = _auto_register_missing(spec)

    report = fsm_validate_spec(spec, raise_on_error=False)

    if report["warnings"]:
        _fsm_emit_log(
            spec,
            "warn",
            "fsm_spec_validation_warning",
            fields={
                "fsm_name": spec.get("name"),
                "warnings": _deep_copy(report["warnings"]),
            },
        )

    if not report["ok"]:
        _fsm_emit_log(
            spec,
            "err",
            "fsm_spec_validation_error",
            fields={
                "fsm_name": spec.get("name"),
                "errors": _deep_copy(report["errors"]),
            },
        )
        if spec["options"].get("strict"):
            raise ValueError(_join_error_messages(report["errors"]))

    _fsm_emit_log(
        spec,
        "info",
        "fsm_spec_built",
        fields={
            "fsm_name": spec.get("name"),
            "stats": _deep_copy(report.get("stats")),
        },
    )

    return spec

# ---------------------------------------------------------------------------
#  Validation
# ---------------------------------------------------------------------------

def _validate_reference_list(reference, registry, label, errors):
    if reference is None:
        return

    if callable(reference):
        return

    for name in _iter_names(reference):
        if name == "*":
            continue
        if name not in registry:
            errors.append(f"{label} references unknown name '{name}'.")

def _validate_action_reference(reference, spec, label, errors):
    if reference is None:
        return

    if callable(reference):
        return

    if isinstance(reference, dict):
        if "func" not in reference:
            errors.append(f"{label} inline action dict requires 'func'.")
        elif reference.get("func") is not None and not callable(reference.get("func")):
            errors.append(f"{label} inline action dict 'func' must be callable or None.")
        return

    if isinstance(reference, str):
        if reference not in spec["registry"]["actions"]:
            errors.append(f"{label} references unknown action '{reference}'.")
        return

    errors.append(f"{label} contains unsupported action reference.")

def _validate_timer_request(timer_request, spec, label, errors):
    if isinstance(timer_request, str):
        if timer_request not in spec["registry"]["timers"]:
            errors.append(f"{label} references unknown timer '{timer_request}'.")
        return

    if not isinstance(timer_request, dict):
        errors.append(f"{label} timer request must be a string or dict.")
        return

    timer_name = timer_request.get("timer")
    inline_event = timer_request.get("event")

    if timer_name is None and inline_event is None:
        errors.append(f"{label} timer request requires 'timer' or 'event'.")
        return

    if timer_name is not None and timer_name not in spec["registry"]["timers"]:
        errors.append(f"{label} references unknown timer '{timer_name}'.")

    if inline_event is not None:
        if isinstance(inline_event, str):
            if inline_event not in spec["registry"]["events"] and not spec["options"].get("auto_register"):
                errors.append(f"{label} inline event references unknown event '{inline_event}'.")
        elif not isinstance(inline_event, dict):
            errors.append(f"{label} inline event must be a string or dict.")

    delay = timer_request.get("delay")
    interval = timer_request.get("interval")
    repeat = timer_request.get("repeat")

    if delay is not None and delay < 0:
        errors.append(f"{label} delay must be >= 0.")

    if interval is not None and interval < 0:
        errors.append(f"{label} interval must be >= 0.")

    if repeat is not None and repeat <= 0:
        errors.append(f"{label} repeat must be > 0 when provided.")

def fsm_validate_spec(spec, raise_on_error=False):
    # Validation is deliberately strict and explicit.
    # The report stays fully data-driven so callers can decide whether
    # to raise, log, or recover at a higher layer.
    errors = []
    warnings = []

    if not isinstance(spec, dict):
        errors.append("FSM spec must be a dict.")
        report = {"ok": False, "errors": errors, "warnings": warnings, "stats": {}}
        if raise_on_error:
            raise ValueError(_join_error_messages(errors))
        return report

    if "registry" not in spec or not isinstance(spec["registry"], dict):
        errors.append("FSM spec requires a registry dict.")

    if errors:
        report = {"ok": False, "errors": errors, "warnings": warnings, "stats": {}}
        if raise_on_error:
            raise ValueError(_join_error_messages(errors))
        return report

    registry = spec["registry"]

    if not registry.get("states"):
        errors.append("FSM spec requires at least one state.")

    initial_state = spec.get("initial_state")
    if not isinstance(initial_state, str) or not initial_state:
        errors.append("FSM spec requires a valid initial_state.")
    elif initial_state not in registry.get("states", {}):
        errors.append(f"Initial state '{initial_state}' is not registered.")

    seen_transition_names = set()
    for transition in registry.get("transitions", []):
        transition_name = transition.get("name")

        if transition_name in seen_transition_names:
            errors.append(f"Transition name '{transition_name}' is duplicated.")
        else:
            seen_transition_names.add(transition_name)

        _validate_reference_list(
            transition.get("source"),
            registry.get("states", {}),
            f"Transition '{transition_name}' source",
            errors,
        )
        _validate_reference_list(
            transition.get("event"),
            registry.get("events", {}),
            f"Transition '{transition_name}' event",
            errors,
        )

        target = transition.get("target")
        if isinstance(target, str):
            if target != "*" and target not in registry.get("states", {}):
                errors.append(
                    f"Transition '{transition_name}' target references unknown state '{target}'."
                )
        elif target is not None and not callable(target):
            errors.append(
                f"Transition '{transition_name}' target must be None, state name, or callable."
            )

        for guard_item in transition.get("guard", []):
            _validate_action_reference(
                guard_item,
                spec,
                f"Transition '{transition_name}' guard",
                errors,
            )

        for action_item in transition.get("actions", []):
            _validate_action_reference(
                action_item,
                spec,
                f"Transition '{transition_name}' action",
                errors,
            )

    for state_name, state_definition in registry.get("states", {}).items():
        for action_item in state_definition.get("on_enter", []):
            _validate_action_reference(
                action_item,
                spec,
                f"State '{state_name}' on_enter",
                errors,
            )

        for action_item in state_definition.get("on_exit", []):
            _validate_action_reference(
                action_item,
                spec,
                f"State '{state_name}' on_exit",
                errors,
            )

        for timer_item in state_definition.get("timers", []):
            _validate_timer_request(
                timer_item,
                spec,
                f"State '{state_name}' timer",
                errors,
            )

    for action_name, action_definition in registry.get("actions", {}).items():
        func = action_definition.get("func")
        if func is not None and not callable(func):
            errors.append(f"Action '{action_name}' function must be callable or None.")

    for event_name, event_definition in registry.get("events", {}).items():
        normalizer = event_definition.get("normalizer")
        if normalizer is not None and not callable(normalizer):
            errors.append(f"Event '{event_name}' normalizer must be callable or None.")

    for timer_name, timer_definition in registry.get("timers", {}).items():
        event_definition = timer_definition.get("event")
        if not isinstance(event_definition, dict):
            errors.append(f"Timer '{timer_name}' event must be a dict.")
        else:
            timer_event_name = event_definition.get("name")
            if not isinstance(timer_event_name, str) or not timer_event_name:
                errors.append(f"Timer '{timer_name}' event requires a valid name.")
            elif timer_event_name not in registry.get("events", {}):
                errors.append(
                    f"Timer '{timer_name}' references unknown event '{timer_event_name}'."
                )

        delay = timer_definition.get("delay")
        interval = timer_definition.get("interval")
        repeat = timer_definition.get("repeat")

        if delay is not None and delay < 0:
            errors.append(f"Timer '{timer_name}' delay must be >= 0.")

        if interval is not None and interval < 0:
            errors.append(f"Timer '{timer_name}' interval must be >= 0.")

        if repeat is not None and repeat <= 0:
            errors.append(f"Timer '{timer_name}' repeat must be > 0 when provided.")

        if interval == 0 and (repeat is None or repeat > 1):
            errors.append(
                f"Timer '{timer_name}' interval=0 would create a non-terminating recurring timer."
            )

        condition = timer_definition.get("condition")
        if isinstance(condition, str) and condition not in registry.get("actions", {}):
            errors.append(f"Timer '{timer_name}' condition references unknown action '{condition}'.")
        elif condition is not None and not isinstance(condition, str) and not callable(condition):
            errors.append(f"Timer '{timer_name}' condition must be an action name, callable, or None.")

    report = {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "states": len(registry.get("states", {})),
            "transitions": len(registry.get("transitions", [])),
            "actions": len(registry.get("actions", {})),
            "events": len(registry.get("events", {})),
            "timers": len(registry.get("timers", {})),
        },
    }

    if raise_on_error and not report["ok"]:
        raise ValueError(_join_error_messages(report["errors"]))

    return report

# ---------------------------------------------------------------------------
#  Runtime model and context construction
# ---------------------------------------------------------------------------

def _new_runtime(initial_state, data=None, clock=0):
    return {
        "state": initial_state,
        "started": False,
        "halted": False,
        "clock": int(clock),
        "queue": [],
        "timers": [],
        "history": [],
        "trace": [],
        "last_event": None,
        "last_transition": None,
        "counters": {
            "event_sequence": 0,
            "timer_sequence": 0,
            "history_sequence": 0,
            "timer_id": 0,
            "events_processed": 0,
            "transitions_taken": 0,
            "timers_fired": 0,
            "actions_executed": 0,
        },
        "data": _deep_copy(data) if data is not None else {},
    }

def _new_machine(spec, ctx, data=None, clock=0):
    return {
        "spec": _deep_copy(spec),
        "ctx": _deep_copy(ctx),
        "runtime": _new_runtime(spec["initial_state"], data=data, clock=clock),
        "meta": {},
    }

def _next_counter(machine, name):
    updated = _deep_copy(machine)
    updated["runtime"]["counters"][name] += 1
    return updated, updated["runtime"]["counters"][name]

def _append_history(machine, entry):
    updated = _deep_copy(machine)
    updated["runtime"]["counters"]["history_sequence"] += 1
    enriched = _deep_copy(entry)
    enriched["sequence"] = updated["runtime"]["counters"]["history_sequence"]
    updated["runtime"]["history"].append(enriched)
    updated["runtime"]["history"].sort(key=_history_sort_key)
    return updated

def _append_trace(machine, messages):
    updated = _deep_copy(machine)
    for item in _as_list(messages):
        updated["runtime"]["trace"].append(_deep_copy(item))
    return updated

def _build_ctx(ctx_builder=None, ctx_seed=None, spec=None, params=None, clock=0):
    # CTX construction is the dependency injection seam of the library.
    # The builder receives a stable scope and may return any mapping-like
    # context structure required by actions, guards, timers, or resolvers.
    if ctx_builder is None:
        if ctx_seed is None:
            return {}
        return _deep_copy(ctx_seed)

    scope = {
        "seed": _deep_copy(ctx_seed),
        "ctx_seed": _deep_copy(ctx_seed),
        "spec": _deep_copy(spec),
        "params": _copy_mapping(params),
        "clock": int(clock),
        "partial": partial,
    }
    result = _invoke_callable(ctx_builder, scope)
    if result is None:
        return {}
    return _deep_copy(result)

def _state_definition(machine, state_name=None):
    name = state_name or machine["runtime"]["state"]
    return machine["spec"]["registry"]["states"][name]

def _event_definition(machine, event_name):
    return machine["spec"]["registry"]["events"].get(event_name)

def _resolve_action(machine, action_reference):
    if action_reference is None:
        return None, "<noop>"

    if callable(action_reference):
        return action_reference, getattr(action_reference, "__name__", "<callable>")

    if isinstance(action_reference, dict):
        func = action_reference.get("func")
        label = action_reference.get("name") or getattr(func, "__name__", "<inline_action>")
        return func, label

    if isinstance(action_reference, str):
        action_definition = machine["spec"]["registry"]["actions"].get(action_reference)
        if action_definition is None:
            raise ValueError(f"Unknown action '{action_reference}'.")
        return action_definition.get("func"), action_reference

    raise ValueError("Unsupported action reference.")

def _resolve_guard(machine, guard_reference):
    return _resolve_action(machine, guard_reference)

def _resolve_timer_condition(machine, condition_reference):
    return _resolve_action(machine, condition_reference)

def _build_scope(
    machine,
    phase,
    event=None,
    transition=None,
    state_name=None,
    target_state=None,
    timer_instance=None,
    extra=None,
):
    current_state_name = state_name or machine["runtime"]["state"]
    scope = {
        "machine": _deep_copy(machine),
        "spec": _deep_copy(machine["spec"]),
        "ctx": _deep_copy(machine["ctx"]),
        "data": _deep_copy(machine["runtime"]["data"]),
        "runtime": _deep_copy(machine["runtime"]),
        "registry": _deep_copy(machine["spec"]["registry"]),
        "phase": phase,
        "clock": machine["runtime"]["clock"],
        "event": _deep_copy(event),
        "transition": _deep_copy(transition),
        "state_name": current_state_name,
        "state": _deep_copy(machine["spec"]["registry"]["states"].get(current_state_name)),
        "source_state": current_state_name,
        "target_state": target_state,
        "timer": _deep_copy(timer_instance),
        "fsm_result": fsm_result,
        "fsm_make_event": fsm_make_event,
        "fsm_schedule_request": fsm_schedule_request,
        "fsm_cancel_request": fsm_cancel_request,
        "partial": partial,
    }
    if extra:
        for key, value in extra.items():
            scope[key] = _deep_copy(value)
    return scope

# ---------------------------------------------------------------------------
#  Event normalization and transition matching
# ---------------------------------------------------------------------------

def _normalize_event_input(event_input):
    if isinstance(event_input, str):
        return fsm_make_event(event_input)

    if isinstance(event_input, dict):
        if "name" not in event_input:
            raise ValueError("Event dict requires a 'name' field.")
        return {
            "name": _normalize_name(event_input.get("name"), "Event name"),
            "payload": _deep_copy(event_input.get("payload")),
            "headers": _copy_mapping(event_input.get("headers")),
            "meta": _copy_mapping(event_input.get("meta")),
        }

    raise ValueError("Event must be a string or dict.")

def _normalize_registered_event(machine, event_input):
    # Event normalization keeps the external API flexible while preserving
    # a strict internal representation for matching, sorting, and tracing.
    event = _normalize_event_input(event_input)
    event_definition = _event_definition(machine, event["name"])

    if event_definition is None:
        if machine["spec"]["options"].get("strict") and not machine["spec"]["options"].get("auto_register"):
            raise ValueError(f"Event '{event['name']}' is not registered.")
        return event

    event["meta"] = _merge_mapping(event_definition.get("meta"), event.get("meta"))

    normalizer = event_definition.get("normalizer")
    if normalizer is None:
        return event

    scope = _build_scope(
        machine=machine,
        phase="event_normalizer",
        event=event,
        extra={"event_definition": _deep_copy(event_definition)},
    )
    normalized = _invoke_callable(normalizer, scope)

    if normalized is None:
        return event

    if isinstance(normalized, str):
        return fsm_make_event(normalized)

    if isinstance(normalized, dict) and "name" in normalized:
        merged = _normalize_event_input(normalized)
        merged["meta"] = _merge_mapping(event["meta"], merged.get("meta"))
        return merged

    if isinstance(normalized, dict):
        merged = _deep_copy(event)
        for key, value in normalized.items():
            merged[key] = _deep_copy(value)
        return _normalize_event_input(merged)

    raise ValueError(f"Event normalizer for '{event['name']}' returned an unsupported value.")

def _enqueue_event(machine, event):
    normalized = _normalize_registered_event(machine, event)
    updated = _deep_copy(machine)
    updated["runtime"]["counters"]["event_sequence"] += 1
    normalized["clock"] = updated["runtime"]["clock"]
    normalized["meta"] = _merge_mapping(
        normalized.get("meta"),
        {"sequence": updated["runtime"]["counters"]["event_sequence"]},
    )
    updated["runtime"]["queue"].append(normalized)
    updated["runtime"]["queue"].sort(key=_event_sort_key)
    return updated

def fsm_enqueue(machine, event):
    return _enqueue_event(machine, event)

def _event_matches(reference, event_name):
    if _reference_is_wildcard(reference):
        return True

    if isinstance(reference, str):
        return reference == event_name

    if isinstance(reference, (list, tuple, set)):
        return event_name in reference or "*" in reference

    return False

def _state_matches(reference, state_name):
    if _reference_is_wildcard(reference):
        return True

    if isinstance(reference, str):
        return reference == state_name

    if isinstance(reference, (list, tuple, set)):
        return state_name in reference or "*" in reference

    return False

def _guard_passes(machine, guard_reference, event, transition):
    func, _ = _resolve_guard(machine, guard_reference)
    if func is None:
        return True

    scope = _build_scope(
        machine=machine,
        phase="guard",
        event=event,
        transition=transition,
        target_state=transition.get("target"),
    )
    result = _invoke_callable(func, scope)
    return bool(result)

def _transition_matches(machine, transition, event):
    current_state = machine["runtime"]["state"]

    if not _state_matches(transition.get("source"), current_state):
        return False

    if not _event_matches(transition.get("event"), event["name"]):
        return False

    for guard_item in transition.get("guard", []):
        if not _guard_passes(machine, guard_item, event, transition):
            return False

    return True

def _transition_selection_sort_key(transition):
    return (-int(transition.get("priority", 0)), int(transition.get("order", 0)), transition.get("name", ""))

def _select_transition(machine, event):
    # Transition selection is deterministic: highest priority first,
    # then registration order, and finally transition name as a stable
    # tie-breaker for diagnostics.
    candidates = []
    for transition in machine["spec"]["registry"]["transitions"]:
        if _transition_matches(machine, transition, event):
            candidates.append(_deep_copy(transition))

    if not candidates:
        return None

    candidates.sort(key=_transition_selection_sort_key)
    return candidates[0]

def _resolve_target_state(machine, transition, event):
    target = transition.get("target")
    if target is None:
        return machine["runtime"]["state"]

    if isinstance(target, str):
        return target

    if callable(target):
        scope = _build_scope(
            machine=machine,
            phase="target_resolver",
            event=event,
            transition=transition,
        )
        resolved = _invoke_callable(target, scope)
        if not isinstance(resolved, str) or not resolved:
            raise ValueError(
                f"Transition '{transition['name']}' target resolver must return a state name."
            )
        return resolved

    raise ValueError(f"Transition '{transition['name']}' has invalid target definition.")

# ---------------------------------------------------------------------------
#  Action execution and result application
# ---------------------------------------------------------------------------

def _record_action_execution(machine):
    updated = _deep_copy(machine)
    updated["runtime"]["counters"]["actions_executed"] += 1
    return updated

def _apply_result(machine, action_result, phase, event=None, transition=None):
    # Action results are normalized into a single declarative structure.
    # This keeps actions simple while allowing them to patch data and CTX,
    # emit events, schedule timers, append traces, and halt the machine.
    if action_result is None:
        return machine

    if isinstance(action_result, str):
        action_result = fsm_result(emit=[action_result])
    elif isinstance(action_result, dict) and "name" in action_result:
        action_result = fsm_result(emit=[action_result])
    elif not isinstance(action_result, dict):
        raise ValueError(
            f"Action result in phase '{phase}' must be None, event, or result dict."
        )

    updated = _deep_copy(machine)

    if "ctx_replace" in action_result:
        updated["ctx"] = _deep_copy(action_result["ctx_replace"])

    if "ctx_patch" in action_result:
        updated["ctx"] = _merge_mapping(updated["ctx"], action_result["ctx_patch"])

    if "data_replace" in action_result:
        updated["runtime"]["data"] = _deep_copy(action_result["data_replace"])

    if "data_patch" in action_result:
        updated["runtime"]["data"] = _merge_mapping(
            updated["runtime"]["data"],
            action_result["data_patch"],
        )

    if "meta_patch" in action_result:
        updated["meta"] = _merge_mapping(updated.get("meta"), action_result["meta_patch"])

    if "trace" in action_result:
        updated = _append_trace(updated, action_result["trace"])

    if action_result.get("halt") is True:
        updated["runtime"]["halted"] = True

    for cancel_request in _as_list(action_result.get("cancel_timers")):
        updated = _cancel_timer_request(updated, cancel_request)

    for schedule_request in _as_list(action_result.get("schedule")):
        updated = _schedule_timer_request(updated, schedule_request)

    for emitted_event in _as_list(action_result.get("emit")):
        updated = _enqueue_event(updated, emitted_event)

    if phase:
        updated = _append_history(
            updated,
            {
                "kind": "action_result",
                "phase": phase,
                "clock": updated["runtime"]["clock"],
                "state": updated["runtime"]["state"],
                "event": _deep_copy(event),
                "transition": _deep_copy(transition),
                "result_keys": sorted(list(action_result.keys())),
            },
        )

    return updated

def _run_action(machine, action_reference, phase, event=None, transition=None, target_state=None, timer_instance=None):
    func, action_label = _resolve_action(machine, action_reference)

    if func is None:
        return machine

    updated = _record_action_execution(machine)
    scope = _build_scope(
        machine=updated,
        phase=phase,
        event=event,
        transition=transition,
        target_state=target_state,
        timer_instance=timer_instance,
        extra={"action_name": action_label},
    )
    result = _invoke_callable(func, scope)
    updated = _apply_result(
        updated,
        result,
        phase=phase,
        event=event,
        transition=transition,
    )
    updated = _append_history(
        updated,
        {
            "kind": "action",
            "phase": phase,
            "clock": updated["runtime"]["clock"],
            "state": updated["runtime"]["state"],
            "action": action_label,
            "event": _deep_copy(event),
            "transition": _deep_copy(transition),
        },
    )
    return updated

def _run_action_list(machine, action_references, phase, event=None, transition=None, target_state=None, timer_instance=None):
    updated = _deep_copy(machine)
    for action_reference in _as_list(action_references):
        updated = _run_action(
            updated,
            action_reference,
            phase=phase,
            event=event,
            transition=transition,
            target_state=target_state,
            timer_instance=timer_instance,
        )
    return updated

# ---------------------------------------------------------------------------
#  Timer runtime handling
# ---------------------------------------------------------------------------

def _normalize_state_timer_request(timer_request, state_name):
    if isinstance(timer_request, str):
        return fsm_schedule_request(
            timer=timer_request,
            state_scoped=True,
            meta={"owner_state": state_name},
        )

    if not isinstance(timer_request, dict):
        raise ValueError("State timer request must be a string or dict.")

    normalized = _deep_copy(timer_request)
    if normalized.get("state_scoped") is None:
        normalized["state_scoped"] = True

    meta = _copy_mapping(normalized.get("meta"))
    if "owner_state" not in meta:
        meta["owner_state"] = state_name
    normalized["meta"] = meta
    return normalized

def _timer_fires_remaining(interval, repeat):
    if interval is None:
        return 1
    if repeat is None:
        return None
    return int(repeat)

def _build_timer_instance(machine, request, owner_state=None):
    request_copy = _deep_copy(request)

    if isinstance(request_copy, str):
        request_copy = {"timer": request_copy}

    if not isinstance(request_copy, dict):
        raise ValueError("Timer schedule request must be a string or dict.")

    timer_name = request_copy.get("timer")
    timer_definition = None
    if timer_name is not None:
        timer_definition = machine["spec"]["registry"]["timers"].get(timer_name)
        if timer_definition is None:
            raise ValueError(f"Unknown timer '{timer_name}'.")
    else:
        timer_definition = {
            "name": request_copy.get("name") or "inline_timer",
            "event": request_copy.get("event"),
            "delay": request_copy.get("delay", 0),
            "interval": request_copy.get("interval"),
            "repeat": request_copy.get("repeat"),
            "payload": request_copy.get("payload"),
            "condition": request_copy.get("condition"),
            "meta": request_copy.get("meta"),
        }

    event_definition = _deep_copy(timer_definition.get("event"))
    if isinstance(event_definition, str):
        event_definition = fsm_make_event(event_definition)

    if request_copy.get("event") is not None:
        inline_event = request_copy.get("event")
        if isinstance(inline_event, str):
            event_definition = fsm_make_event(inline_event)
        else:
            event_definition = _normalize_event_input(inline_event)

    if not isinstance(event_definition, dict):
        raise ValueError("Scheduled timer requires a valid event definition.")

    if "name" not in event_definition:
        raise ValueError("Scheduled timer event definition requires a name.")

    delay = timer_definition.get("delay", 0) if request_copy.get("delay") is None else request_copy.get("delay")
    interval = timer_definition.get("interval") if request_copy.get("interval") is None else request_copy.get("interval")
    repeat = timer_definition.get("repeat") if request_copy.get("repeat") is None else request_copy.get("repeat")
    payload = timer_definition.get("payload") if request_copy.get("payload") is None else request_copy.get("payload")
    condition = timer_definition.get("condition")
    if request_copy.get("condition") is not None:
        condition = request_copy.get("condition")

    if delay is None:
        delay = 0

    if interval is not None and interval < 0:
        raise ValueError("Timer interval must be >= 0.")

    if delay < 0:
        raise ValueError("Timer delay must be >= 0.")

    if repeat is not None and repeat <= 0:
        raise ValueError("Timer repeat must be > 0 when provided.")

    if interval == 0 and (repeat is None or repeat > 1):
        raise ValueError("interval=0 recurring timer would not terminate.")

    updated = _deep_copy(machine)
    updated["runtime"]["counters"]["timer_id"] += 1
    updated["runtime"]["counters"]["timer_sequence"] += 1

    timer_event_meta = _copy_mapping(event_definition.get("meta"))
    meta = _merge_mapping(timer_definition.get("meta"), request_copy.get("meta"))
    if owner_state is not None and "owner_state" not in meta:
        meta["owner_state"] = owner_state

    state_scoped = request_copy.get("state_scoped")
    if state_scoped is None:
        state_scoped = False

    due_at = updated["runtime"]["clock"] + int(delay)

    timer_instance = {
        "id": updated["runtime"]["counters"]["timer_id"],
        "sequence": updated["runtime"]["counters"]["timer_sequence"],
        "name": timer_name or timer_definition.get("name") or "inline_timer",
        "event": _merge_mapping(
            event_definition,
            {
                "payload": _deep_copy(payload) if payload is not None else _deep_copy(event_definition.get("payload")),
                "meta": _merge_mapping(
                    timer_event_meta,
                    {
                        "timer_name": timer_name or timer_definition.get("name") or "inline_timer",
                        "timer_id": updated["runtime"]["counters"]["timer_id"],
                        "source": "timer",
                    },
                ),
            },
        ),
        "due_at": due_at,
        "interval": interval,
        "remaining_fires": _timer_fires_remaining(interval, repeat),
        "condition": condition,
        "payload": _deep_copy(payload),
        "key": request_copy.get("key"),
        "state_scoped": bool(state_scoped),
        "owner_state": meta.get("owner_state"),
        "meta": _copy_mapping(meta),
    }

    return updated, timer_instance

def _schedule_timer_request(machine, schedule_request):
    owner_state = None
    if isinstance(schedule_request, dict):
        owner_state = _copy_mapping(schedule_request.get("meta")).get("owner_state")

    updated, timer_instance = _build_timer_instance(
        machine,
        schedule_request,
        owner_state=owner_state,
    )
    updated["runtime"]["timers"].append(timer_instance)
    updated["runtime"]["timers"].sort(key=_timer_sort_key)
    updated = _append_history(
        updated,
        {
            "kind": "timer_scheduled",
            "clock": updated["runtime"]["clock"],
            "state": updated["runtime"]["state"],
            "timer": _deep_copy(timer_instance),
        },
    )

    _fsm_emit_log(
        updated,
        "info",
        "fsm_timer_scheduled",
        fields={
            "fsm_name": updated["spec"].get("name"),
            "timer_name": timer_instance.get("name"),
            "timer_id": timer_instance.get("id"),
            "due_at": timer_instance.get("due_at"),
            "owner_state": timer_instance.get("owner_state"),
            "state_scoped": timer_instance.get("state_scoped"),
            "state": updated["runtime"]["state"],
        },
    )

    return updated

def fsm_schedule_timer(machine, schedule_request):
    return _schedule_timer_request(machine, schedule_request)

def _timer_cancel_match(timer_instance, request):
    if request is None:
        return False

    if isinstance(request, str):
        return timer_instance.get("name") == request

    if not isinstance(request, dict):
        raise ValueError("Cancel request must be a string or dict.")

    timer_name = request.get("timer")
    timer_id = request.get("timer_id")
    key = request.get("key")
    owner_state = request.get("owner_state")

    if timer_name is not None and timer_instance.get("name") != timer_name:
        return False

    if timer_id is not None and timer_instance.get("id") != timer_id:
        return False

    if key is not None and timer_instance.get("key") != key:
        return False

    if owner_state is not None and timer_instance.get("owner_state") != owner_state:
        return False

    return True

def _cancel_timer_request(machine, cancel_request):
    updated = _deep_copy(machine)
    kept = []
    removed = []

    for timer_instance in updated["runtime"]["timers"]:
        if _timer_cancel_match(timer_instance, cancel_request):
            removed.append(timer_instance)
        else:
            kept.append(timer_instance)

    updated["runtime"]["timers"] = kept

    if removed:
        updated = _append_history(
            updated,
            {
                "kind": "timer_cancelled",
                "clock": updated["runtime"]["clock"],
                "state": updated["runtime"]["state"],
                "removed_timers": _deep_copy(removed),
                "request": _deep_copy(cancel_request),
            },
        )

        _fsm_emit_log(
            updated,
            "info",
            "fsm_timer_cancelled",
            fields={
                "fsm_name": updated["spec"].get("name"),
                "state": updated["runtime"]["state"],
                "removed_count": len(removed),
                "request": _deep_copy(cancel_request),
            },
        )

    return updated

def fsm_cancel_timer(machine, cancel_request):
    return _cancel_timer_request(machine, cancel_request)

def _cancel_state_scoped_timers(machine, state_name):
    return _cancel_timer_request(machine, {"owner_state": state_name})

def _timer_condition_passes(machine, timer_instance):
    condition_reference = timer_instance.get("condition")
    if condition_reference is None:
        return True

    func, _ = _resolve_timer_condition(machine, condition_reference)
    if func is None:
        return True

    scope = _build_scope(
        machine=machine,
        phase="timer_condition",
        event=timer_instance.get("event"),
        timer_instance=timer_instance,
        extra={"timer_name": timer_instance.get("name")},
    )
    result = _invoke_callable(func, scope)
    return bool(result)

def _decrement_timer_instance(timer_instance):
    updated = _deep_copy(timer_instance)
    remaining = updated.get("remaining_fires")
    if remaining is None:
        return updated

    updated["remaining_fires"] = remaining - 1
    return updated

def _should_reschedule_timer(timer_instance):
    interval = timer_instance.get("interval")
    if interval is None:
        return False

    remaining = timer_instance.get("remaining_fires")
    if remaining is None:
        return True

    return remaining > 0

def _reschedule_timer_instance(timer_instance):
    updated = _deep_copy(timer_instance)
    updated["due_at"] = updated["due_at"] + int(updated["interval"])
    updated["sequence"] = updated["sequence"] + 1
    return updated

def _has_due_timers(machine):
    clock = machine["runtime"]["clock"]
    for timer_instance in machine["runtime"]["timers"]:
        if timer_instance.get("due_at", 0) <= clock:
            return True
    return False

def _pop_due_timers(machine):
    updated = _deep_copy(machine)
    clock = updated["runtime"]["clock"]
    due = []
    pending = []

    for timer_instance in updated["runtime"]["timers"]:
        if timer_instance.get("due_at", 0) <= clock:
            due.append(_deep_copy(timer_instance))
        else:
            pending.append(_deep_copy(timer_instance))

    due.sort(key=_timer_sort_key)
    pending.sort(key=_timer_sort_key)
    updated["runtime"]["timers"] = pending
    return updated, due

def _emit_timer_event(machine, timer_instance):
    event = _deep_copy(timer_instance.get("event"))
    event["meta"] = _merge_mapping(
        event.get("meta"),
        {
            "source": "timer",
            "timer_name": timer_instance.get("name"),
            "timer_id": timer_instance.get("id"),
            "owner_state": timer_instance.get("owner_state"),
        },
    )
    updated = _enqueue_event(machine, event)
    updated["runtime"]["counters"]["timers_fired"] += 1
    updated = _append_history(
        updated,
        {
            "kind": "timer_fired",
            "clock": updated["runtime"]["clock"],
            "state": updated["runtime"]["state"],
            "timer": _deep_copy(timer_instance),
            "event": _deep_copy(event),
        },
    )

    _fsm_emit_log(
        updated,
        "info",
        "fsm_timer_fired",
        fields={
            "fsm_name": updated["spec"].get("name"),
            "timer_name": timer_instance.get("name"),
            "timer_id": timer_instance.get("id"),
            "event": event.get("name"),
            "state": updated["runtime"]["state"],
            "clock": updated["runtime"]["clock"],
        },
    )

    return updated

def _enqueue_due_timer_events(machine, remaining_steps):
    if remaining_steps <= 0:
        return machine, 0

    updated, due_timers = _pop_due_timers(machine)
    processed = 0

    for timer_instance in due_timers:
        if processed >= remaining_steps:
            updated["runtime"]["timers"].append(_deep_copy(timer_instance))
            updated["runtime"]["timers"].sort(key=_timer_sort_key)
            continue

        current_timer = _decrement_timer_instance(timer_instance)

        if _timer_condition_passes(updated, current_timer):
            updated = _emit_timer_event(updated, current_timer)
            processed += 1

        if _should_reschedule_timer(current_timer):
            rescheduled = _reschedule_timer_instance(current_timer)
            updated["runtime"]["timers"].append(rescheduled)
            updated["runtime"]["timers"].sort(key=_timer_sort_key)

    return updated, processed

# ---------------------------------------------------------------------------
#  State lifecycle and event consumption
# ---------------------------------------------------------------------------

def _enter_state(machine, state_name, event=None, transition=None):
    # Entering a state executes on_enter actions before state-scoped
    # timers are scheduled. This order allows actions to influence
    # subsequent timer scheduling through emitted results.
    updated = _deep_copy(machine)
    updated["runtime"]["state"] = state_name

    state_definition = _state_definition(updated, state_name)
    updated = _run_action_list(
        updated,
        state_definition.get("on_enter"),
        phase="state_enter",
        event=event,
        transition=transition,
        target_state=state_name,
    )

    for timer_request in state_definition.get("timers", []):
        normalized_timer_request = _normalize_state_timer_request(timer_request, state_name)
        updated = _schedule_timer_request(updated, normalized_timer_request)

    updated = _append_history(
        updated,
        {
            "kind": "state_enter",
            "clock": updated["runtime"]["clock"],
            "state": state_name,
            "event": _deep_copy(event),
            "transition": _deep_copy(transition),
        },
    )
    return updated

def _exit_state(machine, state_name, event=None, transition=None):
    # Exiting a state executes on_exit actions and then cancels
    # all timers owned by that state when they are state-scoped.
    updated = _deep_copy(machine)
    state_definition = _state_definition(updated, state_name)

    updated = _run_action_list(
        updated,
        state_definition.get("on_exit"),
        phase="state_exit",
        event=event,
        transition=transition,
        target_state=transition.get("target") if isinstance(transition, dict) else None,
    )

    updated = _cancel_state_scoped_timers(updated, state_name)

    updated = _append_history(
        updated,
        {
            "kind": "state_exit",
            "clock": updated["runtime"]["clock"],
            "state": state_name,
            "event": _deep_copy(event),
            "transition": _deep_copy(transition),
        },
    )
    return updated

def _consume_unhandled_event(machine, event):
    updated = _deep_copy(machine)
    updated["runtime"]["last_event"] = _deep_copy(event)
    updated["runtime"]["counters"]["events_processed"] += 1
    updated = _append_history(
        updated,
        {
            "kind": "event_unhandled",
            "clock": updated["runtime"]["clock"],
            "state": updated["runtime"]["state"],
            "event": _deep_copy(event),
        },
    )

    _fsm_emit_log(
        updated,
        "warn",
        "fsm_event_unhandled",
        fields={
            "fsm_name": updated["spec"].get("name"),
            "state": updated["runtime"]["state"],
            "event": event.get("name"),
        },
    )

    if not updated["spec"]["options"].get("allow_unhandled_events", True):
        raise ValueError(
            f"Unhandled event '{event['name']}' in state '{updated['runtime']['state']}'."
        )
    return updated

def _transition_is_external(transition):
    if transition.get("internal"):
        return False
    if transition.get("target") is None:
        return False
    return True

def _consume_event(machine, event):
    # Event consumption resolves exactly one transition candidate.
    # The chosen transition is stable because sorting uses priority first
    # and registration order second, which keeps the FSM deterministic.
    updated = _deep_copy(machine)
    selected_transition = _select_transition(updated, event)

    if selected_transition is None:
        return _consume_unhandled_event(updated, event)

    current_state = updated["runtime"]["state"]
    target_state = _resolve_target_state(updated, selected_transition, event)

    if target_state not in updated["spec"]["registry"]["states"]:
        _fsm_emit_log(
            updated,
            "err",
            "fsm_transition_target_invalid",
            fields={
                "fsm_name": updated["spec"].get("name"),
                "transition": selected_transition.get("name"),
                "target_state": target_state,
            },
        )
        raise ValueError(
            f"Transition '{selected_transition['name']}' resolved unknown target state '{target_state}'."
        )

    updated["runtime"]["last_event"] = _deep_copy(event)
    updated["runtime"]["last_transition"] = _deep_copy(selected_transition)
    updated["runtime"]["counters"]["events_processed"] += 1
    updated["runtime"]["counters"]["transitions_taken"] += 1

    external = _transition_is_external(selected_transition)

    if external:
        updated = _exit_state(
            updated,
            current_state,
            event=event,
            transition=selected_transition,
        )

    updated = _run_action_list(
        updated,
        selected_transition.get("actions"),
        phase="transition_action",
        event=event,
        transition=selected_transition,
        target_state=target_state,
    )

    if external:
        updated = _enter_state(
            updated,
            target_state,
            event=event,
            transition=selected_transition,
        )

    updated = _append_history(
        updated,
        {
            "kind": "transition",
            "clock": updated["runtime"]["clock"],
            "state_before": current_state,
            "state_after": updated["runtime"]["state"],
            "event": _deep_copy(event),
            "transition": _deep_copy(selected_transition),
        },
    )

    _fsm_emit_log(
        updated,
        "info",
        "fsm_transition",
        fields={
            "fsm_name": updated["spec"].get("name"),
            "transition": selected_transition.get("name"),
            "event": event.get("name"),
            "source_state": current_state,
            "target_state": updated["runtime"]["state"],
            "internal": bool(selected_transition.get("internal")),
            "priority": int(selected_transition.get("priority", 0)),
        },
    )
    return updated

# ---------------------------------------------------------------------------
#  Public runtime execution API
# ---------------------------------------------------------------------------

def fsm_process_queue(machine, max_steps=1000):
    # Queue processing is the deterministic execution core.
    # Timers due at the current logical clock are turned into events first,
    # then queued events are consumed in stable order until work is exhausted.
    if max_steps <= 0:
        raise ValueError("max_steps must be > 0.")

    updated = _deep_copy(machine)
    steps = 0

    while steps < max_steps:
        if updated["runtime"]["halted"]:
            break

        remaining_steps = max_steps - steps
        updated, timer_steps = _enqueue_due_timer_events(updated, remaining_steps)
        steps += timer_steps

        if steps >= max_steps:
            break

        if not updated["runtime"]["queue"]:
            break

        event = updated["runtime"]["queue"].pop(0)
        updated = _consume_event(updated, event)
        steps += 1

    pending_work = bool(updated["runtime"]["queue"]) or _has_due_timers(updated)

    if pending_work and not updated["runtime"]["halted"]:
        _fsm_emit_log(
            updated,
            "err",
            "fsm_process_queue_limit",
            fields={
                "fsm_name": updated["spec"].get("name"),
                "max_steps": int(max_steps),
                "state": updated["runtime"]["state"],
                "queue_length": len(updated["runtime"]["queue"]),
                "due_timers_remaining": _has_due_timers(updated),
            },
        )
        raise RuntimeError("FSM processing exceeded max_steps before all work items were consumed.")

    return updated

def fsm_dispatch(machine, event, process_queue=True, max_steps=1000):
    updated = _enqueue_event(machine, event)
    if not process_queue:
        return updated
    return fsm_process_queue(updated, max_steps=max_steps)

def fsm_dispatch_many(machine, events, process_queue=True, max_steps=1000):
    updated = _deep_copy(machine)
    for event in _as_list(events):
        updated = _enqueue_event(updated, event)

    if not process_queue:
        return updated

    return fsm_process_queue(updated, max_steps=max_steps)

def fsm_start(machine, process_queue=True, max_steps=1000):
    # Start is idempotent. It records the FSM start event, enters the
    # initial state exactly once, and then optionally drains queued work.
    updated = _deep_copy(machine)
    if updated["runtime"]["started"]:
        return updated

    initial_state = updated["runtime"]["state"]
    updated["runtime"]["started"] = True
    updated = _append_history(
        updated,
        {
            "kind": "fsm_start",
            "clock": updated["runtime"]["clock"],
            "state": initial_state,
        },
    )
    updated = _enter_state(updated, initial_state)

    _fsm_emit_log(
        updated,
        "info",
        "fsm_started",
        fields={
            "fsm_name": updated["spec"].get("name"),
            "state": initial_state,
            "clock": updated["runtime"]["clock"],
        },
    )

    if process_queue:
        updated = fsm_process_queue(updated, max_steps=max_steps)

    return updated

def fsm_create(
    spec,
    ctx_builder=None,
    ctx_seed=None,
    data=None,
    clock=0,
    start=True,
    max_steps=1000,
    logging_enabled=None,
    log_func=None,
    logger_name=None,
):
    # Creation validates the spec, builds the dependency-injected CTX,
    # and returns a fully isolated runtime snapshot. Optional logging
    # overrides can be applied without mutating the original spec.
    validated_spec = _auto_register_missing(_deep_copy(spec))

    if (
        logging_enabled is not None
        or log_func is not None
        or logger_name is not None
    ):
        validated_spec = fsm_set_logging(
            target=validated_spec,
            enabled=logging_enabled,
            log_func=log_func,
            logger_name=logger_name,
        )

    fsm_validate_spec(validated_spec, raise_on_error=True)

    ctx = _build_ctx(
        ctx_builder=ctx_builder,
        ctx_seed=ctx_seed,
        spec=validated_spec,
        params={
            "data": _deep_copy(data),
            "clock": int(clock),
            "start": bool(start),
            "max_steps": int(max_steps),
        },
        clock=clock,
    )
    machine = _new_machine(validated_spec, ctx, data=data, clock=clock)

    _fsm_emit_log(
        machine,
        "info",
        "fsm_machine_created",
        fields={
            "fsm_name": validated_spec.get("name"),
            "initial_state": validated_spec.get("initial_state"),
            "clock": int(clock),
            "start": bool(start),
        },
    )

    if start:
        machine = fsm_start(machine, process_queue=True, max_steps=max_steps)

    return machine

def fsm_advance_to(machine, clock, max_steps=1000):
    if clock < machine["runtime"]["clock"]:
        raise ValueError("Target clock must be >= current clock.")

    updated = fsm_process_queue(machine, max_steps=max_steps)
    updated["runtime"]["clock"] = int(clock)
    updated = _append_history(
        updated,
        {
            "kind": "clock_advance",
            "clock": updated["runtime"]["clock"],
            "state": updated["runtime"]["state"],
        },
    )
    updated = fsm_process_queue(updated, max_steps=max_steps)
    return updated

def fsm_advance_by(machine, delta, max_steps=1000):
    if delta < 0:
        raise ValueError("Clock delta must be >= 0.")
    return fsm_advance_to(
        machine,
        clock=machine["runtime"]["clock"] + int(delta),
        max_steps=max_steps,
    )

# ---------------------------------------------------------------------------
#  Introspection
# ---------------------------------------------------------------------------

def fsm_get_state(machine):
    return machine["runtime"]["state"]

def fsm_is_state(machine, state_name):
    return machine["runtime"]["state"] == state_name

def fsm_snapshot(machine):
    return _deep_copy(machine)

def fsm_describe(target):
    if "runtime" in target:
        return {
            "kind": "machine",
            "fsm_name": target["spec"]["name"],
            "state": target["runtime"]["state"],
            "clock": target["runtime"]["clock"],
            "started": target["runtime"]["started"],
            "halted": target["runtime"]["halted"],
            "queue_length": len(target["runtime"]["queue"]),
            "timer_count": len(target["runtime"]["timers"]),
            "history_length": len(target["runtime"]["history"]),
            "stats": _deep_copy(target["runtime"]["counters"]),
        }

    return {
        "kind": "spec",
        "fsm_name": target["name"],
        "initial_state": target["initial_state"],
        "states": sorted(list(target["registry"]["states"].keys())),
        "events": sorted(list(target["registry"]["events"].keys())),
        "actions": sorted(list(target["registry"]["actions"].keys())),
        "timers": sorted(list(target["registry"]["timers"].keys())),
        "transitions": [item["name"] for item in target["registry"]["transitions"]],
        "options": _deep_copy(target.get("options")),
    }

# ---------------------------------------------------------------------------
#  Registry snapshots and registry logging
# ---------------------------------------------------------------------------

def _registry_names():
    return ["states", "transitions", "actions", "events", "timers"]

def _normalize_registry_names(registry_names=None):
    allowed = _registry_names()

    if registry_names is None:
        return list(allowed)

    if isinstance(registry_names, str):
        registry_names = [registry_names]

    normalized = []
    for registry_name in _as_list(registry_names):
        if not isinstance(registry_name, str):
            raise ValueError("Registry names must be strings.")
        name = registry_name.strip()
        if name not in allowed:
            available = ", ".join(allowed)
            raise ValueError(f"Unknown registry '{name}'. Available: {available}")
        normalized.append(name)

    if not normalized:
        return list(allowed)

    return normalized

def _registry_state_items(spec):
    items = []
    for state_name in sorted(spec["registry"]["states"].keys()):
        definition = spec["registry"]["states"][state_name]
        items.append(
            {
                "name": state_name,
                "on_enter": _deep_copy(definition.get("on_enter")),
                "on_exit": _deep_copy(definition.get("on_exit")),
                "timers": _deep_copy(definition.get("timers")),
                "meta": _copy_mapping(definition.get("meta")),
            }
        )
    return items

def _registry_transition_sort_key(definition):
    return (
        int(definition.get("order", 0)),
        definition.get("name", ""),
    )

def _registry_transition_items(spec):
    items = []
    ordered = sorted(
        spec["registry"]["transitions"],
        key=_registry_transition_sort_key,
    )
    for definition in ordered:
        target = definition.get("target")
        if callable(target):
            target = getattr(target, "__name__", "<callable>")
        items.append(
            {
                "name": definition.get("name"),
                "source": _deep_copy(definition.get("source")),
                "event": _deep_copy(definition.get("event")),
                "target": _deep_copy(target),
                "priority": int(definition.get("priority", 0)),
                "internal": bool(definition.get("internal")),
                "actions": _deep_copy(definition.get("actions")),
                "guard": _deep_copy(definition.get("guard")),
                "order": int(definition.get("order", 0)),
                "meta": _copy_mapping(definition.get("meta")),
            }
        )
    return items

def _registry_action_items(spec):
    items = []
    for action_name in sorted(spec["registry"]["actions"].keys()):
        definition = spec["registry"]["actions"][action_name]
        func = definition.get("func")
        items.append(
            {
                "name": action_name,
                "has_func": callable(func),
                "func_name": getattr(func, "__name__", None) if callable(func) else None,
                "meta": _copy_mapping(definition.get("meta")),
            }
        )
    return items

def _registry_event_items(spec):
    items = []
    for event_name in sorted(spec["registry"]["events"].keys()):
        definition = spec["registry"]["events"][event_name]
        normalizer = definition.get("normalizer")
        items.append(
            {
                "name": event_name,
                "has_normalizer": callable(normalizer),
                "normalizer_name": getattr(normalizer, "__name__", None) if callable(normalizer) else None,
                "meta": _copy_mapping(definition.get("meta")),
            }
        )
    return items

def _registry_timer_items(spec):
    items = []
    for timer_name in sorted(spec["registry"]["timers"].keys()):
        definition = spec["registry"]["timers"][timer_name]
        event_definition = _deep_copy(definition.get("event"))
        event_name = None
        if isinstance(event_definition, dict):
            event_name = event_definition.get("name")
        items.append(
            {
                "name": timer_name,
                "event": event_name,
                "delay": definition.get("delay"),
                "interval": definition.get("interval"),
                "repeat": definition.get("repeat"),
                "has_condition": definition.get("condition") is not None,
                "meta": _copy_mapping(definition.get("meta")),
            }
        )
    return items

def _registry_snapshot_items(spec, registry_name):
    dispatch_map = {
        "states": _registry_state_items,
        "transitions": _registry_transition_items,
        "actions": _registry_action_items,
        "events": _registry_event_items,
        "timers": _registry_timer_items,
    }
    return dispatch_map[registry_name](spec)

def fsm_registry_snapshot(target=None, registry_names=None, include_items=True, spec=None, machine=None):
    # Registry snapshots are the read-only basis for diagnostics,
    # register logging, and test assertions. The output is intentionally
    # deterministic and sorted for stable comparisons.
    resolved_target = _resolve_target_argument(target=target, spec=spec, machine=machine)
    resolved_spec = _extract_spec(resolved_target)
    names = _normalize_registry_names(registry_names)

    snapshot = {
        "kind": "registry_snapshot",
        "fsm_name": resolved_spec.get("name"),
        "registries": {},
    }

    for registry_name in names:
        items = _registry_snapshot_items(resolved_spec, registry_name)
        snapshot["registries"][registry_name] = {
            "count": len(items),
        }
        if include_items:
            snapshot["registries"][registry_name]["items"] = items

    return snapshot

def _log_registry_snapshot(target, registry_name, snapshot_entry, log_func=None, level="info", logger_name=None):
    _fsm_emit_log(
        target,
        level,
        "fsm_registry_snapshot",
        fields={
            "fsm_name": _extract_spec(target).get("name"),
            "registry": registry_name,
            "count": snapshot_entry.get("count"),
            "items": _deep_copy(snapshot_entry.get("items")),
        },
        log_func=log_func,
        logger_name=logger_name,
        force=True,
    )

def fsm_log_registry(
    target=None,
    registry_name=None,
    log_func=None,
    level="info",
    include_items=True,
    spec=None,
    machine=None,
    logger_name=None,
):
    resolved_target = _resolve_target_argument(target=target, spec=spec, machine=machine)
    names = _normalize_registry_names(registry_name)
    if len(names) != 1:
        raise ValueError("fsm_log_registry requires exactly one registry_name.")

    snapshot = fsm_registry_snapshot(
        target=resolved_target,
        registry_names=names,
        include_items=include_items,
    )
    selected_name = names[0]
    _log_registry_snapshot(
        resolved_target,
        selected_name,
        snapshot["registries"][selected_name],
        log_func=log_func,
        level=level,
        logger_name=logger_name,
    )
    return snapshot

def fsm_log_registries(
    target=None,
    registry_names=None,
    log_func=None,
    level="info",
    include_items=True,
    spec=None,
    machine=None,
    logger_name=None,
):
    resolved_target = _resolve_target_argument(target=target, spec=spec, machine=machine)
    names = _normalize_registry_names(registry_names)
    snapshot = fsm_registry_snapshot(
        target=resolved_target,
        registry_names=names,
        include_items=include_items,
    )

    for registry_name in names:
        _log_registry_snapshot(
            resolved_target,
            registry_name,
            snapshot["registries"][registry_name],
            log_func=log_func,
            level=level,
            logger_name=logger_name,
        )

    return snapshot

def fsm_log_states(target=None, log_func=None, level="info", include_items=True, spec=None, machine=None, logger_name=None):
    return fsm_log_registry(
        target=target,
        spec=spec,
        machine=machine,
        registry_name="states",
        log_func=log_func,
        level=level,
        include_items=include_items,
        logger_name=logger_name,
    )

def fsm_log_transitions(target=None, log_func=None, level="info", include_items=True, spec=None, machine=None, logger_name=None):
    return fsm_log_registry(
        target=target,
        spec=spec,
        machine=machine,
        registry_name="transitions",
        log_func=log_func,
        level=level,
        include_items=include_items,
        logger_name=logger_name,
    )

def fsm_log_actions(target=None, log_func=None, level="info", include_items=True, spec=None, machine=None, logger_name=None):
    return fsm_log_registry(
        target=target,
        spec=spec,
        machine=machine,
        registry_name="actions",
        log_func=log_func,
        level=level,
        include_items=include_items,
        logger_name=logger_name,
    )

def fsm_log_events(target=None, log_func=None, level="info", include_items=True, spec=None, machine=None, logger_name=None):
    return fsm_log_registry(
        target=target,
        spec=spec,
        machine=machine,
        registry_name="events",
        log_func=log_func,
        level=level,
        include_items=include_items,
        logger_name=logger_name,
    )

def fsm_log_timers(target=None, log_func=None, level="info", include_items=True, spec=None, machine=None, logger_name=None):
    return fsm_log_registry(
        target=target,
        spec=spec,
        machine=machine,
        registry_name="timers",
        log_func=log_func,
        level=level,
        include_items=include_items,
        logger_name=logger_name,
    )

def fsm_set_logging(target=None, enabled=None, log_func=None, logger_name=None, spec=None, machine=None):
    resolved_target = _resolve_target_argument(target=target, spec=spec, machine=machine)
    updated = _deep_copy(resolved_target)

    if "runtime" in updated and "spec" in updated:
        spec_ref = updated["spec"]
    else:
        spec_ref = updated

    options = spec_ref.setdefault("options", {})
    current_logging = _merge_logging_config(_new_logging_config(), options.get("logging"))

    if enabled is not None:
        current_logging["enabled"] = bool(enabled)
    elif log_func is not None:
        current_logging["enabled"] = True

    if log_func is not None:
        current_logging["log_func"] = log_func

    if logger_name is not None:
        current_logging["logger_name"] = _normalize_logger_name(logger_name)

    options["logging"] = current_logging
    return updated

# ---------------------------------------------------------------------------
#  Main interface
# ---------------------------------------------------------------------------

def generic_finite_state_machine(*args, **params):
    # The main entry point accepts either an operation string or a payload dict.
    # Dispatch stays explicit so every public capability remains discoverable,
    # deterministic, and easy to test.
    operation = None
    payload = {}

    if args:
        if len(args) == 1 and isinstance(args[0], dict):
            payload = _copy_mapping(args[0])
        elif len(args) == 1 and isinstance(args[0], str):
            operation = args[0]
        else:
            raise ValueError(
                "Use a single operation string or a single operation payload dict."
            )

    for key, value in params.items():
        payload[key] = value

    if operation is None:
        operation = payload.pop("operation", None)
    if operation is None:
        operation = payload.pop("command", None)
    if operation is None:
        operation = payload.pop("action", None)

    if not isinstance(operation, str) or not operation:
        raise ValueError("generic_finite_state_machine requires an operation string.")

    dispatch_map = {
        "build_spec": fsm_build_spec,
        "register_state": fsm_register_state,
        "register_action": fsm_register_action,
        "register_event": fsm_register_event,
        "register_timer": fsm_register_timer,
        "register_transition": fsm_register_transition,
        "validate": fsm_validate_spec,
        "set_logging": fsm_set_logging,
        "make_log_func": fsm_make_log_func,
        "registry_snapshot": fsm_registry_snapshot,
        "log_registry": fsm_log_registry,
        "log_registries": fsm_log_registries,
        "log_states": fsm_log_states,
        "log_transitions": fsm_log_transitions,
        "log_actions": fsm_log_actions,
        "log_events": fsm_log_events,
        "log_timers": fsm_log_timers,
        "create": fsm_create,
        "start": fsm_start,
        "enqueue": fsm_enqueue,
        "dispatch": fsm_dispatch,
        "dispatch_many": fsm_dispatch_many,
        "process_queue": fsm_process_queue,
        "advance_to": fsm_advance_to,
        "advance_by": fsm_advance_by,
        "schedule_timer": fsm_schedule_timer,
        "cancel_timer": fsm_cancel_timer,
        "get_state": fsm_get_state,
        "is_state": fsm_is_state,
        "snapshot": fsm_snapshot,
        "describe": fsm_describe,
        "selftest": _selftest,
    }

    if operation not in dispatch_map:
        available = ", ".join(sorted(dispatch_map.keys()))
        raise ValueError(f"Unknown FSM operation '{operation}'. Available: {available}")

    return dispatch_map[operation](**payload)

# ---------------------------------------------------------------------------
#  Demo / Example
# ---------------------------------------------------------------------------

def _demo_build_ctx(seed=None, spec=None, params=None):
    ctx = {
        "target_cycles": 3,
        "line_name": "Mixer-Line-01",
        "seed": _deep_copy(seed),
        "fsm_name": spec["name"] if isinstance(spec, dict) else "unknown",
        "params": _copy_mapping(params),
    }
    return ctx

def _demo_trace(message, ctx=None, data=None, state_name=None, event=None):
    entry = {
        "message": message,
        "line": ctx.get("line_name") if isinstance(ctx, dict) else None,
        "state": state_name,
        "event": event.get("name") if isinstance(event, dict) else None,
        "cycles": data.get("cycles") if isinstance(data, dict) else None,
    }
    return fsm_result(trace=[entry])

def _demo_action_idle_enter(ctx=None, data=None, state_name=None):
    next_data = {
        "status": "idle",
        "cycles": 0,
        "last_state": state_name,
    }
    return fsm_result(
        data_patch=next_data,
        trace=[{"message": "Entered idle", "line": ctx.get("line_name")}],
    )

def _demo_action_schedule_warmup():
    return fsm_result(
        schedule=[fsm_schedule_request(timer="warmup_timer", key="warmup")],
        trace=[{"message": "Warmup timer scheduled"}],
    )

def _demo_action_running_enter(ctx=None):
    return fsm_result(
        data_patch={"status": "running"},
        trace=[{"message": "Production started", "target_cycles": ctx.get("target_cycles")}],
    )

def _demo_action_increment_cycles(ctx=None, data=None):
    current_cycles = int(data.get("cycles", 0)) + 1
    result = fsm_result(
        data_patch={"cycles": current_cycles, "status": "running"},
        trace=[{"message": "Cycle pulse processed", "cycles": current_cycles}],
    )

    if current_cycles >= int(ctx.get("target_cycles", 0)):
        result["emit"] = [fsm_make_event("finished", {"cycles": current_cycles})]

    return result

def _demo_action_completed_enter(data=None):
    return fsm_result(
        data_patch={"status": "completed", "completed_cycles": data.get("cycles", 0)},
        trace=[{"message": "Batch completed", "cycles": data.get("cycles", 0)}],
        halt=True,
    )

def _demo_action_fault_enter(event=None):
    return fsm_result(
        data_patch={"status": "fault", "fault_event": event.get("name") if isinstance(event, dict) else None},
        trace=[{"message": "Fault state entered"}],
    )

def _demo_normalize_operator_event(event=None):
    if event is None:
        return None

    if event.get("name") != "start":
        return event

    payload = _copy_mapping(event.get("payload"))
    if "operator" not in payload:
        payload["operator"] = "unknown"
    return {"payload": payload}

def _build_demo_spec():
    return generic_finite_state_machine(
        operation="build_spec",
        name="production_fsm",
        initial_state="idle",
        strict=True,
        auto_register=False,
        allow_unhandled_events=True,
        states=[
            fsm_state("idle", on_enter=["idle_enter"]),
            fsm_state("warming", on_enter=[partial(_demo_trace, "Warmup phase active")]),
            fsm_state(
                "running",
                on_enter=["running_enter"],
                timers=[
                    fsm_schedule_request(
                        timer="pulse_timer",
                        key="pulse",
                        state_scoped=True,
                    )
                ],
            ),
            fsm_state("completed", on_enter=["completed_enter"]),
            fsm_state("fault", on_enter=["fault_enter"]),
        ],
        actions=[
            fsm_action("idle_enter", _demo_action_idle_enter),
            fsm_action("schedule_warmup", _demo_action_schedule_warmup),
            fsm_action("running_enter", _demo_action_running_enter),
            fsm_action("increment_cycles", _demo_action_increment_cycles),
            fsm_action("completed_enter", _demo_action_completed_enter),
            fsm_action("fault_enter", _demo_action_fault_enter),
        ],
        events=[
            fsm_event("start", normalizer=_demo_normalize_operator_event),
            fsm_event("warmup_done"),
            fsm_event("pulse"),
            fsm_event("finished"),
            fsm_event("fail"),
            fsm_event("reset"),
        ],
        timers=[
            fsm_timer(
                "warmup_timer",
                event="warmup_done",
                delay=2,
                repeat=1,
            ),
            fsm_timer(
                "pulse_timer",
                event="pulse",
                delay=1,
                interval=1,
                repeat=None,
            ),
        ],
        transitions=[
            fsm_transition(
                "idle_to_warming",
                source="idle",
                event="start",
                target="warming",
                actions=["schedule_warmup", partial(_demo_trace, "Start command accepted")],
                priority=100,
            ),
            fsm_transition(
                "warming_to_running",
                source="warming",
                event="warmup_done",
                target="running",
                priority=100,
            ),
            fsm_transition(
                "running_cycle",
                source="running",
                event="pulse",
                target=None,
                actions=["increment_cycles"],
                internal=True,
                priority=100,
            ),
            fsm_transition(
                "running_to_completed",
                source="running",
                event="finished",
                target="completed",
                priority=200,
            ),
            fsm_transition(
                "any_to_fault",
                source="*",
                event="fail",
                target="fault",
                priority=1000,
            ),
            fsm_transition(
                "fault_to_idle",
                source="fault",
                event="reset",
                target="idle",
                priority=100,
            ),
        ],
    )

def _run_demo():
    spec = _build_demo_spec()
    machine = generic_finite_state_machine(
        operation="create",
        spec=spec,
        ctx_builder=_demo_build_ctx,
        ctx_seed={"requested_by": "demo"},
        data={"cycles": 0},
        clock=0,
        start=True,
        max_steps=1000,
    )

    machine = generic_finite_state_machine(
        operation="dispatch",
        machine=machine,
        event=fsm_make_event("start", {"operator": "Philipp"}),
        process_queue=True,
        max_steps=1000,
    )

    machine = generic_finite_state_machine(
        operation="advance_by",
        machine=machine,
        delta=2,
        max_steps=1000,
    )

    machine = generic_finite_state_machine(
        operation="advance_by",
        machine=machine,
        delta=1,
        max_steps=1000,
    )

    machine = generic_finite_state_machine(
        operation="advance_by",
        machine=machine,
        delta=1,
        max_steps=1000,
    )

    machine = generic_finite_state_machine(
        operation="advance_by",
        machine=machine,
        delta=1,
        max_steps=1000,
    )

    print("")
    print("=== FSM SPEC ===")
    pprint(fsm_describe(spec))

    print("")
    print("=== FSM MACHINE SUMMARY ===")
    pprint(fsm_describe(machine))

    print("")
    print("=== FSM DATA ===")
    pprint(machine["runtime"]["data"])

    print("")
    print("=== FSM TRACE ===")
    pprint(machine["runtime"]["trace"])

    print("")
    print("=== FSM HISTORY (last 12 entries) ===")
    pprint(machine["runtime"]["history"][-12:])

# ---------------------------------------------------------------------------
#  Selftesting
# ---------------------------------------------------------------------------

def _selftest_collect_log(level=None, message=None, fields=None, sink=None):
    if sink is None:
        return None
    sink.append(
        {
            "level": level,
            "message": message,
            "fields": _copy_mapping(fields),
        }
    )
    return None

def _selftest():
    # The selftest validates the public contract without relying on
    # external frameworks. It covers registry composition, validation,
    # logging, timers, transitions, and the complete demo scenario.
    collected_logs = []
    log_func = partial(_selftest_collect_log, sink=collected_logs)

    minimal_spec = fsm_build_spec(
        name="selftest_minimal",
        initial_state="idle",
        states=[fsm_state("idle"), fsm_state("done")],
        strict=True,
    )
    assert fsm_describe(minimal_spec)["states"] == ["done", "idle"]

    minimal_spec = fsm_register_event(minimal_spec, fsm_event("go"))
    minimal_spec = fsm_register_action(
        minimal_spec,
        fsm_action("mark_done", partial(fsm_result, data_patch={"marked": True})),
    )
    minimal_spec = fsm_register_timer(
        minimal_spec,
        fsm_timer("go_timer", event="go", delay=1, repeat=1),
    )
    minimal_spec = fsm_register_transition(
        minimal_spec,
        fsm_transition(
            "idle_to_done",
            source="idle",
            event="go",
            target="done",
            actions=["mark_done"],
            priority=10,
        ),
    )
    minimal_report = fsm_validate_spec(minimal_spec, raise_on_error=False)
    assert minimal_report["ok"] is True

    minimal_machine = fsm_create(
        spec=minimal_spec,
        data={"marked": False},
        start=True,
        max_steps=1000,
    )
    minimal_machine = fsm_dispatch(
        minimal_machine,
        "go",
        process_queue=True,
        max_steps=1000,
    )
    assert fsm_get_state(minimal_machine) == "done"
    assert minimal_machine["runtime"]["data"]["marked"] is True

    demo_spec = _build_demo_spec()
    demo_spec = fsm_set_logging(
        target=demo_spec,
        enabled=True,
        log_func=log_func,
        logger_name="generic_fsm.selftest",
    )

    demo_report = fsm_validate_spec(demo_spec, raise_on_error=False)
    assert demo_report["ok"] is True

    registry_snapshot = fsm_registry_snapshot(demo_spec)
    assert registry_snapshot["registries"]["states"]["count"] == 5
    assert registry_snapshot["registries"]["transitions"]["count"] == 6
    assert registry_snapshot["registries"]["actions"]["count"] == 6
    assert registry_snapshot["registries"]["events"]["count"] == 6
    assert registry_snapshot["registries"]["timers"]["count"] == 2

    fsm_log_states(demo_spec, log_func=log_func)
    fsm_log_registries(demo_spec, log_func=log_func)

    machine = fsm_create(
        spec=demo_spec,
        ctx_builder=_demo_build_ctx,
        ctx_seed={"requested_by": "selftest"},
        data={"cycles": 0},
        clock=0,
        start=True,
        max_steps=1000,
    )
    assert fsm_get_state(machine) == "idle"

    machine = fsm_dispatch(
        machine,
        fsm_make_event("start", {"operator": "selftest"}),
        process_queue=True,
        max_steps=1000,
    )
    machine = fsm_advance_by(machine, 2, max_steps=1000)
    machine = fsm_advance_by(machine, 1, max_steps=1000)
    machine = fsm_advance_by(machine, 1, max_steps=1000)
    machine = fsm_advance_by(machine, 1, max_steps=1000)

    assert fsm_get_state(machine) == "completed"
    assert machine["runtime"]["halted"] is True
    assert machine["runtime"]["data"]["completed_cycles"] == 3
    assert machine["runtime"]["counters"]["transitions_taken"] >= 4
    assert len(collected_logs) >= 6

    return {
        "ok": True,
        "checks": 12,
        "final_state": fsm_get_state(machine),
        "completed_cycles": machine["runtime"]["data"]["completed_cycles"],
        "log_entries": len(collected_logs),
        "registry_counts": {
            name: entry["count"]
            for name, entry in registry_snapshot["registries"].items()
        },
    }

if __name__ == "__main__":
    print("")
    print("=== FSM SELFTEST ===")
    pprint(_selftest())

    print("")
    print("=== FSM DEMO ===")
    _run_demo()
