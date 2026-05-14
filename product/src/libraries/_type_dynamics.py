# -*- coding: utf-8 -*-

# src/libraries/_type_dynamics.py


"""
=============================================================================
GENERISCHE FUNKTIONALE TYP-BIBLIOTHEK
=============================================================================
Vollständiger, klassenloser Ersatz für _type_notations_pydantic.py.

Architektur:
  - Kein OOP, keine Klassen, keine Decoratoren
  - Alle "Typen" sind Python-Dicts (Schema- und Record-Dicts)
  - Enums sind Dicts mit frozenset-Wertmengen
  - Validierung ist vollständig funktional
  - functools.partial() statt lambda
  - Aus jedem Modul importierbar

Verwendung:
  from _type_notations_functional import (
      make_device, DEVICE_SCHEMA, validate, validate_strict,
      DEVICE_TYPE_ENUM, is_device_type, run_all_validations
  )

=============================================================================
"""

from functools import partial, update_wrapper
from inspect import signature, Parameter, isawaitable, iscoroutinefunction
from collections.abc import Mapping, Sequence, Set
from types import GeneratorType


__all__ = [
    "any_value",
    "nothing",
    "kind",
    "exact",
    "literal",
    "predicate",
    "optional",
    "one_of",
    "all_of",
    "sequence_of",
    "list_of",
    "tuple_of",
    "mapping_of",
    "set_of",
    "shape",
    "interface",
    "callable_with",
    "awaitable",
    "generator",
    "numeric",
    "text",
    "binary",
    "field",
    "optional_field",
    "required_field",
    "from_sample",
    "infer",
    "merge_specs",
    "describe",
    "check",
    "matches",
    "explain",
    "validate",
    "contract",
    "dispatch",
]


_MISSING = object()


def _is_internal_spec(value):
    return isinstance(value, dict) and value.get("__spec__") is True and "kind" in value


def _is_field_wrapper(value):
    return isinstance(value, dict) and value.get("__field__") is True and "spec" in value


def _make_spec(kind_name, **payload):
    data = {
        "__spec__": True,
        "kind": kind_name,
    }
    data.update(payload)
    return data


def _is_plain_dict(value):
    return isinstance(value, dict)


def _is_mapping(value):
    return isinstance(value, Mapping)


def _is_sequence_but_not_text(value):
    if isinstance(value, (str, bytes, bytearray, memoryview)):
        return False
    return isinstance(value, Sequence)


def _is_set_like(value):
    if isinstance(value, (str, bytes, bytearray, memoryview)):
        return False
    return isinstance(value, Set)


def _is_bool(value):
    return isinstance(value, bool)


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_float(value):
    return isinstance(value, float)


def _is_complex(value):
    return isinstance(value, complex)


def _is_str(value):
    return isinstance(value, str)


def _is_binary(value):
    return isinstance(value, (bytes, bytearray, memoryview))


def _is_generator(value):
    return isinstance(value, GeneratorType)


def _safe_call(fn, *args, **kwargs):
    try:
        return True, fn(*args, **kwargs)
    except Exception as exc:
        return False, exc


def _safe_hasattr(obj, name):
    try:
        return hasattr(obj, name)
    except Exception:
        return False


def _safe_getattr(obj, name, default=_MISSING):
    try:
        return getattr(obj, name)
    except Exception:
        if default is _MISSING:
            raise
        return default


def _callable_name(fn):
    return getattr(fn, "__name__", repr(fn))


def _path_key(path, key):
    return path + "[" + repr(key) + "]"


def _path_attr(path, name):
    return path + "." + str(name)


def _dispatch_registry_sort_key(item):
    return (-item["priority"], item["order"])


def _dedupe_strings(values):
    seen = set()
    result = []
    for item in values:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def field(spec, optional=False):
    return {
        "__field__": True,
        "optional": bool(optional),
        "spec": spec,
    }


def optional_field(spec):
    return field(spec, optional=True)


def required_field(spec):
    return field(spec, optional=False)


def any_value():
    return _make_spec("any")


def nothing():
    return _make_spec("nothing")


def kind(py_type):
    if not isinstance(py_type, type):
        raise TypeError("kind(...) erwartet einen Python-Typ")
    return _make_spec("kind", type=py_type)


def exact(value):
    return _make_spec("exact", value=value)


def literal(value):
    return exact(value)


def predicate(fn, name=None):
    if not callable(fn):
        raise TypeError("predicate(...) erwartet eine callable")
    return _make_spec("predicate", fn=fn, name=name or _callable_name(fn))


def optional(spec):
    return one_of(exact(None), spec)


def one_of(*specs):
    normalized = []
    for spec in specs:
        item = _normalize_spec(spec)
        if item["kind"] == "one_of":
            normalized.extend(item["options"])
        else:
            normalized.append(item)

    normalized = _dedupe_specs(normalized)

    if not normalized:
        return nothing()

    if len(normalized) == 1:
        return normalized[0]

    return _make_spec("one_of", options=tuple(normalized))


def all_of(*specs):
    normalized = []
    for spec in specs:
        item = _normalize_spec(spec)
        if item["kind"] == "all_of":
            normalized.extend(item["parts"])
        else:
            normalized.append(item)

    normalized = _dedupe_specs(normalized)

    if not normalized:
        return any_value()

    if len(normalized) == 1:
        return normalized[0]

    return _make_spec("all_of", parts=tuple(normalized))


def sequence_of(item_spec, min_length=0, max_length=None):
    if min_length < 0:
        raise ValueError("min_length darf nicht negativ sein")
    if max_length is not None and max_length < min_length:
        raise ValueError("max_length darf nicht kleiner als min_length sein")

    return _make_spec(
        "sequence_of",
        item=_normalize_spec(item_spec),
        min_length=int(min_length),
        max_length=max_length,
    )


def list_of(item_spec, min_length=0, max_length=None):
    if min_length < 0:
        raise ValueError("min_length darf nicht negativ sein")
    if max_length is not None and max_length < min_length:
        raise ValueError("max_length darf nicht kleiner als min_length sein")

    return _make_spec(
        "list_of",
        item=_normalize_spec(item_spec),
        min_length=int(min_length),
        max_length=max_length,
    )


def tuple_of(*item_specs):
    normalized = []
    for spec in item_specs:
        normalized.append(_normalize_spec(spec))
    return _make_spec("tuple_of", items=tuple(normalized))


def mapping_of(key_spec, value_spec, min_length=0, max_length=None):
    if min_length < 0:
        raise ValueError("min_length darf nicht negativ sein")
    if max_length is not None and max_length < min_length:
        raise ValueError("max_length darf nicht kleiner als min_length sein")

    return _make_spec(
        "mapping_of",
        key=_normalize_spec(key_spec),
        value=_normalize_spec(value_spec),
        min_length=int(min_length),
        max_length=max_length,
    )


def set_of(item_spec, min_length=0, max_length=None):
    if min_length < 0:
        raise ValueError("min_length darf nicht negativ sein")
    if max_length is not None and max_length < min_length:
        raise ValueError("max_length darf nicht kleiner als min_length sein")

    return _make_spec(
        "set_of",
        item=_normalize_spec(item_spec),
        min_length=int(min_length),
        max_length=max_length,
    )


def shape(required=None, optional_fields=None, allow_extra=False):
    required = required or {}
    optional_fields = optional_fields or {}

    required_keys = set(required.keys())
    optional_keys = set(optional_fields.keys())

    collisions = required_keys & optional_keys
    if collisions:
        names = ", ".join(sorted(repr(item) for item in collisions))
        raise ValueError("Shape enthält doppelte Felder in required/optional: " + names)

    normalized_required, promoted_optional = _normalize_shape_mapping(required, force_optional=False)
    normalized_optional, _ = _normalize_shape_mapping(optional_fields, force_optional=True)

    for key, spec in promoted_optional.items():
        normalized_optional[key] = spec
        if key in normalized_required:
            del normalized_required[key]

    collisions_after = set(normalized_required.keys()) & set(normalized_optional.keys())
    if collisions_after:
        names = ", ".join(sorted(repr(item) for item in collisions_after))
        raise ValueError("Shape enthält widersprüchliche Felddefinitionen: " + names)

    return _make_spec(
        "shape",
        required=normalized_required,
        optional=normalized_optional,
        allow_extra=bool(allow_extra),
    )


def interface(attributes=None, methods=None, predicates=None):
    attributes = attributes or {}
    methods = methods or {}
    predicates = predicates or []

    normalized_attributes = {}
    normalized_methods = {}
    normalized_predicates = []

    for name, spec in attributes.items():
        normalized_attributes[name] = _normalize_spec(spec)

    for name, spec in methods.items():
        normalized_methods[name] = _normalize_method_spec(spec)

    for fn in predicates:
        if _is_internal_spec(fn):
            normalized_predicates.append(_normalize_spec(fn))
        else:
            normalized_predicates.append(predicate(fn))

    return _make_spec(
        "interface",
        attributes=normalized_attributes,
        methods=normalized_methods,
        predicates=tuple(normalized_predicates),
    )


def callable_with(arity=None, min_arity=None, max_arity=None):
    if arity is not None:
        if not isinstance(arity, int) or isinstance(arity, bool) or arity < 0:
            raise TypeError("arity muss ein Integer >= 0 sein")
        if min_arity is not None or max_arity is not None:
            raise ValueError("arity darf nicht zusammen mit min_arity/max_arity verwendet werden")

    if min_arity is not None:
        if not isinstance(min_arity, int) or isinstance(min_arity, bool) or min_arity < 0:
            raise TypeError("min_arity muss ein Integer >= 0 sein")

    if max_arity is not None:
        if not isinstance(max_arity, int) or isinstance(max_arity, bool) or max_arity < 0:
            raise TypeError("max_arity muss ein Integer >= 0 sein")

    if min_arity is not None and max_arity is not None and max_arity < min_arity:
        raise ValueError("max_arity darf nicht kleiner als min_arity sein")

    return _make_spec(
        "callable_with",
        arity=arity,
        min_arity=min_arity,
        max_arity=max_arity,
    )


def awaitable():
    return _make_spec("awaitable")


def generator():
    return _make_spec("generator")


def numeric():
    return one_of(int, float, complex)


def text():
    return kind(str)


def binary():
    return one_of(bytes, bytearray, memoryview)


def from_sample(sample, dict_mode="shape", track_lengths=False):
    return infer(sample, dict_mode=dict_mode, track_lengths=track_lengths)


def infer(value, dict_mode="shape", track_lengths=False):
    if dict_mode not in ("shape", "mapping"):
        raise ValueError("dict_mode muss 'shape' oder 'mapping' sein")

    seen = set()
    return _infer_value(value, dict_mode=dict_mode, track_lengths=bool(track_lengths), seen=seen)


def merge_specs(*specs):
    if len(specs) == 1 and isinstance(specs[0], (list, tuple, set)):
        values = list(specs[0])
    else:
        values = list(specs)
    return _merge_specs(values)


def describe(spec):
    spec = _normalize_spec(spec)
    kind_name = spec["kind"]

    if kind_name == "any":
        return "any"

    if kind_name == "nothing":
        return "nothing"

    if kind_name == "kind":
        return spec["type"].__name__

    if kind_name == "exact":
        return "exact(" + repr(spec["value"]) + ")"

    if kind_name == "predicate":
        return "predicate(" + spec["name"] + ")"

    if kind_name == "one_of":
        parts = []
        for item in spec["options"]:
            parts.append(describe(item))
        return "one_of(" + ", ".join(parts) + ")"

    if kind_name == "all_of":
        parts = []
        for item in spec["parts"]:
            parts.append(describe(item))
        return "all_of(" + ", ".join(parts) + ")"

    if kind_name == "sequence_of":
        return (
            "sequence_of("
            + describe(spec["item"])
            + ", min_length="
            + str(spec["min_length"])
            + ", max_length="
            + repr(spec["max_length"])
            + ")"
        )

    if kind_name == "list_of":
        return (
            "list_of("
            + describe(spec["item"])
            + ", min_length="
            + str(spec["min_length"])
            + ", max_length="
            + repr(spec["max_length"])
            + ")"
        )

    if kind_name == "tuple_of":
        parts = []
        for item in spec["items"]:
            parts.append(describe(item))
        return "tuple_of(" + ", ".join(parts) + ")"

    if kind_name == "mapping_of":
        return (
            "mapping_of("
            + describe(spec["key"])
            + ", "
            + describe(spec["value"])
            + ", min_length="
            + str(spec["min_length"])
            + ", max_length="
            + repr(spec["max_length"])
            + ")"
        )

    if kind_name == "set_of":
        return (
            "set_of("
            + describe(spec["item"])
            + ", min_length="
            + str(spec["min_length"])
            + ", max_length="
            + repr(spec["max_length"])
            + ")"
        )

    if kind_name == "shape":
        req_parts = []
        opt_parts = []

        for key in sorted(spec["required"].keys(), key=repr):
            req_parts.append(repr(key) + ": " + describe(spec["required"][key]))

        for key in sorted(spec["optional"].keys(), key=repr):
            opt_parts.append(repr(key) + ": " + describe(spec["optional"][key]))

        return (
            "shape(required={"
            + ", ".join(req_parts)
            + "}, optional_fields={"
            + ", ".join(opt_parts)
            + "}, allow_extra="
            + str(spec["allow_extra"])
            + ")"
        )

    if kind_name == "interface":
        attr_parts = []
        method_parts = []
        pred_parts = []

        for key in sorted(spec["attributes"].keys(), key=repr):
            attr_parts.append(repr(key) + ": " + describe(spec["attributes"][key]))

        for key in sorted(spec["methods"].keys(), key=repr):
            method_parts.append(repr(key) + ": " + describe(spec["methods"][key]))

        for item in spec["predicates"]:
            pred_parts.append(describe(item))

        return (
            "interface(attributes={"
            + ", ".join(attr_parts)
            + "}, methods={"
            + ", ".join(method_parts)
            + "}, predicates=["
            + ", ".join(pred_parts)
            + "])"
        )

    if kind_name == "callable_with":
        return (
            "callable_with(arity="
            + repr(spec["arity"])
            + ", min_arity="
            + repr(spec["min_arity"])
            + ", max_arity="
            + repr(spec["max_arity"])
            + ")"
        )

    if kind_name == "awaitable":
        return "awaitable()"

    if kind_name == "generator":
        return "generator()"

    if kind_name == "object":
        return spec["module"] + "." + spec["name"]

    return repr(spec)


def check(spec, value):
    compiled = _normalize_spec(spec)
    return _check_spec(compiled, value)


def matches(spec):
    compiled = _normalize_spec(spec)
    return partial(check, compiled)


def explain(spec, value, path="value"):
    compiled = _normalize_spec(spec)
    ok, reasons = _explain_spec(compiled, value, path)
    return ok, _dedupe_strings(reasons)


def validate(spec, value, path="value"):
    ok, reasons = explain(spec, value, path=path)
    if not ok:
        raise TypeError("; ".join(reasons))
    return value


def contract(fn, args=None, result=None):
    if not callable(fn):
        raise TypeError("contract(...) erwartet eine callable")

    args = args or {}

    normalized_args = {}
    for name, spec in args.items():
        normalized_args[name] = _normalize_spec(spec)

    normalized_result = None
    if result is not None:
        normalized_result = _normalize_spec(result)

    sig = signature(fn)

    if iscoroutinefunction(fn):
        async def wrapped(*call_args, **call_kwargs):
            bound = sig.bind(*call_args, **call_kwargs)
            bound.apply_defaults()

            for name, spec in normalized_args.items():
                if name not in bound.arguments:
                    raise TypeError("Fehlendes Vertragsargument: " + name)
                validate(spec, bound.arguments[name], path="arg:" + name)

            output = await fn(*call_args, **call_kwargs)

            if normalized_result is not None:
                validate(normalized_result, output, path="return")

            return output

        update_wrapper(wrapped, fn)
        wrapped.__contract__ = {
            "args": normalized_args,
            "result": normalized_result,
        }
        return wrapped

    def wrapped(*call_args, **call_kwargs):
        bound = sig.bind(*call_args, **call_kwargs)
        bound.apply_defaults()

        for name, spec in normalized_args.items():
            if name not in bound.arguments:
                raise TypeError("Fehlendes Vertragsargument: " + name)
            validate(spec, bound.arguments[name], path="arg:" + name)

        output = fn(*call_args, **call_kwargs)

        if normalized_result is not None:
            validate(normalized_result, output, path="return")

        return output

    update_wrapper(wrapped, fn)
    wrapped.__contract__ = {
        "args": normalized_args,
        "result": normalized_result,
    }
    return wrapped


def dispatch():
    registry = []
    state = {
        "counter": 0,
        "default": None,
    }

    def register(spec, fn, name=None, priority=0):
        if not callable(fn):
            raise TypeError("dispatch.register(...) erwartet eine callable")

        entry = {
            "spec": _normalize_spec(spec),
            "fn": fn,
            "name": name or _callable_name(fn),
            "priority": int(priority),
            "order": state["counter"],
        }

        state["counter"] += 1
        registry.append(entry)
        registry.sort(key=_dispatch_registry_sort_key)
        return fn

    def register_default(fn, name=None):
        if not callable(fn):
            raise TypeError("dispatch.register_default(...) erwartet eine callable")

        state["default"] = {
            "fn": fn,
            "name": name or _callable_name(fn),
        }
        return fn

    def invoke(value, *args, **kwargs):
        for entry in registry:
            if check(entry["spec"], value):
                return entry["fn"](value, *args, **kwargs)

        if state["default"] is not None:
            return state["default"]["fn"](value, *args, **kwargs)

        raise LookupError("Kein passender Dispatch-Fall für " + repr(value))

    invoke.register = register
    invoke.register_default = register_default
    invoke.registry = registry
    return invoke


def _normalize_method_spec(spec):
    if isinstance(spec, int) and not isinstance(spec, bool) and spec >= 0:
        return callable_with(min_arity=spec)
    return _normalize_spec(spec)


def _normalize_shape_mapping(mapping, force_optional=False):
    normalized_required = {}
    normalized_optional = {}

    for key, raw_spec in mapping.items():
        if _is_field_wrapper(raw_spec):
            spec = _normalize_spec(raw_spec["spec"])
            if force_optional or raw_spec["optional"]:
                normalized_optional[key] = spec
            else:
                normalized_required[key] = spec
        else:
            spec = _normalize_spec(raw_spec)
            if force_optional:
                normalized_optional[key] = spec
            else:
                normalized_required[key] = spec

    return normalized_required, normalized_optional


def _normalize_plain_shape(mapping):
    required = {}
    optional_fields = {}

    for key, raw_spec in mapping.items():
        if _is_field_wrapper(raw_spec):
            spec = _normalize_spec(raw_spec["spec"])
            if raw_spec["optional"]:
                optional_fields[key] = spec
            else:
                required[key] = spec
        else:
            required[key] = _normalize_spec(raw_spec)

    return shape(required=required, optional_fields=optional_fields, allow_extra=False)


def _normalize_spec(spec):
    if _is_internal_spec(spec):
        return spec

    if _is_field_wrapper(spec):
        return _normalize_spec(spec["spec"])

    if isinstance(spec, type):
        return kind(spec)

    if isinstance(spec, tuple):
        return tuple_of(*spec)

    if isinstance(spec, list):
        if len(spec) == 0:
            return list_of(any_value())
        if len(spec) == 1:
            return list_of(spec[0])
        return list_of(one_of(*spec))

    if isinstance(spec, set):
        if len(spec) == 0:
            return set_of(any_value())
        if len(spec) == 1:
            item = next(iter(spec))
            return set_of(item)
        return set_of(one_of(*list(spec)))

    if isinstance(spec, dict):
        return _normalize_plain_shape(spec)

    if callable(spec):
        return predicate(spec, name=_callable_name(spec))

    raise TypeError("Ungültige Spezifikation: " + repr(spec))


def _infer_value(value, dict_mode, track_lengths, seen):
    if value is None:
        return exact(None)

    if _is_bool(value):
        return kind(bool)

    if _is_int(value):
        return kind(int)

    if _is_float(value):
        return kind(float)

    if _is_complex(value):
        return kind(complex)

    if _is_str(value):
        return kind(str)

    if _is_binary(value):
        return one_of(bytes, bytearray, memoryview)

    if isawaitable(value):
        return awaitable()

    if _is_generator(value):
        return generator()

    obj_id = id(value)
    if _is_mapping(value) or isinstance(value, (list, tuple)) or _is_set_like(value):
        if obj_id in seen:
            return any_value()

    if _is_mapping(value):
        seen.add(obj_id)
        try:
            if dict_mode == "shape":
                required = {}
                for key, item in value.items():
                    required[key] = _infer_value(
                        item,
                        dict_mode=dict_mode,
                        track_lengths=track_lengths,
                        seen=seen,
                    )
                return shape(required=required, optional_fields={}, allow_extra=False)

            key_specs = []
            value_specs = []

            for key, item in value.items():
                key_specs.append(_infer_value(
                    key,
                    dict_mode=dict_mode,
                    track_lengths=track_lengths,
                    seen=seen,
                ))
                value_specs.append(_infer_value(
                    item,
                    dict_mode=dict_mode,
                    track_lengths=track_lengths,
                    seen=seen,
                ))

            key_spec = _merge_specs(key_specs) if key_specs else any_value()
            value_spec = _merge_specs(value_specs) if value_specs else any_value()

            min_length = len(value) if track_lengths else 0
            max_length = len(value) if track_lengths else None

            return mapping_of(
                key_spec,
                value_spec,
                min_length=min_length,
                max_length=max_length,
            )
        finally:
            seen.remove(obj_id)

    if isinstance(value, list):
        seen.add(obj_id)
        try:
            item_specs = []
            for item in value:
                item_specs.append(_infer_value(
                    item,
                    dict_mode=dict_mode,
                    track_lengths=track_lengths,
                    seen=seen,
                ))

            item_spec = _merge_specs(item_specs) if item_specs else any_value()

            min_length = len(value) if track_lengths else 0
            max_length = len(value) if track_lengths else None

            return list_of(item_spec, min_length=min_length, max_length=max_length)
        finally:
            seen.remove(obj_id)

    if isinstance(value, tuple):
        seen.add(obj_id)
        try:
            items = []
            for item in value:
                items.append(_infer_value(
                    item,
                    dict_mode=dict_mode,
                    track_lengths=track_lengths,
                    seen=seen,
                ))
            return tuple_of(*items)
        finally:
            seen.remove(obj_id)

    if _is_set_like(value):
        seen.add(obj_id)
        try:
            item_specs = []
            for item in value:
                item_specs.append(_infer_value(
                    item,
                    dict_mode=dict_mode,
                    track_lengths=track_lengths,
                    seen=seen,
                ))

            item_spec = _merge_specs(item_specs) if item_specs else any_value()

            min_length = len(value) if track_lengths else 0
            max_length = len(value) if track_lengths else None

            return set_of(item_spec, min_length=min_length, max_length=max_length)
        finally:
            seen.remove(obj_id)

    return _make_spec(
        "object",
        type=type(value),
        module=getattr(type(value), "__module__", ""),
        name=getattr(type(value), "__name__", type(value).__class__.__name__),
    )


def _dedupe_specs(specs):
    seen = set()
    result = []
    for spec in specs:
        key = _spec_key(spec)
        if key not in seen:
            seen.add(key)
            result.append(spec)
    return result


def _spec_key(spec):
    return describe(spec)


def _merge_specs(specs):
    normalized = []
    for spec in specs:
        item = _normalize_spec(spec)
        if item["kind"] == "one_of":
            normalized.extend(item["options"])
        else:
            normalized.append(item)

    normalized = _dedupe_specs(normalized)

    if not normalized:
        return any_value()

    if len(normalized) == 1:
        return normalized[0]

    kind_names = {item["kind"] for item in normalized}
    if len(kind_names) != 1:
        return one_of(*normalized)

    kind_name = normalized[0]["kind"]

    if kind_name == "shape":
        return _merge_shape_specs(normalized)

    if kind_name == "list_of":
        return _merge_list_specs(normalized)

    if kind_name == "set_of":
        return _merge_set_specs(normalized)

    if kind_name == "sequence_of":
        return _merge_sequence_specs(normalized)

    if kind_name == "mapping_of":
        return _merge_mapping_specs(normalized)

    if kind_name == "tuple_of":
        return _merge_tuple_specs(normalized)

    if kind_name == "kind":
        types_ = {item["type"] for item in normalized}
        if len(types_) == 1:
            return normalized[0]
        return one_of(*normalized)

    if kind_name == "exact":
        values = [item["value"] for item in normalized]
        if len(values) == 1:
            return normalized[0]
        return one_of(*normalized)

    if kind_name == "object":
        types_ = {item["type"] for item in normalized}
        if len(types_) == 1:
            return normalized[0]
        return one_of(*normalized)

    return one_of(*normalized)


def _merge_shape_specs(specs):
    present_count = {}
    child_specs = {}
    required_everywhere = {}

    for spec in specs:
        current_keys = set(spec["required"].keys()) | set(spec["optional"].keys())

        for key in current_keys:
            present_count[key] = present_count.get(key, 0) + 1

        for key, child in spec["required"].items():
            child_specs.setdefault(key, []).append(child)

        for key, child in spec["optional"].items():
            child_specs.setdefault(key, []).append(child)

    for key in child_specs.keys():
        required_everywhere[key] = True
        for spec in specs:
            if key not in spec["required"]:
                required_everywhere[key] = False
                break

    required = {}
    optional_fields = {}

    for key in sorted(child_specs.keys(), key=repr):
        merged_child = _merge_specs(child_specs[key])
        if required_everywhere[key]:
            required[key] = merged_child
        else:
            optional_fields[key] = merged_child

    allow_extra = any(spec["allow_extra"] for spec in specs)

    return shape(required=required, optional_fields=optional_fields, allow_extra=allow_extra)


def _merge_list_specs(specs):
    items = []
    mins = []
    maxs = []

    for spec in specs:
        items.append(spec["item"])
        mins.append(spec["min_length"])
        maxs.append(spec["max_length"])

    merged_item = _merge_specs(items)
    merged_min = min(mins) if mins else 0
    merged_max = None if any(item is None for item in maxs) else max(maxs)

    return list_of(merged_item, min_length=merged_min, max_length=merged_max)


def _merge_set_specs(specs):
    items = []
    mins = []
    maxs = []

    for spec in specs:
        items.append(spec["item"])
        mins.append(spec["min_length"])
        maxs.append(spec["max_length"])

    merged_item = _merge_specs(items)
    merged_min = min(mins) if mins else 0
    merged_max = None if any(item is None for item in maxs) else max(maxs)

    return set_of(merged_item, min_length=merged_min, max_length=merged_max)


def _merge_sequence_specs(specs):
    items = []
    mins = []
    maxs = []

    for spec in specs:
        items.append(spec["item"])
        mins.append(spec["min_length"])
        maxs.append(spec["max_length"])

    merged_item = _merge_specs(items)
    merged_min = min(mins) if mins else 0
    merged_max = None if any(item is None for item in maxs) else max(maxs)

    return sequence_of(merged_item, min_length=merged_min, max_length=merged_max)


def _merge_mapping_specs(specs):
    keys_ = []
    values_ = []
    mins = []
    maxs = []

    for spec in specs:
        keys_.append(spec["key"])
        values_.append(spec["value"])
        mins.append(spec["min_length"])
        maxs.append(spec["max_length"])

    merged_key = _merge_specs(keys_)
    merged_value = _merge_specs(values_)
    merged_min = min(mins) if mins else 0
    merged_max = None if any(item is None for item in maxs) else max(maxs)

    return mapping_of(
        merged_key,
        merged_value,
        min_length=merged_min,
        max_length=merged_max,
    )


def _merge_tuple_specs(specs):
    lengths = {len(spec["items"]) for spec in specs}
    if len(lengths) != 1:
        return one_of(*specs)

    length = next(iter(lengths))
    merged_items = []

    for index in range(length):
        position_specs = []
        for spec in specs:
            position_specs.append(spec["items"][index])
        merged_items.append(_merge_specs(position_specs))

    return tuple_of(*merged_items)


def _check_spec(spec, value):
    kind_name = spec["kind"]

    if kind_name == "any":
        return True

    if kind_name == "nothing":
        return False

    if kind_name == "kind":
        return isinstance(value, spec["type"])

    if kind_name == "exact":
        return value == spec["value"]

    if kind_name == "predicate":
        ok, result = _safe_call(spec["fn"], value)
        if not ok:
            return False
        return bool(result)

    if kind_name == "one_of":
        for option in spec["options"]:
            if _check_spec(option, value):
                return True
        return False

    if kind_name == "all_of":
        for part in spec["parts"]:
            if not _check_spec(part, value):
                return False
        return True

    if kind_name == "sequence_of":
        return _check_sequence_of(spec, value)

    if kind_name == "list_of":
        return _check_list_of(spec, value)

    if kind_name == "tuple_of":
        return _check_tuple_of(spec, value)

    if kind_name == "mapping_of":
        return _check_mapping_of(spec, value)

    if kind_name == "set_of":
        return _check_set_of(spec, value)

    if kind_name == "shape":
        return _check_shape(spec, value)

    if kind_name == "interface":
        return _check_interface(spec, value)

    if kind_name == "callable_with":
        return _check_callable_with(spec, value)

    if kind_name == "awaitable":
        return isawaitable(value)

    if kind_name == "generator":
        return _is_generator(value)

    if kind_name == "object":
        return isinstance(value, spec["type"])

    return False


def _check_sequence_of(spec, value):
    if not _is_sequence_but_not_text(value):
        return False

    length = len(value)
    if length < spec["min_length"]:
        return False

    if spec["max_length"] is not None and length > spec["max_length"]:
        return False

    for item in value:
        if not _check_spec(spec["item"], item):
            return False

    return True


def _check_list_of(spec, value):
    if not isinstance(value, list):
        return False

    length = len(value)
    if length < spec["min_length"]:
        return False

    if spec["max_length"] is not None and length > spec["max_length"]:
        return False

    for item in value:
        if not _check_spec(spec["item"], item):
            return False

    return True


def _check_tuple_of(spec, value):
    if not isinstance(value, tuple):
        return False

    if len(value) != len(spec["items"]):
        return False

    for index, item_spec in enumerate(spec["items"]):
        if not _check_spec(item_spec, value[index]):
            return False

    return True


def _check_mapping_of(spec, value):
    if not _is_mapping(value):
        return False

    length = len(value)
    if length < spec["min_length"]:
        return False

    if spec["max_length"] is not None and length > spec["max_length"]:
        return False

    for key, item in value.items():
        if not _check_spec(spec["key"], key):
            return False
        if not _check_spec(spec["value"], item):
            return False

    return True


def _check_set_of(spec, value):
    if not _is_set_like(value):
        return False

    length = len(value)
    if length < spec["min_length"]:
        return False

    if spec["max_length"] is not None and length > spec["max_length"]:
        return False

    for item in value:
        if not _check_spec(spec["item"], item):
            return False

    return True


def _check_shape(spec, value):
    if not _is_mapping(value):
        return False

    for key in spec["required"].keys():
        if key not in value:
            return False

    known_keys = set(spec["required"].keys()) | set(spec["optional"].keys())

    if not spec["allow_extra"]:
        for key in value.keys():
            if key not in known_keys:
                return False

    for key, child_spec in spec["required"].items():
        if not _check_spec(child_spec, value[key]):
            return False

    for key, child_spec in spec["optional"].items():
        if key in value and not _check_spec(child_spec, value[key]):
            return False

    return True


def _check_interface(spec, value):
    for name, child_spec in spec["attributes"].items():
        if not _safe_hasattr(value, name):
            return False
        attr_value = _safe_getattr(value, name)
        if not _check_spec(child_spec, attr_value):
            return False

    for name, child_spec in spec["methods"].items():
        if not _safe_hasattr(value, name):
            return False
        method_value = _safe_getattr(value, name)
        if not callable(method_value):
            return False
        if not _check_spec(child_spec, method_value):
            return False

    for pred in spec["predicates"]:
        if not _check_spec(pred, value):
            return False

    return True


def _callable_arity_info(value):
    ok, sig = _safe_call(signature, value)
    if not ok:
        return None

    required_positional = 0
    max_positional = 0
    has_varargs = False

    for param in sig.parameters.values():
        if param.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD):
            max_positional += 1
            if param.default is Parameter.empty:
                required_positional += 1
        elif param.kind == Parameter.VAR_POSITIONAL:
            has_varargs = True

    return {
        "required_positional": required_positional,
        "max_positional": None if has_varargs else max_positional,
        "has_varargs": has_varargs,
    }


def _matches_callable_arity(info, arity, min_arity, max_arity):
    required_positional = info["required_positional"]
    max_positional = info["max_positional"]
    has_varargs = info["has_varargs"]

    if arity is not None:
        if arity < required_positional:
            return False
        if not has_varargs and arity > max_positional:
            return False
        return True

    if min_arity is not None:
        if not has_varargs and max_positional < min_arity:
            return False

    if max_arity is not None:
        if required_positional > max_arity:
            return False

    return True


def _check_callable_with(spec, value):
    if not callable(value):
        return False

    info = _callable_arity_info(value)
    if info is None:
        return False

    return _matches_callable_arity(
        info,
        spec["arity"],
        spec["min_arity"],
        spec["max_arity"],
    )


def _explain_spec(spec, value, path):
    kind_name = spec["kind"]

    if kind_name == "any":
        return True, []

    if kind_name == "nothing":
        return False, [path + " darf keinen Wert annehmen"]

    if kind_name == "kind":
        if isinstance(value, spec["type"]):
            return True, []
        return False, [
            path + " erwartet " + spec["type"].__name__ + ", erhalten " + type(value).__name__
        ]

    if kind_name == "exact":
        if value == spec["value"]:
            return True, []
        return False, [
            path + " erwartet exakten Wert " + repr(spec["value"]) + ", erhalten " + repr(value)
        ]

    if kind_name == "predicate":
        ok, result = _safe_call(spec["fn"], value)
        if not ok:
            return False, [
                path + " verletzt Prädikat " + spec["name"] + " (Ausnahme: " + repr(result) + ")"
            ]
        if result:
            return True, []
        return False, [
            path + " verletzt Prädikat " + spec["name"]
        ]

    if kind_name == "one_of":
        collected = []
        for option in spec["options"]:
            ok, reasons = _explain_spec(option, value, path)
            if ok:
                return True, []
            collected.extend(reasons)

        if not collected:
            collected.append(path + " passt zu keiner erlaubten Alternative")

        return False, _dedupe_strings(collected)

    if kind_name == "all_of":
        collected = []
        for part in spec["parts"]:
            ok, reasons = _explain_spec(part, value, path)
            if not ok:
                collected.extend(reasons)

        if collected:
            return False, _dedupe_strings(collected)
        return True, []

    if kind_name == "sequence_of":
        if not _is_sequence_but_not_text(value):
            return False, [
                path + " erwartet sequence-artigen Wert, erhalten " + type(value).__name__
            ]

        length = len(value)
        if length < spec["min_length"]:
            return False, [
                path + " ist zu kurz: " + str(length) + " < " + str(spec["min_length"])
            ]

        if spec["max_length"] is not None and length > spec["max_length"]:
            return False, [
                path + " ist zu lang: " + str(length) + " > " + str(spec["max_length"])
            ]

        collected = []
        for index, item in enumerate(value):
            ok, reasons = _explain_spec(spec["item"], item, _path_key(path, index))
            if not ok:
                collected.extend(reasons)

        if collected:
            return False, _dedupe_strings(collected)
        return True, []

    if kind_name == "list_of":
        if not isinstance(value, list):
            return False, [
                path + " erwartet list, erhalten " + type(value).__name__
            ]

        length = len(value)
        if length < spec["min_length"]:
            return False, [
                path + " ist zu kurz: " + str(length) + " < " + str(spec["min_length"])
            ]

        if spec["max_length"] is not None and length > spec["max_length"]:
            return False, [
                path + " ist zu lang: " + str(length) + " > " + str(spec["max_length"])
            ]

        collected = []
        for index, item in enumerate(value):
            ok, reasons = _explain_spec(spec["item"], item, _path_key(path, index))
            if not ok:
                collected.extend(reasons)

        if collected:
            return False, _dedupe_strings(collected)
        return True, []

    if kind_name == "tuple_of":
        if not isinstance(value, tuple):
            return False, [
                path + " erwartet tuple, erhalten " + type(value).__name__
            ]

        if len(value) != len(spec["items"]):
            return False, [
                path + " erwartet Tuple-Länge " + str(len(spec["items"])) + ", erhalten " + str(len(value))
            ]

        collected = []
        for index, item_spec in enumerate(spec["items"]):
            ok, reasons = _explain_spec(item_spec, value[index], _path_key(path, index))
            if not ok:
                collected.extend(reasons)

        if collected:
            return False, _dedupe_strings(collected)
        return True, []

    if kind_name == "mapping_of":
        if not _is_mapping(value):
            return False, [
                path + " erwartet mapping, erhalten " + type(value).__name__
            ]

        length = len(value)
        if length < spec["min_length"]:
            return False, [
                path + " ist zu kurz: " + str(length) + " < " + str(spec["min_length"])
            ]

        if spec["max_length"] is not None and length > spec["max_length"]:
            return False, [
                path + " ist zu lang: " + str(length) + " > " + str(spec["max_length"])
            ]

        collected = []
        for key, item in value.items():
            ok_key, key_reasons = _explain_spec(spec["key"], key, path + ".<key>")
            if not ok_key:
                prefixed = []
                for reason in key_reasons:
                    prefixed.append(reason + " bei Schlüssel " + repr(key))
                collected.extend(prefixed)

            ok_value, value_reasons = _explain_spec(spec["value"], item, _path_key(path, key))
            if not ok_value:
                collected.extend(value_reasons)

        if collected:
            return False, _dedupe_strings(collected)
        return True, []

    if kind_name == "set_of":
        if not _is_set_like(value):
            return False, [
                path + " erwartet set/frozenset-artigen Wert, erhalten " + type(value).__name__
            ]

        length = len(value)
        if length < spec["min_length"]:
            return False, [
                path + " ist zu kurz: " + str(length) + " < " + str(spec["min_length"])
            ]

        if spec["max_length"] is not None and length > spec["max_length"]:
            return False, [
                path + " ist zu lang: " + str(length) + " > " + str(spec["max_length"])
            ]

        collected = []
        for item in value:
            ok, reasons = _explain_spec(spec["item"], item, path + "{" + repr(item) + "}")
            if not ok:
                collected.extend(reasons)

        if collected:
            return False, _dedupe_strings(collected)
        return True, []

    if kind_name == "shape":
        if not _is_mapping(value):
            return False, [
                path + " erwartet mapping, erhalten " + type(value).__name__
            ]

        collected = []

        for key, child_spec in spec["required"].items():
            if key not in value:
                collected.append(path + " fehlt Pflichtfeld " + repr(key))
                continue

            ok, reasons = _explain_spec(child_spec, value[key], _path_key(path, key))
            if not ok:
                collected.extend(reasons)

        for key, child_spec in spec["optional"].items():
            if key in value:
                ok, reasons = _explain_spec(child_spec, value[key], _path_key(path, key))
                if not ok:
                    collected.extend(reasons)

        if not spec["allow_extra"]:
            known_keys = set(spec["required"].keys()) | set(spec["optional"].keys())
            for key in value.keys():
                if key not in known_keys:
                    collected.append(path + " enthält unerwartetes Feld " + repr(key))

        if collected:
            return False, _dedupe_strings(collected)
        return True, []

    if kind_name == "interface":
        collected = []

        for name, child_spec in spec["attributes"].items():
            if not _safe_hasattr(value, name):
                collected.append(path + " fehlt Attribut " + repr(name))
                continue

            attr_value = _safe_getattr(value, name)
            ok, reasons = _explain_spec(child_spec, attr_value, _path_attr(path, name))
            if not ok:
                collected.extend(reasons)

        for name, child_spec in spec["methods"].items():
            if not _safe_hasattr(value, name):
                collected.append(path + " fehlt Methode " + repr(name))
                continue

            method_value = _safe_getattr(value, name)
            if not callable(method_value):
                collected.append(_path_attr(path, name) + " ist nicht callable")
                continue

            ok, reasons = _explain_spec(child_spec, method_value, _path_attr(path, name))
            if not ok:
                collected.extend(reasons)

        for pred in spec["predicates"]:
            ok, reasons = _explain_spec(pred, value, path)
            if not ok:
                collected.extend(reasons)

        if collected:
            return False, _dedupe_strings(collected)
        return True, []

    if kind_name == "callable_with":
        if not callable(value):
            return False, [
                path + " ist nicht callable"
            ]

        info = _callable_arity_info(value)
        if info is None:
            return False, [
                path + " hat keine auswertbare Signatur"
            ]

        if _matches_callable_arity(info, spec["arity"], spec["min_arity"], spec["max_arity"]):
            return True, []

        if info["max_positional"] is None:
            range_text = (
                "mindestens " + str(info["required_positional"]) + " positionale Argument(e), varargs vorhanden"
            )
        else:
            range_text = (
                str(info["required_positional"])
                + " bis "
                + str(info["max_positional"])
                + " positionale Argument(e)"
            )

        return False, [
            path
            + " verletzt callable-Signaturbedingung; gefunden: "
            + range_text
        ]

    if kind_name == "awaitable":
        if isawaitable(value):
            return True, []
        return False, [
            path + " ist nicht awaitable"
        ]

    if kind_name == "generator":
        if _is_generator(value):
            return True, []
        return False, [
            path + " ist kein Generator"
        ]

    if kind_name == "object":
        if isinstance(value, spec["type"]):
            return True, []
        return False, [
            path + " erwartet Objekt von " + spec["type"].__name__ + ", erhalten " + type(value).__name__
        ]

    return False, [
        path + ": unbekannte Spezifikation " + repr(spec)
    ]



# Usage:

'''
spec = {
    "user": {
        "id": int,
        "profile": {
            "name": str,
            "active": bool,
        },
    }
}

value = {
    "user": {
        "id": 42,
        "profile": {
            "name": "Philipp",
            "active": True,
        },
    }
}

print(dt.check(spec, value))
dt.validate(spec, value)



spec = {
    "user": {
        "id": int,
        "name": str,
        "email": dt.optional_field(str),
    }
}



sample = {
    "user": {
        "id": 1,
        "profile": {
            "name": "Philipp"
        }
    }
}

spec = dt.infer(sample)
print(dt.describe(spec))



spec = {
    "user": {
        "profile": {
            "name": str
        }
    }
}

value = {
    "user": {
        "profile": {
            "name": 123
        }
    }
}

ok, errors = dt.explain(spec, value)
print(ok)
print(errors)

'''


'''
spec = dt.shape(
    required={
        "user": dt.shape(
            required={
                "id": int,
                "profile": dt.shape(
                    required={
                        "name": str,
                        "address": dt.shape(
                            required={
                                "city": str,
                                "zip": str
                            }
                        )
                    }
                )
            }
        )
    }
)

value = {
    "user": {
        "id": 123,
        "profile": {
            "name": "Philipp",
            "address": {
                "city": "Berlin",
                "zip": "10115"
            }
        }
    }
}

print(dt.check(spec, value))
dt.validate(spec, value)



spec = dt.shape(
    required={
        "meta": dt.shape(
            required={
                "version": int,
                "tags": dt.sequence_of(str)
            }
        ),
        "items": dt.sequence_of(
            dt.shape(
                required={
                    "id": int,
                    "payload": dt.shape(
                        required={
                            "name": str,
                            "active": bool
                        }
                    )
                }
            )
        )
    }
)

value = {
    "meta": {
        "version": 1,
        "tags": ["python", "automation"]
    },
    "items": [
        {
            "id": 10,
            "payload": {
                "name": "sensor-a",
                "active": True
            }
        },
        {
            "id": 11,
            "payload": {
                "name": "sensor-b",
                "active": False
            }
        }
    ]
}

print(dt.check(spec, value))



sample = {
    "user": {
        "id": 1,
        "name": "Philipp",
        "roles": ["admin", "dev"]
    },
    "enabled": True
}

spec = dt.infer(sample)

print(dt.describe(spec))
print(dt.check(spec, sample))

'''

