# -*- coding: utf-8 -*-

# src/libraries/_generic_rule_engine.py

"""
Generic Rule Engine — Industrielle Prozessautomatisierung
=========================================================

5-Stufen-Pipeline:  Compiler -> Validator -> Diagnostics -> Expression-Eval -> Engine

Arbeitet direkt mit dem Legacy-JSON-Format (___device_state_data.json):
    - automation_rule_sets   (inline when_all/do/else_do)
    - transitions            (trigger_ids -> action_ids, reversible)
    - dynamic_rule_engine    (cond -> action_id, undo_action_id)
    - triggers               (sensor, timer, schedule, expression, compound, state)
    - actions                (set_value, set_process_value, set_parameter, emergency_shutdown, notify)
    - timers                 (duration, interval, schedule)
    - events                 (sensor_threshold, timer_elapsed, state_changed, ...)

Generische Table-Schema-Unterstützung für beliebige Rule-Tables.

Interface für PSM-Integration:
    - create_engine_context()    -> Engine-Kontext erstellen
    - load_config()              -> Config laden und kompilieren
    - evaluate()                 -> Regeln evaluieren, geplante Aktionen zurückgeben
    - get_diagnostics()          -> Diagnose-Report erstellen

Architektur:
    - Rein funktional, kein OOP
    - functools.partial statt lambda
    - Thread-safe (kein shared mutable state zwischen evaluate()-Aufrufen)
    - Deterministisch (prioritätsbasierte Sortierung, stabile Reihenfolge)
    - Erweiterbar über Condition-Handler-Registry
"""

import ast
import copy
import json
import logging
import math
import re
import threading
import time
from functools import partial, reduce
from pathlib import Path
from typing import Any, Callable

# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------

_log = logging.getLogger("generic_rule_engine")


def configure_logging(level=logging.INFO):
    """Logging-Level für die Rule-Engine konfigurieren."""
    _log.setLevel(level)
    if not _log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "[%(levelname)s] %(name)s :: %(message)s"
        ))
        _log.addHandler(handler)


# ---------------------------------------------------------------------------
#  Telemetrie / Stats (Thread-safe Counter)
# ---------------------------------------------------------------------------

_stats_lock = threading.Lock()
_stats = {
    "evaluations_total": 0,
    "rules_matched": 0,
    "rules_skipped_cooldown": 0,
    "rules_skipped_oneshot": 0,
    "conditions_evaluated": 0,
    "triggers_evaluated": 0,
    "triggers_cooldown_blocked": 0,
    "triggers_debounce_pending": 0,
    "actions_planned": 0,
    "undo_actions_planned": 0,
    "compile_calls": 0,
    "compile_cache_hits": 0,
    "expression_errors": 0,
    "condition_errors": 0,
}


def get_stats():
    """Thread-safe Kopie der aktuellen Telemetrie-Daten."""
    with _stats_lock:
        return dict(_stats)


def reset_stats():
    """Alle Telemetrie-Counter zurücksetzen."""
    with _stats_lock:
        for key in _stats:
            _stats[key] = 0


def _inc_stat(key, amount=1):
    """Einen Counter thread-safe inkrementieren."""
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + amount


# ---------------------------------------------------------------------------
#  Generische Hilfsfunktionen (einmalig definiert)
# ---------------------------------------------------------------------------

def normalize_id(value):
    """ID-Wert als String normalisieren."""
    return str(value)


def safe_int(value, default=0):
    """Sicherer int-Cast."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value, default=0.0):
    """Sicherer float-Cast."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_str(value, default=""):
    """Sicherer str-Cast."""
    try:
        return str(value)
    except (TypeError, ValueError):
        return str(default)


def index_by(rows, id_field):
    """Liste von Dicts nach ID-Feld indizieren."""
    result = {}
    for row in rows:
        if isinstance(row, dict) and id_field in row:
            result[normalize_id(row[id_field])] = row
    return result


def get_table(data, name):
    """Sicherer Zugriff auf eine Tabelle (Liste) in einem Dict."""
    value = data.get(name, [])
    return value if isinstance(value, list) else []


def unique_preserve_order(values):
    """Eindeutige Werte, Reihenfolge beibehalten."""
    seen = set()
    result = []
    for value in values:
        key = normalize_id(value)
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result


def get_nested(data, *keys, default=None):
    """Sicherer verschachtelter Dict-Zugriff."""
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key, default)
    return current


def load_json(path):
    """JSON-Datei laden."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def dump_json(path, payload):
    """JSON-Datei schreiben."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def now_timestamp():
    """Aktueller Zeitstempel als float."""
    return time.time()


# ---------------------------------------------------------------------------
#  STUFE 1: Sicherer Expression-Evaluator (AST-basiert)
# ---------------------------------------------------------------------------

_ALLOWED_FUNCTIONS = {
    "abs": abs,
    "min": min,
    "max": max,
    "round": round,
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "len": len,
    "sqrt": math.sqrt,
    "log": math.log,
    "exp": math.exp,
}

_BINOP_MAP = {
    ast.Add: "add", ast.Sub: "sub", ast.Mult: "mul",
    ast.Div: "div", ast.Mod: "mod", ast.Pow: "pow",
    ast.FloorDiv: "floordiv",
}

_CMP_MAP = {
    ast.Eq: "eq", ast.NotEq: "ne", ast.Gt: "gt",
    ast.GtE: "ge", ast.Lt: "lt", ast.LtE: "le",
}


def _binop_apply(left, right, op="add"):
    """Binäre Operation ausführen."""
    _ops = {
        "add": left + right, "sub": left - right,
        "mul": left * right, "div": left / right,
        "mod": left % right, "pow": left ** right,
        "floordiv": left // right,
    }
    return _ops[op]


def _cmp_op(left, right, op_name="eq"):
    """Vergleichsoperation ausführen."""
    _ops = {
        "eq": left == right, "ne": left != right,
        "gt": left > right, "ge": left >= right,
        "lt": left < right, "le": left <= right,
    }
    return _ops[op_name]


def _eval_node(node, names):
    """Rekursiver AST-Evaluator — sicher, ohne eval()."""
    if isinstance(node, ast.Expression):
        return _eval_node(node.body, names)

    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        if node.id not in names:
            raise KeyError("Unbekanntes Symbol: %s" % node.id)
        return names[node.id]

    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, names)
        right = _eval_node(node.right, names)
        op_name = _BINOP_MAP.get(type(node.op))
        if op_name is None:
            raise ValueError("Nicht unterstützter Binäroperator: %s" % type(node.op).__name__)
        return _binop_apply(left, right, op=op_name)

    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, names)
        if isinstance(node.op, ast.UAdd):
            return +operand
        if isinstance(node.op, ast.USub):
            return -operand
        if isinstance(node.op, ast.Not):
            return not operand
        raise ValueError("Nicht unterstützter Unäroperator")

    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(_eval_node(v, names) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(_eval_node(v, names) for v in node.values)
        raise ValueError("Nicht unterstützter Bool-Operator")

    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, names)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval_node(comparator, names)
            op_name = _CMP_MAP.get(type(op))
            if op_name is None:
                raise ValueError("Nicht unterstützter Vergleichsoperator")
            if not _cmp_op(left, right, op_name=op_name):
                return False
            left = right
        return True

    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise ValueError("Nur einfache Funktionsaufrufe erlaubt")
        func_name = node.func.id
        if func_name not in _ALLOWED_FUNCTIONS:
            raise ValueError("Nicht erlaubte Funktion: %s" % func_name)
        if node.keywords:
            raise ValueError("Keyword-Argumente nicht erlaubt")
        args = [_eval_node(arg, names) for arg in node.args]
        return _ALLOWED_FUNCTIONS[func_name](*args)

    if isinstance(node, ast.IfExp):
        test = _eval_node(node.test, names)
        return _eval_node(node.body, names) if test else _eval_node(node.orelse, names)

    raise ValueError("Nicht unterstützter AST-Knoten: %s" % type(node).__name__)


def evaluate_expression(expression, names):
    """Einen Ausdruck sicher evaluieren."""
    tree = ast.parse(expression, mode="eval")
    return _eval_node(tree, names)


def resolve_value_source(value, facts):
    """Werte-Quelle auflösen: Literal, Fact-Referenz oder Ausdruck."""
    if isinstance(value, dict):
        if "expr" in value:
            return evaluate_expression(str(value["expr"]), facts)
        if "fact" in value:
            fact_name = str(value["fact"])
            if fact_name not in facts:
                raise KeyError("Fehlender Fact: %s" % fact_name)
            return facts[fact_name]
    return value


# ---------------------------------------------------------------------------
#  String-basierte Vergleichsoperatoren
# ---------------------------------------------------------------------------

_COMPARE_OPS = {
    "==": partial(_cmp_op, op_name="eq"),
    "!=": partial(_cmp_op, op_name="ne"),
    ">":  partial(_cmp_op, op_name="gt"),
    ">=": partial(_cmp_op, op_name="ge"),
    "<":  partial(_cmp_op, op_name="lt"),
    "<=": partial(_cmp_op, op_name="le"),
}


def compare(operator, left, right):
    """String-basierten Vergleich ausführen."""
    handler = _COMPARE_OPS.get(operator)
    if handler is None:
        raise ValueError("Nicht unterstützter Operator: %s" % operator)
    return handler(left, right)


# ---------------------------------------------------------------------------
#  STUFE 2: State-Type-Auflösung (Schema-Chain)
# ---------------------------------------------------------------------------

def build_state_type_index(config):
    """
    State-Type-Index aufbauen via Schema-Chain:
    process_states.mapping_id -> schema_mappings -> parameter_generic_templates.state_type
    """
    schema_map = {}
    for sm in get_table(config, "schema_mappings"):
        mid = safe_int(sm.get("mapping_id"), default=None)
        sid = safe_int(sm.get("schema_id"), default=None)
        if mid is not None and sid is not None:
            schema_map[mid] = sid

    type_map = {}
    for pt in get_table(config, "parameter_generic_templates"):
        sid = safe_int(pt.get("schema_id"), default=None)
        st = safe_str(pt.get("state_type", ""))
        if sid is not None and st:
            type_map[sid] = st

    result = {}
    for ps in get_table(config, "process_states"):
        state_id = safe_int(ps.get("state_id"), default=None)
        mapping_id = safe_int(ps.get("mapping_id"), default=None)
        if state_id is None or mapping_id is None:
            continue
        schema_id = schema_map.get(mapping_id)
        if schema_id is not None and schema_id in type_map:
            result[state_id] = type_map[schema_id]

    return result


# ---------------------------------------------------------------------------
#  STUFE 2: Legacy-Compiler (Config -> Kanonisches Runtime-Format)
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EXPR_HINT_RE = re.compile(r"[\+\-\*/\(\)><=]|\band\b|\bor\b|\bnot\b")


def _detect_value_source(value):
    """Wert als Value-Source erkennen (expr, fact oder literal)."""
    if not isinstance(value, str):
        return value
    if _EXPR_HINT_RE.search(value):
        return {"expr": value}
    if _IDENTIFIER_RE.match(value):
        return {"fact": value}
    return value


def _extract_comparison_operator(entry):
    """Legacy-Vergleichsoperator aus when_all-Entry extrahieren."""
    mapping = {"eq": "==", "ne": "!=", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}
    for key, operator in mapping.items():
        if key in entry:
            return operator
    return "=="


def _extract_comparison_value(entry):
    """Legacy-Vergleichswert aus when_all-Entry extrahieren."""
    for key in ("eq", "ne", "gt", "ge", "lt", "le"):
        if key in entry:
            return entry[key]
    return None


def _compile_when_entry_to_condition(entry):
    """Ein when_all-Entry in eine kanonische Condition kompilieren."""
    if "timer_id" in entry:
        return {
            "kind": "timer",
            "timer_id": safe_int(entry["timer_id"]),
            "status": safe_str(entry.get("on", "elapsed")),
        }

    if "sensor_id" in entry:
        operator = _extract_comparison_operator(entry)
        condition = {
            "kind": "sensor",
            "sensor_id": safe_int(entry["sensor_id"]),
            "operator": operator,
            "value": _detect_value_source(_extract_comparison_value(entry)),
        }
        if "filter_id" in entry:
            condition["filter_id"] = safe_int(entry["filter_id"])
        return condition

    if "state_id" in entry and "value_id" in entry:
        return {
            "kind": "state",
            "state_id": safe_int(entry["state_id"]),
            "attribute": "value_id",
            "operator": "==",
            "value": safe_int(entry["value_id"]),
        }

    if "value_id" in entry:
        return {
            "kind": "scope_state",
            "attribute": "value_id",
            "operator": "==",
            "value": safe_int(entry["value_id"]),
        }

    return {"kind": "expression", "expr": "True"}


def _compile_dre_condition(cond):
    """DRE-Condition kompilieren (rekursiv, all/any/sensor_id/sensor_type/state_type)."""
    if isinstance(cond, str):
        return {"kind": "expression", "expr": cond}

    if not isinstance(cond, dict):
        return {"kind": "expression", "expr": "False"}

    if "all" in cond:
        nested = cond.get("all", [])
        items = [_compile_dre_condition(c) for c in nested if isinstance(c, (dict, str))]
        return {"kind": "all", "conditions": items}

    if "any" in cond:
        nested = cond.get("any", [])
        items = [_compile_dre_condition(c) for c in nested if isinstance(c, (dict, str))]
        return {"kind": "any", "conditions": items}

    if "sensor_id" in cond:
        return {
            "kind": "sensor",
            "sensor_id": safe_int(cond["sensor_id"]),
            "operator": safe_str(cond.get("op", "==")),
            "value": _detect_value_source(cond.get("val")),
        }

    if "sensor_type" in cond:
        return {
            "kind": "sensor_type",
            "sensor_type": safe_str(cond["sensor_type"]),
            "operator": safe_str(cond.get("op", "==")),
            "value": _detect_value_source(cond.get("val")),
        }

    if "state_type" in cond:
        return {
            "kind": "state",
            "state_type": safe_str(cond["state_type"]),
            "attribute": safe_str(cond.get("attr", "value_id")),
            "operator": "==",
            "value": _detect_value_source(cond.get("eq")),
        }

    if "state_id" in cond:
        return {
            "kind": "state",
            "state_id": safe_int(cond["state_id"]),
            "attribute": safe_str(cond.get("attr", "value_id")),
            "operator": safe_str(cond.get("op", "==")),
            "value": _detect_value_source(cond.get("val", cond.get("eq"))),
        }

    return {"kind": "expression", "expr": "False"}


def _compile_inline_action(rule_id, phase, idx, entry):
    """Inline-Action aus do/else_do in eine Action-Definition kompilieren."""
    action_id = "synthetic:%s:%s:%d" % (normalize_id(rule_id), phase, idx)

    if entry.get("actuator") == "mapped":
        return {
            "action_id": action_id,
            "action_type": "actuator_write_mapped",
            "name": "mapped_actuator_write",
            "value": _detect_value_source(entry.get("value")),
        }

    if "actuator_id" in entry:
        return {
            "action_id": action_id,
            "action_type": "actuator_write",
            "name": "actuator_write",
            "actuator_id": safe_int(entry["actuator_id"]),
            "value": _detect_value_source(entry.get("value")),
        }

    if "state_id" in entry and "value_id" in entry:
        return {
            "action_id": action_id,
            "action_type": "set_state_value",
            "name": "set_state_value",
            "state_id": safe_int(entry["state_id"]),
            "value_id": safe_int(entry["value_id"]),
        }

    return {
        "action_id": action_id,
        "action_type": "notify",
        "name": "unsupported_inline_action",
        "message": "Nicht unterstützte Inline-Aktion",
    }


def _normalize_legacy_action(action):
    """Legacy-Action normalisieren."""
    result = {
        "action_id": action.get("action_id"),
        "action_type": safe_str(action.get("action_type")),
        "name": safe_str(action.get("name", "")),
    }
    # Alle zusätzlichen Felder übernehmen
    for key in action:
        if key not in ("action_id", "action_type", "name"):
            result[key] = action[key]
    return result


def _normalize_trigger(trigger):
    """Legacy-Trigger normalisieren."""
    trigger_type = safe_str(trigger.get("trigger_type"))
    result = {
        "trigger_id": trigger.get("trigger_id"),
        "trigger_type": trigger_type,
        "name": safe_str(trigger.get("name", "")),
    }

    if trigger_type == "sensor":
        if "sensor_id" in trigger:
            result["sensor_id"] = trigger["sensor_id"]
        if "sensor_type" in trigger:
            result["sensor_type"] = trigger["sensor_type"]
        result["operator"] = trigger.get("operator")
        result["value"] = _detect_value_source(trigger.get("value"))
    elif trigger_type in ("timer", "schedule"):
        result["timer_id"] = trigger.get("timer_id")
        result["on"] = trigger.get("on")
    elif trigger_type == "expression":
        result["expr"] = trigger.get("expr")
    elif trigger_type == "compound":
        result["logic"] = trigger.get("operator", "AND")
        result["trigger_ids"] = trigger.get("trigger_ids", [])
    elif trigger_type == "state":
        selector = {}
        if "state_type" in trigger:
            selector["state_type"] = trigger["state_type"]
        if "state_id" in trigger:
            selector["state_id"] = trigger["state_id"]
        result["selector"] = selector
        result["attribute"] = trigger.get("attribute")
        result["operator"] = "=="
        result["value"] = _detect_value_source(trigger.get("equals"))

    for key in ("cooldown_s", "debounce_s"):
        if key in trigger:
            result[key] = trigger[key]

    return result


def compile_config(legacy_config, include_sources=None):
    """
    Legacy-Config in kanonisches Runtime-Format kompilieren.

    Vereinigt: automation_rule_sets + transitions + dynamic_rule_engine
    zu einem einheitlichen rules[]-Array.

    Args:
        legacy_config: Raw Legacy-Config Dict
        include_sources: Optionale Einschraenkung der Quellen.
            None = alle Quellen (Default, backward-compatible).
            Tuple/Set von: "automation_rule_sets", "transitions", "dynamic_rule_engine"
            Beispiel: ("transitions", "dynamic_rule_engine") fuer PSM-Scope.
    """
    _inc_stat("compile_calls")
    _compile_sources = set(include_sources) if include_sources is not None else None
    state_type_index = build_state_type_index(legacy_config)
    automations = index_by(get_table(legacy_config, "automations"), "automation_id")
    engine_defaults = index_by(get_table(legacy_config, "automation_engines"), "engine_id")

    # --- Rule-Controls: Disabled Sets extrahieren ---
    rc_data = get_table(legacy_config, "rule_controls")
    rc_first = rc_data[0] if rc_data and isinstance(rc_data[0], dict) else {}
    disabled_trigger_ids = set()
    for tid in rc_first.get("triggers") or []:
        disabled_trigger_ids.add(normalize_id(tid))
    disabled_transition_ids = set()
    for tid in rc_first.get("transitions") or []:
        disabled_transition_ids.add(normalize_id(tid))
    disabled_dre_rule_ids = set()
    for rid in rc_first.get("rules") or []:
        disabled_dre_rule_ids.add(normalize_id(rid))
    disabled_ars_ids = set()
    for rid in rc_first.get("automation_rule_sets") or []:
        disabled_ars_ids.add(normalize_id(rid))

    # --- Actions normalisieren ---
    compiled_actions = []
    for action in get_table(legacy_config, "actions"):
        compiled_actions.append(_normalize_legacy_action(action))

    # --- Triggers normalisieren ---
    compiled_triggers = []
    for trigger in get_table(legacy_config, "triggers"):
        compiled_triggers.append(_normalize_trigger(trigger))

    # --- Rules kompilieren ---
    compiled_rules = []

    # 1) automation_rule_sets -> Rules (nur wenn Quelle aktiv)
    if _compile_sources is None or "automation_rule_sets" in _compile_sources:
      for entry in get_table(legacy_config, "automation_rule_sets"):
        legacy_rule_id = entry.get("rule_id")
        rule_id = "ars:%s" % normalize_id(legacy_rule_id)
        automation_id = normalize_id(entry.get("automation_id"))
        automation = automations.get(automation_id, {})
        engine_id = automation.get("engine_id")

        # Inline-Actions kompilieren
        action_refs = []
        for idx, do_entry in enumerate(entry.get("do", []) or []):
            if isinstance(do_entry, dict):
                action_def = _compile_inline_action(legacy_rule_id, "then", idx, do_entry)
                compiled_actions.append(action_def)
                action_refs.append(action_def["action_id"])

        else_action_refs = []
        for idx, else_entry in enumerate(entry.get("else_do", []) or []):
            if isinstance(else_entry, dict):
                action_def = _compile_inline_action(legacy_rule_id, "else", idx, else_entry)
                compiled_actions.append(action_def)
                else_action_refs.append(action_def["action_id"])

        # Conditions kompilieren
        conditions = []
        for when_entry in entry.get("when_all", []) or []:
            if isinstance(when_entry, dict):
                conditions.append(_compile_when_entry_to_condition(when_entry))

        condition = {
            "kind": "all",
            "conditions": conditions if conditions else [{"kind": "expression", "expr": "True"}],
        }

        # Scope bestimmen
        scope = {"automation_id": entry.get("automation_id")}
        state_ids = entry.get("state_ids")
        if isinstance(state_ids, list):
            scope["state_ids"] = state_ids
        elif state_ids == "auto":
            scope["state_selector"] = {"mode": "auto"}

        # Engine-Defaults
        defaults = get_nested(engine_defaults, normalize_id(engine_id), "defaults", default={})
        default_priority = safe_int(defaults.get("priority", 100))

        compiled_rules.append({
            "rule_id": rule_id,
            "rule_name": entry.get("rule_name", rule_id),
            "rule_type": "automation",
            "enabled": normalize_id(legacy_rule_id) not in disabled_ars_ids,
            "group": "automation",
            "scope": scope,
            "condition": condition,
            "action_refs": action_refs,
            "else_action_refs": else_action_refs,
            "undo_action_refs": [],
            "execution": {
                "priority": safe_int(entry.get("priority", default_priority)),
                "mode": safe_str(entry.get("exec", "parallel")),
                "cooldown_s": safe_float(entry.get("cooldown_s", 0.0)),
                "one_shot": bool(entry.get("one_shot", False)),
                "reversible": bool(entry.get("reversible", False)),
                "reset_mode": safe_str(entry.get("reset_mode", "auto")),
            },
            "metadata": {"legacy_table": "automation_rule_sets", "legacy_id": legacy_rule_id},
        })

    # 2) transitions -> Rules (nur wenn Quelle aktiv)
    if _compile_sources is None or "transitions" in _compile_sources:
     for tr in get_table(legacy_config, "transitions"):
        transition_id = tr.get("transition_id")
        rule_id = "tr:%s" % normalize_id(transition_id)

        compiled_rules.append({
            "rule_id": rule_id,
            "rule_name": tr.get("name", rule_id),
            "rule_type": "transition",
            "enabled": normalize_id(transition_id) not in disabled_transition_ids,
            "group": "transitions",
            "trigger_ids": tr.get("trigger_ids", []),
            "action_refs": tr.get("action_ids", []),
            "else_action_refs": [],
            "undo_action_refs": tr.get("undo_action_ids", []),
            "execution": {
                "priority": safe_int(tr.get("priority", 5)),
                "mode": "sequential",
                "cooldown_s": 0.0,
                "one_shot": False,
                "reversible": bool(tr.get("reversible", False)),
                "reset_mode": "auto",
            },
            "metadata": {"legacy_table": "transitions", "legacy_id": transition_id},
        })

    # 3) dynamic_rule_engine -> Rules (nur wenn Quelle aktiv)
    actions_index = index_by(compiled_actions, "action_id")
    if _compile_sources is None or "dynamic_rule_engine" in _compile_sources:
     for entry in get_table(legacy_config, "dynamic_rule_engine"):
        dre_rule_id = entry.get("rule_id")
        rule_id = "dre:%s" % normalize_id(dre_rule_id)
        action_id = entry.get("action_id")
        undo_action_id = entry.get("undo_action_id")

        condition = _compile_dre_condition(entry.get("cond"))

        action_refs = []
        if action_id is not None and normalize_id(action_id) in actions_index:
            action_refs = [action_id]

        undo_refs = []
        if undo_action_id is not None and normalize_id(undo_action_id) in actions_index:
            undo_refs = [undo_action_id]

        compiled_rules.append({
            "rule_id": rule_id,
            "rule_name": safe_str(dre_rule_id),
            "rule_type": "dynamic",
            "enabled": normalize_id(dre_rule_id) not in disabled_dre_rule_ids,
            "group": safe_str(entry.get("group", "dynamic")),
            "condition": condition,
            "action_refs": action_refs,
            "else_action_refs": [],
            "undo_action_refs": undo_refs,
            "execution": {
                "priority": safe_int(entry.get("priority", 5)),
                "mode": "parallel",
                "cooldown_s": 0.0,
                "one_shot": False,
                "reversible": bool(entry.get("reversible", False)),
                "reset_mode": "auto",
            },
            "metadata": {"legacy_table": "dynamic_rule_engine", "legacy_id": dre_rule_id},
        })

    # --- Catalog aufbauen ---
    catalog_states = []
    for ps in get_table(legacy_config, "process_states"):
        state_id = safe_int(ps.get("state_id"), default=None)
        catalog_states.append({
            "state_id": ps.get("state_id"),
            "p_id": ps.get("p_id"),
            "value_id": ps.get("value_id"),
            "mode_id": ps.get("mode_id"),
            "mapping_id": ps.get("mapping_id"),
            "state_type": state_type_index.get(state_id),
            "actuator_ids": ps.get("actuator_ids", []),
            "sensor_ids": ps.get("sensor_ids", []),
            "parameters": ps.get("parameters", {}),
            "control_strategy": ps.get("control_strategy"),
        })

    # --- Rule-Controls -> Activation-Profiles ---
    activation_profiles = []
    for rc in get_table(legacy_config, "rule_controls"):
        rule_refs = []
        for legacy_id in rc.get("automation_rule_sets", []):
            rule_refs.append("ars:%s" % normalize_id(legacy_id))
        activation_profiles.append({
            "profile_id": rc.get("rule_controls_id", "default"),
            "enabled": True,
            "rule_refs": unique_preserve_order(rule_refs),
            "rule_set_refs": [],
        })

    if not activation_profiles:
        activation_profiles.append({
            "profile_id": "default",
            "enabled": True,
            "rule_refs": [],
            "rule_set_refs": [],
        })

    return {
        "schema_version": "2.0.0",
        "catalog": {
            "states": catalog_states,
            "processes": get_table(legacy_config, "processes"),
            "process_values": get_table(legacy_config, "process_values"),
            "sensors": get_table(legacy_config, "sensors"),
            "actuators": get_table(legacy_config, "actuators"),
            "sensor_types": get_table(legacy_config, "sensor_types"),
        },
        "actions": compiled_actions,
        "triggers": compiled_triggers,
        "timers": get_table(legacy_config, "timers"),
        "events": get_table(legacy_config, "events"),
        "engines": get_table(legacy_config, "automation_engines"),
        "rules": compiled_rules,
        "rule_sets": [],
        "activation_profiles": activation_profiles,
        "state_type_index": state_type_index,
        "disabled_trigger_ids": disabled_trigger_ids,
    }


# ---------------------------------------------------------------------------
#  STUFE 3: Diagnostics
# ---------------------------------------------------------------------------

def run_diagnostics(legacy_config):
    """Diagnose über die Legacy-Config erstellen."""
    issues = []

    _ID_TABLES = [
        ("sensors", "sensor_id"), ("actuators", "actuator_id"),
        ("process_states", "state_id"), ("triggers", "trigger_id"),
        ("actions", "action_id"), ("timers", "timer_id"),
        ("transitions", "transition_id"), ("processes", "p_id"),
    ]

    # Duplikat-Erkennung
    for table_name, id_field in _ID_TABLES:
        counts = {}
        for row in get_table(legacy_config, table_name):
            key = normalize_id(row.get(id_field, ""))
            counts[key] = counts.get(key, 0) + 1
        for key, count in counts.items():
            if count > 1:
                issues.append({
                    "severity": "error", "code": "duplicate_id",
                    "path": table_name, "message": "Doppelte ID: %s (x%d)" % (key, count),
                })

    # Referenz-Prüfung
    action_ids = set(normalize_id(a.get("action_id")) for a in get_table(legacy_config, "actions"))
    trigger_ids = set(normalize_id(t.get("trigger_id")) for t in get_table(legacy_config, "triggers"))
    timer_ids = set(normalize_id(t.get("timer_id")) for t in get_table(legacy_config, "timers"))
    sensor_ids = set(normalize_id(s.get("sensor_id")) for s in get_table(legacy_config, "sensors"))
    state_ids = set(normalize_id(s.get("state_id")) for s in get_table(legacy_config, "process_states"))

    for tr in get_table(legacy_config, "transitions"):
        tid = normalize_id(tr.get("transition_id"))
        for ref_id in tr.get("trigger_ids", []):
            if normalize_id(ref_id) not in trigger_ids:
                issues.append({
                    "severity": "error", "code": "missing_ref",
                    "path": "transitions[%s].trigger_ids" % tid,
                    "message": "Trigger %s nicht gefunden" % ref_id,
                })
        for ref_id in tr.get("action_ids", []):
            if normalize_id(ref_id) not in action_ids:
                issues.append({
                    "severity": "error", "code": "missing_ref",
                    "path": "transitions[%s].action_ids" % tid,
                    "message": "Action %s nicht gefunden" % ref_id,
                })

    for rule in get_table(legacy_config, "dynamic_rule_engine"):
        rid = normalize_id(rule.get("rule_id"))
        aid = rule.get("action_id")
        if aid is not None and normalize_id(aid) not in action_ids:
            issues.append({
                "severity": "error", "code": "missing_ref",
                "path": "dynamic_rule_engine[%s].action_id" % rid,
                "message": "Action %s nicht gefunden" % aid,
            })

    for trg in get_table(legacy_config, "triggers"):
        tid = normalize_id(trg.get("trigger_id"))
        if "sensor_id" in trg and normalize_id(trg["sensor_id"]) not in sensor_ids:
            issues.append({
                "severity": "error", "code": "missing_ref",
                "path": "triggers[%s].sensor_id" % tid,
                "message": "Sensor %s nicht gefunden" % trg["sensor_id"],
            })
        if "timer_id" in trg and normalize_id(trg["timer_id"]) not in timer_ids:
            issues.append({
                "severity": "error", "code": "missing_ref",
                "path": "triggers[%s].timer_id" % tid,
                "message": "Timer %s nicht gefunden" % trg["timer_id"],
            })

    # State-Type-Mehrdeutigkeiten
    state_type_index = build_state_type_index(legacy_config)
    type_counts = {}
    for sid, stype in state_type_index.items():
        type_counts.setdefault(stype, []).append(sid)
    for stype, sids in type_counts.items():
        if len(sids) > 1:
            issues.append({
                "severity": "info", "code": "ambiguous_state_type",
                "path": "process_states",
                "message": "state_type '%s' wird von %d States verwendet: %s" % (stype, len(sids), sids),
            })

    error_count = sum(1 for i in issues if i["severity"] == "error")
    warning_count = sum(1 for i in issues if i["severity"] == "warning")
    info_count = sum(1 for i in issues if i["severity"] == "info")

    return {
        "summary": {"errors": error_count, "warnings": warning_count, "infos": info_count},
        "issues": issues,
    }


# ---------------------------------------------------------------------------
#  STUFE 4: Condition-Handler Registry
# ---------------------------------------------------------------------------

ConditionHandler = Callable
_CONDITION_HANDLERS = {}
_HANDLER_LOCK = threading.Lock()


def register_condition_handler(kind, handler):
    """Neuen Condition-Kind registrieren (thread-safe)."""
    with _HANDLER_LOCK:
        _CONDITION_HANDLERS[kind] = handler


def unregister_condition_handler(kind):
    """Condition-Kind entfernen (thread-safe)."""
    with _HANDLER_LOCK:
        _CONDITION_HANDLERS.pop(kind, None)


def get_registered_condition_kinds():
    """Alle registrierten Condition-Kinds zurückgeben (thread-safe)."""
    with _HANDLER_LOCK:
        return sorted(_CONDITION_HANDLERS.keys())


# --- Config-Accessor-Funktionen ---

def _states_by_id(config):
    return index_by(get_table(config.get("catalog", {}), "states"), "state_id")


def _actions_by_id(config):
    return index_by(get_table(config, "actions"), "action_id")


def _triggers_by_id(config):
    return index_by(get_table(config, "triggers"), "trigger_id")


# --- State-Selektion ---

def _selected_state_ids(rule, config, context):
    """Scope-basierte State-ID-Auflösung."""
    scope = rule.get("scope", {})
    if not isinstance(scope, dict):
        scope = {}

    if "state_ids" in scope and isinstance(scope["state_ids"], list):
        return [safe_int(v) for v in scope["state_ids"]]

    explicit = context.get("selected_state_ids")
    if isinstance(explicit, list) and explicit:
        return [safe_int(v) for v in explicit]

    selector = scope.get("state_selector", {})
    if not isinstance(selector, dict):
        selector = {}

    states = list(_states_by_id(config).values())
    result = []
    for state in states:
        include = True
        for key in ("p_id", "state_type", "value_id"):
            if key in selector and state.get(key) != selector.get(key):
                include = False
                break
        if include:
            result.append(safe_int(state.get("state_id")))

    return result if result else [safe_int(s.get("state_id")) for s in states]


# --- Eingebaute Condition-Handler ---

def _handle_all(condition, rule, config, context):
    items = condition.get("conditions", [])
    if not isinstance(items, list):
        return False
    return all(condition_matches(item, rule, config, context) for item in items)


def _handle_any(condition, rule, config, context):
    items = condition.get("conditions", [])
    if not isinstance(items, list):
        return False
    return any(condition_matches(item, rule, config, context) for item in items)


def _handle_not(condition, rule, config, context):
    nested = condition.get("condition")
    if not isinstance(nested, dict):
        return False
    return not condition_matches(nested, rule, config, context)


def _handle_sensor(condition, rule, config, context):
    sensor_id = safe_int(condition.get("sensor_id"))
    sensor_values = context.get("sensor_values", {})
    left = sensor_values.get(sensor_id, sensor_values.get(str(sensor_id)))
    if left is None:
        return False
    try:
        right = resolve_value_source(condition.get("value"), context.get("facts", {}))
        return compare(safe_str(condition.get("operator")), left, right)
    except (KeyError, ValueError, TypeError) as exc:
        _log.warning("Sensor-Condition Fehler (sensor_id=%s): %s", sensor_id, exc)
        return False


def _handle_sensor_type(condition, rule, config, context):
    sensor_type = safe_str(condition.get("sensor_type")).lower()
    operator = safe_str(condition.get("operator"))
    sensor_values = context.get("sensor_values", {})
    metadata = context.get("sensor_metadata", {})

    # Wenn keine Metadata vorhanden, aus state_type_index ableiten
    state_type_index = config.get("state_type_index", {})
    states_by_sensor = {}
    for state in get_table(config.get("catalog", {}), "states"):
        for sid in state.get("sensor_ids", []):
            states_by_sensor.setdefault(safe_int(sid), []).append(state)

    try:
        expected = resolve_value_source(condition.get("value"), context.get("facts", {}))
    except (KeyError, ValueError):
        return False

    for sensor_key, value in sensor_values.items():
        key = normalize_id(sensor_key)
        # Prüfe sensor_metadata
        info = metadata.get(key, {})
        if info.get("sensor_type") == sensor_type:
            try:
                if compare(operator, value, expected):
                    return True
            except (TypeError, ValueError):
                continue

        # Fallback: state_type via Schema-Chain
        sid_int = safe_int(sensor_key)
        for state in states_by_sensor.get(sid_int, []):
            state_id = safe_int(state.get("state_id"))
            if state_type_index.get(state_id, "").lower() == sensor_type:
                try:
                    if compare(operator, value, expected):
                        return True
                except (TypeError, ValueError):
                    continue

    return False


def _handle_state(condition, rule, config, context):
    states_idx = _states_by_id(config)
    attribute = safe_str(condition.get("attribute"))
    operator = safe_str(condition.get("operator"))
    try:
        expected = resolve_value_source(condition.get("value"), context.get("facts", {}))
    except (KeyError, ValueError):
        return False

    # Kandidaten ermitteln
    candidate_ids = []
    if "state_id" in condition:
        candidate_ids = [safe_int(condition["state_id"])]
    elif "state_type" in condition:
        wanted_type = safe_str(condition["state_type"]).lower()
        state_type_index = config.get("state_type_index", {})
        for sid, stype in state_type_index.items():
            if safe_str(stype).lower() == wanted_type:
                candidate_ids.append(sid)
    elif condition.get("scope_ref") in ("selected", "active"):
        candidate_ids = _selected_state_ids(rule, config, context)

    state_values = context.get("state_values", {})
    if not isinstance(state_values, dict):
        state_values = {}

    for state_id in candidate_ids:
        state = states_idx.get(normalize_id(state_id), {})
        key = normalize_id(state_id)
        value_entry = state_values.get(key, state_values.get(state_id))
        actual = None
        if isinstance(value_entry, dict) and attribute in value_entry:
            actual = value_entry.get(attribute)
        elif attribute in state:
            actual = state.get(attribute)

        if actual is not None:
            try:
                if compare(operator, actual, expected):
                    return True
            except (TypeError, ValueError):
                continue
    return False


def _handle_scope_state(condition, rule, config, context):
    states_idx = _states_by_id(config)
    attribute = safe_str(condition.get("attribute"))
    operator = safe_str(condition.get("operator"))
    try:
        expected = resolve_value_source(condition.get("value"), context.get("facts", {}))
    except (KeyError, ValueError):
        return False

    state_values = context.get("state_values", {})
    if not isinstance(state_values, dict):
        state_values = {}

    for state_id in _selected_state_ids(rule, config, context):
        state = states_idx.get(normalize_id(state_id), {})
        entry = state_values.get(str(state_id), state_values.get(state_id))
        actual = None
        if isinstance(entry, dict) and attribute in entry:
            actual = entry.get(attribute)
        elif attribute in state:
            actual = state.get(attribute)

        if actual is not None:
            try:
                if compare(operator, actual, expected):
                    return True
            except (TypeError, ValueError):
                continue
    return False


def _handle_timer(condition, rule, config, context):
    timer_id = safe_int(condition.get("timer_id"))
    timers = context.get("timers", {})
    actual = timers.get(timer_id, timers.get(str(timer_id)))
    if actual is None:
        return False
    return safe_str(actual) == safe_str(condition.get("status"))


def _handle_expression(condition, rule, config, context):
    try:
        result = evaluate_expression(safe_str(condition.get("expr")), context.get("facts", {}))
        return bool(result)
    except (KeyError, ValueError, TypeError, SyntaxError):
        _inc_stat("expression_errors")
        return False


def _handle_trigger_ref(condition, rule, config, context):
    return trigger_matches(safe_str(condition.get("trigger_id")), config, context)


def _handle_custom(condition, rule, config, context):
    """Generischer Custom-Condition-Handler."""
    custom_type = safe_str(condition.get("custom_type", ""))
    custom_handlers = context.get("custom_condition_handlers", {})
    handler = custom_handlers.get(custom_type)
    if handler is None:
        return False
    try:
        return bool(handler(condition, rule, config, context))
    except Exception as exc:
        _log.warning("Custom-Condition Fehler (%s): %s", custom_type, exc)
        return False


def _register_builtin_handlers():
    """Alle eingebauten Condition-Handler registrieren."""
    handlers = {
        "all": _handle_all, "any": _handle_any, "not": _handle_not,
        "sensor": _handle_sensor, "sensor_type": _handle_sensor_type,
        "state": _handle_state, "scope_state": _handle_scope_state,
        "timer": _handle_timer, "expression": _handle_expression,
        "trigger_ref": _handle_trigger_ref, "custom": _handle_custom,
    }
    for kind, handler in handlers.items():
        register_condition_handler(kind, handler)


# --- Generisches Condition-Matching ---

def condition_matches(condition, rule, config, context):
    """Generisches Condition-Matching über Handler-Registry (thread-safe)."""
    kind = safe_str(condition.get("kind", ""))
    with _HANDLER_LOCK:
        handler = _CONDITION_HANDLERS.get(kind)
    if handler is None:
        _log.warning("Unbekannter Condition-Kind: '%s'", kind)
        return False
    try:
        result = handler(condition, rule, config, context)
        _inc_stat("conditions_evaluated")
        trace = context.get("_trace")
        if isinstance(trace, list):
            trace.append({"kind": kind, "result": result})
        return result
    except Exception as exc:
        _inc_stat("condition_errors")
        _log.error("Fehler bei Condition-Kind '%s': %s", kind, exc)
        return False


# ---------------------------------------------------------------------------
#  STUFE 5: Engine — Trigger + Rule Evaluation
# ---------------------------------------------------------------------------

def _trigger_raw_matches(trigger, config, context):
    """Rohe Trigger-Auswertung ohne Cooldown/Debounce."""
    trigger_type = safe_str(trigger.get("trigger_type"))

    if trigger_type == "sensor":
        cond = {
            "kind": "sensor" if "sensor_id" in trigger else "sensor_type",
            "operator": trigger.get("operator"),
            "value": trigger.get("value"),
        }
        if "sensor_id" in trigger:
            cond["sensor_id"] = trigger["sensor_id"]
        if "sensor_type" in trigger:
            cond["sensor_type"] = trigger["sensor_type"]
        return condition_matches(cond, {}, config, context)

    if trigger_type in ("timer", "schedule"):
        timer_id = safe_int(trigger.get("timer_id"))
        timers = context.get("timers", {})
        actual = timers.get(timer_id, timers.get(str(timer_id)))
        return safe_str(actual) == safe_str(trigger.get("on"))

    if trigger_type == "expression":
        return condition_matches({"kind": "expression", "expr": trigger.get("expr")}, {}, config, context)

    if trigger_type == "compound":
        trigger_ids = trigger.get("trigger_ids", [])
        if not isinstance(trigger_ids, list):
            return False
        logic = safe_str(trigger.get("logic", "AND"))
        check = partial(trigger_matches, config=config, context=context)
        if logic == "AND":
            return all(check(tid) for tid in trigger_ids)
        return any(check(tid) for tid in trigger_ids)

    if trigger_type == "state":
        selector = trigger.get("selector", {})
        cond = {
            "kind": "state",
            "attribute": trigger.get("attribute"),
            "operator": trigger.get("operator"),
            "value": trigger.get("value"),
        }
        if isinstance(selector, dict):
            for key in ("state_type", "state_id"):
                if key in selector:
                    cond[key] = selector[key]
        return condition_matches(cond, {}, config, context)

    return False


def trigger_matches(trigger_id, config, context):
    """Trigger evaluieren inklusive Debounce und Cooldown."""
    key = normalize_id(trigger_id)

    # Disabled-Check (via rule_controls)
    disabled = config.get("disabled_trigger_ids")
    if isinstance(disabled, (set, frozenset)) and key in disabled:
        return False

    trigger = _triggers_by_id(config).get(key)
    if trigger is None:
        return False

    cache = context.get("_evaluation_cache", {}).get("triggers", {})
    if key in cache:
        return bool(cache[key])

    runtime = context.get("runtime", {})
    now_s = safe_float(context.get("now_s", 0.0))
    last_fired = runtime.get("trigger_last_fired_s", {})
    true_since = runtime.get("trigger_true_since_s", {})

    raw = _trigger_raw_matches(trigger, config, context)

    if not raw:
        true_since.pop(key, None)
        cache[key] = False
        return False

    # Debounce
    debounce_s = safe_float(trigger.get("debounce_s", 0.0))
    if debounce_s > 0.0:
        if key not in true_since:
            true_since[key] = now_s
            _inc_stat("triggers_debounce_pending")
            cache[key] = False
            return False
        if now_s - safe_float(true_since[key]) < debounce_s:
            cache[key] = False
            return False

    # Cooldown
    cooldown_s = safe_float(trigger.get("cooldown_s", 0.0))
    if cooldown_s > 0.0 and key in last_fired:
        if now_s - safe_float(last_fired[key]) < cooldown_s:
            _inc_stat("triggers_cooldown_blocked")
            cache[key] = False
            return False

    _inc_stat("triggers_evaluated")
    last_fired[key] = now_s
    cache[key] = True
    return True


# --- Action-Materialisierung ---

def _materialize_action(action, rule, config, context):
    """Action materialisieren."""
    action_type = safe_str(action.get("action_type"))
    facts = context.get("facts", {})

    if action_type == "actuator_write_mapped":
        state_ids = _selected_state_ids(rule, config, context)
        states_idx = _states_by_id(config)
        actuator_ids = []
        for sid in state_ids:
            state = states_idx.get(normalize_id(sid), {})
            aids = state.get("actuator_ids", [])
            if isinstance(aids, list):
                actuator_ids.extend(safe_int(a) for a in aids)
        actuator_ids = unique_preserve_order(actuator_ids)

        try:
            resolved_value = resolve_value_source(action.get("value"), facts)
        except (KeyError, ValueError):
            resolved_value = action.get("value")

        return [
            {
                "action_id": action.get("action_id"),
                "action_type": "actuator_write",
                "actuator_id": aid,
                "value": resolved_value,
                "source_rule_id": rule.get("rule_id"),
            }
            for aid in actuator_ids
        ]

    entry = dict(action)
    if "value" in entry:
        try:
            entry["value"] = resolve_value_source(entry["value"], facts)
        except (KeyError, ValueError):
            pass
    entry["source_rule_id"] = rule.get("rule_id")
    return [entry]


def _resolve_action_refs(action_refs, rule, config, context):
    """Action-Referenzen auflösen."""
    action_index = _actions_by_id(config)
    result = []
    for ref in action_refs:
        action = action_index.get(normalize_id(ref))
        if action is not None:
            result.extend(_materialize_action(action, rule, config, context))
    return result


# --- Rule-Matching ---

def _rule_matches(rule, config, context):
    """Prüfen ob eine Regel matcht."""
    if "condition" in rule:
        return condition_matches(rule["condition"], rule, config, context)
    trigger_ids = rule.get("trigger_ids", [])
    if not isinstance(trigger_ids, list):
        return False
    return all(trigger_matches(tid, config, context) for tid in trigger_ids)


def _active_rule_ids(config, profile_id):
    """Aktive Regel-IDs basierend auf Profil bestimmen."""
    rules = get_table(config, "rules")
    all_ids = [normalize_id(r.get("rule_id")) for r in rules]
    if profile_id is None:
        return all_ids

    for p in get_table(config, "activation_profiles"):
        if normalize_id(p.get("profile_id")) == normalize_id(profile_id):
            if not p.get("enabled", True):
                return []
            rule_refs = [normalize_id(r) for r in p.get("rule_refs", []) if r is not None]
            return unique_preserve_order(rule_refs) if rule_refs else all_ids

    return all_ids


def _rule_priority(rule):
    """Priorität einer Regel."""
    return safe_int(get_nested(rule, "execution", "priority", default=0))


# ---------------------------------------------------------------------------
#  Haupt-Engine: evaluate_rules
# ---------------------------------------------------------------------------

def evaluate_rules(config, context, profile_id=None):
    """
    Alle aktiven Regeln evaluieren.

    Args:
        config:     Kompiliertes Config-Dict (output von compile_config)
        context:    Evaluation-Context (output von create_engine_context)
        profile_id: Optionales Activation-Profile

    Returns:
        {
            "matched_rules": [...],
            "planned_actions": [...],
            "runtime": {...},
            "trace": [...],
        }
    """
    context["_evaluation_cache"] = {"triggers": {}}
    context.setdefault("_trace", [])
    runtime = _ensure_runtime(context.get("runtime"))
    context["runtime"] = runtime
    now_s = safe_float(context.get("now_s", 0.0))

    rules = get_table(config, "rules")
    allowed_ids = set(_active_rule_ids(config, profile_id))

    active_rules = [
        r for r in rules
        if normalize_id(r.get("rule_id")) in allowed_ids and r.get("enabled", True)
    ]
    ordered_rules = sorted(active_rules, key=_rule_priority, reverse=True)

    matched_rules = []
    planned_actions = []

    last_fired = runtime["rule_last_fired_s"]
    fired_once = runtime["rule_fired_once"]
    previous_match = runtime["rule_previous_match"]

    for rule in ordered_rules:
        rule_id = normalize_id(rule.get("rule_id"))
        execution = rule.get("execution", {})
        if not isinstance(execution, dict):
            execution = {}

        # One-Shot
        if execution.get("one_shot", False) and fired_once.get(rule_id):
            _inc_stat("rules_skipped_oneshot")
            continue

        # Cooldown
        cooldown_s = safe_float(execution.get("cooldown_s", 0.0))
        if cooldown_s > 0.0 and rule_id in last_fired:
            if now_s - safe_float(last_fired[rule_id]) < cooldown_s:
                _inc_stat("rules_skipped_cooldown")
                continue

        matched = _rule_matches(rule, config, context)
        old_match = bool(previous_match.get(rule_id, False))
        previous_match[rule_id] = matched

        if matched:
            actions = _resolve_action_refs(rule.get("action_refs", []), rule, config, context)
            planned_actions.extend(actions)
            matched_rules.append({
                "rule_id": rule.get("rule_id"),
                "rule_name": rule.get("rule_name"),
                "group": rule.get("group"),
                "priority": execution.get("priority"),
                "rule_type": rule.get("rule_type"),
            })
            last_fired[rule_id] = now_s
            fired_once[rule_id] = True
        else:
            if execution.get("reversible", False) and old_match:
                undo_actions = _resolve_action_refs(rule.get("undo_action_refs", []), rule, config, context)
                planned_actions.extend(undo_actions)
            elif rule.get("else_action_refs"):
                else_actions = _resolve_action_refs(rule.get("else_action_refs", []), rule, config, context)
                planned_actions.extend(else_actions)

    _inc_stat("evaluations_total")
    _inc_stat("rules_matched", len(matched_rules))
    _inc_stat("actions_planned", len(planned_actions))

    return {
        "matched_rules": matched_rules,
        "planned_actions": planned_actions,
        "runtime": runtime,
        "trace": context.get("_trace", []),
    }


# ---------------------------------------------------------------------------
#  Runtime-State
# ---------------------------------------------------------------------------

def _ensure_runtime(runtime=None):
    """Runtime-State initialisieren."""
    rt = runtime if isinstance(runtime, dict) else {}
    rt.setdefault("rule_last_fired_s", {})
    rt.setdefault("rule_fired_once", {})
    rt.setdefault("rule_previous_match", {})
    rt.setdefault("trigger_last_fired_s", {})
    rt.setdefault("trigger_true_since_s", {})
    return rt


# ---------------------------------------------------------------------------
#  PSM-Interface: Engine-Context + High-Level API
# ---------------------------------------------------------------------------

def create_engine_context(
    now_s=0.0,
    facts=None,
    sensor_values=None,
    sensor_metadata=None,
    state_values=None,
    timers=None,
    selected_state_ids=None,
    runtime=None,
):
    """
    Engine-Evaluation-Context erstellen.

    Wird für jeden evaluate()-Aufruf frisch erstellt oder wiederverwendet.
    runtime kann zwischen Aufrufen persistiert werden für Edge-Detection.
    """
    return {
        "now_s": now_s,
        "facts": facts if facts is not None else {},
        "sensor_values": sensor_values if sensor_values is not None else {},
        "sensor_metadata": sensor_metadata if sensor_metadata is not None else {},
        "state_values": state_values if state_values is not None else {},
        "timers": timers if timers is not None else {},
        "selected_state_ids": selected_state_ids if selected_state_ids is not None else [],
        "runtime": _ensure_runtime(runtime),
        "_evaluation_cache": {"triggers": {}},
        "_trace": [],
    }


def load_config(path_or_dict, use_cache=False):
    """
    Config laden und kompilieren.

    Akzeptiert Dateipfad (str/Path) oder bereits geladenes Dict.
    Mit use_cache=True wird der Compile-Cache genutzt.
    """
    if isinstance(path_or_dict, (str, Path)):
        raw = load_json(path_or_dict)
    elif isinstance(path_or_dict, dict):
        raw = path_or_dict
    else:
        raise TypeError("Erwartet str, Path oder dict, erhalten: %s" % type(path_or_dict).__name__)

    if use_cache:
        compiled, was_cached = compile_config_cached(raw)
        return compiled

    return compile_config(raw)


def evaluate(compiled_config, context, profile_id=None):
    """
    High-Level Evaluation Interface.

    Args:
        compiled_config:  Output von load_config() oder compile_config()
        context:          Output von create_engine_context()
        profile_id:       Optionales Activation-Profile

    Returns:
        Dict mit matched_rules, planned_actions, runtime, trace
    """
    return evaluate_rules(compiled_config, context, profile_id=profile_id)


def get_diagnostics(config):
    """
    Diagnose-Report für eine Config erstellen.

    Args:
        config: Raw Legacy-Config Dict

    Returns:
        Dict mit summary und issues
    """
    return run_diagnostics(config)


# ---------------------------------------------------------------------------
#  Generisches Rule-Table Schema (extern ladbar, Fallback auf Built-in)
# ---------------------------------------------------------------------------

_DEFAULT_TABLE_SCHEMA = {
    "$schema": "generic_rule_table_schema_v1",
    "description": "Schema fuer generische Rule-Tables in der Generic Rule Engine",
    "table_definitions": {
        "automation_rule_sets": {
            "id_field": "rule_id",
            "compile_to": "automation",
            "required_fields": ["rule_id", "automation_id", "when_all", "do"],
            "optional_fields": ["rule_name", "state_ids", "else_do", "priority", "exec",
                                "cooldown_s", "one_shot", "reversible", "reset_mode"],
        },
        "transitions": {
            "id_field": "transition_id",
            "compile_to": "transition",
            "required_fields": ["transition_id", "trigger_ids", "action_ids"],
            "optional_fields": ["name", "priority", "reversible", "undo_action_ids"],
        },
        "dynamic_rule_engine": {
            "id_field": "rule_id",
            "compile_to": "dynamic",
            "required_fields": ["rule_id", "cond", "action_id"],
            "optional_fields": ["group", "priority", "reversible", "undo_action_id"],
        },
        "triggers": {
            "id_field": "trigger_id",
            "compile_to": "trigger",
            "required_fields": ["trigger_id", "trigger_type", "name"],
            "optional_fields": ["sensor_id", "sensor_type", "operator", "value",
                                "timer_id", "on", "expr", "trigger_ids", "state_type",
                                "attribute", "equals", "cooldown_s", "debounce_s"],
        },
        "actions": {
            "id_field": "action_id",
            "compile_to": "action",
            "required_fields": ["action_id", "action_type", "name"],
            "optional_fields": ["state_id", "value_id", "p_id", "p_ids", "parameters",
                                "message", "actuator_id", "value"],
        },
        "timers": {
            "id_field": "timer_id",
            "compile_to": "timer",
            "required_fields": ["timer_id", "type"],
            "optional_fields": ["duration_s", "interval_s", "at", "cron", "description"],
        },
        "events": {
            "id_field": "event_id",
            "compile_to": "event",
            "required_fields": ["event_id", "event_type"],
            "optional_fields": ["source_type", "priority", "description"],
        },
    },
    "custom_table_template": {
        "id_field": "<your_id_field>",
        "compile_to": "custom",
        "required_fields": ["<id_field>"],
        "optional_fields": [],
        "condition_kind": "custom",
        "handler_registration": "register_condition_handler('<kind>', handler_fn)",
    },
}

# Laufzeit-Schema: wird bei load_table_schema() ueberschrieben
_active_table_schema = None
_schema_lock = threading.Lock()


def load_table_schema(path_or_dict=None):
    """
    Table-Schema aus externer JSON-Datei oder Dict laden.

    Falls None: Built-in Default verwenden.
    Das geladene Schema wird als aktives Schema gesetzt und fuer
    validate_table() und get_table_schema() verwendet.
    """
    global _active_table_schema
    if path_or_dict is None:
        with _schema_lock:
            _active_table_schema = copy.deepcopy(_DEFAULT_TABLE_SCHEMA)
        return _active_table_schema

    if isinstance(path_or_dict, (str, Path)):
        schema = load_json(path_or_dict)
    elif isinstance(path_or_dict, dict):
        schema = copy.deepcopy(path_or_dict)
    else:
        raise TypeError("Erwartet str, Path, dict oder None")

    with _schema_lock:
        _active_table_schema = schema
    _log.info("Table-Schema geladen: %d Tabellen-Definitionen",
              len(schema.get("table_definitions", {})))
    return schema


def get_table_schema():
    """Aktives Table-Schema zurueckgeben (Kopie)."""
    with _schema_lock:
        if _active_table_schema is None:
            return copy.deepcopy(_DEFAULT_TABLE_SCHEMA)
        return copy.deepcopy(_active_table_schema)


def register_custom_table(table_name, id_field, required_fields, optional_fields=None):
    """
    Eigene Tabellen-Definition zum aktiven Schema hinzufuegen.

    Ermoeglicht beliebige Rule-Tables ohne Code-Aenderung.
    """
    global _active_table_schema
    with _schema_lock:
        if _active_table_schema is None:
            _active_table_schema = copy.deepcopy(_DEFAULT_TABLE_SCHEMA)
        _active_table_schema["table_definitions"][table_name] = {
            "id_field": id_field,
            "compile_to": "custom",
            "required_fields": list(required_fields),
            "optional_fields": list(optional_fields or []),
        }
    _log.info("Custom-Table registriert: %s (id=%s)", table_name, id_field)


def export_table_schema(path):
    """Aktives Table-Schema als JSON-Datei exportieren."""
    schema = get_table_schema()
    dump_json(path, schema)
    _log.info("Table-Schema exportiert nach: %s", path)


def validate_table(table_name, rows, schema=None):
    """Eine Tabelle gegen das Schema validieren."""
    if schema is None:
        schema = get_table_schema()
    table_def = schema.get("table_definitions", {}).get(table_name)
    if table_def is None:
        return {"valid": True, "errors": [], "warnings": ["Kein Schema fuer Tabelle '%s'" % table_name]}

    errors = []
    warnings = []
    id_field = table_def.get("id_field", "id")
    required = set(table_def.get("required_fields", []))
    optional = set(table_def.get("optional_fields", []))
    all_known = required | optional | {id_field}

    seen_ids = set()
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            errors.append("Zeile %d ist kein Dict" % idx)
            continue
        for field in required:
            if field not in row:
                errors.append("Zeile %d: Pflichtfeld '%s' fehlt" % (idx, field))
        # Duplikat-Check
        row_id = row.get(id_field)
        if row_id is not None:
            key = normalize_id(row_id)
            if key in seen_ids:
                errors.append("Zeile %d: Doppelte ID '%s'" % (idx, key))
            seen_ids.add(key)
        # Unbekannte Felder
        for key in row:
            if key not in all_known and key != id_field:
                warnings.append("Zeile %d: Unbekanntes Feld '%s'" % (idx, key))

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
#  Compile-Cache (optionaler, thread-safe Cache fuer kompilierte Configs)
# ---------------------------------------------------------------------------

_compile_cache = {}
_compile_cache_lock = threading.Lock()


def _config_hash(config_dict):
    """Deterministischen Hash einer Config berechnen."""
    try:
        raw = json.dumps(config_dict, sort_keys=True, ensure_ascii=True)
        return hash(raw)
    except (TypeError, ValueError):
        return None


def compile_config_cached(legacy_config, include_sources=None):
    """
    Compile mit Cache — identische Configs werden nicht erneut kompiliert.

    Spart CPU bei wiederholten Aufrufen mit unveraenderter Config.
    Gibt (compiled, cache_hit) zurueck.
    """
    config_key = _config_hash(legacy_config)
    # include_sources in Cache-Key einbeziehen (verschiedene Scopes = verschiedene Compiles)
    if config_key is not None and include_sources is not None:
        config_key = hash((config_key, tuple(sorted(include_sources))))
    if config_key is not None:
        with _compile_cache_lock:
            cached = _compile_cache.get(config_key)
        if cached is not None:
            _inc_stat("compile_cache_hits")
            return cached, True

    compiled = compile_config(legacy_config, include_sources=include_sources)
    if config_key is not None:
        with _compile_cache_lock:
            _compile_cache[config_key] = compiled
    return compiled, False


def clear_compile_cache():
    """Compile-Cache leeren."""
    with _compile_cache_lock:
        _compile_cache.clear()


# ---------------------------------------------------------------------------
#  Reset-Interface (für One-Shot-Regeln)
# ---------------------------------------------------------------------------

def reset_one_shot(runtime, rule_id=None):
    """One-Shot-Regeln zurücksetzen."""
    fired_once = runtime.get("rule_fired_once", {})
    if rule_id is not None:
        fired_once.pop(normalize_id(rule_id), None)
    else:
        fired_once.clear()


def reset_runtime(runtime=None):
    """Gesamten Runtime-State zurücksetzen."""
    return _ensure_runtime(None)


# ---------------------------------------------------------------------------
#  Modul-Initialisierung
# ---------------------------------------------------------------------------

_register_builtin_handlers()


# ---------------------------------------------------------------------------
#  Selbsttest
# ---------------------------------------------------------------------------

def _selftest():
    """Schneller Selbsttest der Library."""
    configure_logging(logging.DEBUG)

    # Test 1: Direkt mit Legacy-JSON
    import os
    test_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "config", "___device_state_data.json")

    # Test 2: Programmatische Config
    compiled_actions = [
        {"action_id": "a1", "action_type": "notify", "name": "alarm", "message": "Alarm!"},
        {"action_id": "a2", "action_type": "notify", "name": "ok", "message": "OK"},
    ]
    compiled_rules = [
        {
            "rule_id": "r1", "rule_name": "temp_check", "rule_type": "test",
            "enabled": True, "group": "test",
            "condition": {"kind": "sensor", "sensor_id": 1, "operator": ">", "value": 100.0},
            "action_refs": ["a1"], "else_action_refs": ["a2"], "undo_action_refs": [],
            "execution": {"priority": 100, "mode": "parallel", "cooldown_s": 0.0,
                          "one_shot": False, "reversible": False, "reset_mode": "auto"},
        },
        {
            "rule_id": "r2", "rule_name": "combined", "rule_type": "test",
            "enabled": True, "group": "test",
            "condition": {
                "kind": "all",
                "conditions": [
                    {"kind": "sensor", "sensor_id": 1, "operator": ">", "value": 50.0},
                    {"kind": "expression", "expr": "level > 30"},
                ],
            },
            "action_refs": ["a1"], "else_action_refs": [], "undo_action_refs": [],
            "execution": {"priority": 200, "mode": "parallel", "cooldown_s": 0.0,
                          "one_shot": False, "reversible": False, "reset_mode": "auto"},
        },
    ]
    config = {
        "schema_version": "2.0.0",
        "actions": compiled_actions, "triggers": [], "timers": [],
        "events": [], "engines": [], "rules": compiled_rules,
        "rule_sets": [], "activation_profiles": [], "catalog": {"states": []},
        "state_type_index": {},
    }
    ctx = create_engine_context(
        now_s=100.0,
        facts={"level": 45.0},
        sensor_values={"1": 120.0},
    )
    result = evaluate(config, ctx)
    assert len(result["matched_rules"]) == 2, "Erwartet 2, erhalten %d" % len(result["matched_rules"])
    assert len(result["planned_actions"]) >= 2

    # Test 3: Custom Condition
    def my_check(condition, rule, config, context):
        return condition.get("answer") == 42

    register_condition_handler("my_custom", my_check)
    custom_rule = {
        "rule_id": "r_custom", "rule_name": "custom", "rule_type": "test",
        "enabled": True, "group": "test",
        "condition": {"kind": "my_custom", "answer": 42},
        "action_refs": ["a1"], "else_action_refs": [], "undo_action_refs": [],
        "execution": {"priority": 100, "mode": "parallel", "cooldown_s": 0.0,
                      "one_shot": False, "reversible": False, "reset_mode": "auto"},
    }
    config2 = dict(config)
    config2["rules"] = [custom_rule]
    ctx2 = create_engine_context(now_s=200.0)
    result2 = evaluate(config2, ctx2)
    assert len(result2["matched_rules"]) == 1
    unregister_condition_handler("my_custom")

    print("=== SELFTEST BESTANDEN ===")
    print("  Matched Rules:", len(result["matched_rules"]))
    print("  Planned Actions:", len(result["planned_actions"]))
    print("  Custom Condition: OK")
    print("  Registered Kinds:", get_registered_condition_kinds())


if __name__ == "__main__":
    _selftest()
