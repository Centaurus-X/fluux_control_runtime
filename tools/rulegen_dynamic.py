"""
rulegen_dynamic.py
Erzeugt und validiert engine-konforme dynamic_rule_engine-RuleSets (ohne UI).

# für "async_process_state_management.py"

Features:
- Builder-API (Conditions: sensor/state_type/compound/EXPR; Actions: notify/config_update/state_override/emergency_shutdown)
- Validierung von Builder-Objekten (structural + semantisch engine-nah)
- Validierung roher Dicts/JSON (für GUI-Exporte) via validate_config_dict
- Test-Vektoren + run_selftests(), inkl. „mixed good/bad“-Fällen
- JSON-Schema-Snippet (dokumentarisch; keine externen Dependencies)

Konform zur aktuellen Engine:
- Condition-Typen: "sensor", "state_type", "compound (AND/OR)", String-Expr ("left OP right")
- Action-Typen: "notify", "config_update", "state_override", "emergency_shutdown"

Hinweise:
- Für Expressions (String) erwartet die Engine i. d. R. Leerzeichen um den Operator:
  Beispiel: "temperature >= target_temp*0.98"
- Für config_update IMMER state_id explizit setzen (sonst Heuristik in der Engine).
"""

import json

# ---------------------------------------------------------------------
# JSON-SCHEMA (Dokumentation; optional via get_json_schema_snippet()/write_schema())
# ---------------------------------------------------------------------

JSON_SCHEMA_SNIPPET = r'''{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "dynamic_rule_engine config",
  "type": "object",
  "required": ["dynamic_rule_engine"],
  "properties": {
    "dynamic_rule_engine": {
      "type": "object",
      "required": ["rule_sets"],
      "properties": {
        "rule_sets": {
          "type": "array",
          "minItems": 1,
          "items": {
            "type": "object",
            "required": ["set_id", "evaluation", "rules"],
            "properties": {
              "set_id": { "type": "string", "minLength": 1 },
              "priority": { "type": "integer" },
              "evaluation": { "type": "string", "enum": ["on_event", "continuous"] },
              "rules": {
                "type": "array",
                "minItems": 1,
                "items": {
                  "type": "object",
                  "required": ["rule_id", "condition", "action"],
                  "properties": {
                    "rule_id": { "type": "string", "minLength": 1 },
                    "condition": {
                      "oneOf": [
                        { "type": "string",
                          "pattern": "^\\S+\\s(==|!=|>=|<=|>|<)\\s.+$"
                        },
                        { "type": "object",
                          "required": ["type"],
                          "properties": {
                            "type": { "type": "string", "enum": ["sensor", "state_type", "compound"] },
                            "sensor_type": { "type": ["string", "null"] },
                            "sensor_id":   { "type": ["integer", "null"] },
                            "operator":    { "type": ["string", "null"], "enum": [">", ">=", "<", "<=", "==", "!="] },
                            "value":       {},
                            "state_type":  { "type": ["string", "null"] },
                            "attribute":   { "type": ["string", "null"] },
                            "equals":      { "type": ["boolean", "null"] },
                            "conditions": {
                              "type": "array",
                              "items": { "$ref": "#/definitions/conditionObj" },
                              "minItems": 1
                            }
                          },
                          "additionalProperties": true
                        }
                      ]
                    },
                    "action": {
                      "type": "object",
                      "required": ["type"],
                      "properties": {
                        "type": { "type": "string",
                                  "enum": ["notify", "config_update", "state_override", "emergency_shutdown"] },
                        "message": { "type": "string" },
                        "payload": {
                          "type": "object",
                          "properties": {
                            "state_id": { "type": "integer" },
                            "updates":  { "type": "object", "minProperties": 1 }
                          },
                          "required": ["state_id", "updates"]
                        },
                        "target_meta_state": { "type": "string" },
                        "reason": { "type": "string" }
                      },
                      "additionalProperties": true
                    }
                  },
                  "additionalProperties": true
                }
              }
            },
            "additionalProperties": true
          }
        }
      },
      "additionalProperties": true
    }
  },
  "additionalProperties": true,
  "definitions": {
    "conditionObj": {
      "type": "object",
      "required": ["type"],
      "properties": {
        "type": { "type": "string", "enum": ["sensor", "state_type", "compound"] },
        "sensor_type": { "type": ["string", "null"] },
        "sensor_id":   { "type": ["integer", "null"] },
        "operator":    { "type": ["string", "null"], "enum": [">", ">=", "<", "<=", "==", "!="] },
        "value":       {},
        "state_type":  { "type": ["string", "null"] },
        "attribute":   { "type": ["string", "null"] },
        "equals":      { "type": ["boolean", "null"] },
        "conditions": {
          "type": "array",
          "items": { "$ref": "#/definitions/conditionObj" },
          "minItems": 1
        }
      },
      "additionalProperties": true
    }
  }
}'''


# ============================
# Helper
# ============================

def get_json_schema_snippet():
    """Liefert das dokumentarische JSON-Schema als String."""
    return JSON_SCHEMA_SNIPPET

def write_schema(path):
    """Schreibt das Schema in eine Datei."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(JSON_SCHEMA_SNIPPET)

# ============================
# Human-readable Errors + Linter
# ============================

import re

def humanize_errors(errors):
    """
    Wandelt Fehlerstrings aus validate()/validate_config_dict() in strukturierte Objekte um:
    { code, message, hint, severity, path }
    - path: z. B. "ruleset:process_rules/rule:notify_heating_ready"
    - severity: "error" (immer), der Linter liefert "warning"/"info"
    """
    out = []
    if not isinstance(errors, list):
        return out
    for e in errors:
        item = {
            "code": "UNKNOWN",
            "message": str(e),
            "hint": None,
            "severity": "error",
            "path": None
        }

        # --- RuleSet-Ebene
        m = re.search(r"RuleSet\s+([^\:]+):\s+(.*)", e)
        if m:
            rs, rest = m.group(1), m.group(2)
            item["path"] = "ruleset:" + rs
            if "evaluation must be one of" in rest:
                item["code"] = "BAD_EVALUATION"
                item["hint"] = "Zulässig sind 'on_event' oder 'continuous'."
            elif "rules must not be empty" in rest:
                item["code"] = "EMPTY_RULESET"
                item["hint"] = "Mindestens eine Regel je RuleSet anlegen."
            out.append(item); continue

        # --- Duplicate rule_id
        m = re.search(r"Duplicate rule_id '([^']+)'\s+\(first in ([^)]+)\)", e)
        if m:
            rid, rs = m.group(1), m.group(2)
            item["code"] = "DUP_RULE_ID"
            item["path"] = f"ruleset:{rs}/rule:{rid}"
            item["hint"] = "rule_id muss global eindeutig sein."
            out.append(item); continue

        # --- rule in set ... (Dict-Validator)
        m = re.search(r"rule in set ([^:]+):\s+(.*)", e)
        if m:
            rs, rest = m.group(1), m.group(2)
            item["path"] = "ruleset:" + rs
            if "rule_id must be non-empty string" in rest:
                item["code"] = "MISSING_RULE_ID"
                item["hint"] = "Vergib einen eindeutigen rule_id-String."
            elif "must be object" in rest:
                item["code"] = "RULE_NOT_OBJECT"
                item["hint"] = "Regeln müssen Objekte sein (kein Array/kein String)."
            out.append(item); continue

        # --- Rule {id}: ...
        m = re.search(r"Rule\s+([^\:]+):\s+(.*)", e)
        if m:
            rid, rest = m.group(1), m.group(2)
            item["path"] = f"rule:{rid}"
            # Condition-Fehler
            if "sensor condition needs sensor_type or sensor_id" in rest:
                item["code"] = "SENSOR_IDENT_MISSING"
                item["hint"] = "sensor_type oder sensor_id setzen."
            elif "unsupported operator" in rest or "operator '" in rest and "not allowed" in rest:
                item["code"] = "BAD_OPERATOR"
                item["hint"] = "Erlaubt: >, >=, <, <=, ==, !="
            elif "expr condition must look like" in rest or "string condition must look like" in rest:
                item["code"] = "BAD_EXPR_FORMAT"
                item["hint"] = "Format: 'left OP right' – Operator mit Leerzeichen umgeben."
            elif "unknown condition type" in rest:
                item["code"] = "UNKNOWN_CONDITION"
                item["hint"] = "Zulässig: sensor, state_type, compound, oder Ausdruck als String."
            # Action-Fehler
            elif "config_update requires explicit state_id" in rest or "payload.state_id required" in rest:
                item["code"] = "STATE_ID_REQUIRED"
                item["hint"] = "Bei config_update immer state_id explizit setzen."
            elif "updates must be" in rest:
                item["code"] = "EMPTY_UPDATES"
                item["hint"] = "payload.updates muss ein nicht-leeres Objekt sein."
            elif "state_override.target_meta_state required" in rest:
                item["code"] = "TARGET_META_REQUIRED"
                item["hint"] = "Meta-Ziel (z. B. ERROR/STOP) angeben."
            elif "unsupported action" in rest:
                item["code"] = "UNSUPPORTED_ACTION"
            out.append(item); continue

        # --- rule_set.evaluation must be one of ...
        if "rule_set.evaluation must be one of" in e:
            item["code"] = "BAD_EVALUATION"
            item["hint"] = "Zulässig sind 'on_event' oder 'continuous'."
            out.append(item); continue

        # --- Fallback
        out.append(item)

    return out


def lint_config_dict(obj):
    """
    Leichte Hinweise (nicht-fatal) zu Stil/Konventionen.
    Rückgabe: Liste von dicts {code, message, severity, path, hint}
    """
    out = []
    if not isinstance(obj, dict):
        return out

    dre = obj.get("dynamic_rule_engine") or {}
    rsets = dre.get("rule_sets") or []
    for rs in rsets:
        sid = rs.get("set_id") or "?"
        # Warnung: priority wird derzeit in der Engine nicht genutzt
        if "priority" in rs:
            out.append({
                "code": "PRIORITY_IGNORED",
                "message": "RuleSet.priority wird aktuell von der Engine nicht ausgewertet.",
                "severity": "warning",
                "path": f"ruleset:{sid}",
                "hint": "Kann gesetzt werden (Dokumentation), hat aber derzeit keine Wirkung."
            })

        # Info: sehr lange String-Expressions sind fehleranfällig
        for r in rs.get("rules") or []:
            rid = r.get("rule_id") or "?"
            cond = r.get("condition")
            if isinstance(cond, str) and len(cond) > 80:
                out.append({
                    "code": "EXPR_LONG",
                    "message": "Sehr lange Ausdrucks-Condition – besser in sensor/state_type/compound zerlegen.",
                    "severity": "info",
                    "path": f"ruleset:{sid}/rule:{rid}",
                    "hint": "Komplexe Logik als compound(AND/OR) modellieren; besser wartbar."
                })

    return out


# ============================
# GUI-Unterstützung: Enums + Hilfetexte
# ============================

def enum_lists_for_gui(config=None, extras=None):
    """
    Liefert Dropdown-Listen (value+label) für GUI-Felder.
    - Liest state_types aus config["process_states"][*]["p__type"]
    - Liest sensor_types aus dynamic_rule_engine.rule_sets[*].rules[*].condition.sensor_type (rekursiv)
    - extras kann optionale Ergänzungen liefern: {"sensor_types": [...], "state_types": [...]}

    Rückgabe (dict):
        {
          "evaluation": [{"value":"on_event","label":"bei neuen Sensordaten"}, ...],
          "operators":  [{"value":">=","label":">="}, ...],
          "action_types": [{"value":"notify","label":"Benachrichtigung"}, ...],
          "sensor_types": [{"value":"temperature","label":"Temperature"}, ...],
          "state_types":  [{"value":"heating","label":"Heating"}, ...]
        }
    """
    def _lbl(s):
        if not s: return ""
        # simple „Humanize“: erster Buchstabe groß – bleibt tech-lastig, aber sauber
        return str(s[:1].upper() + s[1:])

    # feste Enums (engine-konform)
    evaluation = [
        {"value": "on_event",   "label": "bei neuen Sensordaten"},
        {"value": "continuous", "label": "periodisch (Heartbeat)"}
    ]
    operators = [{"value": op, "label": op} for op in [">", ">=", "<", "<=", "==", "!="]]
    action_types = [
        {"value": "notify",             "label": "Benachrichtigung"},
        {"value": "config_update",      "label": "Parameter-Update (State)"},
        {"value": "state_override",     "label": "Meta-Zustand setzen (ALLE States)"},
        {"value": "emergency_shutdown", "label": "Not-Aus (ALLE States STOP)"}
    ]

    # dynamisch aus config ermitteln
    sensor_types = set()
    state_types  = set()

    if isinstance(config, dict):
        # process_states → state_types
        for ps in config.get("process_states", []) or []:
            try:
                st = ps.get("p__type")
                if st: state_types.add(str(st))
            except Exception:
                pass

        # dynamic_rule_engine → sensor_types rekursiv sammeln
        def _collect_sensor_types(cond):
            if cond is None:
                return
            if isinstance(cond, dict):
                ctype = str(cond.get("type", "")).lower()
                if ctype == "sensor":
                    st = cond.get("sensor_type")
                    if st: sensor_types.add(str(st))
                elif ctype == "compound":
                    for sub in cond.get("conditions", []) or []:
                        _collect_sensor_types(sub)
            elif isinstance(cond, str):
                # optional: einfache Heuristik aus Ausdruck "temperature >= ..."
                parts = cond.split(" ")
                if len(parts) >= 3:
                    left = parts[0].strip()
                    # whitelist häufiger Namen
                    if left in ("temperature", "level", "pressure", "speed", "reflux"):
                        sensor_types.add(left)

        dre = (config.get("dynamic_rule_engine") or {})
        for rs in dre.get("rule_sets", []) or []:
            for r in rs.get("rules", []) or []:
                _collect_sensor_types(r.get("condition"))

    # fallback-typische Sensoren
    if not sensor_types:
        sensor_types.update(["temperature", "level", "pressure", "speed", "reflux"])

    # extras mergen
    if isinstance(extras, dict):
        for k in ("sensor_types", "state_types"):
            if k in extras and isinstance(extras[k], (list, set, tuple)):
                if k == "sensor_types":
                    sensor_types.update([str(x) for x in extras[k]])
                else:
                    state_types.update([str(x) for x in extras[k]])

    return {
        "evaluation":   evaluation,
        "operators":    operators,
        "action_types": action_types,
        "sensor_types": [{"value": s, "label": _lbl(s)} for s in sorted(sensor_types)],
        "state_types":  [{"value": s, "label": _lbl(s)} for s in sorted(state_types)]
    }


def gui_help_texts():
    """
    Kurztexte für Tooltips/Erklärungen im GUI.
    """
    return {
        "evaluation.on_event":   "Regelsatz wird geprüft, wenn neue Sensordaten eintreffen.",
        "evaluation.continuous": "Regelsatz wird periodisch beim Heartbeat geprüft.",
        "action.notify":         "Versendet eine Benachrichtigung; keine Zustandsänderung.",
        "action.config_update":  "Schreibt differenziell in process_states[state_id] (value_id, mode_id, automation_parameters).",
        "action.state_override": "Setzt den Meta-Zustand ALLER States (z. B. ERROR/STOP).",
        "action.emergency_shutdown": "Setzt alle States auf active=False und Meta STOP; Aktoren gehen aus.",
        "cond.sensor":           "Vergleich eines Sensors (per sensor_type oder sensor_id) mit Wert/Expression.",
        "cond.state_type":       "Prüft Attribute eines State-Typs (z. B. heating.active == true).",
        "cond.compound":         "Logische Verknüpfung (AND/OR) mehrerer Bedingungen.",
        "expr.format":           "Ausdruck als String: 'left OP right', mit Leerzeichen um den Operator."
    }


# ---------------------------------------------------------------------
# Core-Definitionen/Builder
# ---------------------------------------------------------------------

_ALLOWED_OPS = {">", ">=", "<", "<=", "==", "!="}
_ALLOWED_EVAL = {"on_event", "continuous"}
_ALLOWED_ACTIONS = {"notify", "config_update", "state_override", "emergency_shutdown"}

class Condition:
    def to_obj(self):
        raise NotImplementedError("Condition.to_obj muss überschrieben werden")

class SensorCond(Condition):
    def __init__(self, sensor_type=None, sensor_id=None, operator=None, value=None):
        self.sensor_type = sensor_type
        self.sensor_id = sensor_id
        self.operator = operator
        self.value = value

    def to_obj(self):
        obj = {"type": "sensor", "operator": self.operator, "value": self.value}
        if self.sensor_id is not None:
            obj["sensor_id"] = int(self.sensor_id)
        if self.sensor_type is not None:
            obj["sensor_type"] = str(self.sensor_type)
        return obj

class StateTypeCond(Condition):
    def __init__(self, state_type, attribute="active", equals=True):
        self.state_type = str(state_type)
        self.attribute = str(attribute)
        self.equals = bool(equals)

    def to_obj(self):
        return {
            "type": "state_type",
            "state_type": self.state_type,
            "attribute": self.attribute,
            "equals": self.equals
        }

class CompoundCond(Condition):
    def __init__(self, operator, *conditions):
        self.operator = str(operator).upper()
        self.conditions = list(conditions or [])

    def to_obj(self):
        return {
            "type": "compound",
            "operator": self.operator,
            "conditions": [c.to_obj() if isinstance(c, Condition) else c for c in self.conditions]
        }

class ExprCond(Condition):
    def __init__(self, expression):
        self.expression = str(expression)

    def to_obj(self):
        return self.expression

def sensor_type(name):
    return _SensorTypeBuilder(name)

def sensor_id(sid):
    return _SensorIdBuilder(sid)

class _SensorTypeBuilder:
    def __init__(self, name):
        self.name = str(name)

    def _cmp(self, op, value):
        return SensorCond(sensor_type=self.name, operator=op, value=value)

    def gt(self, value):  return self._cmp(">", value)
    def ge(self, value):  return self._cmp(">=", value)
    def lt(self, value):  return self._cmp("<", value)
    def le(self, value):  return self._cmp("<=", value)
    def eq(self, value):  return self._cmp("==", value)
    def ne(self, value):  return self._cmp("!=", value)

class _SensorIdBuilder:
    def __init__(self, sid):
        self.sid = int(sid)

    def _cmp(self, op, value):
        return SensorCond(sensor_id=self.sid, operator=op, value=value)

    def gt(self, value):  return self._cmp(">", value)
    def ge(self, value):  return self._cmp(">=", value)
    def lt(self, value):  return self._cmp("<", value)
    def le(self, value):  return self._cmp("<=", value)
    def eq(self, value):  return self._cmp("==", value)
    def ne(self, value):  return self._cmp("!=", value)

def state_active(state_type_name, equals=True):
    return StateTypeCond(state_type=state_type_name, attribute="active", equals=equals)

def state_attr_eq(state_type_name, attribute, expected):
    return StateTypeCond(state_type=state_type_name, attribute=attribute, equals=expected)

def AND(*conds):
    return CompoundCond("AND", *conds)

def OR(*conds):
    return CompoundCond("OR", *conds)

def EXPR(expression):
    return ExprCond(expression)

class Action:
    def to_obj(self):
        raise NotImplementedError("Action.to_obj muss überschrieben werden")

class Notify(Action):
    def __init__(self, message):
        self.message = str(message)

    def to_obj(self):
        return {"type": "notify", "message": self.message}

class ConfigUpdate(Action):
    def __init__(self, state_id, updates):
        self.state_id = int(state_id)
        self.updates = dict(updates or {})

    def to_obj(self):
        return {
            "type": "config_update",
            "payload": {
                "state_id": self.state_id,
                "updates": self.updates
            }
        }

class StateOverride(Action):
    def __init__(self, target_meta_state, reason="rule_override"):
        self.target_meta_state = str(target_meta_state)
        self.reason = str(reason)

    def to_obj(self):
        return {
            "type": "state_override",
            "target_meta_state": self.target_meta_state,
            "reason": self.reason
        }

class EmergencyShutdown(Action):
    def to_obj(self):
        return {"type": "emergency_shutdown"}

class Rule:
    def __init__(self, rule_id, condition, action):
        self.rule_id = str(rule_id)
        self.condition = condition
        self.action = action

    def to_obj(self):
        cond = self.condition.to_obj() if isinstance(self.condition, Condition) else self.condition
        act  = self.action.to_obj() if isinstance(self.action, Action) else self.action
        return {"rule_id": self.rule_id, "condition": cond, "action": act}

class RuleSet:
    def __init__(self, set_id, evaluation="on_event", priority=None):
        self.set_id = str(set_id)
        self.evaluation = str(evaluation)
        self.priority = int(priority) if priority is not None else None
        self.rules = []

    def add(self, rule):
        self.rules.append(rule)
        return self

    def to_obj(self):
        obj = {
            "set_id": self.set_id,
            "evaluation": self.evaluation,
            "rules": [r.to_obj() for r in self.rules]
        }
        if self.priority is not None:
            obj["priority"] = self.priority
        return obj

class RuleEngineConfig:
    def __init__(self):
        self.rule_sets = []

    def add_ruleset(self, ruleset):
        self.rule_sets.append(ruleset)
        return self

    def validate(self):
        errs = []
        seen_rule_ids = {}
        i = 0
        while i < len(self.rule_sets):
            rs = self.rule_sets[i]; i += 1
            if rs.evaluation not in _ALLOWED_EVAL:
                errs.append(f"RuleSet {rs.set_id}: evaluation must be one of {_ALLOWED_EVAL}")
            if not rs.rules:
                errs.append(f"RuleSet {rs.set_id}: rules must not be empty")

            j = 0
            while j < len(rs.rules):
                r = rs.rules[j]; j += 1
                if r.rule_id in seen_rule_ids:
                    errs.append(f"Duplicate rule_id '{r.rule_id}' (first in {seen_rule_ids[r.rule_id]})")
                else:
                    seen_rule_ids[r.rule_id] = rs.set_id
                errs.extend(_validate_condition_obj(r.rule_id, r.condition))
                errs.extend(_validate_action_obj(r.rule_id, r.action))
        return errs

    def to_obj(self):
        return {"dynamic_rule_engine": {"rule_sets": [rs.to_obj() for rs in self.rule_sets]}}

    def to_json(self, indent=2):
        return json.dumps(self.to_obj(), ensure_ascii=False, indent=indent)

# ---------------------------------------------------------------------
# Validatoren (auch für rohe Dicts/JSON)
# ---------------------------------------------------------------------

def validate_config_dict(obj):
    errs = []
    if not isinstance(obj, dict):
        return ["Top-level must be an object/dict"]

    dre = obj.get("dynamic_rule_engine")
    if not isinstance(dre, dict):
        return ["Missing/invalid 'dynamic_rule_engine' (object expected)"]

    rsets = dre.get("rule_sets")
    if not isinstance(rsets, list) or len(rsets) == 0:
        return ["'dynamic_rule_engine.rule_sets' must be a non-empty array"]

    seen_rule_ids = {}
    i_rs = 0
    while i_rs < len(rsets):
        rs = rsets[i_rs]; i_rs += 1
        if not isinstance(rs, dict):
            errs.append("Each rule_set must be an object")
            continue

        set_id = rs.get("set_id")
        evaluation = rs.get("evaluation")
        rules = rs.get("rules")

        if not set_id or not isinstance(set_id, str):
            errs.append("rule_set.set_id must be a non-empty string")
        if evaluation not in _ALLOWED_EVAL:
            errs.append(f"rule_set.evaluation must be one of {_ALLOWED_EVAL}")
        if not isinstance(rules, list) or len(rules) == 0:
            errs.append(f"rule_set[{set_id or '?'}].rules must be a non-empty array")
            continue

        k = 0
        while k < len(rules):
            r = rules[k]; k += 1
            if not isinstance(r, dict):
                errs.append(f"rule in set {set_id}: must be object")
                continue
            rid = r.get("rule_id")
            cond = r.get("condition")
            act  = r.get("action")

            if not rid or not isinstance(rid, str):
                errs.append(f"rule in set {set_id}: rule_id must be non-empty string")
            else:
                if rid in seen_rule_ids:
                    errs.append(f"Duplicate rule_id '{rid}' (first in {seen_rule_ids[rid]})")
                else:
                    seen_rule_ids[rid] = set_id

            errs.extend(_validate_condition_dict(rid or "?", cond))
            errs.extend(_validate_action_dict(rid or "?", act))

    return errs

def _validate_condition_obj(rule_id, cond):
    errs = []
    if isinstance(cond, SensorCond):
        if cond.operator not in _ALLOWED_OPS:
            errs.append(f"Rule {rule_id}: unsupported operator {cond.operator}")
        if cond.sensor_type is None and cond.sensor_id is None:
            errs.append(f"Rule {rule_id}: sensor condition needs sensor_type or sensor_id")
        if cond.value is None:
            errs.append(f"Rule {rule_id}: sensor condition needs a value/expression")
    elif isinstance(cond, CompoundCond):
        if cond.operator not in ("AND", "OR"):
            errs.append(f"Rule {rule_id}: compound.operator must be AND/OR")
        if not cond.conditions:
            errs.append(f"Rule {rule_id}: compound requires nested conditions")
    elif isinstance(cond, StateTypeCond):
        if not cond.state_type:
            errs.append(f"Rule {rule_id}: state_type condition missing state_type")
    elif isinstance(cond, ExprCond):
        s = str(cond.to_obj())
        parts = s.split(" ")
        if len(parts) < 3:
            errs.append(f"Rule {rule_id}: expr condition must look like 'left OP right'")
        else:
            op = parts[1]
            if op not in _ALLOWED_OPS:
                errs.append(f"Rule {rule_id}: expr operator '{op}' not allowed")
    elif isinstance(cond, str):
        parts = cond.split(" ")
        if len(parts) < 3:
            errs.append(f"Rule {rule_id}: string condition must look like 'left OP right'")
        else:
            op = parts[1]
            if op not in _ALLOWED_OPS:
                errs.append(f"Rule {rule_id}: string expr operator '{op}' not allowed")
    elif isinstance(cond, dict):
        ctype = str(cond.get("type", ""))
        if ctype == "sensor":
            op = cond.get("operator")
            if op not in _ALLOWED_OPS:
                errs.append(f"Rule {rule_id}: sensor.operator must be one of {_ALLOWED_OPS}")
            if cond.get("sensor_type") is None and cond.get("sensor_id") is None:
                errs.append(f"Rule {rule_id}: sensor requires sensor_type or sensor_id")
            if "value" not in cond:
                errs.append(f"Rule {rule_id}: sensor requires value")
        elif ctype == "state_type":
            if not cond.get("state_type"):
                errs.append(f"Rule {rule_id}: state_type requires state_type")
        elif ctype == "compound":
            op = str(cond.get("operator", "")).upper()
            if op not in ("AND", "OR"):
                errs.append(f"Rule {rule_id}: compound.operator must be AND/OR")
            sub = cond.get("conditions", [])
            if not isinstance(sub, list) or len(sub) == 0:
                errs.append(f"Rule {rule_id}: compound.conditions must be non-empty list")
        else:
            errs.append(f"Rule {rule_id}: unknown condition type '{ctype}'")
    else:
        errs.append(f"Rule {rule_id}: unsupported condition type {type(cond)}")
    return errs

def _validate_action_obj(rule_id, act):
    errs = []
    if isinstance(act, Notify):
        if not act.message:
            errs.append(f"Rule {rule_id}: notify.message must be non-empty")
    elif isinstance(act, ConfigUpdate):
        if act.state_id is None:
            errs.append(f"Rule {rule_id}: config_update requires explicit state_id")
        if not isinstance(act.updates, dict) or not act.updates:
            errs.append(f"Rule {rule_id}: config_update.updates must be a non-empty dict")
    elif isinstance(act, StateOverride):
        if not act.target_meta_state:
            errs.append(f"Rule {rule_id}: state_override.target_meta_state required")
    elif isinstance(act, EmergencyShutdown):
        pass
    elif isinstance(act, dict):
        atype = str(act.get("type", ""))
        if atype not in _ALLOWED_ACTIONS:
            errs.append(f"Rule {rule_id}: unsupported action.type '{atype}'")
        if atype == "notify":
            if not act.get("message"):
                errs.append(f"Rule {rule_id}: notify.message must be non-empty")
        if atype == "config_update":
            p = act.get("payload", {})
            if not isinstance(p, dict):
                errs.append(f"Rule {rule_id}: config_update.payload must be object")
            else:
                if "state_id" not in p:
                    errs.append(f"Rule {rule_id}: config_update.payload.state_id required")
                u = p.get("updates")
                if not isinstance(u, dict) or not u:
                    errs.append(f"Rule {rule_id}: config_update.payload.updates must be non-empty dict")
        if atype == "state_override":
            if not act.get("target_meta_state"):
                errs.append(f"Rule {rule_id}: state_override.target_meta_state required")
    else:
        errs.append(f"Rule {rule_id}: unsupported action type {type(act)}")
    return errs

def _validate_condition_dict(rule_id, cond):
    if isinstance(cond, (str, dict)):
        return _validate_condition_obj(rule_id, cond)
    if isinstance(cond, list):
        return [f"Rule {rule_id}: condition must be object/string, not list"]
    if cond is None:
        return [f"Rule {rule_id}: condition missing"]
    return []

def _validate_action_dict(rule_id, act):
    if isinstance(act, dict):
        return _validate_action_obj(rule_id, act)
    if act is None:
        return [f"Rule {rule_id}: action missing"]
    return [f"Rule {rule_id}: action must be object"]

# ---------------------------------------------------------------------
# Test-Vektoren + Selftests (inkl. mixed good/bad)
# ---------------------------------------------------------------------

def _tv_valid_minimal():
    rs = RuleSet("process_rules", evaluation="on_event", priority=5)
    rs.add(Rule(
        "notify_heating_ready",
        AND(sensor_type("temperature").ge("target_temp*0.98"), state_active("heating")),
        Notify("Heating reached nominal range. Consider starting stirring.")
    ))
    cfg = RuleEngineConfig().add_ruleset(rs)
    return cfg

def _tv_valid_param_update():
    rs = RuleSet("process_rules", evaluation="on_event")
    rs.add(Rule(
        "boost_kp",
        sensor_type("temperature").ge("target_temp*0.95"),
        ConfigUpdate(state_id=1, updates={"automation_parameters": {"kp": 1.8}})
    ))
    return RuleEngineConfig().add_ruleset(rs)

def _tv_valid_emergency():
    rs = RuleSet("safety", evaluation="on_event")
    rs.add(Rule(
        "overpressure_stop",
        sensor_type("pressure").gt("max_pressure"),
        EmergencyShutdown()
    ))
    return RuleEngineConfig().add_ruleset(rs)

def _tv_invalid_missing_state_id():
    rs = RuleSet("bad", evaluation="on_event")
    bad_action = {"type": "config_update", "payload": {"updates": {"value_id": 2}}}
    rs.add(Rule("bad_cfg_update", sensor_type("temperature").ge(100), bad_action))
    return RuleEngineConfig().add_ruleset(rs)

def _tv_invalid_expr_spaces():
    rs = RuleSet("bad_expr", evaluation="on_event")
    rs.add(Rule("expr_no_spaces", EXPR("temperature>=target_temp*0.9"), Notify("x")))
    return RuleEngineConfig().add_ruleset(rs)

def _tv_invalid_empty_ruleset():
    rs = RuleSet("empty", evaluation="on_event")
    cfg = RuleEngineConfig().add_ruleset(rs)
    return cfg

def _tv_invalid_unknown_op():
    rs = RuleSet("bad_op", evaluation="on_event")
    # erzeuge absichtlich einen unbekannten Operator ">>"
    rs.add(Rule("bad_op_rule", sensor_type("temperature")._cmp(">>", 42), Notify("x")))
    return RuleEngineConfig().add_ruleset(rs)

def _tv_mixed_good_bad_ruleset_builder():
    """
    Ein RuleSet mit einer gültigen und einer ungültigen Regel (Builder-Variante).
    Erwartung: Validator findet mind. einen Fehler.
    """
    rs = RuleSet("mixed_builder", evaluation="on_event")
    # gültig
    rs.add(Rule("ok_notify",
                AND(sensor_type("level").ge(80), state_active("filling")),
                Notify("Level ok")))
    # ungültig: config_update ohne state_id
    bad_action = {"type": "config_update", "payload": {"updates": {"mode_id": 2}}}
    rs.add(Rule("bad_missing_state_id",
                sensor_type("temperature").ge(50),
                bad_action))
    return RuleEngineConfig().add_ruleset(rs)

def _tv_mixed_good_bad_dict():
    """
    Mixed good/bad als reines Dict (simuliert GUI-Export).
    """
    cfg = {
        "dynamic_rule_engine": {
            "rule_sets": [{
                "set_id": "mixed_dict",
                "evaluation": "on_event",
                "rules": [
                    { "rule_id": "ok",
                      "condition": { "type":"sensor", "sensor_type":"temperature", "operator":">=", "value":"target_temp*0.9" },
                      "action": { "type":"notify", "message":"ok" } },
                    { "rule_id": "bad_no_payload",
                      "condition": "pressure > max_pressure",
                      "action": { "type":"config_update", "payload": {} } }
                ]
            }]
        }
    }
    return cfg

def run_selftests():
    """
    Führt eine kleine Suite über beide Validatoren:
    - Builder-Validierung (RuleEngineConfig.validate)
    - Dict-Validierung (validate_config_dict auf .to_obj() bzw. rohem Dict)
    """
    tests = []

    def add_builder(name, maker, expect_errors):
        tests.append({"name": name, "kind": "builder", "maker": maker, "expect_errors": bool(expect_errors)})

    def add_dict(name, maker, expect_errors):
        tests.append({"name": name, "kind": "dict", "maker": maker, "expect_errors": bool(expect_errors)})

    # gültige
    add_builder("valid_minimal", _tv_valid_minimal, False)
    add_builder("valid_param_update", _tv_valid_param_update, False)
    add_builder("valid_emergency", _tv_valid_emergency, False)

    # ungültige
    add_builder("invalid_missing_state_id", _tv_invalid_missing_state_id, True)
    add_builder("invalid_expr_spaces", _tv_invalid_expr_spaces, True)
    add_builder("invalid_empty_ruleset", _tv_invalid_empty_ruleset, True)
    add_builder("invalid_unknown_op", _tv_invalid_unknown_op, True)

    # mixed good/bad
    add_builder("mixed_good_bad_builder", _tv_mixed_good_bad_ruleset_builder, True)
    add_dict("mixed_good_bad_dict", _tv_mixed_good_bad_dict, True)

    results = []
    i = 0
    while i < len(tests):
        t = tests[i]; i += 1
        name = t["name"]
        kind = t["kind"]
        maker = t["maker"]
        expect_err = t["expect_errors"]

        if kind == "builder":
            cfg = maker()
            errs_builder = cfg.validate()
            errs_dict = validate_config_dict(cfg.to_obj())
        else:
            raw = maker()
            # für den Dict-Test haben wir kein Builder-Objekt:
            errs_builder = ["(skip: dict-only)"]
            errs_dict = validate_config_dict(raw)

        ok_builder = (errs_builder == [] or errs_builder == ["(skip: dict-only)"])
        ok_dict = (len(errs_dict) == 0)

        passed = (not expect_err and ok_builder and ok_dict) or (expect_err and (not ok_builder or not ok_dict))

        results.append({
            "name": name,
            "kind": kind,
            "passed": passed,
            "builder_ok": ok_builder,
            "dict_ok": ok_dict,
            "builder_errors": errs_builder,
            "dict_errors": errs_dict
        })

    summary = {
        "total": len(results),
        "passed": sum(1 for r in results if r["passed"]),
        "failed": [r for r in results if not r["passed"]],
        "details": results
    }
    return summary

# ---------------------------------------------------------------------
# Optional: kleine Demo
# ---------------------------------------------------------------------

def _demo_print():
    cfg = _tv_valid_minimal()
    errors = cfg.validate()
    if errors:
        print("VALIDATE ERRORS:", errors)
    else:
        print(cfg.to_json())

if __name__ == "__main__":
    # Demo + Selftests
    _demo_print()
    s = run_selftests()
    print(json.dumps(s, indent=2))



# ---------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------
"""
# 1) Enums/Hilfen für GUI:
enums = enum_lists_for_gui(config=my_config, extras={"sensor_types": ["flow"]})
helps = gui_help_texts()

# 2) Builder → Validierung → Humanize:
cfg = RuleEngineConfig() \
    .add_ruleset(
        RuleSet("process_rules", evaluation="on_event", priority=5)
        .add(Rule("notify_ok",
                  AND(sensor_type("temperature").ge("target_temp*0.98"), state_active("heating")),
                  Notify("ok")))
        .add(Rule("bad_missing_state_id",
                  sensor_type("temperature").ge(100),
                  {"type":"config_update","payload":{"updates":{"value_id":2}}}))
    )

builder_errs = cfg.validate()
humanized = humanize_errors(builder_errs)

# 3) Dict-Validierung + Lint auf dem Export:
as_dict   = cfg.to_obj()
dict_errs = validate_config_dict(as_dict)
lint      = lint_config_dict(as_dict)

# → Im GUI: humanized + humanize_errors(dict_errs) + lint anzeigen
"""
