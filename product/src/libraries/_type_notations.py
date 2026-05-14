# -*- coding: utf-8 -*-

# src/libraries/_type_notations.py


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

import json
import re
import time
import datetime
from datetime import timezone
from functools import partial, reduce
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple, Union


# =============================================================================
# SECTION 1: PRIMITIVE TYPE-CHECKER FUNKTIONEN
# =============================================================================
# Alle Typ-Checker sind Funktionen: (value: Any) -> bool

def _is_type(expected: type, value: Any) -> bool:
    """Basis-Typprüfung via isinstance."""
    return isinstance(value, expected)

def _is_optional(expected: type, value: Any) -> bool:
    """Optionaler Typ - None ist explizit erlaubt."""
    return value is None or isinstance(value, expected)

def _is_list_of(item_checker: Callable, value: Any) -> bool:
    """Homogene Liste: alle Elemente müssen item_checker bestehen."""
    return isinstance(value, list) and all(item_checker(item) for item in value)

def _is_dict_with_str_keys(value: Any) -> bool:
    """Dict mit String-Keys."""
    return isinstance(value, dict) and all(isinstance(k, str) for k in value)

def _is_union(checkers: list, value: Any) -> bool:
    """Union-Typ: mindestens ein Checker muss True ergeben."""
    return any(checker(value) for checker in checkers)

def _is_literal(allowed_values: frozenset, value: Any) -> bool:
    """Literal-Typ: Wert muss in der erlaubten Menge sein."""
    return value in allowed_values

def _is_range(min_val: Any, max_val: Any, value: Any) -> bool:
    """Numerischer Bereich: min_val <= value <= max_val."""
    return isinstance(value, (int, float)) and min_val <= value <= max_val

def _is_non_empty_str(value: Any) -> bool:
    """Nicht-leerer String."""
    return isinstance(value, str) and len(value) > 0

def _is_ip_address(value: Any) -> bool:
    """Einfache IPv4-Prüfung."""
    if not isinstance(value, str):
        return False
    return bool(re.match(r"^\d{1,3}(\.\d{1,3}){3}$", value))

def _is_positive_int(value: Any) -> bool:
    """Ganzzahl > 0."""
    return isinstance(value, int) and value > 0

def _is_non_negative_int(value: Any) -> bool:
    """Ganzzahl >= 0."""
    return isinstance(value, int) and value >= 0

def _accepts_any(_value: Any) -> bool:
    """Akzeptiert jeden Wert (Any-Typ)."""
    return True

# --- Vordefinierte Typ-Checker (über partial gebunden) ---
is_int            = partial(_is_type, int)
is_str            = partial(_is_type, str)
is_float          = partial(_is_type, (int, float))
is_bool           = partial(_is_type, bool)
is_list           = partial(_is_type, list)
is_dict           = partial(_is_type, dict)
is_tuple          = partial(_is_type, tuple)
is_set            = partial(_is_type, (set, frozenset))
is_bytes          = partial(_is_type, bytes)
is_none           = partial(_is_type, type(None))
is_path           = partial(_is_type, Path)
is_datetime       = partial(_is_type, datetime.datetime)
is_date           = partial(_is_type, datetime.date)
is_any            = _accepts_any

is_optional_int      = partial(_is_optional, int)
is_optional_str      = partial(_is_optional, str)
is_optional_float    = partial(_is_optional, (int, float))
is_optional_bool     = partial(_is_optional, bool)
is_optional_list     = partial(_is_optional, list)
is_optional_dict     = partial(_is_optional, dict)
is_optional_bytes    = partial(_is_optional, bytes)
is_optional_path     = partial(_is_optional, Path)
is_optional_datetime = partial(_is_optional, datetime.datetime)

is_int_or_str   = partial(_is_union, [partial(_is_type, int), partial(_is_type, str)])
is_numeric      = partial(_is_union, [partial(_is_type, int), partial(_is_type, float)])
is_non_empty_str = _is_non_empty_str
is_ip_address    = _is_ip_address

list_of_int    = partial(_is_list_of, partial(_is_type, int))
list_of_str    = partial(_is_list_of, partial(_is_type, str))
list_of_float  = partial(_is_list_of, partial(_is_union, [partial(_is_type, int), partial(_is_type, float)]))
list_of_dict   = partial(_is_list_of, partial(_is_type, dict))
list_of_any    = partial(_is_type, list)
list_of_bool   = partial(_is_list_of, partial(_is_type, bool))

modbus_address_range = partial(_is_range, 0, 65535)
tcp_port_range       = partial(_is_range, 1, 65535)


# =============================================================================
# SECTION 2: ENUM SYSTEM (vollständig funktional, keine Klassen)
# =============================================================================

def make_enum(name: str, values: Any) -> dict:
    """
    Erstellt eine Enum-ähnliche Dict-Struktur.

    Parameters
    ----------
    name   : str  — Bezeichnung des Enums
    values : list | dict  — Liste von Werten ODER {Name: Wert}-Dict

    Returns
    -------
    dict mit Feldern:
      _enum_name : str
      _values    : frozenset  — Menge aller gültigen Werte
      _names     : frozenset  — Menge aller Namen
      _members   : dict       — Name -> Wert Mapping
      _reverse   : dict       — Wert -> Name Mapping
    """
    if isinstance(values, dict):
        members = dict(values)
    else:
        members = {v: v for v in values}

    return {
        "_enum_name": name,
        "_values":    frozenset(members.values()),
        "_names":     frozenset(members.keys()),
        "_members":   members,
        "_reverse":   {v: k for k, v in members.items()},
    }

def enum_contains(enum_def: dict, value: Any) -> bool:
    """Prüft ob value ein gültiger Enum-Wert ist."""
    return value in enum_def["_values"]

def enum_get(enum_def: dict, name: str) -> Any:
    """Gibt den Enum-Wert für einen Namen zurück (oder None)."""
    return enum_def["_members"].get(name)

def enum_name_of(enum_def: dict, value: Any) -> Optional[str]:
    """Gibt den Enum-Namen für einen Wert zurück (oder None)."""
    return enum_def["_reverse"].get(value)

def enum_values(enum_def: dict) -> frozenset:
    """Gibt alle gültigen Enum-Werte zurück."""
    return enum_def["_values"]

def enum_names(enum_def: dict) -> frozenset:
    """Gibt alle Enum-Namen zurück."""
    return enum_def["_names"]

def make_enum_checker(enum_def: dict) -> Callable:
    """
    Erstellt einen Typ-Checker der prüft ob ein Wert zum Enum gehört.
    Rückgabe: (value: Any) -> bool
    """
    return partial(enum_contains, enum_def)

def make_optional_enum_checker(enum_def: dict) -> Callable:
    """
    Wie make_enum_checker, aber None ist ebenfalls erlaubt.
    """
    inner = make_enum_checker(enum_def)
    return partial(_is_union, [is_none, inner])


# --- Domain Enums (alle als Dicts, kein OOP) ---

EVENT_TYPE_ENUM = make_enum("EventType", {
    "STATE_CHANGED":          "state_changed",
    "STATE_ADDED":            "state_added",
    "STATE_REMOVED":          "state_removed",
    "CONFIGURATION_CHANGED":  "config_changed",
    "HEARTBEAT":              "heartbeat",
    "ERROR":                  "error",
    "SENSOR_DATA":            "sensor_data",
    "ACTUATOR_COMMAND":       "actuator_command",
    "AUTOMATION_TRIGGERED":   "automation_triggered",
    "USER_INTERACTION":       "user_interaction",
    "SYSTEM_NOTIFICATION":    "system_notification",
    "TIMER_EVENT":            "timer_event",
})

DEVICE_TYPE_ENUM = make_enum("DeviceType", ["actuator", "sensor"])

MODULE_TYPE_ENUM = make_enum("ModuleType", ["relay", "DA", "DI", "AO", "AI"])

BUS_TYPE_ENUM = make_enum("BusType", ["modbus"])

FUNCTION_TYPE_ENUM = make_enum("FunctionType", [
    "write_coil",
    "read_coil",
    "write_register",
    "read_holding_registers",
    "write_data",
    "read_data",
])

STATE_VALUE_ENUM = make_enum("StateValue", [
    "activated", "deactivated", "error", "maintenance",
])

PROCESS_MODE_ENUM = make_enum("ProcessMode", ["manual", "automatic", "AI"])

CONDITION_TYPE_ENUM = make_enum("ConditionType", [
    "logical_and",
    "logical_or",
    "event_condition",
    "time_condition",
    "value_condition",
    "comparison",
    "mode_condition",
])

RULE_ACTION_TYPE_ENUM = make_enum("RuleActionType", [
    "call_service",
    "update_process_state",
    "pid_action",
])

# --- Enum-Checker als partials ---
is_event_type         = make_enum_checker(EVENT_TYPE_ENUM)
is_device_type        = make_enum_checker(DEVICE_TYPE_ENUM)
is_module_type        = make_enum_checker(MODULE_TYPE_ENUM)
is_bus_type           = make_enum_checker(BUS_TYPE_ENUM)
is_function_type      = make_enum_checker(FUNCTION_TYPE_ENUM)
is_state_value        = make_enum_checker(STATE_VALUE_ENUM)
is_process_mode       = make_enum_checker(PROCESS_MODE_ENUM)
is_condition_type     = make_enum_checker(CONDITION_TYPE_ENUM)
is_rule_action_type   = make_enum_checker(RULE_ACTION_TYPE_ENUM)

is_optional_bus_type          = make_optional_enum_checker(BUS_TYPE_ENUM)
is_optional_function_type     = make_optional_enum_checker(FUNCTION_TYPE_ENUM)
is_optional_state_value       = make_optional_enum_checker(STATE_VALUE_ENUM)
is_optional_process_mode      = make_optional_enum_checker(PROCESS_MODE_ENUM)
is_optional_condition_type    = make_optional_enum_checker(CONDITION_TYPE_ENUM)
is_optional_rule_action_type  = make_optional_enum_checker(RULE_ACTION_TYPE_ENUM)


# =============================================================================
# SECTION 3: SCHEMA-SYSTEM (Felddefinitionen und Schema-Strukturen)
# =============================================================================

def make_field(
    type_check: Callable,
    *,
    required: bool = True,
    default: Any = None,
    description: str = "",
    validator: Optional[Callable] = None,
    coerce: Optional[Callable] = None,
    min_val: Optional[Any] = None,
    max_val: Optional[Any] = None,
    min_len: Optional[int] = None,
    max_len: Optional[int] = None,
    pattern: Optional[str] = None,
    items_schema: Optional[dict] = None,
) -> dict:
    """
    Erstellt eine vollständige Felddefinition.

    Parameters
    ----------
    type_check    : Callable (value) -> bool   — Haupt-Typprüfung
    required      : bool      — Pflichtfeld (True) oder optional (False)
    default       : Any       — Standardwert für optionale Felder
    description   : str       — Dokumentation
    validator     : Callable  — Zusatzvalidierung: (value) -> True | str(Fehlermeldung)
    coerce        : Callable  — Typumwandlung vor der Validierung
    min_val       : num       — Minimalwert (für int/float)
    max_val       : num       — Maximalwert (für int/float)
    min_len       : int       — Mindestlänge (für str/list)
    max_len       : int       — Maximallänge (für str/list)
    pattern       : str       — Regex-Muster (für str)
    items_schema  : dict      — Schema für Listenelemente (nested validation)

    Returns
    -------
    dict — Felddefinition
    """
    return {
        "type_check":   type_check,
        "required":     required,
        "default":      default,
        "description":  description,
        "validator":    validator,
        "coerce":       coerce,
        "min_val":      min_val,
        "max_val":      max_val,
        "min_len":      min_len,
        "max_len":      max_len,
        "pattern":      pattern,
        "items_schema": items_schema,
    }


def make_schema(
    name: str,
    fields: dict,
    *,
    extra_validators: Optional[List[Callable]] = None,
    description: str = "",
) -> dict:
    """
    Erstellt eine Schema-Struktur.

    Parameters
    ----------
    name              : str   — Schema-Name (Pflicht)
    fields            : dict  — {feldname: make_field(...)}
    extra_validators  : list  — Kreuzfeld-Validatoren: (schema, data) -> List[str]
    description       : str   — Dokumentationstext

    Returns
    -------
    dict — Schema-Definition
    """
    return {
        "_schema_name":       name,
        "_fields":            fields,
        "_extra_validators":  extra_validators or [],
        "_description":       description,
    }


# =============================================================================
# SECTION 4: VALIDATION ENGINE
# =============================================================================

def _validate_single_field(field_name: str, field_def: dict, value: Any) -> List[str]:
    """
    Interne Funktion: validiert einen einzelnen Feldwert
    gegen seine vollständige Felddefinition.
    """
    errors = []

    # ── None-Behandlung ──────────────────────────────────────────────────────
    if value is None:
        if field_def["required"] and field_def["default"] is None:
            errors.append(f"['{field_name}'] Pflichtfeld fehlt oder ist None")
        return errors

    # ── Typ-Check ────────────────────────────────────────────────────────────
    if not field_def["type_check"](value):
        checker_repr = getattr(
            field_def["type_check"], "__name__",
            getattr(field_def["type_check"], "func",
                    field_def["type_check"]).__name__
            if hasattr(field_def["type_check"], "func") else str(field_def["type_check"])
        )
        errors.append(
            f"['{field_name}'] Typ-Fehler: Wert={value!r} "
            f"(Checker: {checker_repr})"
        )
        return errors  # Bei Typ-Fehler keine weiteren Checks nötig

    # ── Numerischer Bereich ───────────────────────────────────────────────────
    if field_def["min_val"] is not None and isinstance(value, (int, float)):
        if value < field_def["min_val"]:
            errors.append(
                f"['{field_name}'] {value} < Minimum {field_def['min_val']}"
            )
    if field_def["max_val"] is not None and isinstance(value, (int, float)):
        if value > field_def["max_val"]:
            errors.append(
                f"['{field_name}'] {value} > Maximum {field_def['max_val']}"
            )

    # ── Längen-Prüfung ────────────────────────────────────────────────────────
    if field_def["min_len"] is not None and hasattr(value, "__len__"):
        if len(value) < field_def["min_len"]:
            errors.append(
                f"['{field_name}'] Länge {len(value)} < Min-Länge {field_def['min_len']}"
            )
    if field_def["max_len"] is not None and hasattr(value, "__len__"):
        if len(value) > field_def["max_len"]:
            errors.append(
                f"['{field_name}'] Länge {len(value)} > Max-Länge {field_def['max_len']}"
            )

    # ── Regex-Muster ──────────────────────────────────────────────────────────
    if field_def["pattern"] is not None and isinstance(value, str):
        if not re.fullmatch(field_def["pattern"], value):
            errors.append(
                f"['{field_name}'] Wert '{value}' passt nicht zu Muster "
                f"'{field_def['pattern']}'"
            )

    # ── Benutzerdefinierter Validator ─────────────────────────────────────────
    if field_def["validator"] is not None:
        result = field_def["validator"](value)
        if result is not True and result is not None:
            errors.append(f"['{field_name}'] Validator: {result}")

    # ── Nested items_schema (für Listen mit Dict-Elementen) ───────────────────
    if field_def["items_schema"] is not None and isinstance(value, list):
        for i, item in enumerate(value):
            if isinstance(item, dict):
                sub_errors = validate(field_def["items_schema"], item)
                for err in sub_errors:
                    errors.append(f"['{field_name}'][{i}]{err}")

    return errors


def validate(schema: dict, data: dict) -> List[str]:
    """
    Validiert ein Daten-Dict gegen ein Schema.

    Parameters
    ----------
    schema : dict — Schema aus make_schema()
    data   : dict — Zu validierende Daten

    Returns
    -------
    List[str] — Fehlermeldungen (leer = valide)
    """
    if not isinstance(data, dict):
        return [
            f"Schema '{schema.get('_schema_name', '?')}': "
            f"Daten müssen ein Dict sein, erhalten: {type(data).__name__}"
        ]

    all_errors: List[str] = []

    for field_name, field_def in schema["_fields"].items():
        raw_value = data.get(field_name, None)

        # Coercion anwenden (Typumwandlung vor Validierung)
        if raw_value is not None and field_def["coerce"] is not None:
            try:
                raw_value = field_def["coerce"](raw_value)
            except Exception as exc:
                all_errors.append(
                    f"['{field_name}'] Coercion-Fehler: {exc}"
                )
                continue

        all_errors.extend(
            _validate_single_field(field_name, field_def, raw_value)
        )

    # Kreuzfeld-Validatoren ausführen
    for extra_validator in schema.get("_extra_validators", []):
        all_errors.extend(extra_validator(schema, data))

    return all_errors


def validate_strict(schema: dict, data: dict) -> dict:
    """
    Wie validate(), aber wirft ValueError wenn Fehler vorhanden.

    Returns
    -------
    dict — Das validierte Daten-Dict (bei Erfolg)

    Raises
    ------
    ValueError — Bei Validierungsfehlern
    """
    errors = validate(schema, data)
    if errors:
        raise ValueError(
            f"Validierungsfehler in '{schema.get('_schema_name', '?')}':\n"
            + "\n".join(f"  • {e}" for e in errors)
        )
    return data


def is_valid(schema: dict, data: dict) -> bool:
    """Schnelle True/False Gültigkeitsprüfung."""
    return len(validate(schema, data)) == 0


def validate_list(schema: dict, records: List[dict]) -> Dict[int, List[str]]:
    """
    Validiert eine Liste von Dicts gegen ein Schema.

    Returns
    -------
    dict — {index: [fehler]} für alle ungültigen Einträge (leer = alle valide)
    """
    return {
        i: errors
        for i, record in enumerate(records)
        for errors in [validate(schema, record)]
        if errors
    }


# =============================================================================
# SECTION 5: COERCION FUNKTIONEN (Typumwandlung)
# =============================================================================

def _coerce_to(target_type: type, value: Any) -> Any:
    """Generische Typumwandlung: gibt value zurück falls bereits correct type."""
    if isinstance(value, target_type):
        return value
    return target_type(value)

coerce_int   = partial(_coerce_to, int)
coerce_str   = partial(_coerce_to, str)
coerce_float = partial(_coerce_to, float)
coerce_bool  = partial(_coerce_to, bool)
coerce_list  = partial(_coerce_to, list)

def coerce_datetime_from_iso(value: Any) -> datetime.datetime:
    """Konvertiert ISO-String zu datetime.datetime."""
    if isinstance(value, datetime.datetime):
        return value
    return datetime.datetime.fromisoformat(str(value))

def coerce_path_from_str(value: Any) -> Path:
    """Konvertiert String zu pathlib.Path."""
    if isinstance(value, Path):
        return value
    return Path(str(value))

def coerce_json_str(value: Any) -> Any:
    """Parsed einen JSON-String zu Python-Objekt."""
    if isinstance(value, str):
        return json.loads(value)
    return value


# =============================================================================
# SECTION 6: DOMAIN SCHEMAS (1:1 Ersatz für alle Pydantic-Modelle)
# =============================================================================

# ─── ModbusFunction ──────────────────────────────────────────────────────────
MODBUS_FUNCTION_SCHEMA = make_schema(
    "ModbusFunction",
    {
        "modbus_function_id":   make_field(is_int,           description="Eindeutige Modbus-Funktions-ID"),
        "module_type":          make_field(is_module_type,   description="Typ des Moduls"),
        "modbus_function_type": make_field(is_function_type, description="Modbus-Funktionstyp"),
    },
    description="Beschreibt eine Modbus-Funktion mit Modul- und Funktionstyp.",
)

# ─── Device ──────────────────────────────────────────────────────────────────
DEVICE_SCHEMA = make_schema(
    "Device",
    {
        "device_id":     make_field(is_int,         description="Eindeutige Geräte-ID"),
        "controller_id": make_field(is_int,         description="Referenz auf Controller"),
        "device_type":   make_field(is_device_type, description="Gerätetyp: actuator | sensor"),
        "module_type":   make_field(is_module_type, description="Modultyp"),
        "bus_type":      make_field(is_bus_type,    description="Bus-Protokoll"),
        "address":       make_field(
            is_int,
            min_val=0,
            max_val=65535,
            description="Modbus-Register-Adresse [0..65535]",
        ),
    },
    description="Physikalisches Gerät am Bus.",
)

# ─── Controller ──────────────────────────────────────────────────────────────
CONTROLLER_SCHEMA = make_schema(
    "Controller",
    {
        "controller_id": make_field(is_int,          description="Eindeutige Controller-ID"),
        "device_name":   make_field(is_non_empty_str, description="Gerätename"),
        "bus_type":      make_field(is_bus_type,     description="Bus-Protokoll"),
        "ip_address":    make_field(
            is_ip_address,
            description="IPv4-Adresse des Controllers",
        ),
        "port":          make_field(
            is_int,
            min_val=1,
            max_val=65535,
            description="TCP-Port [1..65535]",
        ),
        "unit":          make_field(is_str, description="Einheit / Slave-Unit"),
    },
    description="Netzwerk-Controller mit Bus-Anbindung.",
)

# ─── VirtualController ───────────────────────────────────────────────────────
VIRTUAL_CONTROLLER_SCHEMA = make_schema(
    "VirtualController",
    {
        "virt_controller_id": make_field(is_int,          description="Virtuelle Controller-ID"),
        "controller_id":      make_field(is_int,          description="Referenz auf physikalischen Controller"),
        "virt_active":        make_field(is_bool,         description="Aktiv-Status"),
        "device_name":        make_field(is_non_empty_str, description="Name des virtuellen Controllers"),
        "ip_address":         make_field(is_str,          description="IP-Adresse"),
        "port":               make_field(
            is_int,
            min_val=1,
            max_val=65535,
            description="TCP-Port",
        ),
    },
    description="Virtueller Controller zur logischen Abstraktion.",
)

# ─── Actuator ────────────────────────────────────────────────────────────────
ACTUATOR_SCHEMA = make_schema(
    "Actuator",
    {
        "actuator_id":     make_field(is_str,                  description="Eindeutige Aktuator-ID (String)"),
        "actuator_number": make_field(is_int,                  description="Aktuator-Nummer"),
        "controller_id":   make_field(is_int,                  description="Referenz auf Controller"),
        "bus_type":        make_field(is_optional_bus_type,    required=False, default=None,
                                      description="Bus-Typ (optional)"),
        "function_type":   make_field(is_function_type,        description="Modbus-Funktionstyp"),
        "address":         make_field(
            is_int,
            min_val=0,
            max_val=65535,
            description="Modbus-Adresse",
        ),
        "actuator_group":  make_field(is_str,                  description="Aktuator-Gruppe"),
    },
    description="Aktuator-Definition (Ausgabegerät).",
)

# ─── Sensor ──────────────────────────────────────────────────────────────────
SENSOR_SCHEMA = make_schema(
    "Sensor",
    {
        "sensor_id":     make_field(is_str,               description="Eindeutige Sensor-ID (String)"),
        "sensor_number": make_field(is_int,               description="Sensor-Nummer"),
        "controller_id": make_field(is_int,               description="Referenz auf Controller"),
        "bus_type":      make_field(is_optional_bus_type, required=False, default=None,
                                    description="Bus-Typ (optional)"),
        "function_type": make_field(is_str,               description="Funktionstyp (String, flexibel)"),
        "address":       make_field(
            is_int,
            min_val=0,
            max_val=65535,
            description="Modbus-Adresse",
        ),
        "sensor_group":  make_field(is_str,               description="Sensor-Gruppe"),
    },
    description="Sensor-Definition (Eingabegerät).",
)

# ─── ActuatorType ────────────────────────────────────────────────────────────
ACTUATOR_TYPE_SCHEMA = make_schema(
    "ActuatorType",
    {
        "state_type_id": make_field(is_str, description="Zustandstyp-ID"),
        "actuator_type": make_field(is_str, description="Bezeichnung des Aktuatortyps"),
    },
)

# ─── SensorType ──────────────────────────────────────────────────────────────
SENSOR_TYPE_SCHEMA = make_schema(
    "SensorType",
    {
        "sensor_types_id": make_field(is_int, description="Sensortyp-ID"),
        "sensor_type":     make_field(is_str, description="Bezeichnung des Sensortyps"),
    },
)

# ─── Process ─────────────────────────────────────────────────────────────────
PROCESS_SCHEMA = make_schema(
    "Process",
    {
        "p_id":             make_field(is_int,           description="Prozess-ID"),
        "process_name":     make_field(is_non_empty_str, description="Prozessname"),
        "process_location": make_field(is_str,           description="Standort des Prozesses"),
    },
)

# ─── ProcessValue ────────────────────────────────────────────────────────────
PROCESS_VALUE_SCHEMA = make_schema(
    "ProcessValue",
    {
        "value_id":    make_field(is_int,         description="Wert-ID"),
        "state_value": make_field(is_state_value, description="Zustandswert aus STATE_VALUE_ENUM"),
    },
)

# ─── ProcessMode ─────────────────────────────────────────────────────────────
PROCESS_MODE_SCHEMA = make_schema(
    "ProcessMode",
    {
        "mode_id": make_field(is_int,          description="Modus-ID"),
        "mode":    make_field(is_process_mode, description="Betriebsmodus aus PROCESS_MODE_ENUM"),
    },
)

# ─── Timer ───────────────────────────────────────────────────────────────────
TIMER_SCHEMA = make_schema(
    "Timer",
    {
        "timer_id":    make_field(is_int,          description="Timer-ID"),
        "duration":    make_field(is_optional_int, required=False, default=None,
                                  description="Dauer in Sekunden (optional)"),
        "frequency":   make_field(is_optional_int, required=False, default=None,
                                  description="Frequenz in Hz (optional)"),
        "description": make_field(is_str,          required=False, default="",
                                  description="Beschreibungstext"),
    },
)

# ─── PIDParameters ───────────────────────────────────────────────────────────
PID_PARAMETERS_SCHEMA = make_schema(
    "PIDParameters",
    {
        "kp": make_field(is_float, description="Proportionalanteil"),
        "ki": make_field(is_float, description="Integralanteil"),
        "kd": make_field(is_float, description="Differentialanteil"),
    },
)

# ─── PIDConfig (mit nested PIDParameters) ────────────────────────────────────
def _extra_validate_pid_config(_schema: dict, data: dict) -> List[str]:
    """Extra-Validator: Nested PIDParameters validieren."""
    pid = data.get("pid_parameters")
    if pid is not None and isinstance(pid, dict):
        return [
            f"['pid_parameters']{e}"
            for e in validate(PID_PARAMETERS_SCHEMA, pid)
        ]
    return []

PID_CONFIG_SCHEMA = make_schema(
    "PIDConfig",
    {
        "pid_config_id":  make_field(is_int,  description="PID-Konfiguration-ID"),
        "pid_parameters": make_field(is_dict, description="PID-Parameter (nested PIDParameters)"),
    },
    extra_validators=[_extra_validate_pid_config],
)

# ─── ProcessState ─────────────────────────────────────────────────────────────
PROCESS_STATE_SCHEMA = make_schema(
    "ProcessState",
    {
        "state_id":              make_field(is_int,       description="Zustands-ID"),
        "p_id":                  make_field(is_int,       description="Referenz auf Prozess"),
        "p_groups":              make_field(list_of_int,  description="Liste von Prozessgruppen-IDs"),
        "actuator_id":           make_field(list_of_int,  description="Referenzierte Aktuatoren"),
        "sensor_id":             make_field(list_of_int,  description="Referenzierte Sensoren"),
        "p__type":               make_field(is_str,       description="Prozesstyp-Bezeichnung"),
        "value_id":              make_field(is_int,       description="Referenz auf ProcessValue"),
        "mode_id":               make_field(is_int,       description="Referenz auf ProcessMode"),
        "automation_parameters": make_field(is_dict,      description="Freie Automatisierungsparameter"),
    },
)

# ─── Action ──────────────────────────────────────────────────────────────────
ACTION_SCHEMA = make_schema(
    "Action",
    {
        "action_id":         make_field(is_int,  description="Aktions-ID"),
        "state_id":          make_field(is_int,  description="Referenz auf ProcessState"),
        "attribute_changes": make_field(is_dict, description="Attributänderungen als Dict"),
        "description":       make_field(is_str,  description="Beschreibungstext"),
    },
)

# ─── Transition ───────────────────────────────────────────────────────────────
TRANSITION_SCHEMA = make_schema(
    "Transition",
    {
        "transition_id": make_field(is_int, description="Übergangs-ID"),
        "action_id":     make_field(is_int, description="Referenz auf Action"),
    },
)

# ─── Condition ───────────────────────────────────────────────────────────────
CONDITION_SCHEMA = make_schema(
    "Condition",
    {
        "type":             make_field(is_condition_type,  description="Bedingungstyp"),
        "operator":         make_field(is_optional_str,    required=False, default=None),
        "value":            make_field(is_any,             required=False, default=None),
        "sensor_id":        make_field(is_optional_int,    required=False, default=None),
        "timer_id":         make_field(is_optional_int,    required=False, default=None),
        "duration":         make_field(is_optional_int,    required=False, default=None),
        "attribute":        make_field(is_optional_str,    required=False, default=None),
        "target_value_id":  make_field(is_optional_int,    required=False, default=None),
        "state_id":         make_field(is_optional_int,    required=False, default=None),
        "target_mode_id":   make_field(is_optional_int,    required=False, default=None),
        "event_type":       make_field(is_optional_str,    required=False, default=None),
        "time_spec":        make_field(is_optional_str,    required=False, default=None),
        "mode":             make_field(is_optional_str,    required=False, default=None),
        "sub_conditions":   make_field(is_optional_list,   required=False, default=None,
                                       description="Verschachtelte Bedingungen (für logical_and / logical_or)"),
    },
    description="Flexible Bedingungsstruktur für Regeln und Trigger.",
)

# ─── RuleAction (ehem. ActionSpec) ───────────────────────────────────────────
RULE_ACTION_SCHEMA = make_schema(
    "RuleAction",
    {
        "type":         make_field(is_rule_action_type,  description="Aktionstyp aus RULE_ACTION_TYPE_ENUM"),
        "service_name": make_field(is_optional_str,      required=False, default=None),
        "service_data": make_field(is_optional_dict,     required=False, default=None),
        "state_id":     make_field(is_optional_int,      required=False, default=None),
        "updates":      make_field(is_optional_dict,     required=False, default=None),
        "kp":           make_field(is_optional_float,    required=False, default=None),
    },
    description="Aktion einer Regel (call_service, update_process_state, pid_action).",
)

# ─── Rule ─────────────────────────────────────────────────────────────────────
RULE_SCHEMA = make_schema(
    "Rule",
    {
        "conditions": make_field(list_of_dict, description="Liste von Condition-Dicts"),
        "actions":    make_field(list_of_dict, description="Liste von RuleAction-Dicts"),
    },
)

# ─── TriggerCondition ────────────────────────────────────────────────────────
TRIGGER_CONDITION_SCHEMA = make_schema(
    "TriggerCondition",
    {
        "sensor_id": make_field(is_optional_int, required=False, default=None),
        "operator":  make_field(is_optional_str, required=False, default=None),
        "value":     make_field(is_any,          required=False, default=None),
        "timer_id":  make_field(is_optional_int, required=False, default=None),
        "duration":  make_field(is_optional_int, required=False, default=None),
    },
)

# ─── Trigger ─────────────────────────────────────────────────────────────────
TRIGGER_SCHEMA = make_schema(
    "Trigger",
    {
        "trigger_id": make_field(is_int,  description="Trigger-ID"),
        "condition":  make_field(is_dict, description="TriggerCondition als Dict"),
    },
)

# ─── Event ────────────────────────────────────────────────────────────────────
EVENT_SCHEMA = make_schema(
    "Event",
    {
        "event_id":    make_field(is_int,  description="Ereignis-ID"),
        "event_type":  make_field(is_str,  description="Ereignistyp-Bezeichnung"),
        "description": make_field(is_str,  description="Beschreibung des Ereignisses"),
        "parameters":  make_field(is_dict, description="Ereignis-Parameter als Dict"),
    },
)

# ─── AutomationState (mit nested PIDParameters) ───────────────────────────────
def _extra_validate_automation_state_pid(_schema: dict, data: dict) -> List[str]:
    """Extra-Validator: Nested PIDParameters in AutomationState validieren."""
    pid = data.get("pid_parameters")
    if pid is not None and isinstance(pid, dict):
        return [
            f"['pid_parameters']{e}"
            for e in validate(PID_PARAMETERS_SCHEMA, pid)
        ]
    return []

AUTOMATION_STATE_SCHEMA = make_schema(
    "AutomationState",
    {
        "state_id":               make_field(is_int,           description="Zustands-ID"),
        "p_id":                   make_field(is_int,           description="Prozess-ID"),
        "state_type":             make_field(is_str,           description="Zustandstyp"),
        "actuator_id":            make_field(list_of_int,      description="Aktuatoren-IDs"),
        "sensor_id":              make_field(list_of_int,      description="Sensor-IDs"),
        "state_value":            make_field(is_str,           description="Aktueller Zustandswert"),
        "timestamp":              make_field(is_str,           description="ISO-Zeitstempel"),
        "target_temp":            make_field(is_optional_float, required=False, default=None),
        "current_temp":           make_field(is_optional_float, required=False, default=None),
        "heating_control_method": make_field(is_optional_str,  required=False, default=None),
        "pid_parameters":         make_field(is_optional_dict, required=False, default=None,
                                             description="PIDParameters als verschachteltes Dict"),
    },
    extra_validators=[_extra_validate_automation_state_pid],
)

# ─── ControlTask ──────────────────────────────────────────────────────────────
CONTROL_TASK_SCHEMA = make_schema(
    "ControlTask",
    {
        "task_id":       make_field(is_str,          description="Aufgaben-ID"),
        "object_type":   make_field(is_str,          description="Objekttyp (actuator/sensor)"),
        "controller_id": make_field(is_int,          description="Controller-Referenz"),
        "bus_type":      make_field(is_str,          description="Bus-Typ"),
        "function_type": make_field(is_str,          description="Funktionstyp"),
        "address":       make_field(is_int,          description="Modbus-Adresse"),
        "unit":          make_field(is_optional_str, required=False, default=None),
        "state_value":   make_field(is_optional_str, required=False, default=None),
        "timestamp":     make_field(is_optional_str, required=False, default=None),
    },
)

# ─── DeviceStateData (Gesamt-Aggregation) ─────────────────────────────────────
DEVICE_STATE_DATA_SCHEMA = make_schema(
    "DeviceStateData",
    {
        "modbus_functions":    make_field(list_of_dict, description="Modbus-Funktionen"),
        "devices":             make_field(list_of_dict, description="Geräte"),
        "controllers":         make_field(list_of_dict, description="Controller"),
        "virtual_controllers": make_field(list_of_dict, description="Virtuelle Controller"),
        "actuators":           make_field(list_of_dict, description="Aktuatoren"),
        "sensors":             make_field(list_of_dict, description="Sensoren"),
        "actuator_types":      make_field(list_of_dict, description="Aktuatortypen"),
        "sensor_types":        make_field(list_of_dict, description="Sensortypen"),
        "processes":           make_field(list_of_dict, description="Prozesse"),
        "process_values":      make_field(list_of_dict, description="Prozesswerte"),
        "process_modes":       make_field(list_of_dict, description="Prozessmodi"),
        "timers":              make_field(list_of_dict, description="Timer"),
        "pid_configs":         make_field(list_of_dict, description="PID-Konfigurationen"),
        "process_states":      make_field(list_of_dict, description="Prozesszustände"),
        "actions":             make_field(list_of_dict, description="Aktionen"),
        "transitions":         make_field(list_of_dict, description="Übergänge"),
        "triggers":            make_field(list_of_dict, description="Trigger"),
        "events":              make_field(list_of_dict, description="Ereignisse"),
        "automation_states":   make_field(list_of_dict, description="Automatisierungszustände"),
        "control_tasks":       make_field(list_of_dict, description="Steueraufgaben"),
        "rules":               make_field(list_of_dict, description="Regeln"),
    },
    description="Vollständige Gerätezustands-Aggregation.",
)


# =============================================================================
# SECTION 7: FACTORY FUNKTIONEN (validierte Record-Dicts erstellen)
# =============================================================================

def _make_record(schema: dict, **kwargs: Any) -> dict:
    """
    Generische Factory: erstellt ein Dict, befüllt Defaults und validiert.

    Parameters
    ----------
    schema  : dict — Schema aus make_schema()
    **kwargs       — Feldwerte

    Returns
    -------
    dict — Validiertes Record-Dict

    Raises
    ------
    ValueError — Bei Validierungsfehlern
    """
    data = dict(kwargs)

    # Defaults für alle fehlenden optionalen Felder befüllen (inkl. None-Defaults)
    for field_name, field_def in schema["_fields"].items():
        if field_name not in data:
            if not field_def["required"]:
                data[field_name] = field_def["default"]

    return validate_strict(schema, data)

# Alle Factory-Funktionen als partials
make_modbus_function     = partial(_make_record, MODBUS_FUNCTION_SCHEMA)
make_device              = partial(_make_record, DEVICE_SCHEMA)
make_controller          = partial(_make_record, CONTROLLER_SCHEMA)
make_virtual_controller  = partial(_make_record, VIRTUAL_CONTROLLER_SCHEMA)
make_actuator            = partial(_make_record, ACTUATOR_SCHEMA)
make_sensor              = partial(_make_record, SENSOR_SCHEMA)
make_actuator_type       = partial(_make_record, ACTUATOR_TYPE_SCHEMA)
make_sensor_type         = partial(_make_record, SENSOR_TYPE_SCHEMA)
make_process             = partial(_make_record, PROCESS_SCHEMA)
make_process_value       = partial(_make_record, PROCESS_VALUE_SCHEMA)
make_process_mode        = partial(_make_record, PROCESS_MODE_SCHEMA)
make_timer               = partial(_make_record, TIMER_SCHEMA)
make_pid_parameters      = partial(_make_record, PID_PARAMETERS_SCHEMA)
make_pid_config          = partial(_make_record, PID_CONFIG_SCHEMA)
make_process_state       = partial(_make_record, PROCESS_STATE_SCHEMA)
make_action              = partial(_make_record, ACTION_SCHEMA)
make_transition          = partial(_make_record, TRANSITION_SCHEMA)
make_condition           = partial(_make_record, CONDITION_SCHEMA)
make_rule_action         = partial(_make_record, RULE_ACTION_SCHEMA)
make_rule                = partial(_make_record, RULE_SCHEMA)
make_trigger_condition   = partial(_make_record, TRIGGER_CONDITION_SCHEMA)
make_trigger             = partial(_make_record, TRIGGER_SCHEMA)
make_event               = partial(_make_record, EVENT_SCHEMA)
make_automation_state    = partial(_make_record, AUTOMATION_STATE_SCHEMA)
make_control_task        = partial(_make_record, CONTROL_TASK_SCHEMA)
make_device_state_data   = partial(_make_record, DEVICE_STATE_DATA_SCHEMA)


# =============================================================================
# SECTION 8: KREUZREFERENZ-VALIDATOREN (funktionale Entsprechung der Methoden)
# =============================================================================

def validate_unique_ids(
    data: dict,
    list_key: str,
    id_key: str,
) -> List[str]:
    """
    Prüft ob alle Werte des id_key in data[list_key] eindeutig sind.

    Parameters
    ----------
    data     : dict — DeviceStateData-Dict
    list_key : str  — Schlüssel zur Liste (z.B. 'devices')
    id_key   : str  — Schlüssel für die ID (z.B. 'device_id')

    Returns
    -------
    List[str] — Fehlermeldungen (leer = ok)
    """
    items = data.get(list_key, [])
    ids   = [item.get(id_key) for item in items if isinstance(item, dict)]
    if len(ids) == len(set(map(str, ids))):
        return []
    duplicates = list({i for i in ids if ids.count(i) > 1})
    return [f"Duplikate in '{list_key}.{id_key}': {duplicates}"]


def _append_unique_id_validation_errors(data: dict, acc: List[str], check: Tuple[str, str]) -> List[str]:
    """Akkumuliert Eindeutigkeitsfehler ohne Inline-Callable."""
    return acc + validate_unique_ids(data, check[0], check[1])


def validate_all_unique_ids(data: dict) -> List[str]:
    """Prüft alle Kern-ID-Felder auf Eindeutigkeit."""
    checks = [
        ("devices",     "device_id"),
        ("actuators",   "actuator_id"),
        ("sensors",     "sensor_id"),
        ("controllers", "controller_id"),
        ("processes",   "p_id"),
    ]
    return list(reduce(
        partial(_append_unique_id_validation_errors, data),
        checks,
        [],
    ))


def validate_controller_refs(data: dict) -> List[str]:
    """Prüft ob alle controller_id-Referenzen auf gültige Controller zeigen."""
    valid_ids = {
        c.get("controller_id")
        for c in data.get("controllers", [])
        if isinstance(c, dict)
    }
    errors = []
    checks = [
        ("devices",             "device_id"),
        ("actuators",           "actuator_id"),
        ("sensors",             "sensor_id"),
        ("virtual_controllers", "virt_controller_id"),
    ]
    for list_key, id_key in checks:
        for item in data.get(list_key, []):
            if not isinstance(item, dict):
                continue
            ref = item.get("controller_id")
            if ref not in valid_ids:
                errors.append(
                    f"'{list_key}' [{id_key}={item.get(id_key)}]: "
                    f"controller_id={ref!r} existiert nicht"
                )
    return errors


def validate_process_state_refs(data: dict) -> List[str]:
    """
    Prüft in ProcessStates alle Referenzen auf Aktuatoren,
    Sensoren und Prozesse.
    """
    actuator_ids = {
        str(a.get("actuator_id"))
        for a in data.get("actuators", [])
        if isinstance(a, dict)
    }
    sensor_ids = {
        str(s.get("sensor_id"))
        for s in data.get("sensors", [])
        if isinstance(s, dict)
    }
    process_ids = {
        p.get("p_id")
        for p in data.get("processes", [])
        if isinstance(p, dict)
    }
    errors = []
    for ps in data.get("process_states", []):
        if not isinstance(ps, dict):
            continue
        sid = ps.get("state_id")
        for aid in ps.get("actuator_id", []):
            if str(aid) not in actuator_ids:
                errors.append(
                    f"process_state[{sid}]: actuator_id={aid} existiert nicht"
                )
        for s_id in ps.get("sensor_id", []):
            if str(s_id) not in sensor_ids:
                errors.append(
                    f"process_state[{sid}]: sensor_id={s_id} existiert nicht"
                )
        if ps.get("p_id") not in process_ids:
            errors.append(
                f"process_state[{sid}]: p_id={ps.get('p_id')!r} existiert nicht"
            )
    return errors


def validate_address_ranges(
    data: dict,
    min_addr: int = 0,
    max_addr: int = 65535,
) -> List[str]:
    """Prüft Modbus-Adressbereiche für Devices, Aktuatoren und Sensoren."""
    errors = []
    checks = [
        ("devices",   "device_id"),
        ("actuators", "actuator_id"),
        ("sensors",   "sensor_id"),
    ]
    for list_key, id_key in checks:
        for item in data.get(list_key, []):
            if not isinstance(item, dict):
                continue
            addr = item.get("address")
            if addr is not None and not (min_addr <= addr <= max_addr):
                errors.append(
                    f"'{list_key}' [{id_key}={item.get(id_key)}]: "
                    f"address={addr} außerhalb [{min_addr}..{max_addr}]"
                )
    return errors


def find_undefined_actuator_ids_in_process_states(data: dict) -> List[str]:
    """
    Gibt alle actuator_ids zurück, die in ProcessStates referenziert werden,
    aber nicht in der actuators-Liste existieren.
    """
    actuator_ids = {
        str(a.get("actuator_id"))
        for a in data.get("actuators", [])
        if isinstance(a, dict)
    }
    used_ids = set()
    for ps in data.get("process_states", []):
        if isinstance(ps, dict):
            used_ids.update(str(aid) for aid in ps.get("actuator_id", []))
    return list(used_ids - actuator_ids)


def get_invalid_controller_references(data: dict) -> List[Tuple[str, Any, Any]]:
    """
    Gibt alle ungültigen Controller-Referenzen zurück als:
    [(Quelle, Objekt-ID, ungültige controller_id), ...]
    """
    valid_ids = {
        c.get("controller_id")
        for c in data.get("controllers", [])
        if isinstance(c, dict)
    }
    invalid_refs: List[Tuple[str, Any, Any]] = []
    checks = [
        ("devices",             "device_id",          "Device"),
        ("actuators",           "actuator_id",         "Actuator"),
        ("sensors",             "sensor_id",           "Sensor"),
        ("virtual_controllers", "virt_controller_id",  "VirtualController"),
    ]
    for list_key, id_key, label in checks:
        for item in data.get(list_key, []):
            if not isinstance(item, dict):
                continue
            if item.get("controller_id") not in valid_ids:
                invalid_refs.append(
                    (label, item.get(id_key), item.get("controller_id"))
                )
    return invalid_refs


# Alle Kreuzvalidierungen kombiniert (als partial für einfache Parametrierung)
_default_address_validator = partial(validate_address_ranges, min_addr=0, max_addr=65535)


def _append_validator_errors(data: dict, acc: List[str], validator: Callable) -> List[str]:
    """Akkumuliert Validator-Fehler ohne Inline-Callable."""
    return acc + validator(data)


def run_all_validations(data: dict) -> List[str]:
    """
    Führt alle Kreuzreferenz- und Eindeutigkeits-Validierungen aus.

    Parameters
    ----------
    data : dict — DeviceStateData-Dict

    Returns
    -------
    List[str] — Alle Fehlermeldungen (leer = alles valide)
    """
    validators = [
        validate_all_unique_ids,
        validate_controller_refs,
        validate_process_state_refs,
        _default_address_validator,
    ]
    return list(reduce(
        partial(_append_validator_errors, data),
        validators,
        [],
    ))


def run_all_validations_strict(data: dict) -> None:
    """
    Wie run_all_validations(), aber wirft ValueError bei Fehlern.

    Raises
    ------
    ValueError — Bei Kreuzreferenz-Fehlern
    """
    errors = run_all_validations(data)
    if errors:
        raise ValueError(
            "Kreuzreferenz-Validierungsfehler:\n"
            + "\n".join(f"  • {e}" for e in errors)
        )


# =============================================================================
# SECTION 9: SERIALISIERUNG / DESERIALISIERUNG
# =============================================================================

def _serialize_value(value: Any) -> Any:
    """Konvertiert einen einzelnen Wert zu einem JSON-serialisierbaren Typ."""
    if isinstance(value, dict):
        return to_dict(value)
    if isinstance(value, list):
        return [_serialize_value(v) for v in value]
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (frozenset, set)):
        return sorted(list(value))
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value

def to_dict(record: dict) -> dict:
    """
    Konvertiert ein Record-Dict rekursiv zu einem vollständig
    JSON-serialisierbaren Dict (deep copy).
    """
    return {k: _serialize_value(v) for k, v in record.items()}

def to_json(
    record: dict,
    *,
    indent: int = 2,
    ensure_ascii: bool = False,
    sort_keys: bool = False,
) -> str:
    """Serialisiert ein Record-Dict zu einem JSON-String."""
    return json.dumps(
        to_dict(record),
        indent=indent,
        ensure_ascii=ensure_ascii,
        sort_keys=sort_keys,
    )

def from_dict(schema: dict, data: dict, *, strict: bool = True) -> dict:
    """
    Lädt und validiert ein Dict gegen ein Schema.

    Parameters
    ----------
    schema  : dict — Ziel-Schema
    data    : dict — Rohdaten
    strict  : bool — True = ValueError bei Fehlern, False = Dict auch bei Fehlern zurückgeben

    Returns
    -------
    dict — Validiertes (oder rohes) Daten-Dict
    """
    if strict:
        return validate_strict(schema, data)
    return data

def from_json(
    schema: dict,
    json_str: str,
    *,
    strict: bool = True,
) -> dict:
    """Deserialisiert einen JSON-String und validiert gegen ein Schema."""
    data = json.loads(json_str)
    return from_dict(schema, data, strict=strict)

def from_json_file(
    schema: dict,
    path: Union[str, Path],
    *,
    strict: bool = True,
    encoding: str = "utf-8",
) -> dict:
    """Liest eine JSON-Datei und validiert deren Inhalt gegen ein Schema."""
    with open(Path(path), "r", encoding=encoding) as fh:
        data = json.load(fh)
    return from_dict(schema, data, strict=strict)

def to_json_file(
    record: dict,
    path: Union[str, Path],
    *,
    indent: int = 2,
    encoding: str = "utf-8",
) -> None:
    """Schreibt ein Record-Dict als formatierte JSON-Datei."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding=encoding) as fh:
        json.dump(to_dict(record), fh, indent=indent, ensure_ascii=False)


# =============================================================================
# SECTION 10: UTILITY / HELPER FUNKTIONEN
# =============================================================================

def get_field_names(schema: dict) -> List[str]:
    """Gibt alle Feldnamen eines Schemas zurück."""
    return list(schema["_fields"].keys())

def get_required_fields(schema: dict) -> List[str]:
    """Gibt alle Pflichtfelder eines Schemas zurück."""
    return [k for k, v in schema["_fields"].items() if v["required"]]

def get_optional_fields(schema: dict) -> List[str]:
    """Gibt alle optionalen Felder eines Schemas zurück."""
    return [k for k, v in schema["_fields"].items() if not v["required"]]

def apply_defaults(schema: dict, data: dict) -> dict:
    """
    Gibt ein neues Dict zurück mit Default-Werten für fehlende optionale Felder.
    Mutiiert das Original nicht.
    """
    result = dict(data)
    for field_name, field_def in schema["_fields"].items():
        if field_name not in result:
            if not field_def["required"]:
                result[field_name] = field_def["default"]
    return result

def _merge_record_pair(left: dict, right: dict) -> dict:
    """Merged zwei Dicts, wobei rechts links überschreibt."""
    return {**left, **right}


def merge_records(*records: dict) -> dict:
    """
    Merged mehrere Dicts (rechts überschreibt links).
    Gibt ein neues Dict zurück.
    """
    return reduce(_merge_record_pair, records, {})

def pick_fields(record: dict, fields: List[str]) -> dict:
    """Gibt ein Dict mit nur den angegebenen Feldern zurück."""
    return {k: record[k] for k in fields if k in record}

def omit_fields(record: dict, fields: List[str]) -> dict:
    """Gibt ein Dict ohne die angegebenen Felder zurück."""
    exclude = frozenset(fields)
    return {k: v for k, v in record.items() if k not in exclude}

def find_by(records: List[dict], key: str, value: Any) -> Optional[dict]:
    """Findet das erste Dict in records mit records[i][key] == value."""
    for record in records:
        if isinstance(record, dict) and record.get(key) == value:
            return record
    return None

def filter_by(records: List[dict], key: str, value: Any) -> List[dict]:
    """Filtert eine Liste von Dicts nach key == value."""
    return [r for r in records if isinstance(r, dict) and r.get(key) == value]

def filter_by_fn(records: List[dict], predicate: Callable) -> List[dict]:
    """Filtert eine Liste von Dicts nach einer beliebigen Bedingung."""
    return [r for r in records if isinstance(r, dict) and predicate(r)]

def index_by(records: List[dict], key: str) -> Dict[Any, dict]:
    """
    Erstellt einen Index (Dict) aus einer Liste.
    Ergebnis: {record[key]: record, ...}
    """
    return {
        r[key]: r
        for r in records
        if isinstance(r, dict) and key in r
    }

def group_by(records: List[dict], key: str) -> Dict[Any, List[dict]]:
    """
    Gruppiert eine Liste von Dicts nach einem Schlüssel.
    Ergebnis: {group_value: [record, ...], ...}
    """
    result: Dict[Any, List[dict]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        group_key = record.get(key)
        if group_key not in result:
            result[group_key] = []
        result[group_key].append(record)
    return result

def pluck(records: List[dict], key: str) -> List[Any]:
    """Extrahiert den Wert eines Schlüssels aus allen Dicts einer Liste."""
    return [r.get(key) for r in records if isinstance(r, dict)]

def unique_values(records: List[dict], key: str) -> List[Any]:
    """Gibt alle eindeutigen Werte eines Schlüssels zurück (Reihenfolge erhalten)."""
    seen: set = set()
    result = []
    for v in pluck(records, key):
        sv = str(v)
        if sv not in seen:
            seen.add(sv)
            result.append(v)
    return result

def deep_get(record: dict, *keys: str, default: Any = None) -> Any:
    """
    Tiefes Nachschlagen in einem verschachtelten Dict.

    Beispiel: deep_get(data, "pid_parameters", "kp", default=0.0)
    """
    current = record
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key, default)
    return current

def deep_set(record: dict, keys: List[str], value: Any) -> dict:
    """
    Setzt einen Wert tief in einem verschachtelten Dict.
    Gibt ein neues Dict zurück (nicht-mutierend).

    Beispiel: deep_set(data, ["pid_parameters", "kp"], 1.5)
    """
    if not keys:
        return record
    result = dict(record)
    if len(keys) == 1:
        result[keys[0]] = value
    else:
        nested = result.get(keys[0], {})
        result[keys[0]] = deep_set(
            nested if isinstance(nested, dict) else {},
            keys[1:],
            value,
        )
    return result

def update_field(record: dict, key: str, value: Any) -> dict:
    """
    Gibt ein neues Dict mit einem aktualisierten Feld zurück.
    Nicht-mutierend.
    """
    return {**record, key: value}

def transform_fields(record: dict, transformers: Dict[str, Callable]) -> dict:
    """
    Wendet je einen Transformer auf bestimmte Felder an.
    Gibt ein neues Dict zurück.

    Beispiel: transform_fields(r, {"address": coerce_int})
    """
    result = dict(record)
    for key, transformer in transformers.items():
        if key in result:
            result[key] = transformer(result[key])
    return result


# =============================================================================
# SECTION 11: TIMESTAMP & DATETIME HELPERS
# =============================================================================

def utc_now_iso() -> str:
    """Gibt den aktuellen UTC-Zeitstempel als ISO 8601-String zurück."""
    return datetime.datetime.now(timezone.utc).isoformat()

def utc_now_ts() -> float:
    """Gibt den aktuellen Unix-Timestamp (UTC) zurück."""
    return time.time()

def iso_to_datetime(iso_str: str) -> datetime.datetime:
    """Parst einen ISO 8601-String zu datetime.datetime."""
    return datetime.datetime.fromisoformat(iso_str)

def datetime_to_iso(dt: datetime.datetime) -> str:
    """Serialisiert ein datetime-Objekt als ISO 8601-String."""
    return dt.isoformat()

def datetime_to_ts(dt: datetime.datetime) -> float:
    """Konvertiert datetime zu Unix-Timestamp."""
    return dt.timestamp()

def ts_to_datetime(ts: float, *, tz: datetime.timezone = timezone.utc) -> datetime.datetime:
    """Konvertiert Unix-Timestamp zu datetime."""
    return datetime.datetime.fromtimestamp(ts, tz=tz)


# =============================================================================
# SECTION 12: SCHEMA INTROSPECTION (Dokumentation & Debugging)
# =============================================================================

def describe_schema(schema: dict, *, width: int = 35) -> str:
    """
    Gibt eine lesbare tabellarische Beschreibung eines Schemas zurück.
    """
    name = schema.get("_schema_name", "Unbekannt")
    desc = schema.get("_description", "")
    lines = [
        f"╔══ Schema: {name} {'=' * max(0, 50 - len(name))}",
    ]
    if desc:
        lines.append(f"║  {desc}")
    lines.append(f"╠{'═' * 60}")

    for field_name, field_def in schema["_fields"].items():
        req     = "✔ req" if field_def["required"] else "○ opt"
        default = f"  [default={field_def['default']!r}]" if field_def["default"] is not None else ""
        fdesc   = field_def.get("description", "")

        # Checker-Name hübsch auflesen
        chk = field_def["type_check"]
        if hasattr(chk, "func"):
            checker_name = getattr(chk.func, "__name__", str(chk.func))
            args = ", ".join(str(a) for a in chk.args)
            checker_repr = f"{checker_name}({args})" if args else checker_name
        else:
            checker_repr = getattr(chk, "__name__", str(chk))

        constraints = []
        if field_def["min_val"] is not None:
            constraints.append(f"min={field_def['min_val']}")
        if field_def["max_val"] is not None:
            constraints.append(f"max={field_def['max_val']}")
        if field_def["min_len"] is not None:
            constraints.append(f"min_len={field_def['min_len']}")
        if field_def["pattern"] is not None:
            constraints.append(f"pattern='{field_def['pattern']}'")
        constraint_str = f"  {{{', '.join(constraints)}}}" if constraints else ""

        line = (
            f"║  {field_name:<{width}} {req}  "
            f"{checker_repr:<30}{default}{constraint_str}"
        )
        if fdesc:
            line += f"\n║    └─ {fdesc}"
        lines.append(line)

    lines.append(f"╚{'═' * 60}")
    return "\n".join(lines)


def list_all_schemas() -> List[str]:
    """Gibt die Namen aller definierten Domain-Schemas zurück."""
    return [
        MODBUS_FUNCTION_SCHEMA["_schema_name"],
        DEVICE_SCHEMA["_schema_name"],
        CONTROLLER_SCHEMA["_schema_name"],
        VIRTUAL_CONTROLLER_SCHEMA["_schema_name"],
        ACTUATOR_SCHEMA["_schema_name"],
        SENSOR_SCHEMA["_schema_name"],
        ACTUATOR_TYPE_SCHEMA["_schema_name"],
        SENSOR_TYPE_SCHEMA["_schema_name"],
        PROCESS_SCHEMA["_schema_name"],
        PROCESS_VALUE_SCHEMA["_schema_name"],
        PROCESS_MODE_SCHEMA["_schema_name"],
        TIMER_SCHEMA["_schema_name"],
        PID_PARAMETERS_SCHEMA["_schema_name"],
        PID_CONFIG_SCHEMA["_schema_name"],
        PROCESS_STATE_SCHEMA["_schema_name"],
        ACTION_SCHEMA["_schema_name"],
        TRANSITION_SCHEMA["_schema_name"],
        CONDITION_SCHEMA["_schema_name"],
        RULE_ACTION_SCHEMA["_schema_name"],
        RULE_SCHEMA["_schema_name"],
        TRIGGER_CONDITION_SCHEMA["_schema_name"],
        TRIGGER_SCHEMA["_schema_name"],
        EVENT_SCHEMA["_schema_name"],
        AUTOMATION_STATE_SCHEMA["_schema_name"],
        CONTROL_TASK_SCHEMA["_schema_name"],
        DEVICE_STATE_DATA_SCHEMA["_schema_name"],
    ]


def list_all_enums() -> Dict[str, frozenset]:
    """
    Gibt alle definierten Enums und ihre gültigen Werte zurück.
    """
    return {
        e["_enum_name"]: e["_values"]
        for e in [
            EVENT_TYPE_ENUM,
            DEVICE_TYPE_ENUM,
            MODULE_TYPE_ENUM,
            BUS_TYPE_ENUM,
            FUNCTION_TYPE_ENUM,
            STATE_VALUE_ENUM,
            PROCESS_MODE_ENUM,
            CONDITION_TYPE_ENUM,
            RULE_ACTION_TYPE_ENUM,
        ]
    }
