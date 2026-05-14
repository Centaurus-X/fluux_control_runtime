# -*- coding: utf-8 -*-
# src/libraries/_type_mapping_registry.py
#
# v20: Type-Mapping-Vertrag (Writer-Ownership-Registry).
#
# Zweck:
#   Legt vertraglich fest, welcher Writer welchen Config-/Runtime-Typ
#   schreiben darf. Bisher war diese Zuordnung implizit aus dem Code
#   ableitbar — ab v20 ist sie explizit als Registry dokumentiert.
#
# Das ist KEIN Enforcement-Mechanismus. Es ist eine deklarative,
# lesbare Quelle der Wahrheit für Code-Reviews, Architektur-Diskussionen
# und spätere Multi-Node-Szenarien (ab v22+).
#
# Entwurfslinie:
#   - Keine Klassen, keine Decorator.
#   - Nur Lookup- und Suchfunktionen auf einem vorab definierten Dict.
#   - Änderungen am Vertrag sind bewusst kleine Commits mit Review.

TYPE_MAPPING_CONTRACT_VERSION = 2  # v21: consistency_zone-Feld ergänzt

# ---------------------------------------------------------------------------
# Kanonische Writer-IDs
# ---------------------------------------------------------------------------

WRITER_CUD_COMPILER = "cud_compiler"
WRITER_DATASTORE = "worker_datastore"
WRITER_SEM = "system_event_management"
WRITER_WORKER_LOCAL = "worker_local"
WRITER_EDGE_GATEWAY = "edge_gateway"
WRITER_RUNTIME_CAPTURE = "runtime_change_capture"

ALL_WRITERS = (
    WRITER_CUD_COMPILER,
    WRITER_DATASTORE,
    WRITER_SEM,
    WRITER_WORKER_LOCAL,
    WRITER_EDGE_GATEWAY,
    WRITER_RUNTIME_CAPTURE,
)


# ---------------------------------------------------------------------------
# Scope-Kategorien
# ---------------------------------------------------------------------------

SCOPE_CONFIG_FILE = "config_file"
SCOPE_PROJECTION_PATCH = "projection_patch"
SCOPE_RUNTIME_HOT_STATE = "runtime_hot_state"
SCOPE_EVENT_QUEUE = "event_queue"
SCOPE_CHANGE_SET_FILE = "change_set_file"
SCOPE_DECOMPILED_FILE = "decompiled_file"


# ---------------------------------------------------------------------------
# v21: Konsistenzzonen
# ---------------------------------------------------------------------------
#
# Jeder registrierte Typ trägt eine Konsistenzzone. Sie beschreibt, welche
# Konsistenzgarantien der Writer für diesen Typ einhalten muss/kann.
#
# strict:      sofortige Konsistenz zwingend — Single-Writer, atomares
#              Commit, Reader sehen nach Commit garantiert neue Daten
# causal:      kausal konsistent — Vector-Clock (v18) + causation_id (v21)
#              reichen aus, lokale Reordering möglich
# eventual:    eventual konsistent — Reader dürfen kurzzeitig alte Daten
#              sehen, Konvergenz über Zeit
# read_only:   Schreiben wird nicht geregelt (nur Lesen wichtig)

CONSISTENCY_ZONE_STRICT = "strict"
CONSISTENCY_ZONE_CAUSAL = "causal"
CONSISTENCY_ZONE_EVENTUAL = "eventual"
CONSISTENCY_ZONE_READ_ONLY = "read_only"

ALL_CONSISTENCY_ZONES = (
    CONSISTENCY_ZONE_STRICT,
    CONSISTENCY_ZONE_CAUSAL,
    CONSISTENCY_ZONE_EVENTUAL,
    CONSISTENCY_ZONE_READ_ONLY,
)


# ---------------------------------------------------------------------------
# Registry: Typ -> Writer + Scope + Notizen
# ---------------------------------------------------------------------------
#
# Der Schlüssel ist der "logische Typ" (z.B. ein Section-Name im Config
# oder eine Runtime-Kategorie). Jeder Typ hat genau einen Primary-Writer;
# zusätzlich kann es Reader-Roles geben, die hier NICHT enthalten sind
# (Reader-Zugriffe sind uneingeschränkt).

_REGISTRY = {
    # ----------- Config-Datei: Writer ist der Datastore -----------
    "device_state_data_json": {
        "primary_writer": WRITER_DATASTORE,
        "scope": SCOPE_CONFIG_FILE,
        "consistency_zone": CONSISTENCY_ZONE_STRICT,
        "notes": "Der Datastore ist Single-Writer für config/___device_state_data.json.",
    },

    # ----------- Projection-Patches: Writer ist der CUD-Compiler ---
    "projection_bundle": {
        "primary_writer": WRITER_CUD_COMPILER,
        "scope": SCOPE_PROJECTION_PATCH,
        "consistency_zone": CONSISTENCY_ZONE_STRICT,
        "notes": "Der CUD-Compiler erzeugt den gesamten Projection-Bundle inkl. Sidecars.",
    },
    "runtime_json_kernel_patch": {
        "primary_writer": WRITER_CUD_COMPILER,
        "scope": SCOPE_PROJECTION_PATCH,
        "consistency_zone": CONSISTENCY_ZONE_STRICT,
    },
    "process_states_live_projection_patch": {
        "primary_writer": WRITER_CUD_COMPILER,
        "scope": SCOPE_PROJECTION_PATCH,
        "consistency_zone": CONSISTENCY_ZONE_STRICT,
        "notes": "Live-Projection für process_states (v14+).",
    },
    "control_methods_live_projection_patch": {
        "primary_writer": WRITER_CUD_COMPILER,
        "scope": SCOPE_PROJECTION_PATCH,
        "consistency_zone": CONSISTENCY_ZONE_STRICT,
        "notes": "Live-Projection für control_methods (v19+).",
    },

    # ----------- Runtime-Hot-State: Worker ist Single-Writer --------
    "worker_hot_state": {
        "primary_writer": WRITER_WORKER_LOCAL,
        "scope": SCOPE_RUNTIME_HOT_STATE,
        "consistency_zone": CONSISTENCY_ZONE_STRICT,
        "notes": "Jeder Worker schreibt nur seinen eigenen Hot-State.",
    },

    # ----------- Events: SEM ist Cross-Worker-Writer ----------------
    "cross_worker_event": {
        "primary_writer": WRITER_SEM,
        "scope": SCOPE_EVENT_QUEUE,
        "consistency_zone": CONSISTENCY_ZONE_CAUSAL,
        "notes": "SEM ist Single-Writer für alle Events, die Worker-Grenzen überschreiten. Kausalität via Vector-Clock (v18) + causation_id (v21).",
    },
    "local_worker_event": {
        "primary_writer": WRITER_WORKER_LOCAL,
        "scope": SCOPE_EVENT_QUEUE,
        "consistency_zone": CONSISTENCY_ZONE_CAUSAL,
        "notes": "Worker-interne Events bleiben local, kein SEM-Durchlauf.",
    },

    # ----------- v17 Runtime-Change-Capture -------------------------
    "runtime_change_set": {
        "primary_writer": WRITER_RUNTIME_CAPTURE,
        "scope": SCOPE_CHANGE_SET_FILE,
        "consistency_zone": CONSISTENCY_ZONE_CAUSAL,
        "notes": "Der Capture-Producer (v17) schreibt Change-Sets in config/runtime_change_sets/. Kausal konsistent über envelope_id.",
    },
    "reverse_compile_output": {
        "primary_writer": WRITER_CUD_COMPILER,
        "scope": SCOPE_DECOMPILED_FILE,
        "consistency_zone": CONSISTENCY_ZONE_EVENTUAL,
        "notes": "Der Reverse-Compile-Consumer (v17) ruft den CUD-Compiler-Adapter, der die Datei erzeugt.",
    },

    # ----------- Edge-Gateway (geplant) -----------------------------
    "edge_inbound_telemetry": {
        "primary_writer": WRITER_EDGE_GATEWAY,
        "scope": SCOPE_EVENT_QUEUE,
        "consistency_zone": CONSISTENCY_ZONE_EVENTUAL,
        "notes": "Nur Edge-Gateway schreibt eingehende Telemetrie (ab v22+). Eventual konsistent wegen Edge-Netzwerk-Delays.",
    },

    # ----------- v22: Hot-Standby-Heartbeat -------------------------
    "ha_heartbeat": {
        "primary_writer": WRITER_WORKER_LOCAL,
        "scope": SCOPE_EVENT_QUEUE,
        "consistency_zone": CONSISTENCY_ZONE_CAUSAL,
        "notes": "Hot-Standby-Heartbeat zwischen Nodes (v22+). Kausal konsistent über term und vector_clock.",
    },

    # ----------- v23: Raft-Log -------------------------------------
    "raft_log_entry": {
        "primary_writer": WRITER_WORKER_LOCAL,
        "scope": SCOPE_EVENT_QUEUE,
        "consistency_zone": CONSISTENCY_ZONE_STRICT,
        "notes": "Raft-Log-Einträge (v23+). Strikt konsistent via Quorum-Commit.",
    },

    # ----------- v24: Regionale Föderation -------------------------
    "federated_event": {
        "primary_writer": WRITER_WORKER_LOCAL,
        "scope": SCOPE_EVENT_QUEUE,
        "consistency_zone": CONSISTENCY_ZONE_EVENTUAL,
        "notes": "Cross-Site-Events via Federation-Bridge (v24+). Eventual konsistent, site-lokal konsistent via Raft innerhalb der Site.",
    },
}


# ---------------------------------------------------------------------------
# Lookup-Funktionen
# ---------------------------------------------------------------------------

def lookup_primary_writer(type_key):
    """Liefert den Primary-Writer für einen Typ oder None, wenn unbekannt."""
    entry = _REGISTRY.get(type_key)
    if not isinstance(entry, dict):
        return None
    return entry.get("primary_writer")


def lookup_entry(type_key):
    """Liefert den vollständigen Registry-Eintrag oder None."""
    entry = _REGISTRY.get(type_key)
    if not isinstance(entry, dict):
        return None
    return dict(entry)


def lookup_consistency_zone(type_key):
    """v21: Liefert die Konsistenzzone eines Typs oder None, wenn unbekannt."""
    entry = _REGISTRY.get(type_key)
    if not isinstance(entry, dict):
        return None
    return entry.get("consistency_zone")


def list_types_by_writer(writer_id):
    """Liefert alle Typen, für die ein bestimmter Writer Primary-Writer ist."""
    result = []
    for type_key, entry in _REGISTRY.items():
        if entry.get("primary_writer") == writer_id:
            result.append(type_key)
    return sorted(result)


def list_types_by_scope(scope):
    """Liefert alle Typen mit einem bestimmten Scope."""
    result = []
    for type_key, entry in _REGISTRY.items():
        if entry.get("scope") == scope:
            result.append(type_key)
    return sorted(result)


def list_types_by_consistency_zone(zone):
    """v21: Liefert alle Typen mit einer bestimmten Konsistenzzone."""
    result = []
    for type_key, entry in _REGISTRY.items():
        if entry.get("consistency_zone") == zone:
            result.append(type_key)
    return sorted(result)


def all_types():
    """Liefert alle im Vertrag registrierten Typen."""
    return sorted(_REGISTRY.keys())


def validate_registry():
    """Selbst-Check: jede Registry-Zeile ist wohlgeformt."""
    reasons = []
    for type_key, entry in _REGISTRY.items():
        if not isinstance(entry, dict):
            reasons.append("%s: entry is not a dict" % type_key)
            continue
        writer = entry.get("primary_writer")
        if writer not in ALL_WRITERS:
            reasons.append("%s: unknown primary_writer '%s'" % (type_key, writer))
        scope = entry.get("scope")
        allowed_scopes = (
            SCOPE_CONFIG_FILE,
            SCOPE_PROJECTION_PATCH,
            SCOPE_RUNTIME_HOT_STATE,
            SCOPE_EVENT_QUEUE,
            SCOPE_CHANGE_SET_FILE,
            SCOPE_DECOMPILED_FILE,
        )
        if scope not in allowed_scopes:
            reasons.append("%s: unknown scope '%s'" % (type_key, scope))
        # v21: consistency_zone prüfen
        zone = entry.get("consistency_zone")
        if zone not in ALL_CONSISTENCY_ZONES:
            reasons.append("%s: unknown consistency_zone '%s'" % (type_key, zone))
    return reasons


def describe_registry():
    """Kompakter Reporting-Output über die Vertrags-Registry."""
    by_writer = {}
    for writer in ALL_WRITERS:
        by_writer[writer] = list_types_by_writer(writer)
    by_zone = {}
    for zone in ALL_CONSISTENCY_ZONES:
        by_zone[zone] = list_types_by_consistency_zone(zone)
    return {
        "contract_version": TYPE_MAPPING_CONTRACT_VERSION,
        "writers": list(ALL_WRITERS),
        "consistency_zones": list(ALL_CONSISTENCY_ZONES),
        "types_by_writer": by_writer,
        "types_by_consistency_zone": by_zone,
        "total_types": len(_REGISTRY),
    }
