# -*- coding: utf-8 -*-
# src/libraries/_event_envelope.py
#
# Event-Envelope-Vertrag v1 (eingeführt in v18).
#
# Zweck:
#   Erweitert die bestehenden minimalen Event-Dicts um semantische
#   Transport-Felder (message_kind, causation_id, sequence_no,
#   vector_clock, trust_level, schema_version, source_ref).
#
#   Der bestehende make_event() in _evt_interface.py bleibt unverändert.
#   Diese Library bietet eine additive Anreicherungsschicht, die im Broker
#   optional aufgerufen werden kann. Default-off.
#
# Entwurfslinien:
#   - Keine Klassen, keine Decorator.
#   - functools.partial statt Lambda.
#   - Alle Funktionen sind idempotent bezüglich bereits vorhandener Felder.
#   - Niemals überschreiben von Feldern, die bereits einen Wert haben
#     (außer bei explizitem overwrite=True).

import copy
import threading
import time
import uuid
from functools import partial


ENVELOPE_SCHEMA_VERSION = 1

ALLOWED_MESSAGE_KINDS = (
    "intent",
    "command",
    "state",
    "telemetry",
    "result",
    "audit",
    "control",
    "unknown",
)

ALLOWED_TRUST_LEVELS = (
    "internal",
    "authorized",
    "untrusted",
)

ENVELOPE_REQUIRED_KEYS = (
    "envelope_schema_version",
    "envelope_id",
    "message_kind",
    "source_ref",
    "sequence_no",
)

ENVELOPE_OPTIONAL_KEYS = (
    "causation_id",
    "correlation_id",
    "vector_clock",
    "trust_level",
    "priority",
    "revision_ref",
    "audit_required",
)


def _default_node_id():
    return "node_main"


def _stable_now_ms():
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Sequence-State je source_ref
# ---------------------------------------------------------------------------

def build_sequence_state():
    """Baut einen Sequence-State, der pro source_ref monoton wachsende
    Sequenznummern vergibt. Thread-safe."""
    return {
        "lock": threading.Lock(),
        "counters": {},
    }


def next_sequence_no(state, source_ref):
    if not isinstance(state, dict) or source_ref is None:
        return 0
    lock = state.get("lock")
    counters = state.get("counters")
    if lock is None or counters is None:
        return 0
    with lock:
        current = counters.get(source_ref, 0) + 1
        counters[source_ref] = current
        return current


# ---------------------------------------------------------------------------
# Vector Clocks (Lamport-artig, pro Node-Zähler)
# ---------------------------------------------------------------------------

def empty_vector_clock():
    return {}


def advance_vector_clock(vc, node_id):
    """Erhöht den eigenen Zähler um 1. Gibt neue Kopie zurück (keine Mutation)."""
    if not isinstance(vc, dict):
        vc = {}
    new_vc = dict(vc)
    node = node_id or _default_node_id()
    new_vc[node] = int(new_vc.get(node, 0)) + 1
    return new_vc


def merge_vector_clocks(vc_a, vc_b):
    """Komponentenweises Maximum, plus eigene Kopie."""
    if not isinstance(vc_a, dict):
        vc_a = {}
    if not isinstance(vc_b, dict):
        vc_b = {}
    all_nodes = set(vc_a.keys()) | set(vc_b.keys())
    merged = {}
    for node in all_nodes:
        merged[node] = max(int(vc_a.get(node, 0)), int(vc_b.get(node, 0)))
    return merged


def happens_before(vc_a, vc_b):
    """Gibt True zurück, wenn vc_a strikt vor vc_b liegt (happens-before)."""
    if not isinstance(vc_a, dict) or not isinstance(vc_b, dict):
        return False
    all_nodes = set(vc_a.keys()) | set(vc_b.keys())
    any_strict_less = False
    for node in all_nodes:
        a = int(vc_a.get(node, 0))
        b = int(vc_b.get(node, 0))
        if a > b:
            return False
        if a < b:
            any_strict_less = True
    return any_strict_less


def concurrent(vc_a, vc_b):
    """Weder vc_a happens-before vc_b, noch umgekehrt, noch identisch."""
    if vc_a == vc_b:
        return False
    return not happens_before(vc_a, vc_b) and not happens_before(vc_b, vc_a)


# ---------------------------------------------------------------------------
# Envelope-Enrichment (non-destructive)
# ---------------------------------------------------------------------------

def _derive_message_kind_from_event(event):
    """Heuristische Zuordnung für bestehende Events, deren message_kind
    noch nicht gesetzt ist."""
    if not isinstance(event, dict):
        return "unknown"

    explicit = event.get("message_kind")
    if isinstance(explicit, str) and explicit in ALLOWED_MESSAGE_KINDS:
        return explicit

    event_type = str(event.get("event_type") or "").upper()
    if not event_type:
        return "unknown"
    if "CONFIG_" in event_type or event_type.startswith("CONFIG"):
        return "state"
    if event_type.startswith("REQUEST") or event_type.endswith("_REQUEST"):
        return "intent"
    if event_type.startswith("RESULT") or event_type.endswith("_RESULT"):
        return "result"
    if event_type.startswith("COMMAND") or event_type.endswith("_COMMAND"):
        return "command"
    if event_type.startswith("TELEMETRY") or event_type.endswith("_TELEMETRY"):
        return "telemetry"
    if event_type.startswith("AUDIT") or event_type.endswith("_AUDIT"):
        return "audit"
    if event_type.startswith("CONTROL") or event_type.endswith("_CONTROL"):
        return "control"
    return "unknown"


def enrich_envelope(
    event,
    source_ref=None,
    sequence_state=None,
    node_id=None,
    causation_id=None,
    revision_ref=None,
    trust_level=None,
    priority=None,
    overwrite=False,
):
    """Reichert ein bestehendes Event-Dict um Envelope-Felder an.

    Idempotent: Bereits gesetzte Felder werden nicht überschrieben,
    es sei denn overwrite=True.

    Never returns a new object — mutation happens in place for
    Kompatibilität mit Downstream-Routing, das das gleiche Dict weitergibt.
    Rückgabewert ist das (eventuell mutierte) Event.
    """
    if not isinstance(event, dict):
        return event

    if overwrite or "envelope_schema_version" not in event:
        event["envelope_schema_version"] = ENVELOPE_SCHEMA_VERSION

    if overwrite or "envelope_id" not in event:
        event["envelope_id"] = str(uuid.uuid4())

    if overwrite or "message_kind" not in event:
        event["message_kind"] = _derive_message_kind_from_event(event)

    if overwrite or "source_ref" not in event:
        event["source_ref"] = source_ref or event.get("source") or "unknown"

    if overwrite or "sequence_no" not in event:
        seq = next_sequence_no(sequence_state, event.get("source_ref")) if sequence_state else 0
        event["sequence_no"] = seq

    if causation_id is not None and (overwrite or "causation_id" not in event):
        event["causation_id"] = causation_id

    if revision_ref is not None and (overwrite or "revision_ref" not in event):
        event["revision_ref"] = revision_ref

    if overwrite or "trust_level" not in event:
        if trust_level is None:
            trust_level = "internal"
        if trust_level in ALLOWED_TRUST_LEVELS:
            event["trust_level"] = trust_level

    if priority is not None and (overwrite or "priority" not in event):
        event["priority"] = priority

    if overwrite or "vector_clock" not in event:
        event["vector_clock"] = advance_vector_clock(
            event.get("vector_clock"),
            node_id or _default_node_id(),
        )

    return event


# ---------------------------------------------------------------------------
# Envelope-Validation (lenient vs. strict)
# ---------------------------------------------------------------------------

def validate_envelope(event, strict=False):
    """Prüft Envelope-Form. Lenient-Modus (default): liefert (True, []),
    wenn keine schwerwiegenden Verletzungen; Strict-Modus: alle Pflichtfelder
    müssen vorhanden und plausibel sein.
    """
    reasons = []
    if not isinstance(event, dict):
        return False, ["event is not a dict"]

    if strict:
        for key in ENVELOPE_REQUIRED_KEYS:
            if key not in event:
                reasons.append("missing required envelope key: %s" % key)

    message_kind = event.get("message_kind")
    if message_kind is not None and message_kind not in ALLOWED_MESSAGE_KINDS:
        reasons.append("message_kind has unsupported value: %s" % message_kind)

    trust_level = event.get("trust_level")
    if trust_level is not None and trust_level not in ALLOWED_TRUST_LEVELS:
        reasons.append("trust_level has unsupported value: %s" % trust_level)

    version = event.get("envelope_schema_version")
    if version is not None and int(version) != ENVELOPE_SCHEMA_VERSION:
        reasons.append(
            "envelope_schema_version mismatch: got %s, expected %s"
            % (version, ENVELOPE_SCHEMA_VERSION)
        )

    vc = event.get("vector_clock")
    if vc is not None and not isinstance(vc, dict):
        reasons.append("vector_clock must be a dict")

    return (len(reasons) == 0), reasons


# ---------------------------------------------------------------------------
# Broker-Integration-Helfer (non-destructive wrapper)
# ---------------------------------------------------------------------------

def build_broker_enrichment_fn(
    source_ref="broker",
    sequence_state=None,
    node_id=None,
    trust_level="internal",
):
    """Liefert einen vorkonfigurierten Enrichment-Callback, den der Broker
    per partial(...) vor dem Weiterleiten auf Events anwenden kann.
    """
    if sequence_state is None:
        sequence_state = build_sequence_state()
    return partial(
        enrich_envelope,
        source_ref=source_ref,
        sequence_state=sequence_state,
        node_id=node_id,
        trust_level=trust_level,
        overwrite=False,
    )


def describe_envelope(event):
    """Extrahiert die Envelope-Felder für Logging / Diagnose."""
    if not isinstance(event, dict):
        return {}
    return {
        "envelope_id": event.get("envelope_id"),
        "message_kind": event.get("message_kind"),
        "source_ref": event.get("source_ref"),
        "sequence_no": event.get("sequence_no"),
        "causation_id": event.get("causation_id"),
        "vector_clock": event.get("vector_clock"),
        "trust_level": event.get("trust_level"),
        "priority": event.get("priority"),
        "revision_ref": event.get("revision_ref"),
    }


# ===========================================================================
# v21: Automatischer causation_id-Tracker
# ===========================================================================
#
# Zweck:
#   Bisher (v18) wurde causation_id nur gesetzt, wenn der Produzent sie
#   explizit übergab. Der Tracker aus v21 merkt sich pro source_ref die
#   letzte envelope_id und setzt sie automatisch als causation_id des
#   nächsten Events aus derselben Quelle. Damit entsteht eine kausale
#   Kette ohne Zutun der Produzenten.


def build_causation_tracker():
    """Baut einen causation-Tracker-State. Thread-safe.

    Struktur: dict mit lock + last_envelope_id-Map je source_ref.
    """
    return {
        "lock": threading.Lock(),
        "last_envelope_id": {},
    }


def record_envelope(tracker, source_ref, envelope_id):
    """Legt die letzte envelope_id für einen source_ref ab."""
    if not isinstance(tracker, dict) or not source_ref or not envelope_id:
        return
    lock = tracker.get("lock")
    store = tracker.get("last_envelope_id")
    if lock is None or store is None:
        return
    with lock:
        store[source_ref] = envelope_id


def lookup_last_envelope_id(tracker, source_ref):
    """Liefert die letzte envelope_id für einen source_ref oder None."""
    if not isinstance(tracker, dict) or not source_ref:
        return None
    lock = tracker.get("lock")
    store = tracker.get("last_envelope_id")
    if lock is None or store is None:
        return None
    with lock:
        return store.get(source_ref)


def build_broker_enrichment_fn_with_tracker(
    source_ref="broker",
    sequence_state=None,
    node_id=None,
    trust_level="internal",
    causation_tracker=None,
):
    """v21: Wie build_broker_enrichment_fn, aber mit automatischem
    causation_id-Tracking. Jedes neu enrichte Event erhält automatisch
    die envelope_id des direkten Vorgängers aus derselben source_ref als
    causation_id.

    Wenn causation_tracker=None ist, verhält sich diese Funktion wie
    build_broker_enrichment_fn (kein Tracking).
    """
    if sequence_state is None:
        sequence_state = build_sequence_state()

    def _enrich_with_tracking(event):
        if not isinstance(event, dict):
            return event

        # Vor dem Enrichment: causation_id aus Tracker ableiten, sofern nicht
        # explizit gesetzt. Die source_ref kommt entweder aus dem Event
        # selbst oder aus dem default-source_ref dieses Enrichment-Handles.
        if causation_tracker is not None and "causation_id" not in event:
            effective_source = event.get("source_ref") or source_ref
            last_id = lookup_last_envelope_id(causation_tracker, effective_source)
            if last_id:
                event["causation_id"] = last_id

        enriched = enrich_envelope(
            event,
            source_ref=source_ref,
            sequence_state=sequence_state,
            node_id=node_id,
            trust_level=trust_level,
            overwrite=False,
        )

        # Nach dem Enrichment: die eben vergebene envelope_id als neuen
        # "last" für diese source_ref merken.
        if causation_tracker is not None:
            effective_source = enriched.get("source_ref") or source_ref
            new_eid = enriched.get("envelope_id")
            if new_eid:
                record_envelope(causation_tracker, effective_source, new_eid)

        return enriched

    return _enrich_with_tracking
