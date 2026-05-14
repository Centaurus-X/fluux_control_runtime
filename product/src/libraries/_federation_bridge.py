# -*- coding: utf-8 -*-
# src/libraries/_federation_bridge.py
#
# v24: Regionale Föderation — Event-Bridge zwischen Sites.
#
# Zweck:
#   Liefert die Datenstrukturen und Routing-Regeln für die lose
#   Kopplung mehrerer Raft-Cluster (Sites) über eine Event-Bridge.
#
#   Jede Site bleibt autonom: intern ein eigener Raft-Cluster (v23) oder
#   ein Hot-Standby-Paar (v22). Die Bridge replicates ausschließlich
#   explizit als "federated" markierte Events zwischen den Sites.
#
# Kernkonzepte:
#   - site_id: eindeutige Kennung eines regionalen Clusters
#   - federation_envelope: zusätzliche Envelope-Felder für Cross-Site-Events
#   - site_vector_clock: Vector-Clock NUR mit site_id-Komponenten
#     (nicht zu verwechseln mit Node-VC aus v18)
#   - de-duplication über (origin_site, origin_envelope_id)
#   - loop-prevention: ein Event, das Site A schon kennt, wird nicht
#     zurück nach A propagiert
#
# Entwurfslinien:
#   - Keine Klassen, keine Decorator.
#   - Library-only.
#   - Thread-safe via threading.Lock.

import threading
import time


FEDERATION_CONTRACT_VERSION = 1

# ---------------------------------------------------------------------------
# Event-Klassifikation
# ---------------------------------------------------------------------------
#
# Nicht jedes interne Event soll über die Bridge laufen. Events werden
# explizit in eine Kategorie eingeordnet:

FED_SCOPE_SITE_LOCAL = "site_local"       # nie über die Bridge
FED_SCOPE_FEDERATED = "federated"         # wird repliziert
FED_SCOPE_BROADCAST = "broadcast"         # an alle Sites (ohne Origin)

ALL_FED_SCOPES = (FED_SCOPE_SITE_LOCAL, FED_SCOPE_FEDERATED, FED_SCOPE_BROADCAST)


# ---------------------------------------------------------------------------
# Federation-Envelope-Felder (additiv zum v18-Envelope)
# ---------------------------------------------------------------------------

def stamp_federation_envelope(event, origin_site_id, fed_scope=FED_SCOPE_SITE_LOCAL):
    """Stempelt ein Event mit Föderations-Metadaten.

    Additiv: der bestehende v18-Envelope bleibt unverändert.
    - fed_origin_site: Site-ID, in der das Event entstanden ist
    - fed_scope: site_local / federated / broadcast
    - fed_visited_sites: Liste bereits besuchter Sites (für Loop-Prevention)
    - fed_stamped_at_ms: Zeitstempel des Stempels
    """
    if not isinstance(event, dict):
        return event

    if "fed_origin_site" not in event:
        event["fed_origin_site"] = str(origin_site_id) if origin_site_id else "unknown"

    if "fed_scope" not in event or event["fed_scope"] not in ALL_FED_SCOPES:
        event["fed_scope"] = fed_scope if fed_scope in ALL_FED_SCOPES else FED_SCOPE_SITE_LOCAL

    if "fed_visited_sites" not in event or not isinstance(event["fed_visited_sites"], list):
        event["fed_visited_sites"] = [event["fed_origin_site"]]

    if "fed_stamped_at_ms" not in event:
        event["fed_stamped_at_ms"] = int(time.monotonic() * 1000)

    return event


def validate_federation_envelope(event):
    reasons = []
    if not isinstance(event, dict):
        return ["event is not a dict"]
    if "fed_origin_site" not in event:
        reasons.append("missing fed_origin_site")
    scope = event.get("fed_scope")
    if scope is not None and scope not in ALL_FED_SCOPES:
        reasons.append("fed_scope has unsupported value: %s" % scope)
    visited = event.get("fed_visited_sites")
    if visited is not None and not isinstance(visited, list):
        reasons.append("fed_visited_sites must be a list")
    return reasons


# ---------------------------------------------------------------------------
# Site-Vector-Clock (pro-Site, nicht pro-Node)
# ---------------------------------------------------------------------------

def empty_site_vector_clock():
    return {}


def advance_site_vector_clock(svc, site_id):
    """Erhöht den Zähler der eigenen Site um 1. Gibt neue Kopie zurück."""
    if not isinstance(svc, dict):
        svc = {}
    new_svc = dict(svc)
    key = site_id or "unknown"
    new_svc[key] = int(new_svc.get(key, 0)) + 1
    return new_svc


def merge_site_vector_clocks(svc_a, svc_b):
    """Komponentenweises Maximum."""
    if not isinstance(svc_a, dict):
        svc_a = {}
    if not isinstance(svc_b, dict):
        svc_b = {}
    merged = {}
    all_sites = set(svc_a.keys()) | set(svc_b.keys())
    for site in all_sites:
        merged[site] = max(int(svc_a.get(site, 0)), int(svc_b.get(site, 0)))
    return merged


# ---------------------------------------------------------------------------
# Bridge-State: De-Duplication + Loop-Prevention
# ---------------------------------------------------------------------------

def build_bridge_state(local_site_id, known_peer_sites=None, dedup_window_size=10000):
    """Baut einen Bridge-State.

    - local_site_id: ID der eigenen Site
    - known_peer_sites: Liste der Peer-Site-IDs (für Broadcast)
    - dedup_window_size: Anzahl der letzten Origin-Envelope-IDs, die
      für De-Duplication im Gedächtnis gehalten werden
    """
    return {
        "lock": threading.Lock(),
        "local_site_id": str(local_site_id),
        "known_peer_sites": list(known_peer_sites) if known_peer_sites else [],
        "dedup_window_size": int(dedup_window_size),
        "seen_origins": [],   # FIFO-Liste von (origin_site, envelope_id)-Tuplen
        "site_vector_clock": {},
        "stats": {
            "outgoing_federated": 0,
            "incoming_accepted": 0,
            "incoming_duplicates": 0,
            "incoming_loop_prevented": 0,
            "incoming_site_local_rejected": 0,
        },
    }


def _dedup_key(event):
    origin = event.get("fed_origin_site")
    eid = event.get("envelope_id")
    if not origin or not eid:
        return None
    return (origin, eid)


def _already_seen(state, key):
    return key in state.get("seen_origins", [])


def _record_seen(state, key):
    window = state.get("dedup_window_size", 10000)
    seen = state["seen_origins"]
    seen.append(key)
    # FIFO-Trimm: älteste Einträge droppen, wenn Fenster voll
    if len(seen) > window:
        overflow = len(seen) - window
        state["seen_origins"] = seen[overflow:]


# ---------------------------------------------------------------------------
# Outgoing: Event für Bridge vorbereiten
# ---------------------------------------------------------------------------

def prepare_outgoing_event(bridge_state, event, fed_scope=FED_SCOPE_FEDERATED):
    """Bereitet ein lokales Event für die Weitergabe über die Bridge vor.

    Rückgabe:
      - stamped-Event (mutiert in-place)
      - list[target_site_ids] der Sites, an die gesendet werden soll
    Bei fed_scope=site_local liefert diese Funktion eine leere Target-Liste,
    der Caller darf das Event nicht über die Bridge senden.
    """
    if not isinstance(bridge_state, dict):
        return event, []
    lock = bridge_state["lock"]
    with lock:
        local_site = bridge_state["local_site_id"]
        stamp_federation_envelope(event, origin_site_id=local_site, fed_scope=fed_scope)
        bridge_state["site_vector_clock"] = advance_site_vector_clock(
            bridge_state.get("site_vector_clock", {}), local_site,
        )
        event["fed_site_vector_clock"] = dict(bridge_state["site_vector_clock"])

        if event["fed_scope"] == FED_SCOPE_SITE_LOCAL:
            return event, []

        targets = []
        for peer in bridge_state.get("known_peer_sites", []):
            if peer != local_site and peer not in event.get("fed_visited_sites", []):
                targets.append(peer)

        if targets:
            bridge_state["stats"]["outgoing_federated"] += 1
        return event, targets


# ---------------------------------------------------------------------------
# Incoming: Event von der Bridge verarbeiten
# ---------------------------------------------------------------------------

def accept_incoming_event(bridge_state, event):
    """Entscheidet, ob ein eingehendes Event akzeptiert und lokal
    angewendet werden soll.

    Rückgabe:
      - ("accept", stamped_event)             → verarbeiten + ggf. weiter-broadcasten
      - ("duplicate", reason)                 → ignorieren
      - ("loop_prevented", reason)            → ignorieren
      - ("site_local_rejected", reason)       → ignorieren
      - ("invalid", reasons)                  → nicht akzeptiert
    """
    if not isinstance(bridge_state, dict):
        return ("invalid", ["bridge_state is not a dict"])
    reasons = validate_federation_envelope(event)
    if reasons:
        return ("invalid", reasons)

    lock = bridge_state["lock"]
    with lock:
        local_site = bridge_state["local_site_id"]
        scope = event.get("fed_scope", FED_SCOPE_SITE_LOCAL)

        if scope == FED_SCOPE_SITE_LOCAL:
            bridge_state["stats"]["incoming_site_local_rejected"] += 1
            return ("site_local_rejected", "event is site_local")

        # Loop-Prevention: wenn dieser Node (Site) bereits in visited steht
        visited = event.get("fed_visited_sites", [])
        if local_site in visited:
            bridge_state["stats"]["incoming_loop_prevented"] += 1
            return ("loop_prevented", "local_site already in fed_visited_sites")

        # De-Duplication: bereits gesehene (origin, eid)?
        key = _dedup_key(event)
        if key and _already_seen(bridge_state, key):
            bridge_state["stats"]["incoming_duplicates"] += 1
            return ("duplicate", "origin+envelope_id already processed")

        # Akzeptieren: visited ergänzen, dedup-memo eintragen, site-VC mergen
        visited_copy = list(visited)
        visited_copy.append(local_site)
        event["fed_visited_sites"] = visited_copy
        if key:
            _record_seen(bridge_state, key)

        incoming_svc = event.get("fed_site_vector_clock", {})
        bridge_state["site_vector_clock"] = merge_site_vector_clocks(
            bridge_state.get("site_vector_clock", {}), incoming_svc,
        )
        event["fed_site_vector_clock"] = dict(bridge_state["site_vector_clock"])

        bridge_state["stats"]["incoming_accepted"] += 1
        return ("accept", event)


def forward_to_peers(bridge_state, event):
    """Berechnet die Peer-Sites, an die ein akzeptiertes Event weitergeleitet
    werden soll (Broadcast-Szenario). Aktualisiert visited nicht (macht
    accept_incoming_event)."""
    if not isinstance(bridge_state, dict) or not isinstance(event, dict):
        return []
    lock = bridge_state["lock"]
    with lock:
        local_site = bridge_state["local_site_id"]
        visited = event.get("fed_visited_sites", [])
        if event.get("fed_scope") != FED_SCOPE_BROADCAST:
            return []
        targets = []
        for peer in bridge_state.get("known_peer_sites", []):
            if peer != local_site and peer not in visited:
                targets.append(peer)
        return targets


def describe_bridge_state(bridge_state):
    if not isinstance(bridge_state, dict):
        return {}
    lock = bridge_state.get("lock")
    if lock is None:
        return {}
    with lock:
        return {
            "local_site_id": bridge_state.get("local_site_id"),
            "known_peer_sites": list(bridge_state.get("known_peer_sites", [])),
            "dedup_window_size": bridge_state.get("dedup_window_size"),
            "dedup_window_used": len(bridge_state.get("seen_origins", [])),
            "site_vector_clock": dict(bridge_state.get("site_vector_clock", {})),
            "stats": dict(bridge_state.get("stats", {})),
        }
