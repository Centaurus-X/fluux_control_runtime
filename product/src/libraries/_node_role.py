# -*- coding: utf-8 -*-
# src/libraries/_node_role.py
#
# v22: Hot-Standby — Node-Role-Library.
#
# Zweck:
#   Liefert deklarative Rollen (leader/standby/observer) und eine
#   Failover-Entscheidungslogik basierend auf Vector Clocks (v18).
#
#   Default: single_node-Betrieb. In diesem Modus ist der lokale Node
#   IMMER leader, es gibt keine Standby-Replikas, und Heartbeat/Failover
#   sind inaktiv. Hot-Standby ist opt-in per Konfiguration.
#
# Entwurfslinien:
#   - Keine Klassen, keine Decorator.
#   - functools.partial statt Lambda.
#   - Thread-safe via threading.Lock für zustandsbehaftete Funktionen.
#   - Kein eigener Scheduler — Heartbeats werden vom Caller getickt.
#   - Integration in worker_datastore.py ist v25+-Scope, v22 liefert
#     nur die Library mit voller Testabdeckung.

import threading
import time


NODE_ROLE_CONTRACT_VERSION = 1

# ---------------------------------------------------------------------------
# Rollen und Zustände
# ---------------------------------------------------------------------------

ROLE_LEADER = "leader"
ROLE_STANDBY = "standby"
ROLE_OBSERVER = "observer"
ROLE_UNKNOWN = "unknown"

ALL_ROLES = (ROLE_LEADER, ROLE_STANDBY, ROLE_OBSERVER, ROLE_UNKNOWN)

MODE_SINGLE_NODE = "single_node"
MODE_HOT_STANDBY = "hot_standby"

ALL_MODES = (MODE_SINGLE_NODE, MODE_HOT_STANDBY)


# ---------------------------------------------------------------------------
# Heartbeat-Struktur
# ---------------------------------------------------------------------------
#
# Ein Heartbeat ist ein dict mit:
#   - node_id       : str
#   - role          : str (leader|standby|observer)
#   - vector_clock  : dict (kompatibel zu v18)
#   - sent_at_ms    : int (monotonic time ms)
#   - term          : int (monoton wachsend pro Leader-Wechsel)


def build_heartbeat(node_id, role, vector_clock, term, sent_at_ms=None):
    """Erzeugt eine Heartbeat-Struktur. Validiert Pflichtfelder nicht hart —
    das ist Aufgabe von validate_heartbeat()."""
    if sent_at_ms is None:
        sent_at_ms = int(time.monotonic() * 1000)
    return {
        "node_id": str(node_id) if node_id is not None else "unknown",
        "role": role if role in ALL_ROLES else ROLE_UNKNOWN,
        "vector_clock": vector_clock if isinstance(vector_clock, dict) else {},
        "term": int(term) if term is not None else 0,
        "sent_at_ms": int(sent_at_ms),
    }


def validate_heartbeat(heartbeat):
    reasons = []
    if not isinstance(heartbeat, dict):
        return ["heartbeat is not a dict"]
    for key in ("node_id", "role", "vector_clock", "term", "sent_at_ms"):
        if key not in heartbeat:
            reasons.append("heartbeat missing required key: %s" % key)
    role = heartbeat.get("role")
    if role is not None and role not in ALL_ROLES:
        reasons.append("heartbeat.role has unsupported value: %s" % role)
    vc = heartbeat.get("vector_clock")
    if vc is not None and not isinstance(vc, dict):
        reasons.append("heartbeat.vector_clock must be a dict")
    term = heartbeat.get("term")
    if term is not None and not isinstance(term, int):
        reasons.append("heartbeat.term must be an int")
    return reasons


# ---------------------------------------------------------------------------
# Node-State: zustandsbehaftete Rollen-Logik
# ---------------------------------------------------------------------------

def build_node_state(
    node_id,
    mode=MODE_SINGLE_NODE,
    initial_role=None,
    heartbeat_interval_ms=1000,
    heartbeat_timeout_ms=3000,
):
    """Baut einen Node-State. Thread-safe.

    Bei mode=MODE_SINGLE_NODE ist der Node immer leader und die
    Failover-Logik ist inaktiv.
    """
    if initial_role is None:
        initial_role = ROLE_LEADER if mode == MODE_SINGLE_NODE else ROLE_STANDBY

    return {
        "lock": threading.Lock(),
        "node_id": str(node_id),
        "mode": mode if mode in ALL_MODES else MODE_SINGLE_NODE,
        "role": initial_role if initial_role in ALL_ROLES else ROLE_UNKNOWN,
        "term": 0,
        "last_leader_heartbeat_ms": 0,
        "known_leader_id": None,
        "peers": {},  # node_id -> last_heartbeat
        "heartbeat_interval_ms": int(heartbeat_interval_ms),
        "heartbeat_timeout_ms": int(heartbeat_timeout_ms),
        "vector_clock": {},
    }


def get_role(state):
    if not isinstance(state, dict):
        return ROLE_UNKNOWN
    lock = state.get("lock")
    if lock is None:
        return state.get("role", ROLE_UNKNOWN)
    with lock:
        return state.get("role", ROLE_UNKNOWN)


def is_leader(state):
    return get_role(state) == ROLE_LEADER


def is_standby(state):
    return get_role(state) == ROLE_STANDBY


def describe_state(state):
    if not isinstance(state, dict):
        return {}
    lock = state.get("lock")
    if lock is None:
        return dict(state)
    with lock:
        return {
            "node_id": state.get("node_id"),
            "mode": state.get("mode"),
            "role": state.get("role"),
            "term": state.get("term"),
            "known_leader_id": state.get("known_leader_id"),
            "peer_count": len(state.get("peers", {})),
            "heartbeat_interval_ms": state.get("heartbeat_interval_ms"),
            "heartbeat_timeout_ms": state.get("heartbeat_timeout_ms"),
        }


# ---------------------------------------------------------------------------
# Heartbeat-Tick: vom Caller (z.B. worker_datastore main loop) aufgerufen
# ---------------------------------------------------------------------------

def receive_heartbeat(state, heartbeat, now_ms=None):
    """Verarbeitet einen eingehenden Heartbeat von einem Peer.

    Regeln:
      - Heartbeat eines Leaders mit höherem term → lokaler Node wird Standby
      - Heartbeat eines Standbys → nur als Peer registriert
    """
    if not isinstance(state, dict):
        return
    if not isinstance(heartbeat, dict):
        return
    if validate_heartbeat(heartbeat):
        return
    if now_ms is None:
        now_ms = int(time.monotonic() * 1000)

    lock = state["lock"]
    with lock:
        if state.get("mode") != MODE_HOT_STANDBY:
            return

        sender_id = heartbeat["node_id"]
        if sender_id == state["node_id"]:
            return  # Eigene Heartbeats ignorieren

        sender_role = heartbeat["role"]
        sender_term = int(heartbeat.get("term", 0))

        state["peers"][sender_id] = {
            "heartbeat": heartbeat,
            "received_at_ms": now_ms,
        }

        if sender_role == ROLE_LEADER:
            if sender_term > state["term"]:
                state["term"] = sender_term
                state["role"] = ROLE_STANDBY
                state["known_leader_id"] = sender_id
                state["last_leader_heartbeat_ms"] = now_ms
            elif sender_term == state["term"] and state["role"] != ROLE_LEADER:
                state["known_leader_id"] = sender_id
                state["last_leader_heartbeat_ms"] = now_ms


def check_leader_timeout(state, now_ms=None):
    """Prüft, ob der bekannte Leader zu lange keinen Heartbeat gesendet hat.
    Rückgabe: True, wenn Leader-Timeout erkannt wurde."""
    if not isinstance(state, dict):
        return False
    if state.get("mode") != MODE_HOT_STANDBY:
        return False
    if now_ms is None:
        now_ms = int(time.monotonic() * 1000)

    lock = state["lock"]
    with lock:
        if state["role"] == ROLE_LEADER:
            return False
        last = state.get("last_leader_heartbeat_ms", 0)
        if last == 0:
            return False
        timeout_ms = state["heartbeat_timeout_ms"]
        return (now_ms - last) > timeout_ms


# ---------------------------------------------------------------------------
# Failover-Entscheidung
# ---------------------------------------------------------------------------

def _vector_clock_rank(vc, node_id):
    """Rank-Funktion für deterministische Tiebreaking: Summe der Zähler,
    dann node_id als Sekundär-Sortierschlüssel (lexikographisch größer wins)."""
    if not isinstance(vc, dict):
        return (0, node_id or "")
    total = 0
    for value in vc.values():
        try:
            total += int(value)
        except Exception:
            continue
    return (total, node_id or "")


def should_become_leader(state, now_ms=None):
    """Entscheidet deterministisch, ob der lokale Node bei Leader-Timeout
    selbst Leader werden soll.

    Regeln:
      1. Nur wenn mode=HOT_STANDBY und role=STANDBY
      2. Nur wenn Leader-Heartbeat getimeoutet ist
      3. Unter allen erreichbaren Peers (+ selbst) mit Standby-Role
         gewinnt der Node mit dem höchsten Vector-Clock-Rank.
         Tiebreaker: node_id lexikographisch.
    """
    if not isinstance(state, dict):
        return False
    if state.get("mode") != MODE_HOT_STANDBY:
        return False
    if now_ms is None:
        now_ms = int(time.monotonic() * 1000)

    lock = state["lock"]
    with lock:
        if state["role"] != ROLE_STANDBY:
            return False
        last = state.get("last_leader_heartbeat_ms", 0)
        if last == 0:
            return False
        if (now_ms - last) <= state["heartbeat_timeout_ms"]:
            return False

        candidates = [(state["node_id"], state.get("vector_clock", {}))]
        timeout_ms = state["heartbeat_timeout_ms"]
        for peer_id, peer_entry in state.get("peers", {}).items():
            peer_received_at = peer_entry.get("received_at_ms", 0)
            if (now_ms - peer_received_at) > timeout_ms:
                continue  # veraltete Peer-Info
            peer_hb = peer_entry.get("heartbeat", {})
            if peer_hb.get("role") == ROLE_STANDBY:
                candidates.append((peer_id, peer_hb.get("vector_clock", {})))

        if not candidates:
            return False

        ranked = [(_vector_clock_rank(vc, nid), nid) for (nid, vc) in candidates]
        ranked.sort()  # aufsteigend
        winner_node_id = ranked[-1][1]
        return winner_node_id == state["node_id"]


def promote_to_leader(state, now_ms=None):
    """Promoviert den lokalen Node zum Leader. Erhöht term monoton."""
    if not isinstance(state, dict):
        return False
    if now_ms is None:
        now_ms = int(time.monotonic() * 1000)
    lock = state["lock"]
    with lock:
        if state.get("mode") == MODE_SINGLE_NODE:
            return state["role"] == ROLE_LEADER
        state["role"] = ROLE_LEADER
        state["term"] = int(state.get("term", 0)) + 1
        state["known_leader_id"] = state["node_id"]
        state["last_leader_heartbeat_ms"] = now_ms
        return True


def demote_to_standby(state, known_leader_id=None, new_term=None):
    """Degradiert den lokalen Node zu Standby. Genutzt, wenn der aktuelle
    Leader einen höheren term meldet."""
    if not isinstance(state, dict):
        return False
    lock = state["lock"]
    with lock:
        if state.get("mode") == MODE_SINGLE_NODE:
            return False
        state["role"] = ROLE_STANDBY
        if known_leader_id is not None:
            state["known_leader_id"] = known_leader_id
        if new_term is not None and int(new_term) > int(state.get("term", 0)):
            state["term"] = int(new_term)
        return True


def update_vector_clock(state, new_vc):
    """Speichert den aktuellen Vector-Clock-Stand im Node-State ab, damit
    eingehende Failover-Entscheidungen den Fortschritt berücksichtigen."""
    if not isinstance(state, dict):
        return
    if not isinstance(new_vc, dict):
        return
    lock = state["lock"]
    with lock:
        state["vector_clock"] = dict(new_vc)


def build_own_heartbeat(state, now_ms=None):
    """Baut einen Heartbeat, den der lokale Node versendet."""
    if not isinstance(state, dict):
        return None
    if now_ms is None:
        now_ms = int(time.monotonic() * 1000)
    lock = state["lock"]
    with lock:
        return build_heartbeat(
            node_id=state["node_id"],
            role=state["role"],
            vector_clock=dict(state.get("vector_clock", {})),
            term=state.get("term", 0),
            sent_at_ms=now_ms,
        )
