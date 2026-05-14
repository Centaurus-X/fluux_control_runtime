# -*- coding: utf-8 -*-
# src/libraries/_raft_election.py
#
# v23: Raft-Multi-Master — minimale Kern-Library.
#
# Zweck:
#   Liefert Raft-konforme Election-Logik mit Quorum-basierter Leader-Wahl,
#   Log-Replication-Grundstrukturen und Konflikt-Auflösung via Term+Index.
#
#   Baut auf v22 (_node_role.py) auf: Rolle/term/vector_clock sind
#   kompatibel. Raft fügt ergänzend hinzu:
#     - CANDIDATE-Rolle während Wahl
#     - RequestVote + AppendEntries RPC-Datenformate
#     - Log-Index-basierte Konsistenzgarantien
#     - Quorum-Rechnung (majority = floor(N/2) + 1)
#
# Entwurfslinien:
#   - Keine Klassen, keine Decorator.
#   - Library-only — Transport und Runtime-Integration nicht enthalten.
#   - Kompatibel zum v22-Heartbeat-Format, wenn gewünscht.
#   - Thread-safe zustandsbehaftete Operationen via threading.Lock.

import threading
import time


RAFT_CONTRACT_VERSION = 1

# ---------------------------------------------------------------------------
# Raft-Rollen (erweitert v22)
# ---------------------------------------------------------------------------

RAFT_ROLE_FOLLOWER = "follower"
RAFT_ROLE_CANDIDATE = "candidate"
RAFT_ROLE_LEADER = "leader"

ALL_RAFT_ROLES = (RAFT_ROLE_FOLLOWER, RAFT_ROLE_CANDIDATE, RAFT_ROLE_LEADER)


# ---------------------------------------------------------------------------
# Quorum-Berechnung
# ---------------------------------------------------------------------------

def compute_quorum_size(cluster_size):
    """Klassisches Raft-Quorum: majority = floor(N/2) + 1.

    N=1 → 1
    N=2 → 2 (kein echtes Quorum — wird blockieren, deshalb nicht empfohlen)
    N=3 → 2
    N=4 → 3
    N=5 → 3
    """
    if not isinstance(cluster_size, int) or cluster_size < 1:
        return 1
    return (cluster_size // 2) + 1


def has_quorum(vote_count, cluster_size):
    return int(vote_count) >= compute_quorum_size(cluster_size)


# ---------------------------------------------------------------------------
# RequestVote-RPC
# ---------------------------------------------------------------------------
#
# Request:
#   - candidate_id       : str
#   - candidate_term     : int
#   - last_log_index     : int
#   - last_log_term      : int
#
# Response:
#   - voter_id           : str
#   - current_term       : int (so hoch wie mindestens candidate_term)
#   - vote_granted       : bool


def build_request_vote(candidate_id, candidate_term, last_log_index, last_log_term):
    return {
        "candidate_id": str(candidate_id),
        "candidate_term": int(candidate_term),
        "last_log_index": int(last_log_index),
        "last_log_term": int(last_log_term),
    }


def build_vote_response(voter_id, current_term, vote_granted):
    return {
        "voter_id": str(voter_id),
        "current_term": int(current_term),
        "vote_granted": bool(vote_granted),
    }


def _log_up_to_date(voter_last_term, voter_last_index, candidate_last_term, candidate_last_index):
    """Raft-Regel: Voter stimmt nur zu, wenn Candidate-Log mindestens so
    aktuell ist wie eigenes Log."""
    if int(candidate_last_term) > int(voter_last_term):
        return True
    if int(candidate_last_term) == int(voter_last_term):
        return int(candidate_last_index) >= int(voter_last_index)
    return False


def handle_request_vote(raft_state, request):
    """Verarbeitet einen eingehenden RequestVote-RPC.
    Rückgabe: vote_response-Dict."""
    if not isinstance(raft_state, dict) or not isinstance(request, dict):
        return build_vote_response("unknown", 0, False)

    lock = raft_state["lock"]
    with lock:
        current_term = int(raft_state.get("current_term", 0))
        voted_for = raft_state.get("voted_for")
        last_log_index = int(raft_state.get("last_log_index", 0))
        last_log_term = int(raft_state.get("last_log_term", 0))

        candidate_term = int(request.get("candidate_term", 0))

        # Term-Check: wenn Candidate älteren term hat, ablehnen
        if candidate_term < current_term:
            return build_vote_response(
                raft_state["node_id"], current_term, False
            )

        # Wenn Candidate neueren term hat, aktualisieren + zurück zu Follower
        if candidate_term > current_term:
            raft_state["current_term"] = candidate_term
            raft_state["voted_for"] = None
            raft_state["role"] = RAFT_ROLE_FOLLOWER
            current_term = candidate_term
            voted_for = None

        candidate_id = request.get("candidate_id")

        # Jede term nur EINMAL voten
        if voted_for is not None and voted_for != candidate_id:
            return build_vote_response(
                raft_state["node_id"], current_term, False
            )

        # Log-Aktualität prüfen
        if not _log_up_to_date(
            last_log_term, last_log_index,
            request.get("last_log_term", 0),
            request.get("last_log_index", 0),
        ):
            return build_vote_response(
                raft_state["node_id"], current_term, False
            )

        # Vote granted
        raft_state["voted_for"] = candidate_id
        return build_vote_response(
            raft_state["node_id"], current_term, True
        )


# ---------------------------------------------------------------------------
# AppendEntries-RPC (Heartbeat + Log-Replication)
# ---------------------------------------------------------------------------

def build_append_entries(
    leader_id, leader_term, prev_log_index, prev_log_term,
    entries=None, leader_commit=0,
):
    return {
        "leader_id": str(leader_id),
        "leader_term": int(leader_term),
        "prev_log_index": int(prev_log_index),
        "prev_log_term": int(prev_log_term),
        "entries": list(entries) if entries else [],
        "leader_commit": int(leader_commit),
    }


def build_append_response(follower_id, current_term, success, match_index=0):
    return {
        "follower_id": str(follower_id),
        "current_term": int(current_term),
        "success": bool(success),
        "match_index": int(match_index),
    }


def handle_append_entries(raft_state, request):
    """Verarbeitet einen eingehenden AppendEntries-RPC.
    Rückgabe: append_response-Dict."""
    if not isinstance(raft_state, dict) or not isinstance(request, dict):
        return build_append_response("unknown", 0, False)

    lock = raft_state["lock"]
    with lock:
        current_term = int(raft_state.get("current_term", 0))
        leader_term = int(request.get("leader_term", 0))

        if leader_term < current_term:
            return build_append_response(
                raft_state["node_id"], current_term, False
            )

        # Höherer Leader-Term → akzeptieren, zu Follower werden
        if leader_term > current_term:
            raft_state["current_term"] = leader_term
            raft_state["voted_for"] = None
            current_term = leader_term

        raft_state["role"] = RAFT_ROLE_FOLLOWER
        raft_state["known_leader_id"] = request.get("leader_id")
        raft_state["last_leader_contact_ms"] = int(time.monotonic() * 1000)

        # Log-Konsistenz prüfen
        prev_log_index = int(request.get("prev_log_index", 0))
        prev_log_term = int(request.get("prev_log_term", 0))
        local_last_index = int(raft_state.get("last_log_index", 0))
        local_last_term = int(raft_state.get("last_log_term", 0))

        if prev_log_index > 0:
            if prev_log_index > local_last_index:
                return build_append_response(
                    raft_state["node_id"], current_term, False,
                    match_index=local_last_index,
                )
            # vereinfachter Check: nur prev_log_index==local_last_index
            # mit prev_log_term==local_last_term
            if prev_log_index == local_last_index and prev_log_term != local_last_term:
                return build_append_response(
                    raft_state["node_id"], current_term, False,
                    match_index=0,
                )

        # Entries anhängen (vereinfacht: wir replacen ab prev_log_index+1)
        entries = request.get("entries") or []
        if entries:
            next_index = prev_log_index + 1
            for i, entry in enumerate(entries):
                idx = next_index + i
                raft_state["log"][idx] = {
                    "term": int(entry.get("term", current_term)),
                    "payload": entry.get("payload"),
                }
            raft_state["last_log_index"] = next_index + len(entries) - 1
            raft_state["last_log_term"] = int(
                entries[-1].get("term", current_term),
            )

        # Commit-Index aktualisieren
        leader_commit = int(request.get("leader_commit", 0))
        if leader_commit > int(raft_state.get("commit_index", 0)):
            raft_state["commit_index"] = min(
                leader_commit, int(raft_state.get("last_log_index", 0)),
            )

        return build_append_response(
            raft_state["node_id"], current_term, True,
            match_index=int(raft_state.get("last_log_index", 0)),
        )


# ---------------------------------------------------------------------------
# Raft-State
# ---------------------------------------------------------------------------

def build_raft_state(node_id, peer_ids=None):
    """Baut einen frischen Raft-State. cluster_size = 1 + len(peer_ids)."""
    if peer_ids is None:
        peer_ids = []
    return {
        "lock": threading.Lock(),
        "node_id": str(node_id),
        "peer_ids": list(peer_ids),
        "role": RAFT_ROLE_FOLLOWER,
        "current_term": 0,
        "voted_for": None,
        "log": {},  # index -> {"term", "payload"}
        "last_log_index": 0,
        "last_log_term": 0,
        "commit_index": 0,
        "known_leader_id": None,
        "last_leader_contact_ms": 0,
        "votes_received": set(),
    }


def get_raft_role(raft_state):
    if not isinstance(raft_state, dict):
        return RAFT_ROLE_FOLLOWER
    lock = raft_state.get("lock")
    if lock is None:
        return raft_state.get("role", RAFT_ROLE_FOLLOWER)
    with lock:
        return raft_state.get("role", RAFT_ROLE_FOLLOWER)


def start_election(raft_state):
    """Lokalen Node in Candidate-Rolle versetzen und eigenen term erhöhen.
    Rückgabe: RequestVote-RPC, der an alle Peers gesendet werden muss."""
    if not isinstance(raft_state, dict):
        return None
    lock = raft_state["lock"]
    with lock:
        raft_state["role"] = RAFT_ROLE_CANDIDATE
        raft_state["current_term"] = int(raft_state.get("current_term", 0)) + 1
        raft_state["voted_for"] = raft_state["node_id"]
        raft_state["votes_received"] = {raft_state["node_id"]}
        return build_request_vote(
            candidate_id=raft_state["node_id"],
            candidate_term=raft_state["current_term"],
            last_log_index=int(raft_state.get("last_log_index", 0)),
            last_log_term=int(raft_state.get("last_log_term", 0)),
        )


def record_vote(raft_state, vote_response):
    """Trägt einen empfangenen Vote ein. Gibt True zurück, wenn Quorum
    erreicht und Candidate zum Leader promoviert wurde."""
    if not isinstance(raft_state, dict) or not isinstance(vote_response, dict):
        return False

    lock = raft_state["lock"]
    with lock:
        if raft_state["role"] != RAFT_ROLE_CANDIDATE:
            return False

        resp_term = int(vote_response.get("current_term", 0))
        if resp_term > int(raft_state["current_term"]):
            raft_state["current_term"] = resp_term
            raft_state["role"] = RAFT_ROLE_FOLLOWER
            raft_state["voted_for"] = None
            raft_state["votes_received"] = set()
            return False

        if not vote_response.get("vote_granted"):
            return False

        voter_id = vote_response.get("voter_id")
        if voter_id:
            raft_state["votes_received"].add(voter_id)

        cluster_size = 1 + len(raft_state.get("peer_ids", []))
        if has_quorum(len(raft_state["votes_received"]), cluster_size):
            raft_state["role"] = RAFT_ROLE_LEADER
            raft_state["known_leader_id"] = raft_state["node_id"]
            return True
        return False


def append_local_entry(raft_state, payload):
    """Leader-seitig: fügt lokal einen neuen Log-Eintrag hinzu.
    Die Replikation zu Followern erfolgt dann per AppendEntries."""
    if not isinstance(raft_state, dict):
        return None
    lock = raft_state["lock"]
    with lock:
        if raft_state["role"] != RAFT_ROLE_LEADER:
            return None
        new_index = int(raft_state.get("last_log_index", 0)) + 1
        raft_state["log"][new_index] = {
            "term": int(raft_state["current_term"]),
            "payload": payload,
        }
        raft_state["last_log_index"] = new_index
        raft_state["last_log_term"] = int(raft_state["current_term"])
        return new_index


def describe_raft_state(raft_state):
    if not isinstance(raft_state, dict):
        return {}
    lock = raft_state.get("lock")
    if lock is None:
        return {}
    with lock:
        return {
            "node_id": raft_state.get("node_id"),
            "role": raft_state.get("role"),
            "current_term": raft_state.get("current_term"),
            "voted_for": raft_state.get("voted_for"),
            "last_log_index": raft_state.get("last_log_index"),
            "last_log_term": raft_state.get("last_log_term"),
            "commit_index": raft_state.get("commit_index"),
            "known_leader_id": raft_state.get("known_leader_id"),
            "peer_count": len(raft_state.get("peer_ids", [])),
            "votes_received_count": len(raft_state.get("votes_received", set())),
        }
