# -*- coding: utf-8 -*-
# tests/test_v23_raft_election.py
#
# v23: Tests für die Raft-Election-Library.

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.libraries import _raft_election as raft
from src.libraries import _type_mapping_registry as tmr


# =============================================================================
# 1. Quorum
# =============================================================================

def test_quorum_size_single_node():
    assert raft.compute_quorum_size(1) == 1


def test_quorum_size_three_nodes():
    assert raft.compute_quorum_size(3) == 2


def test_quorum_size_five_nodes():
    assert raft.compute_quorum_size(5) == 3


def test_has_quorum_majority():
    assert raft.has_quorum(3, 5) is True
    assert raft.has_quorum(2, 5) is False
    assert raft.has_quorum(2, 3) is True


# =============================================================================
# 2. RequestVote
# =============================================================================

def test_request_vote_higher_term_granted():
    state = raft.build_raft_state("n1", peer_ids=["n2", "n3"])
    state["current_term"] = 5
    request = raft.build_request_vote("n2", 6, 0, 0)
    resp = raft.handle_request_vote(state, request)
    assert resp["vote_granted"] is True
    assert resp["current_term"] == 6
    desc = raft.describe_raft_state(state)
    assert desc["voted_for"] == "n2"


def test_request_vote_lower_term_rejected():
    state = raft.build_raft_state("n1", peer_ids=["n2", "n3"])
    state["current_term"] = 10
    request = raft.build_request_vote("n2", 5, 0, 0)
    resp = raft.handle_request_vote(state, request)
    assert resp["vote_granted"] is False
    assert resp["current_term"] == 10


def test_request_vote_twice_in_same_term_rejected():
    state = raft.build_raft_state("n1", peer_ids=["n2", "n3"])
    # Erster Vote an n2 bei term=3
    r1 = raft.build_request_vote("n2", 3, 0, 0)
    assert raft.handle_request_vote(state, r1)["vote_granted"] is True
    # Zweiter Vote an n3 im selben term → abgelehnt
    r2 = raft.build_request_vote("n3", 3, 0, 0)
    assert raft.handle_request_vote(state, r2)["vote_granted"] is False


def test_request_vote_stale_log_rejected():
    state = raft.build_raft_state("n1", peer_ids=["n2"])
    state["current_term"] = 3
    state["last_log_term"] = 3
    state["last_log_index"] = 10
    # Candidate hat älteres Log → abgelehnt
    r = raft.build_request_vote("n2", 3, last_log_index=5, last_log_term=2)
    assert raft.handle_request_vote(state, r)["vote_granted"] is False


def test_request_vote_same_log_term_higher_index_granted():
    state = raft.build_raft_state("n1", peer_ids=["n2"])
    state["last_log_term"] = 3
    state["last_log_index"] = 5
    # Candidate hat längeres Log im selben term → granted
    r = raft.build_request_vote("n2", 4, last_log_index=7, last_log_term=3)
    assert raft.handle_request_vote(state, r)["vote_granted"] is True


# =============================================================================
# 3. AppendEntries (Heartbeat + Log)
# =============================================================================

def test_append_entries_heartbeat_accepted():
    state = raft.build_raft_state("n1", peer_ids=["n2", "n3"])
    state["current_term"] = 2
    req = raft.build_append_entries("n_lead", 2, 0, 0, entries=[], leader_commit=0)
    resp = raft.handle_append_entries(state, req)
    assert resp["success"] is True
    desc = raft.describe_raft_state(state)
    assert desc["known_leader_id"] == "n_lead"


def test_append_entries_stale_leader_rejected():
    state = raft.build_raft_state("n1", peer_ids=["n2"])
    state["current_term"] = 10
    req = raft.build_append_entries("n_old", 5, 0, 0, entries=[])
    resp = raft.handle_append_entries(state, req)
    assert resp["success"] is False


def test_append_entries_appends_payload():
    state = raft.build_raft_state("n1", peer_ids=["n2"])
    state["current_term"] = 2
    entries = [{"term": 2, "payload": "first"}, {"term": 2, "payload": "second"}]
    req = raft.build_append_entries("n_lead", 2, 0, 0, entries=entries)
    resp = raft.handle_append_entries(state, req)
    assert resp["success"] is True
    desc = raft.describe_raft_state(state)
    assert desc["last_log_index"] == 2


def test_append_entries_commit_index_advances():
    state = raft.build_raft_state("n1", peer_ids=["n2"])
    state["current_term"] = 2
    entries = [{"term": 2, "payload": "x"}]
    req = raft.build_append_entries(
        "n_lead", 2, 0, 0, entries=entries, leader_commit=1,
    )
    raft.handle_append_entries(state, req)
    desc = raft.describe_raft_state(state)
    assert desc["commit_index"] == 1


# =============================================================================
# 4. Election-Flow
# =============================================================================

def test_start_election_transitions_to_candidate():
    state = raft.build_raft_state("n1", peer_ids=["n2", "n3"])
    req = raft.start_election(state)
    assert req is not None
    assert req["candidate_id"] == "n1"
    assert req["candidate_term"] == 1
    assert raft.get_raft_role(state) == raft.RAFT_ROLE_CANDIDATE


def test_election_wins_with_quorum():
    """3-Node-Cluster: Candidate braucht 2 Stimmen (inkl. sich selbst)."""
    state = raft.build_raft_state("n1", peer_ids=["n2", "n3"])
    raft.start_election(state)  # n1 hat eigene Stimme

    # Eine weitere Stimme von n2 → Quorum erreicht
    vote = raft.build_vote_response("n2", current_term=1, vote_granted=True)
    became_leader = raft.record_vote(state, vote)
    assert became_leader is True
    assert raft.get_raft_role(state) == raft.RAFT_ROLE_LEADER


def test_election_loses_without_quorum():
    """5-Node-Cluster: Candidate braucht 3 Stimmen. Nur 2 erhalten → kein Leader."""
    state = raft.build_raft_state("n1", peer_ids=["n2", "n3", "n4", "n5"])
    raft.start_election(state)  # n1 hat sich selbst

    vote_n2 = raft.build_vote_response("n2", 1, True)
    assert raft.record_vote(state, vote_n2) is False
    # Nach 2 Stimmen (n1 + n2) noch keine Mehrheit
    assert raft.get_raft_role(state) == raft.RAFT_ROLE_CANDIDATE


def test_higher_term_response_demotes_candidate():
    state = raft.build_raft_state("n1", peer_ids=["n2", "n3"])
    raft.start_election(state)  # term=1
    # Response mit höherem Term → Candidate wird Follower
    late_resp = raft.build_vote_response("n2", current_term=5, vote_granted=False)
    raft.record_vote(state, late_resp)
    assert raft.get_raft_role(state) == raft.RAFT_ROLE_FOLLOWER
    desc = raft.describe_raft_state(state)
    assert desc["current_term"] == 5


# =============================================================================
# 5. Log-Replication (Leader-Seite)
# =============================================================================

def test_leader_appends_local_entry():
    state = raft.build_raft_state("n1", peer_ids=["n2"])
    state["role"] = raft.RAFT_ROLE_LEADER
    state["current_term"] = 3
    new_idx = raft.append_local_entry(state, {"op": "put", "key": "k1"})
    assert new_idx == 1
    desc = raft.describe_raft_state(state)
    assert desc["last_log_index"] == 1


def test_follower_cannot_append_local_entry():
    state = raft.build_raft_state("n1", peer_ids=["n2"])
    # Rolle = follower (Default)
    result = raft.append_local_entry(state, {"op": "put"})
    assert result is None


# =============================================================================
# 6. AppendEntries-Response
# =============================================================================

def test_append_response_structure():
    resp = raft.build_append_response("n1", 5, True, match_index=10)
    assert resp["follower_id"] == "n1"
    assert resp["current_term"] == 5
    assert resp["success"] is True
    assert resp["match_index"] == 10


# =============================================================================
# 7. Registry-Integration
# =============================================================================

def test_raft_log_entry_registered():
    assert tmr.lookup_primary_writer("raft_log_entry") == tmr.WRITER_WORKER_LOCAL
    assert tmr.lookup_consistency_zone("raft_log_entry") == tmr.CONSISTENCY_ZONE_STRICT


def test_registry_validates_with_raft_log_entry():
    assert tmr.validate_registry() == []
