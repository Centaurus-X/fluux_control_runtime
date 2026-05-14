# -*- coding: utf-8 -*-
# tests/test_v22_hot_standby_node_role.py
#
# v22: Tests für die Hot-Standby Node-Role Library.

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.libraries import _node_role as nr
from src.libraries import _type_mapping_registry as tmr


# =============================================================================
# 1. Heartbeat-Struktur
# =============================================================================

def test_build_heartbeat_basic():
    hb = nr.build_heartbeat("node_a", nr.ROLE_LEADER, {"node_a": 5}, term=1)
    assert hb["node_id"] == "node_a"
    assert hb["role"] == nr.ROLE_LEADER
    assert hb["vector_clock"] == {"node_a": 5}
    assert hb["term"] == 1
    assert "sent_at_ms" in hb


def test_build_heartbeat_invalid_role_becomes_unknown():
    hb = nr.build_heartbeat("node_a", "nonsense_role", {}, term=0)
    assert hb["role"] == nr.ROLE_UNKNOWN


def test_validate_heartbeat_happy():
    hb = nr.build_heartbeat("node_a", nr.ROLE_LEADER, {}, term=1)
    assert nr.validate_heartbeat(hb) == []


def test_validate_heartbeat_missing_fields():
    reasons = nr.validate_heartbeat({"node_id": "x"})
    assert any("missing" in r for r in reasons)


def test_validate_heartbeat_invalid_role():
    hb = {"node_id": "x", "role": "weird", "vector_clock": {}, "term": 1, "sent_at_ms": 0}
    reasons = nr.validate_heartbeat(hb)
    assert any("role" in r for r in reasons)


# =============================================================================
# 2. Node-State
# =============================================================================

def test_build_state_single_node_defaults_to_leader():
    state = nr.build_node_state("node_1", mode=nr.MODE_SINGLE_NODE)
    assert nr.get_role(state) == nr.ROLE_LEADER
    assert nr.is_leader(state) is True
    assert nr.is_standby(state) is False


def test_build_state_hot_standby_defaults_to_standby():
    state = nr.build_node_state("node_1", mode=nr.MODE_HOT_STANDBY)
    assert nr.get_role(state) == nr.ROLE_STANDBY


def test_describe_state():
    state = nr.build_node_state("node_x", mode=nr.MODE_HOT_STANDBY)
    desc = nr.describe_state(state)
    assert desc["node_id"] == "node_x"
    assert desc["mode"] == nr.MODE_HOT_STANDBY
    assert desc["peer_count"] == 0


# =============================================================================
# 3. receive_heartbeat
# =============================================================================

def test_receive_heartbeat_ignored_in_single_node_mode():
    state = nr.build_node_state("node_1", mode=nr.MODE_SINGLE_NODE)
    hb = nr.build_heartbeat("node_2", nr.ROLE_LEADER, {}, term=5)
    nr.receive_heartbeat(state, hb, now_ms=1000)
    assert nr.get_role(state) == nr.ROLE_LEADER  # unverändert
    desc = nr.describe_state(state)
    assert desc["peer_count"] == 0


def test_receive_own_heartbeat_ignored():
    state = nr.build_node_state("node_1", mode=nr.MODE_HOT_STANDBY)
    hb = nr.build_heartbeat("node_1", nr.ROLE_LEADER, {}, term=5)
    nr.receive_heartbeat(state, hb, now_ms=1000)
    desc = nr.describe_state(state)
    assert desc["peer_count"] == 0


def test_receive_leader_heartbeat_with_higher_term_demotes_to_standby():
    state = nr.build_node_state(
        "node_1",
        mode=nr.MODE_HOT_STANDBY,
        initial_role=nr.ROLE_LEADER,
    )
    hb = nr.build_heartbeat("node_2", nr.ROLE_LEADER, {}, term=10)
    nr.receive_heartbeat(state, hb, now_ms=1000)
    assert nr.get_role(state) == nr.ROLE_STANDBY
    desc = nr.describe_state(state)
    assert desc["known_leader_id"] == "node_2"
    assert desc["term"] == 10


def test_receive_standby_heartbeat_registers_peer():
    state = nr.build_node_state("node_1", mode=nr.MODE_HOT_STANDBY)
    hb = nr.build_heartbeat("node_2", nr.ROLE_STANDBY, {"node_2": 3}, term=1)
    nr.receive_heartbeat(state, hb, now_ms=1000)
    desc = nr.describe_state(state)
    assert desc["peer_count"] == 1


# =============================================================================
# 4. Leader-Timeout
# =============================================================================

def test_leader_timeout_disabled_in_single_node_mode():
    state = nr.build_node_state("node_1", mode=nr.MODE_SINGLE_NODE)
    assert nr.check_leader_timeout(state, now_ms=999999) is False


def test_leader_timeout_false_for_leader_role():
    state = nr.build_node_state(
        "node_1", mode=nr.MODE_HOT_STANDBY, initial_role=nr.ROLE_LEADER,
    )
    assert nr.check_leader_timeout(state, now_ms=999999) is False


def test_leader_timeout_fires_after_interval():
    state = nr.build_node_state(
        "node_1",
        mode=nr.MODE_HOT_STANDBY,
        heartbeat_timeout_ms=500,
    )
    hb = nr.build_heartbeat("leader_x", nr.ROLE_LEADER, {}, term=3)
    nr.receive_heartbeat(state, hb, now_ms=1000)

    # innerhalb des Timeouts → kein Timeout
    assert nr.check_leader_timeout(state, now_ms=1400) is False
    # nach Ablauf des Timeouts → Timeout erkannt
    assert nr.check_leader_timeout(state, now_ms=1600) is True


# =============================================================================
# 5. Failover-Entscheidung
# =============================================================================

def test_should_become_leader_false_in_single_node():
    state = nr.build_node_state("node_1", mode=nr.MODE_SINGLE_NODE)
    assert nr.should_become_leader(state) is False


def test_should_become_leader_false_without_timeout():
    state = nr.build_node_state(
        "node_1", mode=nr.MODE_HOT_STANDBY, heartbeat_timeout_ms=500,
    )
    hb = nr.build_heartbeat("leader_x", nr.ROLE_LEADER, {}, term=1)
    nr.receive_heartbeat(state, hb, now_ms=1000)
    assert nr.should_become_leader(state, now_ms=1200) is False


def test_should_become_leader_alone_wins_after_timeout():
    state = nr.build_node_state(
        "node_1",
        mode=nr.MODE_HOT_STANDBY,
        heartbeat_timeout_ms=500,
    )
    nr.update_vector_clock(state, {"node_1": 10})
    hb = nr.build_heartbeat("leader_x", nr.ROLE_LEADER, {}, term=1)
    nr.receive_heartbeat(state, hb, now_ms=1000)
    # Leader timed out und keine anderen Standbys → node_1 gewinnt
    assert nr.should_become_leader(state, now_ms=2000) is True


def test_should_become_leader_looses_against_higher_vector_clock():
    """Node mit niedrigerem VC-Rank verliert die Failover-Wahl."""
    state = nr.build_node_state(
        "node_1",
        mode=nr.MODE_HOT_STANDBY,
        heartbeat_timeout_ms=500,
    )
    nr.update_vector_clock(state, {"node_1": 5})

    # Leader-Heartbeat empfangen, dann Timeout
    leader_hb = nr.build_heartbeat("leader_x", nr.ROLE_LEADER, {}, term=1)
    nr.receive_heartbeat(state, leader_hb, now_ms=1000)

    # Peer mit HÖHEREM Vector-Clock-Rank — Heartbeat frisch bei t=1700
    peer_hb = nr.build_heartbeat(
        "node_2", nr.ROLE_STANDBY, {"node_2": 100}, term=1,
    )
    nr.receive_heartbeat(state, peer_hb, now_ms=1700)

    # t=2000 ist >500ms nach Leader-Heartbeat (timeout) und <500ms nach Peer-HB
    assert nr.should_become_leader(state, now_ms=2000) is False


def test_should_become_leader_wins_against_lower_vector_clock():
    """Node mit höherem VC-Rank gewinnt die Failover-Wahl."""
    state = nr.build_node_state(
        "node_1",
        mode=nr.MODE_HOT_STANDBY,
        heartbeat_timeout_ms=500,
    )
    nr.update_vector_clock(state, {"node_1": 100})

    leader_hb = nr.build_heartbeat("leader_x", nr.ROLE_LEADER, {}, term=1)
    nr.receive_heartbeat(state, leader_hb, now_ms=1000)

    # Peer mit NIEDRIGEREM VC-Rank — Heartbeat frisch
    peer_hb = nr.build_heartbeat(
        "node_2", nr.ROLE_STANDBY, {"node_2": 5}, term=1,
    )
    nr.receive_heartbeat(state, peer_hb, now_ms=1700)

    assert nr.should_become_leader(state, now_ms=2000) is True


def test_stale_peer_info_is_ignored_in_failover():
    """Peers mit veralteten Heartbeats zählen nicht für die Wahl."""
    state = nr.build_node_state(
        "node_1",
        mode=nr.MODE_HOT_STANDBY,
        heartbeat_timeout_ms=500,
    )
    nr.update_vector_clock(state, {"node_1": 5})

    leader_hb = nr.build_heartbeat("leader_x", nr.ROLE_LEADER, {}, term=1)
    nr.receive_heartbeat(state, leader_hb, now_ms=1000)

    # Peer hat hohen VC-Rank, aber Heartbeat ist alt (vor Leader-Empfang)
    peer_hb = nr.build_heartbeat(
        "node_2", nr.ROLE_STANDBY, {"node_2": 100}, term=1,
    )
    nr.receive_heartbeat(state, peer_hb, now_ms=500)  # alt!

    # zum Zeitpunkt 2000 ist der Peer-Heartbeat > timeout → wird ignoriert
    # node_1 sollte also doch gewinnen trotz niedrigerer VC
    assert nr.should_become_leader(state, now_ms=2000) is True


# =============================================================================
# 6. Promote / Demote
# =============================================================================

def test_promote_to_leader_increments_term():
    state = nr.build_node_state("node_1", mode=nr.MODE_HOT_STANDBY)
    desc_before = nr.describe_state(state)
    assert nr.promote_to_leader(state, now_ms=1000) is True
    desc_after = nr.describe_state(state)
    assert desc_after["role"] == nr.ROLE_LEADER
    assert desc_after["term"] == desc_before["term"] + 1
    assert desc_after["known_leader_id"] == "node_1"


def test_promote_in_single_node_is_idempotent():
    state = nr.build_node_state("node_1", mode=nr.MODE_SINGLE_NODE)
    before_term = nr.describe_state(state)["term"]
    assert nr.promote_to_leader(state, now_ms=1000) is True
    # im single_node bleibt term unverändert
    assert nr.describe_state(state)["term"] == before_term


def test_demote_to_standby_updates_term_if_higher():
    state = nr.build_node_state(
        "node_1", mode=nr.MODE_HOT_STANDBY, initial_role=nr.ROLE_LEADER,
    )
    nr.demote_to_standby(state, known_leader_id="node_2", new_term=5)
    desc = nr.describe_state(state)
    assert desc["role"] == nr.ROLE_STANDBY
    assert desc["known_leader_id"] == "node_2"
    assert desc["term"] == 5


def test_demote_does_not_lower_term():
    state = nr.build_node_state(
        "node_1", mode=nr.MODE_HOT_STANDBY, initial_role=nr.ROLE_LEADER,
    )
    nr.promote_to_leader(state, now_ms=1000)  # term→1
    nr.promote_to_leader(state, now_ms=2000)  # term→2
    nr.demote_to_standby(state, new_term=1)
    assert nr.describe_state(state)["term"] == 2  # bleibt höher


# =============================================================================
# 7. build_own_heartbeat
# =============================================================================

def test_build_own_heartbeat_reflects_state():
    state = nr.build_node_state("node_x", mode=nr.MODE_HOT_STANDBY)
    nr.update_vector_clock(state, {"node_x": 7})
    nr.promote_to_leader(state, now_ms=1000)

    hb = nr.build_own_heartbeat(state, now_ms=2000)
    assert hb is not None
    assert hb["node_id"] == "node_x"
    assert hb["role"] == nr.ROLE_LEADER
    assert hb["vector_clock"] == {"node_x": 7}
    assert hb["term"] == 1


# =============================================================================
# 8. Registry-Integration
# =============================================================================

def test_ha_heartbeat_registered():
    assert tmr.lookup_primary_writer("ha_heartbeat") == tmr.WRITER_WORKER_LOCAL
    assert tmr.lookup_consistency_zone("ha_heartbeat") == tmr.CONSISTENCY_ZONE_CAUSAL


def test_registry_still_validates_with_ha_heartbeat():
    reasons = tmr.validate_registry()
    assert reasons == []
