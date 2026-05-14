# -*- coding: utf-8 -*-
# tests/test_v24_federation_bridge.py
#
# v24: Tests für die Regional-Federation Event-Bridge.

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.libraries import _federation_bridge as fb
from src.libraries import _type_mapping_registry as tmr


# =============================================================================
# 1. Envelope-Stempel
# =============================================================================

def test_stamp_envelope_basic():
    evt = {"event_type": "X", "envelope_id": "e1"}
    fb.stamp_federation_envelope(evt, origin_site_id="site_a", fed_scope=fb.FED_SCOPE_FEDERATED)
    assert evt["fed_origin_site"] == "site_a"
    assert evt["fed_scope"] == fb.FED_SCOPE_FEDERATED
    assert evt["fed_visited_sites"] == ["site_a"]
    assert "fed_stamped_at_ms" in evt


def test_stamp_is_idempotent():
    evt = {
        "event_type": "X",
        "fed_origin_site": "site_x",
        "fed_scope": fb.FED_SCOPE_FEDERATED,
        "fed_visited_sites": ["site_x", "site_y"],
    }
    fb.stamp_federation_envelope(evt, origin_site_id="site_a", fed_scope=fb.FED_SCOPE_BROADCAST)
    # Schon gesetzt → unverändert
    assert evt["fed_origin_site"] == "site_x"
    assert evt["fed_visited_sites"] == ["site_x", "site_y"]


def test_validate_envelope_missing_origin():
    evt = {"event_type": "X"}
    reasons = fb.validate_federation_envelope(evt)
    assert any("fed_origin_site" in r for r in reasons)


def test_validate_envelope_bad_scope():
    evt = {"fed_origin_site": "site_a", "fed_scope": "bogus"}
    reasons = fb.validate_federation_envelope(evt)
    assert any("fed_scope" in r for r in reasons)


# =============================================================================
# 2. Site-Vector-Clock
# =============================================================================

def test_empty_svc():
    assert fb.empty_site_vector_clock() == {}


def test_advance_svc_does_not_mutate():
    svc = {"site_a": 1}
    new = fb.advance_site_vector_clock(svc, "site_a")
    assert new == {"site_a": 2}
    assert svc == {"site_a": 1}


def test_merge_svc_component_max():
    a = {"site_a": 3, "site_b": 1}
    b = {"site_a": 2, "site_c": 5}
    merged = fb.merge_site_vector_clocks(a, b)
    assert merged == {"site_a": 3, "site_b": 1, "site_c": 5}


# =============================================================================
# 3. Outgoing-Event-Vorbereitung
# =============================================================================

def test_outgoing_site_local_no_targets():
    bs = fb.build_bridge_state("site_a", known_peer_sites=["site_b", "site_c"])
    evt = {"event_type": "X", "envelope_id": "e1"}
    stamped, targets = fb.prepare_outgoing_event(bs, evt, fed_scope=fb.FED_SCOPE_SITE_LOCAL)
    assert targets == []
    assert stamped["fed_scope"] == fb.FED_SCOPE_SITE_LOCAL


def test_outgoing_federated_targets_peers():
    bs = fb.build_bridge_state("site_a", known_peer_sites=["site_b", "site_c"])
    evt = {"event_type": "X", "envelope_id": "e1"}
    stamped, targets = fb.prepare_outgoing_event(bs, evt, fed_scope=fb.FED_SCOPE_FEDERATED)
    assert set(targets) == {"site_b", "site_c"}
    assert stamped["fed_origin_site"] == "site_a"
    assert "fed_site_vector_clock" in stamped
    assert stamped["fed_site_vector_clock"]["site_a"] == 1


def test_outgoing_advances_site_vc_each_call():
    bs = fb.build_bridge_state("site_a", known_peer_sites=["site_b"])
    e1 = {"envelope_id": "e1"}
    e2 = {"envelope_id": "e2"}
    s1, _ = fb.prepare_outgoing_event(bs, e1, fed_scope=fb.FED_SCOPE_FEDERATED)
    s2, _ = fb.prepare_outgoing_event(bs, e2, fed_scope=fb.FED_SCOPE_FEDERATED)
    assert s1["fed_site_vector_clock"]["site_a"] == 1
    assert s2["fed_site_vector_clock"]["site_a"] == 2


# =============================================================================
# 4. Incoming: Akzeptieren, De-Duplication, Loop-Prevention
# =============================================================================

def test_incoming_site_local_rejected():
    bs = fb.build_bridge_state("site_b", known_peer_sites=["site_a"])
    evt = {
        "envelope_id": "e1",
        "fed_origin_site": "site_a",
        "fed_scope": fb.FED_SCOPE_SITE_LOCAL,
        "fed_visited_sites": ["site_a"],
    }
    verdict, _ = fb.accept_incoming_event(bs, evt)
    assert verdict == "site_local_rejected"


def test_incoming_loop_prevented_when_visited():
    bs = fb.build_bridge_state("site_b", known_peer_sites=["site_a", "site_c"])
    evt = {
        "envelope_id": "e1",
        "fed_origin_site": "site_a",
        "fed_scope": fb.FED_SCOPE_FEDERATED,
        "fed_visited_sites": ["site_a", "site_b"],  # site_b schon drin
    }
    verdict, reason = fb.accept_incoming_event(bs, evt)
    assert verdict == "loop_prevented"


def test_incoming_accepted_appends_local_site():
    bs = fb.build_bridge_state("site_b", known_peer_sites=["site_a", "site_c"])
    evt = {
        "envelope_id": "e1",
        "fed_origin_site": "site_a",
        "fed_scope": fb.FED_SCOPE_FEDERATED,
        "fed_visited_sites": ["site_a"],
        "fed_site_vector_clock": {"site_a": 5},
    }
    verdict, stamped = fb.accept_incoming_event(bs, evt)
    assert verdict == "accept"
    assert "site_b" in stamped["fed_visited_sites"]
    # Site-VC wurde gemergt
    assert stamped["fed_site_vector_clock"]["site_a"] == 5


def test_incoming_duplicate_rejected():
    bs = fb.build_bridge_state("site_b", known_peer_sites=["site_a"])
    evt1 = {
        "envelope_id": "e1",
        "fed_origin_site": "site_a",
        "fed_scope": fb.FED_SCOPE_FEDERATED,
        "fed_visited_sites": ["site_a"],
    }
    v1, _ = fb.accept_incoming_event(bs, evt1)
    assert v1 == "accept"

    # Zweites Mal selber Envelope → duplicate
    evt1_copy = {
        "envelope_id": "e1",
        "fed_origin_site": "site_a",
        "fed_scope": fb.FED_SCOPE_FEDERATED,
        "fed_visited_sites": ["site_a"],
    }
    v2, _ = fb.accept_incoming_event(bs, evt1_copy)
    assert v2 == "duplicate"


def test_incoming_invalid_missing_origin():
    bs = fb.build_bridge_state("site_b")
    evt = {"envelope_id": "e1"}
    verdict, reasons = fb.accept_incoming_event(bs, evt)
    assert verdict == "invalid"
    assert any("fed_origin_site" in r for r in reasons)


# =============================================================================
# 5. Broadcast-Forwarding
# =============================================================================

def test_forward_to_peers_broadcast_excludes_visited_and_self():
    bs = fb.build_bridge_state("site_b", known_peer_sites=["site_a", "site_c", "site_d"])
    evt = {
        "fed_scope": fb.FED_SCOPE_BROADCAST,
        "fed_visited_sites": ["site_a", "site_b"],
    }
    targets = fb.forward_to_peers(bs, evt)
    assert set(targets) == {"site_c", "site_d"}


def test_forward_federated_not_broadcast_returns_empty():
    bs = fb.build_bridge_state("site_b", known_peer_sites=["site_a", "site_c"])
    evt = {"fed_scope": fb.FED_SCOPE_FEDERATED, "fed_visited_sites": ["site_a"]}
    assert fb.forward_to_peers(bs, evt) == []


# =============================================================================
# 6. Describe + Stats
# =============================================================================

def test_describe_bridge_state():
    bs = fb.build_bridge_state("site_x", known_peer_sites=["site_y"])
    desc = fb.describe_bridge_state(bs)
    assert desc["local_site_id"] == "site_x"
    assert desc["known_peer_sites"] == ["site_y"]
    assert desc["dedup_window_used"] == 0


def test_stats_track_incoming_events():
    bs = fb.build_bridge_state("site_b", known_peer_sites=["site_a"])
    good = {
        "envelope_id": "eg",
        "fed_origin_site": "site_a",
        "fed_scope": fb.FED_SCOPE_FEDERATED,
        "fed_visited_sites": ["site_a"],
    }
    fb.accept_incoming_event(bs, good)
    dup = {
        "envelope_id": "eg",
        "fed_origin_site": "site_a",
        "fed_scope": fb.FED_SCOPE_FEDERATED,
        "fed_visited_sites": ["site_a"],
    }
    fb.accept_incoming_event(bs, dup)
    loop = {
        "envelope_id": "ex",
        "fed_origin_site": "site_a",
        "fed_scope": fb.FED_SCOPE_FEDERATED,
        "fed_visited_sites": ["site_a", "site_b"],
    }
    fb.accept_incoming_event(bs, loop)

    desc = fb.describe_bridge_state(bs)
    stats = desc["stats"]
    assert stats["incoming_accepted"] == 1
    assert stats["incoming_duplicates"] == 1
    assert stats["incoming_loop_prevented"] == 1


# =============================================================================
# 7. Dedup-Window-Trimming
# =============================================================================

def test_dedup_window_trims_old_entries():
    bs = fb.build_bridge_state(
        "site_b", known_peer_sites=["site_a"], dedup_window_size=3,
    )
    for i in range(5):
        evt = {
            "envelope_id": "e%d" % i,
            "fed_origin_site": "site_a",
            "fed_scope": fb.FED_SCOPE_FEDERATED,
            "fed_visited_sites": ["site_a"],
        }
        fb.accept_incoming_event(bs, evt)

    desc = fb.describe_bridge_state(bs)
    assert desc["dedup_window_used"] == 3  # Window ist auf 3 getrimmt
    # Die ÄLTESTEN sollten gedroppt sein: e0, e1 — ein erneutes e0 würde
    # fälschlicherweise wieder akzeptiert. Das ist eine bewusste Design-
    # Entscheidung: sehr alte Duplicates werden nicht erkannt, wenn das
    # Fenster überfließt.


# =============================================================================
# 8. Integration End-to-End: Site A → Site B
# =============================================================================

def test_end_to_end_site_a_to_site_b():
    """Simuliert: site_a erzeugt ein federiertes Event, site_b akzeptiert es."""
    site_a = fb.build_bridge_state("site_a", known_peer_sites=["site_b"])
    site_b = fb.build_bridge_state("site_b", known_peer_sites=["site_a"])

    # site_a bereitet Event vor
    evt = {"envelope_id": "e_sync_1", "payload": "hello_b"}
    stamped, targets = fb.prepare_outgoing_event(
        site_a, evt, fed_scope=fb.FED_SCOPE_FEDERATED,
    )
    assert targets == ["site_b"]

    # site_b empfängt es (Transport wird simuliert)
    verdict, accepted = fb.accept_incoming_event(site_b, stamped)
    assert verdict == "accept"

    # site_b's VC kennt jetzt site_a
    assert accepted["fed_site_vector_clock"]["site_a"] == 1
    assert "site_b" in accepted["fed_visited_sites"]


# =============================================================================
# 9. Registry-Integration
# =============================================================================

def test_federated_event_registered():
    assert tmr.lookup_primary_writer("federated_event") == tmr.WRITER_WORKER_LOCAL
    assert tmr.lookup_consistency_zone("federated_event") == tmr.CONSISTENCY_ZONE_EVENTUAL


def test_registry_still_validates():
    assert tmr.validate_registry() == []
