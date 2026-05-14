# -*- coding: utf-8 -*-
# tests/test_v26_worker_gateway_and_edge.py
#
# v26: Smoke-Tests fuer die neu integrierten worker_gateway- und
# src/libraries/edge-Module. Stellt sicher, dass die Import-Pipeline
# und die zentralen Edge-Contract-Helfer bei ausgeliefertem Paketstand
# funktional sind.

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# 1. Importierbarkeit der edge-Library
# ---------------------------------------------------------------------------

def test_edge_contracts_importable():
    from src.libraries.edge import _contracts as ec
    assert callable(ec.make_transport_envelope)
    assert callable(ec.normalize_transport_envelope)
    assert callable(ec.validate_transport_envelope)
    assert callable(ec.normalize_application_contract)
    assert callable(ec.build_error_contract)


def test_edge_acl_importable():
    from src.libraries.edge import _acl as acl
    assert callable(acl.authorize_edge_contract)


def test_edge_codec_importable():
    from src.libraries.edge import _codec as codec
    assert codec is not None


def test_edge_sync_importable():
    from src.libraries.edge import _sync as sync
    assert callable(sync.build_config_sync_snapshot)
    assert callable(sync.build_full_sync_payload)
    assert callable(sync.build_health_snapshot)
    assert callable(sync.build_runtime_state_snapshot)
    assert callable(sync.build_worker_delta_payload)
    assert callable(sync.read_resource_snapshot)


# ---------------------------------------------------------------------------
# 2. Importierbarkeit von worker_gateway
# ---------------------------------------------------------------------------

def test_worker_gateway_importable():
    from src.core import worker_gateway as wg
    assert wg.GATEWAY_SOURCE_NAME == "worker_gateway"
    assert wg.DEFAULT_QOS == 1
    assert wg.DEFAULT_MAX_PAYLOAD_BYTES == 262144
    assert "__STOP__" in wg.STOP_MARKERS


# ---------------------------------------------------------------------------
# 3. edge._sync - Basis-Funktionalitaet
# ---------------------------------------------------------------------------

def test_build_config_sync_snapshot_returns_envelope():
    from src.libraries.edge import _sync
    resources = {
        "process_states": [{"state_id": 1}, {"state_id": 2}],
        "controllers": [{"controller_id": "C1"}],
    }
    snap = _sync.build_config_sync_snapshot(resources, node_id="fn-01")
    assert snap["kind"] == "config_sync_snapshot"
    assert snap["node_id"] == "fn-01"
    assert snap["body"]["process_states"] == [{"state_id": 1}, {"state_id": 2}]
    assert snap["body"]["controllers"] == [{"controller_id": "C1"}]
    assert isinstance(snap["generated_at_ms"], int)


def test_build_full_sync_payload_section_filter():
    from src.libraries.edge import _sync
    resources = {"a": 1, "b": 2, "c": 3}
    snap = _sync.build_full_sync_payload(resources, node_id="fn-02", sections=["a", "c"])
    assert snap["body"] == {"a": 1, "c": 3}


def test_build_health_snapshot_is_healthy():
    from src.libraries.edge import _sync
    snap = _sync.build_health_snapshot({"metrics": {"cpu": 0.1}}, node_id="fn-03")
    assert snap["body"]["healthy"] is True
    assert snap["body"]["metrics"] == {"cpu": 0.1}


def test_build_runtime_state_snapshot_shape():
    from src.libraries.edge import _sync
    snap = _sync.build_runtime_state_snapshot(
        {"runtime_state": {"x": 1}, "state_version": {"y": 2}},
        node_id="fn-04",
    )
    assert snap["body"]["runtime_state"] == {"x": 1}
    assert snap["body"]["state_version"] == {"y": 2}


def test_build_worker_delta_payload_returns_worker_state():
    from src.libraries.edge import _sync
    resources = {"workers": {"w1": {"status": "running"}}}
    snap = _sync.build_worker_delta_payload(
        resources, node_id="fn-05", worker_ref="w1", since_version=7,
    )
    assert snap["body"]["worker_ref"] == "w1"
    assert snap["body"]["since_version"] == 7
    assert snap["body"]["worker_state"] == {"status": "running"}


def test_read_resource_snapshot_dispatches_correctly():
    from src.libraries.edge import _sync
    resources = {"metrics": {"cpu": 0.3}}
    health = _sync.read_resource_snapshot("health", resources, node_id="fn-06")
    assert health["kind"] == "health_snapshot"

    config = _sync.read_resource_snapshot(
        "config", {"process_states": [{"state_id": 1}]}, node_id="fn-06",
    )
    assert config["kind"] == "config_sync_snapshot"


def test_read_resource_snapshot_unknown_resource_falls_back():
    from src.libraries.edge import _sync
    resources = {"my_section": {"hello": "world"}}
    snap = _sync.read_resource_snapshot("my_section", resources, node_id="fn-07")
    assert snap["kind"] == "section_snapshot"
    assert snap["body"]["section"] == "my_section"
    assert snap["body"]["content"] == {"hello": "world"}


# ---------------------------------------------------------------------------
# 4. edge._contracts - envelope + contract roundtrip
# ---------------------------------------------------------------------------

def test_make_and_validate_transport_envelope_roundtrip():
    from src.libraries.edge import _contracts as ec
    env = ec.make_transport_envelope(
        message_type="event",
        api_route="event/demo",
        payload={"event_type": "demo"},
        source="fn-01",
        source_node="fn-01",
    )
    assert env is not None
    norm = ec.normalize_transport_envelope(env)
    assert norm is not None
    result = ec.validate_transport_envelope(norm)
    assert isinstance(result, dict)
    assert result.get("ok") is True, result.get("errors")


def test_build_error_contract_contains_error_marker():
    from src.libraries.edge import _contracts as ec
    request_contract = ec.make_application_contract(
        kind="command",
        resource="config",
        operation="snapshot",
        body={},
    )
    err = ec.build_error_contract(
        request_contract,
        code="E_BAD_REQUEST",
        message="missing field",
    )
    assert err is not None
    assert isinstance(err, dict)
    # build_error_contract liefert ein contract-foermiges dict mit Fehlermarkern
    assert "code" in err or "error" in err or "message" in err or "body" in err
