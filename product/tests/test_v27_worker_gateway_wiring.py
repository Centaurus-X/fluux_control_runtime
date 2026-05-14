# -*- coding: utf-8 -*-
# tests/test_v27_worker_gateway_wiring.py
#
# v27: Sichert die Runtime-Verdrahtung des worker_gateway in der
# Thread-Orchestrierung ab. In v26 war die Gateway-Komponente zwar
# vorhanden, wurde aber nicht aus thread_specs heraus instanziiert.

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.orchestration import thread_specs


def _make_runtime_state():
    return {
        "node_id": "fn-01",
        "resources": {"worker_state": {}, "connected": {}, "config": {}, "event_store": {}},
        "config_data": {},
        "config_data_lock": object(),
    }


def _make_queue_tools():
    # Robust: jede angefragte Queue wird lazy erzeugt, sodass
    # build_thread_specs nicht an fehlenden Queue-Namen scheitert.
    queues = {}

    def _get(name):
        if name not in queues:
            queues[name] = object()
        return queues[name]

    def pick(*names):
        return {n: _get(n) for n in names}

    def alias(mapping):
        return {k: _get(v) for k, v in mapping.items()}

    return {
        "queues_dict": queues,
        "pick_q": pick,
        "alias_q": alias,
    }


def _make_runtime_bundle(gateway_enabled):
    return {
        "settings": {
            "mqtt_client": {"enabled": False},
            "datastore": {"bypass_device_state_load": True},
            "network": {"local_mode": True},
            "worker_gateway": {
                "enabled": gateway_enabled,
                "loop_timeout_s": 0.25,
                "default_qos": 1,
            },
            "timing": {"start_delays_s": {}},
        }
    }


# ---------------------------------------------------------------------------
# 1. Component-Resolver kennt worker_gateway
# ---------------------------------------------------------------------------

def test_resolve_component_targets_includes_worker_gateway():
    targets = thread_specs.resolve_component_targets()
    assert "run_worker_gateway" in targets
    assert callable(targets["run_worker_gateway"])
    info = targets["resolved_sources"]["run_worker_gateway"]
    assert info["module"] == "src.core.worker_gateway"
    assert info["attribute"] == "run_worker_gateway_thread"


# ---------------------------------------------------------------------------
# 2. Gateway disabled -> kein Spec
# ---------------------------------------------------------------------------

def test_build_thread_specs_no_gateway_when_disabled():
    rs = _make_runtime_state()
    qt = _make_queue_tools()
    rb = _make_runtime_bundle(gateway_enabled=False)
    specs = thread_specs.build_thread_specs(rs, qt, rb)
    names = [s["name"] for s in specs]
    assert "Worker_Gateway" not in names


# ---------------------------------------------------------------------------
# 3. Gateway enabled -> Spec mit korrekter Komponente
# ---------------------------------------------------------------------------

def test_build_thread_specs_adds_gateway_when_enabled():
    rs = _make_runtime_state()
    qt = _make_queue_tools()
    rb = _make_runtime_bundle(gateway_enabled=True)
    specs = thread_specs.build_thread_specs(rs, qt, rb)
    gateway_specs = [s for s in specs if s["name"] == "Worker_Gateway"]

    assert len(gateway_specs) == 1, "Worker_Gateway spec missing despite enabled=true"
    spec = gateway_specs[0]
    assert spec["component_name"] == "worker_gateway"
    assert spec["enabled"] is True
    assert callable(spec["target"])
    # Entry-Point muss run_worker_gateway_thread aus src.core.worker_gateway sein
    assert spec["target"].__name__ == "run_worker_gateway_thread"


# ---------------------------------------------------------------------------
# 4. Gateway-Spec enthaelt die erwarteten Queue-Aliase
# ---------------------------------------------------------------------------

def test_gateway_spec_has_expected_queues():
    rs = _make_runtime_state()
    qt = _make_queue_tools()
    rb = _make_runtime_bundle(gateway_enabled=True)
    specs = thread_specs.build_thread_specs(rs, qt, rb)
    spec = next(s for s in specs if s["name"] == "Worker_Gateway")
    kwargs = spec["kwargs"]

    # Die Aliase mappen gateway_in -> queue_event_ti und gateway_out -> queue_event_send
    assert "queue_gateway_in" in kwargs
    assert "queue_gateway_out" in kwargs
    assert "queue_event_ti" in kwargs
    assert "queue_event_send" in kwargs
    assert kwargs["node_id"] == "fn-01"
    assert kwargs["resources"] is not None


# ---------------------------------------------------------------------------
# 5. Ownership ist gesetzt (wichtig fuer Audit)
# ---------------------------------------------------------------------------

def test_gateway_spec_declares_ownership():
    rs = _make_runtime_state()
    qt = _make_queue_tools()
    rb = _make_runtime_bundle(gateway_enabled=True)
    specs = thread_specs.build_thread_specs(rs, qt, rb)
    spec = next(s for s in specs if s["name"] == "Worker_Gateway")
    own = spec["ownership"]

    assert "queue_event_ti" in own["ingress_queues"]
    assert "queue_event_send" in own["egress_queues"]
    assert "event_store" in own["write_resources"]
