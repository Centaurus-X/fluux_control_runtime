# -*- coding: utf-8 -*-
import os
import sys
from queue import Queue

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core import _runtime_command_binding as binding
from src.core import proxy_worker_bridge as bridge
from src.core import thread_management as tm
from src.orchestration import runtime_state_factory


def _resources():
    return {
        "worker_state": {"data": {}, "lock": None},
        "user_config": {"data": {}, "lock": None},
        "event_store": {"data": {}, "lock": None},
        "proxy_runtime_command_state": {"data": {"commands": {}, "control_methods": {}, "parameters": {}, "safe_runtime_commands": [], "audit": [], "last_update": None}, "lock": None},
    }


def _event(operation, payload):
    payload = dict(payload)
    payload.setdefault("operation", operation)
    return {
        "event_type": "V34_PROXY_RUNTIME_COMMAND_RECEIVED",
        "target": "thread_management",
        "worker_id": "worker_fn_01",
        "domain": "runtime",
        "operation": operation,
        "request_id": "req-001",
        "correlation_id": "req-001",
        "payload": payload,
        "envelope": {"request_id": "req-001", "correlation_id": "req-001", "payload": payload},
    }


def test_v34_set_control_method_updates_worker_state_safely():
    resources = _resources()
    result = binding.apply_runtime_command(resources, _event("set_control_method", {"value": "auto"}), actor="test")
    assert result["status"] == "applied_to_validated_runtime_state"
    assert result["safe_direct_io_write"] is False
    assert resources["worker_state"]["data"]["RUNTIME_CONTROL_METHOD"] == "auto"
    assert resources["proxy_runtime_command_state"]["data"]["control_methods"]["current"] == "auto"


def test_v34_parameter_update_updates_user_config_runtime_parameters():
    resources = _resources()
    result = binding.apply_runtime_command(resources, _event("parameter_update", {"parameter_key": "gain", "value": 2.5}), actor="test")
    assert result["status"] == "applied_to_validated_runtime_state"
    assert resources["user_config"]["data"]["runtime_parameters"]["gain"] == 2.5
    assert resources["proxy_runtime_command_state"]["data"]["parameters"]["gain"] == 2.5


def test_v34_state_read_and_snapshot_are_available():
    resources = _resources()
    resources["worker_state"]["data"]["CONTROL_METHOD"] = "auto"
    resources["worker_state"]["data"]["RUNTIME_CONTROL_METHOD"] = "auto"
    state = binding.apply_runtime_command(resources, _event("state_read", {"resource": "worker_state", "key": "CONTROL_METHOD"}), actor="test")
    assert state["status"] == "accepted"
    assert state["state"]["value"] == "auto"
    snap = binding.apply_runtime_command(resources, _event("runtime_snapshot", {"include_full": False}), actor="test")
    assert snap["status"] == "accepted"
    assert "resources" in snap["snapshot"]


def test_v34_thread_management_handler_records_runtime_result_without_duplicate_apply():
    resources = _resources()
    ctx = {"resources": resources, "stats": {}}
    event = _event("set_control_method", {"value": "manual"})
    event["runtime_result"] = {"status": "applied_to_validated_runtime_state", "operation": "set_control_method", "control_method": "manual", "safe_direct_io_write": False}
    tm.handle_proxy_runtime_command_event(ctx, event)
    assert ctx["stats"]["proxy_runtime_command_events"] == 1
    assert ctx["stats"]["proxy_runtime_command_records"] == 1
    assert resources["proxy_runtime_command_state"]["data"]["commands"]["req-001"]["operation"] == "set_control_method"


def test_v34_bridge_reply_uses_central_runtime_binding():
    resources = _resources()
    runtime_bundle = {
        "settings": {
            "proxy_worker_bridge": {
                "enabled": True,
                "worker_id": "worker_fn_01",
                "broker_host": "192.168.0.31",
                "use_mqtts": True,
                "ca_cert_file": "config/ssl/certs/mqtt/emqx-root-ca.pem",
            }
        }
    }
    ctx = bridge.build_bridge_ctx(
        node_id="worker_fn_01",
        resources=resources,
        runtime_bundle=runtime_bundle,
        command_event_queue=Queue(),
        outbound_event_queue=None,
        logger=None,
        shutdown_event=None,
    )
    envelope = {
        "envelope_version": "proxy-envelope-v1",
        "envelope_id": "command-test",
        "kind": "command",
        "domain": "runtime",
        "source": {"kind": "client", "ref": "engineering_client"},
        "target": {"kind": "worker", "ref": "worker_fn_01"},
        "request_id": "req-001",
        "correlation_id": "req-001",
        "payload": {"operation": "set_control_method", "value": "auto"},
    }
    reply = bridge.build_runtime_command_reply_payload(ctx, envelope, {"domain": "runtime", "worker_id": "worker_fn_01"})
    assert reply["bridge"] == "v34_preproduction_final_runtime"
    assert reply["result"]["status"] == "applied_to_validated_runtime_state"
    assert resources["worker_state"]["data"]["RUNTIME_CONTROL_METHOD"] == "auto"


def test_v34_runtime_state_has_proxy_runtime_command_state_metadata():
    state = runtime_state_factory.build_runtime_state({"node_id": "fn-01", "network": {"local_mode": True}, "worker_state": {}})
    data = state["resources"]["proxy_runtime_command_state"]["data"]
    assert data["binding_version"] == "v34_preproduction_final_runtime"
