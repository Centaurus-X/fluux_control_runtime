# -*- coding: utf-8 -*-
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core import _runtime_command_binding as binding


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
        "request_id": "req-extra-001",
        "correlation_id": "req-extra-001",
        "payload": payload,
        "envelope": {"request_id": "req-extra-001", "correlation_id": "req-extra-001", "payload": payload},
    }


def test_v34_parameter_update_is_visible_in_worker_state():
    resources = _resources()
    result = binding.apply_runtime_command(resources, _event("parameter_update", {"parameter_key": "gain", "value": 2.5}), actor="test")
    assert result["status"] == "applied_to_validated_runtime_state"
    assert resources["worker_state"]["data"]["RUNTIME_PARAMETERS"]["gain"]["value"] == 2.5


def test_v34_unknown_runtime_operation_rejected():
    resources = _resources()
    result = binding.apply_runtime_command(resources, _event("raw_io_write", {"value": 1}), actor="test")
    assert result["status"] == "rejected"
    assert result["reason"] == "unsupported_operation"
    assert result["safe_direct_io_write"] is False
