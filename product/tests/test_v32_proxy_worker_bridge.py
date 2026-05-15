# -*- coding: utf-8 -*-
# tests/test_v32_proxy_worker_bridge.py

import os
import sys
from queue import Queue
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core import proxy_worker_bridge as bridge
from src.orchestration import runtime_config, thread_specs


def _make_command_envelope(worker_id="worker_fn_01", client_id="engineering_client"):
    return {
        "envelope_version": "proxy-envelope-v1",
        "envelope_id": "command-test-001",
        "kind": "command",
        "domain": "runtime",
        "created_at": "2026-05-10T00:00:00.000Z",
        "source": {"kind": "client", "ref": client_id},
        "target": {"kind": "worker", "ref": worker_id},
        "request_id": "req-001",
        "correlation_id": "req-001",
        "causation_id": None,
        "idempotency_key": "idem-001",
        "delivery": {"mqtt_qos": 2, "app_priority": 3, "retain": False},
        "payload": {"operation": "echo", "value": "demo"},
        "metadata": {},
    }


def _make_runtime_bundle(enabled):
    return {
        "settings": {
            "node_id": "fn-01",
            "mqtt_client": {
                "enabled": False,
                "broker_host": "192.168.0.31",
                "broker_port": 1883,
                "ssl_enabled": False,
            },
            "proxy_worker_bridge": {
                "enabled": enabled,
                "worker_id": "worker_fn_01",
                "broker_host": "192.168.0.31",
                "broker_port": 1883,
                "secure_port": 8883,
                "use_mqtts": True,
                "ca_cert_file": "config/ssl/certs/mqtt/emqx-root-ca.pem",
                "tls_ciphers": "",
                "runtime_binding_enabled": True,
                "runtime_event_target": "thread_management",
            },
            "worker_gateway": {"enabled": False},
            "datastore": {"bypass_device_state_load": True},
            "network": {"local_mode": True},
            "timing": {"start_delays_s": {}},
        }
    }


def _make_runtime_state():
    return {
        "node_id": "worker_fn_01",
        "resources": {"worker_state": {}, "connected": {}, "config": {}, "event_store": {}},
        "config_data": {},
        "config_data_lock": object(),
        "shutdown_event": None,
    }


def _make_queue_tools():
    queues = {}

    def _get(name):
        if name not in queues:
            queues[name] = object()
        return queues[name]

    def pick(*names):
        return {name: _get(name) for name in names}

    def alias(mapping):
        return {parameter_name: _get(queue_name) for parameter_name, queue_name in mapping.items()}

    return {
        "queues_dict": queues,
        "pick_q": pick,
        "alias_q": alias,
    }


def test_proxy_worker_bridge_topics_match_proxy_contract():
    assert bridge.make_proxy_command_topic("worker_fn_01", "runtime") == "worker/worker_fn_01/command/runtime"
    assert bridge.make_proxy_command_wildcard("worker_fn_01") == "worker/worker_fn_01/command/+"
    assert bridge.make_proxy_reply_topic("worker_fn_01", "engineering_client") == "worker/worker_fn_01/reply/engineering_client"
    assert bridge.make_proxy_presence_topic("worker_fn_01") == "worker/worker_fn_01/presence"
    assert bridge.make_proxy_snapshot_topic("worker_fn_01", "runtime") == "worker/worker_fn_01/snapshot/runtime"


def test_parse_proxy_worker_command_topic():
    parsed = bridge.parse_proxy_worker_topic("worker/worker_fn_01/command/runtime")
    assert parsed["ok"] is True
    assert parsed["worker_id"] == "worker_fn_01"
    assert parsed["channel"] == "command"
    assert parsed["domain"] == "runtime"


def test_build_proxy_reply_envelope_shape():
    env = bridge.build_proxy_envelope(
        kind="reply",
        domain="runtime",
        worker_id="worker_fn_01",
        target_kind="client",
        target_ref="engineering_client",
        request_id="req-001",
        correlation_id="req-001",
        causation_id="command-test-001",
        payload={"status": "accepted"},
        mqtt_qos=1,
        app_priority=2,
        retain=False,
    )
    assert env["envelope_version"] == "proxy-envelope-v1"
    assert env["kind"] == "reply"
    assert env["source"]["kind"] == "worker"
    assert env["source"]["ref"] == "worker_fn_01"
    assert env["target"]["kind"] == "client"
    assert env["target"]["ref"] == "engineering_client"
    assert env["delivery"]["mqtt_qos"] == 1


def test_handle_command_emits_event_and_reply_with_fake_client():
    published = []
    command_queue = Queue()
    ctx = bridge.build_bridge_ctx(
        node_id="worker_fn_01",
        resources={},
        runtime_bundle=_make_runtime_bundle(enabled=True),
        command_event_queue=command_queue,
        outbound_event_queue=None,
        logger=None,
        shutdown_event=None,
    )

    def publish(topic, payload, qos=0, retain=False):
        published.append({"topic": topic, "payload": payload, "qos": qos, "retain": retain})
        return SimpleNamespace(rc=0)

    ctx["runtime"]["client_obj"] = SimpleNamespace(publish=publish)
    result = bridge.handle_proxy_command(ctx, "worker/worker_fn_01/command/runtime", _make_command_envelope())
    assert result == "handled"
    assert command_queue.qsize() == 1
    emitted = command_queue.get_nowait()
    assert emitted["event_type"] == "V35_1_PROXY_RUNTIME_COMMAND_RECEIVED"
    assert emitted["legacy_event_type"] == "V34_PROXY_RUNTIME_COMMAND_RECEIVED"
    assert emitted["previous_event_type"] == "V33_PROXY_WORKER_COMMAND_RECEIVED"
    assert emitted["target"] == "thread_management"
    assert emitted["worker_id"] == "worker_fn_01"
    assert published[0]["topic"] == "worker/worker_fn_01/reply/engineering_client"


def test_runtime_config_builds_proxy_worker_bridge_settings(tmp_path):
    project_root = str(tmp_path)
    app_config = {
        "app": {"node_id": "worker_fn_01"},
        "proxy_worker_bridge": {
            "enabled": True,
            "worker_id": "worker_fn_01",
            "broker_host": "192.168.0.31",
            "use_mqtts": True,
            "ca_cert_file": "config/ssl/certs/mqtt/emqx-root-ca.pem",
            "tls_ciphers": "",
        },
    }
    settings = runtime_config.build_proxy_worker_bridge_runtime_settings(app_config, project_root, mqtt_client={})
    assert settings["enabled"] is True
    assert settings["worker_id"] == "worker_fn_01"
    assert settings["effective_port"] == 8883
    assert settings["tls_ciphers"] is None
    assert settings["ca_cert_file"].endswith("config/ssl/certs/mqtt/emqx-root-ca.pem")


def test_thread_specs_adds_proxy_worker_bridge_when_enabled():
    specs = thread_specs.build_thread_specs(_make_runtime_state(), _make_queue_tools(), _make_runtime_bundle(enabled=True))
    names = [spec["name"] for spec in specs]
    assert "Proxy_Worker_Bridge" in names
    spec = next(item for item in specs if item["name"] == "Proxy_Worker_Bridge")
    assert spec["component_name"] == "proxy_worker_bridge"
    assert callable(spec["target"])
    assert spec["target"].__name__ == "run_proxy_worker_bridge_thread"
    assert "command_event_queue" in spec["kwargs"]
    assert "outbound_event_queue" in spec["kwargs"]


def test_thread_specs_skips_proxy_worker_bridge_when_disabled():
    specs = thread_specs.build_thread_specs(_make_runtime_state(), _make_queue_tools(), _make_runtime_bundle(enabled=False))
    names = [spec["name"] for spec in specs]
    assert "Proxy_Worker_Bridge" not in names


def test_runtime_config_does_not_inherit_generic_mqtt_client_cert_defaults(tmp_path):
    app_config = {
        "app": {"node_id": "worker_fn_01"},
        "network": {
            "mqtt_client": {
                "ssl": {
                    "certfile": "config/ssl/certs/mqtt/client.cert.pem",
                    "keyfile": "config/ssl/certs/mqtt/client.key.pem",
                }
            }
        },
        "proxy_worker_bridge": {
            "enabled": True,
            "worker_id": "worker_fn_01",
            "broker_host": "192.168.0.31",
            "use_mqtts": True,
            "ca_cert_file": "config/ssl/certs/mqtt/emqx-root-ca.pem",
            "client_cert_file": None,
            "client_key_file": None,
        },
    }
    mqtt_client = {
        "ssl_enabled": False,
        "ssl_certfile": "config/ssl/certs/mqtt/client.cert.pem",
        "ssl_keyfile": "config/ssl/certs/mqtt/client.key.pem",
    }
    settings = runtime_config.build_proxy_worker_bridge_runtime_settings(app_config, str(tmp_path), mqtt_client=mqtt_client)
    assert settings["client_cert_file"] is None
    assert settings["client_key_file"] is None


def test_proxy_worker_bridge_tls_args_drop_missing_optional_client_cert(tmp_path):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("dummy-ca", encoding="utf-8")
    cfg = {
        "use_mqtts": True,
        "ca_cert_file": str(ca_file),
        "client_cert_file": str(tmp_path / "missing-client.pem"),
        "client_key_file": str(tmp_path / "missing-client.key"),
        "tls_ciphers": "",
        "tls_allow_insecure": False,
    }
    args = bridge.build_tls_arguments(cfg)
    assert args["ca_certs"] == str(ca_file)
    assert "certfile" not in args
    assert "keyfile" not in args
    assert "ciphers" not in args


def test_runtime_command_reply_payload_for_set_control_method_is_runtime_queued():
    ctx = bridge.build_bridge_ctx(
        node_id="worker_fn_01",
        resources={},
        runtime_bundle=_make_runtime_bundle(enabled=True),
        command_event_queue=Queue(),
        outbound_event_queue=None,
        logger=None,
        shutdown_event=None,
    )
    envelope = _make_command_envelope()
    envelope["payload"] = {"operation": "set_control_method", "value": "auto"}
    payload = bridge.build_runtime_command_reply_payload(
        ctx,
        envelope,
        {"domain": "runtime", "worker_id": "worker_fn_01"},
    )
    assert payload["status"] in ("applied_to_validated_runtime_state", "queued_for_runtime", "accepted")
    assert payload["runtime_binding"]["target"] == "thread_management"
    assert payload["runtime_binding"]["safe_direct_io_write"] is False


def test_runtime_command_reply_payload_for_state_read_contains_snapshot():
    ctx = bridge.build_bridge_ctx(
        node_id="worker_fn_01",
        resources={},
        runtime_bundle=_make_runtime_bundle(enabled=True),
        command_event_queue=Queue(),
        outbound_event_queue=None,
        logger=None,
        shutdown_event=None,
    )
    envelope = _make_command_envelope()
    envelope["payload"] = {"operation": "state_read"}
    payload = bridge.build_runtime_command_reply_payload(
        ctx,
        envelope,
        {"domain": "runtime", "worker_id": "worker_fn_01"},
    )
    assert payload["status"] == "accepted"
    assert payload["result"]["status"] == "accepted"
    assert payload["result"]["state"]["status"] == "ok"


def test_v35_1_emitted_command_event_has_resolvable_target():
    command_queue = Queue()
    ctx = bridge.build_bridge_ctx(
        node_id="worker_fn_01",
        resources={},
        runtime_bundle=_make_runtime_bundle(enabled=True),
        command_event_queue=command_queue,
        outbound_event_queue=None,
        logger=None,
        shutdown_event=None,
    )
    bridge._emit_command_event(ctx, {"domain": "runtime", "worker_id": "worker_fn_01"}, _make_command_envelope())
    event = command_queue.get_nowait()
    assert event["event_type"] == "V35_1_PROXY_RUNTIME_COMMAND_RECEIVED"
    assert event["target"] == "thread_management"
    assert event["runtime_binding"]["safe_direct_io_write"] is False


def test_v35_1_parameter_update_is_bound_to_runtime_state():
    ctx = bridge.build_bridge_ctx(
        node_id="worker_fn_01",
        resources={"user_config": {"data": {}, "lock": None}, "event_store": {"data": {}, "lock": None}},
        runtime_bundle=_make_runtime_bundle(enabled=True),
        command_event_queue=Queue(),
        outbound_event_queue=None,
        logger=None,
        shutdown_event=None,
    )
    envelope = _make_command_envelope()
    envelope["payload"] = {"operation": "parameter_update", "parameter_key": "gain", "value": 2.5}
    payload = bridge.build_runtime_command_reply_payload(ctx, envelope, {"domain": "runtime", "worker_id": "worker_fn_01"})
    assert payload["status"] in ("applied_to_validated_runtime_state", "queued_for_runtime")
    assert payload["runtime_binding"]["safe_direct_io_write"] is False
    assert ctx["resources"]["user_config"]["data"]["runtime_parameters"]["gain"] == 2.5
