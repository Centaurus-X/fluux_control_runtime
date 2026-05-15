# -*- coding: utf-8 -*-

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core import proxy_worker_bridge as bridge
from src.core import thread_management as tm
from src.orchestration import runtime_config
from src.orchestration import runtime_health


def _publish_ok(topic, payload, qos=0, retain=False):
    return SimpleNamespace(rc=0)


def test_v35_1_proxy_bridge_security_settings_are_configurable(tmp_path):
    app_config = {
        "app": {"node_id": "worker_fn_01"},
        "proxy_worker_bridge": {
            "enabled": True,
            "worker_id": "worker_fn_01",
            "broker_host": "192.168.0.31",
            "use_mqtts": True,
            "authentication_required": True,
            "username_env": "PROXY_WORKER_BRIDGE_USERNAME",
            "password_env": "PROXY_WORKER_BRIDGE_PASSWORD",
            "mtls_mode": "required",
            "certificate_rotation_enabled": True,
            "certificate_renew_before_days": 21,
            "topic_acl_enforcement": True,
            "topic_acl_default_policy": "deny",
            "allowed_publish_topics": ["worker/{worker_id}/reply/#"],
            "allowed_subscribe_topics": ["worker/{worker_id}/command/+"],
        },
    }
    settings = runtime_config.build_proxy_worker_bridge_runtime_settings(app_config, str(tmp_path), mqtt_client={})
    assert settings["authentication_required"] is True
    assert settings["username_env"] == "PROXY_WORKER_BRIDGE_USERNAME"
    assert settings["mtls_mode"] == "required"
    assert settings["certificate_rotation_enabled"] is True
    assert settings["certificate_renew_before_days"] == 21
    assert settings["topic_acl_enforcement"] is True
    assert settings["topic_acl_default_policy"] == "deny"


def test_v35_1_proxy_bridge_acl_allows_only_worker_topics():
    cfg = bridge.build_bridge_config({
        "node_id": "worker_fn_01",
        "proxy_worker_bridge": {
            "enabled": True,
            "worker_id": "worker_fn_01",
            "topic_acl_enforcement": True,
            "topic_acl_default_policy": "deny",
        },
    })
    assert bridge.topic_acl_allows(cfg, "publish", "worker/worker_fn_01/reply/client_a") is True
    assert bridge.topic_acl_allows(cfg, "publish", "worker/other/reply/client_a") is False
    assert bridge.topic_acl_allows(cfg, "subscribe", "worker/worker_fn_01/command/runtime") is True
    assert bridge.topic_acl_allows(cfg, "subscribe", "worker/other/command/runtime") is False


def test_v35_1_publish_acl_denies_wrong_topic():
    ctx = bridge.build_bridge_ctx(
        node_id="worker_fn_01",
        resources={},
        runtime_bundle={
            "settings": {
                "node_id": "worker_fn_01",
                "proxy_worker_bridge": {
                    "enabled": True,
                    "worker_id": "worker_fn_01",
                    "topic_acl_enforcement": True,
                    "topic_acl_default_policy": "deny",
                },
            }
        },
    )
    ctx["runtime"]["client_obj"] = SimpleNamespace(publish=_publish_ok)
    ok = bridge.publish_proxy_envelope(ctx, "worker/other/reply/client", {"payload": {}}, qos=1, retain=False)
    assert ok is False
    assert ctx["metrics"]["publish_topic_acl_denies"] == 1


def test_v35_1_required_mtls_fails_when_client_material_is_missing(tmp_path):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("dummy-ca", encoding="utf-8")
    cfg = {
        "use_mqtts": True,
        "ca_cert_file": str(ca_file),
        "client_cert_file": str(tmp_path / "missing-client.pem"),
        "client_key_file": str(tmp_path / "missing-client.key"),
        "tls_allow_insecure": False,
        "mtls_mode": "required",
    }
    try:
        bridge.build_tls_arguments(cfg)
        assert False, "required mTLS must fail when client cert/key are missing"
    except FileNotFoundError:
        assert True


def test_v35_1_fieldbus_profile_limits_controller_start_set():
    config = {
        "local_node": [{"node_id": "fn-01", "active_controllers": [1, 2, 3, 4]}],
        "controllers": [
            {"controller_id": 1, "device_name": "C1"},
            {"controller_id": 2, "device_name": "C2"},
            {"controller_id": 3, "device_name": "C3"},
            {"controller_id": 4, "device_name": "C4"},
        ],
    }
    fieldbus_profile = {
        "runtime_profile": {
            "enabled": True,
            "mode": "simulation_subset",
            "active_controller_ids": (2,),
            "optional_controller_ids": (1, 3, 4),
        }
    }
    controllers = tm._resolve_controllers(config, "fn-01", fieldbus_profile)
    assert [item["controller_id"] for item in controllers] == [2]


def test_v35_1_runtime_health_snapshot_documents_postgres_scope(tmp_path):
    runtime_bundle = {
        "settings": {
            "node_id": "fn-01",
            "monitoring": {
                "health": {"enabled": True, "path": str(tmp_path / "runtime_health.json"), "interval_s": 0.1},
                "alerting": {"enabled": False},
            },
            "security": {"runtime_security_version": "v35.1"},
            "fieldbus": {"runtime_profile": {"active_controller_ids": (2,)}},
            "proxy_worker_bridge": {"enabled": True, "worker_id": "worker_fn_01", "broker_host": "192.168.0.31", "effective_port": 8883, "use_mqtts": True},
        }
    }
    runtime_state = {"resources": {"worker_state": {"data": {"NODE_ID": "fn-01"}, "lock": None}}}
    queue_tools = {"queues_dict": {}}
    snapshot = runtime_health.build_runtime_health_snapshot(runtime_bundle, runtime_state, queue_tools, [], status="running")
    assert snapshot["runtime_health_version"] == "v35.1"
    assert snapshot["postgres_scope"] == "gateway_proxy_external_not_worker_runtime_dependency"
    assert snapshot["external_dependencies"]["postgresql"]["worker_local_dependency"] is False
