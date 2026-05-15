# -*- coding: utf-8 -*-
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.orchestration import runtime_config


<<<<<<< HEAD
def test_v33_generic_mqtt_service_can_be_disabled_and_mqtts_ready(tmp_path):
=======
def test_v35_1_generic_mqtt_service_can_be_disabled_and_mqtts_ready(tmp_path):
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
    app_config = {
        "network": {
            "mqtt_client": {
                "enabled": False,
                "broker_ip": "192.168.0.31",
                "broker_port": 8883,
                "ssl": {
                    "enabled": True,
                    "cafile": "config/ssl/certs/mqtt/emqx-root-ca.pem",
                    "certfile": None,
                    "keyfile": None,
                },
            }
        }
    }
    settings = runtime_config.build_mqtt_client_runtime_settings(app_config, str(tmp_path))
    assert settings["enabled"] is False
    assert settings["broker_port"] == 8883
    assert settings["ssl_enabled"] is True
    assert settings["ssl_certfile"] is None
    assert settings["ssl_keyfile"] is None
