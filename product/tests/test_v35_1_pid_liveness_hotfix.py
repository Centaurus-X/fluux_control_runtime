# -*- coding: utf-8 -*-

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core import _controller_thread as controller_thread
from src.core import thread_management
from src.libraries._automation_system_interface import create_pid_controller
from src.libraries._automation_system_interface import pid_update


def test_pid_controlled_sensor_is_forced_from_process_state_config():
    cfg = {
        "automation_settings": {
            "force_automation_for_controlled_sensors": True,
            "force_automation_control_strategy_types": ["PID", "PI", "PD"],
        },
        "process_states": [
            {
                "state_id": 1,
                "sensor_ids": [1],
                "control_strategy": {"type": "PID"},
            },
            {
                "state_id": 11,
                "sensor_ids": [2],
                "control_strategy": None,
            },
        ],
    }

    force_map = controller_thread._resolve_force_automation_sensor_map(cfg)

    assert force_map.get(1) == "control_strategy.PID state=1"
    assert 2 not in force_map


def test_forced_sensor_value_bypasses_duplicate_zero_filter():
    ctx = {"sensor_accepted_values": {}}

    first = thread_management._sensor_value_changed(ctx, 1, 0, force=True)
    second = thread_management._sensor_value_changed(ctx, 1, 0, force=True)
    third = thread_management._sensor_value_changed(ctx, 1, 0, force=False)

    assert first is True
    assert second is True
    assert third is False


def test_conditional_anti_windup_recovers_without_long_negative_integral_tail():
    controller = create_pid_controller(
        0.01,
        0.001,
        0.0,
        0.0,
        10.0,
        0.1,
        True,
        1.0,
        0.5,
        "conditional",
    )

    out_high = pid_update(controller, 554.0, 747.0, 1000.0)
    integral_after_high = float(controller.get("integral", 0.0))
    out_low = pid_update(controller, 554.0, 0.0, 1010.0)

    assert out_high == 0.0
    assert integral_after_high == 0.0
    assert out_low > 0.0


def test_pid_force_automation_can_be_overridden_per_state():
    cfg = {
        "automation_settings": {
            "force_automation_for_controlled_sensors": False,
            "force_automation_control_strategy_types": ["PID"],
        },
        "process_states": [
            {
                "state_id": 1,
                "sensor_ids": [1],
                "control_strategy": {"type": "PID", "force_automation": True},
            },
            {
                "state_id": 2,
                "sensor_ids": [2],
                "control_strategy": {"type": "PID"},
            },
        ],
    }

    force_map = controller_thread._resolve_force_automation_sensor_map(cfg)

    assert force_map.get(1) == "control_strategy.PID state=1"
    assert 2 not in force_map


def test_config_chunking_keeps_pid_process_state_by_sensor_dependency():
    from src.orchestration.config_chunking import chunk_controller_config
    from src.orchestration.config_chunking import materialize_chunk_as_flat_config

    cfg = {
        "automation_settings": {
            "force_automation_for_controlled_sensors": True,
            "force_automation_control_strategy_types": ["PID"],
        },
        "controllers": [
            {"controller_id": 2},
        ],
        "sensors": [
            {"sensor_id": 1, "controller_id": 2},
            {"sensor_id": 12, "controller_id": 3},
        ],
        "actuators": [
            {"actuator_id": 5, "controller_id": 2},
            {"actuator_id": 12, "controller_id": 3},
        ],
        "process_states": [
            {
                "state_id": 1,
                "sensor_ids": [1],
                "actuator_ids": [5],
                "control_strategy": {"type": "PID"},
            },
            {
                "state_id": 18,
                "sensor_ids": [12],
                "actuator_ids": [12],
                "control_strategy": {"type": "PID"},
            },
        ],
    }

    chunk = chunk_controller_config(cfg, 2)
    flat = materialize_chunk_as_flat_config(chunk)
    force_map = controller_thread._resolve_force_automation_sensor_map(flat)

    state_ids = [row.get("state_id") for row in flat.get("process_states", [])]
    assert state_ids == [1]
    assert force_map.get(1) == "control_strategy.PID state=1"
    assert 12 not in force_map


def test_current_v35_1_config_chunking_keeps_sensor_1_pid_liveness_contract():
    import json
    from pathlib import Path
    from src.orchestration.config_chunking import chunk_controller_config
    from src.orchestration.config_chunking import materialize_chunk_as_flat_config

    config_path = Path(__file__).resolve().parents[1] / "config" / "___device_state_data.json"
    cfg = json.loads(config_path.read_text(encoding="utf-8"))

    chunk = chunk_controller_config(cfg, 2)
    flat = materialize_chunk_as_flat_config(chunk)
    force_map = controller_thread._resolve_force_automation_sensor_map(flat)

    assert force_map.get(1) == "control_strategy.PID state=1"
    assert any(row.get("state_id") == 1 for row in flat.get("process_states", []))
