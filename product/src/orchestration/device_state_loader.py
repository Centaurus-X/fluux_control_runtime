# -*- coding: utf-8 -*-

# src/orchestration/device_state_loader.py

import json
import logging
import os


def load_device_state_data(json_path, config_data, config_lock):
    """
    Load the device-state JSON file into the shared config_data mapping.
    """
    if not json_path:
        raise ValueError("json_path must not be empty")

    if not os.path.exists(json_path):
        raise FileNotFoundError("device-state JSON not found: %s" % json_path)

    with open(json_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError("device-state JSON root must be a mapping")

    with config_lock:
        config_data.clear()
        config_data.update(data)

    return data


def load_device_state_if_requested(runtime_state, runtime_bundle, logger=None):
    """
    Load device-state JSON when the runtime settings request it.

    This is a start-time concern only. The Datastore worker owns
    ongoing file synchronization afterwards.
    """
    logger_obj = logger or logging.getLogger(__name__)
    settings = runtime_bundle.get("settings", {})
    datastore_cfg = settings.get("datastore", {})

    bypass_device_state_load = bool(datastore_cfg.get("bypass_device_state_load", True))
    device_state_load_enabled = bool(datastore_cfg.get("device_state_load_enabled", False))
    selected_json_path = datastore_cfg.get("selected_json_path")

    result = {
        "loaded": False,
        "path": selected_json_path,
        "reason": "",
        "counts": {
            "controllers": 0,
            "sensors": 0,
            "actuators": 0,
        },
    }

    if bypass_device_state_load and not device_state_load_enabled:
        result["reason"] = "bypass_enabled"
        return result

    if not selected_json_path:
        result["reason"] = "no_selected_json_path"
        return result

    data = load_device_state_data(
        json_path=selected_json_path,
        config_data=runtime_state["config_data"],
        config_lock=runtime_state["config_data_lock"],
    )

    result["loaded"] = True
    result["reason"] = "loaded"
    result["counts"] = {
        "controllers": len(data.get("controllers", [])) if isinstance(data.get("controllers", []), list) else 0,
        "sensors": len(data.get("sensors", [])) if isinstance(data.get("sensors", []), list) else 0,
        "actuators": len(data.get("actuators", [])) if isinstance(data.get("actuators", []), list) else 0,
    }

    logger_obj.info(
        "[device_state_loader] Loaded device-state JSON: controllers=%d sensors=%d actuators=%d path=%s",
        result["counts"]["controllers"],
        result["counts"]["sensors"],
        result["counts"]["actuators"],
        selected_json_path,
    )

    return result
