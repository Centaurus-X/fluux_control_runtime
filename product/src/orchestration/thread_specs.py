# -*- coding: utf-8 -*-

# src/orchestration/thread_specs.py

import importlib
import logging

from src.orchestration.runtime_config import cfg_bool, cfg_float, cfg_int


def _logger_or_default(logger=None):
    if logger is not None:
        return logger
    return logging.getLogger(__name__)


def _resolve_attr_from_candidates(candidates, logger=None):
    logger_obj = _logger_or_default(logger)

    for module_name, attr_name in tuple(candidates or ()):
        try:
            module_obj = importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
        except Exception as exc:
            logger_obj.debug("[thread_specs] Failed to import %s: %s", module_name, exc)
            continue

        try:
            attr_value = getattr(module_obj, attr_name)
        except Exception:
            continue

        if callable(attr_value):
            return attr_value, module_name, attr_name

    return None, None, None


def _module_importable(module_name):
    try:
        importlib.import_module(module_name)
        return True
    except Exception:
        return False


def resolve_component_targets(logger=None):
    """Resolve core component entrypoints from the current codebase.

    This resolver now follows the current synchronous core first and uses the
    MQTT transport module instead of the old WebSocket server path.
    """
    logger_obj = _logger_or_default(logger)

    candidate_map = {
        "run_worker_datastore": (
            ("src.core.worker_datastore", "run_worker_datastore"),
            ("worker_datastore", "run_worker_datastore"),
        ),
        "run_event_router": (
            ("src.core.sync_event_router", "sync_ev_queue_handler"),
            ("sync_event_router", "sync_ev_queue_handler"),
        ),
        "run_thread_management": (
            ("src.core.thread_management", "run_thread_management"),
            ("thread_management", "run_thread_management"),
        ),
        "run_state_event_management": (
            ("src.core.state_event_management", "run_state_event_management"),
            ("state_event_management", "run_state_event_management"),
        ),
        "run_mqtt_service": (
            ("src.core.generic_mqtt_thread", "run_generic_mqtt_interface_thread"),
            ("generic_mqtt_thread", "run_generic_mqtt_interface_thread"),
        ),
        # v27: worker_gateway als eigenstaendiger Thread in der Component-Map.
        # Entry-Point akzeptiert Queues direkt als kwargs (pick_q-Pattern).
        "run_worker_gateway": (
            ("src.core.worker_gateway", "run_worker_gateway_thread"),
            ("worker_gateway", "run_worker_gateway_thread"),
        ),
        "run_proxy_worker_bridge": (
            ("src.core.proxy_worker_bridge", "run_proxy_worker_bridge_thread"),
            ("proxy_worker_bridge", "run_proxy_worker_bridge_thread"),
        ),
        "start_cpu_probe": (
            ("src.core.cpu_probe", "start_cpu_probe"),
            ("cpu_probe", "start_cpu_probe"),
        ),
        "shutdown_automation_executor": (
            ("src.core._automation_thread", "shutdown_automation_global_executor"),
            ("_automation_thread", "shutdown_automation_global_executor"),
        ),
    }

    resolved = {"resolved_sources": {}}

    for key, candidates in candidate_map.items():
        attr_value, module_name, attr_name = _resolve_attr_from_candidates(candidates, logger=logger_obj)
        resolved[key] = attr_value
        resolved["resolved_sources"][key] = {
            "module": module_name,
            "attribute": attr_name,
        }

    resolved["mqtt_dependency_ready"] = _module_importable("paho.mqtt.client")

    logger_obj.info(
        "[thread_specs] Resolved components: %s",
        {
            key: value
            for key, value in resolved["resolved_sources"].items()
            if value.get("module")
        },
    )

    if not resolved["mqtt_dependency_ready"]:
        logger_obj.warning("[thread_specs] MQTT dependency 'paho.mqtt.client' is not importable")

    return resolved


def _delay(settings, key, default_value):
    start_delays = settings.get("timing", {}).get("start_delays_s", {})
    return cfg_float(start_delays.get(key, default_value), default_value)


def _make_thread_spec(
    name,
    target,
    component_name,
    kwargs,
    start_delay_s=0.0,
    enabled=True,
    target_kind="auto",
    ownership=None,
    daemon=False,
):
    return {
        "name": name,
        "component_name": component_name,
        "target": target,
        "kwargs": kwargs,
        "start_delay_s": float(start_delay_s),
        "enabled": bool(enabled),
        "target_kind": target_kind,
        "ownership": ownership or {},
        "daemon": bool(daemon),
    }


def build_default_shutdown_queue_names():
    """Return the default queue names to nudge during shutdown."""
    return (
        "queue_event_send",
        "queue_event_state",
        "queue_event_ds",
        "queue_event_dc",
        "queue_event_proc",
        "queue_event_pc",
        "queue_event_mba",
        "queue_event_mbc",
        "queue_event_ti",
        "queue_event_worker",
        "queue_ws",
        "queue_ti",
    )


def build_thread_specs(runtime_state, queue_tools, runtime_bundle, component_targets=None, logger=None):
    """Build thread specs for the current codebase.

    The specs follow the real component ownership:
    - one thread ctx per thread
    - shared resources remain inside runtime_state['resources']
    - queue ownership is explicit in the spec metadata
    """
    logger_obj = _logger_or_default(logger)

    if component_targets is None:
        component_targets = resolve_component_targets(logger=logger_obj)

    settings = runtime_bundle.get("settings", {})
    queues_dict = queue_tools["queues_dict"]
    pick_q = queue_tools["pick_q"]
    alias_q = queue_tools["alias_q"]

    node_id = runtime_state.get("node_id")
    resources = runtime_state.get("resources")
    specs = []

    datastore_target = component_targets.get("run_worker_datastore")
    if callable(datastore_target):
        datastore_enabled = bool(
            settings.get("network", {}).get("local_mode", False)
            and settings.get("datastore", {}).get("bypass_device_state_load", True)
            and settings.get("datastore", {}).get("start_datastore_runtime_thread_in_local_mode", True)
        )

        specs.append(_make_thread_spec(
            name="Worker_Datastore_Runtime",
            target=datastore_target,
            component_name="worker_datastore",
            enabled=datastore_enabled,
            target_kind="auto",
            start_delay_s=_delay(settings, "after_datastore", 2.4),
            kwargs={
                "node_id": node_id,
                "resources": resources,
                "config_data": runtime_state["config_data"],
                "config_lock": runtime_state["config_data_lock"],
                **pick_q("queue_event_send", "queue_event_ds"),
            },
            ownership={
                "read_resources": ("config_data",),
                "write_resources": ("config_data",),
                "ingress_queues": ("queue_event_ds",),
                "egress_queues": ("queue_event_send",),
                "resource_names": ("config_data",),
            },
        ))

    event_router_target = component_targets.get("run_event_router")
    broker_count = cfg_int(settings.get("event_router", {}).get("broker_threads", 2), 2)
    router_threads_per_broker = cfg_int(
        settings.get("event_router", {}).get("router_threads_per_broker", 4),
        4,
    )

    if callable(event_router_target):
        for index in range(1, broker_count + 1):
            delay_key = "after_event_broker_%d" % index
            specs.append(_make_thread_spec(
                name="Event_Broker_%d" % index,
                target=event_router_target,
                component_name="event_router",
                enabled=True,
                target_kind="auto",
                start_delay_s=_delay(settings, delay_key, 0.0),
                kwargs={
                    "queues_dict": queues_dict,
                    "router_name": "EventRouter-%d" % index,
                    "num_threads": router_threads_per_broker,
                },
                ownership={
                    "ingress_queues": ("queue_event_send",),
                    "egress_queues": (
                        "queue_event_state",
                        "queue_event_proc",
                        "queue_event_pc",
                        "queue_event_ds",
                        "queue_event_dc",
                        "queue_event_ws",
                        "queue_event_ti",
                        "queue_event_worker",
                        "queue_event_mba",
                        "queue_event_mbc",
                        "queue_event_mbh",
                    ),
                },
            ))

    tm_target = component_targets.get("run_thread_management")
    if callable(tm_target):
        worker_runtime_cfg = settings.get("worker_runtime", {}) or {}
        specs.append(_make_thread_spec(
            name="Thread_Management",
            target=tm_target,
            component_name="thread_management",
            enabled=True,
            target_kind="auto",
            start_delay_s=_delay(settings, "after_thread_management", 1.0),
            kwargs={
                "resources": resources,
                "node_id": node_id,
                "num_automation": settings.get("thread_management", {}).get("num_automation", 2),
                "chunking_enabled": cfg_bool(
                    worker_runtime_cfg.get("config_chunking_enabled", False), False,
                ),
                "writer_sync_enabled": cfg_bool(
                    worker_runtime_cfg.get("writer_sync_enabled", False), False,
                ),
                "gre_integration_enabled": cfg_bool(
                    worker_runtime_cfg.get("gre_integration_enabled", False), False,
                ),
                "fieldbus_profile": settings.get("fieldbus", {}),
                **pick_q("queue_event_send", "queue_event_pc"),
            },
            ownership={
                "read_resources": ("config_data", "sensor_values", "actuator_values"),
                "write_resources": ("event_store",),
                "ingress_queues": ("queue_event_pc",),
                "egress_queues": ("queue_event_send",),
            },
        ))

    # NOTE: The former Process_State_Management thread has been removed
    # entirely. Its responsibilities — evaluating triggers, transitions,
    # timers, dynamic rule sets and emitting CONFIG_PATCH commands — now
    # live inside the per-worker Generic Rule Engine (GRE) in the
    # controller runtime threads. No standalone PSM spec is emitted here.

    sem_target = component_targets.get("run_state_event_management")
    if callable(sem_target):
        specs.append(_make_thread_spec(
            name="State_Event_Management",
            target=sem_target,
            component_name="state_event_management",
            enabled=True,
            target_kind="auto",
            start_delay_s=_delay(settings, "after_state_event_management", 1.0),
            kwargs={
                "resources": resources,
                "node_id": node_id,
                "master_sync_enabled": cfg_bool(
                    settings.get("worker_state", {}).get("master_sync_enabled", True),
                    True,
                ),
                **pick_q("queue_event_send", "queue_event_state"),
            },
            ownership={
                "read_resources": ("config_data", "sensor_values", "actuator_values"),
                "write_resources": ("config_data", "sensor_values", "actuator_values"),
                "ingress_queues": ("queue_event_state",),
                "egress_queues": ("queue_event_send",),
            },
        ))

    mqtt_target = component_targets.get("run_mqtt_service")
    mqtt_cfg = settings.get("mqtt_client", {})
    mqtt_enabled = cfg_bool(mqtt_cfg.get("enabled", True), True)
    mqtt_dependency_ready = bool(component_targets.get("mqtt_dependency_ready", False))

    if callable(mqtt_target) and mqtt_enabled and mqtt_dependency_ready:
        specs.append(_make_thread_spec(
            name="MQTT_Service",
            target=mqtt_target,
            component_name="mqtt_service",
            enabled=True,
            target_kind="sync",
            start_delay_s=_delay(settings, "after_transport_service", 1.0),
            kwargs={
                "node_id": node_id,
                "interface_role": mqtt_cfg.get("interface_role", "worker"),
                "path_gate": mqtt_cfg.get("interface_role", "worker"),
                "broker_host": mqtt_cfg.get("broker_host"),
                "broker_port": mqtt_cfg.get("broker_port"),
                "topic_root": mqtt_cfg.get("topic_root"),
                "username": mqtt_cfg.get("username"),
                "password": mqtt_cfg.get("password"),
                "keepalive": mqtt_cfg.get("keepalive"),
                "qos": mqtt_cfg.get("qos"),
                "protocol_version": mqtt_cfg.get("protocol_version"),
                "transport": mqtt_cfg.get("transport"),
                "clean_session": mqtt_cfg.get("clean_session"),
                "reconnect_on_failure": mqtt_cfg.get("reconnect_on_failure"),
                "reconnect_min_delay": mqtt_cfg.get("reconnect_min_delay"),
                "reconnect_max_delay": mqtt_cfg.get("reconnect_max_delay"),
                "recv_system_events": mqtt_cfg.get("recv_system_events"),
                "auto_probe_on_connect": mqtt_cfg.get("auto_probe_on_connect"),
                "auto_select_single_peer": mqtt_cfg.get("auto_select_single_peer"),
                "connect_wait_timeout_s": mqtt_cfg.get("connect_wait_timeout_s"),
                "shutdown_join_timeout_s": mqtt_cfg.get("shutdown_join_timeout_s"),
                "ssl_enabled": mqtt_cfg.get("ssl_enabled"),
                "ssl_cafile": mqtt_cfg.get("ssl_cafile"),
                "ssl_capath": mqtt_cfg.get("ssl_capath"),
                "ssl_cadata": mqtt_cfg.get("ssl_cadata"),
                "ssl_certfile": mqtt_cfg.get("ssl_certfile"),
                "ssl_keyfile": mqtt_cfg.get("ssl_keyfile"),
                "ssl_password": mqtt_cfg.get("ssl_password"),
                "ssl_minimum_tls_version": mqtt_cfg.get("ssl_minimum_tls_version"),
                "ssl_ciphers": mqtt_cfg.get("ssl_ciphers"),
                "ssl_allow_unverified": mqtt_cfg.get("ssl_allow_unverified"),
                "ssl_auto_allow_unverified_if_no_ca": mqtt_cfg.get("ssl_auto_allow_unverified_if_no_ca"),
                "resources": resources,
                **alias_q({
                    "queue_send": "queue_ws",
                    "queue_recv": "queue_ti",
                    "queue_ws": "queue_ws",
                    "queue_io": "queue_ti",
                }),
                **pick_q("queue_event_send"),
            },
            ownership={
                "read_resources": ("worker_state", "connected"),
                "write_resources": ("connected", "event_store"),
                "ingress_queues": ("queue_ws",),
                "egress_queues": ("queue_ti", "queue_event_send"),
            },
        ))
    elif callable(mqtt_target) and mqtt_enabled and not mqtt_dependency_ready:
        logger_obj.warning("[thread_specs] MQTT service skipped because paho-mqtt is not available")

    # v27: worker_gateway als optionaler, gated Thread.
    # Default: disabled. Wird nur gestartet, wenn
    # settings.worker_gateway.enabled == true gesetzt ist.
    gateway_target = component_targets.get("run_worker_gateway")
    gateway_cfg = settings.get("worker_gateway", {})
    gateway_enabled = cfg_bool(gateway_cfg.get("enabled", False), False)

    if callable(gateway_target) and gateway_enabled:
        specs.append(_make_thread_spec(
            name="Worker_Gateway",
            target=gateway_target,
            component_name="worker_gateway",
            enabled=True,
            target_kind="sync",
            start_delay_s=_delay(settings, "after_transport_service", 1.0),
            kwargs={
                "node_id": node_id,
                "resources": resources,
                "runtime_bundle": runtime_bundle,
                **pick_q("queue_event_ti"),
                **pick_q("queue_event_send"),
                **alias_q({
                    "queue_gateway_in": "queue_event_ti",
                    "queue_gateway_out": "queue_event_send",
                }),
            },
            ownership={
                "read_resources": ("worker_state", "connected", "config"),
                "write_resources": ("event_store",),
                "ingress_queues": ("queue_event_ti",),
                "egress_queues": ("queue_event_send",),
            },
        ))
    elif callable(gateway_target) and not gateway_enabled:
        logger_obj.info("[thread_specs] worker_gateway available but disabled via config")


    # v35.1: Proxy Worker Bridge als optionale native Anbindung an das externe
    # MQTT/WebSocket Proxy-Gateway. Default bleibt konfigurierbar.
    proxy_bridge_target = component_targets.get("run_proxy_worker_bridge")
    proxy_bridge_cfg = settings.get("proxy_worker_bridge", {})
    proxy_bridge_enabled = cfg_bool(proxy_bridge_cfg.get("enabled", False), False)

    if callable(proxy_bridge_target) and proxy_bridge_enabled:
        specs.append(_make_thread_spec(
            name="Proxy_Worker_Bridge",
            target=proxy_bridge_target,
            component_name="proxy_worker_bridge",
            enabled=True,
            target_kind="sync",
            start_delay_s=_delay(settings, "after_proxy_worker_bridge", 1.2),
            kwargs={
                "node_id": node_id,
                "resources": resources,
                "runtime_bundle": runtime_bundle,
                "shutdown_event": runtime_state.get("shutdown_event"),
                **alias_q({
                    "command_event_queue": "queue_event_send",
                    "outbound_event_queue": "queue_event_worker",
                }),
            },
            ownership={
                "read_resources": ("worker_state", "connected", "config"),
                "write_resources": ("event_store",),
                "ingress_queues": ("queue_event_worker",),
                "egress_queues": ("queue_event_send",),
            },
        ))
    elif callable(proxy_bridge_target) and not proxy_bridge_enabled:
        logger_obj.info("[thread_specs] proxy_worker_bridge available but disabled via config")

    logger_obj.info("[thread_specs] Built thread specs: %s", [spec.get("name") for spec in specs])
    return specs



def start_optional_cpu_probe(component_targets, runtime_state, runtime_bundle, logger=None):
    """Start the optional CPU probe thread through the resolved component interface."""
    logger_obj = _logger_or_default(logger)
    cpu_probe_target = component_targets.get("start_cpu_probe")

    if not callable(cpu_probe_target):
        logger_obj.info("[thread_specs] CPU probe interface not found")
        return None

    cpu_probe_cfg = runtime_bundle.get("settings", {}).get("cpu_probe", {})
    if not cfg_bool(cpu_probe_cfg.get("enabled", True), True):
        logger_obj.info("[thread_specs] CPU probe disabled by config")
        return None

    try:
        return cpu_probe_target(
            runtime_state["shutdown_event"],
            runtime_state["cpu_probe_information"],
            runtime_state["cpu_probe_information_lock"],
            interval=cfg_float(cpu_probe_cfg.get("interval_s", 1.0), 1.0),
            top_n=cfg_int(cpu_probe_cfg.get("top_n", 12), 12),
            min_pct=cfg_float(cpu_probe_cfg.get("min_pct", 0.1), 0.1),
            history_max=cfg_int(cpu_probe_cfg.get("history_max", 300), 300),
            log_top=cfg_bool(cpu_probe_cfg.get("log_top", True), True),
        )
    except Exception as exc:
        logger_obj.error("[thread_specs] CPU probe startup failed: %s", exc, exc_info=True)
        return None
