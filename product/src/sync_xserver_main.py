# -*- coding: utf-8 -*-

# project_root/src/sync_xserver_main.py

try:
    from src import bootstrap as _bootstrap
except ModuleNotFoundError:
    import bootstrap as _bootstrap

_bootstrap.apply()

import logging

from concurrent.futures import ThreadPoolExecutor

from src.orchestration.runtime_config import (
    DEFAULT_APP_CONFIG_PATH,
    build_runtime_bundle,
    initialize_runtime_system,
    emit_startup_ascii,
    log_runtime_summary,
    runtime_cfg_sleep,
    reload_runtime_bundle,
)
from src.orchestration.runtime_state_factory import build_runtime_state
from src.orchestration.device_state_loader import load_device_state_if_requested
from src.orchestration.queue_factory import build_queue_tools
from src.orchestration.thread_factory import (
    install_signal_handlers,
    create_threads_from_specs,
    start_threads,
)
from src.orchestration.thread_specs import (
    resolve_component_targets,
    build_thread_specs,
    start_optional_cpu_probe,
    build_default_shutdown_queue_names,
)
from src.orchestration.shutdown import shutdown_runtime
from src.orchestration.runtime_health import maybe_write_runtime_health_snapshot


logger = logging.getLogger(__name__)


LOAD_APP_CONFIG = True
APP_CONFIG_PATH = DEFAULT_APP_CONFIG_PATH


def main():
    logger.info("[main] Starting sync_xserver_main refactored runtime entrypoint")

    runtime_bundle = build_runtime_bundle(
        load_enabled=LOAD_APP_CONFIG,
        config_path=APP_CONFIG_PATH,
        anchor_file=__file__,
        logger=logger,
    )

    initialize_runtime_system(
        bundle=runtime_bundle,
        logger=logger,
        hard_free_threading=True,
    )
    emit_startup_ascii(runtime_bundle, logger=logger)
    log_runtime_summary(runtime_bundle, logger=logger)

    runtime_state = build_runtime_state(
        settings=runtime_bundle["settings"],
        config_store=runtime_bundle["config_store"],
    )

    install_signal_handlers(runtime_state["shutdown_event"], logger=logger)

    queue_register_fn = runtime_bundle.get("interfaces", {}).get("queue_register")
    queue_tools = build_queue_tools(queue_register_fn=queue_register_fn)

    load_device_state_if_requested(
        runtime_state=runtime_state,
        runtime_bundle=runtime_bundle,
        logger=logger,
    )

    component_targets = resolve_component_targets(logger=logger)

    thread_specs = build_thread_specs(
        runtime_state=runtime_state,
        queue_tools=queue_tools,
        runtime_bundle=runtime_bundle,
        component_targets=component_targets,
        logger=logger,
    )

    thread_entries = create_threads_from_specs(
        thread_specs=thread_specs,
        runtime_state=runtime_state,
        config_store=runtime_bundle["config_store"],
        logger=logger,
        default_daemon=False,
    )

    global_executor = ThreadPoolExecutor(
        max_workers=runtime_bundle["settings"]["executor"]["max_workers"],
        thread_name_prefix="main-executor",
    )

    cpu_probe_thread = start_optional_cpu_probe(
        component_targets=component_targets,
        runtime_state=runtime_state,
        runtime_bundle=runtime_bundle,
        logger=logger,
    )

    health_state = {}

    try:
        logger.info("[main] Starting managed threads")
        start_threads(thread_entries, logger=logger)
        maybe_write_runtime_health_snapshot(
            runtime_bundle,
            runtime_state,
            queue_tools,
            thread_entries,
            health_state,
            status="running",
            logger=logger,
        )

        if runtime_bundle["settings"]["system"]["log_level_test"]:
            log_level_test = runtime_bundle.get("interfaces", {}).get("log_level_test")
            if callable(log_level_test):
                log_level_test()

        while not runtime_state["shutdown_event"].is_set():
            reload_runtime_bundle(runtime_bundle, logger=logger)
            maybe_write_runtime_health_snapshot(
                runtime_bundle,
                runtime_state,
                queue_tools,
                thread_entries,
                health_state,
                status="running",
                logger=logger,
            )
            runtime_cfg_sleep(runtime_bundle, "timing.main_loop_sleep_s", 5.0)

    except KeyboardInterrupt:
        logger.info("[main] KeyboardInterrupt received - starting shutdown")
        runtime_state["shutdown_event"].set()

    except Exception as exc:
        logger.error("[main] Unhandled exception: %s", exc, exc_info=True)
        runtime_state["shutdown_event"].set()

    finally:
        logger.info("[main] Starting coordinated shutdown")
        maybe_write_runtime_health_snapshot(
            runtime_bundle,
            runtime_state,
            queue_tools,
            thread_entries,
            health_state,
            status="shutdown_started",
            logger=logger,
        )

        shutdown_settings = runtime_bundle["settings"]["shutdown"]
        automation_shutdown_fn = component_targets.get("shutdown_automation_executor")
        cleanup_system_fn = runtime_bundle.get("interfaces", {}).get("cleanup_system")

        shutdown_runtime(
            shutdown_event=runtime_state["shutdown_event"],
            thread_entries=thread_entries,
            queue_tools=queue_tools,
            queues_to_nudge=build_default_shutdown_queue_names(),
            logger=logger,
            join_timeout_s=shutdown_settings["join_timeout_s"],
            nudge_enabled=shutdown_settings["nudge_queues"]["enabled"],
            nudge_repeats=shutdown_settings["nudge_queues"]["repeats"],
            nudge_put_timeout_s=shutdown_settings["nudge_queues"]["put_timeout_s"],
            nudge_stop_payloads=shutdown_settings["nudge_queues"]["stop_payloads"],
            main_executor=global_executor,
            automation_shutdown_fn=automation_shutdown_fn,
            executor_shutdown_wait=shutdown_settings["executor_shutdown_wait"],
            system_cleanup_fn=cleanup_system_fn,
            enforce_sigkill_timeout_s=shutdown_settings["enforce_sigkill_timeout_s"],
            extra_threads=(cpu_probe_thread,),
        )

        maybe_write_runtime_health_snapshot(
            runtime_bundle,
            runtime_state,
            queue_tools,
            thread_entries,
            health_state,
            status="shutdown_complete",
            logger=logger,
        )
        logger.info("[main] Shutdown complete")


if __name__ == "__main__":
    main()
