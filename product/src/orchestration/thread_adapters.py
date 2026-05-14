# -*- coding: utf-8 -*-

# src/orchestration/thread_adapters.py

import asyncio
import inspect
import logging


def filter_kwargs(func, kwargs):
    """
    Filter keyword arguments to those accepted by the callable.
    """
    try:
        signature = inspect.signature(func)
        parameters = signature.parameters

        accepts_var_keyword = False
        for parameter in parameters.values():
            if parameter.kind == inspect.Parameter.VAR_KEYWORD:
                accepts_var_keyword = True
                break

        if accepts_var_keyword:
            return dict(kwargs)

        filtered = {}
        for key, value in kwargs.items():
            if key in parameters:
                filtered[key] = value

        return filtered
    except Exception:
        return dict(kwargs)


def normalize_target_kind(value):
    """
    Normalize the target kind selector.
    """
    normalized = str(value or "auto").strip().lower()

    if normalized in ("sync", "async", "auto"):
        return normalized

    return "auto"


def is_async_callable(target):
    """
    Detect whether the target should run inside a private event loop.
    """
    try:
        if inspect.iscoroutinefunction(target):
            return True
    except Exception:
        pass

    call_method = getattr(target, "__call__", None)

    try:
        if call_method is not None and inspect.iscoroutinefunction(call_method):
            return True
    except Exception:
        pass

    return False


def cleanup_event_loop(loop, logger=None):
    """
    Cancel pending tasks and close the loop safely.
    """
    logger_obj = logger or logging.getLogger(__name__)

    try:
        pending = asyncio.all_tasks(loop)
    except Exception:
        pending = set()

    try:
        for task in pending:
            try:
                task.cancel()
            except Exception:
                pass

        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    except Exception as exc:
        logger_obj.debug("[thread_adapters] Loop cancel cleanup failed: %s", exc)

    try:
        loop.run_until_complete(loop.shutdown_asyncgens())
    except Exception:
        pass

    try:
        loop.run_until_complete(loop.shutdown_default_executor())
    except Exception:
        pass

    try:
        loop.close()
    except Exception as exc:
        logger_obj.debug("[thread_adapters] Loop close failed: %s", exc)


def invoke_target(target, **kwargs):
    """
    Invoke a target once with filtered kwargs.
    """
    target_kwargs = filter_kwargs(target, kwargs)
    return target(**target_kwargs)


def run_awaitable_result(awaitable_obj, logger=None):
    """
    Run one awaitable result in a dedicated private event loop.
    """
    logger_obj = logger or logging.getLogger(__name__)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        return loop.run_until_complete(awaitable_obj)
    finally:
        cleanup_event_loop(loop, logger=logger_obj)


def run_async_target(target, logger=None, **kwargs):
    """
    Run an async callable inside a dedicated private event loop.
    """
    result = invoke_target(target, **kwargs)

    if inspect.isawaitable(result):
        return run_awaitable_result(result, logger=logger)

    return result


def run_sync_target(target, logger=None, **kwargs):
    """
    Run a synchronous target with filtered kwargs.
    """
    result = invoke_target(target, **kwargs)

    if inspect.isawaitable(result):
        return run_awaitable_result(result, logger=logger)

    return result


def run_target(target, target_kind="auto", logger=None, **kwargs):
    """
    Run a target using an explicit or auto-detected execution strategy.
    """
    normalized_kind = normalize_target_kind(target_kind)

    if normalized_kind == "async":
        return run_async_target(target, logger=logger, **kwargs)

    if normalized_kind == "sync":
        return run_sync_target(target, logger=logger, **kwargs)

    if is_async_callable(target):
        return run_async_target(target, logger=logger, **kwargs)

    return run_sync_target(target, logger=logger, **kwargs)


def run_targets_in_private_event_loop(targets, logger=None, **kwargs):
    """
    Run multiple async targets in one private event loop.
    """
    logger_obj = logger or logging.getLogger(__name__)

    normalized_targets = []
    for target in tuple(targets or ()):
        if callable(target):
            normalized_targets.append(target)

    if not normalized_targets:
        logger_obj.warning("[thread_adapters] No async targets provided")
        return None

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        tasks = []

        for target in normalized_targets:
            target_kwargs = filter_kwargs(target, kwargs)
            result = target(**target_kwargs)

            if inspect.isawaitable(result):
                tasks.append(loop.create_task(result))

        if not tasks:
            return None

        if len(tasks) == 1:
            return loop.run_until_complete(tasks[0])

        return loop.run_until_complete(asyncio.gather(*tasks))
    finally:
        cleanup_event_loop(loop, logger=logger_obj)
