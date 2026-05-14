# -*- coding: utf-8 -*-

# src/libraries/_actuator_batch_timer.py

"""Synchroner Actuator-Batch-Timer.

Ersetzt den asyncio.create_task()-basierten Flush-Timer aus dem alten
Controller-Thread durch einen threading.Timer (synchron, CPU-effizient).

Design:
    - threading.Timer für verzögertes Flush
    - Thread-safe: Lock auf pending-Dict
    - Kein asyncio, kein OOP, kein Decorator, kein Lambda
    - functools.partial statt lambda
"""

import threading
import time
import logging

from functools import partial


logger = logging.getLogger(__name__)


# =============================================================================
# Batch-Timer Management
# =============================================================================

def create_batcher_ctx():
    """Erstellt einen neuen Actuator-Batcher Context."""
    return {
        "pending":      {},         # (function_type, address) → task
        "lock":         threading.Lock(),
        "timer":        None,       # threading.Timer Referenz
        "stats": {
            "batches_executed": 0,
            "tasks_batched":    0,
            "tasks_total":      0,
            "last_flush_ts":    0.0,
        },
    }


def _batch_key(task):
    """Key für Deduplikation: (function_type, address)."""
    ftype = task.get("function_type")
    addr = task.get("address")
    if addr is None:
        return (ftype, "aid", task.get("actuator_id"))
    try:
        addr_key = int(addr)
    except Exception:
        addr_key = str(addr)
    return (ftype, addr_key)


def _task_priority(task):
    """Priorität: 1=Regelung, 2=Direkt, 3=RuleEngine."""
    meta = task.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    origin = str(meta.get("origin") or "").strip().lower()
    state_id = meta.get("state_id")
    if origin == "rule_engine":
        return 3
    if state_id is not None:
        return 1
    return 2


def enqueue_task(batcher_ctx, task, *, timeout_s=0.5, flush_fn=None):
    """Fügt einen Task in den Batch ein (Last-Write-Wins, prioritätsgesteuert).

    Wenn timeout_s > 0: startet einen Timer der nach timeout_s flush_fn aufruft.
    flush_fn: callable(batcher_ctx) – wird vom Timer aufgerufen.
    """
    if not isinstance(task, dict):
        return

    key = _batch_key(task)
    new_prio = _task_priority(task)

    with batcher_ctx["lock"]:
        pending = batcher_ctx["pending"]
        stats = batcher_ctx["stats"]

        existing = pending.get(key)
        if existing is not None:
            existing_prio = _task_priority(existing)
            if new_prio > existing_prio:
                # Niedrigere Priorität darf höhere NICHT überschreiben
                stats["tasks_total"] += 1
                return

        overwritten = key in pending
        pending[key] = task
        stats["tasks_total"] += 1
        if not overwritten:
            stats["tasks_batched"] += 1

    # Timer starten (falls nicht bereits aktiv)
    if timeout_s > 0.0 and flush_fn is not None:
        _ensure_flush_timer(batcher_ctx, timeout_s, flush_fn)


def flush_pending(batcher_ctx, execute_fn, reason="timeout"):
    """Flusht alle pending Tasks (thread-safe).

    execute_fn: callable(task) – wird für jeden Task aufgerufen.
    Gibt die Anzahl ausgeführter Tasks zurück.
    """
    # Timer abbrechen
    _cancel_flush_timer(batcher_ctx)

    # Snapshot unter Lock
    with batcher_ctx["lock"]:
        pending = batcher_ctx["pending"]
        if not pending:
            return 0
        tasks = list(pending.values())
        pending.clear()
        batcher_ctx["stats"]["batches_executed"] += 1
        batcher_ctx["stats"]["last_flush_ts"] = time.time()

    # Ausführung außerhalb des Locks
    executed = 0
    for task in tasks:
        try:
            execute_fn(task)
            executed += 1
        except Exception as exc:
            logger.error("Batch-Flush Fehler: %s", exc)

    return executed


def _ensure_flush_timer(batcher_ctx, timeout_s, flush_fn):
    """Startet einen Timer falls nicht bereits einer läuft.

    threading.Timer: CPU-effizient, schläft bis zum Ablauf.
    """
    with batcher_ctx["lock"]:
        existing = batcher_ctx.get("timer")
        if existing is not None and existing.is_alive():
            return  # Timer läuft bereits

        timer = threading.Timer(
            float(timeout_s),
            partial(_timer_callback, batcher_ctx, flush_fn),
        )
        timer.daemon = True
        timer.name = "ActuatorFlushTimer"
        batcher_ctx["timer"] = timer
        timer.start()


def _cancel_flush_timer(batcher_ctx):
    """Bricht den laufenden Timer ab."""
    with batcher_ctx["lock"]:
        timer = batcher_ctx.get("timer")
        if timer is not None:
            timer.cancel()
        batcher_ctx["timer"] = None


def _timer_callback(batcher_ctx, flush_fn):
    """Wird vom Timer aufgerufen – delegiert an flush_fn."""
    try:
        batcher_ctx["timer"] = None
        flush_fn(batcher_ctx)
    except Exception as exc:
        logger.error("Flush-Timer Callback Fehler: %s", exc)


def batcher_stats(batcher_ctx):
    """Gibt die Batcher-Statistiken zurück (Thread-safe Kopie)."""
    with batcher_ctx["lock"]:
        return dict(batcher_ctx.get("stats", {}))


def batcher_pending_count(batcher_ctx):
    """Anzahl pending Tasks."""
    with batcher_ctx["lock"]:
        return len(batcher_ctx.get("pending", {}))
