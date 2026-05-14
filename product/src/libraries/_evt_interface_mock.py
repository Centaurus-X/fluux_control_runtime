# -*- coding: utf-8 -*-

# src/libraries/_evt_interface_mock.py


# Minimaler Mock für _evt_interface (nur für Tests)
import threading
from queue import Queue, Empty

def make_event(event_type, payload=None, target=None, **kwargs):
    evt = {"event_type": str(event_type), "payload": payload or {}}
    if target:
        evt["target"] = target
    evt.update(kwargs)
    return evt

def create_sync_event_bus():
    bus = {"handlers": {}, "running": False, "task": None, "queue": Queue()}
    def register_handler(et, handler):
        bus["handlers"].setdefault(et, []).append(handler)
    def publish(event):
        try:
            bus["queue"].put_nowait(event)
        except Exception:
            pass
    def start_background():
        bus["running"] = True
    def stop():
        bus["running"] = False
    return {
        "register_handler": register_handler,
        "publish": publish,
        "start_background": start_background,
        "stop": stop,
        "handlers": bus["handlers"],
    }

def async_queue_put(q, item):
    try:
        q.put_nowait(item)
    except Exception:
        pass

def async_queue_get(q):
    try:
        return q.get(timeout=0.1)
    except Empty:
        return None
