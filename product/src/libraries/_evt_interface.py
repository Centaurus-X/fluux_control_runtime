# -*- coding: utf-8 -*-

# src/libraries/_evt_interface.py

"""
Event-Interface Bibliothek
Robuste Event-Kommunikation mit konsistenter JSON-Serialisierung
Funktionale Programmierung mit functools.partial
"""

import json
import uuid
import asyncio

import threading
from queue import Queue, Empty

import time

from functools import partial
from datetime import datetime, timezone

import logging


# ---- Typsicherungen/Kompatibilität ----#
try:
    import src.libraries._type_notations
    import src.libraries._type_dynamics
except ImportError:
    try:
        import _type_notations
        import _type_dynamics
    except ImportError:
        pass

# ---- Metriken/Transportsystem ----#
try:
    from src.libraries import _metrics_system as _metrics
except ImportError:
    try:
        import _metrics_system as _metrics
    except ImportError:
        # Stub-Modul wenn Metriken nicht verfügbar
        _metrics = None


# -------------------------------------------------------
# Logger initialisieren
logger = logging.getLogger(__name__)
# -------------------------------------------------------

# ThreadPoolExecutor für asynchrone Operationen
# _executor = ThreadPoolExecutor(max_workers=8)
_executor = None


# Metric Queue
# q = Queue()
# _metrics.queue_register(q, name="queue_event_send")



# =============================================================================
# Universelle Serialisierung - Kern der robusten Kommunikation
# =============================================================================

def universal_serialize(data):
    """
    Universelle Serialisierung für ALLE Datentypen.
    Pragmatisch: Alles wird zu JSON-String.
    """
    try:
        # Spezialbehandlung für bekannte Typen
        if hasattr(data, 'dict'):  # Pydantic Models
            return json.dumps(data.dict(), default=str)
        elif hasattr(data, '__dict__'):  # Normale Klassen
            return json.dumps(data.__dict__, default=str)
        else:
            # Standard JSON-Serialisierung mit Fallback
            return json.dumps(data, default=str)
    except Exception as e:
        # Letzter Fallback: String-Repräsentation
        logger.warning(f"Serialisierung fehlgeschlagen, nutze str(): {e}")
        return json.dumps({"_fallback": str(data), "_type": type(data).__name__})


def universal_deserialize(data):
    """
    Universelle Deserialisierung.
    Pragmatisch: Versuche JSON, sonst gib Original zurück.
    """
    if isinstance(data, str):
        try:
            return json.loads(data)
        except:
            # Wenn JSON-Parsing fehlschlägt, ist es ein normaler String
            return data
    else:
        # Bereits deserialisiert
        return data


# =============================================================================
# Event Erstellung
# =============================================================================

def make_event(event_type, payload=None, **kwargs):
    """Erstellt ein Event mit minimalen erforderlichen Feldern."""
    evt = {
        'event_id': str(uuid.uuid4()),
        'event_type': event_type,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'payload': payload or {}
    }
    evt.update(kwargs)
    return evt


# Kompatibilität
create_event = make_event


# =============================================================================
# Queue-Funktionen - Kern der asynchronen Kommunikation
# =============================================================================

async def async_queue_put(queue, item):
    """
    VEREINFACHT: Schreibt beliebige Daten in Queue.
    Immer JSON-serialisiert für Konsistenz.
    Nutzt den per-Loop Default-Executor (None),
    um Shutdown-Races mit einem globalen Executor zu vermeiden.
    + Metriken: Serialisierungszeit, Enqueuezeit, Größe, Latency-Fingerprint
    """
    if queue is None:
        logger.error("Queue ist None")
        return False

    # --- Messung: Serialisierung ---
    t0 = time.perf_counter()
    try:
        serialized = universal_serialize(item)
    except Exception as e:
        # Fehlende Serialisierung – als Fehler zählen und aborten
        try: _metrics.queue_mark_put_error(queue, e)
        except Exception: pass
        logger.error(f"Fehler beim Serialisieren in async_queue_put: {e}")
        return False
    t1 = time.perf_counter()

    # Debug Info (wie zuvor)
    try:
        data_type = type(item).__name__
        if isinstance(item, dict):
            if 'event_type' in item:
                data_type = 'event'
            elif 'message_id' in item or 'action' in item:
                data_type = 'message'
        logger.debug(f"Queue PUT: Typ={data_type}, Größe={len(serialized)} bytes")
    except Exception:
        pass

    # --- Enqueue (Executor=None) ---
    try:
        loop = asyncio.get_running_loop()
        # blockierendes put im Default-Executor
        await loop.run_in_executor(None, queue.put, serialized)
        t2 = time.perf_counter()
        # Metrik-Update
        try:
            _metrics.queue_track_put(queue, serialized, ser_sec=(t1 - t0), enqueue_sec=(t2 - t1))
            _metrics.queue_maybe_log()
        except Exception:
            pass
        return True

    except RuntimeError as e:
        msg = str(e)
        if "cannot schedule new futures after shutdown" in msg or "Event loop is closed" in msg:
            logger.debug("Queue PUT abgebrochen: Executor/Loop bereits beendet (Shutdown).")
            return False
        try: _metrics.queue_mark_put_error(queue, e)
        except Exception: pass
        logger.error(f"Fehler beim Queue PUT: {e}")
        return False
    except Exception as e:
        try: _metrics.queue_mark_put_error(queue, e)
        except Exception: pass
        logger.error(f"Fehler beim Queue PUT: {e}")
        return False


async def async_queue_get(queue):
    """
    VEREINFACHT: Liest Daten aus Queue.
    Immer JSON-deserialisiert für Konsistenz.
    MIT TIMEOUT für sauberes Shutdown!
    Nutzt den per-Loop Default-Executor (None),
    um Shutdown-Races mit einem globalen Executor zu vermeiden.
    + Metriken: Dequeue- und Deserialisierungszeit, Latency-Matching
    """
    if queue is None:
        logger.error("Queue ist None")
        return None

    try:
        loop = asyncio.get_running_loop()
        get_from_queue = partial(queue.get, block=True, timeout=5.0)

        # --- Dequeue ---
        t0 = time.perf_counter()
        try:
            serialized = await loop.run_in_executor(None, get_from_queue)
        except Empty:
            # Timeout ist normal im Shutdown – als "timeout" zählen
            try: _metrics.queue_mark_get_error(queue, None, timeout=True)
            except Exception: pass
            return None
        t1 = time.perf_counter()

        # --- Deserialisieren ---
        try:
            data = universal_deserialize(serialized)
        except Exception as e:
            try: _metrics.queue_mark_get_error(queue, e, timeout=False)
            except Exception: pass
            logger.error(f"Fehler beim Deserialisieren in async_queue_get: {e}")
            return None
        t2 = time.perf_counter()

        # Debug Info (wie zuvor)
        try:
            data_type = type(data).__name__
            if isinstance(data, dict):
                if 'event_type' in data:
                    data_type = 'event'
                elif 'message_id' in data or 'action' in data:
                    data_type = 'message'
            logger.debug(f"Queue GET: Typ={data_type}")
        except Exception:
            pass

        # --- Metrik-Update ---
        try:
            _metrics.queue_track_get(queue, serialized, deser_sec=(t2 - t1), dequeue_sec=(t1 - t0))
            _metrics.queue_maybe_log()
        except Exception:
            pass

        return data

    except Exception as e:
        try: _metrics.queue_mark_get_error(queue, e, timeout=False)
        except Exception: pass
        logger.error(f"Fehler beim Queue GET: {e}")
        return None


# Kompatibilitäts-Wrapper
async def queue_put_event(queue, event):
    """Spezialisierte Funktion für Event-Objekte."""
    return await async_queue_put(queue, event)


async def queue_get_event(queue):
    """Spezialisierte Funktion zum Lesen von Event-Objekten."""
    return await async_queue_get(queue)


# =============================================================================
# Event Bus
# =============================================================================

def create_event_bus():
    """
    Minimaler aber vollständiger Event Bus.
    """
    bus = {
        'handlers': {},      # event_type → [handlers]
        'fallbacks': [],
        'running': False,
        'task': None,
        'queue': asyncio.Queue(),
        'recv_queue': None,
        'send_queue': None
    }

    def register_handler(event_type, handler):
        """Registriert einen Handler für einen Event-Typ."""
        bus['handlers'].setdefault(event_type, []).append(handler)
        logger.info(f"Event-Handler registriert für: {event_type}")

    def register_fallback(handler):
        """Registriert einen Fallback-Handler."""
        bus['fallbacks'].append(handler)
        logger.info("Fallback-Handler für Events registriert")

    async def publish(event):
        """Veröffentlicht ein Event."""
        await bus['queue'].put(event)

    async def process_events():
        """Interne Event-Verarbeitung."""
        while bus['running']:
            try:
                evt = await bus['queue'].get()
                event_type = evt.get('event_type')
                
                if event_type:
                    handlers = bus['handlers'].get(event_type, [])
                    
                    if handlers:
                        for handler in handlers:
                            try:
                                await handler(evt)
                            except Exception as e:
                                logger.error(f"Event-Handler-Fehler für {event_type}: {e}")
                    elif bus['fallbacks']:
                        for fallback in bus['fallbacks']:
                            try:
                                await fallback(evt)
                            except Exception as e:
                                logger.error(f"Event-Fallback-Fehler: {e}")
                    else:
                        logger.debug(f"Kein Handler für Event: {event_type}")
                else:
                    logger.debug(f"Event ohne event_type: {evt}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event Bus Fehler: {e}")

    async def start():
        """Startet den Event Bus."""
        bus['running'] = True
        bus['task'] = asyncio.create_task(process_events())
        logger.info("Event Bus gestartet")

    async def start_background():
        """Startet den Bus im Hintergrund."""
        if not bus['task'] or bus['task'].done():
            await start()

    async def stop():
        """Stoppt den Event Bus."""
        bus['running'] = False
        if bus['task'] and not bus['task'].done():
            bus['task'].cancel()
            try:
                await bus['task']
            except asyncio.CancelledError:
                pass
        logger.info("Event Bus gestoppt")

    # Funktionen an Bus-Dict binden
    bus['register_handler'] = register_handler
    bus['register_fallback'] = register_fallback
    bus['publish'] = publish
    bus['start'] = start
    bus['start_background'] = start_background
    bus['stop'] = stop
    
    return bus


# =============================================================================
# Event Handler Registry - Zentrale Verwaltung
# =============================================================================

def create_event_handler_registry():
    """
    Zentrale Registry für Event-Handler.
    """
    registry = {
        'event_handlers': {},
        'event_buses': []
    }

    def register_event_handler(event_type, handler):
        """Registriert einen Event-Handler zentral."""
        registry['event_handlers'].setdefault(event_type, []).append(handler)
        # Bei allen registrierten Event-Bussen anmelden
        for bus in registry['event_buses']:
            bus['register_handler'](event_type, handler)
        logger.info(f"Event-Handler für {event_type} zentral registriert")

    def register_event_bus(bus):
        """Registriert einen Event-Bus in der Registry."""
        registry['event_buses'].append(bus)
        # Alle vorhandenen Handler beim Bus registrieren
        for event_type, handlers in registry['event_handlers'].items():
            for handler in handlers:
                bus['register_handler'](event_type, handler)
        logger.info("EventBus in Registry registriert")

    registry['register_event_handler'] = register_event_handler
    registry['register_event_bus'] = register_event_bus
    
    return registry


# =============================================================================
# Queue functions - synchronous counterparts
# =============================================================================

def sync_queue_put(queue, item):
    """
    Write arbitrary data into a queue synchronously.
    Data is always JSON-serialized for consistency.
    Keeps the same metrics behavior as async_queue_put().
    """
    if queue is None:
        logger.error("Queue ist None")
        return False

    # --- Measure serialization ---
    t0 = time.perf_counter()
    try:
        serialized = universal_serialize(item)
    except Exception as e:
        try:
            _metrics.queue_mark_put_error(queue, e)
        except Exception:
            pass
        logger.error(f"Fehler beim Serialisieren in sync_queue_put: {e}")
        return False
    t1 = time.perf_counter()

    # Debug info
    try:
        data_type = type(item).__name__
        if isinstance(item, dict):
            if 'event_type' in item:
                data_type = 'event'
            elif 'message_id' in item or 'action' in item:
                data_type = 'message'
        logger.debug(f"Queue PUT: Typ={data_type}, Größe={len(serialized)} bytes")
    except Exception:
        pass

    # --- Enqueue ---
    try:
        queue.put(serialized)
        t2 = time.perf_counter()

        try:
            _metrics.queue_track_put(
                queue,
                serialized,
                ser_sec=(t1 - t0),
                enqueue_sec=(t2 - t1),
            )
            _metrics.queue_maybe_log()
        except Exception:
            pass

        return True

    except Exception as e:
        try:
            _metrics.queue_mark_put_error(queue, e)
        except Exception:
            pass
        logger.error(f"Fehler beim Queue PUT: {e}")
        return False


def sync_queue_get(queue):
    """
    Read data from a queue synchronously.
    Data is always JSON-deserialized for consistency.
    Uses a timeout for clean shutdown behavior.
    Keeps the same metrics behavior as async_queue_get().
    """
    if queue is None:
        logger.error("Queue ist None")
        return None

    try:
        get_from_queue = partial(queue.get, block=True, timeout=5.0)

        # --- Dequeue ---
        t0 = time.perf_counter()
        try:
            serialized = get_from_queue()
        except Empty:
            try:
                _metrics.queue_mark_get_error(queue, None, timeout=True)
            except Exception:
                pass
            return None
        t1 = time.perf_counter()

        # --- Deserialize ---
        try:
            data = universal_deserialize(serialized)
        except Exception as e:
            try:
                _metrics.queue_mark_get_error(queue, e, timeout=False)
            except Exception:
                pass
            logger.error(f"Fehler beim Deserialisieren in sync_queue_get: {e}")
            return None
        t2 = time.perf_counter()

        # Debug info
        try:
            data_type = type(data).__name__
            if isinstance(data, dict):
                if 'event_type' in data:
                    data_type = 'event'
                elif 'message_id' in data or 'action' in data:
                    data_type = 'message'
            logger.debug(f"Queue GET: Typ={data_type}")
        except Exception:
            pass

        # --- Metrics update ---
        try:
            _metrics.queue_track_get(
                queue,
                serialized,
                deser_sec=(t2 - t1),
                dequeue_sec=(t1 - t0),
            )
            _metrics.queue_maybe_log()
        except Exception:
            pass

        return data

    except Exception as e:
        try:
            _metrics.queue_mark_get_error(queue, e, timeout=False)
        except Exception:
            pass
        logger.error(f"Fehler beim Queue GET: {e}")
        return None


def sync_queue_put_event(queue, event):
    """
    Specialized synchronous wrapper for event objects.
    """
    return sync_queue_put(queue, event)


def sync_queue_get_event(queue):
    """
    Specialized synchronous wrapper for reading event objects.
    """
    return sync_queue_get(queue)


# =============================================================================
# Event bus - synchronous counterpart
# =============================================================================

def _sync_event_bus_call_handler(handler, evt):
    """
    Call a handler in synchronous mode.
    If an async handler is registered, run it in an isolated event loop.
    """
    result = handler(evt)

    if asyncio.iscoroutine(result):
        return asyncio.run(result)

    return result


def create_sync_event_bus():
    """
    Minimal synchronous event bus analogous to create_event_bus().
    Uses queue.Queue and a background thread instead of asyncio.
    """
    stop_signal = object()

    bus = {
        'handlers': {},
        'fallbacks': [],
        'running': False,
        'task': None,
        'queue': Queue(),
        'recv_queue': None,
        'send_queue': None
    }

    def register_handler(event_type, handler):
        """Register a handler for an event type."""
        bus['handlers'].setdefault(event_type, []).append(handler)
        logger.info(f"Event-Handler registriert für: {event_type}")

    def register_fallback(handler):
        """Register a fallback handler."""
        bus['fallbacks'].append(handler)
        logger.info("Fallback-Handler für Events registriert")

    def publish(event):
        """Publish an event synchronously."""
        try:
            bus['queue'].put(event)
        except Exception as e:
            logger.error(f"Fehler beim Veröffentlichen im Sync Event Bus: {e}")

    def process_events():
        """Internal synchronous event processing loop."""
        while bus['running']:
            try:
                try:
                    evt = bus['queue'].get(block=True, timeout=0.5)
                except Empty:
                    continue

                if evt is stop_signal:
                    continue

                event_type = evt.get('event_type') if isinstance(evt, dict) else None

                if event_type:
                    handlers = bus['handlers'].get(event_type, [])

                    if handlers:
                        for handler in handlers:
                            try:
                                _sync_event_bus_call_handler(handler, evt)
                            except Exception as e:
                                logger.error(f"Event-Handler-Fehler für {event_type}: {e}")

                    elif bus['fallbacks']:
                        for fallback in bus['fallbacks']:
                            try:
                                _sync_event_bus_call_handler(fallback, evt)
                            except Exception as e:
                                logger.error(f"Event-Fallback-Fehler: {e}")

                    else:
                        logger.debug(f"Kein Handler für Event: {event_type}")
                else:
                    logger.debug(f"Event ohne event_type: {evt}")

            except Exception as e:
                logger.error(f"Sync Event Bus Fehler: {e}")

    def start():
        """Start the synchronous event bus in a background thread."""
        try:
            if bus['task'] and bus['task'].is_alive():
                return

            bus['running'] = True
            bus['task'] = threading.Thread(
                target=process_events,
                name="sync-event-bus",
                daemon=True,
            )
            bus['task'].start()
            logger.info("Sync Event Bus gestartet")

        except Exception as e:
            logger.error(f"Fehler beim Starten des Sync Event Bus: {e}")

    def start_background():
        """Start the bus in background mode."""
        start()

    def stop():
        """Stop the synchronous event bus."""
        try:
            bus['running'] = False

            if bus['task'] and bus['task'].is_alive():
                try:
                    bus['queue'].put(stop_signal)
                except Exception:
                    pass

                if bus['task'] is not threading.current_thread():
                    bus['task'].join()

            logger.info("Sync Event Bus gestoppt")

        except Exception as e:
            logger.error(f"Fehler beim Stoppen des Sync Event Bus: {e}")

    bus['register_handler'] = register_handler
    bus['register_fallback'] = register_fallback
    bus['publish'] = publish
    bus['start'] = start
    bus['start_background'] = start_background
    bus['stop'] = stop

    return bus


# =============================================================================
# Validierungs-Stubs für Kompatibilität
# =============================================================================

def create_validation_result(is_valid, errors=None):
    """Erstellt ein Validierungsergebnis für Kompatibilität."""
    return {'is_valid': is_valid, 'errors': errors or []}


def validate_event(evt, config=None):
    """
    Stub für Event-Validierung.
    
    Args:
        evt: Das zu validierende Event
        config: Optional - Konfiguration mit event_types für erweiterte Validierung
        
    Returns:
        Validierungsergebnis (aktuell immer erfolgreich)
    """
    # Basis-Validierung: event_type muss vorhanden sein
    if not isinstance(evt, dict):
        return create_validation_result(False, ["Event muss ein Dictionary sein"])
    
    if 'event_type' not in evt:
        return create_validation_result(False, ["event_type fehlt"])
    
    # Erweiterte Validierung mit Config (falls gewünscht)
    if config and 'event_types' in config:
        event_type = evt.get('event_type')
        if event_type not in config['event_types']:
            logger.debug(f"Event-Typ {event_type} nicht in config definiert")
    
    return create_validation_result(True)


# =============================================================================
# Globale Registry-Instanz
# =============================================================================

# Globale Registry-Instanz für Events
event_handler_registry = create_event_handler_registry()
