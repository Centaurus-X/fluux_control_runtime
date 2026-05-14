# -*- coding: utf-8 -*-

# src/core/cpu_probe.py

"""
# Periodisches Sampling der CPU-Last pro OS-Thread – generisch & thread-safe:
# - Kein OOP, keine Decorators
# - Optional: Shared-Dict + Lock für globale Nutzung (read-mostly)
# - Optional: Event-Queue zum Pushen von Samples
# - Rückwärtskompatibel: alte Signatur funktioniert weiterhin
#
# Hinweis zur Metrik:
#   pct := 100 * (Δuser + Δsystem) / interval  [/ cpu_count (default)]
#   -> Mit normalize_per_cpu=True (Default) liegt 100% in Summe bei Volllast
#      über ALLE Kerne. Das entspricht deinem bisherigen Verhalten.
"""


from __future__ import annotations

import time
import threading
import logging
from functools import partial
from typing import Any, Dict, Optional, Iterable, Callable
import copy

try:
    import psutil  # type: ignore
except ImportError:
    psutil = None


__all__ = [
    "start_cpu_probe",
    "snapshot_cpu_info",
    "wait_for_first_sample",
]


def _sorted_by_pct(items: Iterable[tuple[int, float]]) -> list[tuple[int, float]]:
    # Kein lambda, explizite Key-Funktion
    def _key(kv: tuple[int, float]) -> float:
        return kv[1]
    return sorted(items, key=_key, reverse=True)


def _safe_put(q: Any, item: Any) -> None:
    try:
        q.put_nowait(item)
    except Exception:
        try:
            q.put(item, timeout=0.1)
        except Exception:
            pass


def snapshot_cpu_info(info: Optional[Dict[str, Any]],
                      info_lock: Optional["threading.Lock"] = None) -> Dict[str, Any]:
    """
    Thread-sicherer Schnappschuss vom Shared-Dict. Liefert IMMER eine Kopie.
    """
    if info is None:
        return {}
    if info_lock is not None:
        with info_lock:
            return copy.deepcopy(info)
    # Ohne Lock nur best-effort – besser immer Lock übergeben
    return copy.deepcopy(info)


def wait_for_first_sample(info: Optional[Dict[str, Any]],
                          info_lock: Optional["threading.Lock"] = None,
                          timeout: float = 5.0) -> bool:
    """
    Blockiert bis ein erstes Sample im Info-Dict steht (oder Timeout).
    """
    if info is None:
        return False
    t_end = time.time() + float(timeout)
    while time.time() < t_end:
        if info_lock:
            with info_lock:
                if "last_ts" in info:
                    return True
        else:
            if "last_ts" in info:
                return True
        time.sleep(0.05)
    return False


def start_cpu_probe(
    shutdown_event: "threading.Event",
    info: Optional[Dict[str, Any]] = None,
    info_lock: Optional["threading.Lock"] = None,
    *,
    interval: float = 1.0,
    top_n: int = 10,
    min_pct: float = 0.2,
    history_max: int = 120,
    normalize_per_cpu: bool = True,
    event_queue: Any = None,
    event_type: str = "CPU_PROBE_SAMPLE",
    log_top: bool = True,
    log_level: int = logging.INFO,
) -> Optional["threading.Thread"]:
    """
    Startet den CPU-Probe-Thread.

    Parameter:
      - shutdown_event:  globales Event zum sauberen Stoppen
      - info / info_lock: (optional) Shared-Dict + Lock für andere Threads
      - interval:         Sampling-Intervall
      - top_n:            Anzahl Top-Threads im Sample
      - min_pct:          Schwellwert in %
      - history_max:      Länge der Ring-History im Shared-Dict
      - normalize_per_cpu: Prozentwerte über CPU-Kerne normalisieren
      - event_queue:      (optional) Queue, um Samples als Events zu pushen
      - event_type:       Typ-String für Events
      - log_top:          Top-Liste loggen (ja/nein)
      - log_level:        Log-Level für Top-Liste

    Rückgabe:
      - Thread-Objekt oder None (falls psutil nicht verfügbar)
    """
    if psutil is None:
        logging.warning("CPU-Probe deaktiviert (psutil nicht installiert).")
        return None

    proc = psutil.Process()
    last_times: Dict[int, tuple[float, float]] = {}
    cpu_count = psutil.cpu_count(logical=True) or 1

    # optionaler Event-Pusher
    push_event = partial(_safe_put, event_queue) if event_queue is not None else None

    def _loop() -> None:
        while not (shutdown_event and shutdown_event.is_set()):
            # Mapping native Thread-ID -> Name
            name_by_tid = {t.native_id: t.name for t in threading.enumerate() if t.native_id}
            threads = proc.threads()

            if shutdown_event and shutdown_event.wait(interval):
                break

            now = time.time()
            stats: Dict[int, float] = {}

            # psutil liefert kumulierte Zeiten, wir differenzieren zu 'last_times'
            for th in threads:
                tid = th.id
                u, s = th.user_time, th.system_time
                prev = last_times.get(tid)
                if prev is None:
                    # beim ersten Mal nur baseline setzen
                    last_times[tid] = (u, s)
                    continue
                pu, ps = prev
                du, ds = u - pu, s - ps
                last_times[tid] = (u, s)

                pct = 100.0 * (du + ds) / max(interval, 1e-6)
                if normalize_per_cpu:
                    pct = pct / float(cpu_count)
                stats[tid] = pct

            # filtern/sortieren
            filtered = [(tid, pct) for tid, pct in stats.items() if pct >= float(min_pct)]
            filtered = _sorted_by_pct(filtered)
            top = filtered[: int(top_n)]

            # Sichtbares Top-Set
            top_struct = [
                {"tid": int(tid), "name": str(name_by_tid.get(tid, "?")), "pct": float(round(pct, 2))}
                for (tid, pct) in top
            ]

            # Vollständiges per-Thread-Set (kompakt, nur pct)
            per_thread_compact = {int(tid): float(round(pct, 4)) for tid, pct in stats.items()}

            # Sample-Objekt
            sample = {
                "ts": float(now),
                "interval": float(interval),
                "cpu_count": int(cpu_count),
                "normalized": bool(normalize_per_cpu),
                "top": top_struct,
                "per_thread": per_thread_compact,
            }

            # Optional ins Shared-Dict schreiben
            if info is not None:
                if info_lock is not None:
                    with info_lock:
                        _update_shared_info(info, sample, history_max)
                else:
                    _update_shared_info(info, sample, history_max)

            # Optional Event pushen
            if push_event is not None:
                evt = {
                    "event_type": event_type,
                    "timestamp": float(now),
                    "sample": sample,
                }
                push_event(evt)

            # Optional Log-Ausgabe
            if log_top and top_struct:
                line = " | ".join(f"{d['name']}@{d['tid']}: {d['pct']:.2f}%" for d in top_struct)
                logging.log(log_level, "CPU (Top Threads): %s", line)

            time.sleep(interval)

    t = threading.Thread(target=_loop, name="CPU-Probe", daemon=True)
    t.start()
    return t


def _update_shared_info(info: Dict[str, Any],
                        sample: Dict[str, Any],
                        history_max: int) -> None:
    """
    Aktualisiert das Shared-Dict minimal-locking und speichert Ring-History.
    """
    info["last_ts"] = sample["ts"]
    info["interval"] = sample["interval"]
    info["cpu_count"] = sample["cpu_count"]
    info["normalized"] = sample["normalized"]
    info["top"] = sample["top"]
    info["per_thread"] = sample["per_thread"]

    hist = info.get("history")
    if not isinstance(hist, list):
        hist = []
        info["history"] = hist
    hist.append({"ts": sample["ts"], "top": sample["top"]})
    # Ringbegrenzung
    if len(hist) > int(history_max):
        # effizienter als pop(0) in Schleife
        del hist[: len(hist) - int(history_max)]

"""
Bequemes Lese-Pattern ohne OOP, mit functools.partial

# in async_xserver_main.py (oder wo du liest):
from src.core.cpu_probe import snapshot_cpu_info  # schon vorhanden in cpu_probe.py

# einmal binden (spart Tipparbeit & vermeidet Lambdas):
get_cpu_info = partial(snapshot_cpu_info, cpu_probe_information, cpu_probe_information_lock)

# dann überall nutzen:
snap = get_cpu_info()
top = snap.get("top", [])
per_thread = snap.get("per_thread", {})
# ... weiter verarbeiten (Events absetzen, Thresholds prüfen, Metriken schreiben)


"""
