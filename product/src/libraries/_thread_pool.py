# -*- coding: utf-8 -*-

# src/libraries/_thread_pool.py


"""

Generische, event-getriebene ThreadPool-Bibliothek (rein funktional, kein OOP).

Ziele (Design):
    - Queue-basiertes Job-Ingress / Result-Egress (Queues werden von außen übergeben)
    - Separater Event-Channel (queue_event_threadpool) für Steuerung & Config-Updates
    - Keine "Snapshots"/Overlay-States (aus thread_pool2 entfernt)
    - json_config wird als read-only Config-Snapshot (Copy-on-Write) geteilt
    - Free-Threaded Python (GIL disabled) kompatibel: keine implizite Thread-Safety-Annahmen

Job-Modell (neutral / generisch):
    Der Pool kann Jobs in 2 Formen verarbeiten:

    A) Callable-Job:
        - job ist callable -> wird ohne Parameter aufgerufen

    B) Dict-Job:
        - job ist dict mit mind. "fn": callable
        - optionale Felder:
            * job_id:        str (wird sonst erzeugt)
            * args:          list|tuple
            * kwargs:        dict
            * call_style:    str ("args" | "no_args" | "config_first" | "config_kw")
            * meta:          dict (wird in Result übernommen)

Config-Modell (json_config):
    - Pool friert json_config tief ein (MappingProxyType + tuple/frozenset)
    - Worker lesen nur die aktuelle (version, frozen_config)-Referenz
    - Updates passieren per Copy-on-Write: neue Config erstellen -> atomar referenzieren

Event-Modell (queue_event_threadpool):
    - Events sind dicts mit event_type + payload
    - Unterstützte event_type:
        * THREADPOOL_SHUTDOWN
        * THREADPOOL_SCALE_TO            payload: {"workers": int}
        * THREADPOOL_LIMITS              payload: {"min_workers": int, "max_workers": int}
        * THREADPOOL_CONFIG_REPLACE      payload: {"json_config": dict|list|...}
        * THREADPOOL_CONFIG_PATCH        payload: {"patch": [ops], "array_heads": [str,...]}
        * THREADPOOL_STATUS_REQUEST      payload: {"reply_to": <queue>|None}
        * THREADPOOL_ENQUEUE_JOB         payload: {"job": <callable|dict|...>}

Result-Ausgabe (thread_pool_out):
    - Job-Result:
        {"kind":"result", "job_id":..., "ok":bool, "result":..., "error":..., "traceback":..., ...}
    - Pool-Events:
        {"kind":"event", "event_type":..., "payload":{...}}

Hinweis zu "100% Datenkonsistenz":
    Der Pool garantiert:
        - keine internen Datenraces (Pool-eigener Zustand ist gelockt oder immutable)
        - Ergebnisse werden entweder geliefert oder blockieren am Out-Queue Backpressure
    Für "exactly-once" Side-Effects von Jobs kann eine Bibliothek ohne Transaktionen
    keine absolute Garantie geben. Für Worker-Crash-Retries gilt: Jobs sollten idempotent sein.
"""


# =============================================================================
# Imports
# =============================================================================

import copy
import json
import queue
import threading
import time
import traceback
import types
import uuid

from collections import deque
from functools import partial


# =============================================================================
# Konstante Event-Typen
# =============================================================================

EV_SHUTDOWN = "THREADPOOL_SHUTDOWN"
EV_SCALE_TO = "THREADPOOL_SCALE_TO"
EV_LIMITS = "THREADPOOL_LIMITS"
EV_CFG_REPLACE = "THREADPOOL_CONFIG_REPLACE"
EV_CFG_PATCH = "THREADPOOL_CONFIG_PATCH"
EV_STATUS_REQ = "THREADPOOL_STATUS_REQUEST"
EV_ENQUEUE_JOB = "THREADPOOL_ENQUEUE_JOB"


# =============================================================================
# Sentinel / Erkennung
# =============================================================================

_STOP_JOB = {"_tp": "__STOP_JOB__"}
_STOP_EVENT = {"_tp": "__STOP_EVENT__"}


def _is_stop_job(obj) -> bool:
    try:
        return isinstance(obj, dict) and obj.get("_tp") == "__STOP_JOB__"
    except Exception:
        return False


def _is_stop_event(obj) -> bool:
    try:
        return isinstance(obj, dict) and obj.get("_tp") == "__STOP_EVENT__"
    except Exception:
        return False


# =============================================================================
# Queue Utils
# =============================================================================


def _q_put(q, item, *, block=True, timeout=None) -> bool:
    if q is None:
        return False
    try:
        if timeout is None:
            q.put(item, block=block)
        else:
            q.put(item, block=block, timeout=timeout)
        return True
    except TypeError:
        # Queue-Implementierung ohne (block, timeout)
        try:
            q.put(item)
            return True
        except Exception:
            return False
    except Exception:
        return False


def _q_get(q, *, timeout) -> tuple[bool, object]:
    """Gibt (ok, item). ok=False bei Timeout/Fehler."""
    if q is None:
        return False, None
    try:
        item = q.get(timeout=timeout)
        return True, item
    except queue.Empty:
        return False, None
    except Exception:
        return False, None


def _q_task_done(q) -> None:
    if q is None:
        return
    try:
        td = getattr(q, "task_done", None)
        if td is not None:
            td()
    except Exception:
        return


# =============================================================================
# ID / Time Utils
# =============================================================================


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _now_ts() -> float:
    return time.time()


def _now_perf() -> float:
    return time.perf_counter()


# =============================================================================
# JSON-Config Freeze/Thaw (Copy-on-Write)
# =============================================================================


def _freeze_json(obj):
    """Erzeugt eine strukturell read-only Struktur.

    - dict -> MappingProxyType(dict(...))
    - list/tuple -> tuple
    - set/frozenset -> frozenset
    - andere Werte -> unverändert
    """
    if isinstance(obj, types.MappingProxyType):
        # MappingProxyType enthält bereits ein dict; wir frieren tiefer trotzdem
        try:
            obj = dict(obj)
        except Exception:
            return obj

    if isinstance(obj, dict):
        frozen = {}
        for k, v in obj.items():
            frozen[k] = _freeze_json(v)
        return types.MappingProxyType(frozen)

    if isinstance(obj, (list, tuple)):
        return tuple(_freeze_json(x) for x in obj)

    if isinstance(obj, (set, frozenset)):
        return frozenset(_freeze_json(x) for x in obj)

    return obj


def _thaw_json(obj):
    """Wandelt eine gefreezte Struktur in eine mutable JSON-Struktur zurück."""
    if isinstance(obj, types.MappingProxyType):
        try:
            obj = dict(obj)
        except Exception:
            return {}

    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            out[k] = _thaw_json(v)
        return out

    if isinstance(obj, tuple):
        return [_thaw_json(x) for x in obj]

    if isinstance(obj, frozenset):
        return {_thaw_json(x) for x in obj}

    if isinstance(obj, list):
        return [_thaw_json(x) for x in obj]

    if isinstance(obj, set):
        return {_thaw_json(x) for x in obj}

    return obj


def _normalize_json_config(cfg):
    """Best effort: akzeptiert dict oder JSON-String."""
    if cfg is None:
        return {}
    if isinstance(cfg, str):
        try:
            return json.loads(cfg)
        except Exception:
            return {}
    if isinstance(cfg, dict):
        return cfg
    # JSON kompatible Typen (list, etc.) sind ok, aber wir bevorzugen dict
    return cfg


# =============================================================================
# Patch-Semantik (optional) – kompatibel zu deinem Patch-Format
# =============================================================================


def _as_index(x):
    try:
        return int(x)
    except Exception:
        return None


def _deep_get_parent_and_key(root, path, create, array_heads):
    cur = root
    if not path:
        return None, None

    for seg in path[:-1]:
        if isinstance(cur, dict):
            if seg not in cur:
                if not create:
                    return None, None
                if str(seg) in array_heads:
                    cur[seg] = []
                else:
                    cur[seg] = {}
            cur = cur[seg]
            continue

        if isinstance(cur, list):
            idx = _as_index(seg)
            if idx is None:
                return None, None
            while create and idx >= len(cur):
                cur.append({})
            if idx < 0 or idx >= len(cur):
                return None, None
            cur = cur[idx]
            continue

        return None, None

    return cur, path[-1]


def _deep_set_inplace(root, path, value, array_heads) -> bool:
    parent, last = _deep_get_parent_and_key(root, path, True, array_heads)
    if parent is None:
        return False

    if isinstance(parent, dict):
        old = parent.get(last, object())
        if old is value or old == value:
            return False
        parent[last] = value
        return True

    if isinstance(parent, list):
        idx = _as_index(last)
        if idx is None:
            return False
        while idx >= len(parent):
            parent.append(None)
        old = parent[idx]
        if old is value or old == value:
            return False
        parent[idx] = value
        return True

    return False


def _deep_delete_inplace(root, path, array_heads) -> bool:
    parent, last = _deep_get_parent_and_key(root, path, False, array_heads)
    if parent is None:
        return False

    if isinstance(parent, dict):
        if last in parent:
            del parent[last]
            return True
        return False

    if isinstance(parent, list):
        idx = _as_index(last)
        if idx is None or idx < 0 or idx >= len(parent):
            return False
        if parent[idx] is None:
            return False
        parent[idx] = None
        return True

    return False


def apply_patch_ops_to_root(root, patch_ops, array_heads) -> bool:
    if root is None or not patch_ops:
        return False

    changed = False
    heads = set(array_heads or ())

    for op in patch_ops:
        if not isinstance(op, dict):
            continue

        kind = str(op.get("op", "upsert")).lower()
        path = op.get("path") or []
        value = op.get("value", None)

        if kind in ("upsert", "replace", "set"):
            changed |= _deep_set_inplace(root, path, value, heads)
        elif kind in ("remove", "delete"):
            changed |= _deep_delete_inplace(root, path, heads)
        else:
            continue

    return changed


# =============================================================================
# Job Execution
# =============================================================================


def _job_get_callable_and_meta(job):
    """Gibt (job_id, fn, args, kwargs, call_style, meta).

    Rückgabe fn=None, wenn Job nicht ausführbar ist.
    """
    if callable(job):
        return _new_uuid(), job, (), {}, "no_args", {}

    if not isinstance(job, dict):
        return _new_uuid(), None, (), {}, "no_args", {"job_type": type(job).__name__}

    job_id = job.get("job_id")
    job_id = str(job_id) if job_id else _new_uuid()

    fn = job.get("fn")
    if not callable(fn):
        return job_id, None, (), {}, "no_args", job.get("meta") if isinstance(job.get("meta"), dict) else {}

    args = job.get("args")
    if not isinstance(args, (list, tuple)):
        args = ()

    kwargs = job.get("kwargs")
    if not isinstance(kwargs, dict):
        kwargs = {}

    call_style = job.get("call_style")
    call_style = str(call_style or "args").strip().lower()

    meta = job.get("meta")
    meta = meta if isinstance(meta, dict) else {}

    return job_id, fn, tuple(args), dict(kwargs), call_style, meta


def _call_job_fn(fn, call_style, args, kwargs, config_ref):
    """Ausführung eines Jobs – ohne Signatur-Introspektion (stabil & schnell).

    call_style:
        - "no_args":        fn()
        - "args":           fn(*args, **kwargs)
        - "config_first":   fn(config_ref, *args, **kwargs)
        - "config_kw":      fn(*args, json_config=config_ref, **kwargs)
    """
    style = str(call_style or "args").strip().lower()

    if style == "no_args":
        return fn()

    if style == "config_first":
        return fn(config_ref, *args, **kwargs)

    if style == "config_kw":
        kw = dict(kwargs)
        kw["json_config"] = config_ref
        return fn(*args, **kw)

    return fn(*args, **kwargs)


# =============================================================================
# ThreadPool Factory
# =============================================================================


def make_threadpool(
    *,
    thread_pool_in,
    thread_pool_out,
    queue_event_threadpool=None,
    json_config=None,
    min_workers=1,
    max_workers=8,
    name="threadpool",
    worker_idle_timeout_s=0.2,
    monitor_interval_s=0.5,
    worker_restart_backoff_s=0.25,
    out_put_block=True,
    out_put_timeout=None,
    dedupe_results=True,
    retry_on_worker_crash=True,
    max_retries=1,
):
    """Erzeugt einen ThreadPool als Dict-API (rein funktional)."""

    # -----------------------------
    # Parameter normalisieren
    # -----------------------------
    try:
        min_workers_i = max(0, int(min_workers))
    except Exception:
        min_workers_i = 0
    try:
        max_workers_i = max(1, int(max_workers))
    except Exception:
        max_workers_i = 1
    if max_workers_i < min_workers_i:
        max_workers_i = min_workers_i

    try:
        idle_to = float(worker_idle_timeout_s)
    except Exception:
        idle_to = 0.2
    if idle_to <= 0.0:
        idle_to = 0.05

    try:
        mon_int = float(monitor_interval_s)
    except Exception:
        mon_int = 0.5
    if mon_int <= 0.0:
        mon_int = 0.1

    try:
        restart_backoff = float(worker_restart_backoff_s)
    except Exception:
        restart_backoff = 0.25
    if restart_backoff < 0.0:
        restart_backoff = 0.0

    try:
        max_retries_i = max(0, int(max_retries))
    except Exception:
        max_retries_i = 0

    # -----------------------------
    # Interner Zustand
    # -----------------------------
    lock = threading.RLock()
    shutdown_ev = threading.Event()

    cfg0 = _normalize_json_config(json_config)
    cfg0_frozen = _freeze_json(copy.deepcopy(cfg0))

    # Copy-on-Write Cell: snapshot ist IMMER ein Tuple (version:int, frozen_cfg:any)
    cfg_cell = {"snapshot": (0, cfg0_frozen)}

    # Worker Registry
    workers = []
    worker_seq = {"next": 0}

    # Inflight / Completed (für optionale Dedupe & Crash-Retry)
    inflight = {}
    completed = set()
    recent_results = deque(maxlen=48)

    metrics = {
        "jobs_ok": 0,
        "jobs_err": 0,
        "jobs_total": 0,
        "workers_started": 0,
        "workers_restarted": 0,
        "events_handled": 0,
    }

    _settings0 = {
        "min_workers": min_workers_i,
        "max_workers": max_workers_i,
        "worker_idle_timeout_s": idle_to,
        "monitor_interval_s": mon_int,
        "worker_restart_backoff_s": restart_backoff,
        "out_put_block": bool(out_put_block),
        "out_put_timeout": out_put_timeout,
        "dedupe_results": bool(dedupe_results),
        "retry_on_worker_crash": bool(retry_on_worker_crash),
        "max_retries": max_retries_i,
    }

    # Copy-on-Write Settings Cell (MappingProxyType für Read-Only)
    settings_cell = {"snapshot": types.MappingProxyType(dict(_settings0))}

    def get_settings_snapshot():
        """Lock-free Read: liefert ein read-only Settings-Mapping."""
        try:
            snap = settings_cell.get("snapshot")
            if isinstance(snap, types.MappingProxyType):
                return snap
        except Exception:
            pass
        return types.MappingProxyType(dict(_settings0))

    started = {"flag": False}
    controller_thread = {"thread": None}

    # -----------------------------
    # Kleine Status-/Debug-Helfer
    # -----------------------------
    def _safe_qsize_local(q):
        if q is None:
            return None
        try:
            return int(q.qsize())
        except Exception:
            return None

    def _copy_meta(meta):
        if not isinstance(meta, dict):
            return {}
        try:
            return copy.deepcopy(meta)
        except Exception:
            return dict(meta)

    def _trim_error_text(text, max_len=240):
        if text is None:
            return None
        out = str(text)
        if len(out) <= max_len:
            return out
        return out[: max_len - 3] + "..."

    def _make_current_job_view(job_id, worker_id, call_style, meta, wall0, cfg_ver, retry):
        info = {
            "job_id": str(job_id),
            "worker_id": int(worker_id),
            "call_style": str(call_style or "args"),
            "meta": _copy_meta(meta),
            "ts_start": float(wall0),
            "config_version": int(cfg_ver),
            "retry": int(retry),
        }

        if isinstance(meta, dict):
            info["event_type"] = meta.get("event_type")
            info["event_id"] = meta.get("event_id")
            info["slot_id"] = meta.get("slot_id")
        else:
            info["event_type"] = None
            info["event_id"] = None
            info["slot_id"] = None

        return info

    def _append_recent_result_locked(result_entry):
        if not isinstance(result_entry, dict):
            return
        recent_results.appendleft(result_entry)

    def _build_recent_result(job_id, worker_id, ok, result_obj, err_s, tb_s, meta, cfg_ver, wall0, wall1, dur):
        entry = {
            "job_id": str(job_id),
            "worker_id": int(worker_id),
            "ok": bool(ok),
            "duration_s": float(dur),
            "ts_start": float(wall0),
            "ts_end": float(wall1),
            "config_version": int(cfg_ver),
            "meta": _copy_meta(meta),
            "event_type": None,
            "event_id": None,
            "slot_id": None,
            "result_preview": None,
            "error": _trim_error_text(err_s),
            "traceback": tb_s if not ok else None,
        }

        if isinstance(meta, dict):
            entry["event_type"] = meta.get("event_type")
            entry["event_id"] = meta.get("event_id")
            entry["slot_id"] = meta.get("slot_id")

        if ok:
            try:
                if isinstance(result_obj, dict):
                    entry["result_preview"] = {
                        "keys": sorted(list(result_obj.keys()))[:8],
                    }
                elif isinstance(result_obj, (list, tuple, set, frozenset)):
                    entry["result_preview"] = {"type": type(result_obj).__name__, "len": len(result_obj)}
                elif result_obj is None:
                    entry["result_preview"] = None
                else:
                    entry["result_preview"] = {"repr": _trim_error_text(result_obj, max_len=96)}
            except Exception:
                entry["result_preview"] = None

        return entry

    def _sample_worker_cpu_locked(now_ts):
        cpu_supported = False
        cpu_pct_total = 0.0
        cpu_time_total_s = 0.0

        try:
            import psutil as _psutil
        except Exception:
            _psutil = None

        if _psutil is None:
            for ent in workers:
                ent["cpu_pct_last"] = None
            return {
                "cpu_supported": False,
                "cpu_pct_total": None,
                "cpu_time_total_s": None,
            }

        try:
            proc = _psutil.Process()
            thread_map = {}
            for tinfo in proc.threads():
                try:
                    thread_map[int(tinfo.id)] = float(tinfo.user_time + tinfo.system_time)
                except Exception:
                    continue
        except Exception:
            for ent in workers:
                ent["cpu_pct_last"] = None
            return {
                "cpu_supported": False,
                "cpu_pct_total": None,
                "cpu_time_total_s": None,
            }

        cpu_supported = True

        for ent in workers:
            native_id = ent.get("native_id")
            if native_id is None:
                ent["cpu_pct_last"] = None
                continue

            cur_total = thread_map.get(int(native_id))
            if cur_total is None:
                ent["cpu_pct_last"] = None
                continue

            prev_total = ent.get("cpu_last_sample_total_s")
            prev_ts = ent.get("cpu_last_sample_ts")

            ent["cpu_total_s"] = float(cur_total)
            cpu_time_total_s += float(cur_total)

            if prev_total is None or prev_ts is None:
                prev_total = 0.0
                prev_ts = float(ent.get("started_ts", now_ts) or now_ts)

            try:
                delta_cpu = float(cur_total) - float(prev_total)
            except Exception:
                delta_cpu = 0.0
            try:
                delta_wall = float(now_ts) - float(prev_ts)
            except Exception:
                delta_wall = 0.0

            if delta_cpu < 0.0:
                delta_cpu = 0.0
            if delta_wall <= 0.0:
                ent["cpu_pct_last"] = 0.0
            else:
                ent["cpu_pct_last"] = max(0.0, min(100.0, 100.0 * (delta_cpu / delta_wall)))

            ent["cpu_last_sample_total_s"] = float(cur_total)
            ent["cpu_last_sample_ts"] = float(now_ts)

            try:
                cpu_pct_total += float(ent.get("cpu_pct_last") or 0.0)
            except Exception:
                pass

        return {
            "cpu_supported": cpu_supported,
            "cpu_pct_total": round(cpu_pct_total, 3),
            "cpu_time_total_s": round(cpu_time_total_s, 6),
        }

    # -----------------------------
    # Emit Helper (Pool-Events -> Out-Queue)
    # -----------------------------
    def emit_pool_event(event_type, payload=None):
        st = get_settings_snapshot()
        msg = {
            "kind": "event",
            "event_type": str(event_type),
            "payload": payload if isinstance(payload, dict) else ({} if payload is None else {"value": payload}),
            "timestamp": _now_ts(),
            "pool": str(name),
        }
        _q_put(
            thread_pool_out,
            msg,
            block=bool(st.get("out_put_block", True)),
            timeout=st.get("out_put_timeout"),
        )

    # -----------------------------
    # Config Access
    # -----------------------------
    def get_config_snapshot():
        """Lock-free Read: liefert (version, frozen_config)."""
        try:
            snap = cfg_cell.get("snapshot")
            if isinstance(snap, tuple) and len(snap) == 2:
                return int(snap[0]), snap[1]
        except Exception:
            pass
        return 0, cfg0_frozen

    def replace_config(new_cfg, *, reason="replace"):
        mutable = _normalize_json_config(new_cfg)
        frozen = _freeze_json(copy.deepcopy(mutable))
        with lock:
            old_ver, _old_ref = get_config_snapshot()
            new_ver = int(old_ver) + 1
            cfg_cell["snapshot"] = (new_ver, frozen)
        emit_pool_event("THREADPOOL_CONFIG_UPDATED", {"version": new_ver, "reason": str(reason)})
        return new_ver

    def patch_config(patch_ops, array_heads=None, *, reason="patch"):
        # Aktuelle Config thawen
        old_ver, old_ref = get_config_snapshot()
        base = _thaw_json(old_ref)

        heads = set(array_heads or ())
        changed = apply_patch_ops_to_root(base, patch_ops, heads)
        if not changed:
            return int(old_ver)
        return replace_config(base, reason=reason)

    # -----------------------------
    # Job Inflight / Dedupe
    # -----------------------------
    def _mark_inflight(job_id, worker_id, job_obj):
        with lock:
            inflight[str(job_id)] = {
                "job_id": str(job_id),
                "worker_id": int(worker_id),
                "job": job_obj,
                "ts_start": _now_ts(),
            }

    def _unmark_inflight(job_id):
        with lock:
            inflight.pop(str(job_id), None)

    def _is_completed(job_id) -> bool:
        with lock:
            return str(job_id) in completed

    def _mark_completed(job_id):
        with lock:
            completed.add(str(job_id))

    # -----------------------------
    # Worker Lifecycle
    # -----------------------------
    def _next_worker_id() -> int:
        with lock:
            worker_seq["next"] += 1
            return int(worker_seq["next"])

    def _worker_entry(worker_id: int):
        return {
            "worker_id": int(worker_id),
            "stop_ev": threading.Event(),
            "thread": None,
            "native_id": None,
            "expected_stop": False,
            "started_ts": _now_ts(),
            "jobs_ok": 0,
            "jobs_err": 0,
            "jobs_total": 0,
            "last_job_ts": 0.0,
            "last_error_ts": 0.0,
            "last_job_id": None,
            "last_job_event_type": None,
            "last_job_duration_s": 0.0,
            "last_job_end_ts": 0.0,
            "current_job": None,
            "cpu_total_s": 0.0,
            "cpu_pct_last": None,
            "cpu_last_sample_total_s": None,
            "cpu_last_sample_ts": None,
        }

    def _worker_loop(ent):
        wid = int(ent.get("worker_id", -1))
        stop_ev = ent.get("stop_ev")

        try:
            ent["native_id"] = int(threading.get_native_id())
        except Exception:
            try:
                cur_thread = threading.current_thread()
                ent["native_id"] = getattr(cur_thread, "native_id", None)
            except Exception:
                ent["native_id"] = None

        while True:
            if shutdown_ev.is_set():
                break
            if stop_ev is not None and stop_ev.is_set():
                break

            st = get_settings_snapshot()
            try:
                idle_timeout = float(st.get("worker_idle_timeout_s", 0.2) or 0.2)
            except Exception:
                idle_timeout = 0.2

            ok_get, job = _q_get(thread_pool_in, timeout=idle_timeout)
            if not ok_get:
                continue

            inflight_job_id = None
            try:
                if _is_stop_job(job):
                    break

                job_id, fn, args, kwargs, call_style, meta = _job_get_callable_and_meta(job)

                if bool(st.get("dedupe_results", True)) and _is_completed(job_id):
                    continue

                retry_count = 0
                if isinstance(job, dict):
                    try:
                        retry_count = int(job.get("retry", 0) or 0)
                    except Exception:
                        retry_count = 0

                wall0 = _now_ts()
                cfg_ver, cfg_ref = get_config_snapshot()

                with lock:
                    ent["current_job"] = _make_current_job_view(
                        job_id=job_id,
                        worker_id=wid,
                        call_style=call_style,
                        meta=meta,
                        wall0=wall0,
                        cfg_ver=cfg_ver,
                        retry=retry_count,
                    )

                if fn is None:
                    rec = {
                        "kind": "result",
                        "job_id": str(job_id),
                        "ok": False,
                        "result": None,
                        "error": "Job ist nicht ausführbar (keine callable fn)",
                        "traceback": None,
                        "worker_id": wid,
                        "meta": meta,
                        "config_version": int(cfg_ver),
                        "ts_start": wall0,
                        "ts_end": _now_ts(),
                        "duration_s": 0.0,
                    }
                    _q_put(
                        thread_pool_out,
                        rec,
                        block=bool(st.get("out_put_block", True)),
                        timeout=st.get("out_put_timeout"),
                    )
                    with lock:
                        ent["jobs_total"] = int(ent.get("jobs_total", 0)) + 1
                        ent["jobs_err"] = int(ent.get("jobs_err", 0)) + 1
                        ent["last_error_ts"] = _now_ts()
                        ent["last_job_ts"] = _now_ts()
                        ent["last_job_id"] = str(job_id)
                        ent["last_job_event_type"] = rec.get("meta", {}).get("event_type") if isinstance(rec.get("meta"), dict) else None
                        ent["last_job_duration_s"] = 0.0
                        ent["last_job_end_ts"] = rec["ts_end"]
                        ent["current_job"] = None
                        _append_recent_result_locked(
                            _build_recent_result(
                                job_id=job_id,
                                worker_id=wid,
                                ok=False,
                                result_obj=None,
                                err_s=rec["error"],
                                tb_s=None,
                                meta=meta,
                                cfg_ver=cfg_ver,
                                wall0=wall0,
                                wall1=rec["ts_end"],
                                dur=0.0,
                            )
                        )
                        metrics["jobs_total"] += 1
                        metrics["jobs_err"] += 1
                    continue

                _mark_inflight(job_id, wid, job)
                inflight_job_id = str(job_id)

                ts0 = _now_perf()
                wall0 = wall0

                ok = True
                out = None
                err_s = None
                tb_s = None

                try:
                    out = _call_job_fn(fn, call_style, args, kwargs, cfg_ref)
                except BaseException as exc:
                    ok = False
                    err_s = "{}: {}".format(type(exc).__name__, str(exc))
                    try:
                        tb_s = traceback.format_exc()
                    except Exception:
                        tb_s = None

                ts1 = _now_perf()
                wall1 = _now_ts()
                dur = float(ts1 - ts0)

                rec = {
                    "kind": "result",
                    "job_id": str(job_id),
                    "ok": bool(ok),
                    "result": out if ok else None,
                    "error": None if ok else err_s,
                    "traceback": None if ok else tb_s,
                    "worker_id": wid,
                    "config_version": int(cfg_ver),
                    "meta": meta,
                    "ts_start": wall0,
                    "ts_end": wall1,
                    "duration_s": dur,
                }

                _q_put(
                    thread_pool_out,
                    rec,
                    block=bool(st.get("out_put_block", True)),
                    timeout=st.get("out_put_timeout"),
                )

                with lock:
                    metrics["jobs_total"] += 1
                    if ok:
                        metrics["jobs_ok"] += 1
                    else:
                        metrics["jobs_err"] += 1

                    ent["jobs_total"] = int(ent.get("jobs_total", 0)) + 1
                    if ok:
                        ent["jobs_ok"] = int(ent.get("jobs_ok", 0)) + 1
                    else:
                        ent["jobs_err"] = int(ent.get("jobs_err", 0)) + 1
                        ent["last_error_ts"] = _now_ts()
                    ent["last_job_ts"] = _now_ts()
                    ent["last_job_id"] = str(job_id)
                    ent["last_job_event_type"] = meta.get("event_type") if isinstance(meta, dict) else None
                    ent["last_job_duration_s"] = float(dur)
                    ent["last_job_end_ts"] = float(wall1)
                    ent["current_job"] = None

                    _append_recent_result_locked(
                        _build_recent_result(
                            job_id=job_id,
                            worker_id=wid,
                            ok=ok,
                            result_obj=out,
                            err_s=err_s,
                            tb_s=tb_s,
                            meta=meta,
                            cfg_ver=cfg_ver,
                            wall0=wall0,
                            wall1=wall1,
                            dur=dur,
                        )
                    )

                if bool(st.get("dedupe_results", True)):
                    _mark_completed(job_id)

            finally:
                try:
                    if inflight_job_id:
                        _unmark_inflight(inflight_job_id)
                except Exception:
                    pass

                with lock:
                    if ent.get("current_job") is not None:
                        ent["current_job"] = None

                _q_task_done(thread_pool_in)

        emit_pool_event(
            "THREADPOOL_WORKER_EXIT",
            {"worker_id": wid, "native_id": ent.get("native_id")},
        )

    def _spawn_worker_locked():
        wid = _next_worker_id()
        ent = _worker_entry(wid)

        target = partial(_worker_loop, ent)
        th = threading.Thread(target=target, name="{}-worker-{}".format(str(name), int(wid)), daemon=False)
        ent["thread"] = th
        workers.append(ent)

        metrics["workers_started"] += 1
        th.start()

        try:
            ent["native_id"] = getattr(th, "native_id", None)
        except Exception:
            pass

        emit_pool_event("THREADPOOL_WORKER_STARTED", {"worker_id": wid, "native_id": ent.get("native_id")})
        return ent

    def _count_total_workers_locked() -> int:
        return len(workers)

    def _count_alive_workers_locked() -> int:
        cnt = 0
        for ent in workers:
            th = ent.get("thread")
            try:
                if th is not None and th.is_alive():
                    cnt += 1
            except Exception:
                continue
        return cnt

    def _spawn_workers_to_min_locked():
        st = get_settings_snapshot()
        desired = int(st.get("min_workers", 0) or 0)
        maxw = int(st.get("max_workers", desired) or desired)
        while _count_alive_workers_locked() < desired and _count_total_workers_locked() < maxw:
            _spawn_worker_locked()

    def _prune_dead_workers_locked():
        kept = []
        for ent in workers:
            th = ent.get("thread")
            alive = False
            try:
                alive = bool(th is not None and th.is_alive())
            except Exception:
                alive = False

            if alive:
                kept.append(ent)
                continue

            if bool(ent.get("expected_stop", False)):
                emit_pool_event("THREADPOOL_WORKER_STOPPED", {"worker_id": ent.get("worker_id")})
                continue

            kept.append(ent)

        workers[:] = kept

    def _requeue_jobs_from_dead_worker(dead_worker_id: int):
        st = get_settings_snapshot()
        if not bool(st.get("retry_on_worker_crash", True)):
            return 0

        to_requeue = []
        with lock:
            for _jid, rec in inflight.items():
                try:
                    if int(rec.get("worker_id")) != int(dead_worker_id):
                        continue
                except Exception:
                    continue

                job_obj = rec.get("job")
                if job_obj is None:
                    continue

                if isinstance(job_obj, dict):
                    try:
                        r = int(job_obj.get("retry", 0))
                    except Exception:
                        r = 0

                    if r >= int(st.get("max_retries", 0) or 0):
                        continue
                    job_obj = dict(job_obj)
                    job_obj["retry"] = r + 1

                to_requeue.append(job_obj)

            dead_keys = [k for k, rr in inflight.items() if str(rr.get("worker_id")) == str(dead_worker_id)]
            for k in dead_keys:
                inflight.pop(k, None)

        cnt = 0
        for job_obj in to_requeue:
            if _q_put(thread_pool_in, job_obj, block=True, timeout=None):
                cnt += 1
        return cnt

    def _restart_dead_workers_locked():
        restarted = 0

        for ent in list(workers):
            th = ent.get("thread")
            alive = False
            try:
                alive = bool(th is not None and th.is_alive())
            except Exception:
                alive = False

            if alive:
                continue
            if bool(ent.get("expected_stop", False)):
                continue

            last_err = float(ent.get("last_error_ts", 0.0) or 0.0)
            if last_err and restart_backoff > 0.0:
                try:
                    if (_now_ts() - last_err) < restart_backoff:
                        continue
                except Exception:
                    pass

            wid = int(ent.get("worker_id", -1))
            emit_pool_event("THREADPOOL_WORKER_CRASH", {"worker_id": wid})

            try:
                rq = _requeue_jobs_from_dead_worker(wid)
            except Exception:
                rq = 0

            new_ent = _worker_entry(_next_worker_id())
            target = partial(_worker_loop, new_ent)
            new_th = threading.Thread(
                target=target,
                name="{}-worker-{}".format(str(name), int(new_ent["worker_id"])),
                daemon=False,
            )
            new_ent["thread"] = new_th

            try:
                workers.remove(ent)
            except Exception:
                pass
            workers.append(new_ent)

            metrics["workers_restarted"] += 1
            restarted += 1
            new_th.start()

            emit_pool_event(
                "THREADPOOL_WORKER_RESTARTED",
                {
                    "old_worker_id": wid,
                    "new_worker_id": int(new_ent["worker_id"]),
                    "requeued_jobs": int(rq),
                    "ts": _now_ts(),
                },
            )

        return restarted

    def _scale_to_locked(target_n: int):
        st = get_settings_snapshot()
        try:
            n = max(0, int(target_n))
        except Exception:
            n = 0

        try:
            minw = int(st.get("min_workers", 0) or 0)
        except Exception:
            minw = 0
        try:
            maxw = int(st.get("max_workers", 1) or 1)
        except Exception:
            maxw = 1
        if maxw < minw:
            maxw = minw

        if n < minw:
            n = minw
        if n > maxw:
            n = maxw

        alive = _count_alive_workers_locked()

        if n > alive:
            to_add = n - alive
            for _i in range(to_add):
                _spawn_worker_locked()
            emit_pool_event("THREADPOOL_SCALED", {"to": n, "delta": to_add})
            return n

        if n < alive:
            to_stop = alive - n
            stopped = 0
            for ent in workers:
                th = ent.get("thread")
                if stopped >= to_stop:
                    break
                try:
                    if th is None or not th.is_alive():
                        continue
                except Exception:
                    continue

                ent["expected_stop"] = True
                ev = ent.get("stop_ev")
                if ev is not None:
                    try:
                        ev.set()
                    except Exception:
                        pass
                stopped += 1

            emit_pool_event("THREADPOOL_SCALED", {"to": n, "delta": -stopped})
            return n

        return n

    def _status_snapshot_locked():
        ss = get_settings_snapshot()
        cfg_ver, _cfg_ref = get_config_snapshot()
        alive = _count_alive_workers_locked()
        total = _count_total_workers_locked()
        now_ts = _now_ts()

        cpu_info = _sample_worker_cpu_locked(now_ts)

        current_jobs = []
        details = []

        for ent in workers:
            th = ent.get("thread")
            try:
                alive_th = bool(th is not None and th.is_alive())
            except Exception:
                alive_th = False

            current_job = ent.get("current_job")
            current_job_id = None
            current_job_event_type = None
            current_job_started_ts = None
            current_job_elapsed_s = None
            current_job_retry = None
            current_job_meta = None

            if isinstance(current_job, dict):
                current_job_id = current_job.get("job_id")
                current_job_event_type = current_job.get("event_type")
                current_job_started_ts = current_job.get("ts_start")
                current_job_retry = current_job.get("retry")
                current_job_meta = _copy_meta(current_job.get("meta"))
                try:
                    current_job_elapsed_s = max(0.0, float(now_ts) - float(current_job_started_ts))
                except Exception:
                    current_job_elapsed_s = None

                current_jobs.append({
                    "job_id": current_job_id,
                    "worker_id": int(ent.get("worker_id")),
                    "event_type": current_job_event_type,
                    "event_id": current_job.get("event_id"),
                    "slot_id": current_job.get("slot_id"),
                    "retry": current_job_retry,
                    "elapsed_s": current_job_elapsed_s,
                    "ts_start": current_job_started_ts,
                    "config_version": current_job.get("config_version"),
                    "meta": current_job_meta,
                })

            details.append({
                "worker_id": ent.get("worker_id"),
                "native_id": ent.get("native_id"),
                "alive": alive_th,
                "expected_stop": bool(ent.get("expected_stop", False)),
                "jobs_total": int(ent.get("jobs_total", 0)),
                "jobs_ok": int(ent.get("jobs_ok", 0)),
                "jobs_err": int(ent.get("jobs_err", 0)),
                "last_job_ts": float(ent.get("last_job_ts", 0.0) or 0.0),
                "last_job_id": ent.get("last_job_id"),
                "last_job_event_type": ent.get("last_job_event_type"),
                "last_job_duration_s": float(ent.get("last_job_duration_s", 0.0) or 0.0),
                "last_job_end_ts": float(ent.get("last_job_end_ts", 0.0) or 0.0),
                "current_job_id": current_job_id,
                "current_job_event_type": current_job_event_type,
                "current_job_started_ts": current_job_started_ts,
                "current_job_elapsed_s": current_job_elapsed_s,
                "current_job_retry": current_job_retry,
                "current_job_meta": current_job_meta,
                "cpu_total_s": ent.get("cpu_total_s"),
                "cpu_pct_last": ent.get("cpu_pct_last"),
            })

        try:
            capacity_total = int(ss.get("max_workers", 0) or 0)
        except Exception:
            capacity_total = 0
        try:
            capacity_min = int(ss.get("min_workers", 0) or 0)
        except Exception:
            capacity_min = 0

        busy = len(current_jobs)
        free = max(0, capacity_total - busy)

        st = {
            "pool": str(name),
            "started": bool(started["flag"]),
            "shutdown": bool(shutdown_ev.is_set()),
            "workers_alive": int(alive),
            "workers_total": int(total),
            "workers_busy": int(busy),
            "workers_free": int(free),
            "capacity_total": int(capacity_total),
            "capacity_min": int(capacity_min),
            "queue_depth_in": _safe_qsize_local(thread_pool_in),
            "queue_depth_out": _safe_qsize_local(thread_pool_out),
            "queue_depth_event": _safe_qsize_local(queue_event_threadpool),
            "config_version": int(cfg_ver),
            "inflight": int(len(inflight)),
            "completed": int(len(completed)) if bool(ss.get("dedupe_results", True)) else None,
            "metrics": dict(metrics),
            "cpu_supported": cpu_info.get("cpu_supported"),
            "cpu_pct_total": cpu_info.get("cpu_pct_total"),
            "cpu_time_total_s": cpu_info.get("cpu_time_total_s"),
            "current_jobs": current_jobs,
            "recent_results": [copy.deepcopy(item) for item in list(recent_results)],
            "timestamp": now_ts,
        }

        st["workers"] = details
        return st

    def _handle_event(ev):
        if ev is None:
            return

        if isinstance(ev, dict):
            et = ev.get("event_type") or ev.get("type") or ev.get("cmd")
            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
        else:
            et = None
            payload = {}

        et_s = str(et or "").strip().upper()

        if et_s == EV_SHUTDOWN:
            shutdown_ev.set()
            return

        if et_s == EV_SCALE_TO:
            n = payload.get("workers")
            with lock:
                _scale_to_locked(int(n) if n is not None else 0)
            return

        if et_s == EV_LIMITS:
            mn = payload.get("min_workers")
            mx = payload.get("max_workers")
            new_min = None
            new_max = None
            with lock:
                cur = dict(get_settings_snapshot())

                if mn is not None:
                    try:
                        cur["min_workers"] = max(0, int(mn))
                    except Exception:
                        pass
                if mx is not None:
                    try:
                        cur["max_workers"] = max(1, int(mx))
                    except Exception:
                        pass

                try:
                    if int(cur.get("max_workers", 1)) < int(cur.get("min_workers", 0)):
                        cur["max_workers"] = int(cur.get("min_workers", 0))
                except Exception:
                    pass

                settings_cell["snapshot"] = types.MappingProxyType(cur)

                _scale_to_locked(_count_alive_workers_locked())
                _spawn_workers_to_min_locked()

                try:
                    new_min = int(cur.get("min_workers", 0))
                except Exception:
                    new_min = None
                try:
                    new_max = int(cur.get("max_workers", 0))
                except Exception:
                    new_max = None

            emit_pool_event("THREADPOOL_LIMITS_UPDATED", {"min_workers": new_min, "max_workers": new_max})
            return

        if et_s == EV_CFG_REPLACE:
            new_cfg = payload.get("json_config")
            replace_config(new_cfg, reason="event_replace")
            return

        if et_s == EV_CFG_PATCH:
            patch_ops = payload.get("patch") or []
            array_heads = payload.get("array_heads") or []
            if not isinstance(array_heads, (list, tuple, set)):
                array_heads = []
            patch_config(patch_ops, array_heads=array_heads, reason="event_patch")
            return

        if et_s == EV_STATUS_REQ:
            reply_to = payload.get("reply_to")
            with lock:
                st = _status_snapshot_locked()
            msg = {"kind": "event", "event_type": "THREADPOOL_STATUS", "payload": st, "timestamp": _now_ts()}
            if reply_to is not None:
                _q_put(reply_to, msg, block=True, timeout=None)
            else:
                ss = get_settings_snapshot()
                _q_put(
                    thread_pool_out,
                    msg,
                    block=bool(ss.get("out_put_block", True)),
                    timeout=ss.get("out_put_timeout"),
                )
            return

        if et_s == EV_ENQUEUE_JOB:
            job_obj = payload.get("job")
            _q_put(thread_pool_in, job_obj, block=True, timeout=None)
            return

        emit_pool_event("THREADPOOL_UNKNOWN_EVENT", {"raw": ev})

    def _controller_loop():
        emit_pool_event("THREADPOOL_CONTROLLER_STARTED", {"pool": str(name)})

        with lock:
            _spawn_workers_to_min_locked()

        last_mon = _now_perf()

        while not shutdown_ev.is_set():
            st = get_settings_snapshot()
            try:
                mon_to = float(st.get("monitor_interval_s", 0.5) or 0.5)
            except Exception:
                mon_to = 0.5

            if queue_event_threadpool is not None:
                ok_ev, ev = _q_get(queue_event_threadpool, timeout=mon_to)
            else:
                ok_ev, ev = (False, None)
                time.sleep(mon_to)

            if ok_ev:
                _q_task_done(queue_event_threadpool)
                with lock:
                    metrics["events_handled"] += 1

                if _is_stop_event(ev):
                    shutdown_ev.set()
                    break

                _handle_event(ev)

            now = _now_perf()
            if (now - last_mon) >= mon_to:
                with lock:
                    _restart_dead_workers_locked()
                    _prune_dead_workers_locked()
                    _spawn_workers_to_min_locked()
                last_mon = now

        emit_pool_event("THREADPOOL_CONTROLLER_EXIT", {"pool": str(name)})

    # -----------------------------
    # Public API
    # -----------------------------
    def start():
        if started["flag"]:
            return
        started["flag"] = True
        th = threading.Thread(target=_controller_loop, name="{}-controller".format(str(name)), daemon=False)
        controller_thread["thread"] = th
        th.start()
        emit_pool_event("THREADPOOL_STARTED", {"pool": str(name)})

    def submit_job(job, *, block=True, timeout=None) -> str:
        """Convenience: legt Job in thread_pool_in ab und liefert job_id (best effort)."""
        if job is None:
            return ""
        jid = ""
        if isinstance(job, dict):
            try:
                jid = str(job.get("job_id") or "")
            except Exception:
                jid = ""
            if not jid:
                jid = _new_uuid()
                try:
                    job = dict(job)
                    job["job_id"] = jid
                except Exception:
                    pass

        if not jid:
            jid = _new_uuid()

        _q_put(thread_pool_in, job, block=bool(block), timeout=timeout)
        return jid

    def submit(fn, *args, call_style="args", meta=None, **kwargs) -> str:
        """Convenience: erstellt Dict-Job mit fn/args/kwargs."""
        jid = _new_uuid()
        job = {
            "job_id": jid,
            "fn": fn,
            "args": args,
            "kwargs": kwargs,
            "call_style": call_style,
            "meta": meta if isinstance(meta, dict) else {},
        }
        _q_put(thread_pool_in, job, block=True, timeout=None)
        return jid

    def send_event(event_type, payload=None, *, block=True, timeout=None) -> bool:
        if queue_event_threadpool is None:
            return False
        ev = {
            "event_type": str(event_type),
            "payload": payload if isinstance(payload, dict) else ({} if payload is None else {"value": payload}),
            "timestamp": _now_ts(),
        }
        return _q_put(queue_event_threadpool, ev, block=bool(block), timeout=timeout)

    def scale_to(n: int) -> bool:
        return send_event(EV_SCALE_TO, {"workers": int(n)})

    def set_limits(min_w=None, max_w=None) -> bool:
        payload = {}
        if min_w is not None:
            payload["min_workers"] = int(min_w)
        if max_w is not None:
            payload["max_workers"] = int(max_w)
        return send_event(EV_LIMITS, payload)

    def config_replace(new_cfg) -> bool:
        return send_event(EV_CFG_REPLACE, {"json_config": new_cfg})

    def config_patch(patch_ops, array_heads=None) -> bool:
        payload = {"patch": patch_ops}
        if array_heads is not None:
            payload["array_heads"] = list(array_heads)
        return send_event(EV_CFG_PATCH, payload)

    def request_status(reply_to=None) -> bool:
        payload = {}
        if reply_to is not None:
            payload["reply_to"] = reply_to
        return send_event(EV_STATUS_REQ, payload)

    def get_status_snapshot():
        with lock:
            return _status_snapshot_locked()

    def shutdown(*, wait=True, join_timeout_s=2.0):
        """Stoppt Controller + Worker."""
        if queue_event_threadpool is not None:
            _q_put(queue_event_threadpool, {"event_type": EV_SHUTDOWN, "payload": {}}, block=False, timeout=None)

        shutdown_ev.set()

        with lock:
            num_alive = 0
            for ent in workers:
                ent["expected_stop"] = True
                ev = ent.get("stop_ev")
                if ev is not None:
                    try:
                        ev.set()
                    except Exception:
                        pass
                th = ent.get("thread")
                try:
                    if th is not None and th.is_alive():
                        num_alive += 1
                except Exception:
                    pass

        # Poison Pills in die In-Queue injizieren damit blockierte
        # Worker sofort aus queue.get() aufwachen und terminieren.
        for _i in range(max(num_alive, 1)):
            _q_put(thread_pool_in, _STOP_JOB, block=False, timeout=None)

        if wait:
            deadline = _now_perf() + float(join_timeout_s)

            th = controller_thread.get("thread")
            if th is not None:
                while th.is_alive() and _now_perf() < deadline:
                    try:
                        th.join(timeout=0.2)
                    except Exception:
                        break

            with lock:
                for ent in list(workers):
                    thw = ent.get("thread")
                    if thw is None:
                        continue
                    while thw.is_alive() and _now_perf() < deadline:
                        try:
                            thw.join(timeout=0.2)
                        except Exception:
                            break

        emit_pool_event("THREADPOOL_STOPPED", {"pool": str(name)})

    api = {
        "start": start,
        "shutdown": shutdown,

        "submit_job": submit_job,
        "submit": submit,

        "send_event": send_event,
        "scale_to": scale_to,
        "set_limits": set_limits,
        "config_replace": config_replace,
        "config_patch": config_patch,
        "request_status": request_status,
        "get_status_snapshot": get_status_snapshot,

        "get_config_snapshot": get_config_snapshot,

        "_in_q": thread_pool_in,
        "_out_q": thread_pool_out,
        "_ev_q": queue_event_threadpool,
    }

    return api


# =============================================================================
# Public Alias (Kompatibilität für thread_management.py)
# =============================================================================

create_thread_pool = make_threadpool
