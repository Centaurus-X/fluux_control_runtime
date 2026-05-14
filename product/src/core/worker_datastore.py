# -*- coding: utf-8 -*-

# src/core/async_worker_datastore.py


"""
Datastore-Worker — v3.2 (Synchronous, heap_sync native, inotify file-watch)
=============================================================================

v3.2 changes over v3.1:
    - File watcher uses Linux ``inotify`` via ctypes (zero-polling, ~1ms latency).
    - Separate daemon thread blocks on kernel file-descriptor until change is reported.
    - Main loop no longer depends on queue timeout for file-watch frequency.
    - Graceful fallback to mtime polling on non-Linux platforms.
    - Queue timeout reduced to 1.0s (only affects periodic writer timing).
    - inotify syscalls are identical to C ``inotify_init1()`` / ``inotify_add_watch()``
      / ``read()`` — directly portable to C/C11.

Responsibility
~~~~~~~~~~~~~~
File-based persistence layer for the device-state configuration.
Bridges between the in-memory heap_sync cluster (owned by SEM) and
the ``___device_state_data.json`` file on disk.

Bootstrap (before SEM starts):
    - Reads JSON file, sanitises, writes into resources["config_data"].

Runtime — two directions:
    1. System -> File: CONFIG_PATCH_APPLIED / CONFIG_REPLACE_APPLIED
       -> dirty flag -> periodic atomic write of heap_sync snapshot.
    2. File -> System: inotify detects external edit
       -> semantic change -> CONFIG_REPLACE to SEM.
       -> format-only change -> schedule re-format.

Architecture
~~~~~~~~~~~~
- No OOP, no asyncio, no lambda, no decorator.
- functools.partial instead of lambda.
- Blocking queue + blocking inotify — CPU idle ~ 0 %.
- C/C11 migration friendly.
"""

import copy
import ctypes
import ctypes.util
import hashlib
import json
import logging
import os
import select
import struct
import sys
import threading
import time

from functools import partial
from datetime import datetime, timezone

try:
    from src.libraries._evt_interface import (
        create_sync_event_bus,
        sync_queue_get,
        sync_queue_put,
        make_event,
    )
except Exception:
    from _evt_interface import (
        create_sync_event_bus,
        sync_queue_get,
        sync_queue_put,
        make_event,
    )

try:
    from src.libraries._json_formator import formatiere as _jf_formatiere
    from src.libraries._json_formator import json_reparieren as _jf_reparieren
    from src.libraries._json_formator import sortiere_json_daten as _jf_sortiere_json_daten
    from src.libraries._json_formator import finde_referenz_sort_datei as _jf_finde_referenz_sort_datei
    _json_formator_available = True
except Exception:
    try:
        from _json_formator import formatiere as _jf_formatiere
        from _json_formator import json_reparieren as _jf_reparieren
        from _json_formator import sortiere_json_daten as _jf_sortiere_json_daten
        from _json_formator import finde_referenz_sort_datei as _jf_finde_referenz_sort_datei
        _json_formator_available = True
    except Exception:
        _jf_formatiere = None
        _jf_reparieren = None
        _jf_sortiere_json_daten = None
        _jf_finde_referenz_sort_datei = None
        _json_formator_available = False

try:
    from src.libraries._heap_sync_final_template import (
        export_node_dataset,
        export_node_dataset_cached,
        make_snapshot_cache,
        leader_node,
        deepcopy_json,
    )
except Exception:
    from _heap_sync_final_template import (
        export_node_dataset,
        export_node_dataset_cached,
        make_snapshot_cache,
        leader_node,
        deepcopy_json,
    )


logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

SENTINEL = {"_type": "__STOP__", "_sentinel": True}

EVENT_NAME_CONFIG_CHANGED = "CONFIG_CHANGED"
DEFAULT_MIN_SYNC_INTERVAL_S = 60.0
DEFAULT_ENCODING = "utf-8"
QUEUE_TIMEOUT_S = 1.0

JSON_DUMP_KW = {
    "ensure_ascii": False,
    "indent": 2,
    "sort_keys": False,
    "separators": (", ", ": "),
}

_JSON_DUMPS_CANONICAL = partial(json.dumps, **JSON_DUMP_KW)

FILE_WATCH_EMPTY_RETRY_DELAY_S = 0.15
FILE_WATCH_EMPTY_MAX_RETRIES = 3
FILE_WATCH_FALLBACK_POLL_S = 2.0

FORBIDDEN_STATE_KEYS = frozenset({"automation_parameters"})

_CONFIG_EVENT_TYPES = frozenset({
    "CONFIG_CHANGED",
    "CONFIG_PATCH",
    "CONFIG_REPLACE",
    "CONFIG_UPDATED",
    "CONFIG_CHANGE",
    "CONFIG_PATCH_APPLIED",
    "CONFIG_REPLACE_APPLIED",
    "DATA_LOAD_ERROR",
})

_PASSTHROUGH_SECTION_RULES = {
    "comm_profiles": "merge",
    "controllers_patch_example": "merge",
}

_PASSTHROUGH_TAIL_KEYS = (
    "comm_profiles",
    "controllers_patch_example",
)

_CONFIG_PAYLOAD_DOCUMENT_KEYS = (
    "new_config",
    "config",
    "full_config",
    "snapshot",
    "patched_config",
    "config_patch",
    "patch",
)


# =============================================================================
# Pure utility functions
# =============================================================================


def _is_stop_message(obj):
    if not isinstance(obj, dict):
        return False
    return obj.get("_type") == "__STOP__" and obj.get("_sentinel") is True


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# inotify via ctypes (Linux kernel, zero-polling, C-portable)
# =============================================================================
#
# These are thin wrappers around the Linux inotify syscalls.
# In C/C11 the equivalent is:
#     int fd = inotify_init1(IN_NONBLOCK);
#     int wd = inotify_add_watch(fd, dir, IN_CLOSE_WRITE | IN_MOVED_TO | IN_DELETE);
#     poll(&(struct pollfd){fd, POLLIN}, 1, timeout_ms);
#     read(fd, buf, sizeof(buf));
#
# =============================================================================

# inotify event mask constants (from <sys/inotify.h>)
_IN_CLOSE_WRITE = 0x00000008
_IN_MOVED_TO = 0x00000080
_IN_DELETE = 0x00000200
_IN_MODIFY = 0x00000002

# inotify_event struct: int wd (4) + uint32 mask (4) + uint32 cookie (4) + uint32 len (4) = 16 bytes header
_INOTIFY_EVENT_HEADER_SIZE = 16
_INOTIFY_EVENT_HEADER_FMT = "iIII"

_INOTIFY_AVAILABLE = False
_libc = None

try:
    if sys.platform.startswith("linux"):
        _libc_path = ctypes.util.find_library("c")
        if _libc_path:
            _libc = ctypes.CDLL(_libc_path, use_errno=True)
            # Verify functions exist
            _libc.inotify_init.restype = ctypes.c_int
            _libc.inotify_add_watch.restype = ctypes.c_int
            _libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
            _INOTIFY_AVAILABLE = True
            logger.debug("Datastore: inotify available via libc")
except Exception as _exc:
    logger.debug("Datastore: inotify not available: %s", _exc)


def _inotify_init():
    """Create an inotify file descriptor. Returns fd (int)."""
    fd = _libc.inotify_init()
    if fd < 0:
        errno = ctypes.get_errno()
        raise OSError(errno, "inotify_init failed: errno={}".format(errno))
    return fd


def _inotify_add_watch(fd, path, mask):
    """Add a watch on path. Returns watch descriptor (int)."""
    if isinstance(path, str):
        path = path.encode("utf-8")
    wd = _libc.inotify_add_watch(fd, path, ctypes.c_uint32(mask))
    if wd < 0:
        errno = ctypes.get_errno()
        raise OSError(errno, "inotify_add_watch failed: errno={}".format(errno))
    return wd


def _inotify_read_events(fd, buf_size=4096):
    """
    Read inotify events from fd. Returns list of (wd, mask, cookie, name).
    Non-blocking: caller must ensure fd is readable (via select/poll).
    """
    try:
        raw = os.read(fd, buf_size)
    except OSError:
        return []

    events = []
    offset = 0
    while offset + _INOTIFY_EVENT_HEADER_SIZE <= len(raw):
        wd, mask, cookie, name_len = struct.unpack_from(
            _INOTIFY_EVENT_HEADER_FMT, raw, offset,
        )
        offset += _INOTIFY_EVENT_HEADER_SIZE
        name = b""
        if name_len > 0 and offset + name_len <= len(raw):
            name = raw[offset:offset + name_len].rstrip(b"\x00")
            offset += name_len
        events.append((wd, mask, cookie, name.decode("utf-8", errors="replace")))

    return events


def _file_watch_thread_inotify(config_path, change_event, shutdown_event):
    """
    Blocking inotify watcher.  Runs as a daemon thread.

    Watches the DIRECTORY containing config_path (not the file itself),
    because atomic writes (os.replace) create a new inode — a watch on
    the old file would break.

    When any relevant event is detected for our filename:
        - ``change_event.set()`` is called
        - The main loop picks it up and runs ``_file_watch_cycle``

    Blocks on ``select([fd], [], [], 1.0)`` — the 1.0s timeout is only
    to check ``shutdown_event``.  Zero CPU between file changes.
    """
    dir_path = os.path.dirname(os.path.abspath(config_path))
    file_name = os.path.basename(config_path)
    fd = None

    try:
        fd = _inotify_init()
        mask = _IN_CLOSE_WRITE | _IN_MOVED_TO | _IN_DELETE | _IN_MODIFY
        _inotify_add_watch(fd, dir_path, mask)
        logger.info("Datastore: inotify watch active on %s (fd=%d)", dir_path, fd)

        while not shutdown_event.is_set():
            # Block until fd is readable OR timeout (for shutdown check)
            readable, _, _ = select.select([fd], [], [], 1.0)

            if not readable:
                continue

            events = _inotify_read_events(fd)
            for _wd, _mask, _cookie, name in events:
                # Filter: only our config file or its .tmp (atomic write)
                if name == file_name or name == file_name + ".tmp":
                    change_event.set()
                    break

    except Exception as exc:
        logger.error("Datastore: inotify thread error: %s", exc, exc_info=True)
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        logger.info("Datastore: inotify watch stopped")


def _file_watch_thread_fallback(config_path, change_event, shutdown_event):
    """
    Fallback mtime-polling watcher for non-Linux platforms.
    Checks mtime every FILE_WATCH_FALLBACK_POLL_S seconds.
    """
    last_mtime = None
    try:
        last_mtime = os.path.getmtime(config_path)
    except Exception:
        pass

    logger.info("Datastore: mtime-polling file watch active (fallback, non-Linux)")

    while not shutdown_event.is_set():
        shutdown_event.wait(timeout=FILE_WATCH_FALLBACK_POLL_S)
        if shutdown_event.is_set():
            break
        try:
            if not os.path.exists(config_path):
                change_event.set()
                continue
            mtime = os.path.getmtime(config_path)
            if last_mtime is not None and mtime > last_mtime:
                change_event.set()
            last_mtime = mtime
        except Exception:
            pass

    logger.info("Datastore: mtime-polling file watch stopped")


def _start_file_watch_thread(config_path, change_event, shutdown_event):
    """
    Start the file watch daemon thread.
    Uses inotify on Linux, mtime polling elsewhere.
    Returns the thread object.
    """
    if _INOTIFY_AVAILABLE:
        target = partial(_file_watch_thread_inotify, config_path, change_event, shutdown_event)
        name = "ds-inotify-watch"
    else:
        target = partial(_file_watch_thread_fallback, config_path, change_event, shutdown_event)
        name = "ds-mtime-watch"

    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


# =============================================================================
# Path resolution
# =============================================================================


def _project_root_from_here():
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.abspath(os.path.join(here, "..", ".."))


def _resolve_paths(root_dir=None, env_var="CONFIG_JSON_PATH"):
    rd = root_dir or os.environ.get("PROJECT_ROOT") or _project_root_from_here()
    candidate_env = os.environ.get(env_var)

    if candidate_env and os.path.exists(candidate_env):
        config_path = os.path.abspath(candidate_env)
    else:
        c1 = os.path.join(rd, "config", "___device_state_data.json")
        c2 = os.path.join(os.getcwd(), "config", "___device_state_data.json")
        c3 = os.path.join("/mnt/data", "___device_state_data.json")
        config_path = os.path.abspath(c1)
        for c in (c1, c2, c3):
            if os.path.exists(c):
                config_path = os.path.abspath(c)
                break

    backup_dir = os.path.join(os.path.dirname(config_path), "backup")
    return rd, config_path, backup_dir


def _ensure_dir(path):
    if not path:
        return
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        logger.error("Cannot create directory: %s -> %s", path, e)


# =============================================================================
# JSON utilities
# =============================================================================


def _canonical_json(data):
    try:
        txt = _JSON_DUMPS_CANONICAL(data)
        if not txt.endswith("\n"):
            txt += "\n"
        return txt
    except Exception:
        try:
            txt = json.dumps(data, indent=2, ensure_ascii=False)
            if not txt.endswith("\n"):
                txt += "\n"
            return txt
        except Exception:
            return "{}\n"


def _load_json_formatter_reference_data(config_path):
    if not config_path or _jf_finde_referenz_sort_datei is None:
        return None

    try:
        referenz_pfad = _jf_finde_referenz_sort_datei(config_path)
        if not referenz_pfad:
            return None
        referenz_daten, _ = _read_json_file(referenz_pfad)
        return referenz_daten
    except Exception as exc:
        logger.warning(
            "Datastore: JSON formatter reference load failed for %s: %s",
            config_path,
            exc,
        )
        return None



def _numeric_pk_id_value(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text and text.lstrip("+-").isdigit():
            try:
                return int(text, 10)
            except Exception:
                return None
    return None



def _find_first_numeric_pk_key(records):
    if not records or not isinstance(records[0], dict):
        return None

    first_record = records[0]
    first_id_key = None

    for key in first_record.keys():
        if key == "id" or str(key).endswith("_id"):
            first_id_key = key
            break

    if first_id_key is None:
        return None

    if _numeric_pk_id_value(first_record.get(first_id_key)) is None:
        return None

    for record in records:
        if not isinstance(record, dict):
            return None
        if _numeric_pk_id_value(record.get(first_id_key)) is None:
            return None

    return first_id_key



def _numeric_pk_record_sort_key(pk_key, record):
    return _numeric_pk_id_value(record.get(pk_key))



def _normalize_numeric_pk_lists(value):
    if isinstance(value, dict):
        normalized = {}
        for key, item in value.items():
            normalized[key] = _normalize_numeric_pk_lists(item)
        return normalized

    if isinstance(value, list):
        normalized_list = []
        for item in value:
            normalized_list.append(_normalize_numeric_pk_lists(item))

        if normalized_list and all(isinstance(item, dict) for item in normalized_list):
            pk_key = _find_first_numeric_pk_key(normalized_list)
            if pk_key is not None:
                numeric_ids = []
                for record in normalized_list:
                    numeric_ids.append(_numeric_pk_id_value(record.get(pk_key)))
                if numeric_ids != sorted(numeric_ids):
                    return sorted(
                        normalized_list,
                        key=partial(_numeric_pk_record_sort_key, pk_key),
                    )
        return normalized_list

    return value



def _normalize_json_formatter_data(data, config_path=None, tail_keys=None):
    normalisierte_daten = _normalize_numeric_pk_lists(data)

    if _jf_sortiere_json_daten is not None:
        referenz_daten = _load_json_formatter_reference_data(config_path)
        if referenz_daten is not None:
            try:
                normalisierte_daten, _ = _jf_sortiere_json_daten(
                    normalisierte_daten,
                    sort_modus="reference",
                    referenz_daten=referenz_daten,
                )
            except Exception as exc:
                logger.warning(
                    "Datastore: JSON formatter reference reorder failed, keeping numeric order: %s",
                    exc,
                )

    if tail_keys:
        normalisierte_daten = _move_top_level_sections_to_tail(normalisierte_daten, tail_keys)

    return normalisierte_daten



def _normalize_runtime_document(data, config_path=None, tail_keys=None):
    normalisierte_daten = _normalize_json_formatter_data(
        data,
        config_path=config_path,
        tail_keys=tail_keys,
    )
    _strip_forbidden_process_state_keys_inplace(normalisierte_daten)
    return normalisierte_daten



def _canonical_json_for_storage(data, config_path=None, tail_keys=None):
    normalisierte_daten = _normalize_runtime_document(
        data,
        config_path=config_path,
        tail_keys=tail_keys,
    )
    return _canonical_json(normalisierte_daten)



def _pretty_json_with_formatter(data, config_path=None, tail_keys=None):
    normalisierte_daten = _normalize_runtime_document(
        data,
        config_path=config_path,
        tail_keys=tail_keys,
    )

    if _jf_formatiere is not None:
        try:
            txt = _jf_formatiere(normalisierte_daten, einrueckung=2)
            if not txt.endswith("\n"):
                txt += "\n"
            return txt
        except Exception as exc:
            logger.warning("Datastore: JSON formatter failed, fallback to canonical dump: %s", exc)
    return _canonical_json(normalisierte_daten)



def _sha256_of_text(text):
    h = hashlib.sha256()
    if isinstance(text, str):
        text = text.encode(DEFAULT_ENCODING, errors="replace")
    h.update(text)
    return h.hexdigest()




# --- Lieferung 1b: optional compiler-adapter based loader ---
try:
    from src.adapters.compiler import build_adapter as _ca_build, load_config as _ca_load
except Exception:
    _ca_build = None
    _ca_load = None
_COMPILER_ADAPTER_HANDLE = None

# v17: Runtime-Change-Capture-Producer und Reverse-Compile-Consumer-Loop.
# Beide Handles werden beim Bootstrap gesetzt und sind default deaktiviert.
# Wenn die entsprechenden Config-Flags nicht aktiviert sind, sind die
# Handles inert (No-Op), der Runtime-Code wird dadurch nicht beeinflusst.
try:
    from src.adapters.compiler._runtime_change_capture import (
        build_capture_handle as _rcc_build,
        build_capture_callback as _rcc_callback_factory,
    )
except Exception:
    _rcc_build = None
    _rcc_callback_factory = None

try:
    from src.adapters.compiler._reverse_compile_loop import (
        build_loop_handle as _rcl_build,
        drain_reverse_compile_once as _rcl_drain,
    )
except Exception:
    _rcl_build = None
    _rcl_drain = None

_CAPTURE_HANDLE = None
_CAPTURE_CALLBACK = None
_REVERSE_LOOP_HANDLE = None
_REVERSE_LOOP_LAST_TICK_TS = 0.0
_REVERSE_LOOP_TICK_INTERVAL_S = 5.0


def bind_compiler_adapter(source_settings, logger_obj=None):
    """Called once by worker_datastore bootstrap. When config_source.mode is
    'cud_compiled', subsequent _read_json_file calls for the device-state
    path are intercepted and served from the compiler adapter."""
    global _COMPILER_ADAPTER_HANDLE
    if not callable(_ca_build):
        return None
    try:
        _COMPILER_ADAPTER_HANDLE = _ca_build(source_settings or {}, logger=logger_obj)
        return _COMPILER_ADAPTER_HANDLE
    except Exception as _exc:
        (logger_obj or logger).warning("[datastore] compiler adapter bind failed: %s", _exc)
        _COMPILER_ADAPTER_HANDLE = None
        return None


def bind_runtime_change_capture(source_settings, logger_obj=None):
    """v17: Bind a Runtime-Change-Capture-Producer handle.

    If runtime_change_capture_enabled is false in source_settings, this
    yields an inert (No-Op) handle. Otherwise, capture_change(...) calls
    will persist change sets to runtime_change_capture_dir.
    """
    global _CAPTURE_HANDLE, _CAPTURE_CALLBACK
    if not callable(_rcc_build) or not callable(_rcc_callback_factory):
        return None
    try:
        _CAPTURE_HANDLE = _rcc_build(
            source_settings or {},
            source_ref="worker_datastore",
            logger=logger_obj,
        )
        _CAPTURE_CALLBACK = _rcc_callback_factory(_CAPTURE_HANDLE)
        return _CAPTURE_HANDLE
    except Exception as _exc:
        (logger_obj or logger).warning(
            "[datastore] runtime change capture bind failed: %s", _exc
        )
        _CAPTURE_HANDLE = None
        _CAPTURE_CALLBACK = None
        return None


def bind_reverse_compile_loop(source_settings, logger_obj=None):
    """v17: Bind a Reverse-Compile-Consumer-Loop handle.

    If reverse_compile_enabled is false in source_settings, this yields an
    inert (No-Op) handle. Otherwise, periodic drain ticks will consume
    captured change sets and call the adapter's reverse_compile_fn.
    """
    global _REVERSE_LOOP_HANDLE
    if not callable(_rcl_build):
        return None
    try:
        _REVERSE_LOOP_HANDLE = _rcl_build(source_settings or {}, logger=logger_obj)
        return _REVERSE_LOOP_HANDLE
    except Exception as _exc:
        (logger_obj or logger).warning(
            "[datastore] reverse compile loop bind failed: %s", _exc
        )
        _REVERSE_LOOP_HANDLE = None
        return None


def _capture_commit(payload, envelope_id=None, revision=None):
    """Internal wrapper called from the commit point. No-op if handle is inert."""
    callback = _CAPTURE_CALLBACK
    if callback is None:
        return None
    try:
        return callback(payload, envelope_id=envelope_id, revision=revision)
    except Exception as _exc:
        logger.warning("[datastore] capture_change failed: %s", _exc)
        return None


def _drain_reverse_compile_tick():
    """Periodic drain tick. Called from the main loop. Rate-limited and No-op
    if the loop handle is disabled or the adapter lacks reverse_compile_fn."""
    global _REVERSE_LOOP_LAST_TICK_TS
    handle = _REVERSE_LOOP_HANDLE
    if handle is None:
        return None
    if not callable(_rcl_drain):
        return None
    now = time.time()
    if now - _REVERSE_LOOP_LAST_TICK_TS < _REVERSE_LOOP_TICK_INTERVAL_S:
        return None
    _REVERSE_LOOP_LAST_TICK_TS = now
    adapter_handle = _COMPILER_ADAPTER_HANDLE
    if adapter_handle is None:
        return None
    try:
        return _rcl_drain(handle, adapter_handle, max_items=20)
    except Exception as _exc:
        logger.warning("[datastore] reverse compile drain failed: %s", _exc)
        return None

def _compiler_adapter_active():
    h = _COMPILER_ADAPTER_HANDLE
    return isinstance(h, dict) and h.get("mode") == "cud_compiled"


def _resolve_config_source_settings(runtime_config_store):
    if not isinstance(runtime_config_store, dict):
        return {}

    lock = runtime_config_store.get("lock")
    if lock is None:
        settings = runtime_config_store.get("settings") or {}
        effective_config = runtime_config_store.get("effective_config") or {}
    else:
        with lock:
            settings = copy.deepcopy(runtime_config_store.get("settings") or {})
            effective_config = copy.deepcopy(runtime_config_store.get("effective_config") or {})

    config_source = settings.get("config_source")
    if isinstance(config_source, dict):
        return config_source

    config_source = effective_config.get("config_source")
    if isinstance(config_source, dict):
        return config_source

    return {}


def _ensure_config_source_defaults(source_settings, config_path, runtime_config_store=None):
    normalized = dict(source_settings or {})

    if config_path and not normalized.get("legacy_json_path"):
        normalized["legacy_json_path"] = config_path

    if runtime_config_store and not normalized.get("project_root"):
        normalized["project_root"] = runtime_config_store.get("project_root")

    if not normalized.get("mode"):
        normalized["mode"] = "legacy_json"

    return normalized


def _read_json_file(path):
    if _compiler_adapter_active() and callable(_ca_load):
        try:
            data = _ca_load(_COMPILER_ADAPTER_HANDLE)
            import json as _json
            raw = _json.dumps(data, ensure_ascii=False)
            logger.info("[datastore] config served via compiler adapter (bytes=%d)", len(raw))
            return data, raw
        except Exception as _exc:
            logger.error("[datastore] adapter load failed, falling back to file: %s", _exc)
    with open(path, "r", encoding=DEFAULT_ENCODING) as f:
        raw = f.read()
    if not raw or not raw.strip():
        raise ValueError("File is empty or whitespace-only: {}".format(path))
    try:
        return json.loads(raw), raw
    except json.JSONDecodeError as exc:
        if _jf_reparieren is not None:
            try:
                repaired = _jf_reparieren(raw)
                data = json.loads(repaired)
                logger.warning("JSON repaired during read (%s): %s", path, exc)
                return data, raw
            except Exception:
                pass
        raise exc


def _atomic_write_text(path, text):
    target = os.path.abspath(path)
    dirn = os.path.dirname(target)
    _ensure_dir(dirn)
    tmp_path = target + ".tmp"
    old_mode = None
    if os.path.exists(target):
        try:
            old_mode = os.stat(target).st_mode
        except Exception:
            old_mode = None
    try:
        with open(tmp_path, "w", encoding=DEFAULT_ENCODING, newline="\n") as f:
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        if old_mode is not None:
            try:
                os.chmod(tmp_path, old_mode)
            except Exception:
                pass
        os.replace(tmp_path, target)
        tmp_path = None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def _backup_file(src_path, backup_dir):
    _ensure_dir(backup_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    dst_path = os.path.join(backup_dir, "{}.bak.{}".format(os.path.basename(src_path), ts))
    try:
        with open(src_path, "rb") as fsrc, open(dst_path, "wb") as fdst:
            fdst.write(fsrc.read())
        logger.info("Backup written: %s", dst_path)
        return dst_path
    except Exception as e:
        logger.error("Backup failed: %s", e, exc_info=True)
        return None


def _strip_forbidden_process_state_keys_inplace(cfg):
    ps = cfg.get("process_states") if isinstance(cfg, dict) else None
    if isinstance(ps, list):
        for st in ps:
            if isinstance(st, dict):
                for k in list(st.keys()):
                    if k in FORBIDDEN_STATE_KEYS:
                        st.pop(k, None)
    return cfg


def _copy_json_data(data):
    try:
        return copy.deepcopy(data)
    except Exception:
        return data


def _is_empty_json_container(value):
    if isinstance(value, dict):
        return len(value) == 0
    if isinstance(value, list):
        return len(value) == 0
    return False


def _looks_like_record_id_key(key):
    if not isinstance(key, str):
        return False

    text = key.strip().lower()
    if not text:
        return False

    return text == "id" or text.endswith("_id")


def _normalize_record_id_value(value):
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, str):
        text = value.strip()
        if text:
            return text

    return None


def _detect_record_pk_key(records):
    if not isinstance(records, list) or not records:
        return None

    first_record = None
    for item in records:
        if isinstance(item, dict) and item:
            first_record = item
            break

    if first_record is None:
        return None

    for key in first_record.keys():
        if not _looks_like_record_id_key(key):
            continue

        valid = True
        for record in records:
            if not isinstance(record, dict):
                valid = False
                break
            if key not in record:
                valid = False
                break
            if _normalize_record_id_value(record.get(key)) is None:
                valid = False
                break

        if valid:
            return key

    return None


def _merge_list_of_records_by_pk(primary, fallback):
    if not isinstance(primary, list) or not isinstance(fallback, list):
        return None

    pk_key = _detect_record_pk_key(primary)
    if pk_key is None:
        pk_key = _detect_record_pk_key(fallback)
    if pk_key is None:
        return None

    merged = []
    index_by_pk = {}

    for item in fallback:
        copied_item = _copy_json_data(item)
        item_pk = None
        if isinstance(item, dict):
            item_pk = _normalize_record_id_value(item.get(pk_key))
        if item_pk is None or item_pk in index_by_pk:
            merged.append(copied_item)
            continue
        index_by_pk[item_pk] = len(merged)
        merged.append(copied_item)

    for item in primary:
        copied_item = _copy_json_data(item)
        item_pk = None
        if isinstance(item, dict):
            item_pk = _normalize_record_id_value(item.get(pk_key))
        if item_pk is None:
            merged.append(copied_item)
            continue

        if item_pk in index_by_pk:
            previous_item = merged[index_by_pk[item_pk]]
            merged[index_by_pk[item_pk]] = _merge_json_prefer_primary(item, previous_item)
            continue

        index_by_pk[item_pk] = len(merged)
        merged.append(copied_item)

    return merged


def _merge_json_prefer_primary(primary, fallback):
    if primary is None:
        return _copy_json_data(fallback)
    if fallback is None:
        return _copy_json_data(primary)

    if isinstance(primary, dict) and isinstance(fallback, dict):
        merged = {}

        for key, value in fallback.items():
            if key in primary:
                merged[key] = _merge_json_prefer_primary(primary.get(key), value)
            else:
                merged[key] = _copy_json_data(value)

        for key, value in primary.items():
            if key not in merged:
                merged[key] = _copy_json_data(value)

        return merged

    if isinstance(primary, list) and isinstance(fallback, list):
        merged_list = _merge_list_of_records_by_pk(primary, fallback)
        if merged_list is not None:
            return merged_list
        if len(primary) > 0:
            return _copy_json_data(primary)
        return _copy_json_data(fallback)

    if _is_empty_json_container(primary) and not _is_empty_json_container(fallback):
        return _copy_json_data(fallback)

    return _copy_json_data(primary)


def _json_signature(value):
    return _sha256_of_text(_canonical_json(value))


def _new_passthrough_store():
    return {
        "managed_sections": {},
        "generic_sections": {},
    }



def _ensure_passthrough_store(ctx):
    store = ctx.get("passthrough_store")
    if (
        isinstance(store, dict)
        and isinstance(store.get("managed_sections"), dict)
        and isinstance(store.get("generic_sections"), dict)
    ):
        return store

    store = _new_passthrough_store()
    ctx["passthrough_store"] = store

    resources = ctx.get("resources")
    if isinstance(resources, dict):
        resources["datastore_passthrough_store"] = store

    return store



def _ensure_system_known_top_level_keys(ctx):
    known = ctx.get("system_known_top_level_keys")
    if isinstance(known, set):
        return known

    known = set()
    ctx["system_known_top_level_keys"] = known
    return known



def _remember_system_known_top_level_keys(ctx, data):
    if not isinstance(data, dict):
        return

    known = _ensure_system_known_top_level_keys(ctx)
    for key in data.keys():
        if key in _PASSTHROUGH_SECTION_RULES:
            continue
        known.add(key)



def _remember_last_file_data(ctx, data):
    ctx["last_file_data"] = _copy_json_data(data)



def _set_runtime_passthrough_from_document(ctx, data):
    if not isinstance(data, dict):
        return

    _remember_system_known_top_level_keys(ctx, data)

    store = _ensure_passthrough_store(ctx)
    managed_sections = store["managed_sections"]

    for section_name in _PASSTHROUGH_SECTION_RULES.keys():
        if section_name in data:
            managed_sections[section_name] = _copy_json_data(data.get(section_name))



def _merge_runtime_passthrough_patch(ctx, data):
    if not isinstance(data, dict):
        return

    store = _ensure_passthrough_store(ctx)
    managed_sections = store["managed_sections"]
    generic_sections = store["generic_sections"]
    known_top_level_keys = _ensure_system_known_top_level_keys(ctx)

    for section_name in _PASSTHROUGH_SECTION_RULES.keys():
        if section_name not in data:
            continue

        existing = managed_sections.get(section_name)
        if existing is None:
            managed_sections[section_name] = _copy_json_data(data.get(section_name))
            continue

        managed_sections[section_name] = _merge_json_prefer_primary(
            data.get(section_name),
            existing,
        )

    for section_name, section_value in data.items():
        if section_name in _PASSTHROUGH_SECTION_RULES:
            continue
        if section_name in known_top_level_keys:
            continue

        existing = generic_sections.get(section_name)
        if existing is None:
            generic_sections[section_name] = _copy_json_data(section_value)
            continue

        generic_sections[section_name] = _merge_json_prefer_primary(
            section_value,
            existing,
        )



def _extract_config_documents_from_message(msg):
    payload = msg.get("payload")
    if not isinstance(payload, dict):
        return []

    documents = []
    for key in _CONFIG_PAYLOAD_DOCUMENT_KEYS:
        value = payload.get(key)
        if isinstance(value, dict):
            documents.append(value)

    return documents


def _get_passthrough_section_value(ctx, section_name):
    value = None

    store = ctx.get("passthrough_store")
    if isinstance(store, dict):
        managed_sections = store.get("managed_sections")
        if isinstance(managed_sections, dict) and section_name in managed_sections:
            value = _copy_json_data(managed_sections.get(section_name))

    last_file_data = ctx.get("last_file_data")
    if isinstance(last_file_data, dict) and section_name in last_file_data:
        if value is None:
            value = _copy_json_data(last_file_data.get(section_name))
        else:
            value = _merge_json_prefer_primary(value, last_file_data.get(section_name))

    return value



def _get_generic_passthrough_sections(ctx):
    generic_sections = {}

    store = ctx.get("passthrough_store")
    if isinstance(store, dict):
        stored_generic_sections = store.get("generic_sections")
        if isinstance(stored_generic_sections, dict):
            for key, value in stored_generic_sections.items():
                generic_sections[key] = _copy_json_data(value)

    last_file_data = ctx.get("last_file_data")
    if isinstance(last_file_data, dict):
        for key, value in last_file_data.items():
            if key in _PASSTHROUGH_SECTION_RULES:
                continue
            if key in generic_sections:
                generic_sections[key] = _merge_json_prefer_primary(
                    generic_sections[key],
                    value,
                )
                continue
            generic_sections[key] = _copy_json_data(value)

    known_top_level_keys = _ensure_system_known_top_level_keys(ctx)
    filtered = {}
    for key, value in generic_sections.items():
        if key in known_top_level_keys:
            continue
        filtered[key] = value

    return filtered



def _merge_snapshot_with_runtime_extensions(ctx, master_data):
    if not isinstance(master_data, dict):
        return master_data, [], []

    _remember_system_known_top_level_keys(ctx, master_data)

    merged = _copy_json_data(master_data)
    preserved_managed_sections = []
    preserved_unknown_top_level = []

    for section_name in _PASSTHROUGH_SECTION_RULES.keys():
        passthrough_value = _get_passthrough_section_value(ctx, section_name)
        if passthrough_value is None:
            continue

        current_value = merged.get(section_name)
        if current_value is None:
            merged[section_name] = _copy_json_data(passthrough_value)
            preserved_managed_sections.append(section_name)
            continue

        merged_value = _merge_json_prefer_primary(current_value, passthrough_value)
        if _json_signature(merged_value) != _json_signature(current_value):
            preserved_managed_sections.append(section_name)
        merged[section_name] = merged_value

    generic_passthrough_sections = _get_generic_passthrough_sections(ctx)
    for key, value in generic_passthrough_sections.items():
        current_value = merged.get(key)
        if current_value is None:
            merged[key] = _copy_json_data(value)
            preserved_unknown_top_level.append(key)
            continue

        merged_value = _merge_json_prefer_primary(current_value, value)
        if _json_signature(merged_value) != _json_signature(current_value):
            preserved_unknown_top_level.append(key)
        merged[key] = merged_value

    last_file_data = ctx.get("last_file_data")
    if isinstance(last_file_data, dict):
        for key, value in last_file_data.items():
            if key in merged:
                continue
            merged[key] = _copy_json_data(value)
            preserved_unknown_top_level.append(key)

    return merged, preserved_managed_sections, preserved_unknown_top_level



def _move_top_level_sections_to_tail(data, tail_keys):
    if not isinstance(data, dict):
        return data

    moved = {}
    tail_key_set = set(tail_keys)

    for key, value in data.items():
        if key not in tail_key_set:
            moved[key] = value

    for key in tail_keys:
        if key in data:
            moved[key] = data.get(key)

    return moved


# =============================================================================
# Snapshot access (heap_sync first, legacy fallback)
# =============================================================================


def _get_master_snapshot(ctx):
    resources = ctx.get("resources")
    if isinstance(resources, dict):
        cluster = resources.get("heap_sync_cluster")
        if cluster is not None:
            cache = ctx.get("snapshot_cache")
            try:
                node = leader_node(cluster)
                if cache is not None:
                    return export_node_dataset_cached(node, cache)
                return export_node_dataset(node)
            except Exception as exc:
                logger.warning("Datastore: heap_sync export failed: %s", exc)
    config_data = ctx.get("config_data")
    config_lock = ctx.get("config_lock")
    if config_data is not None and config_lock is not None:
        with config_lock:
            return copy.deepcopy(config_data)
    return {}


def _replace_master_direct(ctx, new_data):
    config_data = ctx.get("config_data")
    config_lock = ctx.get("config_lock")
    if config_data is not None and config_lock is not None:
        with config_lock:
            config_data.clear()
            config_data.update(copy.deepcopy(new_data))


# =============================================================================
# Context creation
# =============================================================================


def create_datastore_context(
    *,
    config_data,
    config_lock,
    queue_event_send,
    queue_event_ds,
    node_id=None,
    shutdown_event=None,
    root_dir=None,
    min_sync_interval_s=DEFAULT_MIN_SYNC_INTERVAL_S,
    resources=None,
):
    rd, config_path, backup_dir = _resolve_paths(root_dir)

    file_change_event = threading.Event()

    ctx = {
        "config_data": config_data,
        "config_lock": config_lock,
        "queue_event_send": queue_event_send,
        "queue_event_ds": queue_event_ds,
        "node_id": node_id or os.environ.get("NODE_ID", "fn-01"),
        "shutdown_event": shutdown_event or threading.Event(),
        "resources": resources,
        "root_dir": rd,
        "config_path": config_path,
        "backup_dir": backup_dir,
        "min_sync_interval_s": float(min_sync_interval_s),
        "snapshot_cache": make_snapshot_cache(),

        # inotify integration
        "file_change_event": file_change_event,
        "file_watch_thread": None,

        # Hash tracking
        "last_file_hash": None,
        "last_file_text_hash": None,
        "last_file_mtime": None,
        "last_file_read_ts": 0.0,
        "last_file_write_ts": 0.0,
        "self_write_hash": None,
        "last_file_data": None,

        # Runtime passthrough for config extensions / future schema additions
        "passthrough_store": _new_passthrough_store(),
        "system_known_top_level_keys": set(),

        # Dirty flag
        "dirty_system_to_file": False,
    }

    if isinstance(resources, dict):
        resources["datastore_passthrough_store"] = ctx["passthrough_store"]

    return ctx


# =============================================================================
# Bootstrap
# =============================================================================


def _initial_backup_and_load(ctx):
    path = ctx["config_path"]
    if not os.path.exists(path):
        raise FileNotFoundError("Config file not found: {}".format(path))

    _ensure_dir(ctx["backup_dir"])
    _backup_file(path, ctx["backup_dir"])

    data, raw = _read_json_file(path)
    data = _normalize_runtime_document(
        data,
        config_path=ctx["config_path"],
        tail_keys=_PASSTHROUGH_TAIL_KEYS,
    )

    cano = _canonical_json_for_storage(
        data,
        ctx["config_path"],
        tail_keys=_PASSTHROUGH_TAIL_KEYS,
    )
    h = _sha256_of_text(cano)

    ctx["last_file_text_hash"] = _sha256_of_text(raw)
    _replace_master_direct(ctx, data)
    _remember_last_file_data(ctx, data)
    _set_runtime_passthrough_from_document(ctx, data)

    ctx["last_file_hash"] = h
    try:
        ctx["last_file_mtime"] = os.path.getmtime(path)
    except Exception:
        ctx["last_file_mtime"] = time.time()
    ctx["last_file_read_ts"] = time.time()

    try:
        pretty = _pretty_json_with_formatter(
            data,
            ctx["config_path"],
            tail_keys=_PASSTHROUGH_TAIL_KEYS,
        )
        if _sha256_of_text(pretty) != ctx["last_file_text_hash"]:
            ctx["dirty_system_to_file"] = True
            ctx["last_file_write_ts"] = 0.0
            logger.info("Datastore: Startup format drift — scheduling re-format")
    except Exception:
        pass

    logger.info("Datastore: Initial config loaded (%d bytes)", len(cano))


# =============================================================================
# File watch cycle (called when inotify fires OR as fallback)
# =============================================================================


def _file_watch_cycle(ctx):
    config_path = ctx["config_path"]

    if not os.path.exists(config_path):
        if ctx["last_file_mtime"] is not None:
            ctx["dirty_system_to_file"] = True
            ctx["last_file_write_ts"] = 0.0
            ctx["last_file_mtime"] = None
            ctx["last_file_text_hash"] = None
            logger.warning("Datastore: Config file missing — scheduling reconstruction")
        return False

    # Read file with retry
    data = None
    raw = None
    for retry in range(FILE_WATCH_EMPTY_MAX_RETRIES + 1):
        try:
            data, raw = _read_json_file(config_path)
            break
        except ValueError:
            if retry < FILE_WATCH_EMPTY_MAX_RETRIES:
                time.sleep(FILE_WATCH_EMPTY_RETRY_DELAY_S)
            else:
                return False
        except json.JSONDecodeError as jde:
            if retry < 1:
                time.sleep(FILE_WATCH_EMPTY_RETRY_DELAY_S)
            else:
                logger.error("Datastore: JSON parse error: %s", jde)
                return False

    if data is None:
        return False

    data = _normalize_runtime_document(
        data,
        config_path=config_path,
        tail_keys=_PASSTHROUGH_TAIL_KEYS,
    )

    raw_hash = _sha256_of_text(raw)
    cano = _canonical_json_for_storage(
        data,
        config_path,
        tail_keys=_PASSTHROUGH_TAIL_KEYS,
    )
    new_hash = _sha256_of_text(cano)
    old_hash = ctx["last_file_hash"]

    try:
        ctx["last_file_mtime"] = os.path.getmtime(config_path)
    except Exception:
        pass
    ctx["last_file_read_ts"] = time.time()
    ctx["last_file_text_hash"] = raw_hash
    _remember_last_file_data(ctx, data)
    _set_runtime_passthrough_from_document(ctx, data)
    _replace_master_direct(ctx, data)

    # Own write? Skip
    if ctx["self_write_hash"] is not None and new_hash == ctx["self_write_hash"]:
        ctx["last_file_hash"] = new_hash
        ctx["self_write_hash"] = None
        return False

    # Format-only change?
    if old_hash is not None and new_hash == old_hash:
        expected = _pretty_json_with_formatter(
            data,
            config_path,
            tail_keys=_PASSTHROUGH_TAIL_KEYS,
        )
        if _sha256_of_text(expected) != raw_hash:
            ctx["dirty_system_to_file"] = True
            ctx["last_file_write_ts"] = 0.0
            logger.info("Datastore: External format change — scheduling re-format")
        return False

    # Semantic change: emit CONFIG_REPLACE
    ctx["last_file_hash"] = new_hash
    evt = make_event(
        "CONFIG_REPLACE",
        payload={
            "new_config": data,
            "reason": "file_changed",
            "config_name": "device_state_data",
        },
        target="state_event_management",
    )
    evt["source"] = "datastore"
    sync_queue_put(ctx["queue_event_send"], evt)
    logger.info("Datastore: CONFIG_REPLACE emitted (File -> System)")
    return True


# =============================================================================
# Event processing
# =============================================================================


def _config_event_payload(msg):
    if isinstance(msg, dict):
        payload = msg.get("payload")
        if isinstance(payload, dict):
            return payload
    return {}


def _config_event_version(msg):
    payload = _config_event_payload(msg)
    try:
        return int(payload.get("version"))
    except Exception:
        return None


def _is_committed_config_event(msg):
    if not isinstance(msg, dict):
        return False

    et = str(msg.get("event_type") or "").upper()
    if et in ("CONFIG_PATCH_APPLIED", "CONFIG_REPLACE_APPLIED"):
        return True
    if et == "CONFIG_CHANGED":
        return _config_event_version(msg) is not None
    return False


def _process_event(ctx, msg):
    if not isinstance(msg, dict):
        return
    if str(msg.get("source", "")).lower() == "datastore":
        return

    et = str(msg.get("event_type") or "").upper()
    if et not in _CONFIG_EVENT_TYPES:
        return

    for document in _extract_config_documents_from_message(msg):
        _merge_runtime_passthrough_patch(ctx, document)

    if et == "DATA_LOAD_ERROR":
        logger.warning("Datastore: DATA_LOAD_ERROR — re-reading and resending config")
        try:
            data, _ = _read_json_file(ctx["config_path"])
            _strip_forbidden_process_state_keys_inplace(data)
            evt = make_event(
                "CONFIG_REPLACE",
                payload={
                    "new_config": data,
                    "reason": "data_resend_after_error",
                    "config_name": "device_state_data",
                },
                target="state_event_management",
            )
            evt["source"] = "datastore"
            sync_queue_put(ctx["queue_event_send"], evt)
        except Exception as exc:
            logger.error("Datastore: Failed to resend data: %s", exc)
        return

    if not _is_committed_config_event(msg):
        logger.debug("Datastore: ignore pre-commit config event for System -> File sync (%s)", et)
        return

    ctx["dirty_system_to_file"] = True
    if et in ("CONFIG_PATCH_APPLIED", "CONFIG_REPLACE_APPLIED", "CONFIG_CHANGED"):
        logger.info(
            "Datastore: %s received — dirty marked (version=%s)",
            et,
            _config_event_version(msg),
        )


# =============================================================================
# Periodic file writer
# =============================================================================


def _periodic_write_cycle(ctx):
    if not ctx["dirty_system_to_file"]:
        return
    if time.time() - ctx["last_file_write_ts"] < ctx["min_sync_interval_s"]:
        return

    data = _get_master_snapshot(ctx)
    if not data:
        return

    data, preserved_managed_sections, preserved_unknown_top_level = _merge_snapshot_with_runtime_extensions(
        ctx,
        data,
    )
    data = _normalize_runtime_document(
        data,
        config_path=ctx["config_path"],
        tail_keys=_PASSTHROUGH_TAIL_KEYS,
    )

    if preserved_managed_sections:
        logger.info(
            "Datastore: preserving managed config extensions during write: %s",
            ", ".join(preserved_managed_sections),
        )

    if preserved_unknown_top_level:
        logger.info(
            "Datastore: preserving unknown top-level config sections during write: %s",
            ", ".join(preserved_unknown_top_level),
        )

    canonical = _canonical_json_for_storage(
        data,
        ctx["config_path"],
        tail_keys=_PASSTHROUGH_TAIL_KEYS,
    )
    new_hash = _sha256_of_text(canonical)
    pretty = _pretty_json_with_formatter(
        data,
        ctx["config_path"],
        tail_keys=_PASSTHROUGH_TAIL_KEYS,
    )
    pretty_hash = _sha256_of_text(pretty)

    semantic_changed = ctx["last_file_hash"] is None or new_hash != ctx["last_file_hash"]
    format_changed = ctx["last_file_text_hash"] is None or pretty_hash != ctx["last_file_text_hash"]
    file_missing = not os.path.exists(ctx["config_path"])

    if not (file_missing or semantic_changed or format_changed):
        ctx["dirty_system_to_file"] = False
        return

    old_hash = ctx["last_file_hash"]
    _atomic_write_text(ctx["config_path"], pretty)

    ctx["last_file_write_ts"] = time.time()
    try:
        ctx["last_file_mtime"] = os.path.getmtime(ctx["config_path"])
    except Exception:
        ctx["last_file_mtime"] = ctx["last_file_write_ts"]
    ctx["last_file_hash"] = new_hash
    ctx["last_file_text_hash"] = pretty_hash
    ctx["self_write_hash"] = new_hash
    _replace_master_direct(ctx, data)
    _remember_last_file_data(ctx, data)
    _set_runtime_passthrough_from_document(ctx, data)
    ctx["dirty_system_to_file"] = False

    if not semantic_changed:
        logger.debug("Datastore: File re-formatted (no semantic change)")
        return

    try:
        evt = make_event(
            EVENT_NAME_CONFIG_CHANGED,
            payload={
                "config_name": "device_state_data",
                "old_hash": old_hash,
                "new_hash": new_hash,
                "reason": "system_synced_to_file",
                "node_id": ctx["node_id"],
            },
        )
        evt["target"] = "event"
        evt["source"] = "datastore"
        sync_queue_put(ctx["queue_event_send"], evt)
        logger.info("Datastore: File written (System -> File, semantic change)")
    except Exception as e:
        logger.warning("Datastore: Post-write event failed: %s", e)

    # v17: Capture the semantic change as a runtime change-set. If capture is
    # disabled (default), this is a No-op. The capture payload describes the
    # hash transition; the reverse-compile consumer can later materialize the
    # corresponding CUD delta from it.
    _capture_commit(
        payload={
            "event": "config_changed",
            "config_name": "device_state_data",
            "old_hash": old_hash,
            "new_hash": new_hash,
            "node_id": ctx.get("node_id"),
            "reason": "system_synced_to_file",
        },
        revision=new_hash,
    )


# =============================================================================
# Main loop (unified, two event sources)
# =============================================================================


def _datastore_main_loop(ctx):
    """
    Unified main loop with two event sources:

    1. ``queue_event_ds`` — CONFIG events from IO-Broker (blocking, timeout 1s)
    2. ``file_change_event`` — inotify kernel notification (set by daemon thread)

    The loop blocks on the queue.  When the queue times out (no events),
    it checks the file_change_event.  When inotify fires, the event is
    already set and gets picked up in the next cycle — worst case 1s later.
    In practice, under load, events flow continuously and the file_change
    is checked every cycle.
    """
    queue_event_ds = ctx["queue_event_ds"]
    shutdown_event = ctx["shutdown_event"]
    file_change_event = ctx["file_change_event"]

    logger.info(
        "Datastore: Main loop started (v3.1, inotify=%s)",
        "active" if _INOTIFY_AVAILABLE else "fallback",
    )

    while shutdown_event is None or not shutdown_event.is_set():
        # 1. Event consumer (blocking with short timeout)
        try:
            msg = sync_queue_get(queue_event_ds)
        except Exception as exc:
            logger.error("Datastore: Queue read error: %s", exc)
            time.sleep(0.1)
            msg = None

        if msg is not None:
            if _is_stop_message(msg):
                logger.info("Datastore: STOP sentinel received")
                break
            _process_event(ctx, msg)

        # 2. File change (from inotify thread or fallback)
        if file_change_event.is_set():
            file_change_event.clear()
            try:
                _file_watch_cycle(ctx)
            except Exception as exc:
                logger.error("Datastore: File watch error: %s", exc, exc_info=True)

        # 3. Periodic writer
        try:
            _periodic_write_cycle(ctx)
        except Exception as exc:
            logger.error("Datastore: Write cycle error: %s", exc, exc_info=True)

        # 4. v17: Reverse-Compile drain tick (rate-limited, No-op if disabled)
        try:
            _drain_reverse_compile_tick()
        except Exception as exc:
            logger.error("Datastore: Reverse compile tick error: %s", exc, exc_info=True)

    # Final flush
    logger.info("Datastore: Shutdown — final flush")
    try:
        ctx["dirty_system_to_file"] = True
        ctx["last_file_write_ts"] = 0.0
        _periodic_write_cycle(ctx)
    except Exception as exc:
        logger.error("Datastore: Final flush failed: %s", exc)

    logger.info("Datastore: Main loop stopped")


# =============================================================================
# Thread entry point
# =============================================================================


def run_worker_datastore(
    node_id,
    queue_event_send,
    queue_event_ds,
    config_data,
    config_lock,
    shutdown_event,
    resources=None,
    **unused_kwargs,
):
    """Thread entry for the Datastore Worker v3.1."""
    try:
        logger.info("Starting Worker Datastore (v3.2 sync, inotify file-watch)")

        ctx = create_datastore_context(
            config_data=config_data,
            config_lock=config_lock,
            queue_event_send=queue_event_send,
            queue_event_ds=queue_event_ds,
            node_id=node_id,
            shutdown_event=shutdown_event,
            resources=resources,
        )

        runtime_config_store = unused_kwargs.get("runtime_config_store")
        source_settings = _resolve_config_source_settings(runtime_config_store)
        source_settings = _ensure_config_source_defaults(
            source_settings,
            ctx.get("config_path"),
            runtime_config_store=runtime_config_store,
        )

        if source_settings:
            bind_compiler_adapter(source_settings, logger_obj=logger)
            # v17: Producer und Consumer binden. Beide sind bei deaktivierten
            # Flags No-Ops und beeinflussen den Runtime-Pfad nicht.
            bind_runtime_change_capture(source_settings, logger_obj=logger)
            bind_reverse_compile_loop(source_settings, logger_obj=logger)
            # v18: Event-Envelope-Enrichment im Broker aktivieren (default off).
            # Der Router wird nur dann aktiv Envelope-Felder ergänzen, wenn
            # event_envelope_enrichment_enabled: true gesetzt ist.
            try:
                from src.core.sync_event_router import bind_envelope_enrichment
                bind_envelope_enrichment(
                    enabled=bool(source_settings.get("event_envelope_enrichment_enabled")),
                    source_ref=source_settings.get("event_envelope_source_ref", "sync_event_router"),
                    node_id=source_settings.get("event_envelope_node_id", "node_main"),
                )
            except Exception as _exc:
                logger.warning("[datastore] envelope enrichment bind failed: %s", _exc)
            logger.info(
                "[datastore] config source bound: mode=%s legacy_json_path=%s cud_source_dir=%s",
                source_settings.get("mode"),
                source_settings.get("legacy_json_path"),
                source_settings.get("cud_source_dir"),
            )

        _initial_backup_and_load(ctx)

        # Start inotify file watch daemon thread
        ctx["file_watch_thread"] = _start_file_watch_thread(
            ctx["config_path"],
            ctx["file_change_event"],
            ctx["shutdown_event"],
        )

        _datastore_main_loop(ctx)

    except Exception as e:
        logger.error("Critical Datastore error: %s", e, exc_info=True)
    finally:
        logger.info("Worker Datastore stopped")
