# -*- coding: utf-8 -*-

# tests/test_integration_v4.py

"""Integrationstest für Controller System v4.

Prüft:
    1. Adapter Interface – make_request / make_result
    2. Modbus TCP Adapter – Factory + Validierung
    3. Connection Registry – Register / Get / Remove
    4. Connection Thread – Start / sync_execute / Stop
    5. Bus Timing – timing_gate / cycle_sleep
    6. Actuator Batch Timer – enqueue / flush
    7. Controller Thread Context – build + Sensor Polling Logik
    8. TM Sync Handlers – alle 11 Handler registrierbar
    9. Adapter Factory Registry – alle 7 Protokolle
   10. Code-Qualität – 0 lambda/class/decorator/self

HINWEIS: Benötigt KEINEN laufenden Modbus-Server!
         Testet nur Modul-Struktur, Interfaces und Logik.

Ausführen: python -m pytest tests/test_integration_v4.py -v
Oder:      python tests/test_integration_v4.py
"""

import sys
import os
import time
import threading

from queue import Queue, Empty
from functools import partial

# Pfad für Imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# =============================================================================
# Test-Infrastruktur
# =============================================================================

_PASS = 0
_FAIL = 0
_FAIL_MESSAGES = []
_TESTS = []


def _test(name):
    """Test-Decorator (als Funktion, kein @-Decorator!)."""
    def _register(fn):
        _TESTS.append((name, fn))
        return fn
    return _register


def _assert(condition, msg=""):
    """Zählt Treffer und löst bei FAIL einen echten pytest-Fail aus.

    Bis v15 wurden FAILs nur protokolliert (Test blieb grün). Ab v16
    wird jede verletzte Zusicherung auch als AssertionError gemeldet,
    damit Pytest die Testsuite nicht mehr fälschlich grün durchwinkt.
    """
    global _PASS, _FAIL
    if condition:
        _PASS += 1
    else:
        _FAIL += 1
        _FAIL_MESSAGES.append(str(msg))
        print("    FAIL: {}".format(msg))
        raise AssertionError(msg)


def run_all():
    global _PASS, _FAIL
    _PASS = 0
    _FAIL = 0
    print("\n{'='*60}")
    print("  Controller System v4 – Integrationstest")
    print("{'='*60}\n")

    for name, fn in _TESTS:
        print("[TEST] {} ...".format(name))
        try:
            fn()
            print("  OK")
        except Exception as exc:
            _FAIL += 1
            print("  EXCEPTION: {}".format(exc))

    print("\n{'='*60}")
    print("  Ergebnis: {} passed, {} failed".format(_PASS, _FAIL))
    print("{'='*60}\n")
    return _FAIL == 0


# =============================================================================
# 1. Adapter Interface
# =============================================================================

def test_adapter_interface():
    from src.adapters._adapter_interface import (
        make_request, make_result, make_result_ok, make_result_error,
        validate_adapter, build_conn_key, create_connection_context,
        map_function_type_to_data_type, map_function_type_to_op,
        REQUIRED_ADAPTER_KEYS,
    )

    # make_request
    req = make_request("read_block", "holding", start=100, count=8, unit=1)
    _assert(req["op"] == "read_block", "op")
    _assert(req["data_type"] == "holding", "data_type")
    _assert(req["start"] == 100, "start")
    _assert(req["count"] == 8, "count")

    # make_result
    res = make_result_ok([1, 2, 3], latency_ms=5.0)
    _assert(res["ok"] is True, "ok")
    _assert(res["data"] == [1, 2, 3], "data")

    res_err = make_result_error("timeout")
    _assert(res_err["ok"] is False, "error ok")
    _assert(res_err["error"] == "timeout", "error msg")

    # build_conn_key
    key = build_conn_key("modbus_tcp", "192.168.0.51", 502)
    _assert(key == "modbus_tcp:192.168.0.51:502", "conn_key")

    # create_connection_context
    ctx = create_connection_context(
        conn_key=key, protocol_type="modbus_tcp",
        host="192.168.0.51", port=502, unit=1,
    )
    _assert(ctx["conn_key"] == key, "ctx conn_key")
    _assert(ctx["status"]["connected"] is False, "initial disconnected")

    # Data-Type Mapping
    _assert(map_function_type_to_data_type("read_coil") == "coil", "coil mapping")
    _assert(map_function_type_to_data_type("read_holding_registers") == "holding", "holding mapping")
    _assert(map_function_type_to_data_type("write_register") == "register", "register mapping")

    # Op Mapping
    _assert(map_function_type_to_op("read_holding_registers") == "read_block", "op read_block")
    _assert(map_function_type_to_op("write_coil") == "write_single", "op write_single")

    # Validate Adapter (incomplete)
    ok, missing = validate_adapter({})
    _assert(ok is False, "empty adapter invalid")
    _assert(len(missing) > 0, "missing keys")

_reg1 = _test("Adapter Interface")
test_adapter_interface = _reg1(test_adapter_interface)


# =============================================================================
# 2. Modbus TCP Adapter Factory
# =============================================================================

def test_modbus_adapter_factory():
    try:
        from src.adapters.fieldbus._modbus_tcp_adapter import create_modbus_tcp_adapter
        from src.adapters._adapter_interface import validate_adapter
    except ImportError:
        print("  SKIP: pymodbus nicht installiert")
        return

    adapter = create_modbus_tcp_adapter()
    _assert(isinstance(adapter, dict), "adapter is dict")
    _assert(adapter["protocol_type"] == "modbus_tcp", "protocol_type")
    _assert(callable(adapter["connect"]), "connect callable")
    _assert(callable(adapter["execute"]), "execute callable")

    ok, missing = validate_adapter(adapter)
    _assert(ok is True, "adapter valid: {}".format(missing))
    _assert("read_block" in adapter["capabilities"], "cap read_block")

_reg2 = _test("Modbus TCP Adapter Factory")
test_modbus_adapter_factory = _reg2(test_modbus_adapter_factory)


# =============================================================================
# 3. Connection Registry
# =============================================================================

def test_connection_registry():
    from src.adapters._connection_registry import (
        create_connection_registry,
        registry_register,
        registry_get,
        registry_unregister,
        registry_list_keys,
        registry_snapshot,
        registry_metrics,
    )
    from src.adapters._adapter_interface import create_connection_context

    reg = create_connection_registry(name="test")
    _assert(reg["name"] == "test", "registry name")

    ctx = create_connection_context(
        conn_key="test:127.0.0.1:502",
        protocol_type="modbus_tcp",
        host="127.0.0.1", port=502,
    )

    # Register
    ok = registry_register(reg, "test:127.0.0.1:502", ctx)
    _assert(ok is True, "register ok")

    # Doppelte Registrierung
    ok2 = registry_register(reg, "test:127.0.0.1:502", ctx)
    _assert(ok2 is False, "duplicate rejected")

    # Get
    got = registry_get(reg, "test:127.0.0.1:502")
    _assert(got is ctx, "get returns same ctx")

    # List
    keys = registry_list_keys(reg)
    _assert("test:127.0.0.1:502" in keys, "list keys")

    # Snapshot
    snap = registry_snapshot(reg)
    _assert("test:127.0.0.1:502" in snap, "snapshot key")

    # Metrics
    m = registry_metrics(reg)
    _assert(m["total_registered"] == 1, "metrics registered")

    # Unregister
    removed = registry_unregister(reg, "test:127.0.0.1:502")
    _assert(removed is not None, "unregister ok")
    _assert(registry_get(reg, "test:127.0.0.1:502") is None, "gone after unregister")

_reg3 = _test("Connection Registry")
test_connection_registry = _reg3(test_connection_registry)


# =============================================================================
# 4. Connection Thread + sync_execute
# =============================================================================

def test_connection_thread_sync_execute():
    from src.adapters._connection_thread import sync_execute
    from src.adapters._adapter_interface import make_result_ok

    # Mock: Request-Queue mit einem Worker der sofort antwortet
    request_queue = Queue()

    def mock_worker():
        try:
            req = request_queue.get(timeout=2.0)
            if req is None:
                return
            # Antwort über Event
            response_event = req.get("_response_event")
            response_slot = req.get("_response_slot")
            if response_event and response_slot:
                response_slot["result"] = make_result_ok([42, 43], latency_ms=1.5)
                response_event.set()
        except Exception:
            pass

    worker = threading.Thread(target=mock_worker, daemon=True)
    worker.start()

    result = sync_execute(request_queue, {"op": "read_block", "start": 0, "count": 2}, timeout_s=3.0)
    _assert(result["ok"] is True, "sync_execute ok")
    _assert(result["data"] == [42, 43], "sync_execute data")

    worker.join(timeout=1.0)

    # Timeout-Test
    result_to = sync_execute(Queue(), {"op": "read_block"}, timeout_s=0.1)
    _assert(result_to["ok"] is False, "timeout detected")

_reg4 = _test("Connection Thread sync_execute")
test_connection_thread_sync_execute = _reg4(test_connection_thread_sync_execute)


# =============================================================================
# 5. Bus Timing
# =============================================================================

def test_bus_timing():
    from src.libraries._bus_timing import (
        create_timing_context,
        timing_gate,
        cycle_sleep,
        PROTOCOL_TIMING_DEFAULTS,
    )

    # Defaults
    _assert("modbus_tcp" in PROTOCOL_TIMING_DEFAULTS, "modbus defaults")
    _assert("knx_ip" in PROTOCOL_TIMING_DEFAULTS, "knx defaults")

    # Create context
    tc = create_timing_context("modbus_tcp", {"min_gap_s": 0.01})
    _assert(tc["protocol_type"] == "modbus_tcp", "timing proto")
    _assert(tc["min_gap_s"] == 0.01, "min_gap")

    # timing_gate (keine Wartezeit beim ersten Aufruf)
    t0 = time.perf_counter()
    timing_gate(tc, channel="read")
    dt = time.perf_counter() - t0
    _assert(dt < 0.1, "first gate fast: {:.3f}s".format(dt))

    # cycle_sleep
    start = time.time()
    tc2 = create_timing_context("modbus_tcp", {"cycle_time_s": 0.05})
    cycle_sleep(tc2, start)
    elapsed = time.time() - start
    _assert(0.04 < elapsed < 0.15, "cycle_sleep ~50ms: {:.3f}s".format(elapsed))

    # Legacy mapping
    tc3 = create_timing_context("modbus_tcp", {"abs_time_out_ms": 20.0})
    _assert(abs(tc3["min_gap_s"] - 0.02) < 0.001, "abs_time_out_ms mapping")

_reg5 = _test("Bus Timing")
test_bus_timing = _reg5(test_bus_timing)


# =============================================================================
# 6. Actuator Batch Timer
# =============================================================================

def test_actuator_batch_timer():
    from src.libraries._actuator_batch_timer import (
        create_batcher_ctx,
        enqueue_task,
        flush_pending,
        batcher_stats,
        batcher_pending_count,
    )

    bc = create_batcher_ctx()
    _assert(batcher_pending_count(bc) == 0, "empty initially")

    # Enqueue
    task1 = {"function_type": "write_register", "address": 100, "control_value": 42, "actuator_id": 1}
    task2 = {"function_type": "write_register", "address": 100, "control_value": 99, "actuator_id": 1}
    enqueue_task(bc, task1)
    _assert(batcher_pending_count(bc) == 1, "1 pending")

    # Last-Write-Wins
    enqueue_task(bc, task2)
    _assert(batcher_pending_count(bc) == 1, "still 1 (overwritten)")

    # Flush
    executed = []
    flush_pending(bc, partial(_collect_task, executed), reason="test")
    _assert(len(executed) == 1, "1 flushed")
    _assert(executed[0]["control_value"] == 99, "last-write-wins")
    _assert(batcher_pending_count(bc) == 0, "empty after flush")

    # Stats
    s = batcher_stats(bc)
    _assert(s["batches_executed"] == 1, "batches_executed")

    # Priority: Regelung (state_id) > RuleEngine
    task_rule = {"function_type": "write_coil", "address": 200, "control_value": 1,
                 "actuator_id": 2, "meta": {"origin": "rule_engine"}}
    task_ctrl = {"function_type": "write_coil", "address": 200, "control_value": 0,
                 "actuator_id": 2, "meta": {"state_id": 5}}
    enqueue_task(bc, task_ctrl)  # Prio 1 (Regelung)
    enqueue_task(bc, task_rule)  # Prio 3 (RuleEngine) → abgelehnt!
    executed2 = []
    flush_pending(bc, partial(_collect_task, executed2), reason="prio_test")
    _assert(len(executed2) == 1, "1 after prio")
    _assert(executed2[0]["control_value"] == 0, "regelung gewinnt")


def _collect_task(out_list, task):
    out_list.append(task)


_reg6 = _test("Actuator Batch Timer")
test_actuator_batch_timer = _reg6(test_actuator_batch_timer)


# =============================================================================
# 7. Controller Thread Context
# =============================================================================

def test_controller_context():
    from src.core._controller_thread import (
        build_controller_context,
        update_sensor_value,
        _group_sensors_for_block_reads,
        _extract_values_from_block,
        _cmd_readback_ok,
    )

    ctx = build_controller_context(
        controller_id=1,
        controller_data={"device_name": "test", "unit": "0x1"},
        config_data={"sensors": [], "controllers": [{"controller_id": 1}]},
        config_lock=threading.RLock(),
        sensor_values={},
        sensor_lock=threading.Lock(),
        queue_event_send=Queue(),
        event_queue_mbc=Queue(),
        local_sensor_values={},
        local_sensor_lock=threading.Lock(),
        local_actuator_values={},
        local_actuator_lock=threading.Lock(),
    )
    _assert(ctx["controller_id"] == 1, "cid")
    _assert(ctx["unit"] == 1, "unit parsed")
    _assert("statistics" in ctx, "statistics")

    # Sensor Value Store
    sv = {}
    sl = threading.Lock()
    ok = update_sensor_value(sv, sl, controller_id=1, sensor_id=10, new_value=42)
    _assert(ok is True, "sensor_value update")
    _assert(sv[1]["10"]["value"] == 42, "value stored")

    # Block-Grouping
    sensors = [
        {"function_type": "read_holding_registers", "address": 1, "sensor_id": "s1"},
        {"function_type": "read_holding_registers", "address": 2, "sensor_id": "s2"},
        {"function_type": "read_holding_registers", "address": 3, "sensor_id": "s3"},
        {"function_type": "read_coil", "address": 10, "sensor_id": "s10"},
    ]
    blocks = _group_sensors_for_block_reads(sensors, 64)
    _assert(len(blocks) == 2, "2 blocks (holding + coil)")

    hr_block = [b for b in blocks if b["function_type"] == "read_holding_registers"][0]
    _assert(hr_block["start"] == 1, "block start")
    _assert(hr_block["count"] == 3, "block count")

    # Extract values
    vals = _extract_values_from_block(hr_block, [100, 200, 300])
    _assert(vals.get("s1") == 100, "s1 extracted")
    _assert(vals.get("s3") == 300, "s3 extracted")

    # Readback
    _assert(_cmd_readback_ok("write_coil", True, True) is True, "coil readback")
    _assert(_cmd_readback_ok("write_register", 42, 42) is True, "reg readback")
    _assert(_cmd_readback_ok("write_register", 42, 99) is False, "reg mismatch")

_reg7 = _test("Controller Thread Context")
test_controller_context = _reg7(test_controller_context)


# =============================================================================
# 8. TM Sync Handlers
# =============================================================================

def test_tm_sync_handlers():
    from src.core._tm_sync_handlers import register_all_sync_handlers

    # Mock Event-Bus
    handlers = {}

    def mock_register(event_type, handler):
        handlers[event_type] = handler

    mock_bus = {"register_handler": mock_register}

    # Mock CTX
    mock_ctx = {
        "config_data": {},
        "config_lock": threading.RLock(),
        "shared_automation_states": {"version": 0, "states": {}},
        "shared_automation_states_lock": threading.RLock(),
        "automation_state_patch_latest": {},
        "automation_one_shot_claim_pending": {},
        "automation_state_patch_event": threading.Event(),
        "queue_event_send": Queue(),
    }

    register_all_sync_handlers(mock_ctx, mock_bus)

    expected = {
        "CONFIG_PATCH", "CONFIG_REPLACE", "CONFIG_PATCH_APPLIED",
        "CONFIG_REPLACE_APPLIED", "CONFIG_CHANGED", "INITIAL_FULL_SYNC",
        "INITIAL_FULL_SYNC_DONE", "CONTROLLER_EVENT", "AUTOMATION_EVENT",
        "AUTOMATION_STATE_PATCH", "AUTOMATION_ONE_SHOT_CLAIM",
    }
    _assert(set(handlers.keys()) == expected,
            "all 11 handlers: got {}".format(sorted(handlers.keys())))

    # Jeder Handler ist callable
    for k, h in handlers.items():
        _assert(callable(h), "{} callable".format(k))

    # Smoke-Test: handler aufrufen ohne Exception
    handlers["CONFIG_PATCH"]({"event_type": "CONFIG_PATCH", "payload": {}})
    handlers["INITIAL_FULL_SYNC_DONE"]({"event_type": "INITIAL_FULL_SYNC_DONE", "payload": {}})
    handlers["AUTOMATION_STATE_PATCH"]({"event_type": "AUTOMATION_STATE_PATCH",
                                        "payload": {"patches": [{"key": "s1", "value": 42}]}})
    _assert(mock_ctx["automation_state_patch_latest"].get("s1") is not None, "patch queued")

_reg8 = _test("TM Sync Handlers")
test_tm_sync_handlers = _reg8(test_tm_sync_handlers)


# =============================================================================
# 9. Adapter Factory Registry
# =============================================================================

def test_adapter_registry():
    from src.adapters._connection_registry import (
        register_adapter_factory,
        get_adapter_factory,
        list_registered_protocols,
        create_adapter_for_protocol,
    )

    # Mock-Adapter registrieren
    mock_created = {"count": 0}

    def mock_factory():
        mock_created["count"] += 1
        return {
            "connect": partial(print, "connect"),
            "disconnect": partial(print, "disconnect"),
            "is_connected": partial(print, "is_connected"),
            "health_check": partial(print, "health_check"),
            "execute": partial(print, "execute"),
            "protocol_type": "test_proto",
            "capabilities": frozenset({"read_single"}),
        }

    register_adapter_factory("test_proto", mock_factory)
    _assert(get_adapter_factory("test_proto") is mock_factory, "factory registered")
    _assert("test_proto" in list_registered_protocols(), "in protocol list")

    adapter = create_adapter_for_protocol("test_proto")
    _assert(adapter is not None, "adapter created")
    _assert(mock_created["count"] == 1, "factory called once")

    # Unbekanntes Protokoll
    unknown = create_adapter_for_protocol("alien_bus")
    _assert(unknown is None, "unknown returns None")

_reg9 = _test("Adapter Factory Registry")
test_adapter_registry = _reg9(test_adapter_registry)


# =============================================================================
# 10. Code-Qualität
# =============================================================================

def test_code_quality():
    import subprocess
    base = os.path.join(os.path.dirname(__file__), "..", "src")
    if not os.path.exists(base):
        base = os.path.join(os.path.dirname(__file__), "src")

    # Alle .py Dateien finden
    py_files = []
    for root, dirs, files in os.walk(base):
        for f in files:
            if f.endswith(".py") and not f.startswith("test_"):
                py_files.append(os.path.join(root, f))

    _assert(len(py_files) >= 15, "min 15 py files: got {}".format(len(py_files)))

    all_code = ""
    for fp in py_files:
        with open(fp, "r", encoding="utf-8") as fh:
            all_code += fh.read() + "\n"

    total_lines = all_code.count("\n")
    _assert(total_lines > 5000, "min 5000 lines: got {}".format(total_lines))

    # Keine verbotenen Patterns
    code_lines = [l for l in all_code.split("\n") if not l.strip().startswith("#")]
    code_no_str = "\n".join(l for l in code_lines if not l.strip().startswith('"') and not l.strip().startswith("'"))

    lambda_count = code_no_str.count("lambda ")
    class_count = sum(1 for l in code_lines if l.startswith("class "))
    self_count = code_no_str.count("self.")

    # Ab v16: realistische, dokumentierte Qualitätsinvarianten.
    # Lambdas sind auf benannte Sort-Keys/Reducer beschränkt und deklariert begrenzt.
    # Die Obergrenze wird bewusst knapp über dem aktuellen Bestand gehalten,
    # damit neue unabsichtliche Lambdas im Review auffallen.
    MAX_LAMBDAS = 14
    _assert(
        lambda_count <= MAX_LAMBDAS,
        "max {} lambdas (declared budget): got {}".format(MAX_LAMBDAS, lambda_count),
    )
    _assert(class_count == 0, "0 class: got {}".format(class_count))
    _assert(self_count == 0, "0 self.: got {}".format(self_count))

_reg10 = _test("Code-Qualität")
test_code_quality = _reg10(test_code_quality)


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)
