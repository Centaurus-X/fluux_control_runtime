# -*- coding: utf-8 -*-

# tests/test_lieferung_2_3.py

"""
Unit tests for Lieferung 2 + 3:

- Per-controller config chunking (correctness, determinism, cross-domain)
- Worker-Writer / Single-Writer sync protocol (event shape, ack match)
- GRE action dispatcher (mode gate, translator coverage)
- GRE worker integration runtime (opt-in, bootstrap without real GRE)
"""

import os
import sys
import unittest

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.orchestration.config_chunking import (
    chunk_controller_config,
    materialize_chunk_as_flat_config,
    chunk_signature,
    build_chunk_map,
    CONTROLLER_SCOPED_TABLES,
)
from src.core._worker_writer_sync import (
    build_patch_command,
    build_config_patch_event,
    build_writer_ack,
    is_config_patch,
    is_config_changed,
    match_config_changed,
    derive_chunk_refresh_plan,
    make_worker_patch_emitter,
    emit_config_patch,
    WORKER_PATCH_TYPE,
    SINGLE_WRITER_ACK_TYPE,
)
from src.core._gre_action_dispatcher import (
    is_automatic_mode_active,
    translate_action,
    dispatch_planned_actions,
    MODE_AUTO,
)
from src.core._gre_worker_integration import (
    create_gre_worker_runtime,
    refresh_runtime_chunk,
    run_gre_tick,
    emit_direct_patch,
    update_controller_data,
)


# ---------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------

def _sample_master_config():
    return {
        "controllers": [
            {"controller_id": 1, "device_name": "Ctrl_1", "mode_id": MODE_AUTO},
            {"controller_id": 2, "device_name": "Ctrl_2", "mode_id": 1},
        ],
        "sensors": [
            {"sensor_id": "S1", "controller_id": 1, "type": "temp"},
            {"sensor_id": "S2", "controller_id": 2, "type": "press"},
            {"sensor_id": "S3", "controller_id": 1,
             "type": "flow",
             "cross_domain_targets": [{"target": "controller", "scope": "Ctrl_2"}]},
        ],
        "actuators": [
            {"actuator_id": "A1", "controller_id": 1},
            {"actuator_id": "A2", "controller_id": 2},
        ],
        "process_states": [
            {"process_state_id": 10, "controller_id": 1, "state_id": 0},
            {"process_state_id": 20, "controller_id": 2, "state_id": 0},
        ],
        "triggers": [
            {"trigger_id": 100, "controller_id": 1, "kind": "sensor"},
        ],
        "timers": [
            {"timer_id": 500, "ctrl_id": 1, "duration_s": 5.0},
        ],
        "dynamic_rule_engine": [
            {"rule_id": 900, "target_controller_id": 1, "expression": "s > 10"},
        ],
        "application_settings": {"locale": "de"},
        "unit_defaults": {"unit": "SI"},
        "some_new_table_we_dont_know": [{"irrelevant": True}],
    }


class FakeQueue(object):
    def __init__(self):
        self.items = []

    def put_nowait(self, item):
        self.items.append(item)

    def put(self, item, block=True, timeout=None):
        self.items.append(item)


def _fake_put_fn(queue, payload):
    if queue is None:
        return False
    queue.put_nowait(payload)
    return True


# =============================================================================
# Config chunking
# =============================================================================

class TestConfigChunking(unittest.TestCase):

    def test_chunk_contains_only_matching_rows(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, controller_id=1)
        scoped = chunk["controller_scoped"]

        self.assertEqual(len(scoped["controllers"]), 1)
        self.assertEqual(scoped["controllers"][0]["controller_id"], 1)

        self.assertEqual(len(scoped["sensors"]), 2)
        sensor_ids = [s["sensor_id"] for s in scoped["sensors"]]
        self.assertIn("S1", sensor_ids)
        self.assertIn("S3", sensor_ids)  # cross-domain source belongs to ctrl 1
        self.assertNotIn("S2", sensor_ids)

        self.assertEqual(len(scoped["actuators"]), 1)
        self.assertEqual(scoped["actuators"][0]["actuator_id"], "A1")

        # Triggers via controller_id
        self.assertEqual(len(scoped["triggers"]), 1)
        # Timers via ctrl_id
        self.assertEqual(len(scoped["timers"]), 1)
        # DRE via target_controller_id
        self.assertEqual(len(scoped["dynamic_rule_engine"]), 1)

    def test_chunk_for_other_controller_is_disjoint(self):
        master = _sample_master_config()
        c1 = chunk_controller_config(master, controller_id=1)
        c2 = chunk_controller_config(master, controller_id=2)

        c1_sensor_ids = {s["sensor_id"] for s in c1["controller_scoped"]["sensors"]}
        c2_sensor_ids = {s["sensor_id"] for s in c2["controller_scoped"]["sensors"]}
        self.assertEqual(c1_sensor_ids & c2_sensor_ids, set())

    def test_global_passthrough_is_shared(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, controller_id=1)
        self.assertIn("application_settings", chunk["global_passthrough"])
        self.assertIn("unit_defaults", chunk["global_passthrough"])
        self.assertIs(
            chunk["global_passthrough"]["application_settings"],
            master["application_settings"],
        )

    def test_unknown_tables_passed_through_untouched(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, controller_id=1)
        self.assertIn("some_new_table_we_dont_know", chunk["unscoped_tables"])

    def test_signature_is_stable(self):
        master = _sample_master_config()
        sig_a = chunk_signature(chunk_controller_config(master, 1))
        sig_b = chunk_signature(chunk_controller_config(master, 1))
        self.assertEqual(sig_a, sig_b)

    def test_signature_changes_when_rows_change(self):
        master = _sample_master_config()
        sig_before = chunk_signature(chunk_controller_config(master, 1))

        master["sensors"].append({"sensor_id": "S4", "controller_id": 1})
        sig_after = chunk_signature(chunk_controller_config(master, 1))
        self.assertNotEqual(sig_before, sig_after)

    def test_materialize_produces_flat_dict(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, 1)
        flat = materialize_chunk_as_flat_config(chunk)

        for table in CONTROLLER_SCOPED_TABLES:
            self.assertIn(table, flat)
        self.assertIn("application_settings", flat)
        self.assertIn("some_new_table_we_dont_know", flat)

    def test_build_chunk_map_multiple_controllers(self):
        master = _sample_master_config()
        chunk_map = build_chunk_map(master, [1, 2, "nope"])
        self.assertEqual(set(chunk_map.keys()), {1, 2})

    def test_empty_master_returns_empty_chunk(self):
        chunk = chunk_controller_config({}, 1)
        self.assertEqual(chunk["meta"]["row_count"], 0)

    def test_non_dict_master_returns_safe_default(self):
        chunk = chunk_controller_config(None, 1)
        self.assertEqual(chunk["controller_id"], 1)
        self.assertEqual(chunk["controller_scoped"], {})


# =============================================================================
# Worker writer sync protocol
# =============================================================================

class TestWorkerWriterSync(unittest.TestCase):

    def test_build_patch_command_supported_op(self):
        cmd = build_patch_command("update", "process_states", row_id=1,
                                  values={"state_id": 7}, worker_id="Ctrl_1")
        self.assertEqual(cmd["op"], "update")
        self.assertEqual(cmd["table"], "process_states")
        self.assertEqual(cmd["values"], {"state_id": 7})
        self.assertEqual(cmd["worker_id"], "Ctrl_1")

    def test_build_patch_command_unknown_op_coerced(self):
        cmd = build_patch_command("destroy_everything", "x", values={})
        self.assertEqual(cmd["op"], "update")

    def test_build_config_patch_event_shape(self):
        cmd = build_patch_command("update", "timers", row_id=5, values={"armed": True})
        event = build_config_patch_event(
            controller_id=1,
            commands=[cmd],
            cause="timer_expired",
        )
        self.assertEqual(event["event_type"], WORKER_PATCH_TYPE)
        self.assertEqual(event["target"], "state")
        self.assertEqual(event["cross_domain_source"], {"kind": "controller", "id": "1"})
        self.assertEqual(len(event["payload"]["commands"]), 1)
        self.assertEqual(event["payload"]["cause"], "timer_expired")
        self.assertIn("event_id", event["payload"])
        self.assertIn("timestamp", event["payload"])

    def test_build_config_patch_event_accepts_single_dict(self):
        cmd = build_patch_command("update", "timers")
        event = build_config_patch_event(controller_id=1, commands=cmd)
        self.assertEqual(len(event["payload"]["commands"]), 1)

    def test_build_config_patch_event_cross_domain(self):
        event = build_config_patch_event(
            controller_id=1,
            commands=[build_patch_command("update", "actuators", row_id=9)],
            cross_domain_targets=[{"target": "controller", "scope": "Ctrl_2", "priority": 30}],
        )
        self.assertIn("cross_domain_targets", event)
        self.assertEqual(len(event["cross_domain_targets"]), 1)

    def test_is_config_patch_and_changed(self):
        patch = build_config_patch_event(1, [build_patch_command("update", "timers")])
        ack = build_writer_ack("evt_123", ok=True, affected_tables=["timers"])
        self.assertTrue(is_config_patch(patch))
        self.assertFalse(is_config_changed(patch))
        self.assertTrue(is_config_changed(ack))
        self.assertFalse(is_config_patch(ack))

    def test_match_config_changed_ignores_failed_ack(self):
        ack = build_writer_ack("evt_456", ok=False, reason="disk_full")
        self.assertFalse(match_config_changed(ack, worker_id="Ctrl_1"))

    def test_match_config_changed_accepts_ok_ack(self):
        ack = build_writer_ack("evt_789", ok=True, affected_tables=["sensors"])
        self.assertTrue(match_config_changed(ack, worker_id="Ctrl_1"))

    def test_derive_chunk_refresh_plan_detects_rule_change(self):
        before = {"meta": {"table_counts": {"process_states": 2, "timers": 1}}}
        after = {"meta": {"table_counts": {"process_states": 3, "timers": 1}}}
        plan = derive_chunk_refresh_plan(before, after)
        self.assertTrue(plan["recompile_required"])
        self.assertFalse(plan["reset_runtime"])
        self.assertIn("process_states", plan["changed_tables"])
        self.assertEqual(plan["row_delta"]["process_states"], 1)

    def test_derive_chunk_refresh_plan_detects_runtime_reset(self):
        before = {"meta": {"table_counts": {"timers": 2, "events": 0}}}
        after = {"meta": {"table_counts": {"timers": 2, "events": 3}}}
        plan = derive_chunk_refresh_plan(before, after)
        self.assertTrue(plan["reset_runtime"])
        self.assertFalse(plan["recompile_required"])

    def test_derive_chunk_refresh_plan_no_change(self):
        before = {"meta": {"table_counts": {"sensors": 5}}}
        after = {"meta": {"table_counts": {"sensors": 5}}}
        plan = derive_chunk_refresh_plan(before, after)
        self.assertFalse(plan["recompile_required"])
        self.assertFalse(plan["reset_runtime"])
        self.assertEqual(plan["changed_tables"], ())

    def test_emit_config_patch_via_emitter(self):
        queue = FakeQueue()
        emitter = make_worker_patch_emitter(queue, _fake_put_fn, controller_id=1)
        ok = emitter(
            commands=[build_patch_command("update", "process_states", row_id=10, values={"state_id": 2})],
            cause="gre_action",
        )
        self.assertTrue(ok)
        self.assertEqual(len(queue.items), 1)
        self.assertEqual(queue.items[0]["event_type"], WORKER_PATCH_TYPE)
        self.assertEqual(queue.items[0]["payload"]["worker_id"], "1")

    def test_emit_config_patch_null_queue_is_safe(self):
        ok = emit_config_patch(None, _fake_put_fn, 1, [build_patch_command("update", "x")])
        self.assertFalse(ok)


# =============================================================================
# GRE action dispatcher
# =============================================================================

class TestGREActionDispatcher(unittest.TestCase):

    def test_automatic_mode_gate(self):
        self.assertTrue(is_automatic_mode_active({"mode_id": MODE_AUTO}))
        self.assertTrue(is_automatic_mode_active({"mode_id": 2}))
        self.assertFalse(is_automatic_mode_active({"mode_id": 1}))
        self.assertFalse(is_automatic_mode_active({}))
        self.assertFalse(is_automatic_mode_active(None))

    def test_translate_set_state(self):
        action = {"kind": "set_state", "row_id": 42, "state_id": 7}
        cmd = translate_action(action, worker_id="Ctrl_1")
        self.assertEqual(cmd["op"], "update")
        self.assertEqual(cmd["table"], "process_states")
        self.assertEqual(cmd["values"], {"state_id": 7})
        self.assertEqual(cmd["worker_id"], "Ctrl_1")

    def test_translate_set_actuator(self):
        action = {"action": "set_actuator", "actuator_id": 12, "value": 42.5}
        cmd = translate_action(action, worker_id="Ctrl_1")
        self.assertEqual(cmd["table"], "actuators")
        self.assertEqual(cmd["values"]["value"], 42.5)

    def test_translate_raise_event(self):
        action = {"type": "raise_event", "event_type": "FAULT", "severity": "high", "message": "Overtemp"}
        cmd = translate_action(action, worker_id=1)
        self.assertEqual(cmd["op"], "insert")
        self.assertEqual(cmd["table"], "events")
        self.assertEqual(cmd["values"]["event_type"], "FAULT")
        self.assertEqual(cmd["values"]["severity"], "high")

    def test_translate_arm_timer(self):
        action = {"kind": "arm_timer", "timer_id": 500, "duration_s": 12.0}
        cmd = translate_action(action, worker_id=1)
        self.assertEqual(cmd["table"], "timers")
        self.assertEqual(cmd["values"]["duration_s"], 12.0)
        self.assertTrue(cmd["values"]["armed"])

    def test_translate_patch_row(self):
        action = {"kind": "patch_row", "table": "sensors",
                  "row_id": 7, "op": "update", "values": {"calibration": 1.1}}
        cmd = translate_action(action, worker_id=1)
        self.assertEqual(cmd["table"], "sensors")
        self.assertEqual(cmd["row_id"], 7)
        self.assertEqual(cmd["values"]["calibration"], 1.1)

    def test_translate_unknown_kind_returns_none(self):
        self.assertIsNone(translate_action({"kind": "destroy_the_world"}, worker_id=1))

    def test_translate_none_returns_none(self):
        self.assertIsNone(translate_action(None, worker_id=1))
        self.assertIsNone(translate_action({"no_kind_here": True}, worker_id=1))

    def test_dispatch_mode_gate_blocks_non_auto(self):
        actions = [{"kind": "set_state", "row_id": 1, "state_id": 2}]
        ctrl = {"mode_id": 1}
        self.assertEqual(dispatch_planned_actions(actions, ctrl, worker_id=1), ())

    def test_dispatch_mode_gate_allows_auto(self):
        actions = [{"kind": "set_state", "row_id": 1, "state_id": 2}]
        ctrl = {"mode_id": MODE_AUTO}
        cmds = dispatch_planned_actions(actions, ctrl, worker_id=1)
        self.assertEqual(len(cmds), 1)

    def test_dispatch_force_bypass(self):
        actions = [{"kind": "set_state", "row_id": 1, "state_id": 2}]
        cmds = dispatch_planned_actions(actions, {"mode_id": 1}, worker_id=1, force_dispatch=True)
        self.assertEqual(len(cmds), 1)

    def test_dispatch_drops_unknown_mixed_with_known(self):
        actions = [
            {"kind": "set_state", "row_id": 1, "state_id": 2},
            {"kind": "destroy_the_world"},
            {"kind": "set_actuator", "actuator_id": 3, "value": 1},
        ]
        cmds = dispatch_planned_actions(actions, {"mode_id": MODE_AUTO}, worker_id=1)
        self.assertEqual(len(cmds), 2)


# =============================================================================
# GRE worker integration
# =============================================================================

class TestGREWorkerIntegration(unittest.TestCase):

    def test_runtime_creation_is_robust_without_gre(self):
        # Even if the real GRE cannot compile an arbitrary chunk, runtime
        # creation must still return a dict with the expected keys.
        master = _sample_master_config()
        chunk = chunk_controller_config(master, 1)

        queue = FakeQueue()
        runtime = create_gre_worker_runtime(
            worker_id=1,
            chunk=chunk,
            controller_data={"mode_id": MODE_AUTO},
            queue=queue,
            put_fn=_fake_put_fn,
        )
        self.assertEqual(runtime["worker_id"], 1)
        self.assertIn("stats", runtime)
        self.assertIn("last_chunk_signature", runtime)
        self.assertIsNotNone(runtime.get("emitter"))

    def test_refresh_noop_when_signature_identical(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, 1)
        runtime = create_gre_worker_runtime(1, chunk, {"mode_id": MODE_AUTO})

        ok = refresh_runtime_chunk(runtime, chunk)
        self.assertTrue(ok)

    def test_update_controller_data(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, 1)
        runtime = create_gre_worker_runtime(1, chunk, {"mode_id": 1})
        update_controller_data(runtime, {"mode_id": MODE_AUTO})
        self.assertEqual(runtime["controller_data"]["mode_id"], MODE_AUTO)

    def test_emit_direct_patch_without_emitter_is_safe(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, 1)
        runtime = create_gre_worker_runtime(1, chunk, {"mode_id": MODE_AUTO})
        runtime["emitter"] = None
        ok = emit_direct_patch(runtime, [build_patch_command("update", "x")])
        self.assertFalse(ok)

    def test_emit_direct_patch_routes_through_emitter(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, 1)
        queue = FakeQueue()
        runtime = create_gre_worker_runtime(
            1, chunk, {"mode_id": MODE_AUTO},
            queue=queue, put_fn=_fake_put_fn,
        )
        cmd = build_patch_command("update", "process_states", row_id=1, values={"state_id": 9})
        ok = emit_direct_patch(runtime, [cmd], cause="bootstrap")
        self.assertTrue(ok)
        self.assertEqual(len(queue.items), 1)
        self.assertEqual(queue.items[0]["payload"]["cause"], "bootstrap")

    def test_run_gre_tick_handles_null_compiled_config_gracefully(self):
        master = _sample_master_config()
        chunk = chunk_controller_config(master, 1)
        runtime = create_gre_worker_runtime(1, chunk, {"mode_id": MODE_AUTO})
        # Force the safe branch: compiled_config may be None if GRE failed.
        runtime["compiled_config"] = None
        result = run_gre_tick(runtime, sensor_values={}, state_values={}, timers={})
        # Must return a dict, must not raise, must mark failure.
        self.assertIsInstance(result, dict)
        self.assertFalse(result["success"])
        self.assertEqual(result["commands_emitted"], 0)


# =============================================================================
# Wiring sanity: thread_management forwards flags
# =============================================================================

class TestThreadManagementWiring(unittest.TestCase):

    def test_create_tm_context_accepts_feature_flags(self):
        # Import locally to avoid pulling in heavy deps at module load.
        from src.core.thread_management import create_tm_context

        class _FakeResources(dict):
            pass

        ctx = create_tm_context(
            resources=_FakeResources({
                "config_data": {"data": {}, "lock": __import__("threading").RLock()},
            }),
            queue_event_send=FakeQueue(),
            queue_event_pc=FakeQueue(),
            chunking_enabled=True,
            writer_sync_enabled=True,
            gre_integration_enabled=True,
        )
        self.assertTrue(ctx["chunking_enabled"])
        self.assertTrue(ctx["writer_sync_enabled"])
        self.assertTrue(ctx["gre_integration_enabled"])

    def test_run_thread_management_signature_has_flags(self):
        import inspect
        from src.core.thread_management import run_thread_management
        sig = inspect.signature(run_thread_management)
        self.assertIn("chunking_enabled", sig.parameters)
        self.assertIn("writer_sync_enabled", sig.parameters)
        self.assertIn("gre_integration_enabled", sig.parameters)


if __name__ == "__main__":
    unittest.main(verbosity=2)
