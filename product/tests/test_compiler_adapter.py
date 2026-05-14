# -*- coding: utf-8 -*-
# tests/test_compiler_adapter.py

import json
import logging
import os
import sys
import tempfile
import textwrap
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.adapters.compiler import (
    SUPPORTED_MODES,
    build_adapter,
    describe,
    is_compiled,
    load_config,
    reverse_compile,
    watch_targets,
)
from src.adapters.compiler._compatibility_validator import validate_compiled_bundle
from src.adapters.compiler._projection_bridge import (
    NEXT_LEGACY_PROJECTION_CANDIDATE,
    PROCESS_STATE_SIDECAR_VERSION,
    PROCESS_STATES_ACTIVATION_CONTRACT_MODE,
    PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE,
    PROJECTED_RUNTIME_JSON_KERNEL_KEYS,
    REVERSE_COMPILE_CONTRACT_MODE,
    build_process_states_live_projection_patch,
    build_runtime_projection_bundle,
)
from src.adapters.compiler import _cud_compiler_adapter



def _write_text(path, text):
    parent = os.path.dirname(path)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)



def _write_json(path, payload):
    parent = os.path.dirname(path)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)



def _reset_cud_adapter_cache():
    _cud_compiler_adapter._BUNDLE_CACHE["hash"] = None
    _cud_compiler_adapter._BUNDLE_CACHE["projection_bundle"] = None


class TestLegacyJsonAdapter(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.json_path = os.path.join(self.tmp, "___device_state_data.json")
        _write_json(self.json_path, {"devices": {"d1": {}}, "controllers": [{"id": "c1"}]})

    def test_load_legacy_dict(self):
        handle = build_adapter({"mode": "legacy_json", "legacy_json_path": self.json_path})
        cfg = load_config(handle)
        self.assertIn("devices", cfg)
        self.assertIn("controllers", cfg)
        self.assertFalse(is_compiled(handle))
        self.assertEqual(watch_targets(handle)[0], self.json_path)
        self.assertEqual(describe(handle)["mode"], "legacy_json")
        self.assertFalse(describe(handle)["supports_reverse_compile"])

    def test_missing_file_returns_empty(self):
        handle = build_adapter({"mode": "legacy_json", "legacy_json_path": "/no/such/file.json"})
        self.assertEqual(load_config(handle), {})

    def test_unknown_mode_falls_back(self):
        handle = build_adapter({"mode": "nonsense"})
        self.assertEqual(handle["mode"], "legacy_json")


class TestCompatibilityValidator(unittest.TestCase):
    def test_accepts_projection_bundle(self):
        ok, reasons = validate_compiled_bundle({
            "projection_version": 1,
            "compile_revision": "REV_01",
            "bundle_ref": "BUNDLE_01",
            "runtime_json_kernel_patch": {
                "config_schema_version": 2,
                "compile_projection_meta": {
                    "projection_version": 1,
                    "compile_revision": "REV_01",
                    "bundle_ref": "BUNDLE_01",
                    "projection_mode": "overlay_onto_legacy_runtime_json_kernel",
                    "projected_kernel_sections": list(PROJECTED_RUNTIME_JSON_KERNEL_KEYS),
                    "prepared_legacy_sections": ["process_states"],
                    "next_legacy_projection_candidate": "process_states",
                    "reverse_compile_contract_mode": REVERSE_COMPILE_CONTRACT_MODE,
                },
            },
            "activation": {
                "mutation_class": "compile_full",
                "activation_class": "global",
                "requires_restart": False,
                "requires_review": False,
                "process_states_live_projection": {
                    "contract_mode": PROCESS_STATES_ACTIVATION_CONTRACT_MODE,
                    "default_mode": PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE,
                    "candidate_count": 1,
                    "requires_review": True,
                },
            },
            "sidecars": {
                "process_state_projection_sidecar": {
                    "version": PROCESS_STATE_SIDECAR_VERSION,
                    "target_section": "process_states",
                    "projection_strategy": "sidecar_preprojection_v1",
                    "projection_status": "planned_not_applied",
                    "requires_legacy_mapping_contract": True,
                    "source_boot_state_ref": "BOOT_01",
                    "records": [
                        {
                            "candidate_ref": "PSTATECAND_01",
                            "target_section": "process_states",
                            "projection_status": "planned_not_applied",
                            "projection_strategy": "sidecar_preprojection_v1",
                            "apply_policy": "requires_legacy_mapping_contract",
                            "legacy_mapping_status": "unresolved",
                            "domain_ref": "LDC_01",
                            "logical_owner_context_ref": "LDC_01",
                            "active_rule_refs": ["RULE_01"],
                            "active_signal_refs": ["SIG_01"],
                            "active_timer_refs": [],
                            "active_binding_refs": [],
                            "control_strategy_hints": [
                                {
                                    "hint_ref": "CMHINT_01",
                                    "rule_ref": "RULE_01",
                                    "strategy": "rule_effect",
                                }
                            ],
                            "legacy_mapping_hints": {
                                "state_id_hint": None,
                                "p_id_hint": None,
                                "value_id_hint": None,
                                "mode_id_hint": None,
                                "mapping_id_hint": None,
                                "sensor_ids_hint": [],
                                "actuator_ids_hint": [],
                            },
                        }
                    ],
                },
                "process_states_activation_contract": {
                    "version": 1,
                    "contract_mode": PROCESS_STATES_ACTIVATION_CONTRACT_MODE,
                    "target_section": "process_states",
                    "default_live_projection_mode": PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE,
                    "allowed_live_projection_modes": ["disabled", "review_only", "apply_resolved"],
                    "allowed_merge_policies": ["baseline_wins", "projection_wins_on_empty_only", "explicit_field_merge", "review_only"],
                    "allowed_rollback_policies": ["restore_previous_row", "remove_new_row", "rebind_previous_mapping", "manual_rollback_required"],
                    "mapping_contract_required": True,
                    "candidate_count": 1,
                    "candidate_refs": ["PSTATECAND_01"],
                }
            },
        })
        self.assertTrue(ok)

    def test_accepts_legacy_minimal_bundle_for_transition(self):
        ok, reasons = validate_compiled_bundle({"devices": {}, "controllers": []})
        self.assertTrue(ok)
        self.assertTrue(any("legacy" in reason for reason in reasons))

    def test_rejects_non_dict(self):
        ok, reasons = validate_compiled_bundle("not a dict")
        self.assertFalse(ok)

    def test_rejects_missing_projection_keys(self):
        ok, reasons = validate_compiled_bundle({"projection_version": 1})
        self.assertFalse(ok)
        self.assertTrue(any("runtime_json_kernel_patch" in reason for reason in reasons))

    def test_rejects_wrong_type(self):
        ok, reasons = validate_compiled_bundle({
            "projection_version": 1,
            "compile_revision": "REV_01",
            "bundle_ref": "BUNDLE_01",
            "runtime_json_kernel_patch": [],
            "activation": {
                "mutation_class": "compile_full",
                "activation_class": "global",
                "requires_restart": False,
                "requires_review": False,
            },
        })
        self.assertFalse(ok)

    def test_rejects_unsupported_projected_top_level_section(self):
        ok, reasons = validate_compiled_bundle({
            "projection_version": 1,
            "compile_revision": "REV_01",
            "bundle_ref": "BUNDLE_01",
            "runtime_json_kernel_patch": {
                "config_schema_version": 2,
                "compile_projection_meta": {
                    "projection_version": 1,
                    "compile_revision": "REV_01",
                    "bundle_ref": "BUNDLE_01",
                    "projection_mode": "overlay_onto_legacy_runtime_json_kernel",
                    "projected_kernel_sections": list(PROJECTED_RUNTIME_JSON_KERNEL_KEYS),
                },
                "controllers": [],
            },
            "activation": {
                "mutation_class": "compile_full",
                "activation_class": "global",
                "requires_restart": False,
                "requires_review": False,
            },
        })
        self.assertFalse(ok)
        self.assertTrue(any("unsupported top-level sections" in reason for reason in reasons))

    def test_rejects_invalid_process_state_sidecar(self):
        ok, reasons = validate_compiled_bundle({
            "projection_version": 1,
            "compile_revision": "REV_01",
            "bundle_ref": "BUNDLE_01",
            "runtime_json_kernel_patch": {
                "config_schema_version": 2,
                "compile_projection_meta": {
                    "projection_version": 1,
                    "compile_revision": "REV_01",
                    "bundle_ref": "BUNDLE_01",
                    "projection_mode": "overlay_onto_legacy_runtime_json_kernel",
                    "projected_kernel_sections": list(PROJECTED_RUNTIME_JSON_KERNEL_KEYS),
                },
            },
            "activation": {
                "mutation_class": "compile_full",
                "activation_class": "global",
                "requires_restart": False,
                "requires_review": False,
            },
            "sidecars": {
                "process_state_projection_sidecar": {
                    "version": 1,
                    "target_section": "process_states",
                    "records": "not-a-list",
                }
            },
        })
        self.assertFalse(ok)
        self.assertTrue(any("process_state_projection_sidecar" in reason for reason in reasons))


class TestProjectionBridge(unittest.TestCase):
    def test_builds_runtime_projection_bundle(self):
        compiler_result = {
            "runtime_contract_bundle": {"bundle_id": "BUNDLE_FAKE", "source_cud_hash": "abcd"},
            "runtime_manifest": {
                "manifest_id": "MANIFEST_FAKE",
                "bundle_hash": "deadbeef",
                "canonical_id": "CUD_DEMO",
                "source_cud_hash": "abcd",
            },
            "runtime_shard_layout": {
                "layout_id": "LAYOUT_FAKE",
                "logical_shards": [{
                    "id": "LSHARD_1",
                    "domain_ref": "LDC_1",
                    "owner_context_ref": "LDC_1",
                    "rule_refs": ["RULE_NOTIFY_A", "RULE_CONTROL_PID_A"],
                    "signal_refs": ["SIG_TEMP_PV", "SIG_TEMP_SP"],
                    "worker_ref": "worker-a",
                }],
                "physical_shards": [{
                    "id": "PSHARD_1",
                    "owner_context_ref": "POC_1",
                    "point_refs": ["P_1"],
                    "worker_ref": "worker-a",
                }],
            },
            "runtime_control_state_boot": {
                "boot_state_id": "BOOT_FAKE",
                "logical_domain_states": [{
                    "domain_ref": "LDC_1",
                    "logical_owner_context_ref": "LDC_1",
                    "active_rules": ["RULE_NOTIFY_A", "RULE_CONTROL_PID_A"],
                    "active_signals": ["SIG_TEMP_PV", "SIG_TEMP_SP", "SIG_HEATER_CV"],
                    "active_timers": ["TMR_SCAN_100MS"],
                    "active_bindings": ["BND_SIG_HEATER_CV", "BND_SIG_TEMP_PV"],
                    "active_fsms": ["FSM_HEAT"],
                }],
                "physical_owner_states": [{
                    "owner_context_ref": "POC_1",
                    "active_bindings": ["BND_SIG_HEATER_CV", "BND_SIG_TEMP_PV"],
                    "active_points": ["POINT_HEATER_CV_RAW", "POINT_TEMP_PV_RAW"],
                }],
            },
            "compiled_rule_ir": {
                "rules": [
                    {
                        "id": "RULE_NOTIFY_A",
                        "domain_ref": "LDC_1",
                        "owner_context_ref": "LDC_1",
                        "stage": "notification",
                        "normalized_then": {"emit": [{"code": "READY", "kind": "notification"}]},
                    },
                    {
                        "id": "RULE_CONTROL_PID_A",
                        "domain_ref": "LDC_1",
                        "owner_context_ref": "LDC_1",
                        "stage": "control",
                        "normalized_when": {
                            "all": [
                                {"signal_ref": "SIG_TEMP_PV", "op": "less_than", "value": 40.0},
                                {"signal_ref": "SIG_TEMP_SP", "op": "greater_than", "value": 30.0},
                            ]
                        },
                        "normalized_then": {
                            "set_signals": [
                                {"signal_ref": "SIG_HEATER_CV", "value": 75.0}
                            ]
                        },
                    },
                ],
            },
        }
        projection = build_runtime_projection_bundle(compiler_result)
        self.assertEqual(projection["projection_version"], 1)
        self.assertIn("runtime_json_kernel_patch", projection)
        self.assertIn("runtime_topology", projection["runtime_json_kernel_patch"])
        self.assertIn("chunk_registry", projection["runtime_json_kernel_patch"])
        self.assertIn("event_routes", projection["runtime_json_kernel_patch"])
        self.assertIn("compiled.notification.ready", projection["runtime_json_kernel_patch"]["event_routes"])
        self.assertEqual(
            projection["runtime_json_kernel_patch"]["compile_projection_meta"]["projected_kernel_sections"],
            list(PROJECTED_RUNTIME_JSON_KERNEL_KEYS),
        )
        self.assertEqual(
            projection["runtime_json_kernel_patch"]["compile_projection_meta"]["prepared_legacy_sections"],
            ["process_states"],
        )
        self.assertEqual(
            projection["runtime_json_kernel_patch"]["compile_projection_meta"]["next_legacy_projection_candidate"],
            NEXT_LEGACY_PROJECTION_CANDIDATE,
        )
        self.assertEqual(
            projection["runtime_json_kernel_patch"]["compile_projection_meta"]["reverse_compile_contract_mode"],
            REVERSE_COMPILE_CONTRACT_MODE,
        )
        self.assertEqual(projection["affected_workers"], ["worker-a"])
        process_state_sidecar = projection["sidecars"]["process_state_projection_sidecar"]
        self.assertEqual(process_state_sidecar["version"], PROCESS_STATE_SIDECAR_VERSION)
        self.assertEqual(process_state_sidecar["target_section"], "process_states")
        self.assertEqual(process_state_sidecar["projection_status"], "planned_not_applied")
        self.assertTrue(process_state_sidecar["records"])
        self.assertEqual(process_state_sidecar["records"][0]["domain_ref"], "LDC_1")
        self.assertEqual(process_state_sidecar["records"][0]["mode_hint"], "automatic")
        self.assertTrue(process_state_sidecar["records"][0]["control_strategy_hints"])
        activation_contract = projection["sidecars"]["process_states_activation_contract"]
        self.assertEqual(activation_contract["contract_mode"], PROCESS_STATES_ACTIVATION_CONTRACT_MODE)
        self.assertEqual(activation_contract["default_live_projection_mode"], PROCESS_STATES_DEFAULT_LIVE_PROJECTION_MODE)
        self.assertEqual(activation_contract["candidate_count"], len(process_state_sidecar["records"]))


class TestCudCompiledAdapter(unittest.TestCase):
    def setUp(self):
        _reset_cud_adapter_cache()
        self.tmp = tempfile.mkdtemp()
        self.legacy_json_path = os.path.join(self.tmp, "config", "___device_state_data.json")
        self.cud_dir = os.path.join(self.tmp, "config", "cud")
        self.compiled_dir = os.path.join(self.tmp, "config", "compiled")
        self.compiler_root = os.path.join(self.tmp, "fake_compiler_root")
        _write_json(self.legacy_json_path, {
            "controllers": [{"controller_id": 1, "name": "legacy-controller"}],
            "process_states": [],
        })
        _write_text(os.path.join(self.cud_dir, "main.yaml"), "canonical_id: TEST\n")
        self._write_fake_compiler_package(self.compiler_root)

    def tearDown(self):
        _reset_cud_adapter_cache()

    def _write_fake_compiler_package(self, root_dir):
        _write_text(os.path.join(root_dir, "compiler_engine", "__init__.py"), "")
        _write_text(os.path.join(root_dir, "compiler_engine", "application", "__init__.py"), "")
        _write_text(
            os.path.join(root_dir, "compiler_engine", "application", "compile_service.py"),
            textwrap.dedent(
                '''
                import os

                def compile_from_paths(source_path, output_dir):
                    if not os.path.isfile(source_path):
                        raise FileNotFoundError(source_path)
                    os.makedirs(output_dir, exist_ok=True)
                    return {
                        "runtime_contract_bundle": {
                            "bundle_id": "BUNDLE_FAKE_01",
                            "source_cud_hash": "source-hash-01",
                        },
                        "runtime_manifest": {
                            "manifest_id": "MANIFEST_FAKE_01",
                            "bundle_hash": "bundle-hash-01",
                            "canonical_id": "CUD_FAKE_01",
                            "source_cud_hash": "source-hash-01",
                        },
                        "runtime_shard_layout": {
                            "layout_id": "LAYOUT_FAKE_01",
                            "logical_shards": [
                                {
                                    "id": "LSHARD_FAKE_01",
                                    "domain_ref": "LDC_FAKE_01",
                                    "owner_context_ref": "LDC_FAKE_01",
                                    "worker_ref": "worker-alpha",
                                    "mode": "single_writer_commit",
                                    "rule_refs": ["RULE_NOTIFY_FAKE", "RULE_CONTROL_PID_FAKE"],
                                    "signal_refs": ["SIG_TEMP_PV", "SIG_TEMP_SP", "SIG_HEAT_CV"],
                                }
                            ],
                            "physical_shards": [
                                {
                                    "id": "PSHARD_FAKE_01",
                                    "owner_context_ref": "POC_FAKE_01",
                                    "worker_ref": "worker-alpha",
                                    "mode": "physical_adapter",
                                    "point_refs": ["POINT_TEMP"],
                                    "asset_refs": ["CTRL_1"],
                                }
                            ],
                        },
                        "runtime_control_state_boot": {
                            "boot_state_id": "BOOT_FAKE_01",
                            "logical_domain_states": [
                                {
                                    "domain_ref": "LDC_FAKE_01",
                                    "logical_owner_context_ref": "LDC_FAKE_01",
                                    "active_rules": ["RULE_NOTIFY_FAKE", "RULE_CONTROL_PID_FAKE"],
                                    "active_signals": ["SIG_TEMP_PV", "SIG_TEMP_SP", "SIG_HEAT_CV"],
                                    "active_timers": ["TMR_100MS"],
                                    "active_bindings": ["BND_SIG_HEAT_CV", "BND_SIG_TEMP_PV"],
                                    "active_fsms": ["FSM_HEAT_LOOP"],
                                }
                            ],
                            "physical_owner_states": [
                                {
                                    "owner_context_ref": "POC_FAKE_01",
                                    "active_bindings": ["BND_SIG_HEAT_CV", "BND_SIG_TEMP_PV"],
                                    "active_points": ["POINT_TEMP"],
                                }
                            ],
                        },
                        "compiled_rule_ir": {
                            "rules": [
                                {
                                    "id": "RULE_NOTIFY_FAKE",
                                    "domain_ref": "LDC_FAKE_01",
                                    "owner_context_ref": "LDC_FAKE_01",
                                    "stage": "notification",
                                    "normalized_then": {
                                        "emit": [
                                            {"code": "HEAT_READY", "kind": "notification"}
                                        ]
                                    },
                                },
                                {
                                    "id": "RULE_CONTROL_PID_FAKE",
                                    "domain_ref": "LDC_FAKE_01",
                                    "owner_context_ref": "LDC_FAKE_01",
                                    "stage": "control",
                                    "normalized_when": {
                                        "all": [
                                            {"signal_ref": "SIG_TEMP_PV", "op": "less_than", "value": 40.0},
                                            {"signal_ref": "SIG_TEMP_SP", "op": "greater_than", "value": 30.0}
                                        ]
                                    },
                                    "normalized_then": {
                                        "set_signals": [
                                            {"signal_ref": "SIG_HEAT_CV", "value": 75.0}
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                '''
            ),
        )
        _write_text(
            os.path.join(root_dir, "compiler_engine", "application", "decompile_service.py"),
            textwrap.dedent(
                '''
                def decompile_data(bundle, runtime_change_set=None):
                    result = {
                        "decompiled_cud": {
                            "canonical_id": bundle.get("bundle_id"),
                            "schema_version": "1.0",
                        }
                    }
                    if runtime_change_set is not None:
                        result["cud_delta"] = {
                            "bundle_ref": bundle.get("bundle_id"),
                            "compile_revision": runtime_change_set.get("compile_revision"),
                            "changes": runtime_change_set.get("changes", []),
                        }
                    return result
                '''
            ),
        )

    def test_load_cud_compiled_projection_overlay(self):
        handle = build_adapter({
            "mode": "cud_compiled",
            "legacy_json_path": self.legacy_json_path,
            "cud_source_dir": self.cud_dir,
            "compiled_cache_dir": self.compiled_dir,
            "compiler_python_paths": [self.compiler_root],
            "runtime_compile_enabled": False,
        })
        cfg = load_config(handle)
        self.assertIn("controllers", cfg)
        self.assertEqual(cfg["controllers"][0]["name"], "legacy-controller")
        self.assertEqual(cfg["config_schema_version"], 2)
        self.assertIn("runtime_topology", cfg)
        self.assertIn("chunk_registry", cfg)
        self.assertIn("compile_projection_meta", cfg)
        self.assertEqual(
            cfg["compile_projection_meta"]["projection_scope_class"],
            "minimal_kernel_overlay_v1",
        )
        self.assertEqual(
            cfg["compile_projection_meta"]["projected_kernel_sections"],
            list(PROJECTED_RUNTIME_JSON_KERNEL_KEYS),
        )
        self.assertEqual(
            cfg["compile_projection_meta"]["prepared_legacy_sections"],
            ["process_states"],
        )
        self.assertEqual(
            cfg["compile_projection_meta"]["process_states_live_projection"]["status"],
            "disabled",
        )
        self.assertIn("compiled.notification.heat_ready", cfg.get("event_routes", {}))
        self.assertTrue(is_compiled(handle))
        self.assertTrue(describe(handle)["supports_reverse_compile"])
        self.assertEqual(set(watch_targets(handle)), {self.cud_dir, self.legacy_json_path})

    def test_reverse_compile_uses_runtime_contract_sidecar(self):
        handle = build_adapter({
            "mode": "cud_compiled",
            "legacy_json_path": self.legacy_json_path,
            "cud_source_dir": self.cud_dir,
            "compiled_cache_dir": self.compiled_dir,
            "compiler_python_paths": [self.compiler_root],
            "runtime_compile_enabled": False,
        })
        load_config(handle)
        result = reverse_compile(handle, {
            "change_set_id": "RCS_FAKE_01",
            "changes": [
                {
                    "id": "CHG_01",
                    "classification": "safe_reversible",
                    "target_ref": "PSTATECAND_FAKE",
                    "payload": {"parameters": {"target_temp_c": 40.0}},
                }
            ],
        })
        self.assertIsInstance(result, dict)
        self.assertEqual(result["decompiled_cud"]["canonical_id"], "BUNDLE_FAKE_01")
        self.assertEqual(result["cud_delta"]["bundle_ref"], "BUNDLE_FAKE_01")
        self.assertEqual(result["cud_delta"]["compile_revision"], "REV_bundle_hash_01")
        self.assertEqual(len(result["cud_delta"]["changes"]), 1)


    def test_process_states_live_projection_patch_applies_resolved_mapping_contract(self):
        compiler_result = {
            "runtime_contract_bundle": {"bundle_id": "BUNDLE_LIVE_01", "source_cud_hash": "abcd"},
            "runtime_manifest": {
                "manifest_id": "MANIFEST_LIVE_01",
                "bundle_hash": "beadfeed",
                "canonical_id": "CUD_LIVE_01",
                "source_cud_hash": "abcd",
            },
            "runtime_shard_layout": {
                "layout_id": "LAYOUT_LIVE_01",
                "logical_shards": [{
                    "id": "LSHARD_LIVE_01",
                    "domain_ref": "LDC_1",
                    "owner_context_ref": "LDC_1",
                    "worker_ref": "worker-a",
                }],
                "physical_shards": [],
            },
            "runtime_control_state_boot": {
                "boot_state_id": "BOOT_LIVE_01",
                "logical_domain_states": [{
                    "domain_ref": "LDC_1",
                    "logical_owner_context_ref": "LDC_1",
                    "active_rules": ["RULE_NOTIFY_A"],
                    "active_signals": ["SIG_TEMP_PV"],
                    "active_timers": [],
                    "active_bindings": [],
                }],
                "physical_owner_states": [],
            },
            "compiled_rule_ir": {
                "rules": [{
                    "id": "RULE_NOTIFY_A",
                    "domain_ref": "LDC_1",
                    "owner_context_ref": "LDC_1",
                    "stage": "notification",
                    "normalized_then": {"emit": [{"code": "READY", "kind": "notification"}]},
                }],
            },
        }
        projection = build_runtime_projection_bundle(compiler_result)
        candidate_ref = projection["sidecars"]["process_state_projection_sidecar"]["records"][0]["candidate_ref"]
        patch, report = build_process_states_live_projection_patch(
            {"process_states": []},
            projection,
            mapping_contract={
                "mapping_contract_ref": "PSTATEMAP_CONTRACT_01",
                "records": [{
                    "candidate_ref": candidate_ref,
                    "mapping_decision_ref": "PSTATEMAP_01",
                    "mapping_mode": "create_new_legacy_state",
                    "state_id_strategy": "reserve_new",
                    "resolved_state_id": 18,
                    "resolved_p_id": 1,
                    "resolved_value_id": 1,
                    "resolved_mode_id": 2,
                    "resolved_mapping_id": 100,
                    "resolved_sensor_ids": [1],
                    "resolved_actuator_ids": [5],
                    "parameters_strategy": "explicit_field_merge",
                    "resolved_parameters": {"target_temp_c": 120.0},
                    "control_strategy_strategy": "placeholder_pid_to_pid_config",
                    "resolved_control_strategy": {"type": "PID", "pid_config_id": 1},
                    "setpoint_source_strategy": "leave_null",
                    "resolved_setpoint_source": None,
                    "merge_policy": "explicit_field_merge",
                    "rollback_policy": "remove_new_row",
                    "owner_context_ref": "LDC_1",
                    "worker_refs": ["worker-a"],
                    "requires_review": False,
                }],
            },
            activation_settings={
                "process_states_live_projection_enabled": True,
                "process_states_live_projection_mode": "apply_resolved",
                "process_states_live_projection_apply_limit": 1,
                "process_states_live_projection_allow_review_required": False,
            },
        )
        self.assertIsInstance(patch, dict)
        self.assertEqual(report["status"], "applied")
        self.assertEqual(report["applied_candidate_refs"], [candidate_ref])
        self.assertEqual(patch["process_states"][0]["state_id"], 18)
        self.assertEqual(patch["process_states"][0]["mapping_id"], 100)


class TestCudCompiledAdapterLiveProjection(unittest.TestCase):
    def setUp(self):
        _reset_cud_adapter_cache()
        self.tmp = tempfile.mkdtemp()
        self.legacy_json_path = os.path.join(self.tmp, "config", "___device_state_data.json")
        self.cud_dir = os.path.join(self.tmp, "config", "cud")
        self.compiled_dir = os.path.join(self.tmp, "config", "compiled")
        self.compiler_root = os.path.join(self.tmp, "fake_compiler_root")
        self.mapping_contract_path = os.path.join(self.tmp, "config", "contracts", "process_states_mapping_contract.json")
        _write_json(self.legacy_json_path, {
            "controllers": [{"controller_id": 1, "name": "legacy-controller"}],
            "process_states": [],
        })
        _write_text(os.path.join(self.cud_dir, "main.yaml"), "canonical_id: TEST\n")
        helper = TestCudCompiledAdapter()
        helper._write_fake_compiler_package(self.compiler_root)

    def tearDown(self):
        _reset_cud_adapter_cache()

    def _build_settings(self):
        return {
            "mode": "cud_compiled",
            "legacy_json_path": self.legacy_json_path,
            "cud_source_dir": self.cud_dir,
            "compiled_cache_dir": self.compiled_dir,
            "compiler_python_paths": [self.compiler_root],
            "runtime_compile_enabled": False,
            "process_states_mapping_contract_path": self.mapping_contract_path,
            "process_states_live_projection_enabled": True,
            "process_states_live_projection_mode": "apply_resolved",
            "process_states_live_projection_apply_limit": 1,
            "process_states_live_projection_allow_review_required": False,
        }

    def test_load_config_applies_process_states_live_projection(self):
        settings = self._build_settings()
        bundle = _cud_compiler_adapter.compile_if_needed(settings, logging.getLogger(__name__))
        candidate_ref = bundle["sidecars"]["process_state_projection_sidecar"]["records"][0]["candidate_ref"]
        _write_json(self.mapping_contract_path, {
            "mapping_contract_ref": "PSTATEMAP_CONTRACT_01",
            "records": [{
                "candidate_ref": candidate_ref,
                "mapping_decision_ref": "PSTATEMAP_01",
                "mapping_mode": "create_new_legacy_state",
                "state_id_strategy": "reserve_new",
                "resolved_state_id": 18,
                "resolved_p_id": 1,
                "resolved_value_id": 1,
                "resolved_mode_id": 2,
                "resolved_mapping_id": 100,
                "resolved_sensor_ids": [1],
                "resolved_actuator_ids": [5],
                "parameters_strategy": "explicit_field_merge",
                "resolved_parameters": {"target_temp_c": 120.0},
                "control_strategy_strategy": "placeholder_pid_to_pid_config",
                "resolved_control_strategy": {"type": "PID", "pid_config_id": 1},
                "setpoint_source_strategy": "leave_null",
                "resolved_setpoint_source": None,
                "merge_policy": "explicit_field_merge",
                "rollback_policy": "remove_new_row",
                "owner_context_ref": "LDC_FAKE_01",
                "worker_refs": ["worker-alpha"],
                "requires_review": False,
            }],
        })

        handle = build_adapter(settings)
        cfg = load_config(handle)
        self.assertEqual(len(cfg["process_states"]), 1)
        self.assertEqual(cfg["process_states"][0]["state_id"], 18)
        self.assertEqual(cfg["process_states"][0]["mapping_id"], 100)
        self.assertEqual(
            cfg["compile_projection_meta"]["process_states_live_projection"]["status"],
            "applied",
        )
        self.assertIn(self.mapping_contract_path, set(watch_targets(handle)))


class TestSupportedModes(unittest.TestCase):
    def test_modes_declared(self):
        self.assertEqual(set(SUPPORTED_MODES), {"legacy_json", "cud_compiled"})


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    unittest.main()
