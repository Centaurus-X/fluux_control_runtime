import sys, os, json, tempfile, unittest
from pathlib import Path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.core import _state_version as sv
from src.core import _snapshot_freeze as sf
from src.core import _worker_writer_sync as wws
from src.adapters.compiler import build_adapter, load_config
from src.orchestration.runtime_config import build_runtime_settings


class TestClosureStateVersion(unittest.TestCase):
    def setUp(self):
        sv.reset_for_tests()
        self.cap = []

    def _put(self, q, ev):
        self.cap.append(ev)
        return True

    def test_closure_bumps_correctly(self):
        res = {}
        settings = {"worker_runtime": {"state_version_enabled": True}}
        for i in (1, 2, 3):
            wws.emit_config_patch(
                "q",
                self._put,
                "ctrl",
                [wws.build_patch_command("UPDATE", "t", "r", {"v": i})],
                sv_settings=settings,
                sv_resources=res,
            )
        self.assertEqual([e.get("state_version") for e in self.cap], [1, 2, 3])

    def test_no_context_no_stamp(self):
        wws.emit_config_patch(
            "q",
            self._put,
            "ctrl",
            [wws.build_patch_command("UPDATE", "t", "r", {"v": 1})],
        )
        self.assertNotIn("state_version", self.cap[0])


class TestLegacyAdapterStillWorks(unittest.TestCase):
    def test_legacy_roundtrip(self):
        tmp = tempfile.mkdtemp()
        jp = os.path.join(tmp, "d.json")
        json.dump({"devices": {}, "controllers": []}, open(jp, "w"))
        h = build_adapter({"mode": "legacy_json", "legacy_json_path": jp})
        self.assertIn("devices", load_config(h))


class TestSnapshotFreeze(unittest.TestCase):
    def test_deepcopy_isolates(self):
        live = {"sensor_values": {"s": 1}}
        snap = sf.wrap_tick_inputs({"worker_runtime": {"snapshot_freeze_enabled": True}}, live)
        live["sensor_values"]["s"] = 99
        self.assertEqual(snap["data"]["sensor_values"]["s"], 1)


class TestRuntimeSettingsBridge(unittest.TestCase):
    def test_runtime_settings_expose_config_source_and_worker_gateway(self):
        tmp = tempfile.mkdtemp()
        compiler_root = os.path.join(tmp, "vendor", "current_compiler_engine", "code_compiler_engine")
        os.makedirs(compiler_root, exist_ok=True)

        contracts_runtime = Path(tmp) / "contracts" / "runtime"
        contracts_runtime.mkdir(parents=True, exist_ok=True)
        (contracts_runtime / "PROCESS_STATES_MAPPING_CONTRACT_V1.yaml").write_text("schema_version: 1\n", encoding="utf-8")
        (contracts_runtime / "CONTROL_METHODS_MAPPING_CONTRACT_V1.yaml").write_text("schema_version: 1\n", encoding="utf-8")
        registry_path = Path(tmp) / "contracts" / "contract_registry.yaml"
        registry_path.write_text(
            """schema_version: 1
registry_id: TEST_CONTRACT_REGISTRY
status: active_runtime_resolved
product_release_target: test
canonical_contract_root: product/contracts
entries:
- contract_id: C08
  title: Process States Mapping
  status: active
  classification: runtime_mapping
  source_of_truth: runtime_instance
  current_paths:
    runtime_instance: product/contracts/runtime/PROCESS_STATES_MAPPING_CONTRACT_V1.yaml
- contract_id: C17
  title: Control Methods Live Projection
  status: active
  classification: runtime_mapping
  source_of_truth: runtime_instance
  current_paths:
    runtime_instance: product/contracts/runtime/CONTROL_METHODS_MAPPING_CONTRACT_V1.yaml
""",
            encoding="utf-8",
        )

        app_config = {
            "contract_registry": {
                "enabled": True,
                "registry_path": "contracts/contract_registry.yaml",
                "strict": True,
            },
            "app": {"node_id": "fn-01"},
            "network": {
                "local_mode": True,
                "master": {"protocol": "mqtt", "ip": "127.0.0.1", "port": 1883, "use_ipv6": False},
                "mqtt_client": {"topic_root": "xserver/v1", "ssl": {"enabled": False}},
            },
            "paths": {
                "device_state_json": "config/___device_state_data.json",
                "device_state_json_test": "config/___device_state_data.variant_b.json",
            },
            "config_source": {
                "mode": "cud_compiled",
                "legacy_json_path": "config/___device_state_data.json",
                "cud_source_dir": "config/cud",
                "compiled_cache_dir": "config/compiled",
                "runtime_compile_enabled": False,
                "reverse_compile_enabled": False,
                "reverse_compile_output_dir": "config/decompiled",
                "runtime_change_capture_enabled": False,
                "runtime_change_capture_dir": "config/runtime_change_sets",
                "next_legacy_projection_candidate": "process_states",
                "process_states_mapping_contract_id": "C08",
                "process_states_live_projection_enabled": False,
                "process_states_live_projection_mode": "disabled",
                "process_states_live_projection_apply_limit": 0,
                "process_states_live_projection_allow_review_required": False,
                "control_methods_mapping_contract_id": "C17",
            },
            "worker_gateway": {
                "enabled": True,
                "topic_root_override": None,
                "default_qos": 1,
            },
            "datastore": {"device_state_source": "main"},
        }

        settings = build_runtime_settings(app_config=app_config, project_root=tmp)
        self.assertEqual(settings["config_source"]["mode"], "cud_compiled")
        self.assertTrue(settings["config_source"]["legacy_json_path"].endswith("config/___device_state_data.json"))
        self.assertIn(compiler_root, settings["config_source"]["compiler_python_paths"])
        self.assertFalse(settings["config_source"]["reverse_compile_enabled"])
        self.assertTrue(settings["config_source"]["reverse_compile_output_dir"].endswith("config/decompiled"))
        self.assertFalse(settings["config_source"]["runtime_change_capture_enabled"])
        self.assertTrue(settings["config_source"]["runtime_change_capture_dir"].endswith("config/runtime_change_sets"))
        self.assertEqual(settings["config_source"]["next_legacy_projection_candidate"], "process_states")
        self.assertTrue(settings["contract_registry"]["loaded"])
        self.assertEqual(settings["config_source"]["process_states_mapping_contract_id"], "C08")
        self.assertEqual(settings["config_source"]["control_methods_mapping_contract_id"], "C17")
        self.assertTrue(settings["config_source"]["process_states_mapping_contract_path"].endswith("contracts/runtime/PROCESS_STATES_MAPPING_CONTRACT_V1.yaml"))
        self.assertTrue(settings["config_source"]["control_methods_mapping_contract_path"].endswith("contracts/runtime/CONTROL_METHODS_MAPPING_CONTRACT_V1.yaml"))
        self.assertEqual(settings["config_source"]["process_states_mapping_contract"]["source_of_truth"], "runtime_instance")
        self.assertFalse(settings["config_source"]["process_states_live_projection_enabled"])
        self.assertEqual(settings["config_source"]["process_states_live_projection_mode"], "disabled")
        self.assertEqual(settings["config_source"]["process_states_live_projection_apply_limit"], 0)
        self.assertFalse(settings["config_source"]["process_states_live_projection_allow_review_required"])
        self.assertTrue(settings["worker_gateway"]["enabled"])
        self.assertEqual(settings["worker_gateway"]["topic_root_effective"], "xserver/v1")


if __name__ == "__main__":
    unittest.main()
