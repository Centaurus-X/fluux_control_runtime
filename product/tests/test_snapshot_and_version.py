# -*- coding: utf-8 -*-
# tests/test_snapshot_and_version.py
import unittest
from src.core import _snapshot_freeze as sf
from src.core import _state_version as sv


def make_settings(freeze=False, version=False):
    return {"worker_runtime": {
        "snapshot_freeze_enabled": freeze,
        "state_version_enabled": version,
    }}


class TestSnapshotFreeze(unittest.TestCase):
    def test_disabled_returns_live(self):
        res = {"config_data": {"a": 1}, "sensor_values": {"s": 9}}
        snap = sf.wrap_tick_inputs(make_settings(freeze=False), res)
        self.assertFalse(snap["frozen"])
        self.assertIs(snap["data"], res)

    def test_enabled_freezes_deepcopy(self):
        res = {"config_data": {"a": {"nested": 1}}, "sensor_values": {"s": 9}}
        snap = sf.wrap_tick_inputs(make_settings(freeze=True), res)
        self.assertTrue(snap["frozen"])
        # mutate live -> snapshot unchanged
        res["config_data"]["a"]["nested"] = 999
        self.assertEqual(sf.read_from(snap, "config_data")["a"]["nested"], 1)

    def test_read_from_disabled(self):
        res = {"config_data": {"k": "v"}}
        snap = sf.wrap_tick_inputs(make_settings(freeze=False), res)
        self.assertEqual(sf.read_from(snap, "config_data"), {"k": "v"})

    def test_custom_keys(self):
        res = {"config_data": {}, "actuator_values": {"x": 1}, "unused": {"y": 2}}
        snap = sf.build_snapshot(res, keys=("actuator_values",))
        self.assertEqual(snap["keys"], ("actuator_values",))
        self.assertNotIn("unused", snap["data"])

    def test_missing_resources_ok(self):
        snap = sf.wrap_tick_inputs(make_settings(freeze=True), {})
        self.assertTrue(snap["frozen"])
        self.assertEqual(snap["data"], {})


class TestStateVersion(unittest.TestCase):
    def setUp(self):
        self.res = {}
        sv.reset_for_tests()

    def test_disabled_stamp_noop(self):
        env = {"resource_key": "config_data", "payload": {"x": 1}}
        out = sv.stamp_patch(make_settings(version=False), self.res, env)
        self.assertNotIn("state_version", out)
        self.assertEqual(sv.current_version(self.res, "config_data"), 0)

    def test_enabled_stamp_bumps(self):
        env = {"resource_key": "config_data", "payload": {"x": 1}}
        out = sv.stamp_patch(make_settings(version=True), self.res, env)
        self.assertEqual(out["state_version"], 1)
        self.assertEqual(sv.current_version(self.res, "config_data"), 1)

    def test_monotonic_bump(self):
        s = make_settings(version=True)
        for expected in (1, 2, 3, 4):
            out = sv.stamp_patch(s, self.res, {"resource_key": "config_data"})
            self.assertEqual(out["state_version"], expected)

    def test_per_resource_counters(self):
        s = make_settings(version=True)
        sv.stamp_patch(s, self.res, {"resource_key": "config_data"})
        sv.stamp_patch(s, self.res, {"resource_key": "config_data"})
        sv.stamp_patch(s, self.res, {"resource_key": "actuator_values"})
        self.assertEqual(sv.current_version(self.res, "config_data"), 2)
        self.assertEqual(sv.current_version(self.res, "actuator_values"), 1)

    def test_validate_based_on_disabled(self):
        ok, reason = sv.validate_based_on(make_settings(version=False), self.res, {"based_on_state_version": 99})
        self.assertTrue(ok)

    def test_validate_based_on_stale(self):
        s = make_settings(version=True)
        for _ in range(5):
            sv.stamp_patch(s, self.res, {"resource_key": "config_data"})
        ok, reason = sv.validate_based_on(s, self.res, {"resource_key": "config_data", "based_on_state_version": 1})
        self.assertFalse(ok)
        self.assertIn("stale", reason)

    def test_validate_based_on_fresh(self):
        s = make_settings(version=True)
        sv.stamp_patch(s, self.res, {"resource_key": "config_data"})
        ok, reason = sv.validate_based_on(s, self.res, {"resource_key": "config_data", "based_on_state_version": 1})
        self.assertTrue(ok)

    def test_non_dict_envelope_passthrough(self):
        self.assertEqual(sv.stamp_patch(make_settings(version=True), self.res, "not a dict"), "not a dict")


if __name__ == "__main__":
    unittest.main()
