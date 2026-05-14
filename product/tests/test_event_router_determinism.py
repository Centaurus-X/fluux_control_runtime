# -*- coding: utf-8 -*-

# tests/test_event_router_determinism.py

"""
Unit tests for the refactored event router:

- PSM has been fully removed from the default routing table and from the
  default broadcast rules.
- Deterministic hierarchical priority ordering of primary targets.
- Cross-domain resolution: an event on Sensor_1 (wired to Ctrl_1) can
  deterministically fan out to Actor_2 / Ctrl_2 targets.
- Inline cross-domain targets carried on the event itself.

The tests are fully self-contained: no real queues, no threads, no MQTT.
They exercise the pure functions that back the router.
"""

import os
import sys
import unittest

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core._event_priority import (
    classify_target,
    order_targets_deterministic,
    PRIO_SYSTEM_CRITICAL,
    PRIO_STATE_WRITER,
    PRIO_CONTROLLER,
    PRIO_TELEMETRY,
    PRIO_DEFAULT,
    DOMAIN_UNKNOWN,
)
from src.core._cross_domain_resolver import (
    normalize_mapping,
    resolve_targets,
    extract_cross_domain_target_names,
    combine_cross_domain_targets,
    extract_source_key,
)
from src.core import sync_event_router as router


# =============================================================================
# Priority classification
# =============================================================================

class TestPriorityClassification(unittest.TestCase):

    def test_heartbeat_is_system_critical(self):
        priority, _domain = classify_target("heartbeat")
        self.assertEqual(priority, PRIO_SYSTEM_CRITICAL)

    def test_state_writer_beats_controller(self):
        state_prio, _ = classify_target("state")
        ctrl_prio, _ = classify_target("controller")
        self.assertLess(state_prio, ctrl_prio)

    def test_controller_beats_telemetry(self):
        ctrl_prio, _ = classify_target("controller")
        tele_prio, _ = classify_target("telemetry")
        self.assertLess(ctrl_prio, tele_prio)

    def test_unknown_target_is_default(self):
        priority, domain = classify_target("this_is_not_a_real_target")
        self.assertEqual(priority, PRIO_DEFAULT)
        self.assertEqual(domain, DOMAIN_UNKNOWN)

    def test_none_and_empty_are_default(self):
        self.assertEqual(classify_target(None)[0], PRIO_DEFAULT)
        self.assertEqual(classify_target("")[0], PRIO_DEFAULT)
        self.assertEqual(classify_target("   ")[0], PRIO_DEFAULT)


# =============================================================================
# Deterministic ordering
# =============================================================================

class TestDeterministicOrdering(unittest.TestCase):

    def test_strictly_sorted_by_priority(self):
        inputs = ("telemetry", "controller", "state", "heartbeat")
        ordered = order_targets_deterministic(inputs)
        self.assertEqual(ordered, ("heartbeat", "state", "controller", "telemetry"))

    def test_same_result_regardless_of_input_order(self):
        perm_a = ("controller", "state", "telemetry", "heartbeat")
        perm_b = ("heartbeat", "telemetry", "state", "controller")
        perm_c = ("state", "heartbeat", "controller", "telemetry")
        self.assertEqual(
            order_targets_deterministic(perm_a),
            order_targets_deterministic(perm_b),
        )
        self.assertEqual(
            order_targets_deterministic(perm_b),
            order_targets_deterministic(perm_c),
        )

    def test_duplicates_are_removed(self):
        inputs = ("state", "controller", "state", "controller", "controller")
        ordered = order_targets_deterministic(inputs)
        self.assertEqual(ordered, ("state", "controller"))

    def test_unknown_targets_land_at_the_end(self):
        inputs = ("zzz_unknown", "state", "heartbeat")
        ordered = order_targets_deterministic(inputs)
        self.assertEqual(ordered[:2], ("heartbeat", "state"))
        self.assertEqual(ordered[2], "zzz_unknown")

    def test_empty_returns_empty(self):
        self.assertEqual(order_targets_deterministic(()), ())
        self.assertEqual(order_targets_deterministic(None), ())


# =============================================================================
# Cross-domain resolver
# =============================================================================

class TestCrossDomainResolver(unittest.TestCase):

    def _mapping(self):
        return normalize_mapping({
            "sensor:Sensor_1": [
                {"target": "controller", "scope": "Ctrl_2", "priority": 10},
                {"target": "automation", "scope": "Ctrl_2", "priority": 20},
            ],
            "actor:Actor_2": [
                {"target": "controller", "scope": "Ctrl_2", "priority": 10},
            ],
        })

    def test_sensor_on_ctrl1_routes_to_ctrl2_targets(self):
        mapping = self._mapping()
        event = {"sensor_id": "Sensor_1", "value": 42}
        targets = extract_cross_domain_target_names(event, mapping)
        self.assertEqual(targets, ("controller", "automation"))

    def test_source_key_detection_modes(self):
        # sensor_id
        self.assertEqual(extract_source_key({"sensor_id": "S1"}), "sensor:S1")
        # actor_id
        self.assertEqual(extract_source_key({"actor_id": "A2"}), "actor:A2")
        # explicit cross_domain_source
        self.assertEqual(
            extract_source_key({"cross_domain_source": {"kind": "sensor", "id": "S1"}}),
            "sensor:S1",
        )
        # entity_id + entity_kind
        self.assertEqual(
            extract_source_key({"entity_id": "E9", "entity_kind": "trigger"}),
            "trigger:E9",
        )

    def test_resolve_unknown_source_returns_empty(self):
        mapping = self._mapping()
        self.assertEqual(extract_cross_domain_target_names({"other": 1}, mapping), ())

    def test_inline_targets_are_respected(self):
        event = {
            "sensor_id": "Sensor_99",
            "cross_domain_targets": [
                {"target": "controller", "scope": "Ctrl_3", "priority": 5},
                {"target": "telemetry", "priority": 70},
            ],
        }
        targets = combine_cross_domain_targets(event, {})
        self.assertEqual(targets, ("controller", "telemetry"))

    def test_inline_plus_mapping_combined_deterministically(self):
        mapping = self._mapping()
        event = {
            "sensor_id": "Sensor_1",
            "cross_domain_targets": [
                {"target": "modbus", "priority": 60},
            ],
        }
        combined = combine_cross_domain_targets(event, mapping)
        # inline first (modbus), then mapping-based (controller, automation)
        self.assertEqual(combined, ("modbus", "controller", "automation"))

    def test_disabled_entries_are_dropped(self):
        mapping = normalize_mapping({
            "sensor:S1": [
                {"target": "controller", "enabled": False},
                {"target": "telemetry", "enabled": True},
            ],
        })
        targets = extract_cross_domain_target_names({"sensor_id": "S1"}, mapping)
        self.assertEqual(targets, ("telemetry",))


# =============================================================================
# PSM removal — routing table + broadcast rules
# =============================================================================

class TestPSMRemoval(unittest.TestCase):

    def test_default_routing_table_has_no_psm_keys(self):
        table = router.build_default_routing_table()
        for forbidden in ("psm", "process_state_management", "process_manager", "process", "proc", "process_handler"):
            self.assertNotIn(
                forbidden,
                table,
                "Default routing table must not contain '%s'" % forbidden,
            )

    def test_default_broadcast_rules_contain_no_psm_rule(self):
        rules = router.build_default_broadcast_rules(
            queue_registry={
                "queue_event_proc": object(),
                "queue_event_ti": object(),
                "queue_event_worker": object(),
                "queue_event_pc": object(),
                "queue_event_state": object(),
                "queue_event_ds": object(),
            },
            enable_process_broadcast=True,  # must be ignored
            enable_telemetry_mirror=True,
            enable_worker_mirror=True,
            enable_config_mirrors=True,
            enable_automation_status_mirror=True,
        )
        rule_names = tuple(r.get("name", "") for r in rules)
        self.assertNotIn("broadcast_to_process_state_management", rule_names)
        # But mirrors that are unrelated to PSM must still be present.
        self.assertIn("mirror_to_telemetry", rule_names)

    def test_enable_process_broadcast_is_a_noop(self):
        # Same rules regardless of the flag.
        queue_registry = {
            "queue_event_proc": object(),
            "queue_event_ti": object(),
            "queue_event_state": object(),
            "queue_event_ds": object(),
            "queue_event_pc": object(),
        }
        on = router.build_default_broadcast_rules(
            queue_registry=queue_registry,
            enable_process_broadcast=True,
        )
        off = router.build_default_broadcast_rules(
            queue_registry=queue_registry,
            enable_process_broadcast=False,
        )
        self.assertEqual(
            tuple(r.get("name", "") for r in on),
            tuple(r.get("name", "") for r in off),
        )


# =============================================================================
# Deterministic primary dispatch — integration at the pure function level
# =============================================================================

class FakeQueue(object):
    """
    Minimal dict-like stand-in that captures put_nowait calls.
    We deliberately avoid OOP-heavy patterns; this class exists only
    because the evt_interface expects a queue-ish object.
    """

    def __init__(self):
        self.items = []

    def put_nowait(self, item):
        self.items.append(item)

    def put(self, item, block=True, timeout=None):
        self.items.append(item)


class TestDeterministicPrimaryDispatch(unittest.TestCase):

    def _make_router_context(self, cross_domain_mapping=None):
        # One fake queue per distinct queue_key our router will try to reach.
        queues = {
            "queue_event_state": FakeQueue(),
            "queue_event_ds": FakeQueue(),
            "queue_event_mbc": FakeQueue(),
            "queue_event_mba": FakeQueue(),
            "queue_event_pc": FakeQueue(),
            "queue_event_mbh": FakeQueue(),
            "queue_event_ti": FakeQueue(),
            "queue_event_worker": FakeQueue(),
            "queue_event_ws": FakeQueue(),
            "queue_event_dc": FakeQueue(),
        }
        routing_table = router.build_default_routing_table()
        broadcast_rules = router.build_default_broadcast_rules(queue_registry=queues)
        return router.create_router_context(
            queue_registry=queues,
            routing_table=routing_table,
            broadcast_rules=broadcast_rules,
            router_name="TestRouter",
            cross_domain_mapping=cross_domain_mapping,
        )

    def test_primary_targets_are_ordered_by_priority(self):
        ctx = self._make_router_context()
        event = {
            "target": ["telemetry", "controller", "state"],
            "event_type": "SENSOR_UPDATE",
            "payload": {"value": 3.14},
        }
        ordered = router.build_deterministic_primary_targets(event, ctx)
        self.assertEqual(ordered, ("state", "controller", "telemetry"))

    def test_cross_domain_is_merged_after_primary_but_priority_sorted(self):
        mapping = normalize_mapping({
            "sensor:Sensor_1": [
                {"target": "controller", "priority": 30},
                {"target": "automation", "priority": 40},
            ],
        })
        ctx = self._make_router_context(cross_domain_mapping=mapping)
        event = {
            "target": "state",
            "sensor_id": "Sensor_1",
            "event_type": "SENSOR_UPDATE",
        }
        ordered = router.build_deterministic_primary_targets(event, ctx)
        # state (10) < controller (30) < automation (40)
        self.assertEqual(ordered, ("state", "controller", "automation"))

    def test_dispatch_deterministic_primary_writes_queues_in_order(self):
        mapping = normalize_mapping({
            "sensor:Sensor_1": [
                {"target": "controller", "priority": 30},
                {"target": "automation", "priority": 40},
            ],
        })
        ctx = self._make_router_context(cross_domain_mapping=mapping)
        event = {
            "target": "state",
            "sensor_id": "Sensor_1",
            "event_type": "SENSOR_UPDATE",
            "payload": {"v": 1},
        }
        results = router.dispatch_deterministic_primary(
            data=event,
            router_context=ctx,
            sent_queue_keys=set(),
        )
        # Expect exactly three dispatch results: state, controller, automation.
        self.assertEqual(len(results), 3)
        dispatched_targets = tuple(r.get("target") for r in results)
        self.assertEqual(dispatched_targets, ("state", "controller", "automation"))

        # And the fake queues must have received exactly one item each.
        self.assertEqual(len(ctx["queues"]["queue_event_state"].items), 1)
        self.assertEqual(len(ctx["queues"]["queue_event_mbc"].items), 1)
        self.assertEqual(len(ctx["queues"]["queue_event_mba"].items), 1)

    def test_no_target_and_no_cross_domain_yields_empty(self):
        ctx = self._make_router_context()
        results = router.dispatch_deterministic_primary(
            data={"event_type": "PING"},
            router_context=ctx,
            sent_queue_keys=set(),
        )
        self.assertEqual(results, ())

    def test_inline_cross_domain_targets_alone_drive_dispatch(self):
        ctx = self._make_router_context()
        event = {
            "event_type": "RULE_OUT",
            "cross_domain_targets": [
                {"target": "controller", "priority": 30},
                {"target": "telemetry", "priority": 70},
            ],
        }
        ordered = router.build_deterministic_primary_targets(event, ctx)
        self.assertEqual(ordered, ("controller", "telemetry"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
