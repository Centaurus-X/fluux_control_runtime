# -*- coding: utf-8 -*-

# src/core/_replication_commit_validator.py

"""
Commit-Contract Validator for replicated configuration snapshots.

Purpose
-------
This module validates candidate configuration snapshots *before* they are
committed into the live replicated heap_sync state.  The focus is on hard
topology/ownership invariants and on reference integrity for the tables that
drive TM, PSM, Automation and the controller runtime.

Design principles
-----------------
- no OOP
- deterministic report structure
- explicit node-ownership semantics via local_node.active_controllers
- safe preview support for CONFIG_PATCH before the real commit happens
"""

import copy

from functools import partial

try:
    from src.libraries._heap_sync_final_template import (
        apply_command_to_state,
        deepcopy_json,
    )
except Exception:
    from _heap_sync_final_template import (
        apply_command_to_state,
        deepcopy_json,
    )


def _as_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _as_str(value, default=""):
    try:
        return str(value)
    except Exception:
        return str(default)


def _normalize_int_list(value):
    items = []
    if value is None:
        return items
    if not isinstance(value, (list, tuple, set)):
        return items
    for item in value:
        num = _as_int(item, None)
        if num is not None:
            items.append(num)
    return items


def _normalize_dict_rows(value):
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _make_report(node_id):
    return {
        "ok": True,
        "node_id": node_id,
        "effective_node_id": None,
        "owned_controller_ids": [],
        "errors": [],
        "warnings": [],
        "summary": {},
    }


def _add_error(report, code, message, table=None, pk=None, field=None, ref=None):
    report["errors"].append({
        "code": str(code),
        "message": str(message),
        "table": None if table is None else str(table),
        "pk": pk,
        "field": None if field is None else str(field),
        "ref": ref,
    })
    report["ok"] = False


def _add_warning(report, code, message, table=None, pk=None, field=None, ref=None):
    report["warnings"].append({
        "code": str(code),
        "message": str(message),
        "table": None if table is None else str(table),
        "pk": pk,
        "field": None if field is None else str(field),
        "ref": ref,
    })


def _index_rows_by_int_id(report, config, table_name, id_field):
    rows = _normalize_dict_rows((config or {}).get(table_name))
    index = {}

    for pos, row in enumerate(rows):
        raw_id = row.get(id_field)
        row_id = _as_int(raw_id, None)
        if row_id is None:
            _add_error(
                report,
                "{0}.id_missing".format(table_name),
                "{0}: row without valid {1}".format(table_name, id_field),
                table=table_name,
                pk=raw_id if raw_id is not None else "row:{0}".format(pos),
                field=id_field,
            )
            continue
        if row_id in index:
            _add_error(
                report,
                "{0}.duplicate_id".format(table_name),
                "{0}: duplicate {1}={2}".format(table_name, id_field, row_id),
                table=table_name,
                pk=row_id,
                field=id_field,
            )
            continue
        index[row_id] = row

    return index


def _lookup_local_nodes(config):
    local = (config or {}).get("local_node")
    if isinstance(local, dict):
        return [local]
    if isinstance(local, list):
        return [row for row in local if isinstance(row, dict)]
    return []


def _resolve_effective_local_nodes(report, config, node_id):
    local_nodes = _lookup_local_nodes(config)

    if not local_nodes:
        _add_error(
            report,
            "local_node.missing",
            "local_node table is missing or empty; ownership cannot be resolved",
            table="local_node",
        )
        return []

    if node_id is None:
        if len(local_nodes) == 1:
            report["effective_node_id"] = _as_str(local_nodes[0].get("node_id"), "")
            return [local_nodes[0]]
        report["effective_node_id"] = None
        return list(local_nodes)

    selected = []
    node_id_s = _as_str(node_id, "")
    for row in local_nodes:
        if _as_str(row.get("node_id"), "") == node_id_s:
            selected.append(row)

    if selected:
        report["effective_node_id"] = node_id_s
        return selected

    if len(local_nodes) == 1:
        fallback_node_id = _as_str(local_nodes[0].get("node_id"), "")
        report["effective_node_id"] = fallback_node_id
        _add_warning(
            report,
            "local_node.fallback",
            "node_id={0} not declared in local_node; using only local entry {1}".format(node_id_s, fallback_node_id),
            table="local_node",
        )
        return [local_nodes[0]]

    _add_error(
        report,
        "local_node.node_missing",
        "node_id={0} not declared in local_node and multiple entries exist".format(node_id_s),
        table="local_node",
    )
    return []


def _resolve_owned_controllers(report, config, controller_index, node_id):
    selected_nodes = _resolve_effective_local_nodes(report, config, node_id)
    owned = set()

    for row in selected_nodes:
        local_node_id = _as_str(row.get("node_id"), "")
        if not local_node_id:
            _add_error(
                report,
                "local_node.node_id_missing",
                "local_node row without node_id",
                table="local_node",
            )
        raw = row.get("active_controllers")
        if not isinstance(raw, list):
            _add_error(
                report,
                "local_node.active_controllers_invalid",
                "local_node[{0}] active_controllers must be a list".format(local_node_id or "?"),
                table="local_node",
                pk=local_node_id or None,
                field="active_controllers",
            )
            continue

        seen = set()
        for item in raw:
            controller_id = _as_int(item, None)
            if controller_id is None:
                _add_error(
                    report,
                    "local_node.active_controller_invalid",
                    "local_node[{0}] contains non-integer active controller reference {1!r}".format(local_node_id or "?", item),
                    table="local_node",
                    pk=local_node_id or None,
                    field="active_controllers",
                    ref=item,
                )
                continue
            if controller_id in seen:
                _add_error(
                    report,
                    "local_node.active_controller_duplicate",
                    "local_node[{0}] duplicates controller_id={1}".format(local_node_id or "?", controller_id),
                    table="local_node",
                    pk=local_node_id or None,
                    field="active_controllers",
                    ref=controller_id,
                )
                continue
            seen.add(controller_id)
            owned.add(controller_id)
            if controller_id not in controller_index:
                _add_error(
                    report,
                    "local_node.controller_missing",
                    "local_node[{0}] references unknown controller_id={1}".format(local_node_id or "?", controller_id),
                    table="local_node",
                    pk=local_node_id or None,
                    field="active_controllers",
                    ref=controller_id,
                )

    report["owned_controller_ids"] = sorted(owned)
    return owned


def _controller_id_for_row(row):
    if not isinstance(row, dict):
        return None
    return _as_int(row.get("controller_id"), None)


def _check_owned_reference(report, controller_id, table_name, pk, field_name, ref_id, owned_controller_ids):
    if not owned_controller_ids:
        return
    if controller_id in owned_controller_ids:
        return
    _add_error(
        report,
        "{0}.ownership".format(table_name),
        "{0}[{1}] references {2}={3} on controller_id={4}, which is outside local ownership".format(
            table_name, pk, field_name, ref_id, controller_id
        ),
        table=table_name,
        pk=pk,
        field=field_name,
        ref=ref_id,
    )


def _validate_controller_topology(report, config, controller_index, owned_controller_ids):
    virtual_index = _index_rows_by_int_id(report, config, "virtual_controllers", "virt_controller_id")
    active_virtual_by_controller = {}

    for virt_controller_id, row in virtual_index.items():
        controller_id = _controller_id_for_row(row)
        if controller_id is None:
            _add_error(
                report,
                "virtual_controllers.controller_missing",
                "virtual_controllers[{0}] without valid controller_id".format(virt_controller_id),
                table="virtual_controllers",
                pk=virt_controller_id,
                field="controller_id",
            )
            continue
        if controller_id not in controller_index:
            _add_error(
                report,
                "virtual_controllers.controller_unknown",
                "virtual_controllers[{0}] references unknown controller_id={1}".format(virt_controller_id, controller_id),
                table="virtual_controllers",
                pk=virt_controller_id,
                field="controller_id",
                ref=controller_id,
            )
            continue

        virt_active = bool(row.get("virt_active", False))
        if virt_active:
            active_virtual_by_controller.setdefault(controller_id, []).append(virt_controller_id)

    for controller_id, virt_ids in active_virtual_by_controller.items():
        if len(virt_ids) > 1:
            _add_error(
                report,
                "virtual_controllers.multiple_active",
                "controller_id={0} has multiple active virtual controllers {1}".format(controller_id, virt_ids),
                table="virtual_controllers",
                pk=controller_id,
                field="virt_active",
            )


def _validate_sensor_actuator_tables(report, config, controller_index, owned_controller_ids):
    sensors = _index_rows_by_int_id(report, config, "sensors", "sensor_id")
    actuators = _index_rows_by_int_id(report, config, "actuators", "actuator_id")

    for sensor_id, row in sensors.items():
        controller_id = _controller_id_for_row(row)
        if controller_id is None:
            _add_error(
                report,
                "sensors.controller_missing",
                "sensors[{0}] without valid controller_id".format(sensor_id),
                table="sensors",
                pk=sensor_id,
                field="controller_id",
            )
            continue
        if controller_id not in controller_index:
            _add_error(
                report,
                "sensors.controller_unknown",
                "sensors[{0}] references unknown controller_id={1}".format(sensor_id, controller_id),
                table="sensors",
                pk=sensor_id,
                field="controller_id",
                ref=controller_id,
            )
            continue

    for actuator_id, row in actuators.items():
        controller_id = _controller_id_for_row(row)
        if controller_id is None:
            _add_error(
                report,
                "actuators.controller_missing",
                "actuators[{0}] without valid controller_id".format(actuator_id),
                table="actuators",
                pk=actuator_id,
                field="controller_id",
            )
            continue
        if controller_id not in controller_index:
            _add_error(
                report,
                "actuators.controller_unknown",
                "actuators[{0}] references unknown controller_id={1}".format(actuator_id, controller_id),
                table="actuators",
                pk=actuator_id,
                field="controller_id",
                ref=controller_id,
            )
            continue

    return sensors, actuators


def _validate_process_states(
    report,
    config,
    sensors,
    actuators,
    owned_controller_ids,
):
    states = _index_rows_by_int_id(report, config, "process_states", "state_id")
    processes = _index_rows_by_int_id(report, config, "processes", "p_id")
    values = _index_rows_by_int_id(report, config, "process_values", "value_id")
    modes = _index_rows_by_int_id(report, config, "process_modes", "mode_id")
    mappings = _index_rows_by_int_id(report, config, "schema_mappings", "mapping_id")
    pid_configs = _index_rows_by_int_id(report, config, "pid_configs", "pid_config_id")
    control_methods = _index_rows_by_int_id(report, config, "control_methods", "method_id")
    timers = _index_rows_by_int_id(report, config, "timers", "timer_id")

    for state_id, row in states.items():
        process_id = _as_int(row.get("p_id"), None)
        value_id = _as_int(row.get("value_id"), None)
        mode_id = _as_int(row.get("mode_id"), None)
        mapping_id = _as_int(row.get("mapping_id"), None)

        if process_id not in processes:
            _add_error(
                report,
                "process_states.process_ref",
                "process_states[{0}] references unknown p_id={1}".format(state_id, process_id),
                table="process_states",
                pk=state_id,
                field="p_id",
                ref=process_id,
            )
        if value_id not in values:
            _add_error(
                report,
                "process_states.value_ref",
                "process_states[{0}] references unknown value_id={1}".format(state_id, value_id),
                table="process_states",
                pk=state_id,
                field="value_id",
                ref=value_id,
            )
        if mode_id not in modes:
            _add_error(
                report,
                "process_states.mode_ref",
                "process_states[{0}] references unknown mode_id={1}".format(state_id, mode_id),
                table="process_states",
                pk=state_id,
                field="mode_id",
                ref=mode_id,
            )
        if mapping_id not in mappings:
            _add_error(
                report,
                "process_states.mapping_ref",
                "process_states[{0}] references unknown mapping_id={1}".format(state_id, mapping_id),
                table="process_states",
                pk=state_id,
                field="mapping_id",
                ref=mapping_id,
            )

        raw_actuators = row.get("actuator_ids") or []
        if not isinstance(raw_actuators, list):
            _add_error(
                report,
                "process_states.actuator_ids_invalid",
                "process_states[{0}] actuator_ids must be a list".format(state_id),
                table="process_states",
                pk=state_id,
                field="actuator_ids",
            )
            raw_actuators = []

        for actuator_id in raw_actuators:
            actuator_id_i = _as_int(actuator_id, None)
            if actuator_id_i not in actuators:
                _add_error(
                    report,
                    "process_states.actuator_ref",
                    "process_states[{0}] references unknown actuator_id={1}".format(state_id, actuator_id_i),
                    table="process_states",
                    pk=state_id,
                    field="actuator_ids",
                    ref=actuator_id_i,
                )
                continue
            controller_id = _controller_id_for_row(actuators[actuator_id_i])
            _check_owned_reference(
                report,
                controller_id,
                "process_states",
                state_id,
                "actuator_ids",
                actuator_id_i,
                owned_controller_ids,
            )

        raw_sensors = row.get("sensor_ids") or []
        if not isinstance(raw_sensors, list):
            _add_error(
                report,
                "process_states.sensor_ids_invalid",
                "process_states[{0}] sensor_ids must be a list".format(state_id),
                table="process_states",
                pk=state_id,
                field="sensor_ids",
            )
            raw_sensors = []

        for sensor_id in raw_sensors:
            sensor_id_i = _as_int(sensor_id, None)
            if sensor_id_i not in sensors:
                _add_error(
                    report,
                    "process_states.sensor_ref",
                    "process_states[{0}] references unknown sensor_id={1}".format(state_id, sensor_id_i),
                    table="process_states",
                    pk=state_id,
                    field="sensor_ids",
                    ref=sensor_id_i,
                )
                continue
            controller_id = _controller_id_for_row(sensors[sensor_id_i])
            _check_owned_reference(
                report,
                controller_id,
                "process_states",
                state_id,
                "sensor_ids",
                sensor_id_i,
                owned_controller_ids,
            )

        parameters = row.get("parameters")
        if parameters is not None and not isinstance(parameters, dict):
            _add_error(
                report,
                "process_states.parameters_invalid",
                "process_states[{0}] parameters must be a dict when present".format(state_id),
                table="process_states",
                pk=state_id,
                field="parameters",
            )
        elif isinstance(parameters, dict) and "timer_id" in parameters:
            timer_id = _as_int(parameters.get("timer_id"), None)
            if timer_id not in timers:
                _add_error(
                    report,
                    "process_states.timer_ref",
                    "process_states[{0}] parameters.timer_id={1} does not exist".format(state_id, timer_id),
                    table="process_states",
                    pk=state_id,
                    field="parameters.timer_id",
                    ref=timer_id,
                )

        strategy = row.get("control_strategy")
        if strategy is None:
            continue
        if not isinstance(strategy, dict):
            _add_error(
                report,
                "process_states.control_strategy_invalid",
                "process_states[{0}] control_strategy must be a dict or null".format(state_id),
                table="process_states",
                pk=state_id,
                field="control_strategy",
            )
            continue

        strategy_type = _as_str(strategy.get("type"), "")
        if strategy_type == "PID":
            pid_config_id = _as_int(strategy.get("pid_config_id"), None)
            if pid_config_id not in pid_configs:
                _add_error(
                    report,
                    "process_states.pid_config_ref",
                    "process_states[{0}] references unknown pid_config_id={1}".format(state_id, pid_config_id),
                    table="process_states",
                    pk=state_id,
                    field="control_strategy.pid_config_id",
                    ref=pid_config_id,
                )
            continue

        if strategy_type == "control_method":
            method_id = _as_int(strategy.get("method_id"), None)
            if method_id not in control_methods:
                _add_error(
                    report,
                    "process_states.control_method_ref",
                    "process_states[{0}] references unknown method_id={1}".format(state_id, method_id),
                    table="process_states",
                    pk=state_id,
                    field="control_strategy.method_id",
                    ref=method_id,
                )
            continue

        _add_error(
            report,
            "process_states.control_strategy_type",
            "process_states[{0}] uses unsupported control_strategy.type={1!r}".format(state_id, strategy_type),
            table="process_states",
            pk=state_id,
            field="control_strategy.type",
            ref=strategy_type,
        )

    return states


def _validate_control_tables(report, config):
    templates = _index_rows_by_int_id(report, config, "control_method_templates", "template_id")
    methods = _index_rows_by_int_id(report, config, "control_methods", "method_id")
    for method_id, row in methods.items():
        template_id = _as_int(row.get("template_id"), None)
        if template_id not in templates:
            _add_error(
                report,
                "control_methods.template_ref",
                "control_methods[{0}] references unknown template_id={1}".format(method_id, template_id),
                table="control_methods",
                pk=method_id,
                field="template_id",
                ref=template_id,
            )


def _validate_automation_tables(report, config, states, sensors, actuators, owned_controller_ids):
    engines = _index_rows_by_int_id(report, config, "automation_engines", "engine_id")
    filters = _index_rows_by_int_id(report, config, "automation_filters", "filter_id")
    automations = _index_rows_by_int_id(report, config, "automations", "automation_id")
    rule_sets = _index_rows_by_int_id(report, config, "automation_rule_sets", "rule_id")

    for filter_id, row in filters.items():
        apply_to = row.get("apply_to") or {}
        if apply_to and not isinstance(apply_to, dict):
            _add_error(
                report,
                "automation_filters.apply_to_invalid",
                "automation_filters[{0}] apply_to must be a dict when present".format(filter_id),
                table="automation_filters",
                pk=filter_id,
                field="apply_to",
            )
            continue
        for sensor_id in apply_to.get("sensor_ids") or []:
            sensor_id_i = _as_int(sensor_id, None)
            if sensor_id_i not in sensors:
                _add_error(
                    report,
                    "automation_filters.sensor_ref",
                    "automation_filters[{0}] references unknown sensor_id={1}".format(filter_id, sensor_id_i),
                    table="automation_filters",
                    pk=filter_id,
                    field="apply_to.sensor_ids",
                    ref=sensor_id_i,
                )
                continue
            controller_id = _controller_id_for_row(sensors[sensor_id_i])
            _check_owned_reference(
                report,
                controller_id,
                "automation_filters",
                filter_id,
                "apply_to.sensor_ids",
                sensor_id_i,
                owned_controller_ids,
            )

    for automation_id, row in automations.items():
        engine_id = _as_int(row.get("engine_id"), None)
        if engine_id not in engines:
            _add_error(
                report,
                "automations.engine_ref",
                "automations[{0}] references unknown engine_id={1}".format(automation_id, engine_id),
                table="automations",
                pk=automation_id,
                field="engine_id",
                ref=engine_id,
            )
        for filter_id in row.get("filter_ids") or []:
            filter_id_i = _as_int(filter_id, None)
            if filter_id_i not in filters:
                _add_error(
                    report,
                    "automations.filter_ref",
                    "automations[{0}] references unknown filter_id={1}".format(automation_id, filter_id_i),
                    table="automations",
                    pk=automation_id,
                    field="filter_ids",
                    ref=filter_id_i,
                )

    for rule_id, row in rule_sets.items():
        automation_id = _as_int(row.get("automation_id"), None)
        if automation_id not in automations:
            _add_error(
                report,
                "automation_rule_sets.automation_ref",
                "automation_rule_sets[{0}] references unknown automation_id={1}".format(rule_id, automation_id),
                table="automation_rule_sets",
                pk=rule_id,
                field="automation_id",
                ref=automation_id,
            )

        state_ids = row.get("state_ids")
        if state_ids != "auto":
            if state_ids is not None and not isinstance(state_ids, list):
                _add_error(
                    report,
                    "automation_rule_sets.state_ids_invalid",
                    "automation_rule_sets[{0}] state_ids must be a list or 'auto'".format(rule_id),
                    table="automation_rule_sets",
                    pk=rule_id,
                    field="state_ids",
                )
            else:
                for state_id in state_ids or []:
                    state_id_i = _as_int(state_id, None)
                    if state_id_i not in states:
                        _add_error(
                            report,
                            "automation_rule_sets.state_ref",
                            "automation_rule_sets[{0}] references unknown state_id={1}".format(rule_id, state_id_i),
                            table="automation_rule_sets",
                            pk=rule_id,
                            field="state_ids",
                            ref=state_id_i,
                        )

        for block_name in ("when_all", "when_any"):
            conditions = row.get(block_name) or []
            if not isinstance(conditions, list):
                _add_error(
                    report,
                    "automation_rule_sets.conditions_invalid",
                    "automation_rule_sets[{0}] {1} must be a list".format(rule_id, block_name),
                    table="automation_rule_sets",
                    pk=rule_id,
                    field=block_name,
                )
                continue
            for condition in conditions:
                if not isinstance(condition, dict):
                    _add_error(
                        report,
                        "automation_rule_sets.condition_invalid",
                        "automation_rule_sets[{0}] contains non-dict condition in {1}".format(rule_id, block_name),
                        table="automation_rule_sets",
                        pk=rule_id,
                        field=block_name,
                    )
                    continue
                if "sensor_id" in condition:
                    sensor_id = _as_int(condition.get("sensor_id"), None)
                    if sensor_id not in sensors:
                        _add_error(
                            report,
                            "automation_rule_sets.sensor_ref",
                            "automation_rule_sets[{0}] references unknown sensor_id={1}".format(rule_id, sensor_id),
                            table="automation_rule_sets",
                            pk=rule_id,
                            field="{0}.sensor_id".format(block_name),
                            ref=sensor_id,
                        )
                    else:
                        controller_id = _controller_id_for_row(sensors[sensor_id])
                        _check_owned_reference(
                            report,
                            controller_id,
                            "automation_rule_sets",
                            rule_id,
                            "{0}.sensor_id".format(block_name),
                            sensor_id,
                            owned_controller_ids,
                        )
                if "state_id" in condition:
                    state_id = _as_int(condition.get("state_id"), None)
                    if state_id not in states:
                        _add_error(
                            report,
                            "automation_rule_sets.state_condition_ref",
                            "automation_rule_sets[{0}] references unknown state_id={1} in {2}".format(rule_id, state_id, block_name),
                            table="automation_rule_sets",
                            pk=rule_id,
                            field="{0}.state_id".format(block_name),
                            ref=state_id,
                        )
                if "filter_id" in condition:
                    filter_id = _as_int(condition.get("filter_id"), None)
                    if filter_id not in filters:
                        _add_error(
                            report,
                            "automation_rule_sets.filter_ref",
                            "automation_rule_sets[{0}] references unknown filter_id={1}".format(rule_id, filter_id),
                            table="automation_rule_sets",
                            pk=rule_id,
                            field="{0}.filter_id".format(block_name),
                            ref=filter_id,
                        )

        for block_name in ("do", "else_do"):
            actions = row.get(block_name) or []
            if not isinstance(actions, list):
                _add_error(
                    report,
                    "automation_rule_sets.action_block_invalid",
                    "automation_rule_sets[{0}] {1} must be a list".format(rule_id, block_name),
                    table="automation_rule_sets",
                    pk=rule_id,
                    field=block_name,
                )
                continue
            for action in actions:
                if not isinstance(action, dict):
                    _add_error(
                        report,
                        "automation_rule_sets.action_invalid",
                        "automation_rule_sets[{0}] contains non-dict action in {1}".format(rule_id, block_name),
                        table="automation_rule_sets",
                        pk=rule_id,
                        field=block_name,
                    )
                    continue
                if "actuator_id" in action:
                    actuator_id = _as_int(action.get("actuator_id"), None)
                    if actuator_id not in actuators:
                        _add_error(
                            report,
                            "automation_rule_sets.actuator_ref",
                            "automation_rule_sets[{0}] references unknown actuator_id={1}".format(rule_id, actuator_id),
                            table="automation_rule_sets",
                            pk=rule_id,
                            field="{0}.actuator_id".format(block_name),
                            ref=actuator_id,
                        )
                    else:
                        controller_id = _controller_id_for_row(actuators[actuator_id])
                        _check_owned_reference(
                            report,
                            controller_id,
                            "automation_rule_sets",
                            rule_id,
                            "{0}.actuator_id".format(block_name),
                            actuator_id,
                            owned_controller_ids,
                        )


def _validate_trigger_action_tables(report, config, states, sensors, actuators, owned_controller_ids):
    timers = _index_rows_by_int_id(report, config, "timers", "timer_id")
    values = _index_rows_by_int_id(report, config, "process_values", "value_id")
    processes = _index_rows_by_int_id(report, config, "processes", "p_id")
    triggers = _index_rows_by_int_id(report, config, "triggers", "trigger_id")
    actions = _index_rows_by_int_id(report, config, "actions", "action_id")
    transitions = _index_rows_by_int_id(report, config, "transitions", "transition_id")

    for trigger_id, row in triggers.items():
        trigger_type = _as_str(row.get("trigger_type"), "")
        if trigger_type == "sensor":
            sensor_id = _as_int(row.get("sensor_id"), None)
            if sensor_id not in sensors:
                _add_error(
                    report,
                    "triggers.sensor_ref",
                    "triggers[{0}] references unknown sensor_id={1}".format(trigger_id, sensor_id),
                    table="triggers",
                    pk=trigger_id,
                    field="sensor_id",
                    ref=sensor_id,
                )
            else:
                controller_id = _controller_id_for_row(sensors[sensor_id])
                _check_owned_reference(
                    report,
                    controller_id,
                    "triggers",
                    trigger_id,
                    "sensor_id",
                    sensor_id,
                    owned_controller_ids,
                )

        if trigger_type in ("timer", "schedule"):
            timer_id = _as_int(row.get("timer_id"), None)
            if timer_id not in timers:
                _add_error(
                    report,
                    "triggers.timer_ref",
                    "triggers[{0}] references unknown timer_id={1}".format(trigger_id, timer_id),
                    table="triggers",
                    pk=trigger_id,
                    field="timer_id",
                    ref=timer_id,
                )

        if trigger_type == "compound":
            for ref_trigger_id in row.get("trigger_ids") or []:
                ref_trigger_id_i = _as_int(ref_trigger_id, None)
                if ref_trigger_id_i not in triggers:
                    _add_error(
                        report,
                        "triggers.compound_ref",
                        "triggers[{0}] references unknown trigger_id={1}".format(trigger_id, ref_trigger_id_i),
                        table="triggers",
                        pk=trigger_id,
                        field="trigger_ids",
                        ref=ref_trigger_id_i,
                    )

    for action_id, row in actions.items():
        if "state_id" in row:
            state_id = _as_int(row.get("state_id"), None)
            if state_id not in states:
                _add_error(
                    report,
                    "actions.state_ref",
                    "actions[{0}] references unknown state_id={1}".format(action_id, state_id),
                    table="actions",
                    pk=action_id,
                    field="state_id",
                    ref=state_id,
                )

        if "value_id" in row:
            value_id = _as_int(row.get("value_id"), None)
            if value_id not in values:
                _add_error(
                    report,
                    "actions.value_ref",
                    "actions[{0}] references unknown value_id={1}".format(action_id, value_id),
                    table="actions",
                    pk=action_id,
                    field="value_id",
                    ref=value_id,
                )

        if "p_id" in row:
            process_id = _as_int(row.get("p_id"), None)
            if process_id not in processes:
                _add_error(
                    report,
                    "actions.process_ref",
                    "actions[{0}] references unknown p_id={1}".format(action_id, process_id),
                    table="actions",
                    pk=action_id,
                    field="p_id",
                    ref=process_id,
                )

        if "p_ids" in row:
            raw_p_ids = row.get("p_ids")
            if not isinstance(raw_p_ids, list):
                _add_error(
                    report,
                    "actions.process_ids_invalid",
                    "actions[{0}] p_ids must be a list".format(action_id),
                    table="actions",
                    pk=action_id,
                    field="p_ids",
                )
            else:
                for process_id in raw_p_ids:
                    process_id_i = _as_int(process_id, None)
                    if process_id_i not in processes:
                        _add_error(
                            report,
                            "actions.process_ids_ref",
                            "actions[{0}] references unknown p_id={1} in p_ids".format(action_id, process_id_i),
                            table="actions",
                            pk=action_id,
                            field="p_ids",
                            ref=process_id_i,
                        )

    for transition_id, row in transitions.items():
        for field_name in ("trigger_ids", "action_ids", "undo_action_ids"):
            raw_refs = row.get(field_name) or []
            if not isinstance(raw_refs, list):
                _add_error(
                    report,
                    "transitions.refs_invalid",
                    "transitions[{0}] {1} must be a list".format(transition_id, field_name),
                    table="transitions",
                    pk=transition_id,
                    field=field_name,
                )
                continue
            target_index = triggers if field_name == "trigger_ids" else actions
            for ref_id in raw_refs:
                ref_id_i = _as_int(ref_id, None)
                if ref_id_i not in target_index:
                    _add_error(
                        report,
                        "transitions.ref_missing",
                        "transitions[{0}] references unknown {1}={2}".format(transition_id, field_name[:-1], ref_id_i),
                        table="transitions",
                        pk=transition_id,
                        field=field_name,
                        ref=ref_id_i,
                    )


def _validate_dynamic_rule_engine(report, config, sensors, actions, owned_controller_ids):
    rules = _normalize_dict_rows((config or {}).get("dynamic_rule_engine"))
    for pos, row in enumerate(rules):
        rule_id = row.get("rule_id")
        cond = row.get("cond") or {}
        if cond is not None and not isinstance(cond, (dict, str)):
            _add_error(
                report,
                "dynamic_rule_engine.cond_invalid",
                "dynamic_rule_engine[{0}] cond must be a dict or string".format(rule_id if rule_id is not None else pos),
                table="dynamic_rule_engine",
                pk=rule_id if rule_id is not None else pos,
                field="cond",
            )
            continue
        if isinstance(cond, dict) and "sensor_id" in cond:
            sensor_id = _as_int(cond.get("sensor_id"), None)
            if sensor_id not in sensors:
                _add_error(
                    report,
                    "dynamic_rule_engine.sensor_ref",
                    "dynamic_rule_engine[{0}] references unknown sensor_id={1}".format(rule_id, sensor_id),
                    table="dynamic_rule_engine",
                    pk=rule_id,
                    field="cond.sensor_id",
                    ref=sensor_id,
                )
            else:
                controller_id = _controller_id_for_row(sensors[sensor_id])
                _check_owned_reference(
                    report,
                    controller_id,
                    "dynamic_rule_engine",
                    rule_id,
                    "cond.sensor_id",
                    sensor_id,
                    owned_controller_ids,
                )

        for field_name in ("action_id", "undo_action_id"):
            if field_name not in row:
                continue
            action_id = _as_int(row.get(field_name), None)
            if action_id not in actions:
                _add_error(
                    report,
                    "dynamic_rule_engine.action_ref",
                    "dynamic_rule_engine[{0}] references unknown {1}={2}".format(rule_id, field_name, action_id),
                    table="dynamic_rule_engine",
                    pk=rule_id,
                    field=field_name,
                    ref=action_id,
                )


def _validate_rule_controls(report, config):
    controls = _normalize_dict_rows((config or {}).get("rule_controls"))
    automation_rule_sets = _index_rows_by_int_id(report, config, "automation_rule_sets", "rule_id")
    automations = _index_rows_by_int_id(report, config, "automations", "automation_id")
    automation_filters = _index_rows_by_int_id(report, config, "automation_filters", "filter_id")
    timers = _index_rows_by_int_id(report, config, "timers", "timer_id")
    triggers = _index_rows_by_int_id(report, config, "triggers", "trigger_id")
    actions = _index_rows_by_int_id(report, config, "actions", "action_id")
    transitions = _index_rows_by_int_id(report, config, "transitions", "transition_id")

    lookup = {
        "automation_rule_sets": automation_rule_sets,
        "automations": automations,
        "automation_filters": automation_filters,
        "timers": timers,
        "triggers": triggers,
        "actions": actions,
        "transitions": transitions,
    }

    for pos, row in enumerate(controls):
        rc_id = row.get("rule_controls_id")
        for field_name, target_index in lookup.items():
            refs = row.get(field_name)
            if refs is None:
                continue
            if not isinstance(refs, list):
                _add_error(
                    report,
                    "rule_controls.refs_invalid",
                    "rule_controls[{0}] {1} must be a list".format(rc_id if rc_id is not None else pos, field_name),
                    table="rule_controls",
                    pk=rc_id if rc_id is not None else pos,
                    field=field_name,
                )
                continue
            for ref_id in refs:
                ref_id_i = _as_int(ref_id, None)
                if ref_id_i not in target_index:
                    _add_error(
                        report,
                        "rule_controls.ref_missing",
                        "rule_controls[{0}] references unknown {1}={2}".format(rc_id if rc_id is not None else pos, field_name[:-1], ref_id_i),
                        table="rule_controls",
                        pk=rc_id if rc_id is not None else pos,
                        field=field_name,
                        ref=ref_id_i,
                    )


def build_config_commit_validation_report(config, node_id=None):
    cfg = config if isinstance(config, dict) else {}
    report = _make_report(node_id)

    controller_index = _index_rows_by_int_id(report, cfg, "controllers", "controller_id")
    owned_controller_ids = _resolve_owned_controllers(report, cfg, controller_index, node_id)

    _validate_controller_topology(report, cfg, controller_index, owned_controller_ids)
    sensors, actuators = _validate_sensor_actuator_tables(report, cfg, controller_index, owned_controller_ids)
    states = _validate_process_states(report, cfg, sensors, actuators, owned_controller_ids)
    _validate_control_tables(report, cfg)
    _validate_trigger_action_tables(report, cfg, states, sensors, actuators, owned_controller_ids)
    _validate_automation_tables(report, cfg, states, sensors, actuators, owned_controller_ids)

    actions = _index_rows_by_int_id(report, cfg, "actions", "action_id")
    _validate_dynamic_rule_engine(report, cfg, sensors, actions, owned_controller_ids)
    _validate_rule_controls(report, cfg)

    report["summary"] = {
        "errors": len(report["errors"]),
        "warnings": len(report["warnings"]),
        "controllers": len(controller_index),
        "owned_controllers": len(owned_controller_ids),
        "sensors": len(sensors),
        "actuators": len(actuators),
        "states": len(states),
    }
    report["ok"] = len(report["errors"]) == 0
    return report


def format_validation_report(report, max_errors=12):
    errors = (report or {}).get("errors") or []
    warnings = (report or {}).get("warnings") or []
    effective_node_id = (report or {}).get("effective_node_id")
    parts = [
        "Config commit contract violation"
        if errors else
        "Config commit contract validation OK"
    ]
    if effective_node_id:
        parts.append("node={0}".format(effective_node_id))
    elif (report or {}).get("node_id") is not None:
        parts.append("node={0}".format((report or {}).get("node_id")))
    parts.append("errors={0}".format(len(errors)))
    parts.append("warnings={0}".format(len(warnings)))

    lines = [" | ".join(parts)]

    for entry in errors[:max_errors]:
        lines.append(
            "- [{0}] {1}".format(
                entry.get("code"),
                entry.get("message"),
            )
        )

    remaining = len(errors) - min(len(errors), int(max_errors))
    if remaining > 0:
        lines.append("- ... +{0} weitere Fehler".format(remaining))

    return "\n".join(lines)


def validate_config_commit_or_raise(config, node_id=None):
    report = build_config_commit_validation_report(config, node_id=node_id)
    if report.get("ok"):
        return report
    raise ValueError(format_validation_report(report))


def _dataset_to_preview_state(dataset, schema):
    state = {
        "tables": {},
        "versions": {},
        "global_version": 0,
    }

    for table_name, schema_entry in (schema or {}).items():
        shape = _as_str(schema_entry.get("shape"), "rows")
        state["versions"][table_name] = 0
        value = deepcopy_json((dataset or {}).get(table_name))

        if shape == "rows":
            pk_field = _as_str(schema_entry.get("pk"), "")
            row_map = {}
            if isinstance(value, dict):
                for pk_value, row in value.items():
                    row_map[_as_str(pk_value)] = deepcopy_json(row)
            elif isinstance(value, list):
                for row in value:
                    if not isinstance(row, dict):
                        raise ValueError("preview dataset table {0!r} contains non-dict row".format(table_name))
                    pk_value = _as_str(row.get(pk_field), "")
                    if not pk_value:
                        raise ValueError("preview dataset table {0!r} contains row without pk={1!r}".format(table_name, pk_field))
                    if pk_value in row_map:
                        raise ValueError("preview dataset table {0!r} contains duplicate pk={1!r}".format(table_name, pk_value))
                    row_map[pk_value] = deepcopy_json(row)
            state["tables"][table_name] = row_map
            continue

        state["tables"][table_name] = value

    return state


def _row_sort_key(pk_field, row):
    if not isinstance(row, dict):
        return (2, "")
    value = row.get(pk_field)
    num = _as_int(value, None)
    if num is not None:
        return (0, num)
    return (1, _as_str(value))


def _preview_state_to_dataset(state, schema, base_dataset):
    candidate = deepcopy_json(base_dataset or {})

    for table_name, schema_entry in (schema or {}).items():
        shape = _as_str(schema_entry.get("shape"), "rows")
        table_value = (state or {}).get("tables", {}).get(table_name)

        if shape == "rows":
            pk_field = _as_str(schema_entry.get("pk"), "")
            rows = list((table_value or {}).values())
            rows.sort(key=partial(_row_sort_key, pk_field))
            candidate[table_name] = rows
            continue

        if table_value is None:
            candidate.pop(table_name, None)
        else:
            candidate[table_name] = deepcopy_json(table_value)

    return candidate


def preview_candidate_config_from_patch(current_config, schema, commands):
    if not isinstance(current_config, dict):
        raise ValueError("current_config must be a dict")
    if not isinstance(schema, dict):
        raise ValueError("schema must be a dict")
    if not isinstance(commands, list) or not commands:
        raise ValueError("commands must be a non-empty list")

    state = _dataset_to_preview_state(current_config, schema)
    for command in commands:
        if not isinstance(command, dict):
            raise ValueError("command must be a dict")
        apply_command_to_state(state, schema, copy.deepcopy(command))

    return _preview_state_to_dataset(state, schema, current_config)


def prevalidate_patch_commands_or_raise(current_config, schema, commands, node_id=None):
    candidate = preview_candidate_config_from_patch(current_config, schema, commands)
    report = build_config_commit_validation_report(candidate, node_id=node_id)
    if report.get("ok"):
        return report
    raise ValueError(format_validation_report(report))
