#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# json_config_diff_check.py

import os
import sys
import json
import glob
import copy
import hashlib

from functools import partial


JSON_DUMP_CANONICAL = partial(
    json.dumps,
    ensure_ascii=False,
    indent=2,
    sort_keys=False,
    separators=(", ", ": "),
)


def read_text(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def sha256_text(text):
    h = hashlib.sha256()
    if isinstance(text, str):
        text = text.encode("utf-8", errors="replace")
    h.update(text)
    return h.hexdigest()


def canonical_json(obj):
    txt = JSON_DUMP_CANONICAL(obj)
    if not txt.endswith("\n"):
        txt += "\n"
    return txt


def load_json(path):
    raw = read_text(path)
    try:
        return json.loads(raw), raw, None
    except json.JSONDecodeError as exc:
        return None, raw, exc


def pick_latest_backup(current_path):
    base_dir = os.path.dirname(os.path.abspath(current_path))
    backup_dir = os.path.join(base_dir, "backup")
    pattern = os.path.join(backup_dir, "*.bak.*")
    candidates = glob.glob(pattern)

    if not candidates:
        return None

    candidates.sort(key=partial(os.path.getmtime))
    return candidates[-1]


def index_by(items, key):
    out = {}
    dups = []
    for it in items:
        if not isinstance(it, dict):
            continue
        k = it.get(key)
        if k in out:
            dups.append(k)
        out[k] = it
    return out, dups


def validate(cfg):
    issues = []

    def add(msg, path, detail):
        issues.append({"msg": msg, "path": path, "detail": detail})

    nodes, dups_nodes = index_by(cfg.get("nodes", []), "node_id")
    controllers, dups_ctrl = index_by(cfg.get("controllers", []), "controller_id")
    sensors, dups_sens = index_by(cfg.get("sensors", []), "sensor_id")
    actuators, dups_act = index_by(cfg.get("actuators", []), "actuator_id")
    processes, dups_proc = index_by(cfg.get("processes", []), "p_id")
    values, dups_val = index_by(cfg.get("process_values", []), "value_id")
    modes, dups_mode = index_by(cfg.get("process_modes", []), "mode_id")
    pid_cfgs, _ = index_by(cfg.get("pid_configs", []), "pid_config_id")
    cm_methods, _ = index_by(cfg.get("control_methods", []), "method_id")
    cm_templates, _ = index_by(cfg.get("control_method_templates", []), "template_id")

    if dups_nodes:
        add("Doppelte node_id", "nodes", {"dups": dups_nodes})
    if dups_ctrl:
        add("Doppelte controller_id", "controllers", {"dups": dups_ctrl})
    if dups_sens:
        add("Doppelte sensor_id", "sensors", {"dups": dups_sens})
    if dups_act:
        add("Doppelte actuator_id", "actuators", {"dups": dups_act})
    if dups_proc:
        add("Doppelte p_id", "processes", {"dups": dups_proc})
    if dups_val:
        add("Doppelte value_id", "process_values", {"dups": dups_val})
    if dups_mode:
        add("Doppelte mode_id", "process_modes", {"dups": dups_mode})

    # Controller refs in sensors/actuators
    ctrl_ids = set(controllers.keys())
    for i, s in enumerate(cfg.get("sensors", [])):
        if not isinstance(s, dict):
            continue
        cid = s.get("controller_id")
        if cid is not None and cid not in ctrl_ids:
            add("sensor.controller_id unbekannt", "sensors[%d].controller_id" % i, {"sensor_id": s.get("sensor_id"), "controller_id": cid})

    for i, a in enumerate(cfg.get("actuators", [])):
        if not isinstance(a, dict):
            continue
        cid = a.get("controller_id")
        if cid is not None and cid not in ctrl_ids:
            add("actuator.controller_id unbekannt", "actuators[%d].controller_id" % i, {"actuator_id": a.get("actuator_id"), "controller_id": cid})

    # process_states refs
    sensor_ids = set(sensors.keys())
    actuator_ids = set(actuators.keys())
    process_ids = set(processes.keys())
    value_ids = set(values.keys())
    mode_ids = set(modes.keys())

    for i, st in enumerate(cfg.get("process_states", [])):
        if not isinstance(st, dict):
            continue
        sid = st.get("state_id")

        pid = st.get("p_id")
        if pid is not None and pid not in process_ids:
            add("process_states.p_id unbekannt", "process_states[%d].p_id" % i, {"state_id": sid, "p_id": pid})

        vid = st.get("value_id")
        if vid is not None and vid not in value_ids:
            add("process_states.value_id unbekannt", "process_states[%d].value_id" % i, {"state_id": sid, "value_id": vid})

        mid = st.get("mode_id")
        if mid is not None and mid not in mode_ids:
            add("process_states.mode_id unbekannt", "process_states[%d].mode_id" % i, {"state_id": sid, "mode_id": mid})

        ivid = st.get("initial_value_id")
        if ivid is not None and ivid not in value_ids:
            add("process_states.initial_value_id unbekannt", "process_states[%d].initial_value_id" % i, {"state_id": sid, "initial_value_id": ivid})

        for aid in st.get("actuator_ids", []) if isinstance(st.get("actuator_ids"), list) else []:
            if aid not in actuator_ids:
                add("process_states.actuator_id unbekannt", "process_states[%d].actuator_ids" % i, {"state_id": sid, "actuator_id": aid})

        for sid2 in st.get("sensor_ids", []) if isinstance(st.get("sensor_ids"), list) else []:
            if sid2 not in sensor_ids:
                add("process_states.sensor_id unbekannt", "process_states[%d].sensor_ids" % i, {"state_id": sid, "sensor_id": sid2})

        cs = st.get("control_strategy")
        if isinstance(cs, dict):
            if cs.get("type") == "PID":
                pidc = cs.get("pid_config_id")
                if pidc is not None and pidc not in pid_cfgs:
                    add("control_strategy.pid_config_id unbekannt", "process_states[%d].control_strategy.pid_config_id" % i, {"state_id": sid, "pid_config_id": pidc})
            if cs.get("type") == "control_method":
                mid2 = cs.get("method_id")
                if mid2 is not None and mid2 not in cm_methods:
                    add("control_strategy.method_id unbekannt", "process_states[%d].control_strategy.method_id" % i, {"state_id": sid, "method_id": mid2})

    # control_methods template refs
    tmpl_ids = set(cm_templates.keys())
    for i, m in enumerate(cfg.get("control_methods", [])):
        if not isinstance(m, dict):
            continue
        tid = m.get("template_id")
        if tid is not None and tid not in tmpl_ids:
            add("control_methods.template_id unbekannt", "control_methods[%d].template_id" % i, {"method_id": m.get("method_id"), "template_id": tid})

    return issues


def diff_paths(a, b, path="", out=None, max_diffs=500):
    if out is None:
        out = []

    if len(out) >= max_diffs:
        return out

    if type(a) != type(b):
        out.append({"path": path or "$", "a": type(a).__name__, "b": type(b).__name__})
        return out

    if isinstance(a, dict):
        akeys = set(a.keys())
        bkeys = set(b.keys())

        for k in sorted(akeys - bkeys):
            if len(out) >= max_diffs:
                break
            out.append({"path": (path + "." + k) if path else "$." + k, "change": "removed"})

        for k in sorted(bkeys - akeys):
            if len(out) >= max_diffs:
                break
            out.append({"path": (path + "." + k) if path else "$." + k, "change": "added"})

        for k in sorted(akeys & bkeys):
            if len(out) >= max_diffs:
                break
            diff_paths(a.get(k), b.get(k), (path + "." + k) if path else "$." + k, out, max_diffs)

        return out

    if isinstance(a, list):
        if len(a) != len(b):
            out.append({"path": path or "$", "change": "list_len", "a": len(a), "b": len(b)})

        n = min(len(a), len(b))
        for i in range(n):
            if len(out) >= max_diffs:
                break
            diff_paths(a[i], b[i], "%s[%d]" % (path or "$", i), out, max_diffs)
        return out

    if a != b:
        out.append({"path": path or "$", "change": "value", "a": a, "b": b})
    return out


def main(argv):
    if len(argv) < 2:
        print("Usage: check_config.py <current.json> [previous.json|AUTO]")
        return 2

    cur_path = argv[1]
    prev_path = argv[2] if len(argv) >= 3 else None

    if prev_path in (None, "AUTO"):
        auto = pick_latest_backup(cur_path)
        if auto:
            prev_path = auto
        else:
            prev_path = None

    cur_obj, cur_raw, cur_err = load_json(cur_path)
    if cur_err:
        print("ERROR: current.json ist nicht parsebar:", cur_err)
        return 1

    cur_can = canonical_json(cur_obj)
    print("Current:", cur_path)
    print("  sha256(raw):     ", sha256_text(cur_raw))
    print("  sha256(canonical):", sha256_text(cur_can))
    cur_issues = validate(cur_obj)
    print("  validation_issues:", len(cur_issues))
    for it in cur_issues[:50]:
        print("   -", it["msg"], "@", it["path"], it["detail"])

    if not prev_path:
        print("\nKein previous.json gefunden. (Wenn du Backups hast: <current_dir>/backup/*.bak.*)")
        return 0

    prev_obj, prev_raw, prev_err = load_json(prev_path)
    if prev_err:
        print("ERROR: previous.json ist nicht parsebar:", prev_err)
        return 1

    prev_can = canonical_json(prev_obj)
    print("\nPrevious:", prev_path)
    print("  sha256(raw):     ", sha256_text(prev_raw))
    print("  sha256(canonical):", sha256_text(prev_can))
    prev_issues = validate(prev_obj)
    print("  validation_issues:", len(prev_issues))

    same_semantic = sha256_text(cur_can) == sha256_text(prev_can)
    print("\nSemantic equal:", same_semantic)

    diffs = diff_paths(prev_obj, cur_obj, path="")
    print("Diff entries:", len(diffs))
    for d in diffs[:200]:
        print(" -", d)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
