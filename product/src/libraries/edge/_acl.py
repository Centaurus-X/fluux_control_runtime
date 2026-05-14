# -*- coding: utf-8 -*-

import copy
import threading

from src.libraries.edge._contracts import (
    derive_action_from_contract,
    normalize_application_contract,
)


_DEFAULT_ALLOW_ROLES = frozenset(("admin", "superuser", "master", "gateway_proxy"))


def _deepcopy_safe(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _safe_str(value, default=""):
    try:
        if value is None:
            return str(default)
        return str(value)
    except Exception:
        return str(default)


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(sorted(value))
    return [value]


def _normalize_text_set(value):
    result = set()
    for item in _as_list(value):
        text = _safe_str(item, "").strip()
        if text:
            result.add(text)
    return result


def _normalize_operation_text(value):
    return _safe_str(value, "").strip().lower()


def _resource_entry(resources, name):
    if not isinstance(resources, dict):
        return None
    entry = resources.get(name)
    if isinstance(entry, dict) and "data" in entry:
        return entry
    return None


def _read_resource(resources, name, default=None):
    entry = _resource_entry(resources, name)
    if entry is None:
        return _deepcopy_safe(default)

    lock = entry.get("lock")
    if lock is None:
        lock = threading.RLock()

    with lock:
        return _deepcopy_safe(entry.get("data", default))


def _find_acl_config(resources):
    candidates = []

    client_cfg = _read_resource(resources, "client_config", {})
    user_cfg = _read_resource(resources, "user_config", {})

    if isinstance(client_cfg, dict):
        candidates.append(client_cfg.get("edge_acl"))
        candidates.append(client_cfg.get("acl"))
    if isinstance(user_cfg, dict):
        candidates.append(user_cfg.get("edge_acl"))
        candidates.append(user_cfg.get("acl"))

    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate

    return None


def _extract_actor(contract):
    actor = contract.get("actor") if isinstance(contract.get("actor"), dict) else {}
    return {
        "principal_id": _safe_str(actor.get("principal_id") or actor.get("user_id"), "").strip(),
        "roles": _normalize_text_set(actor.get("roles")),
        "scopes": _normalize_text_set(actor.get("scopes")),
        "session_id": _safe_str(actor.get("session_id"), "").strip(),
    }


def _extract_acl_context(contract, envelope, node_id):
    body = contract.get("body")
    if not isinstance(body, dict):
        body = {}

    tables = set()
    paths = set()

    table_single = _safe_str(body.get("table"), "").strip()
    if table_single:
        tables.add(table_single)

    path_single = _safe_str(body.get("path"), "").strip()
    if path_single:
        paths.add(path_single)

    operations = body.get("operations")
    if isinstance(operations, list):
        for item in operations:
            if not isinstance(item, dict):
                continue
            table_name = _safe_str(item.get("table"), "").strip()
            path_value = _safe_str(item.get("path"), "").strip()
            if table_name:
                tables.add(table_name)
            if path_value:
                paths.add(path_value)

    commands = body.get("commands")
    if isinstance(commands, list):
        for item in commands:
            if not isinstance(item, dict):
                continue
            table_name = _safe_str(item.get("table"), "").strip()
            if table_name:
                tables.add(table_name)
            patch = item.get("patch")
            if isinstance(patch, list):
                for op in patch:
                    if not isinstance(op, dict):
                        continue
                    path_value = _safe_str(op.get("path"), "").strip()
                    if path_value:
                        paths.add(path_value)

    target_node = _safe_str(envelope.get("target_node") or node_id, "").strip() or _safe_str(node_id, "").strip()

    return {
        "action": derive_action_from_contract(contract),
        "resource": _normalize_operation_text(contract.get("resource")),
        "operation": _normalize_operation_text(contract.get("operation")),
        "tables": tables,
        "paths": paths,
        "target_node": target_node,
    }


def _matches_value(rule_value, current_values):
    if rule_value in (None, "", [], {}, set()):
        return True

    allowed = _normalize_text_set(rule_value)
    if not allowed:
        return True

    if not current_values:
        return False

    return bool(allowed.intersection(current_values))


def _match_rule(rule, actor, acl_ctx):
    if not isinstance(rule, dict):
        return False

    if not _matches_value(rule.get("principals"), set((actor.get("principal_id"),)) if actor.get("principal_id") else set()):
        return False
    if not _matches_value(rule.get("roles"), actor.get("roles") or set()):
        return False
    if not _matches_value(rule.get("scopes"), actor.get("scopes") or set()):
        return False
    if not _matches_value(rule.get("actions"), set((acl_ctx.get("action"),))):
        return False
    if not _matches_value(rule.get("resources"), set((acl_ctx.get("resource"),))):
        return False
    if not _matches_value(rule.get("operations"), set((acl_ctx.get("operation"),))):
        return False
    if not _matches_value(rule.get("tables"), acl_ctx.get("tables") or set()):
        return False
    if not _matches_value(rule.get("paths"), acl_ctx.get("paths") or set()):
        return False
    if not _matches_value(rule.get("nodes"), set((acl_ctx.get("target_node"),))):
        return False
    return True


def _collect_rules_from_bundle(bundle, actor):
    deny_rules = []
    allow_rules = []

    if not isinstance(bundle, dict):
        return deny_rules, allow_rules

    def _append_rules(target, value):
        for item in _as_list(value):
            if isinstance(item, dict):
                target.append(item)

    _append_rules(allow_rules, bundle.get("allow"))
    _append_rules(deny_rules, bundle.get("deny"))

    principals = bundle.get("principals") if isinstance(bundle.get("principals"), dict) else {}
    roles_map = bundle.get("roles") if isinstance(bundle.get("roles"), dict) else {}
    scopes_map = bundle.get("scopes") if isinstance(bundle.get("scopes"), dict) else {}

    principal_id = actor.get("principal_id")
    if principal_id and principal_id in principals:
        pd, pa = _collect_rules_from_bundle(principals.get(principal_id), actor)
        deny_rules.extend(pd)
        allow_rules.extend(pa)

    for role in sorted(actor.get("roles") or set()):
        if role in roles_map:
            rd, ra = _collect_rules_from_bundle(roles_map.get(role), actor)
            deny_rules.extend(rd)
            allow_rules.extend(ra)

    for scope in sorted(actor.get("scopes") or set()):
        if scope in scopes_map:
            sd, sa = _collect_rules_from_bundle(scopes_map.get(scope), actor)
            deny_rules.extend(sd)
            allow_rules.extend(sa)

    return deny_rules, allow_rules


def authorize_edge_contract(resources, contract, envelope, node_id=None, default_policy="allow"):
    normalized = normalize_application_contract(contract)
    actor = _extract_actor(normalized)
    acl_ctx = _extract_acl_context(normalized, envelope if isinstance(envelope, dict) else {}, node_id)
    acl_cfg = _find_acl_config(resources)

    report = {
        "ok": True,
        "decision": "allow",
        "reason": "default_allow",
        "actor": {
            "principal_id": actor.get("principal_id"),
            "roles": sorted(actor.get("roles") or set()),
            "scopes": sorted(actor.get("scopes") or set()),
        },
        "context": {
            "action": acl_ctx.get("action"),
            "resource": acl_ctx.get("resource"),
            "operation": acl_ctx.get("operation"),
            "target_node": acl_ctx.get("target_node"),
            "tables": sorted(acl_ctx.get("tables") or set()),
            "paths": sorted(acl_ctx.get("paths") or set()),
        },
    }

    if acl_ctx.get("action") == "event":
        return report

    if acl_cfg is None:
        if actor.get("roles") and actor.get("roles").intersection(_DEFAULT_ALLOW_ROLES):
            report["reason"] = "implicit_admin_role"
            return report

        if _normalize_operation_text(default_policy) == "deny" and acl_ctx.get("action") in ("write", "delete", "command", "admin"):
            report["ok"] = False
            report["decision"] = "deny"
            report["reason"] = "missing_acl_default_deny"
            return report

        return report

    effective_default = _normalize_operation_text(acl_cfg.get("default_policy") or default_policy or "deny") or "deny"
    deny_rules, allow_rules = _collect_rules_from_bundle(acl_cfg, actor)

    for rule in deny_rules:
        if _match_rule(rule, actor, acl_ctx):
            report["ok"] = False
            report["decision"] = "deny"
            report["reason"] = _safe_str(rule.get("name") or "deny_rule", "deny_rule")
            return report

    for rule in allow_rules:
        if _match_rule(rule, actor, acl_ctx):
            report["reason"] = _safe_str(rule.get("name") or "allow_rule", "allow_rule")
            return report

    if effective_default == "allow":
        report["reason"] = "acl_default_allow"
        return report

    report["ok"] = False
    report["decision"] = "deny"
    report["reason"] = "acl_default_deny"
    return report
