#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NDJSON Konverter für dynamic_rule_engine.rule_sets

Funktionen:
- export_rules_to_ndjson(config, path, set_ids=None)
- import_rules_from_ndjson(path) -> Dict[set_id, List[rules]]
- apply_ndjson_to_config(config, path, config_lock=None, overwrite=True)

CLI:
    python ndjson_converter.py export <config.json> <out.ndjson> [--sets set1,set2]
    python ndjson_converter.py import <config.json> <in.ndjson> [--backup] [--overwrite]
    python ndjson_converter.py export_mem <out.ndjson>  # Beispiel: nutzt config_data variable (wenn importiert)

Hinweise:
- NDJSON Format: jede Zeile ist ein JSON-Objekt mit { "set_id": "<id>", "rule": { ... } }
- Thread-sicher: apply_ndjson_to_config akzeptiert optional einen threading.Lock
- Keine Decorators, keine dataclasses, functools.partial in examples
"""

import json
import os
import shutil
import sys
import threading
import logging
from functools import partial
from typing import Dict, List, Any, Optional, Tuple

# Logging Grundkonfiguration
logger = logging.getLogger("ndjson_converter")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)


# -------------------------
# Export: config -> NDJSON
# -------------------------
def export_rules_to_ndjson(config: Dict[str, Any],
                           path: str,
                           set_ids: Optional[List[str]] = None,
                           ensure_ascii: bool = False) -> int:
    """
    Exportiert Regeln in eine NDJSON-Datei. Jede Zeile: {"set_id": "...", "rule": {...}}

    Args:
        config: Das geladene Config-Objekt (dict)
        path: Pfad zur NDJSON-Ausgabedatei
        set_ids: Optional; Liste von set_id, die exportiert werden sollen (wenn None -> alle)
        ensure_ascii: JSON-Dumps ensure_ascii flag

    Returns:
        Anzahl exportierter Regeln (int)
    """
    dre = config.get("dynamic_rule_engine", {})
    rule_sets = dre.get("rule_sets", [])
    count = 0

    # Ordner ggf. anlegen
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)

    try:
        with open(path, "w", encoding="utf-8") as fh:
            for rs in rule_sets:
                sid = str(rs.get("set_id", "unknown"))
                if set_ids and sid not in set_ids:
                    continue
                for r in rs.get("rules", []):
                    out = {"set_id": sid, "rule": r}
                    fh.write(json.dumps(out, ensure_ascii=ensure_ascii) + "\n")
                    count += 1
        logger.info("Export abgeschlossen: %d Regeln -> %s", count, path)
        return count
    except Exception as e:
        logger.exception("Fehler beim Exportieren nach NDJSON: %s", e)
        raise


# -------------------------
# Import: NDJSON -> dict
# -------------------------
def import_rules_from_ndjson(path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Liest eine NDJSON-Datei, gibt Mapping set_id -> [rule, rule, ...]

    Args:
        path: Pfad zur NDJSON-Datei

    Returns:
        Dict mit set_id keys und Listen von Rule-Objekten
    """
    if not os.path.exists(path):
        raise FileNotFoundError("NDJSON Datei nicht gefunden: " + str(path))

    result: Dict[str, List[Dict[str, Any]]] = {}
    line_no = 0
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line_no += 1
                s = raw.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError as e:
                    logger.error("JSONDecodeError in Zeile %d: %s", line_no, e)
                    raise
                set_id = str(obj.get("set_id", "unknown"))
                rule = obj.get("rule")
                if rule is None:
                    logger.warning("Zeile %d enthält kein 'rule' Feld, wird übersprungen", line_no)
                    continue
                result.setdefault(set_id, []).append(rule)
        logger.info("Import gelesen: %d Zeilen -> %d rule_sets", line_no, len(result))
        return result
    except Exception:
        logger.exception("Fehler beim Lesen der NDJSON-Datei.")
        raise


# -------------------------
# Merge / Apply into config
# -------------------------
def apply_ndjson_to_config(config: Dict[str, Any],
                           path: str,
                           config_lock: Optional[threading.Lock] = None,
                           overwrite: bool = True,
                           default_priority: int = 5,
                           default_evaluation: str = "on_event") -> Dict[str, Any]:
    """
    Liest NDJSON und ersetzt / ergänzt config['dynamic_rule_engine']['rule_sets'].

    Args:
        config: Das Konfigurations-Dict (wird modifiziert und zurückgegeben)
        path: Pfad zur NDJSON-Datei
        config_lock: Optional threading.Lock für thread-sichere Änderungen
        overwrite: Wenn True -> ersetze existierende rule_sets mit gleichen set_id
                   Wenn False -> ergänze existierende rule_sets (anhängen)
        default_priority: Default priority für neue rule_sets (falls nicht gesetzt)
        default_evaluation: Default evaluation für neue rule_sets

    Returns:
        Das veränderte config dict
    """
    new_sets = import_rules_from_ndjson(path)

    def _apply():
        dre = config.setdefault("dynamic_rule_engine", {})
        existing_list = dre.get("rule_sets", [])
        # build map existing
        existing: Dict[str, Dict[str, Any]] = {}
        for rs in existing_list:
            if "set_id" in rs:
                existing[str(rs["set_id"])] = rs
        # update/replace or append
        for set_id, rules in new_sets.items():
            if set_id in existing:
                if overwrite:
                    existing[set_id] = {"set_id": set_id, "priority": default_priority, "evaluation": default_evaluation, "rules": list(rules)}
                else:
                    # append: kombiniere Regeln dedupliziert nach rule_id (einfache heuristik)
                    merged_rules = list(existing[set_id].get("rules", []))
                    existing_rule_ids = {str(r.get("rule_id")) for r in merged_rules if "rule_id" in r}
                    for r in rules:
                        rid = str(r.get("rule_id")) if "rule_id" in r else None
                        if rid and rid in existing_rule_ids:
                            # ersetze vorhandene rule mit gleicher id
                            merged_rules = [r if str(r.get("rule_id")) != rid else r for r in merged_rules]
                        else:
                            merged_rules.append(r)
                    existing[set_id]["rules"] = merged_rules
            else:
                existing[set_id] = {"set_id": set_id, "priority": default_priority, "evaluation": default_evaluation, "rules": list(rules)}
        # write back
        dre["rule_sets"] = list(existing.values())
        logger.info("apply_ndjson_to_config: %d rule_sets im Config-Dict", len(dre["rule_sets"]))
        return config

    if config_lock is not None:
        with config_lock:
            return _apply()
    else:
        return _apply()


# -------------------------
# Utilities: JSON file load/save + backup
# -------------------------
def load_json_file(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError("Config Datei nicht gefunden: " + path)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_json_file(config: Dict[str, Any], path: str, backup: bool = True) -> None:
    if backup and os.path.exists(path):
        bak = path + ".bak"
        shutil.copy2(path, bak)
        logger.info("Backup der Config erzeugt: %s", bak)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)
    logger.info("Config gespeichert: %s", path)


# -------------------------
# CLI Hilfe / Main
# -------------------------
def _print_usage():
    print("NDJSON Konverter")
    print("Usage:")
    print("  python ndjson_converter.py export <config.json> <out.ndjson> [<set1,set2>]")
    print("  python ndjson_converter.py import <config.json> <in.ndjson> [--backup] [--no-overwrite]")
    print("  python ndjson_converter.py export_mem <out.ndjson>   # nutzt variable config_data (in-memory example)")
    print("")
    print("Notes:")
    print("  NDJSON format: each line = {\"set_id\": \"...\", \"rule\": { ... } }")


def _cli_export(config_path: str, out_path: str, set_ids_csv: Optional[str]):
    config = load_json_file(config_path)
    set_ids = None
    if set_ids_csv:
        set_ids = [s.strip() for s in set_ids_csv.split(",") if s.strip()]
    count = export_rules_to_ndjson(config, out_path, set_ids=set_ids)
    print("Exported rules:", count)


def _cli_import(config_path: str, in_path: str, backup_flag: bool, overwrite_flag: bool):
    config = load_json_file(config_path)
    apply_ndjson_to_config(config, in_path, config_lock=None, overwrite=overwrite_flag)
    save_json_file(config, config_path, backup=backup_flag)
    print("Import applied and config saved.")


# -------------------------
# Beispiel: partial usage (für integration in async_xserver_main)
# -------------------------
# Beispiel-Partial (so binden):
#   add_export_partial = partial(export_rules_to_ndjson, config_data, "/tmp/my_rules.ndjson")
#   add_import_partial = partial(apply_ndjson_to_config, config_data, "/tmp/my_rules.ndjson", config_lock)
# Dann: add_export_partial() / add_import_partial()
#
# Hinweis: partial ersetzt die ersten positional args, du kannst damit einfache callbacks erzeugen.
# -------------------------

# -------------------------
# Wenn als Skript ausgeführt
# -------------------------
def main(argv: List[str]):
    if len(argv) < 2:
        _print_usage()
        return 1

    cmd = argv[1].lower()
    if cmd == "export":
        if len(argv) < 4:
            _print_usage()
            return 2
        config_path = argv[2]
        out_path = argv[3]
        set_ids_csv = argv[4] if len(argv) > 4 else None
        _cli_export(config_path, out_path, set_ids_csv)
        return 0

    if cmd == "import":
        if len(argv) < 4:
            _print_usage()
            return 2
        config_path = argv[2]
        in_path = argv[3]
        backup_flag = True
        overwrite_flag = True
        # simple options scanning
        for opt in argv[4:]:
            if opt == "--no-backup":
                backup_flag = False
            if opt == "--no-overwrite":
                overwrite_flag = False
        _cli_import(config_path, in_path, backup_flag, overwrite_flag)
        return 0

    if cmd == "export_mem":
        # demo: expects a global variable config_data in this interpreter
        if len(argv) < 3:
            _print_usage()
            return 2
        out_path = argv[2]
        if "config_data" not in globals():
            print("Kein config_data im globalen Namespace verfügbar.")
            return 3
        config = globals()["config_data"]
        export_rules_to_ndjson(config, out_path)
        return 0

    _print_usage()
    return 1


if __name__ == "__main__":
    # Aufruf: python ndjson_converter.py ...
    sys.exit(main(sys.argv))
