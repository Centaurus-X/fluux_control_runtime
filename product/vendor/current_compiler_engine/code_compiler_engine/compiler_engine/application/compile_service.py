from pathlib import Path
from typing import Any, Dict

from compiler_engine.adapters.filesystem import ensure_dir
from compiler_engine.adapters.yaml_codec import dump_yaml_file, load_data_file
from compiler_engine.domain.compile_runtime_contracts import (
    build_runtime_contract_bundle,
    compile_boot_control_state,
    compile_inventory,
    compile_io_bindings,
    compile_manifest,
    compile_rule_ir,
    compile_shard_layout,
    compile_signal_catalog,
)
from compiler_engine.domain.cud_normalize import normalize_cud
from compiler_engine.domain.owner_context import build_owner_context_partition
from compiler_engine.domain.validate import validate_cud


def compile_data(cud: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_cud(cud)
    validate_cud(normalized)
    partition = build_owner_context_partition(normalized)
    compiled_inventory = compile_inventory(normalized, partition)
    signal_catalog = compile_signal_catalog(normalized, partition)
    io_bindings = compile_io_bindings(normalized, partition, signal_catalog)
    rule_ir = compile_rule_ir(normalized, partition)
    shard_layout = compile_shard_layout(normalized, partition, signal_catalog)
    manifest = compile_manifest(normalized, partition, compiled_inventory, signal_catalog, io_bindings, rule_ir, shard_layout)
    boot_control_state = compile_boot_control_state(normalized, partition, signal_catalog, io_bindings, rule_ir)
    bundle = build_runtime_contract_bundle(normalized, partition, compiled_inventory, signal_catalog, io_bindings, rule_ir, shard_layout, manifest, boot_control_state)
    return {
        "normalized_cud": normalized,
        "owner_context_partition": partition,
        "compiled_inventory": compiled_inventory,
        "compiled_signal_catalog": signal_catalog,
        "compiled_io_bindings": io_bindings,
        "compiled_rule_ir": rule_ir,
        "runtime_shard_layout": shard_layout,
        "runtime_manifest": manifest,
        "runtime_control_state_boot": boot_control_state,
        "runtime_contract_bundle": bundle,
    }


def compile_from_paths(source_path: str, output_dir: str) -> Dict[str, Any]:
    cud = load_data_file(source_path)
    result = compile_data(cud)
    target_dir = Path(output_dir)
    ensure_dir(target_dir)
    ordered_outputs = [
        ("01_normalized_cud.yaml", result["normalized_cud"]),
        ("02_owner_context_partition.yaml", result["owner_context_partition"]),
        ("03_compiled_inventory.yaml", result["compiled_inventory"]),
        ("04_compiled_signal_catalog.yaml", result["compiled_signal_catalog"]),
        ("05_compiled_io_bindings.yaml", result["compiled_io_bindings"]),
        ("06_compiled_rule_ir.yaml", result["compiled_rule_ir"]),
        ("07_runtime_shard_layout.yaml", result["runtime_shard_layout"]),
        ("08_runtime_manifest.yaml", result["runtime_manifest"]),
        ("09_runtime_control_state.boot.yaml", result["runtime_control_state_boot"]),
        ("10_runtime_contract_bundle.yaml", result["runtime_contract_bundle"]),
    ]
    for filename, payload in ordered_outputs:
        dump_yaml_file(target_dir / filename, payload)
    return result
