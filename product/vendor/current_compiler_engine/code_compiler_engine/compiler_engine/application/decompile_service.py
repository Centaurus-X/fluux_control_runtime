from pathlib import Path
from typing import Any, Dict, Optional

from compiler_engine.adapters.filesystem import ensure_dir
from compiler_engine.adapters.yaml_codec import dump_yaml_file, load_data_file
from compiler_engine.domain.decompile_runtime_contracts import build_cud_delta_from_runtime_change_set, decompile_runtime_contract_bundle


def decompile_data(bundle: Dict[str, Any], runtime_change_set: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    decompiled_cud = decompile_runtime_contract_bundle(bundle)
    result = {"decompiled_cud": decompiled_cud}
    if runtime_change_set is not None:
        result["cud_delta"] = build_cud_delta_from_runtime_change_set(bundle, runtime_change_set)
    return result


def decompile_from_paths(bundle_path: str, output_dir: str, runtime_change_set_path: Optional[str] = None) -> Dict[str, Any]:
    bundle = load_data_file(bundle_path)
    runtime_change_set = None
    if runtime_change_set_path:
        runtime_change_set = load_data_file(runtime_change_set_path)
    result = decompile_data(bundle, runtime_change_set)
    target_dir = Path(output_dir)
    ensure_dir(target_dir)
    dump_yaml_file(target_dir / "01_decompiled_cud.yaml", result["decompiled_cud"])
    if "cud_delta" in result:
        dump_yaml_file(target_dir / "02_cud_delta.yaml", result["cud_delta"])
    return result
