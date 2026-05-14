# -*- coding: utf-8 -*-

from pathlib import Path

from src.orchestration.runtime_config import build_runtime_bundle
from src.orchestration.runtime_config import resolve_contract_registry_entry


def _product_root():
    return Path(__file__).resolve().parents[1]


def _text(path):
    return path.read_text(encoding="utf-8")


def test_v31_contract_registry_is_canonical_and_complete():
    root = _product_root()
    registry_path = root / "contracts" / "contract_registry.yaml"
    registry = _text(registry_path)

    assert registry_path.is_file()
    assert "status: active_runtime_resolved" in registry
    assert "product_release_target: v34_preproduction_final_runtime" in registry
    assert "canonical_contract_root: product/contracts" in registry
    assert "mode: contract_id_to_registry_path" in registry

    for index in range(1, 28):
        contract_id = "C" + str(index).zfill(2)
        assert "contract_id: " + contract_id in registry


def test_v31_formal_runtime_data_contracts_exist_at_canonical_location():
    root = _product_root()
    contracts_root = root / "contracts"

    formal_files = sorted((contracts_root / "formal").glob("[0-9][0-9]_*.md"))
    assert len(formal_files) == 27

    assert (contracts_root / "runtime" / "PROCESS_STATES_MAPPING_CONTRACT_V1.yaml").is_file()
    assert (contracts_root / "runtime" / "CONTROL_METHODS_MAPPING_CONTRACT_V1.yaml").is_file()
    assert (contracts_root / "data" / "EVENT_ENVELOPE_CONTRACT_V1.yaml").is_file()
    assert (contracts_root / "data" / "TYPE_MAPPING_REGISTRY_CONTRACT_V1.yaml").is_file()
    assert (contracts_root / "data" / "CONSISTENCY_ZONES_CONTRACT_V1.yaml").is_file()

    assert (contracts_root / "runtime" / "RUNTIME_COMMAND_BINDING_CONTRACT_V1.yaml").is_file()
    assert (contracts_root / "formal" / "25_PROXY_WORKER_BRIDGE_CONTRACT_V1.md").is_file()


def test_v31_legacy_product_config_contract_mirror_removed():
    root = _product_root()

    assert not (root / "config" / "contracts").exists()


def test_v31_app_config_uses_contract_ids_not_runtime_contract_paths():
    root = _product_root()
    app_config = _text(root / "config" / "application" / "app_config.yaml")

    assert "contract_registry:" in app_config
    assert "registry_path: contracts/contract_registry.yaml" in app_config
    assert "process_states_mapping_contract_id: C08" in app_config
    assert "control_methods_mapping_contract_id: C17" in app_config
    assert "process_states_mapping_contract_path:" not in app_config
    assert "control_methods_mapping_contract_path:" not in app_config
    assert "config/contracts/process_states_mapping_contract.yaml" not in app_config
    assert "config/contracts/control_methods_mapping_contract.yaml" not in app_config


def test_v31_runtime_bundle_resolves_contract_paths_from_registry():
    root = _product_root()
    bundle = build_runtime_bundle(anchor_file=str(root / "run.py"))
    settings = bundle["settings"]
    registry = settings["contract_registry"]
    config_source = settings["config_source"]

    assert registry["loaded"] is True
    assert registry["registry_id"] == "CONTRACT_REGISTRY_V1"
    assert registry["product_release_target"] == "v34_preproduction_final_runtime"
    assert config_source["contract_registry_loaded"] is True
    assert config_source["process_states_mapping_contract_id"] == "C08"
    assert config_source["control_methods_mapping_contract_id"] == "C17"
    assert config_source["process_states_mapping_contract_path"].endswith(
        "contracts/runtime/PROCESS_STATES_MAPPING_CONTRACT_V1.yaml"
    )
    assert config_source["control_methods_mapping_contract_path"].endswith(
        "contracts/runtime/CONTROL_METHODS_MAPPING_CONTRACT_V1.yaml"
    )

    assert settings["proxy_worker_bridge"]["contract_id"] == "C27"
    assert settings["proxy_worker_bridge"]["enabled"] is True

    c08 = resolve_contract_registry_entry(registry, "C08", path_kind="runtime_instance")
    c17 = resolve_contract_registry_entry(registry, "C17", path_kind="runtime_instance")

    assert c08["found"] is True
    assert c08["exists"] is True
    assert c08["source_of_truth"] == "runtime_instance"
    assert c17["found"] is True
    assert c17["exists"] is True
    assert c17["source_of_truth"] == "runtime_instance"


def test_v31_registry_points_to_externalized_data_contracts():
    root = _product_root()
    registry = _text(root / "contracts" / "contract_registry.yaml")

    assert "data_contract: product/contracts/data/EVENT_ENVELOPE_CONTRACT_V1.yaml" in registry
    assert "data_contract: product/contracts/data/TYPE_MAPPING_REGISTRY_CONTRACT_V1.yaml" in registry
    assert "data_contract: product/contracts/data/CONSISTENCY_ZONES_CONTRACT_V1.yaml" in registry
    assert "runtime_instance: product/contracts/runtime/PROCESS_STATES_MAPPING_CONTRACT_V1.yaml" in registry
    assert "runtime_instance: product/contracts/runtime/CONTROL_METHODS_MAPPING_CONTRACT_V1.yaml" in registry


def test_v34_registry_resolves_runtime_command_activation_contract():
    root = _product_root()
    bundle = build_runtime_bundle(anchor_file=str(root / "run.py"))
    registry = bundle["settings"]["contract_registry"]
    c25 = resolve_contract_registry_entry(registry, "C27", path_kind="runtime_instance")
    assert c25["found"] is True
    assert c25["exists"] is True
    assert c25["source_of_truth"] == "formal_spec_and_runtime_contract"
    assert c25["resolved_path"].endswith("contracts/runtime/RUNTIME_COMMAND_ACTIVATION_BINDING_CONTRACT_V1.yaml")
