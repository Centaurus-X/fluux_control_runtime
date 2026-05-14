# -*- coding: utf-8 -*-

import os

from src.orchestration.runtime_config import (
    build_runtime_bundle,
    load_effective_app_config,
    resolve_project_root,
)


def _suffix(parts):
    return os.path.join(*parts)


def test_resolve_project_root_without_anchor_file_points_to_product_root():
    project_root = resolve_project_root(None)
    assert project_root.endswith(_suffix(("product",)))
    assert os.path.isfile(os.path.join(project_root, "config", "application", "app_config.yaml"))


def test_load_effective_app_config_without_anchor_file_uses_delivered_config():
    runtime_meta = load_effective_app_config()
    assert runtime_meta["project_root"].endswith(_suffix(("product",)))
    assert os.path.isfile(runtime_meta["config_file"])
    assert runtime_meta["config"]["config_source"]["mode"] == "cud_compiled"


def test_build_runtime_bundle_without_anchor_file_is_self_contained():
    bundle = build_runtime_bundle()
    settings = bundle["settings"]
    compiler_paths = settings["config_source"]["compiler_python_paths"]

    assert bundle["project_root"].endswith(_suffix(("product",)))
    assert os.path.isfile(bundle["config_meta"]["config_file"])
    assert settings["config_source"]["legacy_json_path"].endswith(
        _suffix(("product", "config", "___device_state_data.json"))
    )
    assert any(
        candidate.endswith(_suffix(("product", "vendor", "current_compiler_engine", "code_compiler_engine")))
        for candidate in compiler_paths
    )
