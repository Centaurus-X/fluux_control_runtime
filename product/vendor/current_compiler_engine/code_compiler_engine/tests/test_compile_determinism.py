from pathlib import Path

import yaml

from compiler_engine.application.compile_service import compile_data


def test_compile_is_deterministic_for_same_input() -> None:
    example_path = Path(__file__).resolve().parents[2] / 'examples' / '01_cud.example.yaml'
    cud = yaml.safe_load(example_path.read_text(encoding='utf-8'))
    first = compile_data(cud)
    second = compile_data(cud)
    assert first['runtime_contract_bundle'] == second['runtime_contract_bundle']
    assert first['runtime_manifest'] == second['runtime_manifest']
