from pathlib import Path

import yaml

from compiler_engine.application.compile_service import compile_data
from compiler_engine.application.decompile_service import decompile_data


def build_id_set(items: list[dict]) -> set[str]:
    result = set()
    item: dict
    for item in items:
        result.add(str(item['id']))
    return result


def test_decompile_restores_expected_core_sections() -> None:
    example_path = Path(__file__).resolve().parents[2] / 'examples' / '01_cud.example.yaml'
    cud = yaml.safe_load(example_path.read_text(encoding='utf-8'))
    compiled = compile_data(cud)
    decompiled = decompile_data(compiled['runtime_contract_bundle'])
    restored = decompiled['decompiled_cud']
    assert restored['canonical_id'] == cud['canonical_id']
    assert build_id_set(restored['assets']) == build_id_set(cud['assets'])
    assert build_id_set(restored['signals']) == build_id_set(cud['signals'])
    assert build_id_set(restored['domains']) == build_id_set(cud['domains'])
