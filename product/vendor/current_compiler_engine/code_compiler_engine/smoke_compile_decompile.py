from pathlib import Path

from compiler_engine.application.compile_service import compile_from_paths
from compiler_engine.application.decompile_service import decompile_from_paths


def main() -> int:
    base_dir = Path(__file__).resolve().parent.parent
    examples_dir = base_dir / 'examples'
    compile_out = examples_dir / 'generated_compile_out'
    decompile_out = examples_dir / 'generated_decompile_out'

    compile_from_paths(str(examples_dir / '01_cud.example.yaml'), str(compile_out))
    decompile_from_paths(
        str(compile_out / '10_runtime_contract_bundle.yaml'),
        str(decompile_out),
        str(examples_dir / '04_runtime_change_set.example.yaml'),
    )
    print('compiler_engine smoke OK')
    print(f'compile_out={compile_out}')
    print(f'decompile_out={decompile_out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
