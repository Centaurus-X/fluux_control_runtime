import argparse
from pathlib import Path

from compiler_engine.application.compile_service import compile_from_paths
from compiler_engine.application.decompile_service import decompile_from_paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="compiler-engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    compile_parser = subparsers.add_parser("compile")
    compile_parser.add_argument("source_cud_path")
    compile_parser.add_argument("output_dir")

    decompile_parser = subparsers.add_parser("decompile")
    decompile_parser.add_argument("runtime_contract_bundle_path")
    decompile_parser.add_argument("output_dir")
    decompile_parser.add_argument("--runtime-change-set", dest="runtime_change_set_path")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "compile":
        compile_from_paths(args.source_cud_path, args.output_dir)
        return 0
    if args.command == "decompile":
        decompile_from_paths(args.runtime_contract_bundle_path, args.output_dir, args.runtime_change_set_path)
        return 0
    parser.error("unknown command")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
