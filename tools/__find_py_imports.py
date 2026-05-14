#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import ast
import functools
import sys
import tokenize
from pathlib import Path
from typing import Iterable, Iterator, Set, Tuple


def iter_py_files(directory: Path) -> Iterator[Path]:
    for path in sorted(directory.iterdir()):
        if path.is_file() and path.suffix == ".py":
            yield path


def read_source(path: Path) -> str:
    # Respektiert PEP-263 Encoding-Cookie / BOM über tokenize.open()
    with tokenize.open(path) as fh:
        return fh.read()


def extract_import_lines(source: str, filename: str, include_relative: bool) -> Set[str]:
    lines: Set[str] = set()

    tree = ast.parse(source, filename=filename)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                # alias.name ist z.B. "os" oder "pkg.sub"
                if alias.name:
                    lines.add(f"import {alias.name}")

        elif isinstance(node, ast.ImportFrom):
            # node.module kann None sein bei: "from . import x"
            level = int(getattr(node, "level", 0) or 0)
            module = getattr(node, "module", None)

            if level > 0:
                # Relative Imports lassen sich nicht sinnvoll zu "import ..." umschreiben,
                # daher optional als "from ... import *" ausgeben.
                if not include_relative:
                    continue

                dots = "." * level
                mod_part = module or ""
                target = f"{dots}{mod_part}"
                # Leerstring wäre "from . import *" (bei "from . import x")
                lines.add(f"from {target} import *".rstrip())

            else:
                # Absolute "from x import y" -> "import x"
                if module:
                    lines.add(f"import {module}")

    return lines


def collect_imports(directory: Path, include_relative: bool) -> Tuple[Set[str], Set[Tuple[Path, str]]]:
    all_lines: Set[str] = set()
    errors: Set[Tuple[Path, str]] = set()

    for py_file in iter_py_files(directory):
        try:
            src = read_source(py_file)
            all_lines |= extract_import_lines(src, str(py_file), include_relative)
        except (SyntaxError, UnicodeError, OSError) as exc:
            errors.add((py_file, f"{type(exc).__name__}: {exc}"))

    return all_lines, errors


def write_lines(output_path: Path, lines: Iterable[str]) -> None:
    with output_path.open("w", encoding="utf-8", newline="\n") as fh:
        write = functools.partial(fh.write)
        for line in lines:
            write(line)
            write("\n")


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scannt alle .py-Dateien im Verzeichnis und extrahiert deduplizierte Imports."
    )
    parser.add_argument(
        "-d",
        "--dir",
        default=".",
        help="Verzeichnis mit .py-Dateien (Standard: aktuelles Verzeichnis).",
    )
    parser.add_argument(
        "-o",
        "--out",
        default="imports.txt",
        help="Ausgabedatei (Standard: imports.txt).",
    )
    parser.add_argument(
        "--include-relative",
        action="store_true",
        help="Relative Imports als 'from .pkg import *' mit ausgeben (sonst ignorieren).",
    )
    return parser.parse_args(list(argv))


def _error_path_sort_key(item):
    return str(item[0])


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    eprint = functools.partial(print, file=sys.stderr)

    directory = Path(args.dir).resolve()
    output_path = Path(args.out).resolve()

    if not directory.exists() or not directory.is_dir():
        eprint(f"Fehler: Verzeichnis nicht gefunden oder kein Verzeichnis: {directory}")
        return 2

    lines, errors = collect_imports(directory, bool(args.include_relative))

    # Stabiler Output
    sorted_lines = sorted(lines)

    try:
        write_lines(output_path, sorted_lines)
    except OSError as exc:
        eprint(f"Fehler beim Schreiben von {output_path}: {type(exc).__name__}: {exc}")
        return 2

    print(f"OK: {len(sorted_lines)} eindeutige Import-Zeilen nach {output_path} geschrieben.")

    if errors:
        eprint("\nWarnungen (Dateien konnten nicht geparst/gelesen werden):")
        for path, msg in sorted(errors, key=_error_path_sort_key):
            eprint(f" - {path}: {msg}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
