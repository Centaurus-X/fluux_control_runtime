#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kleines Werkzeug zum Auflisten aller Dateien in einem Verzeichnis (inkl. Unterverzeichnisse).

Beispiele:
  python list_files.py .                 # alle Dateien ab aktuellem Verzeichnis
  python list_files.py /srv/data -p "*.py" --max-depth 2
  python list_files.py src -p "plugin_*.py" --hidden --out files.txt
"""

from __future__ import annotations
import argparse
import fnmatch
import functools
import sys
from collections import deque
from pathlib import Path
from typing import Iterator, Optional


def list_files(
    root: Path | str,
    pattern: str = "*",
    include_hidden: bool = False,
    max_depth: Optional[int] = None,
) -> Iterator[Path]:
    """
    Generator: liefert alle Dateien (als absolute, aufgelöste Paths) unter `root`,
    die dem `pattern` entsprechen.

    Arguments
    ----------
    root:
        Startverzeichnis (Path oder str).
    pattern:
        Unix-Style Pattern (z.B. '*.py', 'plugin_*.py'). Default '*'.
    include_hidden:
        Wenn False, werden Dateien/Verzeichnisse, die mit '.' beginnen, ausgeschlossen.
    max_depth:
        Maximale Rekursionstiefe (0 = nur root, 1 = root + 1 Ebene, None = unbegrenzt).

    Yields
    ------
    Path
        aufgelöster Pfad jeder passenden Datei.
    """
    base = Path(root).expanduser().resolve()
    if not base.exists():
        raise FileNotFoundError(f"Startverzeichnis existiert nicht: {base!s}")
    if not base.is_dir():
        raise NotADirectoryError(f"Startpfad ist kein Verzeichnis: {base!s}")

    # Matcher mit functools.partial: wir fixieren das Pattern als 'pat' Argument
    matcher = functools.partial(fnmatch.fnmatch, pat=pattern)

    # Breadth-first Traversal (vermeidet tiefe Rekursion)
    start_parts_len = len(base.parts)
    q = deque([(base, 0)])  # (current_dir, depth_from_root)

    while q:
        current_dir, depth = q.popleft()
        try:
            entries = list(current_dir.iterdir())
        except PermissionError:
            # Keine Leserechte: überspringen, aber Programm nicht abstürzen lassen.
            continue
        except OSError:
            # Sonstige IO-Fehler: überspringen
            continue

        for entry in entries:
            # Optional: versteckte Dateien/Ordner überspringen
            rel_parts = entry.relative_to(base).parts
            if not include_hidden and any(p.startswith(".") for p in rel_parts):
                continue

            try:
                if entry.is_file():
                    if matcher(entry.name):
                        yield entry.resolve()
                elif entry.is_dir():
                    # prüfen, ob wir noch tiefer gehen dürfen
                    if max_depth is None or depth < max_depth:
                        q.append((entry, depth + 1))
            except OSError:
                # Dateien/Links, die beim Zugriff Fehler werfen -> überspringen
                continue


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="list_files",
        description="Listet Dateien eines Verzeichnisses rekursiv auf (inkl. Unterverzeichnisse).",
    )
    p.add_argument("root", nargs="?", default=".", help="Startverzeichnis (Default: aktuelles Verzeichnis)")
    p.add_argument(
        "-p", "--pattern", default="*", help="Dateinamens-Pattern (z. B. '*.py' oder 'plugin_*.py'). Default='*'"
    )
    p.add_argument(
        "--hidden", action="store_true", help="Versteckte Dateien/Verzeichnisse (Beginn mit '.') mitaufnehmen"
    )
    p.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="Maximale Tiefe (0 = nur root). Default: unbegrenzt",
    )
    p.add_argument(
        "-o", "--out", type=str, default=None, help="Optional: Ausgabedatei zum Schreiben der Liste (eine Zeile pro Pfad)"
    )
    p.add_argument(
        "-q", "--quiet", action="store_true", help="Nur Fehler (keine zusätzlichen Statusausgaben)"
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI-Entrypoint. Gibt 0 bei Erfolg zurück."""
    args = _parse_args(argv)

    root = Path(args.root)
    out_path = Path(args.out) if args.out else None

    try:
        files_iter = list_files(root, pattern=args.pattern, include_hidden=args.hidden, max_depth=args.max_depth)
    except (FileNotFoundError, NotADirectoryError) as err:
        print(f"[ERROR] {err}", file=sys.stderr)
        return 2

    # Schreibe entweder auf stdout oder in Datei
    try:
        if out_path:
            with out_path.open("w", encoding="utf-8") as f:
                for p in files_iter:
                    f.write(str(p) + "\n")
            if not args.quiet:
                print(f"[OK] {out_path} geschrieben.")
        else:
            for p in files_iter:
                print(p)
    except OSError as err:
        print(f"[ERROR] Kann nicht schreiben: {err}", file=sys.stderr)
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
