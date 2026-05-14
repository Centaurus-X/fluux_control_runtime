#!/usr/bin/env python3
"""Print a compact, filtered directory tree.

This helper is intentionally small and dependency-free. By default it hides
common noise directories and hidden files. Use --all to include hidden entries.
"""

import argparse
import ctypes
import os
import sys


NOISE_DIRS = frozenset({
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "node_modules",
})

FILE_ATTRIBUTE_HIDDEN = 0x2
INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF


def normalize_root(path):
    return os.path.normpath(os.path.abspath(path))


def is_windows():
    return os.name == "nt"


def get_windows_file_attributes(path):
    try:
        get_file_attributes = ctypes.windll.kernel32.GetFileAttributesW
    except AttributeError:
        return None

    get_file_attributes.argtypes = [ctypes.c_wchar_p]
    get_file_attributes.restype = ctypes.c_uint32

    attrs = get_file_attributes(path)
    if attrs == INVALID_FILE_ATTRIBUTES:
        return None
    return attrs


def has_windows_hidden_attribute(path):
    if not is_windows():
        return False

    attrs = get_windows_file_attributes(path)
    if attrs is None:
        return False

    return bool(attrs & FILE_ATTRIBUTE_HIDDEN)


def is_hidden_name(name):
    return name.startswith(".")


def is_hidden_path(path):
    name = os.path.basename(path.rstrip("\\/"))
    if is_hidden_name(name):
        return True

    if has_windows_hidden_attribute(path):
        return True

    return False


def should_skip_entry(path, name, show_all):
    if name in NOISE_DIRS:
        return True

    if not show_all and is_hidden_path(path):
        return True

    return False


def safe_scandir(path):
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                yield entry
    except (PermissionError, FileNotFoundError, OSError):
        return


def sort_entries(entries):
    def sort_key(entry):
        return (not entry.is_dir(follow_symlinks=False), os.fsencode(entry.name))

    return sorted(entries, key=sort_key)


def format_line(depth, name, is_dir_like):
    indent = "│   " * max(depth - 1, 0)
    suffix = "/" if is_dir_like else ""
    return f"{indent}├─ {name}{suffix}"


def entry_is_dir_like(entry):
    try:
        if entry.is_dir(follow_symlinks=False):
            return True
        if entry.is_symlink():
            return entry.is_dir(follow_symlinks=True)
    except OSError:
        return False
    return False


def walk(path, depth, max_depth, show_all, lines):
    if max_depth is not None and depth > max_depth:
        return

    entries = []
    for entry in safe_scandir(path):
        if should_skip_entry(entry.path, entry.name, show_all):
            continue
        entries.append(entry)

    for entry in sort_entries(entries):
        is_dir = entry_is_dir_like(entry)
        lines.append(format_line(depth, entry.name, is_dir))
        if is_dir:
            walk(entry.path, depth + 1, max_depth, show_all, lines)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Print a filtered directory tree.")
    parser.add_argument("path", nargs="?", default=".")
    parser.add_argument("--all", action="store_true", help="include hidden entries")
    parser.add_argument("--max-depth", type=int, default=None)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    root = normalize_root(args.path)
    lines = [root]
    walk(root, 1, args.max_depth, args.all, lines)
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
