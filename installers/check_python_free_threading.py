#!/usr/bin/env python3
import sys


def detect_free_threading():
    is_free_threading = getattr(sys, "is_free_threading", None)
    if callable(is_free_threading):
        return bool(is_free_threading())

    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if callable(is_gil_enabled):
        return not bool(is_gil_enabled())

    return False


def main():
    enabled = detect_free_threading()
    print("python=", sys.version)
    print("free_threading_enabled=", enabled)
    if not enabled:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
