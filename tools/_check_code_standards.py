# -*- coding: utf-8 -*-

# _check_code_standards.py


"""
Dynamic standards checker for Python projects.

Supported external tools:
- Ruff lint
- Ruff format check
- Black
- Pylint
- Bandit
- Vulture

Supported internal tool:
- project-specific standards checker from _check_code_specific_standards

The module is designed for two integration styles:
- library usage with direct function calls
- workflow automation with normalized JSON-friendly results

Example:

    from _check_code_standards import run_extended_code_standards

    result = run_extended_code_standards(
        paths=["src", "tests"],
        cwd=".",
        guidelines_file="Entwicklungsrichtlinieren.md",
        include_versions=True,
    )

    if result["status"] != "passed":
        print(result["counts"])

python _check_code_standards.py suite --paths src tests --include-specific-standards --guidelines-file Entwicklungsrichtlinieren.md --pretty

"""

import argparse
import importlib
import importlib.util
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any, Mapping, Sequence


MODULE_NAME = "_check_code_standards"
MODULE_VERSION = "1.1.0"

TOOL_RUFF = "ruff"
TOOL_RUFF_FORMAT = "ruff_format"
TOOL_BLACK = "black"
TOOL_PYLINT = "pylint"
TOOL_BANDIT = "bandit"
TOOL_VULTURE = "vulture"
TOOL_SPECIFIC_STANDARDS = "specific_standards"

SUPPORTED_TOOLS = (
    TOOL_RUFF,
    TOOL_RUFF_FORMAT,
    TOOL_BLACK,
    TOOL_PYLINT,
    TOOL_BANDIT,
    TOOL_VULTURE,
    TOOL_SPECIFIC_STANDARDS,
)

DEFAULT_SUITE_TOOLS = (
    TOOL_RUFF,
    TOOL_BLACK,
    TOOL_PYLINT,
    TOOL_BANDIT,
    TOOL_VULTURE,
)

EXTENDED_SUITE_TOOLS = DEFAULT_SUITE_TOOLS + (TOOL_SPECIFIC_STANDARDS,)

TOOL_SPECS = {
    TOOL_RUFF: {
        "kind": "external",
        "binary": "ruff",
        "module": "ruff",
        "version_args": ["--version"],
    },
    TOOL_RUFF_FORMAT: {
        "kind": "external",
        "binary": "ruff",
        "module": "ruff",
        "version_args": ["--version"],
    },
    TOOL_BLACK: {
        "kind": "external",
        "binary": "black",
        "module": "black",
        "version_args": ["--version"],
    },
    TOOL_PYLINT: {
        "kind": "external",
        "binary": "pylint",
        "module": "pylint",
        "version_args": ["--version"],
    },
    TOOL_BANDIT: {
        "kind": "external",
        "binary": "bandit",
        "module": "bandit",
        "version_args": ["--version"],
    },
    TOOL_VULTURE: {
        "kind": "external",
        "binary": "vulture",
        "module": "vulture",
        "version_args": ["--version"],
    },
    TOOL_SPECIFIC_STANDARDS: {
        "kind": "internal",
        "module": "_check_code_specific_standards",
        "callable": "run_specific_standards",
        "metadata_callable": "get_module_metadata",
    },
}

STATUS_PASSED = "passed"
STATUS_FINDINGS = "findings"
STATUS_ERROR = "error"
STATUS_TIMEOUT = "timeout"
STATUS_MISSING_TOOL = "missing_tool"

_BLACK_REFORMAT_RE = re.compile(r"^\s*would reformat\s+(?P<path>.+?)\s*$", re.MULTILINE)
_BLACK_ERROR_RE = re.compile(
    r"^\s*error:\s+cannot format\s+(?P<path>.+?):\s+(?P<message>.+?)\s*$",
    re.MULTILINE,
)

_RUFF_FORMAT_REFORMAT_RE = re.compile(
    r"^\s*Would reformat:\s+(?P<path>.+?)\s*$",
    re.MULTILINE,
)
_RUFF_FORMAT_ERROR_RE = re.compile(r"^\s*error:\s+(?P<message>.+?)\s*$", re.MULTILINE)

_VULTURE_FINDING_RE = re.compile(
    r"^(?P<path>.+?):(?P<line>\d+):\s+(?P<message>.+?)\s+\((?P<confidence>\d+)% confidence\)\s*$",
    re.MULTILINE,
)


def _utc_now_iso() -> str:
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def _coerce_path(value: Any) -> str:
    try:
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, os.PathLike):
            return os.fspath(value)
        return str(value)
    except Exception:
        return str(value)


def _ensure_sequence(value: Any) -> list[Any]:
    try:
        if value is None:
            return []
        if isinstance(value, (str, bytes, os.PathLike, Path)):
            return [value]
        return list(value)
    except Exception:
        return [value]


def _normalize_paths(paths: Any) -> list[str]:
    normalized = []
    for item in _ensure_sequence(paths):
        text = _coerce_path(item).strip()
        if text:
            normalized.append(text)
    if not normalized:
        return ["."]
    return normalized


def _normalize_tool_names(tools: Any, *, default_tools: Sequence[str] | None = None) -> list[str]:
    names = []
    for item in _ensure_sequence(tools):
        name = str(item).strip()
        if not name:
            continue
        if name not in SUPPORTED_TOOLS:
            supported = ", ".join(SUPPORTED_TOOLS)
            raise ValueError(f"Unknown tool '{name}'. Supported tools: {supported}")
        names.append(name)
    if not names:
        return list(default_tools or DEFAULT_SUITE_TOOLS)
    return names


def _normalize_mapping(value: Any) -> dict[str, Any]:
    try:
        if isinstance(value, Mapping):
            return dict(value)
        return {}
    except Exception:
        return {}


def _normalize_additional_args(additional_args: Any) -> list[str]:
    if additional_args is None:
        return []
    if isinstance(additional_args, str):
        return shlex.split(additional_args)
    return [str(item) for item in additional_args]


def _normalize_command_prefix(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return shlex.split(value)
    return [str(item) for item in value]


def _normalize_env(env: Mapping[str, Any] | None) -> dict[str, str]:
    merged = dict(os.environ)
    if env:
        for key, value in env.items():
            merged[str(key)] = "" if value is None else str(value)
    return merged


def _join_csv(values: Any) -> str:
    parts = []
    for item in _ensure_sequence(values):
        text = str(item).strip()
        if text:
            parts.append(text)
    return ",".join(parts)


def _append_csv_flag(command: list[str], flag: str, values: Any) -> None:
    joined = _join_csv(values)
    if joined:
        command.extend([flag, joined])


def _stringify_command(command: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in command)


def _safe_json_loads(text: str) -> Any:
    if not text or not text.strip():
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _combined_output(stdout: str, stderr: str) -> str:
    if stdout and stderr:
        return stdout + "\n" + stderr
    return stdout or stderr or ""


def _count_by(findings: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in findings:
        value = finding.get(key)
        if value is None:
            continue
        text = str(value)
        counts[text] = counts.get(text, 0) + 1
    return counts


def _make_error_payload(kind: str, message: str, details: Any = None) -> dict[str, Any]:
    payload = {
        "type": kind,
        "message": message,
    }
    if details is not None:
        payload["details"] = details
    return payload


def _contains_any(text: str, needles: Sequence[str]) -> bool:
    lowered = text.lower()
    return any(needle.lower() in lowered for needle in needles)


def _is_usage_error_returncode(tool_name: str, returncode: int | None) -> bool:
    if returncode is None:
        return False
    if tool_name == TOOL_PYLINT:
        return bool(returncode & 32)
    if tool_name == TOOL_VULTURE:
        return returncode == 2
    if tool_name == TOOL_RUFF:
        return returncode == 2
    if tool_name == TOOL_RUFF_FORMAT:
        return returncode == 2
    if tool_name == TOOL_BLACK:
        return returncode == 123
    return False


def _tool_is_internal(tool_name: str) -> bool:
    return TOOL_SPECS[tool_name]["kind"] == "internal"


def _resolve_external_command(
    tool_name: str,
    *,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    spec = TOOL_SPECS[tool_name]
    chosen_python = _coerce_path(python_executable) if python_executable else sys.executable

    explicit_command = _normalize_command_prefix(executable)
    if explicit_command:
        return {
            "available": True,
            "command": explicit_command,
            "source": "explicit",
            "module_available": False,
            "binary_available": False,
        }

    binary_name = spec["binary"]
    module_name = spec["module"]

    binary_available = shutil.which(binary_name) is not None
    module_available = importlib.util.find_spec(module_name) is not None if module_name else False

    if prefer_module and module_available:
        return {
            "available": True,
            "command": [chosen_python, "-m", module_name],
            "source": "module",
            "module_available": True,
            "binary_available": binary_available,
        }

    if binary_available:
        return {
            "available": True,
            "command": [binary_name],
            "source": "binary",
            "module_available": module_available,
            "binary_available": True,
        }

    if module_available:
        return {
            "available": True,
            "command": [chosen_python, "-m", module_name],
            "source": "module",
            "module_available": True,
            "binary_available": False,
        }

    return {
        "available": False,
        "command": [],
        "source": "missing",
        "module_available": False,
        "binary_available": False,
        "error": _make_error_payload(
            "missing_tool",
            f"Tool '{tool_name}' was not found as executable '{binary_name}' or Python module '{module_name}'.",
        ),
    }


def _import_internal_tool(tool_name: str) -> dict[str, Any]:
    spec = TOOL_SPECS[tool_name]
    module_name = spec["module"]
    callable_name = spec["callable"]
    metadata_name = spec.get("metadata_callable")

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        return {
            "available": False,
            "module_name": module_name,
            "callable_name": callable_name,
            "metadata_callable_name": metadata_name,
            "error": _make_error_payload(
                "missing_tool",
                f"Internal tool module '{module_name}' could not be imported.",
                {"exception": f"{type(exc).__name__}: {exc}"},
            ),
        }

    runner = getattr(module, callable_name, None)
    if not callable(runner):
        return {
            "available": False,
            "module_name": module_name,
            "callable_name": callable_name,
            "metadata_callable_name": metadata_name,
            "error": _make_error_payload(
                "missing_tool",
                f"Internal tool callable '{module_name}.{callable_name}' is missing.",
            ),
        }

    metadata_callable = getattr(module, metadata_name, None) if metadata_name else None

    return {
        "available": True,
        "module_name": module_name,
        "callable_name": callable_name,
        "metadata_callable_name": metadata_name,
        "module": module,
        "runner": runner,
        "metadata_callable": metadata_callable if callable(metadata_callable) else None,
        "source": "internal",
        "command": [module_name],
    }


def _run_subprocess(
    command: Sequence[str],
    *,
    cwd: str | None = None,
    env: Mapping[str, Any] | None = None,
    timeout_seconds: float = 300.0,
) -> dict[str, Any]:
    started_at = _utc_now_iso()
    started_perf = time.perf_counter()
    normalized_cwd = _coerce_path(cwd) if cwd else None
    normalized_env = _normalize_env(env)

    try:
        completed = subprocess.run(
            [str(part) for part in command],
            cwd=normalized_cwd,
            env=normalized_env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        ended_at = _utc_now_iso()
        duration_seconds = round(time.perf_counter() - started_perf, 6)
        stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout.decode("utf-8", errors="replace") if exc.stdout else "")
        stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr.decode("utf-8", errors="replace") if exc.stderr else "")
        return {
            "spawn_ok": False,
            "timed_out": True,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds,
            "command": [str(part) for part in command],
            "command_text": _stringify_command(command),
            "cwd": normalized_cwd,
            "returncode": None,
            "stdout": stdout,
            "stderr": stderr,
            "error": _make_error_payload("timeout", f"Timeout after {timeout_seconds} seconds."),
        }
    except OSError as exc:
        ended_at = _utc_now_iso()
        duration_seconds = round(time.perf_counter() - started_perf, 6)
        return {
            "spawn_ok": False,
            "timed_out": False,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds,
            "command": [str(part) for part in command],
            "command_text": _stringify_command(command),
            "cwd": normalized_cwd,
            "returncode": None,
            "stdout": "",
            "stderr": "",
            "error": _make_error_payload(type(exc).__name__, str(exc)),
        }

    ended_at = _utc_now_iso()
    duration_seconds = round(time.perf_counter() - started_perf, 6)
    return {
        "spawn_ok": True,
        "timed_out": False,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "command": [str(part) for part in command],
        "command_text": _stringify_command(command),
        "cwd": normalized_cwd,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "error": None,
    }


def _make_base_result(
    tool_name: str,
    paths: Sequence[str],
    resolution: Mapping[str, Any],
    process_result: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if process_result is None:
        process_result = {}

    return {
        "module": MODULE_NAME,
        "tool": tool_name,
        "version": MODULE_VERSION,
        "paths": list(paths),
        "command_source": resolution.get("source"),
        "command": list(process_result.get("command", resolution.get("command", []))),
        "command_text": process_result.get(
            "command_text",
            _stringify_command(process_result.get("command", resolution.get("command", []))),
        ),
        "cwd": process_result.get("cwd"),
        "started_at": process_result.get("started_at"),
        "ended_at": process_result.get("ended_at"),
        "duration_seconds": process_result.get("duration_seconds"),
        "returncode": process_result.get("returncode"),
        "stdout": process_result.get("stdout", ""),
        "stderr": process_result.get("stderr", ""),
        "status": STATUS_ERROR,
        "execution_ok": False,
        "timed_out": bool(process_result.get("timed_out")),
        "missing_tool": False,
        "passed": False,
        "has_findings": False,
        "finding_count": 0,
        "findings": [],
        "summary": {},
        "raw": None,
        "error": process_result.get("error"),
        "compatibility_warnings": [],
    }


def _finalize_result(
    result: dict[str, Any],
    *,
    status: str,
    execution_ok: bool,
    findings: Sequence[Mapping[str, Any]] | None = None,
    summary: Mapping[str, Any] | None = None,
    raw: Any = None,
    error: Mapping[str, Any] | None = None,
    compatibility_warnings: Sequence[str] | None = None,
) -> dict[str, Any]:
    normalized_findings = list(findings or [])
    result["status"] = status
    result["execution_ok"] = bool(execution_ok)
    result["findings"] = normalized_findings
    result["finding_count"] = len(normalized_findings)
    result["has_findings"] = bool(normalized_findings)
    result["passed"] = status == STATUS_PASSED
    result["summary"] = dict(summary or {})
    result["raw"] = raw
    result["error"] = dict(error) if error is not None else result.get("error")
    result["compatibility_warnings"] = list(compatibility_warnings or [])
    return result


def _missing_tool_result(
    tool_name: str,
    paths: Sequence[str],
    resolution: Mapping[str, Any],
    *,
    cwd: str | None = None,
) -> dict[str, Any]:
    result = _make_base_result(
        tool_name,
        paths,
        resolution,
        {
            "cwd": _coerce_path(cwd) if cwd else None,
            "command": resolution.get("command", []),
            "command_text": _stringify_command(resolution.get("command", [])),
            "stdout": "",
            "stderr": "",
            "returncode": None,
            "started_at": _utc_now_iso(),
            "ended_at": _utc_now_iso(),
            "duration_seconds": 0.0,
            "timed_out": False,
            "error": resolution.get("error"),
        },
    )
    result["missing_tool"] = True
    return _finalize_result(
        result,
        status=STATUS_MISSING_TOOL,
        execution_ok=False,
        findings=[],
        summary={},
        raw=None,
        error=resolution.get("error"),
    )


def _parse_ruff_json(raw: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(raw, list):
        return [], {"rule_counts": {}, "fixable_count": 0}

    findings: list[dict[str, Any]] = []
    fixable_count = 0

    for item in raw:
        if not isinstance(item, Mapping):
            continue

        location = item.get("location") or {}
        end_location = item.get("end_location") or {}
        fix = item.get("fix")
        has_fix = bool(fix)
        if has_fix:
            fixable_count += 1

        extra: dict[str, Any] = {}
        if isinstance(fix, Mapping):
            applicability = fix.get("applicability")
            message = fix.get("message")
            edits = fix.get("edits")
            if applicability is not None:
                extra["applicability"] = applicability
            if message is not None:
                extra["fix_message"] = message
            if edits is not None:
                extra["fix_edits"] = edits
        if item.get("url"):
            extra["url"] = item.get("url")
        if item.get("noqa_row") is not None:
            extra["noqa_row"] = item.get("noqa_row")

        findings.append(
            {
                "tool": TOOL_RUFF,
                "path": item.get("filename"),
                "line": location.get("row"),
                "column": location.get("column"),
                "end_line": end_location.get("row"),
                "end_column": end_location.get("column"),
                "code": item.get("code"),
                "symbol": None,
                "message": item.get("message"),
                "severity": None,
                "confidence": None,
                "object": None,
                "module": None,
                "kind": "lint",
                "fixable": has_fix,
                "extra": extra,
            }
        )

    summary = {
        "rule_counts": _count_by(findings, "code"),
        "fixable_count": fixable_count,
    }
    return findings, summary


def _parse_ruff_format_text(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    findings = []
    for match in _RUFF_FORMAT_REFORMAT_RE.finditer(text):
        path = match.group("path").strip()
        findings.append(
            {
                "tool": TOOL_RUFF_FORMAT,
                "path": path,
                "line": None,
                "column": None,
                "end_line": None,
                "end_column": None,
                "code": "FORMAT",
                "symbol": None,
                "message": "File would be reformatted by ruff format.",
                "severity": None,
                "confidence": None,
                "object": None,
                "module": None,
                "kind": "format",
                "fixable": True,
                "extra": {},
            }
        )

    errors = []
    for match in _RUFF_FORMAT_ERROR_RE.finditer(text):
        message = match.group("message").strip()
        if message:
            errors.append({"message": message})

    summary = {
        "files_to_reformat": [item["path"] for item in findings],
        "error_count": len(errors),
    }
    return findings, summary, errors


def _parse_black_text(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    findings = []
    for match in _BLACK_REFORMAT_RE.finditer(text):
        path = match.group("path").strip()
        findings.append(
            {
                "tool": TOOL_BLACK,
                "path": path,
                "line": None,
                "column": None,
                "end_line": None,
                "end_column": None,
                "code": "FORMAT",
                "symbol": None,
                "message": "File would be reformatted by Black.",
                "severity": None,
                "confidence": None,
                "object": None,
                "module": None,
                "kind": "format",
                "fixable": True,
                "extra": {},
            }
        )

    errors = []
    for match in _BLACK_ERROR_RE.finditer(text):
        errors.append(
            {
                "path": match.group("path").strip(),
                "message": match.group("message").strip(),
            }
        )

    summary = {
        "files_to_reformat": [item["path"] for item in findings],
        "error_count": len(errors),
    }
    return findings, summary, errors


def _normalize_pylint_message(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "tool": TOOL_PYLINT,
        "path": item.get("path"),
        "line": item.get("line"),
        "column": item.get("column"),
        "end_line": item.get("endLine") if item.get("endLine") is not None else item.get("end_line"),
        "end_column": item.get("endColumn") if item.get("endColumn") is not None else item.get("end_column"),
        "code": item.get("messageId") if item.get("messageId") is not None else item.get("message-id"),
        "symbol": item.get("symbol"),
        "message": item.get("message"),
        "severity": item.get("type"),
        "confidence": item.get("confidence"),
        "object": item.get("obj"),
        "module": item.get("module"),
        "kind": "lint",
        "fixable": False,
        "extra": {},
    }


def _parse_pylint_json(raw: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    messages: list[Mapping[str, Any]] = []
    score = None
    statistics: dict[str, Any] = {}
    used_output_format = "json2"

    if isinstance(raw, Mapping):
        score = raw.get("score")
        stats = raw.get("statistics")
        if isinstance(stats, Mapping):
            statistics = dict(stats)
        raw_messages = raw.get("messages")
        if isinstance(raw_messages, list):
            messages = [item for item in raw_messages if isinstance(item, Mapping)]
    elif isinstance(raw, list):
        used_output_format = "json"
        messages = [item for item in raw if isinstance(item, Mapping)]

    findings = [_normalize_pylint_message(item) for item in messages]
    summary = {
        "score": score,
        "statistics": statistics,
        "message_counts": _count_by(findings, "severity"),
        "used_output_format": used_output_format,
    }
    return findings, summary


def _parse_bandit_json(raw: Any) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    if not isinstance(raw, Mapping):
        return [], {"metrics": {}, "severity_counts": {}, "confidence_counts": {}}, []

    results = raw.get("results")
    errors = raw.get("errors")
    metrics = raw.get("metrics")

    normalized_results = []
    if isinstance(results, list):
        for item in results:
            if not isinstance(item, Mapping):
                continue

            normalized_results.append(
                {
                    "tool": TOOL_BANDIT,
                    "path": item.get("filename"),
                    "line": item.get("line_number"),
                    "column": None,
                    "end_line": None,
                    "end_column": None,
                    "code": item.get("test_id"),
                    "symbol": item.get("test_name"),
                    "message": item.get("issue_text"),
                    "severity": item.get("issue_severity"),
                    "confidence": item.get("issue_confidence"),
                    "object": None,
                    "module": None,
                    "kind": "security",
                    "fixable": False,
                    "extra": {
                        "more_info": item.get("more_info"),
                        "line_range": item.get("line_range"),
                        "issue_cwe": item.get("issue_cwe"),
                    },
                }
            )

    normalized_errors = []
    if isinstance(errors, list):
        for item in errors:
            if isinstance(item, Mapping):
                normalized_errors.append(dict(item))
            else:
                normalized_errors.append({"message": str(item)})

    totals = {}
    if isinstance(metrics, Mapping):
        totals_mapping = metrics.get("_totals")
        if isinstance(totals_mapping, Mapping):
            totals = dict(totals_mapping)

    summary = {
        "metrics": totals,
        "severity_counts": {
            "LOW": totals.get("SEVERITY.LOW", 0),
            "MEDIUM": totals.get("SEVERITY.MEDIUM", 0),
            "HIGH": totals.get("SEVERITY.HIGH", 0),
            "UNDEFINED": totals.get("SEVERITY.UNDEFINED", 0),
        },
        "confidence_counts": {
            "LOW": totals.get("CONFIDENCE.LOW", 0),
            "MEDIUM": totals.get("CONFIDENCE.MEDIUM", 0),
            "HIGH": totals.get("CONFIDENCE.HIGH", 0),
            "UNDEFINED": totals.get("CONFIDENCE.UNDEFINED", 0),
        },
    }
    return normalized_results, summary, normalized_errors


def _parse_vulture_text(text: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    findings = []
    for match in _VULTURE_FINDING_RE.finditer(text):
        findings.append(
            {
                "tool": TOOL_VULTURE,
                "path": match.group("path").strip(),
                "line": int(match.group("line")),
                "column": None,
                "end_line": None,
                "end_column": None,
                "code": None,
                "symbol": None,
                "message": match.group("message").strip(),
                "severity": None,
                "confidence": int(match.group("confidence")),
                "object": None,
                "module": None,
                "kind": "dead_code",
                "fixable": True,
                "extra": {},
            }
        )

    summary = {
        "confidence_counts": _count_by(findings, "confidence"),
    }
    return findings, summary


def _build_ruff_check_args(
    *,
    paths: Sequence[str],
    config_file: Any = None,
    select: Any = None,
    ignore: Any = None,
    extend_select: Any = None,
    fix: bool = False,
    unsafe_fixes: bool = False,
    preview: bool = False,
    output_format: str = "json",
    exit_zero: bool = False,
    exit_non_zero_on_fix: bool = False,
    additional_args: Any = None,
) -> list[str]:
    command = ["check"]

    if config_file:
        command.extend(["--config", _coerce_path(config_file)])
    if output_format:
        command.extend(["--output-format", str(output_format)])
    _append_csv_flag(command, "--select", select)
    _append_csv_flag(command, "--ignore", ignore)
    _append_csv_flag(command, "--extend-select", extend_select)

    if fix:
        command.append("--fix")
    if unsafe_fixes:
        command.append("--unsafe-fixes")
    if preview:
        command.append("--preview")
    if exit_zero:
        command.append("--exit-zero")
    if exit_non_zero_on_fix:
        command.append("--exit-non-zero-on-fix")

    command.extend(_normalize_additional_args(additional_args))
    command.extend(_normalize_paths(paths))
    return command


def _build_ruff_format_args(
    *,
    paths: Sequence[str],
    config_file: Any = None,
    check: bool = True,
    diff: bool = False,
    preview: bool = False,
    additional_args: Any = None,
) -> list[str]:
    command = ["format"]

    if config_file:
        command.extend(["--config", _coerce_path(config_file)])
    if check:
        command.append("--check")
    if diff:
        command.append("--diff")
    if preview:
        command.append("--preview")

    command.extend(_normalize_additional_args(additional_args))
    command.extend(_normalize_paths(paths))
    return command


def _build_black_args(
    *,
    paths: Sequence[str],
    config_file: Any = None,
    check: bool = True,
    diff: bool = False,
    quiet: bool = False,
    preview: bool = False,
    additional_args: Any = None,
) -> list[str]:
    command: list[str] = []

    if config_file:
        command.extend(["--config", _coerce_path(config_file)])
    if check:
        command.append("--check")
    if diff:
        command.append("--diff")
    if quiet:
        command.append("--quiet")
    if preview:
        command.append("--preview")

    command.extend(_normalize_additional_args(additional_args))
    command.extend(_normalize_paths(paths))
    return command


def _build_pylint_args(
    *,
    paths: Sequence[str],
    rcfile: Any = None,
    output_format: str = "json2",
    recursive: bool | None = True,
    jobs: int | None = None,
    score: bool = True,
    exit_zero: bool = False,
    additional_args: Any = None,
) -> list[str]:
    command: list[str] = []

    if rcfile:
        command.extend(["--rcfile", _coerce_path(rcfile)])
    if output_format:
        command.extend(["--output-format", str(output_format)])
    if recursive is not None:
        command.append(f"--recursive={'y' if recursive else 'n'}")
    if jobs is not None:
        command.extend(["--jobs", str(jobs)])
    if score is not None:
        command.append(f"--score={'y' if score else 'n'}")
    if exit_zero:
        command.append("--exit-zero")

    command.extend(_normalize_additional_args(additional_args))
    command.extend(_normalize_paths(paths))
    return command


def _build_bandit_args(
    *,
    paths: Sequence[str],
    config_file: Any = None,
    recursive: bool = True,
    severity_level: str | None = None,
    confidence_level: str | None = None,
    baseline: Any = None,
    format_name: str = "json",
    exit_zero: bool = False,
    additional_args: Any = None,
) -> list[str]:
    command: list[str] = []

    if recursive:
        command.append("-r")
    if config_file:
        command.extend(["-c", _coerce_path(config_file)])
    if severity_level:
        command.extend(["--severity-level", str(severity_level)])
    if confidence_level:
        command.extend(["--confidence-level", str(confidence_level)])
    if baseline:
        command.extend(["-b", _coerce_path(baseline)])
    if format_name:
        command.extend(["-f", str(format_name)])
    if exit_zero:
        command.append("--exit-zero")

    command.extend(_normalize_additional_args(additional_args))
    command.extend(_normalize_paths(paths))
    return command


def _build_vulture_args(
    *,
    paths: Sequence[str],
    config_file: Any = None,
    min_confidence: int | None = None,
    sort_by_size: bool = False,
    make_whitelist: bool = False,
    additional_args: Any = None,
) -> list[str]:
    command: list[str] = []

    if config_file:
        command.extend(["--config", _coerce_path(config_file)])
    if min_confidence is not None:
        command.extend(["--min-confidence", str(min_confidence)])
    if sort_by_size:
        command.append("--sort-by-size")
    if make_whitelist:
        command.append("--make-whitelist")

    command.extend(_normalize_additional_args(additional_args))
    command.extend(_normalize_paths(paths))
    return command


def _normalize_internal_result(
    tool_name: str,
    payload: Mapping[str, Any] | Any,
    *,
    paths: Sequence[str],
    cwd: Any = None,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {
            "module": MODULE_NAME,
            "tool": tool_name,
            "version": MODULE_VERSION,
            "paths": list(paths),
            "command_source": "internal",
            "command": [TOOL_SPECS[tool_name]["module"]],
            "command_text": TOOL_SPECS[tool_name]["module"],
            "cwd": _coerce_path(cwd) if cwd else None,
            "started_at": _utc_now_iso(),
            "ended_at": _utc_now_iso(),
            "duration_seconds": 0.0,
            "returncode": None,
            "stdout": "",
            "stderr": "",
            "status": STATUS_ERROR,
            "execution_ok": False,
            "timed_out": False,
            "missing_tool": False,
            "passed": False,
            "has_findings": False,
            "finding_count": 0,
            "findings": [],
            "summary": {},
            "raw": None,
            "error": _make_error_payload(
                "internal_tool_error",
                f"Internal tool '{tool_name}' returned a non-mapping payload.",
            ),
            "compatibility_warnings": [],
        }

    normalized = dict(payload)
    normalized.setdefault("module", MODULE_NAME)
    normalized["tool"] = tool_name
    normalized.setdefault("version", normalized.get("version", MODULE_VERSION))
    normalized.setdefault("paths", list(paths))
    normalized.setdefault("command_source", "internal")
    normalized.setdefault("command", [TOOL_SPECS[tool_name]["module"]])
    normalized.setdefault("command_text", TOOL_SPECS[tool_name]["module"])
    normalized.setdefault("cwd", _coerce_path(cwd) if cwd else None)
    normalized.setdefault("started_at", _utc_now_iso())
    normalized.setdefault("ended_at", _utc_now_iso())
    normalized.setdefault("duration_seconds", 0.0)
    normalized.setdefault("returncode", None)
    normalized.setdefault("stdout", "")
    normalized.setdefault("stderr", "")
    normalized.setdefault("timed_out", False)
    normalized.setdefault("missing_tool", False)
    normalized.setdefault("passed", normalized.get("status") == STATUS_PASSED)
    normalized.setdefault("has_findings", bool(normalized.get("findings")))
    normalized.setdefault("finding_count", len(list(normalized.get("findings", []))))
    normalized.setdefault("findings", [])
    normalized.setdefault("summary", {})
    normalized.setdefault("raw", None)
    normalized.setdefault("error", None)
    normalized.setdefault("compatibility_warnings", [])
    normalized.setdefault("execution_ok", True)
    return normalized


def run_ruff_check(
    paths: Any = None,
    *,
    cwd: Any = None,
    config_file: Any = None,
    select: Any = None,
    ignore: Any = None,
    extend_select: Any = None,
    fix: bool = False,
    unsafe_fixes: bool = False,
    preview: bool = False,
    output_format: str = "json",
    exit_zero: bool = False,
    exit_non_zero_on_fix: bool = False,
    additional_args: Any = None,
    timeout_seconds: float = 300.0,
    env: Mapping[str, Any] | None = None,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    normalized_paths = _normalize_paths(paths)
    resolution = _resolve_external_command(
        TOOL_RUFF,
        executable=executable,
        python_executable=python_executable,
        prefer_module=prefer_module,
    )
    if not resolution.get("available"):
        return _missing_tool_result(TOOL_RUFF, normalized_paths, resolution, cwd=_coerce_path(cwd) if cwd else None)

    command_args = _build_ruff_check_args(
        paths=normalized_paths,
        config_file=config_file,
        select=select,
        ignore=ignore,
        extend_select=extend_select,
        fix=fix,
        unsafe_fixes=unsafe_fixes,
        preview=preview,
        output_format=output_format,
        exit_zero=exit_zero,
        exit_non_zero_on_fix=exit_non_zero_on_fix,
        additional_args=additional_args,
    )
    process_result = _run_subprocess(
        list(resolution["command"]) + command_args,
        cwd=_coerce_path(cwd) if cwd else None,
        env=env,
        timeout_seconds=timeout_seconds,
    )
    result = _make_base_result(TOOL_RUFF, normalized_paths, resolution, process_result)

    if not process_result.get("spawn_ok"):
        return _finalize_result(
            result,
            status=STATUS_TIMEOUT if process_result.get("timed_out") else STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary={},
            raw=None,
            error=process_result.get("error"),
        )

    raw = _safe_json_loads(process_result.get("stdout", ""))
    findings, summary = _parse_ruff_json(raw)
    combined = _combined_output(result["stdout"], result["stderr"])

    if raw is None and result["returncode"] == 0 and not combined.strip():
        return _finalize_result(
            result,
            status=STATUS_PASSED,
            execution_ok=True,
            findings=[],
            summary={"rule_counts": {}, "fixable_count": 0},
            raw=None,
        )

    if raw is None and result["returncode"] == 2:
        return _finalize_result(
            result,
            status=STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary={},
            raw=None,
            error=_make_error_payload(
                "parse_error",
                "Ruff output could not be parsed as JSON.",
                {"output": combined},
            ),
        )

    if result["returncode"] == 0 and not findings:
        return _finalize_result(
            result,
            status=STATUS_PASSED,
            execution_ok=True,
            findings=findings,
            summary=summary,
            raw=raw,
        )

    if result["returncode"] in (0, 1) or findings:
        return _finalize_result(
            result,
            status=STATUS_FINDINGS,
            execution_ok=True,
            findings=findings,
            summary=summary,
            raw=raw,
        )

    return _finalize_result(
        result,
        status=STATUS_ERROR,
        execution_ok=False,
        findings=findings,
        summary=summary,
        raw=raw,
        error=_make_error_payload("ruff_error", "Ruff did not complete successfully.", {"output": combined}),
    )


def run_ruff_format_check(
    paths: Any = None,
    *,
    cwd: Any = None,
    config_file: Any = None,
    check: bool = True,
    diff: bool = False,
    preview: bool = False,
    additional_args: Any = None,
    timeout_seconds: float = 300.0,
    env: Mapping[str, Any] | None = None,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    normalized_paths = _normalize_paths(paths)
    resolution = _resolve_external_command(
        TOOL_RUFF_FORMAT,
        executable=executable,
        python_executable=python_executable,
        prefer_module=prefer_module,
    )
    if not resolution.get("available"):
        return _missing_tool_result(TOOL_RUFF_FORMAT, normalized_paths, resolution, cwd=_coerce_path(cwd) if cwd else None)

    command_args = _build_ruff_format_args(
        paths=normalized_paths,
        config_file=config_file,
        check=check,
        diff=diff,
        preview=preview,
        additional_args=additional_args,
    )
    process_result = _run_subprocess(
        list(resolution["command"]) + command_args,
        cwd=_coerce_path(cwd) if cwd else None,
        env=env,
        timeout_seconds=timeout_seconds,
    )
    result = _make_base_result(TOOL_RUFF_FORMAT, normalized_paths, resolution, process_result)

    if not process_result.get("spawn_ok"):
        return _finalize_result(
            result,
            status=STATUS_TIMEOUT if process_result.get("timed_out") else STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary={},
            raw=None,
            error=process_result.get("error"),
        )

    combined = _combined_output(result["stdout"], result["stderr"])
    findings, summary, errors = _parse_ruff_format_text(combined)
    if errors:
        summary = dict(summary)
        summary["errors"] = errors

    if result["returncode"] == 0 and not findings and not errors:
        return _finalize_result(
            result,
            status=STATUS_PASSED,
            execution_ok=True,
            findings=[],
            summary=summary,
            raw=None,
        )

    if result["returncode"] == 1 or findings:
        return _finalize_result(
            result,
            status=STATUS_FINDINGS,
            execution_ok=True,
            findings=findings,
            summary=summary,
            raw=None,
        )

    return _finalize_result(
        result,
        status=STATUS_ERROR,
        execution_ok=False,
        findings=findings,
        summary=summary,
        raw=None,
        error=_make_error_payload(
            "ruff_format_error",
            "ruff format did not complete successfully.",
            {"output": combined},
        ),
    )


def run_black_check(
    paths: Any = None,
    *,
    cwd: Any = None,
    config_file: Any = None,
    check: bool = True,
    diff: bool = False,
    quiet: bool = False,
    preview: bool = False,
    additional_args: Any = None,
    timeout_seconds: float = 300.0,
    env: Mapping[str, Any] | None = None,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    normalized_paths = _normalize_paths(paths)
    resolution = _resolve_external_command(
        TOOL_BLACK,
        executable=executable,
        python_executable=python_executable,
        prefer_module=prefer_module,
    )
    if not resolution.get("available"):
        return _missing_tool_result(TOOL_BLACK, normalized_paths, resolution, cwd=_coerce_path(cwd) if cwd else None)

    command_args = _build_black_args(
        paths=normalized_paths,
        config_file=config_file,
        check=check,
        diff=diff,
        quiet=quiet,
        preview=preview,
        additional_args=additional_args,
    )
    process_result = _run_subprocess(
        list(resolution["command"]) + command_args,
        cwd=_coerce_path(cwd) if cwd else None,
        env=env,
        timeout_seconds=timeout_seconds,
    )
    result = _make_base_result(TOOL_BLACK, normalized_paths, resolution, process_result)

    if not process_result.get("spawn_ok"):
        return _finalize_result(
            result,
            status=STATUS_TIMEOUT if process_result.get("timed_out") else STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary={},
            raw=None,
            error=process_result.get("error"),
        )

    combined = _combined_output(result["stdout"], result["stderr"])
    findings, summary, errors = _parse_black_text(combined)
    if errors:
        summary = dict(summary)
        summary["errors"] = errors

    if result["returncode"] == 0 and not findings and not errors:
        return _finalize_result(
            result,
            status=STATUS_PASSED,
            execution_ok=True,
            findings=[],
            summary=summary,
            raw=None,
        )

    if result["returncode"] == 1 or findings:
        return _finalize_result(
            result,
            status=STATUS_FINDINGS,
            execution_ok=True,
            findings=findings,
            summary=summary,
            raw=None,
        )

    return _finalize_result(
        result,
        status=STATUS_ERROR,
        execution_ok=False,
        findings=findings,
        summary=summary,
        raw=None,
        error=_make_error_payload(
            "black_error",
            "Black did not complete successfully.",
            {"output": combined},
        ),
    )


def _run_pylint_attempt(
    *,
    paths: Sequence[str],
    cwd: Any = None,
    rcfile: Any = None,
    output_format: str = "json2",
    recursive: bool | None = True,
    jobs: int | None = None,
    score: bool = True,
    exit_zero: bool = False,
    additional_args: Any = None,
    timeout_seconds: float = 300.0,
    env: Mapping[str, Any] | None = None,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], Any]:
    resolution = _resolve_external_command(
        TOOL_PYLINT,
        executable=executable,
        python_executable=python_executable,
        prefer_module=prefer_module,
    )
    if not resolution.get("available"):
        return resolution, _missing_tool_result(TOOL_PYLINT, paths, resolution, cwd=_coerce_path(cwd) if cwd else None), None

    command_args = _build_pylint_args(
        paths=paths,
        rcfile=rcfile,
        output_format=output_format,
        recursive=recursive,
        jobs=jobs,
        score=score,
        exit_zero=exit_zero,
        additional_args=additional_args,
    )
    process_result = _run_subprocess(
        list(resolution["command"]) + command_args,
        cwd=_coerce_path(cwd) if cwd else None,
        env=env,
        timeout_seconds=timeout_seconds,
    )
    result = _make_base_result(TOOL_PYLINT, paths, resolution, process_result)
    raw = _safe_json_loads(result["stdout"])
    return resolution, result, raw


def run_pylint(
    paths: Any = None,
    *,
    cwd: Any = None,
    rcfile: Any = None,
    output_format: str = "json2",
    recursive: bool | None = True,
    jobs: int | None = None,
    score: bool = True,
    exit_zero: bool = False,
    additional_args: Any = None,
    timeout_seconds: float = 300.0,
    env: Mapping[str, Any] | None = None,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    normalized_paths = _normalize_paths(paths)
    compatibility_warnings: list[str] = []

    attempts = [
        {"output_format": output_format, "recursive": recursive},
    ]

    if output_format == "json2":
        attempts.append({"output_format": "json", "recursive": recursive})

    if recursive is not None:
        attempts.append({"output_format": output_format, "recursive": None})

    if output_format == "json2" and recursive is not None:
        attempts.append({"output_format": "json", "recursive": None})

    seen = set()
    deduplicated_attempts = []
    for attempt in attempts:
        key = (attempt["output_format"], attempt["recursive"])
        if key in seen:
            continue
        seen.add(key)
        deduplicated_attempts.append(attempt)

    chosen_result = None
    chosen_raw = None
    chosen_attempt = None

    for attempt in deduplicated_attempts:
        _, result, raw = _run_pylint_attempt(
            paths=normalized_paths,
            cwd=cwd,
            rcfile=rcfile,
            output_format=attempt["output_format"],
            recursive=attempt["recursive"],
            jobs=jobs,
            score=score,
            exit_zero=exit_zero,
            additional_args=additional_args,
            timeout_seconds=timeout_seconds,
            env=env,
            executable=executable,
            python_executable=python_executable,
            prefer_module=prefer_module,
        )

        if result["status"] == STATUS_MISSING_TOOL:
            return result
        if not result["timed_out"] and result["returncode"] is None and result["error"]:
            return _finalize_result(
                result,
                status=STATUS_ERROR,
                execution_ok=False,
                findings=[],
                summary={},
                raw=None,
                error=result["error"],
            )
        if result["timed_out"]:
            return _finalize_result(
                result,
                status=STATUS_TIMEOUT,
                execution_ok=False,
                findings=[],
                summary={},
                raw=None,
                error=result["error"],
            )

        chosen_result = result
        chosen_raw = raw
        chosen_attempt = attempt

        usage_error = _is_usage_error_returncode(TOOL_PYLINT, result["returncode"])
        if raw is not None:
            break

        combined = _combined_output(result["stdout"], result["stderr"])
        looks_like_format_problem = _contains_any(
            combined,
            ("json2", "invalid choice", "unknown", "unrecognized", "unsupported"),
        )
        looks_like_recursive_problem = _contains_any(
            combined,
            ("recursive", "unrecognized", "unknown", "unsupported"),
        )

        if not usage_error:
            break

        if attempt["output_format"] == "json2" and looks_like_format_problem:
            compatibility_warnings.append("Pylint does not appear to support 'json2'; fallback to 'json' was used.")
            continue

        if attempt["recursive"] is not None and looks_like_recursive_problem:
            compatibility_warnings.append("Pylint does not appear to support '--recursive' in this form; fallback without '--recursive' was used.")
            continue

    if chosen_result is None:
        resolution = _resolve_external_command(
            TOOL_PYLINT,
            executable=executable,
            python_executable=python_executable,
            prefer_module=prefer_module,
        )
        return _missing_tool_result(TOOL_PYLINT, normalized_paths, resolution, cwd=_coerce_path(cwd) if cwd else None)

    findings, summary = _parse_pylint_json(chosen_raw)
    summary = dict(summary)
    summary["requested_output_format"] = output_format
    summary["requested_recursive"] = recursive
    summary["effective_output_format"] = chosen_attempt["output_format"] if chosen_attempt else output_format
    summary["effective_recursive"] = chosen_attempt["recursive"] if chosen_attempt else recursive

    if chosen_raw is None and _is_usage_error_returncode(TOOL_PYLINT, chosen_result["returncode"]):
        return _finalize_result(
            chosen_result,
            status=STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary=summary,
            raw=None,
            error=_make_error_payload(
                "pylint_usage_error",
                "Pylint could not run with the given arguments.",
                {"output": _combined_output(chosen_result["stdout"], chosen_result["stderr"])},
            ),
            compatibility_warnings=compatibility_warnings,
        )

    if chosen_raw is None:
        return _finalize_result(
            chosen_result,
            status=STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary=summary,
            raw=None,
            error=_make_error_payload(
                "pylint_parse_error",
                "Pylint output could not be parsed as JSON.",
                {"output": _combined_output(chosen_result["stdout"], chosen_result["stderr"])},
            ),
            compatibility_warnings=compatibility_warnings,
        )

    execution_ok = not _is_usage_error_returncode(TOOL_PYLINT, chosen_result["returncode"])
    if execution_ok and not findings and chosen_result["returncode"] == 0:
        return _finalize_result(
            chosen_result,
            status=STATUS_PASSED,
            execution_ok=True,
            findings=[],
            summary=summary,
            raw=chosen_raw,
            compatibility_warnings=compatibility_warnings,
        )

    if execution_ok and findings:
        return _finalize_result(
            chosen_result,
            status=STATUS_FINDINGS,
            execution_ok=True,
            findings=findings,
            summary=summary,
            raw=chosen_raw,
            compatibility_warnings=compatibility_warnings,
        )

    return _finalize_result(
        chosen_result,
        status=STATUS_ERROR,
        execution_ok=False,
        findings=findings,
        summary=summary,
        raw=chosen_raw,
        error=_make_error_payload(
            "pylint_error",
            "Pylint did not complete successfully.",
            {"output": _combined_output(chosen_result["stdout"], chosen_result["stderr"])},
        ),
        compatibility_warnings=compatibility_warnings,
    )


def run_bandit(
    paths: Any = None,
    *,
    cwd: Any = None,
    config_file: Any = None,
    recursive: bool = True,
    severity_level: str | None = None,
    confidence_level: str | None = None,
    baseline: Any = None,
    format_name: str = "json",
    exit_zero: bool = False,
    additional_args: Any = None,
    timeout_seconds: float = 300.0,
    env: Mapping[str, Any] | None = None,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    normalized_paths = _normalize_paths(paths)
    resolution = _resolve_external_command(
        TOOL_BANDIT,
        executable=executable,
        python_executable=python_executable,
        prefer_module=prefer_module,
    )
    if not resolution.get("available"):
        return _missing_tool_result(TOOL_BANDIT, normalized_paths, resolution, cwd=_coerce_path(cwd) if cwd else None)

    command_args = _build_bandit_args(
        paths=normalized_paths,
        config_file=config_file,
        recursive=recursive,
        severity_level=severity_level,
        confidence_level=confidence_level,
        baseline=baseline,
        format_name=format_name,
        exit_zero=exit_zero,
        additional_args=additional_args,
    )
    process_result = _run_subprocess(
        list(resolution["command"]) + command_args,
        cwd=_coerce_path(cwd) if cwd else None,
        env=env,
        timeout_seconds=timeout_seconds,
    )
    result = _make_base_result(TOOL_BANDIT, normalized_paths, resolution, process_result)

    if not process_result.get("spawn_ok"):
        return _finalize_result(
            result,
            status=STATUS_TIMEOUT if process_result.get("timed_out") else STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary={},
            raw=None,
            error=process_result.get("error"),
        )

    raw = _safe_json_loads(result["stdout"])
    findings, summary, errors = _parse_bandit_json(raw)
    if errors:
        summary = dict(summary)
        summary["errors"] = errors

    if raw is None:
        return _finalize_result(
            result,
            status=STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary=summary,
            raw=None,
            error=_make_error_payload(
                "bandit_parse_error",
                "Bandit output could not be parsed as JSON.",
                {"output": _combined_output(result["stdout"], result["stderr"])},
            ),
        )

    if errors:
        return _finalize_result(
            result,
            status=STATUS_ERROR,
            execution_ok=False,
            findings=findings,
            summary=summary,
            raw=raw,
            error=_make_error_payload(
                "bandit_partial_error",
                "Bandit reported errors inside the analysis result.",
                {"errors": errors},
            ),
        )

    if not findings and result["returncode"] == 0:
        return _finalize_result(
            result,
            status=STATUS_PASSED,
            execution_ok=True,
            findings=[],
            summary=summary,
            raw=raw,
        )

    if findings:
        return _finalize_result(
            result,
            status=STATUS_FINDINGS,
            execution_ok=True,
            findings=findings,
            summary=summary,
            raw=raw,
        )

    return _finalize_result(
        result,
        status=STATUS_ERROR,
        execution_ok=False,
        findings=findings,
        summary=summary,
        raw=raw,
        error=_make_error_payload(
            "bandit_error",
            "Bandit did not complete successfully.",
            {"output": _combined_output(result["stdout"], result["stderr"])},
        ),
    )


def _run_vulture_attempt(
    *,
    paths: Sequence[str],
    cwd: Any = None,
    config_file: Any = None,
    min_confidence: int | None = None,
    sort_by_size: bool = False,
    make_whitelist: bool = False,
    additional_args: Any = None,
    timeout_seconds: float = 300.0,
    env: Mapping[str, Any] | None = None,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolution = _resolve_external_command(
        TOOL_VULTURE,
        executable=executable,
        python_executable=python_executable,
        prefer_module=prefer_module,
    )
    if not resolution.get("available"):
        return resolution, _missing_tool_result(TOOL_VULTURE, paths, resolution, cwd=_coerce_path(cwd) if cwd else None)

    command_args = _build_vulture_args(
        paths=paths,
        config_file=config_file,
        min_confidence=min_confidence,
        sort_by_size=sort_by_size,
        make_whitelist=make_whitelist,
        additional_args=additional_args,
    )
    process_result = _run_subprocess(
        list(resolution["command"]) + command_args,
        cwd=_coerce_path(cwd) if cwd else None,
        env=env,
        timeout_seconds=timeout_seconds,
    )
    result = _make_base_result(TOOL_VULTURE, paths, resolution, process_result)
    return resolution, result


def run_vulture(
    paths: Any = None,
    *,
    cwd: Any = None,
    config_file: Any = None,
    min_confidence: int | None = None,
    sort_by_size: bool = False,
    make_whitelist: bool = False,
    additional_args: Any = None,
    timeout_seconds: float = 300.0,
    env: Mapping[str, Any] | None = None,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    normalized_paths = _normalize_paths(paths)
    compatibility_warnings: list[str] = []

    attempts = [{"config_file": config_file}]
    if config_file:
        attempts.append({"config_file": None})

    chosen_result = None
    chosen_config_file = config_file

    for attempt in attempts:
        _, result = _run_vulture_attempt(
            paths=normalized_paths,
            cwd=cwd,
            config_file=attempt["config_file"],
            min_confidence=min_confidence,
            sort_by_size=sort_by_size,
            make_whitelist=make_whitelist,
            additional_args=additional_args,
            timeout_seconds=timeout_seconds,
            env=env,
            executable=executable,
            python_executable=python_executable,
            prefer_module=prefer_module,
        )
        if result["status"] == STATUS_MISSING_TOOL:
            return result
        if not result["timed_out"] and result["returncode"] is None and result["error"]:
            return _finalize_result(
                result,
                status=STATUS_ERROR,
                execution_ok=False,
                findings=[],
                summary={},
                raw=None,
                error=result["error"],
            )
        if result["timed_out"]:
            return _finalize_result(
                result,
                status=STATUS_TIMEOUT,
                execution_ok=False,
                findings=[],
                summary={},
                raw=None,
                error=result["error"],
            )

        chosen_result = result
        chosen_config_file = attempt["config_file"]

        if config_file and attempt["config_file"] is not None and result["returncode"] == 2:
            combined = _combined_output(result["stdout"], result["stderr"])
            if _contains_any(combined, ("--config", "unrecognized", "unknown", "no such option")):
                compatibility_warnings.append("Vulture does not appear to support '--config'; fallback without '--config' was used.")
                continue

        break

    if chosen_result is None:
        resolution = _resolve_external_command(
            TOOL_VULTURE,
            executable=executable,
            python_executable=python_executable,
            prefer_module=prefer_module,
        )
        return _missing_tool_result(TOOL_VULTURE, normalized_paths, resolution, cwd=_coerce_path(cwd) if cwd else None)

    combined = _combined_output(chosen_result["stdout"], chosen_result["stderr"])
    findings, summary = _parse_vulture_text(chosen_result["stdout"])
    summary = dict(summary)
    summary["requested_config_file"] = _coerce_path(config_file) if config_file else None
    summary["effective_config_file"] = _coerce_path(chosen_config_file) if chosen_config_file else None

    if make_whitelist:
        summary["make_whitelist"] = True

    if chosen_result["returncode"] == 0 and not findings:
        return _finalize_result(
            chosen_result,
            status=STATUS_PASSED,
            execution_ok=True,
            findings=[],
            summary=summary,
            raw=chosen_result["stdout"],
            compatibility_warnings=compatibility_warnings,
        )

    if chosen_result["returncode"] == 3 or findings:
        return _finalize_result(
            chosen_result,
            status=STATUS_FINDINGS,
            execution_ok=True,
            findings=findings,
            summary=summary,
            raw=chosen_result["stdout"],
            compatibility_warnings=compatibility_warnings,
        )

    return _finalize_result(
        chosen_result,
        status=STATUS_ERROR,
        execution_ok=False,
        findings=findings,
        summary=summary,
        raw=chosen_result["stdout"],
        error=_make_error_payload(
            "vulture_error",
            "Vulture did not complete successfully.",
            {"output": combined},
        ),
        compatibility_warnings=compatibility_warnings,
    )


def run_specific_standards(
    paths: Any = None,
    *,
    cwd: Any = None,
    guidelines_file: Any = None,
    **kwargs: Any,
) -> dict[str, Any]:
    normalized_paths = _normalize_paths(paths)
    resolution = _import_internal_tool(TOOL_SPECIFIC_STANDARDS)
    if not resolution.get("available"):
        missing_resolution = {
            "source": "internal",
            "command": [TOOL_SPECS[TOOL_SPECIFIC_STANDARDS]["module"]],
            "error": resolution.get("error"),
        }
        return _missing_tool_result(
            TOOL_SPECIFIC_STANDARDS,
            normalized_paths,
            missing_resolution,
            cwd=_coerce_path(cwd) if cwd else None,
        )

    call_kwargs = dict(kwargs)
    call_kwargs.setdefault("cwd", cwd)
    if guidelines_file is not None:
        call_kwargs.setdefault("guidelines_file", guidelines_file)

    started_at = _utc_now_iso()
    started_perf = time.perf_counter()
    try:
        payload = resolution["runner"](paths=normalized_paths, **call_kwargs)
    except Exception as exc:
        ended_at = _utc_now_iso()
        duration_seconds = round(time.perf_counter() - started_perf, 6)
        result = _make_base_result(
            TOOL_SPECIFIC_STANDARDS,
            normalized_paths,
            {"source": "internal", "command": [resolution["module_name"]]},
            {
                "cwd": _coerce_path(cwd) if cwd else None,
                "command": [resolution["module_name"]],
                "command_text": resolution["module_name"],
                "stdout": "",
                "stderr": "",
                "returncode": None,
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": duration_seconds,
                "timed_out": False,
                "error": _make_error_payload(
                    "internal_tool_error",
                    f"Internal tool '{resolution['module_name']}.{resolution['callable_name']}' raised an exception.",
                    {"exception": f"{type(exc).__name__}: {exc}"},
                ),
            },
        )
        return _finalize_result(
            result,
            status=STATUS_ERROR,
            execution_ok=False,
            findings=[],
            summary={},
            raw=None,
            error=result["error"],
        )

    normalized_payload = _normalize_internal_result(
        TOOL_SPECIFIC_STANDARDS,
        payload,
        paths=normalized_paths,
        cwd=cwd,
    )
    return normalized_payload


def run_tool(tool_name: str, paths: Any = None, **kwargs: Any) -> dict[str, Any]:
    normalized_tool_name = str(tool_name).strip()
    if normalized_tool_name not in SUPPORTED_TOOLS:
        supported = ", ".join(SUPPORTED_TOOLS)
        raise ValueError(f"Unknown tool '{normalized_tool_name}'. Supported tools: {supported}")

    runners = {
        TOOL_RUFF: run_ruff_check,
        TOOL_RUFF_FORMAT: run_ruff_format_check,
        TOOL_BLACK: run_black_check,
        TOOL_PYLINT: run_pylint,
        TOOL_BANDIT: run_bandit,
        TOOL_VULTURE: run_vulture,
        TOOL_SPECIFIC_STANDARDS: run_specific_standards,
    }
    runner = runners[normalized_tool_name]
    return runner(paths=paths, **kwargs)


def summarize_suite_results(
    results: Sequence[Mapping[str, Any]],
    *,
    tools: Sequence[str] | None = None,
    paths: Sequence[str] | None = None,
    cwd: Any = None,
    started_at: str | None = None,
    ended_at: str | None = None,
    duration_seconds: float | None = None,
    versions: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_results = [dict(item) for item in results]
    counts = {
        "tools": len(normalized_results),
        "passed": 0,
        "findings": 0,
        "errors": 0,
        "timeouts": 0,
        "missing_tools": 0,
        "findings_total": 0,
    }
    finding_counts_by_tool: dict[str, int] = {}
    status_by_tool: dict[str, str] = {}

    suite_status = STATUS_PASSED
    execution_ok = True

    for result in normalized_results:
        tool_name = str(result.get("tool"))
        status = result.get("status")
        finding_count = int(result.get("finding_count") or 0)

        counts["findings_total"] += finding_count
        finding_counts_by_tool[tool_name] = finding_count
        status_by_tool[tool_name] = str(status)

        if status == STATUS_PASSED:
            counts["passed"] += 1
        elif status == STATUS_FINDINGS:
            counts["findings"] += 1
            if suite_status == STATUS_PASSED:
                suite_status = STATUS_FINDINGS
        elif status == STATUS_TIMEOUT:
            counts["timeouts"] += 1
            counts["errors"] += 1
            suite_status = STATUS_ERROR
            execution_ok = False
        elif status == STATUS_MISSING_TOOL:
            counts["missing_tools"] += 1
            counts["errors"] += 1
            suite_status = STATUS_ERROR
            execution_ok = False
        else:
            counts["errors"] += 1
            suite_status = STATUS_ERROR
            execution_ok = False

    return {
        "module": MODULE_NAME,
        "version": MODULE_VERSION,
        "status": suite_status,
        "execution_ok": execution_ok,
        "passed": suite_status == STATUS_PASSED,
        "tools": list(tools or []),
        "paths": list(paths or []),
        "cwd": _coerce_path(cwd) if cwd else None,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "counts": counts,
        "status_by_tool": status_by_tool,
        "finding_counts_by_tool": finding_counts_by_tool,
        "results": normalized_results,
        "versions": dict(versions or {}),
    }


def _prepare_suite_tools(
    tools: Any = None,
    *,
    include_specific_standards: bool = False,
) -> list[str]:
    normalized_tools = _normalize_tool_names(tools, default_tools=DEFAULT_SUITE_TOOLS)
    if include_specific_standards and TOOL_SPECIFIC_STANDARDS not in normalized_tools:
        normalized_tools.append(TOOL_SPECIFIC_STANDARDS)
    return normalized_tools


def run_code_standards(
    paths: Any = None,
    *,
    tools: Any = None,
    cwd: Any = None,
    tool_options: Mapping[str, Mapping[str, Any]] | None = None,
    shared_options: Mapping[str, Any] | None = None,
    include_versions: bool = False,
    stop_on_error: bool = False,
    include_specific_standards: bool = False,
    guidelines_file: Any = None,
) -> dict[str, Any]:
    normalized_paths = _normalize_paths(paths)
    normalized_tools = _prepare_suite_tools(
        tools,
        include_specific_standards=include_specific_standards,
    )
    tool_options = {key: dict(value) for key, value in dict(tool_options or {}).items() if isinstance(value, Mapping)}
    shared_options = dict(shared_options or {})

    if TOOL_SPECIFIC_STANDARDS in normalized_tools and guidelines_file is not None:
        tool_options.setdefault(TOOL_SPECIFIC_STANDARDS, {})
        tool_options[TOOL_SPECIFIC_STANDARDS].setdefault("guidelines_file", guidelines_file)

    started_at = _utc_now_iso()
    started_perf = time.perf_counter()

    results = []
    for tool_name in normalized_tools:
        merged_options = dict(shared_options)
        merged_options.setdefault("cwd", cwd)
        merged_options.update(dict(tool_options.get(tool_name, {})))
        merged_options.setdefault("cwd", cwd)
        result = run_tool(tool_name, paths=normalized_paths, **merged_options)
        results.append(result)

        if stop_on_error and result.get("status") in (STATUS_ERROR, STATUS_TIMEOUT, STATUS_MISSING_TOOL):
            break

    ended_at = _utc_now_iso()
    duration_seconds = round(time.perf_counter() - started_perf, 6)

    versions = get_tool_versions(normalized_tools) if include_versions else {}
    return summarize_suite_results(
        results,
        tools=normalized_tools,
        paths=normalized_paths,
        cwd=cwd,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
        versions=versions,
    )


def run_default_code_standards(paths: Any = None, **kwargs: Any) -> dict[str, Any]:
    kwargs = dict(kwargs)
    kwargs.setdefault("tools", list(DEFAULT_SUITE_TOOLS))
    return run_code_standards(paths=paths, **kwargs)


def run_extended_code_standards(paths: Any = None, **kwargs: Any) -> dict[str, Any]:
    kwargs = dict(kwargs)
    kwargs.setdefault("tools", list(EXTENDED_SUITE_TOOLS))
    kwargs.setdefault("include_specific_standards", True)
    return run_code_standards(paths=paths, **kwargs)


def _probe_external_tool_version(
    tool_name: str,
    *,
    executable: Any = None,
    python_executable: Any = None,
    prefer_module: bool = False,
    cwd: Any = None,
    env: Mapping[str, Any] | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    resolution = _resolve_external_command(
        tool_name,
        executable=executable,
        python_executable=python_executable,
        prefer_module=prefer_module,
    )
    if not resolution.get("available"):
        return {
            "tool": tool_name,
            "available": False,
            "command_source": resolution.get("source"),
            "command": resolution.get("command", []),
            "version": None,
            "error": resolution.get("error"),
        }

    spec = TOOL_SPECS[tool_name]
    version_args = list(spec.get("version_args", ["--version"]))
    process_result = _run_subprocess(
        list(resolution["command"]) + version_args,
        cwd=_coerce_path(cwd) if cwd else None,
        env=env,
        timeout_seconds=timeout_seconds,
    )
    combined = _combined_output(process_result.get("stdout", ""), process_result.get("stderr", ""))
    version_text = combined.strip().splitlines()[0] if combined.strip() else None

    return {
        "tool": tool_name,
        "available": bool(process_result.get("spawn_ok")) and process_result.get("returncode") == 0,
        "command_source": resolution.get("source"),
        "command": process_result.get("command", resolution.get("command", [])),
        "version": version_text,
        "returncode": process_result.get("returncode"),
        "error": process_result.get("error"),
    }


def _probe_internal_tool(tool_name: str) -> dict[str, Any]:
    resolution = _import_internal_tool(tool_name)
    if not resolution.get("available"):
        return {
            "tool": tool_name,
            "available": False,
            "command_source": "internal",
            "command": [TOOL_SPECS[tool_name]["module"]],
            "version": None,
            "error": resolution.get("error"),
        }

    metadata = {}
    metadata_callable = resolution.get("metadata_callable")
    if callable(metadata_callable):
        try:
            loaded = metadata_callable()
            if isinstance(loaded, Mapping):
                metadata = dict(loaded)
        except Exception as exc:
            metadata = {
                "error": _make_error_payload(
                    "metadata_error",
                    "Internal tool metadata could not be loaded.",
                    {"exception": f"{type(exc).__name__}: {exc}"},
                ),
            }

    return {
        "tool": tool_name,
        "available": True,
        "command_source": "internal",
        "command": [resolution["module_name"]],
        "version": metadata.get("version"),
        "metadata": metadata,
        "error": metadata.get("error"),
    }


def get_available_tools(
    tools: Any = None,
    *,
    executable_overrides: Mapping[str, Any] | None = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    normalized_tools = _normalize_tool_names(tools, default_tools=SUPPORTED_TOOLS)
    overrides = dict(executable_overrides or {})

    result = {}
    for tool_name in normalized_tools:
        if _tool_is_internal(tool_name):
            probe = _probe_internal_tool(tool_name)
        else:
            probe = _probe_external_tool_version(
                tool_name,
                executable=overrides.get(tool_name),
                python_executable=python_executable,
                prefer_module=prefer_module,
            )
        result[tool_name] = {
            "available": probe.get("available", False),
            "version": probe.get("version"),
            "command_source": probe.get("command_source"),
            "command": probe.get("command"),
            "error": probe.get("error"),
        }
    return result


def get_tool_versions(
    tools: Any = None,
    *,
    executable_overrides: Mapping[str, Any] | None = None,
    python_executable: Any = None,
    prefer_module: bool = False,
) -> dict[str, Any]:
    normalized_tools = _normalize_tool_names(tools, default_tools=SUPPORTED_TOOLS)
    overrides = dict(executable_overrides or {})

    versions = {}
    for tool_name in normalized_tools:
        if _tool_is_internal(tool_name):
            probe = _probe_internal_tool(tool_name)
        else:
            probe = _probe_external_tool_version(
                tool_name,
                executable=overrides.get(tool_name),
                python_executable=python_executable,
                prefer_module=prefer_module,
            )
        versions[tool_name] = probe
    return versions


def make_tool_runner(tool_name: str, **default_kwargs: Any) -> partial:
    if tool_name not in SUPPORTED_TOOLS:
        supported = ", ".join(SUPPORTED_TOOLS)
        raise ValueError(f"Unknown tool '{tool_name}'. Supported tools: {supported}")
    return partial(run_tool, tool_name, **default_kwargs)


def make_suite_runner(**default_kwargs: Any) -> partial:
    return partial(run_code_standards, **default_kwargs)


def normalized_exit_code(
    payload: Mapping[str, Any],
    *,
    fail_on_findings: bool = False,
    fail_on_error: bool = True,
) -> int:
    status = payload.get("status")

    if status == STATUS_PASSED:
        return 0
    if status == STATUS_FINDINGS:
        return 1 if fail_on_findings else 0
    if status in (STATUS_ERROR, STATUS_TIMEOUT, STATUS_MISSING_TOOL):
        return 2 if fail_on_error else 0

    if "counts" in payload:
        counts = payload.get("counts") or {}
        errors = int(counts.get("errors") or 0)
        findings = int(counts.get("findings") or 0)
        if errors > 0:
            return 2 if fail_on_error else 0
        if findings > 0:
            return 1 if fail_on_findings else 0
        return 0

    return 2 if fail_on_error else 0


def to_json(payload: Any, *, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False)
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def write_json_report(payload: Any, destination: Any, *, pretty: bool = True) -> str:
    target = Path(_coerce_path(destination))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(to_json(payload, pretty=pretty), encoding="utf-8")
    return str(target)


def _load_json_payload_from_cli(json_text: str | None, json_file: str | None) -> dict[str, Any]:
    if json_text and json_file:
        raise ValueError("Please provide either JSON text or a JSON file, not both.")

    if json_text:
        payload = json.loads(json_text)
        if not isinstance(payload, Mapping):
            raise ValueError("The loaded JSON must be an object.")
        return dict(payload)

    if json_file:
        content = Path(json_file).read_text(encoding="utf-8")
        payload = json.loads(content)
        if not isinstance(payload, Mapping):
            raise ValueError("The loaded JSON file must contain an object.")
        return dict(payload)

    return {}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Check Python code with Ruff, Black, Pylint, Bandit, Vulture and optional project-specific rules.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    common_parent = argparse.ArgumentParser(add_help=False)
    common_parent.add_argument("--cwd", default=None, help="Working directory for tool execution.")
    common_parent.add_argument("--paths", nargs="*", default=["."], help="Files or directories to inspect.")
    common_parent.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    common_parent.add_argument("--write-report", default=None, help="Write a JSON report to a file.")
    common_parent.add_argument("--shared-options-json", default=None, help="Shared tool options as JSON text.")
    common_parent.add_argument("--shared-options-file", default=None, help="Shared tool options from a JSON file.")
    common_parent.add_argument("--tool-options-json", default=None, help="Tool-specific options as JSON text.")
    common_parent.add_argument("--tool-options-file", default=None, help="Tool-specific options from a JSON file.")
    common_parent.add_argument("--guidelines-file", default=None, help="Guidelines markdown file for the specific standards tool.")
    common_parent.add_argument("--fail-on-findings", action="store_true", help="Return exit code 1 when findings exist.")
    common_parent.add_argument("--allow-errors", action="store_true", help="Return exit code 0 even if errors exist.")

    parser_available = subparsers.add_parser("available", help="Check which tools are available.")
    parser_available.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser_available.add_argument("--tools", nargs="*", default=list(SUPPORTED_TOOLS), help="Tools to probe.")

    parser_versions = subparsers.add_parser("versions", help="Show tool versions.")
    parser_versions.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser_versions.add_argument("--tools", nargs="*", default=list(SUPPORTED_TOOLS), help="Tools to probe.")

    parser_tool = subparsers.add_parser("tool", parents=[common_parent], help="Run a single tool.")
    parser_tool.add_argument("tool", choices=SUPPORTED_TOOLS, help="Tool name.")

    parser_suite = subparsers.add_parser("suite", parents=[common_parent], help="Run a tool suite.")
    parser_suite.add_argument("--tools", nargs="*", default=list(DEFAULT_SUITE_TOOLS), help="Tools to execute.")
    parser_suite.add_argument("--include-versions", action="store_true", help="Include tool versions in the summary.")
    parser_suite.add_argument("--stop-on-error", action="store_true", help="Stop the suite on the first error.")
    parser_suite.add_argument("--include-specific-standards", action="store_true", help="Append the internal specific standards tool.")

    return parser


def _emit_output(payload: Any, *, pretty: bool, write_report: str | None = None) -> None:
    text = to_json(payload, pretty=pretty)
    print(text)
    if write_report:
        write_json_report(payload, write_report, pretty=pretty)


def _inject_guidelines_file(
    tool_name: str | None,
    tool_options: dict[str, Any],
    guidelines_file: Any,
) -> dict[str, Any]:
    updated = dict(tool_options)
    if guidelines_file is None:
        return updated

    if tool_name == TOOL_SPECIFIC_STANDARDS:
        updated.setdefault("guidelines_file", guidelines_file)
    return updated


def _inject_guidelines_file_for_suite(
    tool_names: Sequence[str],
    tool_options_payload: Mapping[str, Any],
    guidelines_file: Any,
) -> dict[str, Any]:
    updated = {key: dict(value) for key, value in dict(tool_options_payload).items() if isinstance(value, Mapping)}
    if guidelines_file is None:
        return updated
    if TOOL_SPECIFIC_STANDARDS in tool_names:
        updated.setdefault(TOOL_SPECIFIC_STANDARDS, {})
        updated[TOOL_SPECIFIC_STANDARDS].setdefault("guidelines_file", guidelines_file)
    return updated


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "available":
        payload = {
            "module": MODULE_NAME,
            "version": MODULE_VERSION,
            "available_tools": get_available_tools(args.tools),
        }
        _emit_output(payload, pretty=args.pretty)
        return 0

    if args.command == "versions":
        payload = {
            "module": MODULE_NAME,
            "version": MODULE_VERSION,
            "versions": get_tool_versions(args.tools),
        }
        _emit_output(payload, pretty=args.pretty)
        return 0

    shared_options = _load_json_payload_from_cli(args.shared_options_json, args.shared_options_file)
    tool_options_payload = _load_json_payload_from_cli(args.tool_options_json, args.tool_options_file)

    if args.command == "tool":
        tool_specific = dict(shared_options)
        nested_tool_options = tool_options_payload.get(args.tool)
        if isinstance(nested_tool_options, Mapping):
            tool_specific.update(dict(nested_tool_options))
        tool_specific = _inject_guidelines_file(args.tool, tool_specific, args.guidelines_file)

        payload = run_tool(
            args.tool,
            paths=args.paths,
            cwd=args.cwd,
            **tool_specific,
        )
        _emit_output(payload, pretty=args.pretty, write_report=args.write_report)
        return normalized_exit_code(
            payload,
            fail_on_findings=args.fail_on_findings,
            fail_on_error=not args.allow_errors,
        )

    if args.command == "suite":
        suite_tools = _prepare_suite_tools(
            args.tools,
            include_specific_standards=args.include_specific_standards,
        )
        adjusted_tool_options = _inject_guidelines_file_for_suite(
            suite_tools,
            tool_options_payload,
            args.guidelines_file,
        )
        payload = run_code_standards(
            paths=args.paths,
            tools=suite_tools,
            cwd=args.cwd,
            tool_options=adjusted_tool_options,
            shared_options=shared_options,
            include_versions=args.include_versions,
            stop_on_error=args.stop_on_error,
            include_specific_standards=args.include_specific_standards,
            guidelines_file=args.guidelines_file,
        )
        _emit_output(payload, pretty=args.pretty, write_report=args.write_report)
        return normalized_exit_code(
            payload,
            fail_on_findings=args.fail_on_findings,
            fail_on_error=not args.allow_errors,
        )

    parser.error("Unknown command.")
    return 2


__all__ = [
    "MODULE_NAME",
    "MODULE_VERSION",
    "TOOL_RUFF",
    "TOOL_RUFF_FORMAT",
    "TOOL_BLACK",
    "TOOL_PYLINT",
    "TOOL_BANDIT",
    "TOOL_VULTURE",
    "TOOL_SPECIFIC_STANDARDS",
    "SUPPORTED_TOOLS",
    "DEFAULT_SUITE_TOOLS",
    "EXTENDED_SUITE_TOOLS",
    "run_tool",
    "run_ruff_check",
    "run_ruff_format_check",
    "run_black_check",
    "run_pylint",
    "run_bandit",
    "run_vulture",
    "run_specific_standards",
    "run_code_standards",
    "run_default_code_standards",
    "run_extended_code_standards",
    "summarize_suite_results",
    "get_available_tools",
    "get_tool_versions",
    "make_tool_runner",
    "make_suite_runner",
    "normalized_exit_code",
    "to_json",
    "write_json_report",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
