# Development Tools

This directory contains development and inspection utilities that were moved out of `product/` so the runtime source tree stays focused on executable product code.

The tools are retained for local diagnostics, repository hygiene and configuration analysis. They are not imported by the runtime service.

Recommended tools:

```text
tree_finder.py
purge_pycache.sh
gil-free_benchmark.py
json_config_diff_check.py
list_files.py
ndjson_converter.py
rulegen_dynamic.py
_check_code_specific_standards.py
_check_code_standards.py
__find_py_imports.py
__find_py_imports_v2.py
__tree_finder.py
__tree_finder.sh
```

Some legacy helper comments remain intentionally unchanged to avoid changing behavior in a release-cleanup step. Runtime code and active release documentation remain separate from these tools.
