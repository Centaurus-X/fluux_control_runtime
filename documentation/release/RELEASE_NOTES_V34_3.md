# Release Notes v34.3

## Summary

v34.3 is a cleanup and GitHub-preparation release of the v34 preproduction runtime package.

It keeps the validated runtime contract marker unchanged:

```text
v34_preproduction_final_runtime
```

The package marker is updated to:

```text
v34_3_preproduction_final_runtime
```

## Changes since v34.2

```text
Kept documentation/directory_structure.md and updated it for the v34.3 repository layout.
Moved sync_xserver target architecture diagram artifacts from product/ to documentation/architecture/.
Moved polling/example configuration files to product/config/other_runtime_examples/.
Moved generated config backups to _private/development_history/config_backups/.
Stored the provided archive.zip in _private/development_history/archive.zip for local-only retention.
Restored selected development tools in the top-level tools/ directory.
Updated installer defaults to v34.3 while remaining compatible with existing v34.2/v34.1 virtual environments.
Updated GitHub release strategy and publication boundary documentation.
Regenerated project manifest and file checksums.
```

## Compatibility

The runtime command binding remains compatible with the previously validated Product Proxy Gateway v01.8.6 setup.

## Status

```text
Pre-Production Ready
Controlled Production Test Ready
Not final unattended production-ready
```

## Note on v34.2 references

The remaining v34.2 mentions are changelog or backward-compatibility references only. They do not identify the active package version.
