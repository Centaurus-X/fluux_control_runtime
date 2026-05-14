# Archive Policy

The public repository keeps only this policy file in `archive/`.

Historical development material, generated backups and large local archives must stay outside the public GitHub release. In the local ZIP package, such material may be stored under `_private/development_history/`, which is excluded by `.gitignore`.

For GitHub publication, commit the full repository root, but do not commit `_private/` or generated local archives.
