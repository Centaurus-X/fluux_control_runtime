# Directory Structure Runtime v35.1

Release marker: `v35_1_preproduction_final_runtime`  
Runtime contract marker: `v35_1_preproduction_final_runtime`

```text
projektstand_v35_1_preproduction_final_runtime/
├── product/
│   ├── src/                         Runtime source code
│   ├── config/                      Runtime configuration and local JSON state inputs
│   ├── contracts/                   Canonical product contracts
│   └── tests/                       Unit/integration tests
├── contracts/                       Repository-level contract references
├── documentation/                   Architecture, rollout, hardening and release docs
├── installers/                      Runtime host rollout/service helper scripts
├── service/                         systemd service template
├── reports/                         Acceptance, diagnostics and diff reports
├── tools/                           Utilities and runtime health checker
├── .github/                         Issue and pull-request templates
├── README.md
├── DESCRIPTION.md
├── LICENSE.md
├── COMMERCIAL_LICENSE.md
├── SECURITY.md
└── Diff-System-Report.md
```

Excluded from release archives:

```text
_private/
__pycache__/
.pytest_cache/
*.pyc
local logs
local runtime health snapshots
```
