# Runtime Test Plan v35.1

## Local release checks

```bash
python -m compileall -q .
python -m pytest -q
bash -n installers/*.sh
python - <<'PY'
import yaml
from pathlib import Path
yaml.safe_load(Path('product/config/application/app_config.yaml').read_text(encoding='utf-8'))
yaml.safe_load(Path('product/contracts/contract_registry.yaml').read_text(encoding='utf-8'))
PY
```

## Runtime host checks

```bash
bash installers/run_runtime_smoke_tests.sh --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
bash installers/run_runtime_healthcheck.sh --runtime-root /opt/projektstand_v35_1_preproduction_final_runtime
bash installers/check_runtime_service.sh --service-name product-runtime-v35-1.service
```

## External checks for release candidate

```text
- EMQX anonymous access disabled.
- Broker-side runtime/proxy users configured.
- Broker-side ACLs match worker allowed_publish_topics and allowed_subscribe_topics.
- Production CA/client certificate workflow tested if mTLS is required.
- Gateway/PostgreSQL backup and restore validated on the Gateway host.
- 24h+ soak test with EMQX and runtime service restart.
```
