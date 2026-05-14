import json
from pathlib import Path
from typing import Any

import yaml


def load_data_file(path_like: str | Path) -> Any:
    path = Path(path_like)
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(text)
    return yaml.safe_load(text)


def dump_yaml_file(path_like: str | Path, data: Any) -> None:
    path = Path(path_like)
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
