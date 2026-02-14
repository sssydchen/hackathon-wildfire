from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

CACHE_DIR = Path(__file__).resolve().parents[1] / "data_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(name: str) -> Path:
    return CACHE_DIR / f"{name}.json"


def load_cache(name: str, max_age_seconds: int) -> Optional[Any]:
    path = _cache_path(name)
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    ts = payload.get("timestamp")
    if ts is None:
        return None

    age = time.time() - ts
    if age > max_age_seconds:
        return None

    return payload.get("data")


def save_cache(name: str, data: Any) -> None:
    path = _cache_path(name)
    payload = {"timestamp": time.time(), "data": data}
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f)
