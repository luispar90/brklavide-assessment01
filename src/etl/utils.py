import hashlib
import json
from typing import Any


def stable_hash(payload: dict[str, Any]) -> str:
    """
    Hash determinístico para generar IDs cuando el API no provee uno.
    """
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
