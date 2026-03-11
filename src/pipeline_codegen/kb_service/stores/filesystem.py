"""Filesystem object store implementation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class FilesystemObjectStore:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    def put_json(self, key: str, payload: dict[str, Any]) -> None:
        path = self._root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def get_json(self, key: str) -> dict[str, Any]:
        path = self._root / key
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError(f"stored object must be json object: {key}")
        return data
