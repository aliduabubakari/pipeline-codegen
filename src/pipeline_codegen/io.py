"""Shared I/O helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


def load_document(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if p.suffix.lower() in {".yaml", ".yml"}:
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"Document root must be an object: {p}")
    return data


def dump_document(doc: dict[str, Any], path: str | Path) -> None:
    p = Path(path)
    if p.suffix.lower() in {".yaml", ".yml"}:
        content = yaml.safe_dump(doc, sort_keys=False, allow_unicode=False)
    else:
        content = json.dumps(doc, indent=2, sort_keys=True)
    p.write_text(content + "\n", encoding="utf-8")
