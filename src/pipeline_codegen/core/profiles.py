"""Target capability profile loader."""

from __future__ import annotations

import json
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any

from pipeline_codegen.errors import MappingError


def _load_profile_text_from_package(target: str, version: str) -> str | None:
    try:
        resource = resources.files("pipeline_codegen").joinpath(
            "resources", "profiles", target, f"{version}.json"
        )
        if resource.is_file():
            return resource.read_text(encoding="utf-8")
    except Exception:  # noqa: BLE001
        return None
    return None


def _load_profile_text_from_repo(target: str, version: str) -> str | None:
    base = Path(__file__).resolve().parents[3] / "profiles"
    profile_path = base / target / f"{version}.json"
    if profile_path.exists():
        return profile_path.read_text(encoding="utf-8")
    return None


@lru_cache(maxsize=32)
def load_profile(target: str, version: str) -> dict[str, Any]:
    text = _load_profile_text_from_package(target, version)
    if text is None:
        text = _load_profile_text_from_repo(target, version)
    if text is None:
        raise MappingError("MAP001", f"unsupported target profile: {target}@{version}")
    return json.loads(text)
