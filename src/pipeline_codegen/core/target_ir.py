"""Target IR validation."""

from __future__ import annotations

import json
from functools import lru_cache
from importlib import resources
from pathlib import Path

from jsonschema import Draft7Validator

from pipeline_codegen.errors import TargetIRError


def _load_schema_text_from_package() -> str | None:
    try:
        resource = resources.files("pipeline_codegen").joinpath(
            "resources", "schemas", "target_ir_v1.json"
        )
        if resource.is_file():
            return resource.read_text(encoding="utf-8")
    except Exception:  # noqa: BLE001
        return None
    return None


def _load_schema_text_from_repo() -> str | None:
    path = Path(__file__).resolve().parents[3] / "schemas" / "target_ir_v1.json"
    if path.exists():
        return path.read_text(encoding="utf-8")
    return None


@lru_cache(maxsize=1)
def _validator() -> Draft7Validator:
    text = _load_schema_text_from_package()
    if text is None:
        text = _load_schema_text_from_repo()
    if text is None:
        raise TargetIRError("IR002", "target ir schema not found")
    schema = json.loads(text)
    return Draft7Validator(schema)


def validate_target_ir(ir: dict) -> None:
    errs = sorted(_validator().iter_errors(ir), key=lambda e: str(e.path))
    if not errs:
        return
    first = errs[0]
    path = "$" + "".join([f"[{repr(p)}]" for p in first.path])
    raise TargetIRError("IR001", f"invalid target ir: {first.message}", path)
