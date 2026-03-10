from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_opos_fixture(name: str) -> dict:
    path = ROOT / "samples" / "opos_codegen_inputs" / name
    return yaml.safe_load(path.read_text(encoding="utf-8"))
