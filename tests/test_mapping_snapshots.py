from __future__ import annotations

import json

from pipeline_codegen.api import map_to_target_ir
from helpers import ROOT, load_opos_fixture

TARGETS = [("airflow", "2.8"), ("prefect", "3.x"), ("dagster", "1.8"), ("kestra", "0.18")]


def test_mapping_snapshots_ok_sequential() -> None:
    opos = load_opos_fixture("ok_sequential.opos.yaml")
    for target, version in TARGETS:
        ir = map_to_target_ir(opos, target=target, target_version=version, config={"strict": True}).data
        snap = ROOT / "tests" / "snapshots" / f"target_ir_ok_sequential_{target}.json"
        expected = json.loads(snap.read_text(encoding="utf-8"))
        assert ir == expected
