from __future__ import annotations

from pipeline_codegen.api import map_to_target_ir
from pipeline_codegen.core import profiles, target_ir
from helpers import load_opos_fixture


def test_profile_loader_uses_packaged_resources(monkeypatch) -> None:
    profiles.load_profile.cache_clear()

    def _fail_repo_loader(target: str, version: str) -> str | None:
        raise AssertionError(f"repo fallback should not be used for {target}@{version}")

    monkeypatch.setattr(profiles, "_load_profile_text_from_repo", _fail_repo_loader)
    profile = profiles.load_profile("airflow", "2.8")
    assert profile["target"] == "airflow"
    assert profile["version"] == "2.8"
    assert profile["target_family"] == "imperative"


def test_schema_loader_uses_packaged_resources(monkeypatch) -> None:
    target_ir._validator.cache_clear()

    def _fail_repo_loader() -> str | None:
        raise AssertionError("schema repo fallback should not be used")

    monkeypatch.setattr(target_ir, "_load_schema_text_from_repo", _fail_repo_loader)
    opos = load_opos_fixture("ok_sequential.opos.yaml")
    ir = map_to_target_ir(opos, target="airflow", target_version="2.8", config={"strict": True}).data
    assert ir["ir_version"] == "1.0"
