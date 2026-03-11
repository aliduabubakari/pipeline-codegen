from __future__ import annotations

import json
from pathlib import Path

import typer

from pipeline_codegen.cli.generate import Mode, Target, run


class _FakeKBClient:
    def __init__(self, base_url: str, token: str, timeout_seconds: int = 10) -> None:
        self.base_url = base_url
        self.token = token
        self.timeout_seconds = timeout_seconds
        self._pack_calls = 0

    def get_pack(self, target: str, version: str) -> dict:
        self._pack_calls += 1
        if version == "9.9":
            from pipeline_codegen.kb import KBNotFoundError

            raise KBNotFoundError("not found")
        return {
            "pack_id": "pack-effective",
            "status": "active",
            "pack": {"context_compact": f"{target}@{version} guidance"},
        }

    def resolve_version(self, target: str, requested_version: str) -> dict:
        return {
            "target": target,
            "requested_version": requested_version,
            "resolved_version": "2.8",
            "exact_match": False,
            "reason": "nearest_numeric_fallback",
            "known_versions": ["2.8"],
        }

    def start_backfill(self, target: str, version: str) -> dict:
        return {"job_id": "job-123", "status": "queued", "target": target, "version": version}


def test_cli_uses_kb_fallback_for_unknown_version(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.setattr("pipeline_codegen.cli.generate.KnowledgeBaseServiceClient", _FakeKBClient)
    sample = Path("samples/opos_codegen_inputs/ok_sequential.opos.yaml")
    out_dir = tmp_path / "out"

    try:
        run(
            input=sample,
            target=Target.airflow,
            target_version="9.9",
            out_dir=out_dir,
            mode=Mode.llm_assisted,
            llm_provider="stub",
            llm_model=None,
            llm_api_key=None,
            llm_base_url=None,
            llm_timeout_seconds=60,
            llm_temperature=0.0,
            llm_max_tokens=256,
            llm_env_file=Path(".env"),
            no_llm_env=True,
            kb_service_url="http://kb.local",
            kb_service_token="token",
            kb_timeout_seconds=3,
            no_kb_remote=False,
            strict=True,
            json_report=True,
        )
    except typer.Exit as exc:
        assert exc.exit_code == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["kb"]["kb_context_source"] == "fallback"
    assert payload["kb"]["kb_backfill_job_id"] == "job-123"
    assert payload["bundle"]["requested_target_version"] == "9.9"
    assert payload["bundle"]["effective_target_version"] == "2.8"
