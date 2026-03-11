from __future__ import annotations

import tempfile
import time
from pathlib import Path

from fastapi.testclient import TestClient

from pipeline_codegen.kb_service.app import create_app
from pipeline_codegen.kb_service.settings import ServiceSettings


def _settings(tmp_path: Path) -> ServiceSettings:
    return ServiceSettings(
        service_token="test-token",
        object_store_dir=tmp_path / "objects",
        sqlite_path=tmp_path / "metadata.db",
        exa_api_key="fake-key",
        exa_search_type="deep",
        exa_num_results=5,
        min_trusted_sources=1,
        min_confidence=0.35,
        trusted_domains=("docs.airflow.apache.org", "github.com"),
    )


def test_kb_service_requires_bearer_token() -> None:
    with tempfile.TemporaryDirectory() as td:
        app = create_app(_settings(Path(td)))
        client = TestClient(app)
        response = client.get("/v1/kb/airflow/2.8")
        assert response.status_code == 401


def test_kb_service_resolve_and_backfill_success(monkeypatch) -> None:
    retrieval = {
        "structured": {
            "compatibility_profile": "airflow-2.8-compatible",
            "operators": ["PythonOperator", "BashOperator"],
            "imports": ["from airflow import DAG"],
            "syntax_constraints": ["Task IDs must be unique"],
            "deprecations": [],
            "migration_notes": ["Prefer providers package imports"],
        },
        "sources": [
            {"url": "https://docs.airflow.apache.org/docs/apache-airflow/stable/index.html", "title": "Airflow Docs"},
            {"url": "https://github.com/apache/airflow", "title": "Airflow GitHub"},
        ],
        "trusted_source_count": 2,
        "confidence": 0.82,
    }

    def _fake_fetch(self, target, requested_version, resolved_version):  # type: ignore[no-untyped-def]
        return retrieval

    monkeypatch.setattr(
        "pipeline_codegen.kb_service.exa_client.ExaKnowledgeRetriever.fetch_orchestrator_knowledge",
        _fake_fetch,
    )

    with tempfile.TemporaryDirectory() as td:
        app = create_app(_settings(Path(td)))
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}

        resolve_resp = client.post(
            "/v1/version/resolve",
            headers=headers,
            json={"target": "airflow", "requested_version": "2.9"},
        )
        assert resolve_resp.status_code == 200
        assert resolve_resp.json()["resolved_version"] == "2.8"

        backfill = client.post(
            "/v1/kb/backfill",
            headers=headers,
            json={"target": "airflow", "version": "2.9"},
        )
        assert backfill.status_code == 202
        job_id = backfill.json()["job_id"]

        status_payload = None
        for _ in range(20):
            status_resp = client.get(f"/v1/kb/backfill/{job_id}", headers=headers)
            assert status_resp.status_code == 200
            status_payload = status_resp.json()
            if status_payload["status"] in {"succeeded", "failed"}:
                break
            time.sleep(0.05)
        assert status_payload is not None
        assert status_payload["status"] == "succeeded"

        pack_resp = client.get("/v1/kb/airflow/2.9", headers=headers)
        assert pack_resp.status_code == 200
        payload = pack_resp.json()
        assert payload["status"] == "active"
        assert payload["pack"]["compatibility_profile"]["resolved_profile_version"] == "2.8"


def test_kb_service_backfill_failure_stays_inactive(monkeypatch) -> None:
    retrieval = {
        "structured": {
            "compatibility_profile": "unknown",
            "operators": [],
            "imports": [],
            "syntax_constraints": [],
            "deprecations": [],
            "migration_notes": [],
        },
        "sources": [{"url": "https://untrusted.example.com/doc", "title": "Untrusted"}],
        "trusted_source_count": 0,
        "confidence": 0.1,
    }

    def _fake_fetch(self, target, requested_version, resolved_version):  # type: ignore[no-untyped-def]
        return retrieval

    monkeypatch.setattr(
        "pipeline_codegen.kb_service.exa_client.ExaKnowledgeRetriever.fetch_orchestrator_knowledge",
        _fake_fetch,
    )

    with tempfile.TemporaryDirectory() as td:
        app = create_app(_settings(Path(td)))
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        backfill = client.post(
            "/v1/kb/backfill",
            headers=headers,
            json={"target": "airflow", "version": "9.9"},
        )
        assert backfill.status_code == 202
        job_id = backfill.json()["job_id"]

        status_payload = None
        for _ in range(20):
            status_resp = client.get(f"/v1/kb/backfill/{job_id}", headers=headers)
            assert status_resp.status_code == 200
            status_payload = status_resp.json()
            if status_payload["status"] in {"succeeded", "failed"}:
                break
            time.sleep(0.05)
        assert status_payload is not None
        assert status_payload["status"] == "failed"

        missing = client.get("/v1/kb/airflow/9.9", headers=headers)
        assert missing.status_code == 404
