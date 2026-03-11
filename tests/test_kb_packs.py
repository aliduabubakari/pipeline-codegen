from __future__ import annotations

from pipeline_codegen.kb_service.packs import build_pack_payload, validate_pack


def test_build_pack_payload_and_validation_success() -> None:
    pack = build_pack_payload(
        target="airflow",
        requested_version="2.9",
        resolved_version="2.8",
        retrieval={
            "structured": {
                "compatibility_profile": "airflow-2.8-compatible",
                "operators": ["PythonOperator"],
                "imports": ["from airflow import DAG"],
                "syntax_constraints": ["Task IDs unique"],
                "deprecations": [],
                "migration_notes": ["Use providers packages"],
            },
            "sources": [{"url": "https://docs.airflow.apache.org/docs/apache-airflow/stable/"}],
            "confidence": 0.9,
        },
    )
    errors = validate_pack(
        pack,
        min_trusted_sources=1,
        min_confidence=0.35,
        trusted_domains=("docs.airflow.apache.org",),
    )
    assert errors == []
    assert pack["compatibility_profile"]["resolved_profile_version"] == "2.8"


def test_validate_pack_rejects_low_confidence() -> None:
    pack = build_pack_payload(
        target="airflow",
        requested_version="2.9",
        resolved_version="2.8",
        retrieval={
            "structured": {
                "compatibility_profile": "airflow-2.8-compatible",
                "operators": [],
                "imports": [],
                "syntax_constraints": [],
                "deprecations": [],
                "migration_notes": [],
            },
            "sources": [{"url": "https://untrusted.example.com/doc"}],
            "confidence": 0.1,
        },
    )
    errors = validate_pack(
        pack,
        min_trusted_sources=1,
        min_confidence=0.35,
        trusted_domains=("docs.airflow.apache.org",),
    )
    assert any("confidence below threshold" in error for error in errors)
