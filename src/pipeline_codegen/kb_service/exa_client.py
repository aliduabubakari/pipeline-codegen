"""Exa retrieval wrapper for orchestrator knowledge packs."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

from pipeline_codegen.errors import GenerationError

OFFICIAL_DOMAINS = {
    "airflow": ["docs.airflow.apache.org", "airflow.apache.org", "github.com"],
    "prefect": ["docs.prefect.io", "prefect.io", "github.com"],
    "dagster": ["docs.dagster.io", "dagster.io", "github.com"],
    "kestra": ["kestra.io", "github.com"],
}


class ExaKnowledgeRetriever:
    def __init__(self, api_key: str, search_type: str = "deep", num_results: int = 8) -> None:
        self._api_key = api_key
        self._search_type = search_type
        self._num_results = num_results

    def _require_client(self) -> Any:
        if not self._api_key:
            raise GenerationError("GEN006", "EXA_API_KEY is required by kb service")
        try:
            from exa_py import Exa  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - exercised by runtime packaging checks
            raise GenerationError("GEN006", "exa-py dependency is required by kb service") from exc
        return Exa(api_key=self._api_key)

    def _structured_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "required": [
                "operators",
                "imports",
                "syntax_constraints",
                "deprecations",
                "migration_notes",
                "compatibility_profile",
            ],
            "properties": {
                "compatibility_profile": {"type": "string"},
                "operators": {"type": "array", "items": {"type": "string"}},
                "imports": {"type": "array", "items": {"type": "string"}},
                "syntax_constraints": {"type": "array", "items": {"type": "string"}},
                "deprecations": {"type": "array", "items": {"type": "string"}},
                "migration_notes": {"type": "array", "items": {"type": "string"}},
            },
        }

    def _extract_sources(self, response: Any) -> list[dict[str, str]]:
        raw_results = getattr(response, "results", None)
        if not isinstance(raw_results, list):
            return []
        sources: list[dict[str, str]] = []
        for item in raw_results:
            url = getattr(item, "url", None)
            title = getattr(item, "title", None)
            if isinstance(url, str):
                sources.append({"url": url, "title": title if isinstance(title, str) else url})
        return sources

    def _extract_output_content(self, response: Any) -> dict[str, Any]:
        output = getattr(response, "output", None)
        content = getattr(output, "content", None) if output is not None else None
        if isinstance(content, dict):
            return content
        if isinstance(content, str):
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _extract_confidence(self, response: Any) -> float:
        output = getattr(response, "output", None)
        grounding = getattr(output, "grounding", None) if output is not None else None
        if not isinstance(grounding, list) or not grounding:
            return 0.4
        weights = {"high": 1.0, "medium": 0.7, "low": 0.4}
        scores: list[float] = []
        for row in grounding:
            confidence = row.get("confidence") if isinstance(row, dict) else None
            if isinstance(confidence, str):
                scores.append(weights.get(confidence.lower(), 0.4))
            elif isinstance(confidence, (int, float)):
                scores.append(float(confidence))
        if not scores:
            return 0.4
        return max(0.0, min(1.0, sum(scores) / len(scores)))

    def fetch_orchestrator_knowledge(
        self, target: str, requested_version: str, resolved_version: str
    ) -> dict[str, Any]:
        exa = self._require_client()
        include_domains = OFFICIAL_DOMAINS.get(target, [])
        query = (
            f"{target} orchestrator version {requested_version} API reference, migration guide, "
            f"operators, imports, and deprecations. Include compatibility with {resolved_version}."
        )
        response = exa.search(
            query=query,
            type=self._search_type,
            num_results=self._num_results,
            include_domains=include_domains,
            output_schema=self._structured_schema(),
            contents={"highlights": {"max_characters": 4000}},
        )

        structured = self._extract_output_content(response)
        sources = self._extract_sources(response)
        confidence = self._extract_confidence(response)
        trusted_sources = 0
        for source in sources:
            host = urlparse(source["url"]).netloc.lower()
            if any(host.endswith(domain) for domain in include_domains):
                trusted_sources += 1
        return {
            "structured": structured,
            "sources": sources,
            "trusted_source_count": trusted_sources,
            "confidence": confidence,
        }
