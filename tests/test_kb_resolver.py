from __future__ import annotations

from pipeline_codegen.kb.resolver import resolve_version


def test_resolve_version_exact_match() -> None:
    result = resolve_version("airflow", "2.8", ["2.7", "2.8"])
    assert result.exact_match is True
    assert result.resolved_version == "2.8"
    assert result.reason == "exact_match"


def test_resolve_version_wildcard_major_fallback() -> None:
    result = resolve_version("prefect", "3.x", ["3.0", "3.1", "2.10"])
    assert result.exact_match is False
    assert result.resolved_version == "3.1"
    assert result.reason == "wildcard_major_fallback"


def test_resolve_version_nearest_numeric_fallback() -> None:
    result = resolve_version("airflow", "2.9", ["2.6", "2.8", "3.0"])
    assert result.exact_match is False
    assert result.resolved_version == "2.8"
    assert result.reason == "nearest_numeric_fallback"


def test_resolve_version_lexical_fallback_for_non_numeric() -> None:
    result = resolve_version("custom", "alpha", ["beta", "gamma"])
    assert result.exact_match is False
    assert result.resolved_version == "gamma"
    assert result.reason == "lexical_fallback"
