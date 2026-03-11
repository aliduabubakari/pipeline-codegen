"""Knowledge pack building and validation."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse


def _normalize_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            result.append(item.strip())
    return result


def build_pack_payload(
    *,
    target: str,
    requested_version: str,
    resolved_version: str,
    retrieval: dict[str, Any],
) -> dict[str, Any]:
    structured = retrieval.get("structured", {})
    if not isinstance(structured, dict):
        structured = {}
    compatibility_profile = structured.get("compatibility_profile")
    if not isinstance(compatibility_profile, str) or not compatibility_profile.strip():
        compatibility_profile = resolved_version

    pack = {
        "pack_version": "1.0",
        "target": target,
        "version": requested_version,
        "compatibility_profile": {
            "requested_version": requested_version,
            "resolved_profile_version": resolved_version,
            "compatibility_profile": compatibility_profile.strip(),
        },
        "operators": _normalize_str_list(structured.get("operators")),
        "imports": _normalize_str_list(structured.get("imports")),
        "syntax_constraints": _normalize_str_list(structured.get("syntax_constraints")),
        "deprecations": _normalize_str_list(structured.get("deprecations")),
        "migration_notes": _normalize_str_list(structured.get("migration_notes")),
        "sources": retrieval.get("sources", []),
        "confidence": float(retrieval.get("confidence", 0.0)),
        "generated_at": datetime.now(UTC).isoformat(),
    }
    context_lines = [
        f"target={target}",
        f"requested_version={requested_version}",
        f"resolved_profile_version={resolved_version}",
        f"compatibility_profile={pack['compatibility_profile']['compatibility_profile']}",
    ]
    for label in ("operators", "imports", "syntax_constraints", "deprecations", "migration_notes"):
        values = pack[label]
        if values:
            context_lines.append(f"{label}={'; '.join(values)}")
    pack["context_compact"] = "\n".join(context_lines)
    return pack


def validate_pack(
    pack: dict[str, Any],
    *,
    min_trusted_sources: int,
    min_confidence: float,
    trusted_domains: tuple[str, ...],
) -> list[str]:
    errors: list[str] = []
    for required in (
        "pack_version",
        "target",
        "version",
        "compatibility_profile",
        "operators",
        "imports",
        "syntax_constraints",
        "deprecations",
        "migration_notes",
        "sources",
        "confidence",
        "generated_at",
        "context_compact",
    ):
        if required not in pack:
            errors.append(f"missing field: {required}")

    confidence = pack.get("confidence")
    if not isinstance(confidence, (int, float)) or float(confidence) < min_confidence:
        errors.append(f"confidence below threshold: {confidence}")

    sources = pack.get("sources")
    trusted_count = 0
    if not isinstance(sources, list):
        errors.append("sources must be a list")
        sources = []
    for source in sources:
        if not isinstance(source, dict):
            continue
        url = source.get("url")
        if not isinstance(url, str):
            continue
        host = urlparse(url).netloc.lower()
        if any(host.endswith(domain) for domain in trusted_domains):
            trusted_count += 1
    if trusted_count < min_trusted_sources:
        errors.append(f"insufficient trusted sources: {trusted_count} < {min_trusted_sources}")

    return errors
