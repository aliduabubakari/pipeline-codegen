"""Deterministic target-version resolver for profile fallback."""

from __future__ import annotations

import re
from dataclasses import dataclass
from importlib import resources


@dataclass(frozen=True)
class ResolutionResult:
    requested_version: str
    resolved_version: str
    exact_match: bool
    reason: str


def available_profile_versions(target: str) -> list[str]:
    try:
        base = resources.files("pipeline_codegen").joinpath("resources", "profiles", target)
        versions: list[str] = []
        for item in base.iterdir():
            if item.is_file() and item.name.endswith(".json"):
                versions.append(item.name[:-5])
        return sorted(versions)
    except Exception:  # noqa: BLE001
        return []


def _parse_version_parts(version: str) -> tuple[tuple[int, ...], bool]:
    wildcard = "x" in version.lower() or "*" in version
    numbers = tuple(int(part) for part in re.findall(r"\d+", version))
    return numbers, wildcard


def _candidate_rank(requested: tuple[int, ...], candidate: tuple[int, ...]) -> tuple[int, int, tuple[int, ...]]:
    requested_major = requested[0] if requested else -1
    candidate_major = candidate[0] if candidate else -1
    major_mismatch_penalty = 0 if requested_major == candidate_major else 1
    requested_minor = requested[1] if len(requested) > 1 else 0
    candidate_minor = candidate[1] if len(candidate) > 1 else 0
    minor_distance = abs(candidate_minor - requested_minor)
    return (major_mismatch_penalty, minor_distance, tuple(-x for x in candidate))


def resolve_version(target: str, requested_version: str, candidates: list[str] | None = None) -> ResolutionResult:
    versions = sorted(candidates or available_profile_versions(target))
    if not versions:
        return ResolutionResult(
            requested_version=requested_version,
            resolved_version=requested_version,
            exact_match=True,
            reason="no_known_versions",
        )
    if requested_version in versions:
        return ResolutionResult(
            requested_version=requested_version,
            resolved_version=requested_version,
            exact_match=True,
            reason="exact_match",
        )

    requested_parts, requested_wildcard = _parse_version_parts(requested_version)
    parsed_candidates = [(version, _parse_version_parts(version)[0]) for version in versions]

    # If request is wildcard major family (e.g. 3.x), pick highest matching major.
    if requested_wildcard and requested_parts:
        same_major = [v for v, p in parsed_candidates if p and p[0] == requested_parts[0]]
        if same_major:
            resolved = sorted(same_major)[-1]
            return ResolutionResult(
                requested_version=requested_version,
                resolved_version=resolved,
                exact_match=False,
                reason="wildcard_major_fallback",
            )

    numeric_candidates = [(v, p) for v, p in parsed_candidates if p]
    if requested_parts and numeric_candidates:
        resolved = sorted(numeric_candidates, key=lambda item: _candidate_rank(requested_parts, item[1]))[0][0]
        return ResolutionResult(
            requested_version=requested_version,
            resolved_version=resolved,
            exact_match=False,
            reason="nearest_numeric_fallback",
        )

    # Deterministic lexical fallback for non-numeric versions.
    resolved = versions[-1]
    return ResolutionResult(
        requested_version=requested_version,
        resolved_version=resolved,
        exact_match=False,
        reason="lexical_fallback",
    )
