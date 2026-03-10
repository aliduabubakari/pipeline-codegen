"""Core datatypes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class MappingReport:
    warnings: list[str]
    decisions: list[str]


@dataclass
class TargetIR:
    data: dict[str, Any]
    report: MappingReport


@dataclass
class ArtifactBundle:
    target: str
    target_version: str
    out_dir: str
    entrypoint: str
    files: list[str]
    manifest: dict[str, Any]


@dataclass
class VerificationReport:
    valid: bool
    errors: list[str]
    checks: list[str]
