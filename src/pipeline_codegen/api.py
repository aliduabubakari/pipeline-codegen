"""Top-level public APIs."""

from __future__ import annotations

from typing import Any

from pipeline_codegen.core.orchmapper import map_to_target_ir as _map
from pipeline_codegen.generation.renderer import generate_artifacts as _generate
from pipeline_codegen.types import ArtifactBundle, TargetIR, VerificationReport
from pipeline_codegen.verification.verifier import verify_artifacts as _verify


def map_to_target_ir(
    opos_doc: dict[str, Any], target: str, target_version: str, config: dict[str, Any] | None = None
) -> TargetIR:
    return _map(opos_doc, target, target_version, config)


def generate_artifacts(
    target_ir: TargetIR,
    mode: str,
    out_dir: str,
    llm_config: dict[str, Any] | None = None,
) -> ArtifactBundle:
    return _generate(target_ir=target_ir, out_dir=out_dir, mode=mode, llm_config=llm_config)


def verify_artifacts(bundle: ArtifactBundle, target: str, target_version: str) -> VerificationReport:
    return _verify(bundle, target, target_version)
