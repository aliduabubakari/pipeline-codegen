"""Artifact verification checks."""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import yaml

from pipeline_codegen.errors import VerificationError
from pipeline_codegen.types import ArtifactBundle, VerificationReport


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_artifacts(bundle: ArtifactBundle, target: str, target_version: str) -> VerificationReport:
    checks: list[str] = []
    errors: list[str] = []
    base = Path(bundle.out_dir)

    manifest_path = base / "artifacts.json"
    if not manifest_path.exists():
        raise VerificationError("VFY001", "missing artifacts.json")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    checks.append("manifest_exists")

    for required in ("target", "target_version", "entrypoint", "files", "checksums"):
        if required not in manifest:
            errors.append(f"VFY002 missing manifest key: {required}")
    if errors:
        return VerificationReport(valid=False, errors=errors, checks=checks)
    checks.append("manifest_keys")

    files = manifest.get("files")
    checksums = manifest.get("checksums")
    if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        errors.append("VFY008 invalid manifest files listing")
    if not isinstance(checksums, dict):
        errors.append("VFY008 invalid manifest checksums mapping")
    if errors:
        return VerificationReport(valid=False, errors=errors, checks=checks)

    for relpath in files:
        path = base / relpath
        if not path.exists():
            errors.append(f"VFY009 manifest file missing: {relpath}")
            continue
        expected = checksums.get(relpath)
        if not isinstance(expected, str):
            errors.append(f"VFY008 missing checksum for file: {relpath}")
            continue
        actual = _sha256_file(path)
        if actual != expected:
            errors.append(f"VFY010 checksum mismatch for file: {relpath}")

    if not errors:
        checks.append("checksum_integrity")

    entrypoint = base / str(manifest["entrypoint"])
    if not entrypoint.exists():
        errors.append("VFY003 entrypoint file missing")
        return VerificationReport(valid=False, errors=errors, checks=checks)

    text = entrypoint.read_text(encoding="utf-8")
    if target in {"airflow", "prefect", "dagster"}:
        try:
            ast.parse(text)
            checks.append("python_syntax")
        except SyntaxError as exc:
            errors.append(f"VFY004 invalid python syntax: {exc}")
    elif target == "kestra":
        try:
            data = yaml.safe_load(text)
            if not isinstance(data, dict) or "tasks" not in data:
                errors.append("VFY005 invalid kestra yaml structure")
            else:
                checks.append("yaml_structure")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"VFY006 invalid yaml: {exc}")

    if manifest.get("target") != target or manifest.get("target_version") != target_version:
        errors.append("VFY007 manifest target metadata mismatch")

    return VerificationReport(valid=len(errors) == 0, errors=errors, checks=checks)
