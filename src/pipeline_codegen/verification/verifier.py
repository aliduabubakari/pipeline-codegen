"""Artifact verification checks."""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import yaml

from pipeline_codegen.core.profiles import load_profile
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
    profile = load_profile(target, target_version)
    target_family = str(profile.get("target_family", "imperative"))

    if target_family == "imperative":
        try:
            ast.parse(text)
            checks.append("python_syntax")
        except SyntaxError as exc:
            errors.append(f"VFY004 invalid python syntax: {exc}")
    elif target_family == "declarative":
        try:
            data = yaml.safe_load(text)
            _verify_declarative_document(data, target, errors, checks)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"VFY006 invalid yaml: {exc}")

    if manifest.get("target") != target or manifest.get("target_version") != target_version:
        errors.append("VFY007 manifest target metadata mismatch")

    return VerificationReport(valid=len(errors) == 0, errors=errors, checks=checks)


def _verify_declarative_document(
    data: object,
    target: str,
    errors: list[str],
    checks: list[str],
) -> None:
    semantic_errors: list[str] = []

    if target != "kestra":
        errors.append(f"VFY011 unsupported declarative verification target: {target}")
        return
    if not isinstance(data, dict):
        errors.append("VFY005 invalid declarative yaml structure")
        return

    required_keys = ("id", "namespace", "labels", "tasks")
    for key in required_keys:
        if key not in data:
            semantic_errors.append(f"VFY005 missing declarative workflow key: {key}")
    tasks = data.get("tasks")
    labels = data.get("labels")
    if semantic_errors:
        errors.extend(semantic_errors)
        return
    if not isinstance(labels, dict):
        semantic_errors.append("VFY005 invalid declarative labels structure")
    if not isinstance(tasks, list) or not tasks:
        semantic_errors.append("VFY005 invalid declarative tasks structure")
    if semantic_errors:
        errors.extend(semantic_errors)
        return

    checks.append("yaml_structure")

    task_ids: set[str] = set()
    for task in tasks:
        if not isinstance(task, dict):
            semantic_errors.append("VFY012 invalid declarative task entry")
            continue
        task_id = task.get("id")
        task_type = task.get("type")
        description = task.get("description")
        if not isinstance(task_id, str) or not task_id:
            semantic_errors.append("VFY012 declarative task missing id")
            continue
        if task_id in task_ids:
            semantic_errors.append(f"VFY012 duplicate declarative task id: {task_id}")
        task_ids.add(task_id)
        if not isinstance(task_type, str) or not task_type:
            semantic_errors.append(f"VFY012 declarative task missing type: {task_id}")
        if not isinstance(description, str) or not description:
            semantic_errors.append(f"VFY012 declarative task missing description: {task_id}")

        depends_on = task.get("dependsOn")
        if depends_on is not None and (
            not isinstance(depends_on, list) or not all(isinstance(item, str) for item in depends_on)
        ):
            semantic_errors.append(f"VFY012 invalid dependsOn for task: {task_id}")

        retry = task.get("retry")
        if retry is not None:
            _verify_retry(task_id, retry, semantic_errors)

        _verify_kestra_task_config(task_id, task_type, task, semantic_errors)

    if semantic_errors:
        errors.extend(semantic_errors)
        return

    for task in tasks:
        depends_on = task.get("dependsOn") or []
        for dep in depends_on:
            if dep not in task_ids:
                semantic_errors.append(f"VFY013 unknown declarative dependency: {task['id']}->{dep}")

    if semantic_errors:
        errors.extend(semantic_errors)
    else:
        checks.append("declarative_semantics")


def _verify_retry(task_id: str, retry: object, errors: list[str]) -> None:
    if not isinstance(retry, dict):
        errors.append(f"VFY012 invalid retry structure for task: {task_id}")
        return
    if not isinstance(retry.get("type"), str) or not retry["type"]:
        errors.append(f"VFY012 retry missing type for task: {task_id}")
    if not isinstance(retry.get("maxAttempts"), int):
        errors.append(f"VFY012 retry missing maxAttempts for task: {task_id}")
    interval = retry.get("interval")
    if interval is not None and (not isinstance(interval, str) or not interval.startswith("PT")):
        errors.append(f"VFY012 invalid retry interval for task: {task_id}")


def _verify_kestra_task_config(task_id: str, task_type: object, task: dict[str, object], errors: list[str]) -> None:
    required_by_type = {
        "io.kestra.plugin.core.http.Request": ("method", "uri"),
        "io.kestra.plugin.scripts.python.Script": ("script",),
        "io.kestra.plugin.scripts.shell.Commands": ("commands",),
        "io.kestra.plugin.jdbc.Query": ("sql",),
        "io.kestra.plugin.notifications.mail.MailSend": ("to", "subject", "text"),
    }

    if not isinstance(task_type, str):
        return
    required_fields = required_by_type.get(task_type)
    if required_fields is None:
        errors.append(f"VFY012 unsupported declarative task type: {task_id}")
        return

    for field in required_fields:
        value = task.get(field)
        if field == "commands":
            if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
                errors.append(f"VFY012 invalid commands for task: {task_id}")
        elif field == "to":
            if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
                errors.append(f"VFY012 invalid recipients for task: {task_id}")
        elif not isinstance(value, str) or not value:
            errors.append(f"VFY012 missing {field} for task: {task_id}")
