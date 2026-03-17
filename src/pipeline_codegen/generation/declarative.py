"""Declarative projection models and renderers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml

from pipeline_codegen.errors import GenerationError


@dataclass(frozen=True)
class DeclarativeTaskSpec:
    id: str
    type: str
    description: str
    depends_on: list[str]
    config: dict[str, Any]
    retry: dict[str, Any] | None = None


@dataclass(frozen=True)
class DeclarativeWorkflowSpec:
    target: str
    pipeline_id: str
    namespace: str
    labels: dict[str, str]
    tasks: list[DeclarativeTaskSpec]


def project_workflow(
    ir: dict[str, Any],
    profile: dict[str, Any],
    upstreams: dict[str, list[str]],
) -> DeclarativeWorkflowSpec:
    target = str(profile.get("target"))
    if target != "kestra":
        raise GenerationError("GEN002", f"unsupported declarative generation target in v1: {target}")
    return _project_kestra(ir, upstreams)


def render_workflow(spec: DeclarativeWorkflowSpec) -> str:
    if spec.target != "kestra":
        raise GenerationError("GEN002", f"unsupported declarative rendering target in v1: {spec.target}")

    doc: dict[str, Any] = {
        "id": spec.pipeline_id,
        "namespace": spec.namespace,
        "labels": spec.labels,
        "tasks": [],
    }

    for task in spec.tasks:
        rendered: dict[str, Any] = {
            "id": task.id,
            "type": task.type,
            "description": task.description,
            **task.config,
        }
        if task.depends_on:
            rendered["dependsOn"] = task.depends_on
        if task.retry:
            rendered["retry"] = task.retry
        doc["tasks"].append(rendered)

    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=False)


def _project_kestra(ir: dict[str, Any], upstreams: dict[str, list[str]]) -> DeclarativeWorkflowSpec:
    tasks: list[DeclarativeTaskSpec] = []
    for task in ir["tasks"]:
        task_id = str(task["id"])
        tasks.append(
            DeclarativeTaskSpec(
                id=task_id,
                type=str(task["operator"]),
                description=str(task["name"]),
                depends_on=upstreams[task_id],
                config=_kestra_task_config(ir["pipeline_id"], task),
                retry=_kestra_retry(task.get("retry") or {}),
            )
        )

    return DeclarativeWorkflowSpec(
        target="kestra",
        pipeline_id=str(ir["pipeline_id"]),
        namespace="opos.codegen",
        labels={
            "generatedBy": "pipeline-codegen",
            "targetFamily": "declarative",
        },
        tasks=tasks,
    )


def _kestra_task_config(pipeline_id: str, task: dict[str, Any]) -> dict[str, Any]:
    task_id = str(task["id"])
    task_name = str(task["name"])
    execution_type = str(task["execution_type"])
    integration = _primary_integration(task)

    if execution_type == "http_request":
        return {
            "method": "GET",
            "uri": f"https://example.invalid/{integration}/{pipeline_id}/{task_id}",
        }
    if execution_type == "python_script":
        return {
            "script": "\n".join(
                [
                    f"print('pipeline={pipeline_id}')",
                    f"print('task={task_id}')",
                ]
            )
        }
    if execution_type in {"bash", "container"}:
        return {
            "commands": [
                f"echo pipeline={pipeline_id}",
                f"echo task={task_id}",
            ]
        }
    if execution_type == "sql":
        return {
            "sql": "\n".join(
                [
                    f"-- pipeline: {pipeline_id}",
                    f"-- integration: {integration}",
                    f"SELECT '{task_id}' AS task_id;",
                ]
            )
        }
    if execution_type == "email":
        return {
            "to": ["ops@example.com"],
            "subject": f"[{pipeline_id}] {task_name}",
            "text": f"Template notification emitted for task {task_id}.",
        }
    raise GenerationError("GEN002", f"unsupported declarative execution type in v1: {execution_type}")


def _kestra_retry(retry_cfg: dict[str, Any]) -> dict[str, Any] | None:
    if not retry_cfg:
        return None

    strategy = str(retry_cfg.get("strategy", "constant"))
    delay_seconds = int(retry_cfg.get("delay_seconds", 30))
    max_attempts = int(retry_cfg.get("max_attempts", 1))

    retry: dict[str, Any] = {
        "type": strategy,
        "maxAttempts": max_attempts,
    }
    if strategy in {"constant", "exponential"}:
        retry["interval"] = _duration_seconds(delay_seconds)
    if strategy == "exponential":
        retry["maxInterval"] = _duration_seconds(max(delay_seconds * 4, delay_seconds))
    return retry


def _primary_integration(task: dict[str, Any]) -> str:
    integrations = task.get("integrations_used") or []
    if integrations:
        return str(integrations[0])
    return "service"


def _duration_seconds(seconds: int) -> str:
    if seconds <= 0:
        return "PT0S"
    return f"PT{seconds}S"
