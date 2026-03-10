"""Deterministic OrchMapper: OPOS -> Target IR."""

from __future__ import annotations

from typing import Any

from pipeline_codegen.adapters.targets import get_adapter
from pipeline_codegen.core.profiles import load_profile
from pipeline_codegen.core.target_ir import validate_target_ir
from pipeline_codegen.errors import MappingError
from pipeline_codegen.types import MappingReport, TargetIR

SUPPORTED_EXEC_TYPES = {"python_script", "bash", "container", "http_request", "sql", "email"}


def map_to_target_ir(
    opos_doc: dict[str, Any], target: str, target_version: str, config: dict[str, Any] | None = None
) -> TargetIR:
    cfg = config or {}
    strict = bool(cfg.get("strict", False))
    packaging_strategy = str(cfg.get("packaging_strategy", "single_workflow"))

    if packaging_strategy not in {"single_workflow", "split_workflows"}:
        raise MappingError("MAP003", "invalid packaging strategy", "$.config.packaging_strategy")

    adapter = get_adapter(target)
    profile = load_profile(target, target_version)

    components = opos_doc.get("components") or []
    if not components:
        raise MappingError("MAP004", "OPOS must contain components", "$.components")

    warnings: list[str] = []
    tasks: list[dict[str, Any]] = []

    for idx, comp in enumerate(components):
        exec_type = (comp.get("executor") or {}).get("type")
        if exec_type not in SUPPORTED_EXEC_TYPES:
            msg = f"unsupported execution type in v1: {exec_type}"
            if strict:
                raise MappingError("MAP005", msg, f"$.components[{idx}].executor.type")
            warnings.append(msg)
            exec_type = "python_script"

        try:
            operator = adapter.map_operator(exec_type, profile)
        except Exception as exc:  # noqa: BLE001
            if strict:
                raise MappingError("MAP006", str(exc), f"$.components[{idx}]") from exc
            warnings.append(str(exc))
            operator = "UNSUPPORTED"

        tasks.append(
            {
                "id": comp.get("id"),
                "name": comp.get("name", comp.get("id")),
                "category": comp.get("category", "Custom"),
                "execution_type": exec_type,
                "operator": operator,
                "script_language": comp.get("script_language", "python"),
                "integrations_used": sorted(comp.get("integrations_used") or []),
                "retry": comp.get("retry") or {},
            }
        )

    edges = sorted(
        [
            {
                "from": e.get("from"),
                "to": e.get("to"),
                "edge_type": e.get("edge_type", "success"),
            }
            for e in (opos_doc.get("flow") or {}).get("edges", [])
        ],
        key=lambda x: (x.get("from", ""), x.get("to", "")),
    )

    ir: dict[str, Any] = {
        "ir_version": "1.0",
        "target": target,
        "target_version": target_version,
        "pipeline_id": opos_doc.get("pipeline_id"),
        "runtime_style": adapter.runtime_style,
        "packaging": {"strategy": packaging_strategy},
        "tasks": tasks,
        "edges": edges,
        "entry_points": sorted((opos_doc.get("flow") or {}).get("entry_points") or []),
        "diagnostics": {
            "warnings": warnings,
            "decisions": [
                f"profile={target}@{target_version}",
                f"packaging={packaging_strategy}",
                f"task_count={len(tasks)}",
            ],
        },
    }

    validate_target_ir(ir)
    return TargetIR(data=ir, report=MappingReport(warnings=warnings, decisions=ir["diagnostics"]["decisions"]))
