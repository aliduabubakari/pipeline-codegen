"""Renderer for Target IR artifacts."""

from __future__ import annotations

import hashlib
import json
import keyword
import re
from pathlib import Path
from typing import Any

import yaml

from pipeline_codegen.errors import GenerationError
from pipeline_codegen.llm.client import generate_python_task_body
from pipeline_codegen.types import ArtifactBundle, TargetIR


def _task_body_lines(task: dict[str, Any], mode: str, llm_config: dict[str, Any] | None) -> list[str]:
    message = f"task {task['id']}"
    # Bounded v1 policy: "llm-assisted" still only changes task internals.
    if mode == "llm-assisted":
        code = generate_python_task_body(task, llm_config=llm_config)
        return code.splitlines()
    return [f"print({_py_str(message)})"]


def _py_str(value: str) -> str:
    return json.dumps(value)


def _sanitize_symbol(value: str) -> str:
    symbol = re.sub(r"\W+", "_", value).strip("_")
    if not symbol:
        symbol = "task"
    if symbol[0].isdigit():
        symbol = f"task_{symbol}"
    if keyword.iskeyword(symbol):
        symbol = f"{symbol}_task"
    return symbol


def _task_maps(tasks: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, int]]:
    task_by_id: dict[str, dict[str, Any]] = {}
    symbol_by_id: dict[str, str] = {}
    order_by_id: dict[str, int] = {}
    used_symbols: set[str] = set()

    for index, task in enumerate(tasks):
        task_id = str(task["id"])
        if task_id in task_by_id:
            raise GenerationError("GEN003", f"duplicate task id in target ir: {task_id}")
        task_by_id[task_id] = task
        order_by_id[task_id] = index

        base = _sanitize_symbol(task_id)
        symbol = base
        i = 2
        while symbol in used_symbols:
            symbol = f"{base}_{i}"
            i += 1
        used_symbols.add(symbol)
        symbol_by_id[task_id] = symbol

    return task_by_id, symbol_by_id, order_by_id


def _upstreams_by_task(ir: dict[str, Any], order_by_id: dict[str, int]) -> dict[str, list[str]]:
    upstreams: dict[str, list[str]] = {task_id: [] for task_id in order_by_id}
    for edge in ir["edges"]:
        source = str(edge["from"])
        target = str(edge["to"])
        if source not in upstreams or target not in upstreams:
            raise GenerationError("GEN004", f"edge references unknown task: {source}->{target}")
        upstreams[target].append(source)

    for task_id, parents in upstreams.items():
        unique = sorted(set(parents), key=lambda parent: order_by_id[parent])
        upstreams[task_id] = unique
    return upstreams


def _topological_order(upstreams: dict[str, list[str]], order_by_id: dict[str, int]) -> list[str]:
    indegree = {task_id: len(parents) for task_id, parents in upstreams.items()}
    children: dict[str, list[str]] = {task_id: [] for task_id in upstreams}
    for task_id, parents in upstreams.items():
        for parent in parents:
            children[parent].append(task_id)

    ready = sorted([task_id for task_id, count in indegree.items() if count == 0], key=lambda t: order_by_id[t])
    ordered: list[str] = []

    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for child in sorted(children[current], key=lambda t: order_by_id[t]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
                ready.sort(key=lambda t: order_by_id[t])

    if len(ordered) != len(order_by_id):
        raise GenerationError("GEN005", "cyclic dependencies detected in target ir")
    return ordered


def _graph_view(ir: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, list[str]], list[str]]:
    task_by_id, symbol_by_id, order_by_id = _task_maps(ir["tasks"])
    upstreams = _upstreams_by_task(ir, order_by_id)
    order = _topological_order(upstreams, order_by_id)
    return task_by_id, symbol_by_id, upstreams, order


def _render_airflow(
    ir: dict[str, Any],
    mode: str,
    llm_config: dict[str, Any] | None,
    symbol_by_id: dict[str, str],
) -> str:
    lines = [
        "from airflow import DAG",
        "from datetime import datetime",
        "from airflow.operators.python import PythonOperator",
        "from airflow.operators.bash import BashOperator",
        "",
        f"with DAG(dag_id={_py_str(ir['pipeline_id'])}, start_date=datetime(2024,1,1), schedule=None, catchup=False) as dag:",
    ]

    for t in ir["tasks"]:
        task_id = str(t["id"])
        var = symbol_by_id[task_id]
        if t["operator"] == "BashOperator":
            lines.append(
                f"    {var} = BashOperator(task_id={_py_str(task_id)}, bash_command={_py_str(f'echo {task_id}')})"
            )
        else:
            fn_name = f"_fn_{var}"
            lines.append(f"    def {fn_name}():")
            for body_line in _task_body_lines(t, mode, llm_config):
                lines.append(f"        {body_line}")
            lines.append(f"    {var} = PythonOperator(task_id={_py_str(task_id)}, python_callable={fn_name})")

    lines.append("")
    for e in ir["edges"]:
        source = str(e["from"])
        target = str(e["to"])
        if source not in symbol_by_id or target not in symbol_by_id:
            raise GenerationError("GEN004", f"edge references unknown task: {source}->{target}")
        lines.append(f"    {symbol_by_id[source]} >> {symbol_by_id[target]}")
    lines.append("")
    return "\n".join(lines)


def _render_prefect(
    ir: dict[str, Any],
    mode: str,
    llm_config: dict[str, Any] | None,
    symbol_by_id: dict[str, str],
    upstreams: dict[str, list[str]],
    order: list[str],
) -> str:
    lines = [
        "from prefect import flow, task",
        "",
    ]
    for t in ir["tasks"]:
        task_id = str(t["id"])
        symbol = symbol_by_id[task_id]
        lines.append("@task")
        lines.append(f"def {symbol}():")
        for body_line in _task_body_lines(t, mode, llm_config):
            lines.append(f"    {body_line}")
        lines.append("")
    lines.append("@flow")
    lines.append(f"def {_sanitize_symbol(ir['pipeline_id'])}():")
    for task_id in order:
        symbol = symbol_by_id[task_id]
        future = f"{symbol}_future"
        deps = upstreams[task_id]
        if deps:
            wait_for = ", ".join(f"{symbol_by_id[parent]}_future" for parent in deps)
            lines.append(f"    {future} = {symbol}.submit(wait_for=[{wait_for}])")
        else:
            lines.append(f"    {future} = {symbol}.submit()")
    lines.append("")
    lines.append("if __name__ == '__main__':")
    lines.append(f"    {_sanitize_symbol(ir['pipeline_id'])}()")
    return "\n".join(lines)


def _render_dagster(
    ir: dict[str, Any],
    mode: str,
    llm_config: dict[str, Any] | None,
    symbol_by_id: dict[str, str],
    upstreams: dict[str, list[str]],
    order: list[str],
) -> str:
    lines = ["from dagster import op, job", ""]
    for t in ir["tasks"]:
        task_id = str(t["id"])
        symbol = symbol_by_id[task_id]
        params = ", ".join(f"upstream_{idx}" for idx, _ in enumerate(upstreams[task_id]))
        lines.append("@op")
        if params:
            lines.append(f"def {symbol}({params}):")
        else:
            lines.append(f"def {symbol}():")
        for body_line in _task_body_lines(t, mode, llm_config):
            lines.append(f"    {body_line}")
        lines.append(f"    return {_py_str(task_id)}")
        lines.append("")
    lines.append("@job")
    lines.append(f"def {_sanitize_symbol(ir['pipeline_id'])}():")
    for task_id in order:
        symbol = symbol_by_id[task_id]
        deps = upstreams[task_id]
        result = f"{symbol}_result"
        if deps:
            args = ", ".join(f"{symbol_by_id[parent]}_result" for parent in deps)
            lines.append(f"    {result} = {symbol}({args})")
        else:
            lines.append(f"    {result} = {symbol}()")
    return "\n".join(lines)


def _render_kestra(ir: dict[str, Any], upstreams: dict[str, list[str]]) -> str:
    tasks = []
    for t in ir["tasks"]:
        task_id = str(t["id"])
        task = {
            "id": task_id,
            "type": t["operator"],
            "description": t["name"],
        }
        deps = upstreams[task_id]
        if deps:
            task["dependsOn"] = deps
        tasks.append(task)

    doc = {
        "id": ir["pipeline_id"],
        "namespace": "opos.codegen",
        "tasks": tasks,
    }
    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=False)


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def generate_artifacts(
    target_ir: TargetIR, out_dir: str, mode: str = "template", llm_config: dict[str, Any] | None = None
) -> ArtifactBundle:
    ir = target_ir.data
    _, symbol_by_id, upstreams, order = _graph_view(ir)
    target = ir["target"]
    target_version = ir["target_version"]
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)

    if mode not in {"template", "llm-assisted"}:
        raise GenerationError("GEN001", f"unsupported generation mode: {mode}")

    if target == "airflow":
        entrypoint = "pipeline.py"
        content = _render_airflow(ir, mode, llm_config, symbol_by_id=symbol_by_id)
    elif target == "prefect":
        entrypoint = "flow.py"
        content = _render_prefect(
            ir,
            mode,
            llm_config,
            symbol_by_id=symbol_by_id,
            upstreams=upstreams,
            order=order,
        )
    elif target == "dagster":
        entrypoint = "definitions.py"
        content = _render_dagster(
            ir,
            mode,
            llm_config,
            symbol_by_id=symbol_by_id,
            upstreams=upstreams,
            order=order,
        )
    elif target == "kestra":
        entrypoint = "flow.yaml"
        content = _render_kestra(ir, upstreams=upstreams)
    else:
        raise GenerationError("GEN002", f"unsupported generation target in v1: {target}")

    out_file = p / entrypoint
    out_file.write_text(content, encoding="utf-8")

    manifest = {
        "target": target,
        "target_version": target_version,
        "mode": mode,
        "entrypoint": entrypoint,
        "files": [entrypoint],
        "checksums": {entrypoint: _sha256(content)},
        "pipeline_id": ir["pipeline_id"],
    }
    (p / "artifacts.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return ArtifactBundle(
        target=target,
        target_version=target_version,
        out_dir=str(p),
        entrypoint=entrypoint,
        files=[entrypoint, "artifacts.json"],
        manifest=manifest,
    )
