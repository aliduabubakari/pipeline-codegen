from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from helpers import ROOT

TARGETS = [("airflow", "2.8"), ("prefect", "3.x"), ("dagster", "1.8"), ("kestra", "0.18")]
FIXTURES = [
    "ok_sequential.opos.yaml",
    "ok_parallel_like_dag.opos.yaml",
    "ok_notifier.opos.yaml",
]
TARGET_MODES = {
    "airflow": ["template", "llm-assisted"],
    "prefect": ["template", "llm-assisted"],
    "dagster": ["template", "llm-assisted"],
    "kestra": ["template"],
}
ENTRYPOINTS = {
    "airflow": "pipeline.py",
    "prefect": "flow.py",
    "dagster": "definitions.py",
    "kestra": "flow.yaml",
}

def test_cli_end_to_end_matrix(tmp_path: Path) -> None:
    env = {**os.environ, "PYTHONPATH": "src"}

    for fixture in FIXTURES:
        for target, version in TARGETS:
            for mode in TARGET_MODES[target]:
                out_dir = tmp_path / fixture / target / mode
                cmd = [
                    sys.executable,
                    "-m",
                    "pipeline_codegen.cli.generate",
                    "--input",
                    str(ROOT / "samples" / "opos_codegen_inputs" / fixture),
                    "--target",
                    target,
                    "--target-version",
                    version,
                    "--out-dir",
                    str(out_dir),
                    "--mode",
                    mode,
                    "--strict",
                    "--json-report",
                ]
                p = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, env=env)
                assert p.returncode == 0, p.stderr

                payload = json.loads(p.stdout)
                assert payload["verification"]["valid"] is True
                assert payload["bundle"]["entrypoint"] == ENTRYPOINTS[target]
                assert (out_dir / ENTRYPOINTS[target]).exists()
                assert (out_dir / "artifacts.json").exists()


def test_cli_rejects_llm_assisted_for_declarative_targets(tmp_path: Path) -> None:
    env = {**os.environ, "PYTHONPATH": "src"}
    out_dir = tmp_path / "kestra" / "llm-assisted"
    cmd = [
        sys.executable,
        "-m",
        "pipeline_codegen.cli.generate",
        "--input",
        str(ROOT / "samples" / "opos_codegen_inputs" / "ok_sequential.opos.yaml"),
        "--target",
        "kestra",
        "--target-version",
        "0.18",
        "--out-dir",
        str(out_dir),
        "--mode",
        "llm-assisted",
        "--strict",
        "--json-report",
    ]
    p = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, env=env)
    assert p.returncode == 1
    assert "GEN007" in p.stderr
