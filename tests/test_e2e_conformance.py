from __future__ import annotations

from pathlib import Path

from pipeline_codegen.api import generate_artifacts, map_to_target_ir, verify_artifacts
from helpers import load_opos_fixture

TARGETS = [("airflow", "2.8"), ("prefect", "3.x"), ("dagster", "1.8"), ("kestra", "0.18")]
FIXTURES = [
    "ok_sequential.opos.yaml",
    "ok_parallel_like_dag.opos.yaml",
    "ok_notifier.opos.yaml",
]
ENTRYPOINTS = {
    "airflow": "pipeline.py",
    "prefect": "flow.py",
    "dagster": "definitions.py",
    "kestra": "flow.yaml",
}
MODES = ["template", "llm-assisted"]


def test_e2e_conformance_counts_and_edges() -> None:
    for fixture in FIXTURES:
        opos = load_opos_fixture(fixture)
        expected_tasks = len(opos["components"])
        expected_edges = len(opos["flow"]["edges"])
        expected_entry = sorted(opos["flow"]["entry_points"])

        for target, version in TARGETS:
            ir = map_to_target_ir(opos, target=target, target_version=version, config={"strict": True}).data
            assert len(ir["tasks"]) == expected_tasks
            assert len(ir["edges"]) == expected_edges
            assert sorted(ir["entry_points"]) == expected_entry
            assert all(t["operator"] for t in ir["tasks"])


def test_e2e_conformance_generation_and_verification(tmp_path: Path) -> None:
    for fixture in FIXTURES:
        opos = load_opos_fixture(fixture)
        for target, version in TARGETS:
            for mode in MODES:
                ir = map_to_target_ir(opos, target=target, target_version=version, config={"strict": True})
                out_dir = tmp_path / fixture / target / mode
                bundle = generate_artifacts(
                    ir,
                    out_dir=str(out_dir),
                    mode=mode,
                    llm_config={"provider": "stub"},
                )
                report = verify_artifacts(bundle, target=target, target_version=version)
                assert report.valid is True
                assert bundle.entrypoint == ENTRYPOINTS[target]
                assert (out_dir / bundle.entrypoint).exists()
                assert (out_dir / "artifacts.json").exists()
