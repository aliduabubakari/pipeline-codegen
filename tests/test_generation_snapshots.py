from __future__ import annotations

from pathlib import Path

from pipeline_codegen.api import generate_artifacts, map_to_target_ir, verify_artifacts
from helpers import ROOT, load_opos_fixture

TARGETS = [("airflow", "2.8"), ("prefect", "3.x"), ("dagster", "1.8"), ("kestra", "0.18")]
PYTHON_TARGETS = [("airflow", "2.8"), ("prefect", "3.x"), ("dagster", "1.8")]


def test_generation_snapshots_and_determinism(tmp_path: Path) -> None:
    opos = load_opos_fixture("ok_sequential.opos.yaml")

    for target, version in TARGETS:
        ir = map_to_target_ir(opos, target=target, target_version=version, config={"strict": True})
        out1 = tmp_path / f"{target}_1"
        out2 = tmp_path / f"{target}_2"
        bundle1 = generate_artifacts(ir, mode="template", out_dir=str(out1))
        bundle2 = generate_artifacts(ir, mode="template", out_dir=str(out2))

        entry1 = (out1 / bundle1.entrypoint).read_text(encoding="utf-8")
        entry2 = (out2 / bundle2.entrypoint).read_text(encoding="utf-8")
        assert entry1 == entry2

        snap = ROOT / "tests" / "snapshots" / f"artifact_ok_sequential_{target}.txt"
        assert entry1 == snap.read_text(encoding="utf-8")


def test_llm_assisted_mode_is_syntax_valid_for_python_targets(tmp_path: Path) -> None:
    opos = load_opos_fixture("ok_sequential.opos.yaml")
    for target, version in PYTHON_TARGETS:
        ir = map_to_target_ir(opos, target=target, target_version=version, config={"strict": True})
        out = tmp_path / f"llm_{target}"
        bundle = generate_artifacts(
            ir,
            mode="llm-assisted",
            out_dir=str(out),
            llm_config={"provider": "stub"},
        )
        report = verify_artifacts(bundle, target=target, target_version=version)
        assert report.valid is True
        assert "python_syntax" in report.checks


def test_dependency_wiring_for_prefect_and_dagster(tmp_path: Path) -> None:
    opos = load_opos_fixture("ok_parallel_like_dag.opos.yaml")

    prefect_ir = map_to_target_ir(opos, target="prefect", target_version="3.x", config={"strict": True})
    prefect_bundle = generate_artifacts(prefect_ir, mode="template", out_dir=str(tmp_path / "prefect"))
    prefect_text = (tmp_path / "prefect" / prefect_bundle.entrypoint).read_text(encoding="utf-8")
    assert "start_future = start.submit()" in prefect_text
    assert "branch_a_future = branch_a.submit(wait_for=[start_future])" in prefect_text
    assert "branch_b_future = branch_b.submit(wait_for=[start_future])" in prefect_text
    assert "join_future = join.submit(wait_for=[branch_a_future, branch_b_future])" in prefect_text

    dagster_ir = map_to_target_ir(opos, target="dagster", target_version="1.8", config={"strict": True})
    dagster_bundle = generate_artifacts(dagster_ir, mode="template", out_dir=str(tmp_path / "dagster"))
    dagster_text = (tmp_path / "dagster" / dagster_bundle.entrypoint).read_text(encoding="utf-8")
    assert "def branch_a(upstream_0):" in dagster_text
    assert "def join(upstream_0, upstream_1):" in dagster_text
    assert "start_result = start()" in dagster_text
    assert "branch_a_result = branch_a(start_result)" in dagster_text
    assert "branch_b_result = branch_b(start_result)" in dagster_text
    assert "join_result = join(branch_a_result, branch_b_result)" in dagster_text
