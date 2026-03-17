from __future__ import annotations

from pathlib import Path

from pipeline_codegen.api import generate_artifacts, map_to_target_ir, verify_artifacts
from pipeline_codegen.io import load_document
from helpers import ROOT

TARGETS = [("airflow", "2.8"), ("prefect", "3.x"), ("dagster", "1.8"), ("kestra", "0.18")]
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


def test_curated_opos_suite_generates_for_all_targets_and_modes(tmp_path: Path) -> None:
    suite_dir = ROOT / "samples" / "opos_e2e_suite"
    sample_files = sorted(suite_dir.glob("*.opos.yaml"))
    assert sample_files, f"No suite files found in {suite_dir}"

    for sample in sample_files:
        opos = load_document(sample)
        sample_name = sample.name.replace(".opos.yaml", "")
        for target, version in TARGETS:
            for mode in TARGET_MODES[target]:
                ir = map_to_target_ir(opos, target=target, target_version=version, config={"strict": True})
                out_dir = tmp_path / sample_name / target / mode
                llm_config = {"provider": "stub"} if mode == "llm-assisted" else None
                bundle = generate_artifacts(ir, out_dir=str(out_dir), mode=mode, llm_config=llm_config)
                report = verify_artifacts(bundle, target=target, target_version=version)
                assert report.valid is True
                assert bundle.entrypoint == ENTRYPOINTS[target]
                assert (out_dir / bundle.entrypoint).exists()
                assert (out_dir / "artifacts.json").exists()
