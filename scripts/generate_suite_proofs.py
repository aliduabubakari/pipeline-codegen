#!/usr/bin/env python3
"""Generate and verify proof artifacts for the curated OPOS E2E suite."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from pipeline_codegen.api import generate_artifacts, map_to_target_ir, verify_artifacts
from pipeline_codegen.io import load_document

ROOT = Path(__file__).resolve().parents[1]
SAMPLES_DIR = ROOT / "samples" / "opos_e2e_suite"
PROOF_DIR = ROOT / "samples" / "generated_workflows_proof"
TARGETS = [("airflow", "2.8"), ("prefect", "3.x"), ("dagster", "1.8"), ("kestra", "0.18")]
TARGET_MODES = {
    "airflow": ["template", "llm-assisted"],
    "prefect": ["template", "llm-assisted"],
    "dagster": ["template", "llm-assisted"],
    "kestra": ["template"],
}


def main() -> int:
    sample_files = sorted(SAMPLES_DIR.glob("*.opos.yaml"))
    if not sample_files:
        raise SystemExit(f"No suite samples found in {SAMPLES_DIR}")

    if PROOF_DIR.exists():
        shutil.rmtree(PROOF_DIR)
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    (PROOF_DIR / "README.md").write_text(
        "\n".join(
            [
                "# Generated Workflow Proofs",
                "",
                "This directory contains generated artifacts for `samples/opos_e2e_suite`.",
                "Matrix:",
                "- targets: airflow@2.8, prefect@3.x, dagster@1.8, kestra@0.18",
                "- imperative modes: template, llm-assisted (stub provider)",
                "- declarative modes: template",
                "",
                "Verification details are captured in `verification_summary.json`.",
                "",
                "Regenerate with:",
                "```bash",
                "python scripts/generate_suite_proofs.py",
                "```",
                "",
            ]
        ),
        encoding="utf-8",
    )

    results: list[dict[str, object]] = []

    for sample in sample_files:
        sample_name = sample.name.replace(".opos.yaml", "")
        opos = load_document(sample)
        for target, version in TARGETS:
            for mode in TARGET_MODES[target]:
                ir = map_to_target_ir(opos, target=target, target_version=version, config={"strict": True})
                out_dir = PROOF_DIR / sample_name / target / mode
                llm_config = {"provider": "stub"} if mode == "llm-assisted" else None
                bundle = generate_artifacts(ir, out_dir=str(out_dir), mode=mode, llm_config=llm_config)
                report = verify_artifacts(bundle, target=target, target_version=version)
                row = {
                    "sample": sample.name,
                    "target": target,
                    "target_version": version,
                    "mode": mode,
                    "valid": report.valid,
                    "entrypoint": bundle.entrypoint,
                    "checks": report.checks,
                    "errors": report.errors,
                }
                results.append(row)
                if not report.valid:
                    (PROOF_DIR / "verification_summary.json").write_text(
                        json.dumps({"results": results}, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    raise SystemExit(
                        f"Verification failed for {sample.name} -> {target}@{version} mode={mode}: {report.errors}"
                    )

    (PROOF_DIR / "verification_summary.json").write_text(
        json.dumps({"results": results}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Generated proof artifacts in {PROOF_DIR}")
    print(f"Total runs: {len(results)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
