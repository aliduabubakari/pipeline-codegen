from __future__ import annotations

from pathlib import Path

import yaml

from pipeline_codegen.api import generate_artifacts, map_to_target_ir, verify_artifacts
from helpers import load_opos_fixture


def test_verifier_manifest_and_syntax(tmp_path: Path) -> None:
    opos = load_opos_fixture("ok_notifier.opos.yaml")
    ir = map_to_target_ir(opos, target="airflow", target_version="2.8", config={"strict": True})
    bundle = generate_artifacts(ir, mode="template", out_dir=str(tmp_path / "a"))
    report = verify_artifacts(bundle, target="airflow", target_version="2.8")

    assert report.valid is True
    assert "manifest_exists" in report.checks
    assert "checksum_integrity" in report.checks
    assert "python_syntax" in report.checks


def test_verifier_detects_checksum_tampering(tmp_path: Path) -> None:
    opos = load_opos_fixture("ok_notifier.opos.yaml")
    ir = map_to_target_ir(opos, target="airflow", target_version="2.8", config={"strict": True})
    bundle = generate_artifacts(ir, mode="template", out_dir=str(tmp_path / "tamper"))

    entrypoint = Path(bundle.out_dir) / bundle.entrypoint
    entrypoint.write_text("print('tampered')\n", encoding="utf-8")

    report = verify_artifacts(bundle, target="airflow", target_version="2.8")
    assert report.valid is False
    assert any(err.startswith("VFY010") for err in report.errors)


def test_verifier_accepts_semantic_kestra_template(tmp_path: Path) -> None:
    opos = load_opos_fixture("ok_notifier.opos.yaml")
    ir = map_to_target_ir(opos, target="kestra", target_version="0.18", config={"strict": True})
    bundle = generate_artifacts(ir, mode="template", out_dir=str(tmp_path / "kestra"))
    report = verify_artifacts(bundle, target="kestra", target_version="0.18")

    assert report.valid is True
    assert "yaml_structure" in report.checks
    assert "declarative_semantics" in report.checks


def test_verifier_detects_missing_declarative_fields(tmp_path: Path) -> None:
    opos = load_opos_fixture("ok_sequential.opos.yaml")
    ir = map_to_target_ir(opos, target="kestra", target_version="0.18", config={"strict": True})
    bundle = generate_artifacts(ir, mode="template", out_dir=str(tmp_path / "kestra_invalid"))

    entrypoint = Path(bundle.out_dir) / bundle.entrypoint
    doc = yaml.safe_load(entrypoint.read_text(encoding="utf-8"))
    del doc["tasks"][0]["uri"]
    entrypoint.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=False), encoding="utf-8")

    report = verify_artifacts(bundle, target="kestra", target_version="0.18")
    assert report.valid is False
    assert any("missing uri" in err for err in report.errors)
