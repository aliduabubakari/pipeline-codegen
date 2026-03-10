"""CLI entrypoint: opos-generate."""

from __future__ import annotations

import argparse
import json
import sys

from pipeline_codegen.api import generate_artifacts, map_to_target_ir, verify_artifacts
from pipeline_codegen.io import load_document


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="opos-generate")
    p.add_argument("--input", required=True, help="OPOS document path")
    p.add_argument("--target", required=True, choices=["airflow", "prefect", "dagster", "kestra"])
    p.add_argument("--target-version", required=True, help="Target profile version")
    p.add_argument("--out-dir", required=True, help="Output directory")
    p.add_argument("--mode", choices=["template", "llm-assisted"], default="template")
    p.add_argument("--strict", action="store_true", help="Strict mapping mode")
    p.add_argument("--json-report", action="store_true", help="Emit machine report")
    return p


def main() -> int:
    args = build_parser().parse_args()
    try:
        opos = load_document(args.input)
        target_ir = map_to_target_ir(
            opos,
            target=args.target,
            target_version=args.target_version,
            config={"strict": args.strict, "packaging_strategy": "single_workflow"},
        )
        bundle = generate_artifacts(target_ir, mode=args.mode, out_dir=args.out_dir)
        verify = verify_artifacts(bundle, target=args.target, target_version=args.target_version)

        if args.json_report:
            print(
                json.dumps(
                    {
                        "target_ir": target_ir.data,
                        "mapping_report": {
                            "warnings": target_ir.report.warnings,
                            "decisions": target_ir.report.decisions,
                        },
                        "bundle": bundle.manifest,
                        "verification": {
                            "valid": verify.valid,
                            "errors": verify.errors,
                            "checks": verify.checks,
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(f"Generated: {bundle.out_dir}/{bundle.entrypoint}")
            print(f"Verified: valid={verify.valid} checks={len(verify.checks)} errors={len(verify.errors)}")

        return 0 if verify.valid else 2
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
