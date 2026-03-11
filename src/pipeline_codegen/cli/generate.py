"""CLI entrypoint: opos-generate."""

from __future__ import annotations

import json
import os
from enum import Enum
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from pipeline_codegen.api import generate_artifacts, map_to_target_ir, verify_artifacts
from pipeline_codegen.io import load_document
from pipeline_codegen.kb import KBClientError, KBNotFoundError, KnowledgeBaseServiceClient
from pipeline_codegen.llm import build_llm_config

console = Console(stderr=False)
err_console = Console(stderr=True)


class Target(str, Enum):
    airflow = "airflow"
    prefect = "prefect"
    dagster = "dagster"
    kestra = "kestra"


class Mode(str, Enum):
    template = "template"
    llm_assisted = "llm-assisted"


def _persist_manifest(out_dir: Path, manifest: dict[str, Any]) -> None:
    path = out_dir / "artifacts.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(
    input: Path = typer.Option(..., "--input", help="OPOS document path"),
    target: Target = typer.Option(..., "--target", help="Generation target"),
    target_version: str = typer.Option(..., "--target-version", help="Target profile version"),
    out_dir: Path = typer.Option(..., "--out-dir", help="Output directory"),
    mode: Mode = typer.Option(Mode.template, "--mode", help="Generation mode"),
    llm_provider: str | None = typer.Option(None, "--llm-provider", help="LLM provider"),
    llm_model: str | None = typer.Option(None, "--llm-model", help="LLM model"),
    llm_api_key: str | None = typer.Option(None, "--llm-api-key", help="LLM API key override"),
    llm_base_url: str | None = typer.Option(None, "--llm-base-url", help="LLM base URL override"),
    llm_timeout_seconds: int | None = typer.Option(
        60,
        "--llm-timeout-seconds",
        help="LLM request timeout",
    ),
    llm_temperature: float | None = typer.Option(0.0, "--llm-temperature", help="LLM temperature"),
    llm_max_tokens: int | None = typer.Option(256, "--llm-max-tokens", help="LLM max tokens"),
    llm_env_file: Path = typer.Option(Path(".env"), "--llm-env-file", help="LLM .env file path"),
    no_llm_env: bool = typer.Option(False, "--no-llm-env", help="Disable .env loading"),
    kb_service_url: str | None = typer.Option(None, "--kb-service-url", help="KB service base URL"),
    kb_service_token: str | None = typer.Option(None, "--kb-service-token", help="KB service bearer token"),
    kb_timeout_seconds: int | None = typer.Option(None, "--kb-timeout-seconds", help="KB service timeout"),
    no_kb_remote: bool = typer.Option(False, "--no-kb-remote", help="Disable hosted KB lookup"),
    strict: bool = typer.Option(False, "--strict", help="Strict mapping mode"),
    json_report: bool = typer.Option(False, "--json-report", help="Emit machine report"),
) -> None:
    try:
        opos = load_document(input)
        requested_target_version = target_version
        effective_target_version = target_version
        kb_metadata: dict[str, Any] = {
            "kb_context_source": "disabled",
            "kb_context_target_version": requested_target_version,
            "kb_pack_id": None,
            "kb_backfill_job_id": None,
        }
        orchestrator_context: dict[str, Any] | str | None = None

        llm_config = None
        if mode == Mode.llm_assisted:
            llm_config = build_llm_config(
                provider=llm_provider,
                model=llm_model,
                api_key=llm_api_key,
                base_url=llm_base_url,
                temperature=llm_temperature,
                max_tokens=llm_max_tokens,
                timeout_seconds=llm_timeout_seconds,
                env_file=llm_env_file,
                load_env=not no_llm_env,
            )
            resolved_kb_service_url = kb_service_url or os.getenv("KB_SERVICE_URL")
            resolved_kb_service_token = kb_service_token or os.getenv("KB_SERVICE_TOKEN")
            resolved_kb_timeout = kb_timeout_seconds
            if resolved_kb_timeout is None:
                timeout_env = os.getenv("KB_TIMEOUT_SECONDS")
                resolved_kb_timeout = int(timeout_env) if timeout_env else 10

            if resolved_kb_service_url and not no_kb_remote:
                if not resolved_kb_service_token:
                    raise ValueError("--kb-service-token is required when --kb-service-url is set")
                kb_client = KnowledgeBaseServiceClient(
                    base_url=resolved_kb_service_url,
                    token=resolved_kb_service_token,
                    timeout_seconds=resolved_kb_timeout,
                )
                try:
                    pack_payload = kb_client.get_pack(target.value, requested_target_version)
                    kb_metadata["kb_context_source"] = "exact"
                    kb_metadata["kb_pack_id"] = pack_payload.get("pack_id")
                    pack = pack_payload.get("pack")
                    if isinstance(pack, dict):
                        orchestrator_context = pack.get("context_compact", pack)
                except KBNotFoundError:
                    kb_metadata["kb_context_source"] = "fallback"
                    resolution = kb_client.resolve_version(target.value, requested_target_version)
                    effective_target_version = str(resolution.get("resolved_version", requested_target_version))
                    kb_metadata["kb_resolved_target_version"] = effective_target_version
                    try:
                        backfill_payload = kb_client.start_backfill(target.value, requested_target_version)
                        kb_metadata["kb_backfill_job_id"] = backfill_payload.get("job_id")
                    except KBClientError as exc:
                        kb_metadata["kb_backfill_error"] = str(exc)
                    if effective_target_version != requested_target_version:
                        try:
                            fallback_pack = kb_client.get_pack(target.value, effective_target_version)
                            pack = fallback_pack.get("pack")
                            if isinstance(pack, dict):
                                orchestrator_context = pack.get("context_compact", pack)
                            kb_metadata["kb_pack_id"] = fallback_pack.get("pack_id")
                        except KBClientError:
                            pass
                except KBClientError as exc:
                    kb_metadata["kb_context_source"] = "unavailable"
                    kb_metadata["kb_error"] = str(exc)
            elif no_kb_remote:
                kb_metadata["kb_context_source"] = "disabled_by_flag"
            elif not kb_service_url:
                kb_metadata["kb_context_source"] = "disabled_no_service_url"

            llm_config["target"] = target.value
            llm_config["requested_target_version"] = requested_target_version
            llm_config["effective_target_version"] = effective_target_version
            if orchestrator_context is not None:
                llm_config["orchestrator_context"] = orchestrator_context

        target_ir = map_to_target_ir(
            opos,
            target=target.value,
            target_version=effective_target_version,
            config={"strict": strict, "packaging_strategy": "single_workflow"},
        )

        bundle = generate_artifacts(target_ir, mode=mode.value, out_dir=str(out_dir), llm_config=llm_config)
        bundle.manifest.update(
            {
                "requested_target_version": requested_target_version,
                "effective_target_version": effective_target_version,
                **kb_metadata,
            }
        )
        _persist_manifest(out_dir, bundle.manifest)
        verify = verify_artifacts(bundle, target=target.value, target_version=effective_target_version)

        payload = {
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
            "kb": kb_metadata,
        }

        if json_report:
            typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        else:
            table = Table(title="OPOS Codegen Result")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="white")
            table.add_row("Output", f"{bundle.out_dir}/{bundle.entrypoint}")
            table.add_row("Mode", mode.value)
            table.add_row("Target", f"{target.value}@{requested_target_version}")
            table.add_row("Effective Profile", effective_target_version)
            table.add_row("Valid", str(verify.valid))
            table.add_row("Checks", str(len(verify.checks)))
            table.add_row("Errors", str(len(verify.errors)))
            if llm_config:
                table.add_row("LLM Provider", str(llm_config.get("provider", "unknown")))
                table.add_row("LLM Model", str(llm_config.get("model", "default")))
            table.add_row("KB Context Source", str(kb_metadata.get("kb_context_source")))
            if kb_metadata.get("kb_pack_id"):
                table.add_row("KB Pack", str(kb_metadata.get("kb_pack_id")))
            if kb_metadata.get("kb_backfill_job_id"):
                table.add_row("KB Backfill Job", str(kb_metadata.get("kb_backfill_job_id")))
            console.print(table)

        raise typer.Exit(code=0 if verify.valid else 2)
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        err_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1) from exc


def main() -> None:
    typer.run(run)


if __name__ == "__main__":
    main()
