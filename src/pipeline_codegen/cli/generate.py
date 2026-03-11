"""CLI entrypoint: opos-generate."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from pipeline_codegen.api import generate_artifacts, map_to_target_ir, verify_artifacts
from pipeline_codegen.io import load_document
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
    strict: bool = typer.Option(False, "--strict", help="Strict mapping mode"),
    json_report: bool = typer.Option(False, "--json-report", help="Emit machine report"),
) -> None:
    try:
        opos = load_document(input)
        target_ir = map_to_target_ir(
            opos,
            target=target.value,
            target_version=target_version,
            config={"strict": strict, "packaging_strategy": "single_workflow"},
        )

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

        bundle = generate_artifacts(target_ir, mode=mode.value, out_dir=str(out_dir), llm_config=llm_config)
        verify = verify_artifacts(bundle, target=target.value, target_version=target_version)

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
        }

        if json_report:
            typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        else:
            table = Table(title="OPOS Codegen Result")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="white")
            table.add_row("Output", f"{bundle.out_dir}/{bundle.entrypoint}")
            table.add_row("Mode", mode.value)
            table.add_row("Target", f"{target.value}@{target_version}")
            table.add_row("Valid", str(verify.valid))
            table.add_row("Checks", str(len(verify.checks)))
            table.add_row("Errors", str(len(verify.errors)))
            if llm_config:
                table.add_row("LLM Provider", str(llm_config.get("provider", "unknown")))
                table.add_row("LLM Model", str(llm_config.get("model", "default")))
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
