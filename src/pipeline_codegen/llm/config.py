"""LLM runtime config loader with .env support and BYOK precedence."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from pipeline_codegen.errors import GenerationError


def _read_float(value: str | None, field: str) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise GenerationError("GEN006", f"invalid float for {field}: {value}") from exc


def _read_int(value: str | None, field: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise GenerationError("GEN006", f"invalid integer for {field}: {value}") from exc


def build_llm_config(
    *,
    provider: str | None = None,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    timeout_seconds: int | None = None,
    env_file: str | Path = ".env",
    load_env: bool = True,
) -> dict[str, Any]:
    if load_env:
        load_dotenv(dotenv_path=Path(env_file), override=False)

    selected_provider = (provider or os.getenv("LLM_PROVIDER") or "stub").strip().lower()
    provider_prefix = selected_provider.upper().replace("-", "_")

    selected_model = model or os.getenv("LLM_MODEL") or os.getenv(f"{provider_prefix}_MODEL")
    selected_base_url = base_url or os.getenv("LLM_BASE_URL") or os.getenv(f"{provider_prefix}_BASE_URL")
    selected_api_key = api_key or os.getenv("LLM_API_KEY")

    selected_temperature = (
        temperature
        if temperature is not None
        else _read_float(os.getenv("LLM_TEMPERATURE"), field="LLM_TEMPERATURE")
    )
    selected_max_tokens = (
        max_tokens
        if max_tokens is not None
        else _read_int(os.getenv("LLM_MAX_TOKENS"), field="LLM_MAX_TOKENS")
    )
    selected_timeout_seconds = (
        timeout_seconds
        if timeout_seconds is not None
        else _read_int(os.getenv("LLM_TIMEOUT_SECONDS"), field="LLM_TIMEOUT_SECONDS")
    )

    cfg: dict[str, Any] = {"provider": selected_provider}
    if selected_model:
        cfg["model"] = selected_model
    if selected_api_key:
        cfg["api_key"] = selected_api_key
    if selected_base_url:
        cfg["base_url"] = selected_base_url
    if selected_temperature is not None:
        cfg["temperature"] = selected_temperature
    if selected_max_tokens is not None:
        cfg["max_tokens"] = selected_max_tokens
    if selected_timeout_seconds is not None:
        cfg["timeout_seconds"] = selected_timeout_seconds
    return cfg
