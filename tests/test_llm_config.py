from __future__ import annotations

from pathlib import Path

import pytest

from pipeline_codegen.errors import GenerationError
from pipeline_codegen.llm.config import build_llm_config


def test_build_llm_config_loads_dotenv_defaults(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "LLM_PROVIDER=openrouter",
                "LLM_MODEL=openai/gpt-4o-mini",
                "LLM_BASE_URL=https://openrouter.ai/api/v1/chat/completions",
                "LLM_TEMPERATURE=0.2",
                "LLM_MAX_TOKENS=512",
                "LLM_TIMEOUT_SECONDS=30",
                "OPENROUTER_API_KEY=test_key_from_env",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("LLM_MODEL", raising=False)

    cfg = build_llm_config(env_file=env_file, load_env=True)
    assert cfg["provider"] == "openrouter"
    assert cfg["model"] == "openai/gpt-4o-mini"
    assert cfg["base_url"] == "https://openrouter.ai/api/v1/chat/completions"
    assert cfg["temperature"] == 0.2
    assert cfg["max_tokens"] == 512
    assert cfg["timeout_seconds"] == 30


def test_build_llm_config_cli_overrides_dotenv(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "LLM_PROVIDER=openrouter",
                "LLM_MODEL=openai/gpt-4o-mini",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = build_llm_config(
        provider="openai",
        model="gpt-4o-mini",
        api_key="cli_key",
        env_file=env_file,
        load_env=True,
    )
    assert cfg["provider"] == "openai"
    assert cfg["model"] == "gpt-4o-mini"
    assert cfg["api_key"] == "cli_key"


def test_build_llm_config_rejects_invalid_numeric_env(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("LLM_MAX_TOKENS", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_MAX_TOKENS=abc\n", encoding="utf-8")
    with pytest.raises(GenerationError) as exc:
        build_llm_config(env_file=env_file, load_env=True)
    assert "invalid integer for LLM_MAX_TOKENS" in str(exc.value)
