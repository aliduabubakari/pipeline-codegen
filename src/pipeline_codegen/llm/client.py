"""Minimal multi-provider LLM client for bounded task-body generation."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any
from urllib import error, request

from pipeline_codegen.errors import GenerationError

OPENAI_COMPATIBLE_PROVIDERS = {"openai", "openrouter", "deepinfra", "deepseek"}
SUPPORTED_PROVIDERS = {"stub", "deepinfra", "openrouter", "ollama", "openai", "claude", "deepseek"}
DEFAULT_BASE_URLS = {
    "openai": "https://api.openai.com/v1/chat/completions",
    "openrouter": "https://openrouter.ai/api/v1/chat/completions",
    "deepinfra": "https://api.deepinfra.com/v1/openai/chat/completions",
    "deepseek": "https://api.deepseek.com/chat/completions",
    "claude": "https://api.anthropic.com/v1/messages",
    "ollama": "http://localhost:11434/api/chat",
}
DEFAULT_MODELS = {
    "openai": "gpt-4o-mini",
    "openrouter": "openai/gpt-4o-mini",
    "deepinfra": "meta-llama/Llama-3.3-70B-Instruct",
    "deepseek": "deepseek-chat",
    "claude": "claude-sonnet-4-20250514",
    "ollama": "llama3.1:8b",
}
API_KEY_ENV = {
    "openai": "OPENAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "deepinfra": "DEEPINFRA_API_TOKEN",
    "deepseek": "DEEPSEEK_API_KEY",
    "claude": "ANTHROPIC_API_KEY",
}


def _strip_code_fences(text: str) -> str:
    clean = text.strip()
    if clean.startswith("```"):
        lines = clean.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        clean = "\n".join(lines).strip()
    return clean


def _http_json_post(url: str, headers: dict[str, str], payload: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url=url, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise GenerationError("GEN006", f"llm request failed ({exc.code}): {detail}") from exc
    except error.URLError as exc:
        raise GenerationError("GEN006", f"llm request failed: {exc.reason}") from exc

    try:
        decoded = json.loads(body)
    except json.JSONDecodeError as exc:
        raise GenerationError("GEN006", f"llm response is not valid JSON: {body[:200]}") from exc
    if not isinstance(decoded, dict):
        raise GenerationError("GEN006", "llm response JSON root must be an object")
    return decoded


def _api_key(provider: str, llm_config: dict[str, Any]) -> str:
    key = llm_config.get("api_key")
    if isinstance(key, str) and key:
        return key
    env_key = API_KEY_ENV.get(provider)
    if env_key:
        env_value = os.getenv(env_key)
        if env_value:
            return env_value
    raise GenerationError("GEN006", f"missing api key for provider={provider}")


def _openai_compatible_chat(provider: str, llm_config: dict[str, Any], system_prompt: str, user_prompt: str) -> str:
    model = str(llm_config.get("model") or DEFAULT_MODELS[provider])
    url = str(llm_config.get("base_url") or DEFAULT_BASE_URLS[provider])
    timeout_seconds = int(llm_config.get("timeout_seconds", 60))
    temperature = float(llm_config.get("temperature", 0))
    max_tokens = int(llm_config.get("max_tokens", 256))
    key = _api_key(provider, llm_config)

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    response = _http_json_post(url, headers=headers, payload=payload, timeout_seconds=timeout_seconds)

    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        raise GenerationError("GEN006", f"invalid llm response format for provider={provider}: missing choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise GenerationError("GEN006", f"invalid llm response format for provider={provider}: missing content")
    return content


def _claude_chat(llm_config: dict[str, Any], system_prompt: str, user_prompt: str) -> str:
    provider = "claude"
    model = str(llm_config.get("model") or DEFAULT_MODELS[provider])
    url = str(llm_config.get("base_url") or DEFAULT_BASE_URLS[provider])
    timeout_seconds = int(llm_config.get("timeout_seconds", 60))
    max_tokens = int(llm_config.get("max_tokens", 256))
    key = _api_key(provider, llm_config)

    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    headers = {
        "x-api-key": key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    response = _http_json_post(url, headers=headers, payload=payload, timeout_seconds=timeout_seconds)

    content = response.get("content")
    if not isinstance(content, list) or not content:
        raise GenerationError("GEN006", "invalid llm response format for provider=claude: missing content")
    block = content[0] if isinstance(content[0], dict) else None
    text = block.get("text") if isinstance(block, dict) else None
    if not isinstance(text, str) or not text.strip():
        raise GenerationError("GEN006", "invalid llm response format for provider=claude: missing text")
    return text


def _ollama_chat(llm_config: dict[str, Any], system_prompt: str, user_prompt: str) -> str:
    provider = "ollama"
    model = str(llm_config.get("model") or DEFAULT_MODELS[provider])
    url = str(llm_config.get("base_url") or DEFAULT_BASE_URLS[provider])
    timeout_seconds = int(llm_config.get("timeout_seconds", 60))
    temperature = float(llm_config.get("temperature", 0))

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
        "options": {"temperature": temperature},
    }
    headers = {"Content-Type": "application/json"}
    response = _http_json_post(url, headers=headers, payload=payload, timeout_seconds=timeout_seconds)

    message = response.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise GenerationError("GEN006", "invalid llm response format for provider=ollama: missing content")
    return content


def complete_chat(system_prompt: str, user_prompt: str, llm_config: dict[str, Any] | None = None) -> str:
    cfg = llm_config or {}
    provider = str(cfg.get("provider", "stub")).lower()
    if provider not in SUPPORTED_PROVIDERS:
        raise GenerationError("GEN006", f"unsupported llm provider: {provider}")

    if provider == "stub":
        return "print('stub llm output')"
    if provider in OPENAI_COMPATIBLE_PROVIDERS:
        return _openai_compatible_chat(provider, cfg, system_prompt, user_prompt)
    if provider == "claude":
        return _claude_chat(cfg, system_prompt, user_prompt)
    if provider == "ollama":
        return _ollama_chat(cfg, system_prompt, user_prompt)
    raise GenerationError("GEN006", f"unsupported llm provider: {provider}")


def generate_python_task_body(task: dict[str, Any], llm_config: dict[str, Any] | None = None) -> str:
    cfg = llm_config or {"provider": "stub"}
    provider = str(cfg.get("provider", "stub")).lower()

    if provider == "stub":
        return f"# generated by llm-assisted ({provider})\nprint('task {task['id']}')"

    system_prompt = (
        "You generate Python task internals for orchestration tasks. "
        "Return Python statements only. Do not include markdown fences or function definitions."
    )
    user_prompt = (
        "Generate safe minimal Python statements for this task. Keep it deterministic and side-effect light.\n"
        f"task_json={json.dumps(task, sort_keys=True)}"
    )
    response = complete_chat(system_prompt=system_prompt, user_prompt=user_prompt, llm_config=cfg)
    code = _strip_code_fences(response)
    if not code:
        raise GenerationError("GEN006", "llm returned empty task body")
    return code


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="opos-llm-client")
    p.add_argument("--provider", required=True, choices=sorted(SUPPORTED_PROVIDERS - {"stub"}))
    p.add_argument("--prompt", required=True)
    p.add_argument("--system", default="You are a concise assistant.")
    p.add_argument("--model")
    p.add_argument("--api-key")
    p.add_argument("--base-url")
    p.add_argument("--temperature", type=float, default=0)
    p.add_argument("--max-tokens", type=int, default=256)
    p.add_argument("--timeout-seconds", type=int, default=60)
    return p


def main() -> int:
    args = _build_parser().parse_args()
    cfg: dict[str, Any] = {
        "provider": args.provider,
        "temperature": args.temperature,
        "max_tokens": args.max_tokens,
        "timeout_seconds": args.timeout_seconds,
    }
    if args.model:
        cfg["model"] = args.model
    if args.api_key:
        cfg["api_key"] = args.api_key
    if args.base_url:
        cfg["base_url"] = args.base_url

    output = complete_chat(system_prompt=args.system, user_prompt=args.prompt, llm_config=cfg)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
