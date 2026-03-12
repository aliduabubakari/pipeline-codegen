"""Settings for hosted KB service."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path


def _default_data_root() -> Path:
    explicit = os.getenv("PIPELINE_CODEGEN_DATA_DIR")
    if explicit:
        return Path(explicit).expanduser()

    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "pipeline-codegen"

    if os.name == "nt":
        appdata = os.getenv("APPDATA")
        if appdata:
            return Path(appdata) / "pipeline-codegen"
        return Path.home() / "AppData" / "Roaming" / "pipeline-codegen"

    xdg_data_home = os.getenv("XDG_DATA_HOME")
    if xdg_data_home:
        return Path(xdg_data_home) / "pipeline-codegen"
    return Path.home() / ".local" / "share" / "pipeline-codegen"


@dataclass(frozen=True)
class ServiceSettings:
    service_token: str
    object_store_dir: Path
    sqlite_path: Path
    exa_api_key: str
    exa_search_type: str
    exa_num_results: int
    min_trusted_sources: int
    min_confidence: float
    trusted_domains: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "ServiceSettings":
        trusted_raw = os.getenv(
            "KB_SERVICE_TRUSTED_DOMAINS",
            "docs.airflow.apache.org,docs.prefect.io,docs.dagster.io,kestra.io,github.com",
        )
        trusted_domains = tuple(part.strip().lower() for part in trusted_raw.split(",") if part.strip())
        data_root = _default_data_root().resolve()
        default_object_store = data_root / "kb_service" / "object_store"
        default_sqlite_path = data_root / "kb_service" / "metadata.db"
        return cls(
            service_token=os.getenv("KB_SERVICE_TOKEN", "dev-token"),
            object_store_dir=Path(os.getenv("KB_OBJECT_STORE_DIR", str(default_object_store))).expanduser().resolve(),
            sqlite_path=Path(os.getenv("KB_SQLITE_PATH", str(default_sqlite_path))).expanduser().resolve(),
            exa_api_key=os.getenv("EXA_API_KEY", ""),
            exa_search_type=os.getenv("KB_EXA_SEARCH_TYPE", "deep"),
            exa_num_results=int(os.getenv("KB_EXA_NUM_RESULTS", "8")),
            min_trusted_sources=int(os.getenv("KB_MIN_TRUSTED_SOURCES", "1")),
            min_confidence=float(os.getenv("KB_MIN_CONFIDENCE", "0.35")),
            trusted_domains=trusted_domains,
        )
