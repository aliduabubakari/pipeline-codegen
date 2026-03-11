"""Settings for hosted KB service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


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
        default_tmp_dir = Path(os.getenv("TMPDIR", "/tmp"))
        return cls(
            service_token=os.getenv("KB_SERVICE_TOKEN", "dev-token"),
            object_store_dir=Path(
                os.getenv("KB_OBJECT_STORE_DIR", str(default_tmp_dir / "pipeline_codegen_kb/object_store"))
            ).resolve(),
            sqlite_path=Path(
                os.getenv("KB_SQLITE_PATH", str(default_tmp_dir / "pipeline_codegen_kb/metadata.db"))
            ).resolve(),
            exa_api_key=os.getenv("EXA_API_KEY", ""),
            exa_search_type=os.getenv("KB_EXA_SEARCH_TYPE", "deep"),
            exa_num_results=int(os.getenv("KB_EXA_NUM_RESULTS", "8")),
            min_trusted_sources=int(os.getenv("KB_MIN_TRUSTED_SOURCES", "1")),
            min_confidence=float(os.getenv("KB_MIN_CONFIDENCE", "0.35")),
            trusted_domains=trusted_domains,
        )
