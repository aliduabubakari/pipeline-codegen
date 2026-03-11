"""Storage interfaces for KB service."""

from __future__ import annotations

from typing import Any, Protocol


class ObjectStore(Protocol):
    def put_json(self, key: str, payload: dict[str, Any]) -> None: ...

    def get_json(self, key: str) -> dict[str, Any]: ...


class MetadataStore(Protocol):
    def create_backfill_job(self, job_id: str, target: str, version: str) -> None: ...

    def update_backfill_job(
        self,
        job_id: str,
        status: str,
        *,
        error: str | None = None,
        pack_id: str | None = None,
    ) -> None: ...

    def get_backfill_job(self, job_id: str) -> dict[str, Any] | None: ...

    def put_pack_record(self, record: dict[str, Any]) -> None: ...

    def activate_pack(self, target: str, version: str, pack_id: str) -> None: ...

    def get_active_pack(self, target: str, version: str) -> dict[str, Any] | None: ...
