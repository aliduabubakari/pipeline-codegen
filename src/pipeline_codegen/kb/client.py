"""HTTP client for hosted orchestrator knowledge service."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import error, request


class KBClientError(RuntimeError):
    pass


class KBNotFoundError(KBClientError):
    pass


@dataclass
class KnowledgeBaseServiceClient:
    base_url: str
    token: str
    timeout_seconds: int = 10

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _call(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
        url = f"{self.base_url.rstrip('/')}{path}"
        req = request.Request(url=url, data=body, headers=self._headers(), method=method)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                text = resp.read().decode("utf-8")
                return json.loads(text)
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            if exc.code == 404:
                raise KBNotFoundError(detail) from exc
            raise KBClientError(f"kb service http error ({exc.code}): {detail}") from exc
        except error.URLError as exc:
            raise KBClientError(f"kb service connection failed: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise KBClientError("kb service returned invalid JSON") from exc

    def get_pack(self, target: str, version: str) -> dict[str, Any]:
        return self._call("GET", f"/v1/kb/{target}/{version}")

    def resolve_version(self, target: str, requested_version: str) -> dict[str, Any]:
        return self._call(
            "POST",
            "/v1/version/resolve",
            {"target": target, "requested_version": requested_version},
        )

    def start_backfill(self, target: str, version: str) -> dict[str, Any]:
        return self._call("POST", "/v1/kb/backfill", {"target": target, "version": version})

    def backfill_status(self, job_id: str) -> dict[str, Any]:
        return self._call("GET", f"/v1/kb/backfill/{job_id}")
