"""Pydantic schemas for KB service API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ResolveVersionRequest(BaseModel):
    target: str
    requested_version: str


class BackfillRequest(BaseModel):
    target: str
    version: str


class KnowledgePackResponse(BaseModel):
    target: str
    version: str
    pack_id: str
    status: str
    pack: dict[str, Any]


class ResolveVersionResponse(BaseModel):
    target: str
    requested_version: str
    resolved_version: str
    exact_match: bool
    reason: str
    known_versions: list[str] = Field(default_factory=list)


class BackfillAcceptedResponse(BaseModel):
    job_id: str
    status: str
    target: str
    version: str


class BackfillStatusResponse(BaseModel):
    job_id: str
    target: str
    version: str
    status: str
    error: str | None = None
    pack_id: str | None = None
    created_at: str
    updated_at: str
