"""FastAPI app for orchestrator version knowledge service."""

from __future__ import annotations

import uuid
from functools import partial
from typing import Any

import uvicorn
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Header, status

from pipeline_codegen.kb.resolver import available_profile_versions, resolve_version
from pipeline_codegen.kb_service.auth import require_bearer_token
from pipeline_codegen.kb_service.exa_client import ExaKnowledgeRetriever
from pipeline_codegen.kb_service.packs import build_pack_payload, validate_pack
from pipeline_codegen.kb_service.schemas import (
    BackfillAcceptedResponse,
    BackfillRequest,
    BackfillStatusResponse,
    KnowledgePackResponse,
    ResolveVersionRequest,
    ResolveVersionResponse,
)
from pipeline_codegen.kb_service.settings import ServiceSettings
from pipeline_codegen.kb_service.stores import FilesystemObjectStore, SQLiteMetadataStore


def _process_backfill_job(
    *,
    job_id: str,
    target: str,
    version: str,
    settings: ServiceSettings,
    object_store: FilesystemObjectStore,
    metadata_store: SQLiteMetadataStore,
    retriever: ExaKnowledgeRetriever,
) -> None:
    metadata_store.update_backfill_job(job_id, "running")
    try:
        resolution = resolve_version(target, version)
        retrieval = retriever.fetch_orchestrator_knowledge(
            target=target,
            requested_version=version,
            resolved_version=resolution.resolved_version,
        )
        pack = build_pack_payload(
            target=target,
            requested_version=version,
            resolved_version=resolution.resolved_version,
            retrieval=retrieval,
        )
        errors = validate_pack(
            pack,
            min_trusted_sources=settings.min_trusted_sources,
            min_confidence=settings.min_confidence,
            trusted_domains=settings.trusted_domains,
        )
        pack_id = str(uuid.uuid4())
        object_key = f"{target}/{version}/{pack_id}.json"
        pack["pack_id"] = pack_id
        object_store.put_json(object_key, pack)
        record = {
            "pack_id": pack_id,
            "target": target,
            "version": version,
            "status": "active" if not errors else "inactive",
            "object_key": object_key,
            "confidence": pack["confidence"],
            "source_count": len(pack.get("sources", [])),
            "validation_errors": errors,
        }
        metadata_store.put_pack_record(record)
        if not errors:
            metadata_store.activate_pack(target, version, pack_id)
            metadata_store.update_backfill_job(job_id, "succeeded", pack_id=pack_id)
        else:
            metadata_store.update_backfill_job(
                job_id,
                "failed",
                error="; ".join(errors),
                pack_id=pack_id,
            )
    except Exception as exc:  # noqa: BLE001
        metadata_store.update_backfill_job(job_id, "failed", error=str(exc))


def create_app(settings: ServiceSettings | None = None) -> FastAPI:
    cfg = settings or ServiceSettings.from_env()
    object_store = FilesystemObjectStore(cfg.object_store_dir)
    metadata_store = SQLiteMetadataStore(cfg.sqlite_path)
    retriever = ExaKnowledgeRetriever(
        api_key=cfg.exa_api_key,
        search_type=cfg.exa_search_type,
        num_results=cfg.exa_num_results,
    )

    app = FastAPI(title="Pipeline Codegen KB Service", version="1.0")

    def auth_dep(authorization: str | None = Header(None)) -> None:
        require_bearer_token(cfg.service_token, authorization)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/v1/version/resolve",
        response_model=ResolveVersionResponse,
        dependencies=[Depends(auth_dep)],
    )
    def resolve_version_route(payload: ResolveVersionRequest) -> dict[str, Any]:
        known_versions = available_profile_versions(payload.target)
        resolution = resolve_version(payload.target, payload.requested_version, known_versions)
        return {
            "target": payload.target,
            "requested_version": payload.requested_version,
            "resolved_version": resolution.resolved_version,
            "exact_match": resolution.exact_match,
            "reason": resolution.reason,
            "known_versions": known_versions,
        }

    @app.post(
        "/v1/kb/backfill",
        response_model=BackfillAcceptedResponse,
        dependencies=[Depends(auth_dep)],
        status_code=status.HTTP_202_ACCEPTED,
    )
    def create_backfill(
        payload: BackfillRequest,
        background_tasks: BackgroundTasks,
    ) -> dict[str, str]:
        job_id = str(uuid.uuid4())
        metadata_store.create_backfill_job(job_id, payload.target, payload.version)
        background_tasks.add_task(
            partial(
                _process_backfill_job,
                job_id=job_id,
                target=payload.target,
                version=payload.version,
                settings=cfg,
                object_store=object_store,
                metadata_store=metadata_store,
                retriever=retriever,
            )
        )
        return {
            "job_id": job_id,
            "status": "queued",
            "target": payload.target,
            "version": payload.version,
        }

    @app.get(
        "/v1/kb/backfill/{job_id}",
        response_model=BackfillStatusResponse,
        dependencies=[Depends(auth_dep)],
    )
    def get_backfill(job_id: str) -> dict[str, Any]:
        row = metadata_store.get_backfill_job(job_id)
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")
        return row

    @app.get(
        "/v1/kb/{target}/{version}",
        response_model=KnowledgePackResponse,
        dependencies=[Depends(auth_dep)],
    )
    def get_knowledge_pack(target: str, version: str) -> dict[str, Any]:
        active = metadata_store.get_active_pack(target, version)
        if active is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": "pack_not_found", "target": target, "version": version},
            )
        pack = object_store.get_json(active["object_key"])
        return {
            "target": target,
            "version": version,
            "pack_id": active["pack_id"],
            "status": "active",
            "pack": pack,
        }

    return app


app = create_app()


def main() -> None:
    uvicorn.run("pipeline_codegen.kb_service.app:app", host="0.0.0.0", port=8787, reload=False)


if __name__ == "__main__":
    main()
