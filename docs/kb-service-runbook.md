# KB Service Runbook

Hosted knowledge service provides orchestrator-version context for `llm-assisted` generation.

## Purpose

- Keep Exa API usage server-side (service-owned key).
- Serve cached structured knowledge packs by `target@version`.
- Resolve unknown versions to nearest compatible profile and backfill asynchronously.

## Local Startup

```bash
pip install -e '.[dev]'
export KB_SERVICE_TOKEN=dev-token
export EXA_API_KEY=YOUR_BACKEND_KEY
opos-kb-service
```

Service defaults:

- URL: `http://localhost:8787`
- Object store: `/tmp/pipeline_codegen_kb/object_store`
- Metadata DB: `/tmp/pipeline_codegen_kb/metadata.db`

## API Summary

- `GET /v1/kb/{target}/{version}`: fetch active pack.
- `POST /v1/version/resolve`: deterministic version fallback.
- `POST /v1/kb/backfill`: enqueue retrieval/build/promotion.
- `GET /v1/kb/backfill/{job_id}`: job state and diagnostics.

All `/v1/*` routes require:

`Authorization: Bearer <KB_SERVICE_TOKEN>`

## Generation Lifecycle

1. CLI requests KB pack for requested `target@version`.
2. Hit:
   - use exact context in LLM prompt.
3. Miss:
   - resolve nearest compatible profile.
   - generate immediately with resolved profile.
   - enqueue backfill for requested version.
4. Backfill validates pack and auto-promotes on pass.
5. Future requests use exact promoted context.

## Operations

- Rotate `EXA_API_KEY` via backend secret manager.
- Never expose Exa key in CLI flags or client `.env`.
- Monitor failed backfill jobs via `/v1/kb/backfill/{job_id}`.
- Investigate validation failures (trusted source count/confidence/field completeness).
