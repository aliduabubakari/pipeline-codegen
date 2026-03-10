# OPOS to Orchestrator Code Generation Plan (v1)

## Summary
Build `pipeline-codegen` to transform validated OPOS into runnable orchestrator artifacts via:

`OPOS -> OrchMapper -> Target IR -> Renderer (+ optional bounded LLM task-body generation) -> Verifier`

## Implementation Changes
1. Bootstrap repo with Python package, CLI, tests, CI, and this plan file.
2. Define core contracts:
   - `schemas/target_ir_v1.json`
   - target capability profiles under `profiles/` for `airflow@2.8`, `prefect@3.x`, `dagster@1.8`, `kestra@0.18`
   - deterministic mapping policy + error taxonomy (`MAP*`, `IR*`, `GEN*`, `VFY*`)
3. Implement deterministic OrchMapper:
   - input: OPOS + target + version + config
   - output: TargetIR + diagnostics
   - no LLM usage in mapper
   - supported execution types: `python_script`, `bash`, `container`, `http_request`, `sql`
4. Implement generation subsystem:
   - template renderer by target
   - optional `llm-assisted` mode constrained to task internals only
   - emit `artifacts.json` manifest with checksums and entrypoint
5. Implement target adapters:
   - Airflow, Prefect, Dagster, Kestra for v1
   - Kubeflow/Kubernetes as v1.1 stubs only

## Public Interfaces
- CLI:
  - `opos-generate --input <opos.yaml> --target <airflow|prefect|dagster|kestra> --target-version <ver> --out-dir <dir> --mode <template|llm-assisted> --strict`
- Library:
  - `map_to_target_ir(opos_doc, target, target_version, config) -> TargetIR`
  - `generate_artifacts(target_ir, mode, llm_config=None) -> ArtifactBundle`
  - `verify_artifacts(bundle, target, target_version) -> VerificationReport`

## Test Plan
1. Mapping snapshot tests (OPOS fixture -> TargetIR snapshot) and `MAP*` failures.
2. Generation snapshot + determinism tests.
3. End-to-end conformance using:
   - `ok_sequential`, `ok_parallel_like_dag`, `ok_notifier`
4. Verifier tests for syntax checks + manifest integrity.
5. CI gates: lint, types, tests, snapshots, determinism.

## Assumptions and Defaults
- OPOS is the only input contract.
- v1 focuses on common operators, not full platform coverage.
- `template` mode is default.
- `llm-assisted` mode is opt-in and constrained.
- single workflow artifact is default unless split policy is explicitly configured.
