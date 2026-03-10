# Architecture

Pipeline-codegen executes in deterministic stages:

1. Validate OPOS input externally (upstream project).
2. OrchMapper maps OPOS to Target IR using target+version profile.
3. Renderer generates target artifacts from Target IR.
4. Verifier checks artifact syntax/structure and manifest integrity.

LLM-assisted mode is bounded to task internals only and never controls graph wiring or operator selection.
