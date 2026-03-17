# Architecture

Pipeline-codegen executes in deterministic stages:

1. Validate OPOS input externally (upstream project).
2. OrchMapper maps OPOS to Target IR using target+version profile.
3. Generation routes by target family.
4. Imperative targets render Python artifacts directly from Target IR.
5. Declarative targets project Target IR into a declarative workflow model, then render YAML.
6. Verifier checks artifact syntax/structure, manifest integrity, and target semantics.

LLM-assisted mode is bounded to imperative task internals only and never controls graph wiring or operator selection.
