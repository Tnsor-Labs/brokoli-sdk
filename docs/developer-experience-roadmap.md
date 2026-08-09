# Brokoli Python SDK developer-experience roadmap

**Status:** Draft for review
**Date:** 2026-08-09
**Tracking:** [brokoli-sdk#15](https://github.com/Tnsor-Labs/brokoli-sdk/issues/15)

The canonical cross-project strategy is [Brokoli's SDK developer experience RFC](https://github.com/Tnsor-Labs/brokoli/blob/main/docs/rfcs/sdk-developer-experience-strategy.md). Backend decisions are tracked in the core repository's ADRs, starting with ADR-014 (pipeline IR ownership and compatibility) and ADR-015 (logical and physical execution plans).

This document records only the Python SDK work needed to deliver that strategy.

## Product boundary

The SDK is an authoring, compilation, validation, and operations client. It does not own distributed scheduling.

The intended experience is:

```python
with Pipeline("daily-orders", schedule="0 6 * * *"):
    orders = source_api("Orders", url="...", records="data")
    cleaned = clean(orders)
    cleaned >> sink_file("Archive", path="...")
```

The backend decides how many page, partition, or mapped instances execute; where they run; how they retry; and how large values move.

## Support matrix

| Area | Current status | Required SDK action |
|---|---|---|
| Static DAGs and built-in nodes | Available | Preserve and cover with backend contract fixtures |
| Decorated Python tasks | Available with packaging limits | Add runtime/image contracts only after backend ADRs |
| HTTP pagination | Available | Keep config and backend behavior tested together |
| Artifact response and automatic spill | Available with local-store limits | Document the runtime reference row (`uri`, `media_type`, `size_bytes`, `checksum`) that replaced the inlined body, and authoring refs versus runtime refs |
| Capability negotiation | Server available, SDK missing preflight | Implement [#9](https://github.com/Tnsor-Labs/brokoli-sdk/issues/9) |
| `.expand()` | Partial: sequential in one node | Mark limits clearly; require physical-instance capability for distributed semantics |
| Collection union | Partial: in-memory dataset union | Keep available; distinguish it from future manifest union |
| Dataset map/filter | Compile-only | Mark experimental until partition execution exists |
| Conditions | Available with composition gaps | Fix or reject nested branch inputs; serialize branch intent |
| Operational CLI | Minimal | Add diff/run/status/logs/cancel/retry/backfill |
| Local testing | Function tests only | Add graph snapshots and backend-backed local harness |
| Static typing | Partial annotations | Ship `py.typed`, type-check CI, parameter/data contracts |

## Highest-priority problems

### Deterministic compilation

Logical node identity is now deterministic: an explicit `node_key` wins; otherwise the canonical display name is qualified by a per-name, per-pipeline counter. Source position is never an identity input. Same-name insertion can renumber later same-name nodes, so durable identities should use `node_key`. Upgrading from older releases causes a one-time change from their random IDs.

This is only the deterministic-identity slice. It does not implement normalized semantic snapshots/diffs, the commands below, or the full issue #15 M1 scope.

The normalized semantic representation excludes layout-only changes.

Required commands:

```bash
brokoli compile pipeline.py --check
brokoli diff pipeline.py --server "$BROKOLI_URL"
```

### Runtime honesty

The SDK currently exposes operations that only emit future-facing IR. Until the server advertises and executes those capabilities:

- label them experimental in API documentation;
- require explicit opt-in when authoring them;
- block deployment to a server that does not support them;
- never imply that successful local serialization means executable support.

### Safe authoring context

`Pipeline._current` should be replaced by a context-local stack so nested pipelines restore their parent and parallel async/threaded compilation cannot register nodes into the wrong graph.

Pipeline discovery should use explicit entrypoints and minimize arbitrary top-level execution. Importing a module remains available as a compatibility path, but its side effects should be documented and avoidable.

### Local tests

Provide three layers without implementing another scheduler:

1. Test a decorated function as ordinary Python.
2. Compile and snapshot normalized graph/IR contracts.
3. Execute supported nodes through a local Brokoli backend harness with mock connections and connectors.

The third layer must exercise Go behavior so local success predicts deployed success. The control plane is one Go binary, so the harness should *be* that binary — embedded, downloaded, or spawned — never a Python approximation of it. This is the one seam Airflow, Dagster, and Prefect all leak at, and the only one of the four architectures that can close it completely is this one.

### Operational CLI

The SDK CLI should become the complete client for routine pipeline operations:

```bash
brokoli diff pipeline.py
brokoli run daily-orders --param ds=2026-08-09
brokoli status RUN_ID
brokoli logs --follow RUN_ID
brokoli retry RUN_ID --failed
brokoli cancel RUN_ID
brokoli backfill daily-orders --from 2026-08-01 --to 2026-08-08
```

These commands call backend APIs. They do not execute distributed tasks in the SDK process.

### Typing and configuration

- Add `py.typed` and test installed-package typing.
- Infer run-parameter schemas from function signatures where semantics are unambiguous.
- Add explicit connection, secret, and resource references instead of relying only on string IDs.
- Keep `DatasetRef` and `ArtifactRef` documented as logical authoring handles, not runtime storage references.
- Define one canonical invocation style and make `>>` equivalent convenience syntax.

## Delivery order

### 1. Quality foundation

- Python 3.9-3.13 CI matrix.
- Tests, formatting, linting, typing, and wheel/sdist build/install checks.
- Fix README links, ports, dependencies, and nonexistent examples.
- Build a generated support matrix from capabilities or tested fixtures where practical.

### 2. Compatibility and deterministic IR

- Implement server capability preflight.
- Add stable node identity and normalized semantic diff. This step is a hard prerequisite of the core repository's physical-plan milestone, not a parallel track: instance keys derive from logical node IDs (core ADR-015), so key derivation must not freeze while IDs still churn.
- Serialize every accepted `Pipeline` option or reject it locally.
- Add cross-repository IR fixtures from the core-owned schema.

### 3. Test and discovery experience

- Context-local pipeline stack.
- Explicit module/entrypoint discovery.
- Graph assertions and snapshot helpers.
- Local backend harness and connector mocks.

### 4. Operations

- Run, status, logs, cancel, retry, backfill, and pull/diff commands.
- Environment-aware deployments and code/IR digests.
- Actionable error taxonomy for auth, network, compatibility, validation, and run failure.

### 5. Runtime-backed advanced APIs

Promote distributed `.expand()` and dataset partition methods from experimental only after the core backend provides durable physical instances, partial retry, manifest-backed collections, and capability reporting for them. Keep current in-memory union available, but name and document future manifest union as a different execution capability.

## Competitive direction

- From Airflow: adopt runtime-map safety limits, named instances, operational completeness, and runtime isolation; avoid operator boilerplate and metadata-driven large-data transfer.
- From Dagster: adopt local testability, explicit resources, partitions, data contracts, and quality metadata; keep simple data flow as the entry-level model.
- From Prefect: adopt normal-function ergonomics, parameter schemas, local confidence, and deployment/worker flexibility; retain language-neutral compilation and Go-owned orchestration.

## Definition of done for a public SDK feature

- The API has one canonical documented form.
- Static types describe accepted inputs and logical output kind.
- Local validation covers invalid combinations.
- Capability negotiation rejects unsupported servers.
- A cross-repository test proves the backend accepts and executes the emitted IR.
- One maintained example runs end to end.
- CLI and UI report failures with the logical node and physical attempt context available from the backend.

## Non-goals

- A Python distributed scheduler.
- A local execution engine with semantics different from Go.
- General automatic parallelization of Python functions.
- Public APIs that are only placeholders for possible backend work.
