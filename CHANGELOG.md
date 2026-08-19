# Changelog

All notable changes to the Brokoli Python SDK are documented here. The
format is loosely [Keep a Changelog](https://keepachangelog.com/); the
project is pre-1.0, so a breaking change can land in a minor release —
those are called out explicitly.

## Unreleased

### Changed

- **`server` now defaults to the hosted platform** (`https://in-brokoli.orkestri.site`)
  everywhere it was previously required or defaulted to a local address:
  `Client()`/`AsyncClient()` (previously `server` was a required positional
  arg), `Client.from_env()`/`AsyncClient.from_env()` (previously fell back
  to an empty string, erroring, when `BROKOLI_SERVER` was unset), and every
  CLI command (previously only `deploy`/`validate` defaulted to
  `http://localhost:8080`; the other seven required `--server`/`--env`
  outright). Self-hosted users are unaffected as long as they already pass
  `server`/`--server`/`--env`/`BROKOLI_SERVER` -- this only changes what
  happens when none of those are given. *(Breaking: code that relied on
  `Client()` raising without a server, or a CLI command raising without
  `--server`/`--env`, now silently targets the hosted platform instead.)*

## 0.5.0 — 2026-08-19

First release actually published to PyPI (`pip install brokoli` now works — prior
tags were GitHub-only, no publish pipeline existed).

### Added

- **Observability reads** (#57 item 7). `client.dlq(pipeline, include_resolved=, limit=)`
  and `Run.node_preview(node_id)` — the dead-letter queue and per-node
  output sample the CLI already exposes, now importable. Cancel/retry
  verification scripts assert DLQ emptiness and spot-check row values
  without exporting through a sink first.

- **Async run-ops client** (#57 item 8, `brokoli.AsyncClient`/`AsyncRun`).
  A parallel async counterpart to `Client`/`Run` -- not a background-
  thread wrapper around them. Every REST method delegates to a plain
  `Client` via `asyncio.to_thread` (one tested HTTP implementation, not
  two); `AsyncRun.watch()`/`.wait()` are genuinely async-native, backed
  by a real WebSocket subscription against the server's SODP endpoint
  for near-instant run-completion notice instead of a fixed poll
  interval.
  - `AsyncClient(server, api_key=...)` / `.from_env(...)` -- same
    construction as `Client`.
  - `client.run(pipeline, params=...) -> AsyncRun`;
    `AsyncRun.watch(poll_interval=...)` (an async generator yielding the
    run's detail on every status change) and `.wait(timeout,
    poll_interval, raise_on_failure=...)`, `.status()`, `.node_runs()`,
    `.cancel()`, `.logs(...)`.
  - The SODP subscription is a signal to refetch, not the value
    returned -- every yield is a real REST `detail()` call, since the
    pushed state is a narrower rollup than the full run object. A REST
    poll runs alongside the subscription regardless, because a `blocked`
    run (failed cross-pipeline dependency check) never emits an event at
    all and would otherwise wait forever.
  - The push path requires the new `watch` extra: `pip install
    "brokoli[watch]"` (installs `sodp-client`). Without it, or if the
    connection doesn't complete within a bounded timeout, `watch()`/
    `wait()` still work correctly -- they fall back to plain polling,
    exactly like the sync `Run.wait()`. Every other `AsyncClient` method
    needs no extra dependency either way.

- **Live-test fixture** (#57 item 9, `brokoli.testing.live_pipeline`).
  Formalizes what every verification script under #57 reimplemented by
  hand: deploy a pipeline under a unique id (dodging the server's slug
  uniqueness index on concurrent or repeated runs), yield a handle to
  fire real runs against it, and delete it from the server on exit --
  whether the test passed, failed, or raised.
  - `with live_pipeline(client, pipeline) as lp: lp.run().wait(...)`.
    `cleanup=False` skips the delete, for inspecting a failing live test
    afterward.
  - New `Client.delete_pipeline(pipeline)` (and the `AsyncClient`
    equivalent) backing the fixture's teardown -- the SDK had every
    other pipeline CRUD operation already, this was the missing one.

### Fixed

- **`Client.deploy()` on a fresh credentialed client.** `preflight_server_compatibility`
  and `validate_pipeline` read the auth header directly instead of going
  through `_request`, so a `username`/`password` client whose first-ever
  call was `deploy()` sent those two requests unauthenticated and failed
  with a misleading "verify your token" error despite valid credentials.
  Lazy login now triggers before either call.

## 0.4.0 — 2026-08-15

### Added

- **Programmatic run-ops client** (#57 items 1–6, `brokoli.Client`). The
  operational surface the CLI already had, as a library: fire runs, wait
  on them, cancel them, read their logs, and deploy pipelines in-process.
  Written to replace the raw-urllib scripts the platform's own
  production-readiness verification had to hand-roll.
  - `Client(server, api_key=...)` or `Client(server, username=...,
    password=...)` — credentialed clients log in lazily and renegotiate
    exactly once per request on 401, so long-lived harnesses survive
    token expiry. Thread-safe; stdlib-only.
  - `client.run(pipeline, params=...) -> Run`; `Run.wait(timeout,
    poll_interval, raise_on_failure=...)`, `.status()`, `.node_runs()`,
    `.cancel()`, `.logs(level=..., node=...)`.
  - `client.pipelines()` / `client.pipeline(id_or_pipeline_id_or_name)`
    with the CLI's resolution precedence, cursor pagination, and every
    historical response shape absorbed in one place — including the
    trigger response's `run_id`/`id`/nested variants that scripts kept
    re-parsing wrong.
  - `client.deploy(pipeline)` with the same fail-closed matching as
    `brokoli deploy` (pipeline_id first, never a guess across
    duplicates), minus the printing.
  - Exceptions: `APIError` (status/url/body attached), `AuthError`,
    `RunFailed` (final run object attached for assertions).

## 0.3.0 — 2026-08-11

The maturation release: the SDK becomes deterministic, testable,
operationally complete from the CLI, and honest about what the target
server can actually run. First tagged release.

### Added

- **Operational CLI.** `run`, `status`, `logs`, `cancel`, `retry`, and
  `backfill` drive the backend run APIs, so routine run/observe/retry work
  no longer means leaving the terminal for the console. `run`/`backfill`
  accept a pipeline's logical id, name, or internal id.
- **Named deployment environments.** Define `environments` in a
  `brokoli.yaml` (or point `BROKOLI_CONFIG` at one) and target them with
  `--env prod` instead of repeating `--server`/`--api-key`. Tokens are read
  from a named env var (`token_env`), so no secret lives in the file.
- **Auditable IR digests.** `deploy` prints a stable `sha256:` digest of
  exactly what it deployed — identical across create/update and cosmetic
  churn, so a no-op redeploy is visible. `compile --digest` emits the same
  digest with no server, for CI to capture and diff.
- **Typed resource references** (`brokoli.resources`): `Connection` (a
  typed, validated `conn_id`) and `Secret`/`Variable`/`Param`/`EnvVar`,
  which compile to the engine's `${namespace.name}` interpolation and are
  resolved in node configs at run time. Kept deliberately distinct — in
  type and documentation — from the authoring-time data refs (`DatasetRef`
  et al.).
- **A local test harness** (`brokoli.testing`): `graph()` to assert node
  and edge shape, `run_task()` to unit-test a task's Python logic,
  `ir_snapshot()`/`assert_stable_ir()` for golden-file and determinism
  tests — all without deploying, and without emulating the engine.
- **`py.typed`** and a supported type-check configuration, so consumers'
  type checkers see the SDK's annotations.
- **Per-feature examples** and a support matrix (`examples/README.md`),
  each example gated in CI by `compile --check`.
- **CI**: a test matrix across Python 3.9–3.13, a build/package job that
  verifies `py.typed` and the test harness ship in the wheel, and an
  examples-compile gate.

### Changed

- The `with Pipeline(...)` authoring context is now nesting-, thread-, and
  async-safe (backed by `contextvars` instead of a class global).
- Pipeline **discovery is separated from run-time side effects**: files
  import under their own module name (never `__main__`), and
  `BROKOLI_DISCOVERY` is set during import so module-level code can skip
  run-time-only setup.
- **Deterministic compilation**: stable node ids, a normalized IR
  snapshot/diff, and validation that the same source compiles to the same
  semantic diff.
- Deploy negotiates the server's supported **IR versions** and
  **execution features** before persisting, refusing — with a named
  reason — anything the target server can't run.
- Every decorator (`@source`/`@sink`/`@map`/…) now shares `@task`'s
  automatic packaging (helpers, constants, imports, closures), and package
  metadata declares third-party requirements.

### Fixed / hardened

- Local validation catches cycles, enum typos, fan-in truncation, and
  unsupported/compile-only features before deploy, and validates every
  emitted payload against core's canonical schema.
- Unsupported `Pipeline(...)` options (`catch_up`, `max_retries`,
  `concurrency`) are **rejected at construction** rather than silently
  dropped; hooks (`on_failure=…`) are real, persisted webhook hooks.
  *(Breaking: these previously no-op'd.)*
- Auth/network errors during the deploy upsert lookup surface as errors
  instead of being swallowed into a spurious create.
