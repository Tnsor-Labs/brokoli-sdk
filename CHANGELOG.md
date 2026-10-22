# Changelog

All notable changes to the Brokoli Python SDK are documented here. The
format is loosely [Keep a Changelog](https://keepachangelog.com/); the
project is pre-1.0, so a breaking change can land in a minor release —
those are called out explicitly.

## Unreleased

### Changed

- **`brokoli propose` changes a value built from a constant where the
  constant is defined.** A URL written as `f"{SHOP_API}/products"`, asked
  to become `https://dummyjson.com/products`, used to be overwritten with
  the literal URL in `pipeline.py`, leaving the wrong `SHOP_API` in place
  for everything else that reads it. It now sets `SHOP_API`, in
  `pipeline.py` or in the project module it is imported from, when the new
  value keeps the expression's fixed text and the pipeline compiles to
  exactly that change; otherwise the value is written in `pipeline.py` as
  before. The applied change says which constant it set, in which file,
  and which other files import it. The JSON result gains `files`, every
  file the proposal changes; `--write` writes each of them.

### Fixed

- **`brokoli propose` compiled against stale project modules.** Each
  recompile reused the project modules the first compile had imported
  (`shared.endpoints`, say), and the same name from another project loaded
  earlier in the process. The project's own modules are now imported
  afresh for every compile.

## 0.11.0 - 2026-10-20

### Added

- **`brokoli mcp`** relays a Brokoli server's MCP endpoint (`/api/mcp`) on
  stdio, for agent clients that only start local processes. Each JSON-RPC
  message goes to the server with the command's usual credentials
  (`BROKOLI_TOKEN`, `--api-key`, a project environment, or `brokoli auth`);
  a failed request is answered with a JSON-RPC error saying why (not
  signed in, not permitted, unreachable). The tools are the server's.

- **`brokoli propose`** writes changes to a pipeline's definition, in the
  IR's terms, into its `pipeline.py` and keeps the rest of the file as
  written: the pipeline's name, description, schedule, timezone and tags;
  a node's name and settings; nodes added and removed; connections made
  and dropped. Every change is checked by compiling the file again and
  kept only when it compiles to exactly that change; anything else is
  skipped with the reason. Needs `pip install 'brokoli[propose]'`. See
  `docs/projects.md`.

- **Projects.** A repository of pipelines with a `brokoli.yaml` that
  declares `version: 1`: one folder per workspace, one folder per
  pipeline (`workspaces/<workspace>/pipelines/<pipeline_id>/pipeline.py`),
  and a `shared/` package every pipeline can import. The folder name is
  the pipeline's `pipeline_id`, each `pipeline.py` defines one pipeline,
  and only `main` may be a production branch. A `brokoli.yaml` without
  `version` keeps working for `--env` as before. See `docs/projects.md`.
- `brokoli init`: creates a project, with an example pipeline and a GitHub
  Actions workflow that validates it. Writes nothing if any file exists.
- `brokoli new pipeline <id>`: adds a pipeline folder from a template
  (`basic` or `python`).
- `brokoli validate` without a file validates the whole project, offline
  unless given a server.
- `brokoli compile --project [--out build]`: writes every pipeline's
  normalized IR, its task bundles, and a manifest. The build is
  deterministic, and each IR file's sha256 is its manifest digest.
- `brokoli plan`: compares the project with a server, each workspace with
  its own server workspace: added, changed, unchanged, and pipelines on
  the server that the folder does not have. `--diff`, `--json`,
  `--detailed-exitcode`.

## 0.10.0 - 2026-10-15

Catches the SDK up with core v0.12 to v0.18. Each feature that needs a
newer server names the version below; the SDK checks the node types a
server offers before deploying, so an unsupported node fails at deploy
with its name rather than at run time. The vendored schemas and the
backend contract tests now track core v0.18.12.

### Fixed

- **`dbt(vars=...)` given a dict was silently dropped.** The server reads
  `vars` as a string, so a dict never reached dbt's `--vars`. A dict is
  now sent as JSON; a string is sent as written.
- **Device login no longer gives up on a rate-limited poll.** HTTP 429,
  502, 503 and 504 from the poll endpoint now widen the polling interval
  and keep going until the code expires, instead of aborting a login
  that may be being approved in the browser at that moment.

### Added

- **Dataset schemas** (core v0.12.0): `dataset_schema(...)` builds a
  portable `brokoli.dataset-schema/v1` descriptor (ordered columns,
  scalar and decimal types with precision and scale, nullability, enum,
  array, map and record types, `additional_columns` open or closed).
  Sources take `schema=`; declared schemas propagate through joins,
  `project` and `aggregate`, and the server enforces them at run time, so
  a source that stops returning a column fails instead of producing a
  narrower dataset.
- **Join collision policy**: `join(..., collision_policy="prefix" |
  "error" | "alias", right_alias=...)` states what happens when both sides
  carry the same column name (`prefix` is what the engine always did).
- **Native relational operators**: `project()`, `filter_rows()` and
  `aggregate()`, with the expression builders (`column`, `literal`,
  `add`, `subtract`, `multiply`, `divide`, `concat`, `coalesce`, `eq`,
  `neq`, `lt`, `lte`, `gt`, `gte`, `is_null`, `logical_not`, `all_of`,
  `any_of`, `case_when`). Output types are inferred from declared input
  types. Needs core v0.12.0.
- **API execution profiles**: `source_api(execution_profile=...)` with
  `public_api_safe()`, `high_throughput()` and `strict()`, which expand to
  the full explicit policy core requires.
- **S3 connections on file nodes**: `source_file`/`sink_file` take
  `conn_id=` naming an S3 connection (core v0.12.0).
- **`wait()`**: park a run until a condition holds (`file_exists`,
  `http`, `interval_elapsed`, `pipeline`) without holding a concurrency
  slot. Durations take a Go duration string, seconds, or a `timedelta`.
  Needs a server on v0.10.79 or newer.
- **`contract_gate()`**: enforce an actually-fine data contract on a
  dataset. Takes the contract IR as a dict, or an `actually_fine.Contract`.
  Needs a server on v0.13.0 or newer.
- **`bash()`**: run a trusted Bash command with dynamic values passed
  through `env`. Platform references in `command` are refused locally, as
  the server refuses them. Needs a server on v0.15.0 or newer that has not
  disabled bash nodes.
- **`dbt()` options `conn_id`, `target_schema` and `output_model`**: build
  the dbt profile from a Brokoli connection, set its schema, and pick the
  model whose table the node hands downstream.
- **Partitioned `migrate()`**: `partition={"column", "strategy",
  "boundaries", "max_parallel"}` copies ranges in parallel, each retried on
  its own (core v0.17.0). Validated locally the way the server does it: an
  upsert whose `key_columns` include the partition column, into an existing
  table. `datetime` boundaries are sent as RFC3339.
- **`pool=`** on every node that reads or writes data, to share a
  concurrency limit across nodes on the server.
- **`code()` options `max_memory_mb`, `max_cpu_seconds` and `node_path`**;
  **`sink_db()` options `create_table` and `table_engine`** (ClickHouse);
  **`source_api(source_type=...)`**.
- **Workspaces.** `Client(workspace=...)`, `BROKOLI_WORKSPACE`, the CLI's
  `--workspace` and a named environment's `workspace` send `X-Workspace-ID`
  on every request, including validation's connection check. Without one,
  the server uses your default workspace.
- **`Client.dry_run()`, `Client.backfill()` and `Run.resume()`** (also
  `Client.resume(run_id)`). `resume` returns the new run, and takes
  `from_node` to re-run from a chosen node onward. `backfill` takes dates
  as whole days or `datetime`s/RFC3339 strings as instants.
- **Validation refuses `${param.x}` in a code script**, as the server has
  since v0.15.0; read `params["x"]` instead.
- **The vendored IR schemas are refreshed from core v0.18.12**, and
  `pipeline-ir-2.2.json` (with `task-interface-v1.json`) is now vendored,
  so pipelines with task interfaces are schema-checked too.
- **Deploy checks the node types.** `GET /api/capabilities` lists the node
  types a server knows and the ones its operator disabled
  (`disabled_node_types`); deploy now refuses a pipeline that uses either,
  naming the type, before anything is saved. Servers that predate the list
  are not checked.

## 0.9.0 - 2026-09-08

Requires a Brokoli server on **v0.11.6 or newer** for pipelines that
declare typed parameters; see the first entry for why that is enforced
rather than advisory.

### Fixed

- **A task now actually receives the parameters it declares** (core
  issue #487). The generated wrapper called your function as
  `func(rows)` -- no keyword arguments -- so a declared parameter could
  never arrive. Combined with the server side, which validated a
  submitted value, recorded it on the run row, and then ran the task
  with its default, this meant:

  ```python
  @task("Score")
  def score(rows, threshold: float = 0.5):   # declares a parameter
      ...
  ```

  triggering a run with `threshold=0.9` computed with `0.5`. **A green
  run and a wrong answer**, with nothing reporting a problem.

  The call now forwards the names your function actually accepts, and
  only when the run supplied them, so anything unsupplied still falls
  back to your own default rather than arriving as `None`. **If you have
  a pipeline with declared parameters, re-check any result that depended
  on one.**

### Added

- **Deploy preflight gates three more server capabilities**, each
  closing a "deploys clean, then misbehaves" path:
  - `task-parameters-v1` for a pipeline declaring typed parameters.
    `task-interface-v1` only ever meant the server *accepts* the
    declaration; it never meant the values would be honoured. Those are
    different questions and now have different names, so a declaring
    pipeline is refused on a server that would silently ignore what you
    submit.
  - `code-typescript` for `code(language="typescript")` nodes. The
    server advertises this only when a Node runtime resolved at startup,
    so a TypeScript node used to deploy cleanly against a server with no
    Node and fail at run time.
  - `task-runtime-v1`/`task-bundle-v2` for ADR-033 `task` nodes, and
    `task-ports-v1` for edges naming `from_port`/`to_port`. No authoring
    API emits either shape yet, so these cover hand-assembled or
    round-tripped IR today and go live unchanged when authoring lands.
    Note `task-bundles` (ADR-031, `task-bundle/1`) is a different mount
    mechanism and is deliberately not accepted in its place.


### Added

- **`@task` infers a portable node interface and typed pipeline
  parameters from type hints** (Brokoli core ADR-032 rollout step 3,
  core issue #439). A `TypedDict`/dataclass-annotated `rows` parameter
  and return value compile to a BPTD record interface (IR 2.2); any
  other annotated keyword parameter becomes a typed pipeline parameter
  -- optional with its default if it has one, required otherwise.
  Unannotated or unrecognized types stay honestly unknown rather than a
  guessed shape, and a plain, fully untyped `@task` compiles exactly as
  before (no interface/parameters keys, IR stays at 2.0/2.1). Pass
  `interface={...}` to `@task` to declare the row schema explicitly
  instead of relying on inference. Requires a server advertising the
  `task-interface-v1` execution feature; deploy preflight refuses older
  servers the same way every other execution-feature gate already does.
- **Deploy preflight gates `emit()` scripts on server support** (#84).
  Core servers now advertise the `code-streaming-emit` execution
  feature (ADR-029); a code-node script using `emit(`/`begin_emit(`
  requires it, so deploying to a server whose wrapper predates the
  streaming idiom is refused at deploy time with a feature-naming error
  instead of failing (or silently passthrough-ing) at run time. Servers
  that don't advertise features at all are, as ever, waved through
  unless `allow_legacy_server=False`.

## 0.8.0 - 2026-08-29

One fix, minor bump: it is a behavior break called out below, and under
this project's pre-1.0 rules breaks ride minor versions.

### Fixed

- **`when()`/`otherwise()` refuse `>>` chains instead of silently
  misrouting them** (#81). `gate.when(transform(...) >> sink(...))`
  routed the branch edge to the *sink* -- `>>` returns its right-hand
  node -- and left the transform with no input at all. The pipeline
  deployed and ran without a single error; the sink just received the
  gate's rows untransformed. Both methods now raise a `PipelineError`
  when the branch target already has incoming edges, naming the feeding
  nodes and showing the explicit form (`shaped = transform(...);
  gate.when(shaped); shaped >> sink(...)`). This also refuses targets
  fed from elsewhere in the graph, where skip propagation was never
  well defined. Converging branches downstream of the decision is
  unaffected. Pre-1.0 breaking note: code that relied on the silent
  misroute was already producing a wrong graph -- it now fails loudly
  at authoring time instead.

## 0.7.1 - 2026-08-29

All three found by walking the new tutorial live against a fresh
install, which is the release note and the moral.

### Fixed

- **`brokoli --version` tells the truth** (#78). Every release since
  0.4.0 reported 0.4.0: the version string was hand-maintained and
  never bumped again. Now single-sourced from the installed
  distribution, with an ownership check so a source-tree import next to
  some other installed brokoli does not borrow its number.
- **`brokoli backfill` speaks the interval-native API** (#79). RFC3339
  bounds went into the legacy date-only fields and the server refused
  them; bounds now route by shape (bare dates keep their inclusive-day
  meaning, timestamps use start/end), the plan response is rendered,
  and legacy servers still get the old fields.
- **`brokoli auth` output flushes before the wait** (#79). The code and
  link printed, then the process polled for minutes; piped or
  redirected stdout showed nothing until exit, which read as a hang.

## 0.7.0 - 2026-08-29

### Added

- **`brokoli auth`: browser-confirmed login** (#76, closes #75). The
  terminal requests a short code, opens the server's confirm page, and
  receives a revocable `brk_` API token once you approve in a browser
  that is already logged in -- no password ever touches the shell, its
  history, or a CI log. The token lands in a per-user credentials file
  (0600, keyed by server URL, `BROKOLI_CREDENTIALS` override) that
  every CLI verb and `Client` picks up as the LAST auth fallback:
  `BROKOLI_TOKEN`, `--api-key`, and a project environment's
  `token_env` all win over stored credentials. `--no-browser` prints
  the link instead of opening it.
- **`Client(device_auth=True)`** runs the same grant from a library or
  notebook, or reuses the stored token. It excludes the other auth
  styles, and an expired token surfaces the 401 with "run brokoli auth
  again" rather than re-prompting mid-request.
- Servers without the grant (any open-source server, or an enterprise
  server from before the feature) are refused up front naming the
  alternatives -- the same capability-honesty rule as deploy
  preflight.

## 0.6.0 - 2026-08-29

### Changed

- **`Pipeline(catch_up=...)` is real now** (#71). It joined the
  serialize-or-reject refusal list when the server had no field for it;
  the server grew per-interval catch-up (Brokoli core ADR-028, shipped
  in v0.10.78), so the honest state changed: `catch_up=True` compiles to
  the pipeline-level `catchup` field (omitted unless set -- older
  fail-closed decoders 400 on unknown fields), requires a `schedule` at
  construction, and deploy preflight requires the server to advertise
  the `data_intervals` execution feature -- servers that advertise
  features but not this one refuse with a message naming it, before
  their strict decoder could 400 opaquely. `max_retries` and
  `concurrency` stay refused. Code that previously CAUGHT the
  `catch_up` refusal will no longer see it against any server; that
  behavior change is this release's minor bump.

### Fixed

- **`Run.wait()` no longer treats the first-poll 404 as fatal** (#73,
  fixes #72). A server that dispatches runs asynchronously can answer
  the trigger with the run id a moment before the run row is readable,
  and the deliberately fast 50ms first poll won that race. For
  `visibility_grace` seconds (default 2) a 404 from the status poll
  means "not visible yet"; after the window it raises exactly as
  before. The async client's `watch()` had the same race and gets the
  same treatment. Found live against a running server.

### Added

- **`sink_db(truncate=True)`** clears an `overwrite` with `TRUNCATE`
  instead of `DELETE`. A DELETE leaves one dead row per row removed, so a
  table replaced on every run sits at a multiple of its real size and each
  clear scans the leftovers. Ten overwrite cycles of a 50,000-row table on
  Postgres 16: `DELETE` 7013ms / 500,000 dead rows / 137 MB against
  `TRUNCATE` 2591ms / 0 dead rows / 13 MB.

  Off by default, and not a free win: TRUNCATE takes a lock that blocks
  anything *reading* the table for the whole load, where DELETE lets
  readers keep seeing the previous contents until the write commits. Turn
  it on for a table nothing queries mid-load; leave it off for one backing
  a dashboard. Ignored on SQLite, which has no TRUNCATE and already
  optimises a whole-table DELETE into the same thing.

  Requires a server with the matching support.

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
