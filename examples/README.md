# Brokoli SDK — Examples & Support Matrix

Each example is a complete, deployable pipeline. Validate any of them
without a server:

```bash
brokoli compile examples/01_hello_world.py --check
```

Deploy one to a running Brokoli server:

```bash
brokoli deploy examples/01_hello_world.py --url https://your-server --token $BROKOLI_TOKEN
```

All examples in this directory pass `compile --check` in CI.

## Examples

| File | What it demonstrates |
| --- | --- |
| `01_hello_world.py` | `@source`, `@map`, `@filter`, `sink_file`, `>>` chaining |
| `02_api_to_database.py` | `source_api`, `@validate`, `transform` (built-in rules), `sink_db` |
| `03_join_and_quality.py` | `source_db` ×2, `join`, `quality_check`, custom scoring, fan-out to db + file |
| `04_dbt_with_alerts.py` | `source_db`, `dbt`, `condition_node`, `notify`, success/failure branching |
| `05_sensor_custom_pipeline.py` | `@sensor`, fully custom `@source`/`@sink` — zero built-in nodes |
| `06_pagination.py` | `source_api(pagination=…)`, `numbered_pages` (+ `offset_pages`, `cursor_pages`, `next_link_pages`, `link_header_pages` documented inline) |
| `07_union_and_parallel.py` | `parallel` fan-out, `union` fan-in, `transform` |
| `gbif_biodiversity_monitor.py` | Real-world: paginated ingestion, normalization, multi-report publishing |
| `global_earthquake_watch.py` | Real-world: live GeoJSON feed, validation, CSV + JSON exports |

## Feature → example index

Every public building block, and where to see it run:

| Feature | Example |
| --- | --- |
| `@source` / `source_api` / `source_db` / `source_file` | 01, 02, 03, 05, 06 |
| `@sink` / `sink_api` / `sink_db` / `sink_file` | 01, 02, 03, 05 |
| `@map`, `@filter` | 01 |
| `@validate`, `quality_check` | 02, 03 |
| `@sensor` | 05 |
| `transform` (rename/drop/filter/… rules) | 02, 07 |
| `join` | 03 |
| `union`, `parallel` | 07 |
| `dbt` | 04 |
| `condition_node`, `notify` | 04 |
| Pagination (`numbered_pages`, `offset_pages`, `cursor_pages`, `next_link_pages`, `link_header_pages`) | 06 |
| `>>` chaining / DAG assembly | all |

## Operating deployed pipelines

Once a pipeline is deployed, trigger and observe runs from the CLI —
no need to leave the terminal for the console:

```bash
brokoli run orders --server ...                       # trigger (async)
brokoli run orders --server ... --param date=2026-08-11
brokoli status <run-id> --server ...                  # check a run
brokoli logs   <run-id> --server ... [--level error] [--node <id>]
brokoli cancel <run-id> --server ...                  # stop an in-progress run
brokoli retry  <run-id> --server ...                  # resume from where it failed
brokoli backfill orders --start 2026-01-01 --end 2026-01-07 --server ...
```

`run` and `backfill` accept the pipeline's logical id, its name, or the
server's internal id. `retry` resumes a failed run, preserving successful
nodes rather than starting over. Auth via `--api-key` or the
`BROKOLI_TOKEN` env var.

`deploy` prints a stable **IR digest** (`sha256:…`) of exactly what it
deployed — the same content digests identically whether it's a create or
an update, so a redeploy that changed nothing is visible as an unchanged
digest. `brokoli compile <file> --digest` prints the same digest without a
server, for CI to capture and diff across builds.

### Named environments

Define environments once in a `brokoli.yaml` (or point `BROKOLI_CONFIG` at
one) and target them with `--env`, instead of repeating `--server`/`--api-key`:

```yaml
environments:
  dev:  { server: http://localhost:8080 }
  prod: { server: https://prod.example.com, token_env: BROKOLI_PROD_TOKEN }
```

```bash
brokoli deploy pipe.py --env prod        # server + token from the config
brokoli run orders --env prod            # any server command takes --env
```

The token is read from the env var named by `token_env` (never stored in
the file). An explicit `--server`/`BROKOLI_TOKEN` always overrides the
environment.

## Authoring vs. run-time side effects

`compile`, `validate`, and `deploy` **import your file** to discover its
pipelines, so the `with Pipeline(...)` block runs. Two rules keep that
import free of run-time side effects:

- Your file is imported under its own module name, never `__main__` — so
  put any deploy/run code under `if __name__ == "__main__":` and it won't
  fire during discovery.
- `BROKOLI_DISCOVERY` is set in the environment during discovery. Guard
  expensive run-time-only setup on it:

  ```python
  import os
  if not os.getenv("BROKOLI_DISCOVERY"):
      client = connect_to_warehouse()   # skipped during compile/deploy
  ```

## Local testing (no server)

`brokoli.testing` lets you assert graph shape and task logic without
deploying. It *inspects* a compiled pipeline and *calls* your task
functions in isolation — it does not run the DAG (that's the engine's
job).

```python
from brokoli.testing import graph, run_task, assert_stable_ir

def test_shape():
    g = graph(build_pipeline())
    g.assert_nodes("Fetch", "Clean", "Load")
    g.assert_edge("Fetch", "Clean")
    assert g.kind("Fetch") == "source_api"

def test_task_logic():
    # @task/@map/... wrappers, or a plain function — call the real logic
    assert run_task(my_clean_task, [{"raw": 1}]) == [{"value": 1}]

def test_deterministic():
    assert_stable_ir(build_pipeline)   # recompiling must not churn the IR
```

`graph()` exposes `node_names`, `edges`, `config()`, `kind()`,
`upstream()`/`downstream()`, and `has_edge()`; `ir_snapshot()` returns a
canonical IR string for golden-file regression tests.

## Python support

The SDK supports **CPython 3.9 – 3.13** (tested on every minor version in CI).

| Python | Status | Notes |
| --- | --- | --- |
| 3.9 | ✅ Supported | `requires_modules` auto-declaration is skipped — it needs `sys.stdlib_module_names` (3.10+). Everything else is identical. |
| 3.10 | ✅ Supported | Full feature set. |
| 3.11 | ✅ Supported | Full feature set. |
| 3.12 | ✅ Supported | Full feature set. |
| 3.13 | ✅ Supported | Full feature set. |

The package ships a PEP 561 `py.typed` marker, so `import brokoli` is
type-checked in your project.

## Server compatibility

The SDK negotiates against the server's `GET /api/capabilities` on
`deploy`/`diff`, so it adapts to what the target server supports rather
than assuming. Rules of thumb:

- **IR 2.0** — core pipeline shapes (sources, sinks, transforms, joins,
  quality, sensors, pagination). Supported by all current servers.
- **IR 2.1** — conditional routing (`condition_node` branching). Requires
  a server advertising IR 2.1 in `supported_ir_versions`.
- **Execution-feature gating** — features the server lists in
  `supported_execution_features` are honored; anything the SDK emits that
  the server can't run is **refused at preflight, naming the feature**,
  instead of being silently dropped. Against older servers that don't
  advertise the field, gating is skipped for backward compatibility.
- **`union` runtime** — the SDK compiles a `union` node today, but the
  server-side manifest merge is not implemented yet (brokoli-sdk#2). The
  node deploys and validates; it starts concatenating once the backend
  lands it.

When in doubt, `brokoli diff` shows exactly what would change on a
specific server before you deploy.
