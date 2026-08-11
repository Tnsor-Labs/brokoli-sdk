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
