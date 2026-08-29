"""Brokoli Python SDK — define data pipelines in Python, deploy and run them visually."""

from brokoli.pipeline import (
    Pipeline,
    NodeRef,
    ConditionRef,
    ScalarRef,
    ArtifactRef,
    DatasetRef,
    CollectionRef,
)
from brokoli.decorators import (
    task,
    condition,
    source,
    sink,
    filter,
    map,
    validate,
    sensor,
)
from brokoli.result import TaskResult
from brokoli.parsing import ParseError
from brokoli.nodes import (
    source_db,
    source_api,
    source_file,
    transform,
    join,
    quality_check,
    sink_db,
    sink_file,
    sink_api,
    code,
    migrate,
    dbt,
    notify,
    condition_node,
    parallel,
    union,
)
from brokoli.pagination import (
    offset_pages,
    cursor_pages,
    numbered_pages,
    next_link_pages,
    link_header_pages,
)
from brokoli.resources import (
    Connection,
    ResourceRef,
    InterpolationRef,
    Secret,
    Variable,
    Param,
    EnvVar,
)
from brokoli.ir import canonical_json, diff_ir, ir_digest, normalize_ir, render_ir
from brokoli.client import APIError, AuthError, Client, Run, RunFailed, TERMINAL_RUN_STATUSES
from brokoli.async_client import AsyncClient, AsyncRun


# Single-sourced from the installed distribution (pyproject's version):
# every release since 0.4.0 shipped a --version that still said 0.4.0,
# because this string was hand-maintained and never bumped again. The
# ownership check matters: a source-tree import on a machine that ALSO
# has some other brokoli version installed must not report that other
# install's number -- only a distribution that actually provides this
# very file gets to name the version.
def _resolve_version() -> str:
    try:
        from importlib.metadata import distribution
        from pathlib import Path

        dist = distribution("brokoli")
        this_file = Path(__file__).resolve()
        for f in dist.files or []:
            if str(f).endswith("brokoli/__init__.py"):
                if Path(str(dist.locate_file(f))).resolve() == this_file:
                    return dist.version
                break
    except Exception:  # pragma: no cover - exotic packaging
        pass
    return "0.0.0.dev0"


__version__ = _resolve_version()
__all__ = [
    # Core
    "Pipeline",
    "TaskResult",
    "ParseError",
    # Typed node references (brokoli-sdk#2) -- see brokoli.pipeline. These are
    # authoring-time DATA refs: they point at another node's output.
    "NodeRef",
    "ConditionRef",
    "ScalarRef",
    "ArtifactRef",
    "DatasetRef",
    "CollectionRef",
    # Typed RESOURCE refs (brokoli-sdk#15 M4) -- see brokoli.resources. These
    # point at server-side resources, distinct from the data refs above.
    # Connection is a bare-name field; Secret/Variable/Param/EnvVar compile to
    # the engine's ${...} interpolation, resolved in node configs at run time.
    "Connection",
    "ResourceRef",
    "InterpolationRef",
    "Secret",
    "Variable",
    "Param",
    "EnvVar",
    # Decorators
    "task",
    "condition",
    "source",
    "sink",
    "filter",
    "map",
    "validate",
    "sensor",
    # Built-in sources
    "source_db",
    "source_api",
    "source_file",
    # Built-in processing
    "transform",
    "join",
    "quality_check",
    "code",
    # Built-in sinks
    "sink_db",
    "sink_file",
    "sink_api",
    # Built-in integrations
    "dbt",
    "notify",
    "migrate",
    "condition_node",
    "parallel",
    # Dataset-manifest combination (brokoli-sdk#2) -- see also
    # CollectionRef.collect(mode="union")
    "union",
    # source_api pagination DSL (declarative config only -- see brokoli.pagination)
    "offset_pages",
    "cursor_pages",
    "numbered_pages",
    "next_link_pages",
    "link_header_pages",
    # Normalized comparison artifacts
    "normalize_ir",
    "canonical_json",
    "render_ir",
    "diff_ir",
    "ir_digest",
    # Run-ops client (brokoli-sdk#57) -- fire, wait on, cancel, and read
    # runs programmatically; deploy without shelling out to the CLI.
    "Client",
    "Run",
    "APIError",
    "AuthError",
    "RunFailed",
    "TERMINAL_RUN_STATUSES",
    # Async counterpart (brokoli-sdk#57 item 8) -- same operations, plus a
    # genuinely push-based Run.watch()/wait() over SODP instead of polling
    # (requires the "watch" extra: pip install "brokoli[watch]").
    "AsyncClient",
    "AsyncRun",
]
