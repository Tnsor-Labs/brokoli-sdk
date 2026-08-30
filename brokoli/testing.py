"""Local test harness -- assert graph shape and task logic without a server.

brokoli-sdk#15 M2. These helpers *inspect* a compiled pipeline and *call*
your task functions in isolation. They deliberately do **not** run the DAG
or emulate the engine: executing a pipeline is the Go runtime's job, never
a Python approximation of it. What you get:

* :class:`Graph` / :func:`graph` -- assert nodes, edges, and config
  without deploying.
* :func:`run_task` -- unit-test a task's Python logic on sample input.
* :func:`ir_snapshot` -- a canonical IR string for golden-file tests.
* :func:`assert_stable_ir` -- prove a builder recompiles to identical IR.

Typical use::

    from brokoli import Pipeline, source_api, transform, sink_db
    from brokoli.testing import graph, run_task, assert_stable_ir

    def build():
        with Pipeline("orders", pipeline_id="orders") as p:
            src = source_api("Fetch", url="https://api/orders")
            clean = transform("Clean", input=src,
                              rules=[{"type": "drop_columns", "columns": ["raw"]}])
            src >> clean >> sink_db("Load", table="orders", conn_id="dw")
        return p

    def test_shape():
        g = graph(build())
        g.assert_nodes("Fetch", "Clean", "Load")
        g.assert_edge("Fetch", "Clean")
        assert g.kind("Fetch") == "source_api"

    def test_stable():
        assert_stable_ir(build)   # recompiling unchanged source must not churn IR
"""

from __future__ import annotations

from typing import Any, Callable

from brokoli.ir import canonical_json, diff_ir, normalize_ir
from brokoli.pipeline import Pipeline

__all__ = ["Graph", "graph", "run_task", "ir_snapshot", "assert_stable_ir"]


class Graph:
    """Read-only view of a compiled pipeline's nodes and edges.

    Edges are exposed by node *name* rather than internal id, so assertions
    read the way the pipeline was written. Look nodes up by either name or
    id; when two nodes share a name, name lookup resolves to the first one
    registered (ids are always unambiguous).
    """

    def __init__(self, pipeline: Pipeline) -> None:
        ir = pipeline.to_json()
        self._nodes: dict[str, dict[str, Any]] = {n["id"]: n for n in ir["nodes"]}
        self._id_by_name: dict[str, str] = {}
        for node in ir["nodes"]:
            self._id_by_name.setdefault(node["name"], node["id"])
        self._raw_edges: list[tuple[str, str]] = [
            (e["from"], e["to"]) for e in ir.get("edges", [])
        ]

    def _resolve(self, key: str) -> str:
        if key in self._nodes:
            return key
        if key in self._id_by_name:
            return self._id_by_name[key]
        raise KeyError(
            f"no node named or id'd {key!r}; nodes are {sorted(self.node_names)}"
        )

    @property
    def node_names(self) -> list[str]:
        return [n["name"] for n in self._nodes.values()]

    @property
    def ids(self) -> list[str]:
        return list(self._nodes)

    @property
    def edges(self) -> list[tuple[str, str]]:
        """Edges as ``(from_name, to_name)`` tuples."""
        name_of = {i: n["name"] for i, n in self._nodes.items()}
        return [(name_of[a], name_of[b]) for a, b in self._raw_edges]

    def node(self, key: str) -> dict[str, Any]:
        """The full node dict, by name or id."""
        return self._nodes[self._resolve(key)]

    def config(self, key: str) -> dict[str, Any]:
        """The node's ``config`` dict, by name or id."""
        return self.node(key).get("config", {})

    def kind(self, key: str) -> str:
        """The node's IR ``type`` (e.g. ``source_api``), by name or id."""
        return self.node(key)["type"]

    def has_edge(self, src: str, dst: str) -> bool:
        return (src, dst) in self.edges

    def downstream(self, key: str) -> list[str]:
        """Names of nodes directly downstream of *key*."""
        name = self.node(key)["name"]
        return [d for s, d in self.edges if s == name]

    def upstream(self, key: str) -> list[str]:
        """Names of nodes directly upstream of *key*."""
        name = self.node(key)["name"]
        return [s for s, d in self.edges if d == name]

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, key: str) -> bool:
        try:
            self._resolve(key)
            return True
        except KeyError:
            return False

    # -- assertions: raise AssertionError with the actual state on failure --

    def assert_nodes(self, *names: str) -> None:
        """Assert the node-name set is exactly *names*."""
        actual, expected = set(self.node_names), set(names)
        if actual != expected:
            raise AssertionError(
                f"node set mismatch -- missing {sorted(expected - actual)}, "
                f"unexpected {sorted(actual - expected)}"
            )

    def assert_edge(self, src: str, dst: str) -> None:
        """Assert a direct edge ``src -> dst`` (by name) exists."""
        if not self.has_edge(src, dst):
            raise AssertionError(
                f"expected edge {src!r} -> {dst!r}; edges are {self.edges}"
            )


def graph(pipeline: Pipeline) -> Graph:
    """Build a read-only :class:`Graph` view of *pipeline*."""
    return Graph(pipeline)


def run_task(task: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a task's underlying Python function directly and return its result.

    Accepts any decorated wrapper (``@task``/``@source``/``@map``/...) or a
    plain function, and invokes the *real* function -- bypassing node
    registration -- so you can assert on its output. This tests your logic,
    not the engine; mock whatever I/O the function performs with
    :mod:`unittest.mock` as usual.
    """
    fn = getattr(task, "__wrapped__", task)
    if not callable(fn):
        raise TypeError(
            "run_task expected a callable or a brokoli task wrapper, got "
            f"{type(task).__name__}"
        )
    return fn(*args, **kwargs)


def ir_snapshot(pipeline: Pipeline) -> str:
    """A canonical, normalized IR string -- stable across unrelated edits.

    Ideal as a golden-file snapshot: store it once, and any *semantic*
    change to the pipeline shows up as a readable diff while cosmetic
    churn (dict ordering, layout positions) does not.
    """
    return canonical_json(normalize_ir(pipeline.to_json()))


def assert_stable_ir(builder: Callable[[], Pipeline]) -> None:
    """Assert that *builder* recompiles to identical IR.

    *builder* is a zero-arg callable that constructs and returns a fresh
    pipeline; it is called twice and the two normalized snapshots must
    match. This makes the SDK's determinism contract -- stable node ids and
    ordering, so unchanged source produces an unchanged semantic diff --
    testable in your own suite.
    """
    first = normalize_ir(builder().to_json())
    second = normalize_ir(builder().to_json())
    if first != second:
        raise AssertionError(
            "pipeline IR is not stable across rebuilds:\n"
            + diff_ir(
                second, first,
                local_label="rebuild-2", remote_label="rebuild-1",
            )
        )
