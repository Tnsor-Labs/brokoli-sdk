"""Mapping a node over an upstream collection (ADR-050 phase 1).

``map=Map(...)`` on ``code()``, ``source_db()``, ``source_api()`` or a
``@task`` call runs that node once per row of an upstream node, each row
its own instance with its own status, instead of once over the whole
input. It compiles to the node's ``map`` block::

    customers = source_db("Customers", query="SELECT id FROM customers", conn_id="shop")
    orders = source_db(
        "Orders",
        query="SELECT * FROM orders WHERE customer_id = ${item.id}",
        conn_id="shop",
        map=Map(over=customers, key="id", max_parallel=8),
    )

Not to be confused with the ``@map`` decorator, which applies a function
to every row inside one node: ``Map`` runs the whole node once per row.

The server decides the limits (``max_items`` and ``max_parallel`` have
operator-set defaults and ceilings) and refuses a block it cannot honour
when the pipeline is saved. A server from before mapping would ignore the
block, so deploy refuses a pipeline that maps unless the server advertises
``node-map``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Optional, Union

from brokoli.exceptions import PipelineError

if TYPE_CHECKING:
    from brokoli.pipeline import NodeRef

ON_ITEM_FAILURE = ("fail", "continue", "skip")


def _positive_int(name: str, value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PipelineError(f"Map({name}=...) must be a whole number of at least 1, got {value!r}")
    return value


@dataclass(frozen=True)
class Map:
    """Run a node once per item of an upstream collection.

    Args:
        over: The node whose output rows are the items. To zip several
            collections item by item, a dict of name to node: each item is
            that position's row from each, merged, and every collection
            must have the same number of rows.
        key: The item column whose value identifies the item, so a retry
            reruns the same items and the run shows them by name. Values
            must be present and unique. Without it, items are identified
            by position.
        max_items: The most items the node may fan out into; more fail the
            node. Server default 1000.
        max_parallel: How many items run at once. Server default 4.
        on_item_failure: ``"fail"`` (the default) stops at the first failed
            item, ``"continue"`` runs every item and then fails if any did,
            ``"skip"`` leaves failed items out of the output (and still
            fails when none succeeded).
    """

    over: Union["NodeRef", Mapping[str, "NodeRef"]]
    key: Optional[str] = None
    max_items: Optional[int] = None
    max_parallel: Optional[int] = None
    on_item_failure: Optional[str] = None

    def __post_init__(self) -> None:
        from brokoli.pipeline import NodeRef

        over = self.over
        if isinstance(over, NodeRef):
            pass
        elif isinstance(over, Mapping):
            if not over:
                raise PipelineError("Map(over={}) needs at least one collection")
            for name, ref in over.items():
                if not isinstance(name, str) or not name:
                    raise PipelineError(
                        f"Map(over=...) collection names must be non-empty strings, got {name!r}"
                    )
                if not isinstance(ref, NodeRef):
                    raise PipelineError(
                        f"Map(over={{{name!r}: ...}}) must name an upstream node, got {type(ref).__name__}"
                    )
            object.__setattr__(self, "over", dict(over))
        else:
            raise PipelineError(
                f"Map(over=...) must be an upstream node or a dict of name to node, got {type(over).__name__}"
            )
        if self.key is not None and (not isinstance(self.key, str) or not self.key.strip()):
            raise PipelineError(f"Map(key=...) must be a column name, got {self.key!r}")
        _positive_int("max_items", self.max_items)
        _positive_int("max_parallel", self.max_parallel)
        if self.on_item_failure is not None and self.on_item_failure not in ON_ITEM_FAILURE:
            raise PipelineError(
                f"Map(on_item_failure=...) must be one of {', '.join(ON_ITEM_FAILURE)}, got {self.on_item_failure!r}"
            )

    def refs(self) -> list["NodeRef"]:
        """The upstream nodes the items come from, each needing an edge."""
        from brokoli.pipeline import NodeRef

        if isinstance(self.over, NodeRef):
            return [self.over]
        return list(self.over.values())  # type: ignore[union-attr]

    def to_config(self) -> dict[str, Any]:
        """The node's ``map`` block, as the IR spells it."""
        from brokoli.pipeline import NodeRef

        if isinstance(self.over, NodeRef):
            over: Any = self.over.node_id
        else:
            over = {name: ref.node_id for name, ref in self.over.items()}  # type: ignore[union-attr]
        block: dict[str, Any] = {"over": over}
        if self.key is not None:
            block["key"] = self.key
        if self.max_items is not None:
            block["max_items"] = self.max_items
        if self.max_parallel is not None:
            block["max_parallel"] = self.max_parallel
        if self.on_item_failure is not None:
            block["on_item_failure"] = self.on_item_failure
        return block


def prepare_map(pipeline: Any, config: dict[str, Any], spec: Optional[Map]) -> list["NodeRef"]:
    """Check ``spec`` against the pipeline and put its ``map`` block in
    ``config``, before the node is registered, and return the upstream
    nodes that need an edge to it (the server requires one from each).
    No spec, no change."""
    if spec is None:
        return []
    if not isinstance(spec, Map):
        raise PipelineError(f"map=... must be a brokoli.Map, got {type(spec).__name__}")
    if "expansion" in config:
        raise PipelineError("A node may have map or expansion, not both")
    refs = spec.refs()
    pipeline._validate_refs(refs)
    config["map"] = spec.to_config()
    return refs
