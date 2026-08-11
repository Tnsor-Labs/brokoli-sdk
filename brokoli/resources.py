"""Typed references to server-side resources (brokoli-sdk#15 M4).

A *resource reference* points at something configured on the **server** —
today, a named connection — that the server resolves at run time. It is
deliberately a different kind of thing from the authoring-time *data*
references in :mod:`brokoli.pipeline` (``DatasetRef``, ``ArtifactRef``,
``ScalarRef``, ``CollectionRef``), which point at another node's output
*inside the DAG*:

* a **data ref** names something the pipeline *produces* (a node's output);
* a **resource ref** names something the pipeline *depends on* (a connection
  the operator configured on the server).

Keeping them distinct in type and name is the point of this module — you
cannot accidentally wire a ``Connection`` as a node input, or a
``DatasetRef`` as a connection.

Passing ``Connection("warehouse")`` where a ``conn_id`` is accepted is
equivalent to passing the string ``"warehouse"`` -- it compiles to the same
wire value -- but it is a distinct type a checker can verify, it validates
its name at construction, and it documents intent at the call site::

    source_db("Extract", query="...", conn_id=Connection("warehouse"))
"""

from __future__ import annotations

import re

__all__ = ["ResourceRef", "Connection"]

# Resource names are operator-chosen identifiers; keep them to the safe set
# the server and connection UI already use, so an invalid name is caught at
# authoring time rather than as a lookup miss at deploy.
_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


class ResourceRef:
    """Base for a typed reference to a named server-side resource.

    Subclasses set :attr:`kind`. The reference compiles to its bare name on
    the wire (:meth:`ir_value`), so it is a drop-in for the string it
    replaces while being a distinct authoring-time type.
    """

    kind = "resource"

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"{type(self).__name__} name must be a non-empty string"
            )
        if not _NAME_RE.match(name):
            raise ValueError(
                f"{type(self).__name__} name {name!r} may contain only letters, "
                "digits, dot, dash, and underscore"
            )
        self.name = name

    def ir_value(self) -> str:
        """The wire value this reference compiles to (its bare name)."""
        return self.name

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"

    def __eq__(self, other: object) -> bool:
        # Distinct types never compare equal -- a Connection is not the
        # string "warehouse", and not a Secret of the same name.
        return type(self) is type(other) and self.name == other.name  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self).__name__, self.name))


class Connection(ResourceRef):
    """A typed reference to a named connection configured on the server.

    Use anywhere a ``conn_id`` is accepted (``source_db``, ``sink_db``,
    ``source_api``, ``sink_api``, ``migrate``)::

        raw = source_db("Extract", query="...", conn_id=Connection("warehouse"))

    Compiles to the connection name on the wire, so it is interchangeable
    with the plain string form and validated the same way at deploy.
    """

    kind = "connection"
