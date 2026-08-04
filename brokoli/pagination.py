"""Pagination-strategy and execution-policy config builders for ``source_api``.

These are pure IR-config producers -- calling one of the ``*_pages()``
functions below returns a small immutable object whose ``.to_config()``
serializes into the ``pagination`` block of a ``source_api`` node's
``config``. Chaining ``.with_execution(...)`` attaches a sibling
``execution`` block describing concurrency/rate-limit/retry/checkpoint
policy (RFC V2 §14.4).

Scope note: this module produces *declarative config only*. Expanding a
paginated source into concrete per-page fetch instances, running them
under the configured concurrency/rate-limit policy, retrying individual
pages, and stitching results back together is backend (physical-planner)
work that is not implemented here and, as of this writing, has not been
started on the Go side either. Nothing in this module performs HTTP
requests or loops over pages.
"""

from __future__ import annotations

from typing import Any

from brokoli.sentinel import UNSET

# Single source of truth for valid strategy names -- also consulted by
# brokoli.validation to reject unknown/misspelled strategies.
VALID_PAGINATION_STRATEGIES: frozenset[str] = frozenset(
    {"offset", "cursor", "numbered", "next_link", "link_header"}
)


def _filtered(fields: dict[str, Any]) -> dict[str, Any]:
    """Drop UNSET/None entries, keeping meaningful falsy values (0, "", False).

    Mirrors the UNSET-vs-None-vs-omitted discipline established in
    ``brokoli.nodes._build_config`` -- e.g. ``numbered_pages(start=0)``
    must survive serialization, not be dropped by a truthy check.
    """
    return {k: v for k, v in fields.items() if v is not UNSET and v is not None}


class ExecutionPolicy:
    """Concurrency/rate-limit/retry/checkpoint policy for a paginated source.

    Produced by :meth:`PaginationStrategy.with_execution`, not
    constructed directly. Serializes into the ``execution`` block of a
    ``source_api`` node's config, a sibling of ``pagination``.
    """

    def __init__(
        self,
        max_concurrency: Any = UNSET,
        requests_per_second: Any = UNSET,
        retry_scope: Any = UNSET,
        checkpoint_every: Any = UNSET,
    ) -> None:
        self._fields = {
            "max_concurrency": max_concurrency,
            "requests_per_second": requests_per_second,
            "retry_scope": retry_scope,
            "checkpoint_every": checkpoint_every,
        }

    def to_config(self) -> dict[str, Any]:
        """Return the ``execution`` config block."""
        return _filtered(self._fields)


class PaginationStrategy:
    """A pagination strategy's declarative config, plus an optional execution policy.

    Not constructed directly -- use one of the builder functions below
    (``offset_pages``, ``cursor_pages``, ``numbered_pages``,
    ``next_link_pages``, ``link_header_pages``).
    """

    def __init__(self, strategy: str, **fields: Any) -> None:
        self.strategy = strategy
        self._fields = fields
        self._execution: ExecutionPolicy | None = None

    def with_execution(
        self,
        max_concurrency: Any = UNSET,
        requests_per_second: Any = UNSET,
        retry_scope: Any = UNSET,
        checkpoint_every: Any = UNSET,
    ) -> "PaginationStrategy":
        """Attach an execution policy, per the RFC's chained ``offset_pages(...).with_execution(...)`` style.

        Returns a new :class:`PaginationStrategy` (the original is left
        untouched) so a strategy object can safely be built once and
        reused with different execution policies across sources.
        """
        new = PaginationStrategy(self.strategy, **self._fields)
        new._execution = ExecutionPolicy(
            max_concurrency=max_concurrency,
            requests_per_second=requests_per_second,
            retry_scope=retry_scope,
            checkpoint_every=checkpoint_every,
        )
        return new

    def to_config(self) -> dict[str, Any]:
        """Return the ``pagination`` config block, e.g. ``{"strategy": "offset", ...}``."""
        config: dict[str, Any] = {"strategy": self.strategy}
        config.update(_filtered(self._fields))
        return config

    def execution_config(self) -> dict[str, Any] | None:
        """Return the attached ``execution`` config block, or ``None`` if unset."""
        if self._execution is None:
            return None
        return self._execution.to_config()

    def __repr__(self) -> str:
        return f"PaginationStrategy({self.strategy!r}, {self._fields!r})"


def offset_pages(
    page_size: Any,
    max_records: Any = UNSET,
    offset_param: str = "offset",
    limit_param: str = "limit",
    end_flag: Any = UNSET,
) -> PaginationStrategy:
    """Offset/limit pagination -- ``?offset=N&limit=page_size``.

    Args:
        page_size: Number of records requested per page.
        max_records: Stop once this many total records have been fetched.
        offset_param: Query-string param name carrying the offset.
        limit_param: Query-string param name carrying the page size.
        end_flag: Dot-path into the JSON body of a boolean flag that,
            when true, signals the last page (e.g. GBIF's
            ``"endOfRecords"``).

    Example::

        offset_pages(page_size=300, max_records=30_000, end_flag="endOfRecords")
    """
    return PaginationStrategy(
        "offset",
        page_size=page_size,
        max_records=max_records,
        offset_param=offset_param,
        limit_param=limit_param,
        end_flag=end_flag,
    )


def cursor_pages(cursor_path: str, cursor_param: str) -> PaginationStrategy:
    """Cursor-based pagination -- next page's cursor comes from the response body.

    Args:
        cursor_path: Dot-path into the JSON body where the next-page
            cursor value is found (e.g. ``"meta.next_cursor"``).
        cursor_param: Query-string param name used to send the cursor
            back on the next request.
    """
    return PaginationStrategy("cursor", cursor_path=cursor_path, cursor_param=cursor_param)


def numbered_pages(
    page_param: str,
    start: Any = 1,
    total_pages_path: Any = UNSET,
) -> PaginationStrategy:
    """Numbered-page pagination -- ``?page=1``, ``?page=2``, ...

    Args:
        page_param: Query-string param name carrying the page number.
        start: First page number (``0`` for zero-indexed APIs -- a
            meaningful, non-default value that must survive
            serialization, not just the default ``1``).
        total_pages_path: Dot-path into the JSON body for the total
            page count, used to know when to stop.
    """
    return PaginationStrategy(
        "numbered",
        page_param=page_param,
        start=start,
        total_pages_path=total_pages_path,
    )


def next_link_pages(next_path: str) -> PaginationStrategy:
    """Follow a "next page" URL embedded in the JSON response body.

    Args:
        next_path: Dot-path into the JSON body where the next-page URL
            is found (e.g. ``"paging.next"``). An absent/null value at
            this path signals the last page.
    """
    return PaginationStrategy("next_link", next_path=next_path)


def link_header_pages(rel: str = "next") -> PaginationStrategy:
    """Follow pagination via an HTTP ``Link`` response header (RFC 8288).

    Args:
        rel: The ``rel`` value identifying the next-page link
            (``Link: <...>; rel="next"``).
    """
    return PaginationStrategy("link_header", rel=rel)
