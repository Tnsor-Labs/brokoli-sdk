"""Structured result types for @task functions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskResult:
    """Structured result from a @task function.

    Provides typed access to output data, warnings, errors, and metadata.
    The engine uses ``to_rows()`` to extract data for downstream nodes.

    Example::

        from brokoli import task
        from brokoli.result import TaskResult

        @task
        def clean_orders(rows: list[dict]) -> TaskResult:
            clean = [r for r in rows if r.get("amount", 0) > 0]
            skipped = len(rows) - len(clean)
            return TaskResult(
                data=clean,
                warnings=[f"Skipped {skipped} rows with non-positive amount"] if skipped else [],
                metadata={"skipped": skipped},
            )
    """

    data: list[dict] | Any = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_warnings(self) -> bool:
        """True if any warnings were recorded."""
        return len(self.warnings) > 0

    @property
    def has_errors(self) -> bool:
        """True if any errors were recorded."""
        return len(self.errors) > 0

    @property
    def row_count(self) -> int:
        """Number of rows in ``data``. Returns 0 for non-list data."""
        if not isinstance(self.data, list):
            return 0
        return len(self.data)

    def to_rows(self) -> list[dict]:
        """Convert data to a list of dicts for the Brokoli engine.

        Supports:
            - ``list[dict]`` — returned as-is
            - pandas ``DataFrame`` — converted via ``.to_dict("records")``

        Raises:
            TypeError: If ``data`` is not a supported type.
        """
        if isinstance(self.data, list):
            return self.data

        if hasattr(self.data, "to_dict"):
            return self.data.to_dict("records")

        raise TypeError(
            f"TaskResult.data must be list[dict] or DataFrame, got {type(self.data).__name__}. "
            f"Return a list of dicts from your @task function."
        )
