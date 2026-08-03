"""Shared UNSET sentinel.

Lives in its own module (rather than ``nodes.py``) so that ``pipeline.py``
can use it too without creating a circular import -- ``nodes.py`` and
``decorators.py`` both import from ``pipeline.py``.
"""

from __future__ import annotations


class _UnsetType:
    """Sentinel type distinguishing "not passed" from an explicit falsy value.

    ``retries=0`` and ``timeout=0`` are meaningful, valid values (e.g. "no
    retries", "no timeout") that must survive serialization. A plain
    ``if value:`` truthy check would silently drop them. Call sites should
    use ``value is not UNSET and value is not None`` instead.
    """

    _instance: "_UnsetType | None" = None

    def __new__(cls) -> "_UnsetType":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNSET"

    def __bool__(self) -> bool:
        return False


UNSET = _UnsetType()
