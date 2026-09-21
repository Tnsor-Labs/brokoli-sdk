"""Portable execution profiles for ``source_api``.

Profiles are expanded at authoring time.  The profile marker is retained for
explainability, but the emitted config contains every effective value so a
different SDK or server does not need Python-specific defaults.
"""

from __future__ import annotations

from typing import Any

from brokoli.sentinel import UNSET

PROFILE_VERSION = 1


def _profile(name: str, *, strict: bool, **values: Any) -> dict[str, Any]:
    return {
        "profile": {"name": name, "version": PROFILE_VERSION, "strict": strict},
        **{key: value for key, value in values.items() if value is not UNSET},
    }


def public_api_safe(
    *, requests_per_second: float = 2, max_concurrency: int = 2,
    checkpoint_every: int = 10,
) -> dict[str, Any]:
    """Bounded, retrying, resumable policy suitable for public APIs."""
    return _profile(
        "public_api_safe", strict=False,
        timeout=30, max_retries=3, retry_backoff="exponential", retry_delay=1000,
        max_concurrency=max_concurrency, requests_per_second=requests_per_second,
        retry_scope="page", checkpoint_every=checkpoint_every,
        page_max_retries=3, page_retry_backoff="exponential",
    )


def high_throughput(
    *, requests_per_second: float, max_concurrency: int = 8,
    checkpoint_every: int = 25,
) -> dict[str, Any]:
    """Higher parallelism with an explicit caller-provided rate limit."""
    return _profile(
        "high_throughput", strict=False,
        timeout=30, max_retries=2, retry_backoff="exponential", retry_delay=500,
        max_concurrency=max_concurrency, requests_per_second=requests_per_second,
        retry_scope="page", checkpoint_every=checkpoint_every,
        page_max_retries=2, page_retry_backoff="exponential",
    )


def strict(
    *, requests_per_second: float = 1, checkpoint_every: int = 10,
) -> dict[str, Any]:
    """Conservative policy that refuses ineffective or unavailable settings."""
    return _profile(
        "strict", strict=True,
        timeout=30, max_retries=2, retry_backoff="exponential", retry_delay=1000,
        max_concurrency=1, requests_per_second=requests_per_second,
        retry_scope="page", checkpoint_every=checkpoint_every,
        page_max_retries=2, page_retry_backoff="exponential",
    )
