"""retry_delay is milliseconds; timeout on the same node is seconds.

Nothing said so, and the natural Python reading of retry_delay=1 is one
second. It became one millisecond, which disables the backoff and
retries a failing upstream as fast as the network allows — measured on a
real run: three attempts inside 0.4s against an endpoint that was
returning 503.
"""

from __future__ import annotations

from brokoli import Pipeline, source_api, sink_file
from brokoli.validation import validate_pipeline


def _pipeline(**retry_kwargs):
    with Pipeline("retry_units", schedule=None) as p:
        rows = source_api("Fetch", url="https://api.example.com/data", **retry_kwargs)
        sink_file("Land", input=rows, path="/tmp/out.json", format="json")
    return p


def _errors_for(result, key):
    return [e for e in result.errors if getattr(e, "field", None) == key]


def test_seconds_shaped_delay_is_rejected():
    result = validate_pipeline(_pipeline(retries=3, retry_delay=1))
    errs = _errors_for(result, "retry_delay")
    assert errs, "expected retry_delay=1 to be flagged as a unit mistake"
    assert "millisecond" in str(errs[0]).lower()
    assert "1000" in str(errs[0]), "the message should suggest the intended value"


def test_millisecond_shaped_delay_is_accepted():
    result = validate_pipeline(_pipeline(retries=3, retry_delay=1000))
    assert not _errors_for(result, "retry_delay")


def test_zero_delay_is_left_alone():
    """Zero is an explicit 'no wait', not a unit slip."""
    result = validate_pipeline(_pipeline(retries=3, retry_delay=0))
    assert not _errors_for(result, "retry_delay")


def test_negative_delay_is_rejected():
    result = validate_pipeline(_pipeline(retries=2, retry_delay=-5))
    assert _errors_for(result, "retry_delay")


def test_small_delay_without_retries_is_not_flagged():
    """With no retries configured the delay is inert, so do not nag."""
    result = validate_pipeline(_pipeline(retry_delay=1))
    assert not _errors_for(result, "retry_delay")


def test_delay_absent_is_fine():
    result = validate_pipeline(_pipeline(retries=3))
    assert not _errors_for(result, "retry_delay")
