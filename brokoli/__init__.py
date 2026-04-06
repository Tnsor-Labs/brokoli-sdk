"""Brokoli Python SDK — define data pipelines in Python, deploy and run them visually."""

from brokoli.pipeline import Pipeline
from brokoli.decorators import (
    task, condition,
    source, sink,
    filter, map,
    validate, sensor,
)
from brokoli.result import TaskResult
from brokoli.parsing import ParseError
from brokoli.nodes import (
    source_db, source_api, source_file,
    transform, join, quality_check,
    sink_db, sink_file, sink_api, code,
    migrate, dbt, notify, condition_node,
    parallel,
)

__version__ = "0.2.0"
__all__ = [
    # Core
    "Pipeline", "TaskResult", "ParseError",
    # Decorators
    "task", "condition", "source", "sink", "filter", "map", "validate", "sensor",
    # Built-in sources
    "source_db", "source_api", "source_file",
    # Built-in processing
    "transform", "join", "quality_check", "code",
    # Built-in sinks
    "sink_db", "sink_file", "sink_api",
    # Built-in integrations
    "dbt", "notify", "migrate", "condition_node", "parallel",
]
