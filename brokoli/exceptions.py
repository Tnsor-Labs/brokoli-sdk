"""Brokoli SDK exceptions."""


class BrokoliError(Exception):
    """Base exception for all Brokoli SDK errors."""


class PipelineError(BrokoliError):
    """Raised when pipeline definition is invalid."""


class NodeError(BrokoliError):
    """Raised when a node configuration is invalid."""

    def __init__(self, node_name: str, field: str, message: str) -> None:
        self.node_name = node_name
        self.field = field
        super().__init__(f"[{node_name}] {field}: {message}")


class ValidationError(BrokoliError):
    """Raised when pipeline validation fails."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        summary = f"{len(errors)} validation error(s)"
        super().__init__(summary)


class DeployError(BrokoliError):
    """Raised when pipeline deployment fails."""

    def __init__(self, pipeline_name: str, status_code: int, body: str) -> None:
        self.pipeline_name = pipeline_name
        self.status_code = status_code
        self.body = body
        super().__init__(f"Deploy failed for '{pipeline_name}': HTTP {status_code} — {body}")


class ConnectionError(BrokoliError):
    """Raised when server connection fails."""


class ContextError(BrokoliError):
    """Raised when SDK is used outside a Pipeline context."""

    def __init__(self, operation: str) -> None:
        super().__init__(f"{operation} must be used inside a `with Pipeline(...):` block")


class ParseError(BrokoliError):
    """Raised when a quality rule string cannot be parsed."""

    def __init__(self, rule_string: str, reason: str) -> None:
        self.rule_string = rule_string
        super().__init__(f"Cannot parse rule '{rule_string}': {reason}")
