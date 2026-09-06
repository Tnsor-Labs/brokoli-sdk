"""Server compatibility negotiation for pipeline deployment."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
import warnings
from dataclasses import dataclass
from typing import Any, Iterable

from brokoli.exceptions import CompatibilityError

REQUEST_TIMEOUT = 10
LEGACY_STATUS_CODES = {404, 405}

# ADR-030 §3: a feature that proves a server-side RUNTIME capability
# exists (a wrapper idiom, a worker language, a mount mechanism) must
# fail closed even when the server's response simply omits
# supported_execution_features -- absence cannot prove the runtime
# exists, and treating it as "predates the whole mechanism" stopped
# being a safe assumption the moment new runtime-existence features
# started shipping after capabilities advertising already existed. A
# purely declarative feature (data_intervals, conditional-routing, ...)
# keeps the legacy waiver below: an old server that genuinely predates
# GET /api/capabilities' execution-feature field can still run those.
RUNTIME_EXISTENCE_FEATURES = frozenset({"code-streaming-emit", "task-bundles"})


class LegacyServerWarning(UserWarning):
    """The target server could not prove pipeline IR compatibility."""


@dataclass(frozen=True)
class ServerCapabilities:
    """Capability fields required by the current SDK preflight.

    ``supported_execution_features`` is ``None`` when the server predates
    the field (pre-v0.10.11): those servers execute several gated
    features without advertising them, so absence means "skip feature
    gating", never "no features". A present-but-malformed field fails
    closed like every other capability field.
    """

    supported_ir_versions: tuple[str, ...]
    supported_execution_features: tuple[str, ...] | None = None


def _legacy_or_raise(message: str, allow_legacy_server: bool) -> None:
    if not allow_legacy_server:
        raise CompatibilityError(
            f"{message} Re-run with --allow-legacy-server only if this is a "
            "trusted server version that predates GET /api/capabilities."
        )
    warnings.warn(
        f"{message} Continuing because --allow-legacy-server was explicitly set; "
        "pipeline IR compatibility could not be verified.",
        LegacyServerWarning,
        stacklevel=3,
    )


def fetch_server_capabilities(
    server: str,
    auth_header: str = "",
    *,
    allow_legacy_server: bool = False,
) -> ServerCapabilities | None:
    """Fetch and validate the server's pipeline compatibility response.

    ``None`` is returned only when an unavailable legacy endpoint was
    explicitly allowed. A server that reports an incompatible version can
    never be bypassed with the legacy flag.
    """
    server = server.rstrip("/")
    headers = {"Authorization": auth_header} if auth_header else {}
    request = urllib.request.Request(
        f"{server}/api/capabilities",
        headers=headers,
        method="GET",
    )

    try:
        response = urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT)
        payload = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise CompatibilityError(
                f"Server {server} rejected the capability check with HTTP {exc.code}. "
                "Verify the Brokoli token or API key."
            ) from exc
        if exc.code in LEGACY_STATUS_CODES:
            _legacy_or_raise(
                f"Server {server} does not expose GET /api/capabilities (HTTP {exc.code}).",
                allow_legacy_server,
            )
            return None
        raise CompatibilityError(
            f"Server {server} capability check failed with HTTP {exc.code}; deployment was blocked."
        ) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        reason = getattr(exc, "reason", str(exc))
        _legacy_or_raise(
            f"Could not reach {server} to verify pipeline compatibility: {reason}.",
            allow_legacy_server,
        )
        return None
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CompatibilityError(
            f"Server {server} returned malformed JSON from /api/capabilities; "
            "deployment was blocked."
        ) from exc

    if not isinstance(payload, dict):
        raise CompatibilityError(
            f"Server {server} returned an invalid /api/capabilities payload: "
            "expected a JSON object."
        )

    versions = payload.get("supported_ir_versions")
    if (
        not isinstance(versions, list)
        or not versions
        or any(not isinstance(version, str) or not version for version in versions)
    ):
        raise CompatibilityError(
            f"Server {server} returned an invalid /api/capabilities payload: "
            "'supported_ir_versions' must be a non-empty list of strings."
        )

    features = payload.get("supported_execution_features")
    if features is not None and (
        not isinstance(features, list) or any(not isinstance(f, str) or not f for f in features)
    ):
        raise CompatibilityError(
            f"Server {server} returned an invalid /api/capabilities payload: "
            "'supported_execution_features' must be a list of non-empty strings."
        )

    return ServerCapabilities(
        tuple(versions),
        None if features is None else tuple(features),
    )


# Feature names follow the server's vocabulary (models.SupportedExecutionFeatures
# in the core repo). "dataset-map"/"dataset-filter" are reserved names the
# server deliberately does not advertise yet -- its runtime rejects
# SDK-emitted configs for those nodes -- so requiring them here correctly
# blocks deployment to any feature-advertising server until they run.
def required_execution_features(payload: dict[str, Any]) -> set[str]:
    """The execution features a compiled pipeline payload depends on."""
    required: set[str] = set()
    for edge in payload.get("edges") or []:
        if isinstance(edge, dict) and "condition" in edge:
            required.add("conditional-routing")
            break
    for node in payload.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        config = node.get("config") or {}
        node_type = node.get("type")
        # ADR-032 rollout step 3 (#439): a node's "interface" field is
        # additive IR, but a server that doesn't advertise
        # task-interface-v1 may not even accept IR 2.2 -- refuse at
        # deploy preflight rather than let a strict decoder 400 it.
        if node.get("interface") is not None:
            required.add("task-interface-v1")
        if "expansion" in config:
            required.add("dynamic-expansion")
        if node_type == "union":
            required.add("union")
        elif node_type == "dataset_map":
            required.add("dataset-map")
        elif node_type == "dataset_filter":
            required.add("dataset-filter")
        # Plain pagination long predates feature advertising; the
        # execution policy block is what implies checkpoint/page-retry
        # runtime semantics.
        if "execution" in config:
            required.add("pagination-checkpoints")
        # emit()/begin_emit() are wrapper contract features (ADR-029,
        # core "code-streaming-emit"): on a server whose wrapper
        # predates them the names simply don't exist and the script
        # fails at run time -- or worse, a bare emit falls back to
        # passthrough. A string scan is the same pragmatic test the
        # pagination gate uses: false positives (the word in a comment)
        # only ever refuse a deploy against a server too old to run
        # modern scripts anyway.
        if node_type == "code":
            script = config.get("script") or ""
            if "emit(" in script or "begin_emit(" in script:
                required.add("code-streaming-emit")
            if "task_bundle" in config:
                # ADR-031: task bundles need a server that can resolve and
                # mount them; an older server (no such feature) must be
                # refused at deploy, never reach "deployed, then fails at
                # run time".
                required.add("task-bundles")
    # catch_up compiles to the pipeline-level catchup field (ADR-028):
    # per-interval catch-up only exists on servers that advertise
    # data_intervals, and older strict decoders reject the field outright.
    if payload.get("catchup"):
        required.add("data_intervals")
    # ADR-032 rollout step 3: a pipeline-level "parameters" declaration
    # needs the same gate as a node's own "interface" field above.
    if payload.get("parameters"):
        required.add("task-interface-v1")
    return required


def preflight_server_compatibility(
    pipelines: Iterable[Any],
    server: str,
    auth_header: str = "",
    *,
    allow_legacy_server: bool = False,
) -> None:
    """Fail before persistence if any pipeline IR is unsupported."""
    pipeline_list = list(pipelines)
    capabilities = fetch_server_capabilities(
        server,
        auth_header,
        allow_legacy_server=allow_legacy_server,
    )
    if capabilities is None:
        return

    supported = set(capabilities.supported_ir_versions)
    for pipeline in pipeline_list:
        payload = pipeline.to_json()
        ir_version = payload.get("ir_version")
        if not isinstance(ir_version, str) or not ir_version:
            name = getattr(pipeline, "name", "<unnamed>")
            raise CompatibilityError(
                f"Pipeline {name!r} does not declare a valid 'ir_version'; deployment was blocked."
            )
        if ir_version not in supported:
            name = getattr(pipeline, "name", "<unnamed>")
            supported_text = ", ".join(capabilities.supported_ir_versions)
            raise CompatibilityError(
                f"Pipeline {name!r} requires IR {ir_version}, but server "
                f"{server.rstrip('/')} supports: {supported_text}. Upgrade the "
                "server or use a compatible SDK. --allow-legacy-server cannot "
                "override a version mismatch reported by the server."
            )

        required = required_execution_features(payload)
        if capabilities.supported_execution_features is not None:
            missing = required - set(capabilities.supported_execution_features)
        else:
            # This server responded to GET /api/capabilities (it is not
            # the LEGACY_STATUS_CODES case above) but its payload simply
            # has no 'supported_execution_features' key -- a real,
            # narrow window of servers that shipped capabilities before
            # that field existed. Declarative features get the legacy
            # waiver they always had; runtime-existence features do not,
            # because a server old enough to omit the field cannot have
            # the wrapper/worker/mount machinery a 0.11.x+ feature name
            # refers to.
            missing = required & RUNTIME_EXISTENCE_FEATURES
        if missing:
            name = getattr(pipeline, "name", "<unnamed>")
            missing_text = ", ".join(sorted(missing))
            raise CompatibilityError(
                f"Pipeline {name!r} requires execution feature(s) the "
                f"server does not support: {missing_text}. The server "
                "advertises what it can actually run; deploying anyway "
                "would persist a pipeline that fails at run time. "
                "--allow-legacy-server cannot override this."
            )
