"""Environment boundaries for ACP provider and PA MCP child processes."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping

ASSIGNED_SERVICE_CREDENTIAL_ENV = "PA_ASSIGNED_SERVICE_CREDENTIAL"
ASSIGNED_SERVICE_AUTHORITY_URL_ENV = "PA_ASSIGNED_SERVICE_AUTHORITY_URL"
ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV = (
    "PA_ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ID"
)
ASSIGNED_SERVICE_MODE_ENV = "PA_ASSIGNED_SERVICE_MODE"
ASSIGNED_SERVICE_DISPATCH_ENV = "PA_ASSIGNED_SERVICE_DISPATCH_ID"
ASSIGNED_SERVICE_SESSION_ENV = "PA_ASSIGNED_SERVICE_SESSION_ID"


def assigned_service_session_capability(
    *,
    secret: str,
    dispatch_id: str,
    session_id: str,
    target_instance_id: str,
) -> str:
    """Derive one restart-stable capability for an exact local dispatch session."""

    if not all(
        value.strip()
        for value in (secret, dispatch_id, session_id, target_instance_id)
    ):
        raise ValueError("assigned service session capability scope is incomplete")
    scope = (
        f"pa-assigned-session:v1:{dispatch_id}:{session_id}:{target_instance_id}"
    )
    digest = hmac.new(secret.encode(), scope.encode(), hashlib.sha256).hexdigest()
    return f"pas1.{digest}"


def assigned_service_mcp_environment(
    *,
    dispatch_id: str,
    session_id: str,
) -> dict[str, str]:
    """Build the non-secret binding for an assigned PA MCP tool surface."""

    if not dispatch_id.strip() or not session_id.strip():
        raise ValueError("assigned service MCP session binding is incomplete")
    return {
        ASSIGNED_SERVICE_MODE_ENV: "1",
        ASSIGNED_SERVICE_DISPATCH_ENV: dispatch_id,
        ASSIGNED_SERVICE_SESSION_ENV: session_id,
    }

# These values belong to the PA service or to the dedicated PA MCP bridge.  ACP
# providers are general-purpose agent processes and must never inherit them from
# the service environment (or reintroduce them through per-session overrides).
PRIVATE_PROVIDER_ENVIRONMENT = frozenset(
    {
        "PA_ACP_QUIESCE",
        "PA_ACP_RESUME",
        "PA_AGENT_ARGS",
        "PA_AGENT_COMMAND",
        "PA_AGENT_ENABLED",
        "PA_AGENT_PROVIDER",
        ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV,
        ASSIGNED_SERVICE_AUTHORITY_URL_ENV,
        ASSIGNED_SERVICE_CREDENTIAL_ENV,
        ASSIGNED_SERVICE_DISPATCH_ENV,
        ASSIGNED_SERVICE_MODE_ENV,
        ASSIGNED_SERVICE_SESSION_ENV,
        "PA_AUTH_REQUIRED",
        "PA_CAPABILITIES",
        "PA_CLI_TOKEN",
        "PA_DATA_DIR",
        "PA_DEBUG",
        "PA_DEV_TOOLS",
        "PA_FLEET_ID",
        "PA_FLEET_OWNER_URL",
        "PA_FLEET_TOKEN",
        "PA_GITHUB_TOKEN",
        "PA_GITHUB_WEBHOOK_SECRET",
        "PA_HOST",
        "PA_INSTANCE_ID",
        "PA_INSTANCE_NAME",
        "PA_INSTANCE_URL",
        "PA_LISTENER_HEALTH",
        "PA_LOCAL_API_ENDPOINT_TYPE",
        "PA_LOCAL_API_SOCKET",
        "PA_LOCAL_API_TOKEN",
        "PA_LOCAL_API_URL",
        "PA_LOG_LEVEL",
        "PA_OIDC_CLIENT_ID",
        "PA_OIDC_CLIENT_SECRET",
        "PA_OIDC_ISSUER",
        "PA_OWNER_API_URL",
        "PA_OWNER_SOCKET",
        "PA_PEERS",
        "PA_PORT",
        "PA_PR_SUPERVISOR_AUTHORITY_URL",
        "PA_PR_SUPERVISOR_TOKEN",
        "PA_RELAY_ENABLED",
        "PA_RELEASE_TRACK",
        "PA_RUNTIME_DIR",
        "PA_SESSION_SECRET",
        "PA_SUBSCRIBED_REALMS",
        "PA_SYNC_TOKEN",
        "PA_WEB_LISTENERS",
        "PA_ZONE",
    }
)


def _server_private_name(name: str) -> bool:
    return name in PRIVATE_PROVIDER_ENVIRONMENT


def sanitize_provider_environment(
    inherited: Mapping[str, str],
    *overrides: Mapping[str, str] | None,
) -> dict[str, str]:
    """Build a provider environment with PA-private controls removed.

    Filtering happens after merging so a hostile or stale session override
    cannot restore a server-private endpoint or credential.
    """
    environment = dict(inherited)
    for override in overrides:
        if override:
            environment.update(override)
    environment = {
        name: value
        for name, value in environment.items()
        if not _server_private_name(name)
    }
    return environment


def private_provider_environment_names(
    environment: Mapping[str, str],
) -> list[str]:
    """Return private names present in an environment without exposing values."""
    return sorted(name for name in environment if _server_private_name(name))
