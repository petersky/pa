"""Environment boundaries for ACP provider and PA MCP child processes."""

from __future__ import annotations

from collections.abc import Mapping

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
