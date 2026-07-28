"""ACP MCP server configuration."""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from ipaddress import ip_address
from urllib.parse import urlsplit

import httpx
from acp.schema import EnvVariable, McpServerStdio

from pa.auth.users import UserDirectory
from pa.config import Settings


@dataclass(frozen=True)
class OwnerEndpoint:
    url: str
    kind: str


class OwnerChannelError(RuntimeError):
    def __init__(self, classification: str, endpoint_kind: str, recovery: str):
        self.classification = classification
        self.endpoint_kind = endpoint_kind
        self.recovery = recovery
        super().__init__(
            "PA MCP owner channel "
            f"{classification} (endpoint={endpoint_kind}). {recovery}"
        )


def owner_endpoint(settings: Settings) -> OwnerEndpoint:
    """Resolve the listener address without using an advertised/fleet URL.

    ACP and its MCP bridge are child processes of PA and therefore share PA's
    network namespace. Concrete binds are directly reachable there; wildcard
    binds use a loopback address of the same family.
    """
    explicit = os.environ.get("PA_OWNER_API_URL", "").strip()
    if explicit:
        parsed = urlsplit(explicit)
        host = parsed.hostname or ""
        kind = "loopback" if host in {"localhost", "127.0.0.1", "::1"} else "concrete"
        return OwnerEndpoint(explicit.rstrip("/"), kind)
    host = settings.host.strip()
    if host in {"0.0.0.0", ""}:
        return OwnerEndpoint(f"http://127.0.0.1:{settings.port}", "wildcard_ipv4")
    if host in {"::", "[::]"}:
        return OwnerEndpoint(f"http://[::1]:{settings.port}", "wildcard_ipv6")
    normalized = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    try:
        address = ip_address(normalized)
    except ValueError:
        rendered = normalized
        kind = "loopback" if normalized == "localhost" else "concrete_hostname"
    else:
        rendered = f"[{normalized}]" if address.version == 6 else normalized
        kind = "loopback" if address.is_loopback else f"concrete_ipv{address.version}"
    return OwnerEndpoint(f"http://{rendered}:{settings.port}", kind)


def probe_owner_channel(settings: Settings, *, timeout: float = 4.0) -> dict[str, str]:
    """Verify reachability, authentication, API readiness, and instance identity."""
    endpoint = owner_endpoint(settings)
    token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    deadline = time.monotonic() + timeout
    delay = 0.1
    while True:
        try:
            response = httpx.get(
                f"{endpoint.url}/api/ready",
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-PA-MCP-Instance-ID": settings.instance_id,
                },
                timeout=min(1.0, max(0.1, deadline - time.monotonic())),
            )
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            if time.monotonic() < deadline:
                time.sleep(delay)
                delay = min(delay * 2, 0.8)
                continue
            raise OwnerChannelError(
                "unreachable",
                endpoint.kind,
                "Verify that PA is listening on its configured bind address.",
            ) from exc
        actual = response.headers.get("X-PA-Instance-ID", "").strip()
        if actual != settings.instance_id:
            raise OwnerChannelError(
                "instance_mismatch",
                endpoint.kind,
                "Stop the conflicting listener or reload this ACP session.",
            )
        if response.status_code in {401, 403}:
            raise OwnerChannelError(
                "authentication_rejected",
                endpoint.kind,
                "Reload the ACP session to refresh its owner token.",
            )
        if response.status_code == 404:
            raise OwnerChannelError(
                "api_incompatible",
                endpoint.kind,
                "Upgrade PA and reload the ACP session.",
            )
        if response.status_code == 503:
            if time.monotonic() < deadline:
                time.sleep(delay)
                delay = min(delay * 2, 0.8)
                continue
            raise OwnerChannelError(
                "api_not_ready",
                endpoint.kind,
                "Wait for PA startup to finish, then reconnect the session.",
            )
        if response.status_code != 200:
            raise OwnerChannelError(
                "api_incompatible",
                endpoint.kind,
                "Inspect PA logs and verify the owner API version.",
            )
        return {"state": "connected", "endpoint_type": endpoint.kind}


def pa_mcp_servers(settings: Settings) -> list[McpServerStdio]:
    """Stdio MCP bridge so ACP agents get PA tools in-session."""
    # The ACP provider may have a different cwd, PATH, or inherited PA_* set.
    # Pin both the bridge executable and its owner API target to this server.
    # The server creates and forwards the CLI bearer token so the MCP child
    # never needs to create auth state. Mutations go through PA_LOCAL_API_URL.
    cli_token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    endpoint = owner_endpoint(settings)
    owner_env = {
        "PA_DATA_DIR": str(settings.data_dir),
        "PA_LOCAL_API_URL": endpoint.url,
        "PA_LOCAL_API_ENDPOINT_TYPE": endpoint.kind,
        "PA_LOCAL_API_TOKEN": cli_token,
        "PA_INSTANCE_ID": settings.instance_id,
    }
    browser_env = {
        name: os.environ[name]
        for name in (
            "PA_BROWSER_CDP_URL",
            "PA_BROWSER_TARGET_ID",
            "PA_BROWSER_ATTACHMENT_ID",
            "PA_BROWSER_SESSION_ID",
        )
        if os.environ.get(name)
    }
    forwarded_env = [
        EnvVariable(name=name, value=value)
        for name, value in {**owner_env, **browser_env}.items()
    ]
    return [
        McpServerStdio(
            name="pa",
            command=sys.executable,
            args=["-m", "pa", "mcp"],
            env=forwarded_env,
        )
    ]
