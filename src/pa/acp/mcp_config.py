"""ACP MCP server configuration."""

from __future__ import annotations

import asyncio
import importlib.metadata
import os
import platform
import sys
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from acp.schema import EnvVariable, McpServerStdio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.logging import redact_log_text
from pa.server.listeners import owner_socket_path, record_owner_probe


@dataclass(frozen=True)
class OwnerEndpoint:
    url: str
    kind: str
    uds: str | None = None


class OwnerChannelError(RuntimeError):
    def __init__(self, classification: str, endpoint_kind: str, recovery: str):
        self.classification = classification
        self.endpoint_kind = endpoint_kind
        self.recovery = recovery
        super().__init__(
            "PA MCP owner channel "
            f"{classification} (endpoint={endpoint_kind}). {recovery}"
        )


class McpHandshakeError(RuntimeError):
    def __init__(
        self,
        classification: str,
        recovery: str,
        detail: str = "",
        *,
        phase: str | None = None,
        context: Mapping[str, Any] | None = None,
        root_exception: str | None = None,
    ):
        self.classification = classification
        self.recovery = recovery
        self.detail = detail
        self.phase = phase or classification.removesuffix("_failed")
        self.context = dict(context or {})
        self.root_exception = root_exception
        suffix = f" ({detail})" if detail else ""
        super().__init__(f"PA stdio MCP handshake {classification}{suffix}. {recovery}")


def _exception_leaves(exc: BaseException) -> list[BaseException]:
    """Flatten nested TaskGroup failures while preserving the useful root cause."""
    if isinstance(exc, BaseExceptionGroup):
        return [leaf for child in exc.exceptions for leaf in _exception_leaves(child)]
    return [exc]


def _installed_version(distribution: str) -> str:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return "not-installed"


def _bootstrap_context(
    settings: Settings,
    server: McpServerStdio,
    *,
    owner_environment: Mapping[str, str] | None,
) -> dict[str, Any]:
    endpoint = owner_endpoint(settings, owner_environment)
    source = (
        "PA_OWNER_API_URL"
        if endpoint.kind == "explicit_private_http"
        else (
            "PA_OWNER_SOCKET"
            if (owner_environment or os.environ).get("PA_OWNER_SOCKET")
            else "runtime_default"
        )
    )
    return {
        "pa_executable": server.command,
        "pa_arguments": list(server.args),
        "pa_version": _installed_version("pa"),
        "mcp_sdk_version": _installed_version("mcp"),
        "python_version": platform.python_version(),
        "owner_endpoint_type": endpoint.kind,
        "owner_endpoint_source": source,
        "owner_endpoint_path": endpoint.uds or endpoint.url,
        "cwd": str(Path.cwd()),
        "process_exit_code": None,
        "stderr_provenance": "stdio_child_captured",
        "environment_provenance": "minimal_pa_owner_environment",
    }


def _ensure_supported_mcp_sdk(context: Mapping[str, Any]) -> None:
    version = str(context.get("mcp_sdk_version") or "")
    try:
        major = int(version.split(".", 1)[0])
    except ValueError:
        return
    if major >= 2:
        raise McpHandshakeError(
            "dependency_incompatible",
            "Reinstall or upgrade PA so its declared dependency resolves mcp>=1.9.0,<2, then restart PA.",
            f"installed mcp SDK {version} is incompatible with pa.mcp.server FastMCP imports",
            phase="dependency_preflight",
            context=context,
            root_exception="ModuleNotFoundError: mcp.server.fastmcp",
        )


def owner_endpoint(
    settings: Settings, environment: Mapping[str, str] | None = None
) -> OwnerEndpoint:
    """Resolve the private owner channel without consulting web/fleet URLs."""
    environment = os.environ if environment is None else environment
    explicit = environment.get("PA_OWNER_API_URL", "").strip()
    if explicit:
        return OwnerEndpoint(explicit.rstrip("/"), "explicit_private_http")
    path = environment.get("PA_OWNER_SOCKET", "").strip() or str(
        owner_socket_path(settings, environment)
    )
    return OwnerEndpoint("http://pa-owner", "unix", path)


def _get_ready(endpoint: OwnerEndpoint, token: str, instance_id: str, timeout: float):
    transport = httpx.HTTPTransport(uds=endpoint.uds) if endpoint.uds else None
    with httpx.Client(transport=transport) as client:
        return client.get(
            f"{endpoint.url}/api/ready",
            headers={
                "Authorization": f"Bearer {token}",
                "X-PA-MCP-Instance-ID": instance_id,
            },
            timeout=timeout,
        )


def probe_owner_channel(
    settings: Settings,
    *,
    timeout: float = 4.0,
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Verify reachability, authentication, API readiness, and instance identity."""
    endpoint = owner_endpoint(settings, environment)
    token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    deadline = time.monotonic() + timeout
    delay = 0.1
    try:
        while True:
            try:
                response = _get_ready(
                    endpoint,
                    token,
                    settings.instance_id,
                    min(1.0, max(0.1, deadline - time.monotonic())),
                )
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                if time.monotonic() < deadline:
                    time.sleep(delay)
                    delay = min(delay * 2, 0.8)
                    continue
                raise OwnerChannelError(
                    "unreachable",
                    endpoint.kind,
                    "Verify the loaded owner endpoint and restart PA if its listener is missing.",
                ) from exc
            actual = response.headers.get("X-PA-Instance-ID", "").strip()
            if actual and actual != settings.instance_id:
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
            if response.status_code >= 500:
                raise OwnerChannelError(
                    "api_error",
                    endpoint.kind,
                    "Inspect the owning PA server logs for the readiness failure.",
                )
            if response.status_code != 200:
                raise OwnerChannelError(
                    "api_incompatible",
                    endpoint.kind,
                    "Inspect PA logs and verify the owner API version.",
                )
            if not actual:
                raise OwnerChannelError(
                    "identity_missing",
                    endpoint.kind,
                    "Inspect the listener and verify that the PA identity middleware is active.",
                )
            record_owner_probe(endpoint_type=endpoint.kind, success=True)
            return {"state": "connected", "endpoint_type": endpoint.kind}
    except OwnerChannelError as exc:
        record_owner_probe(
            endpoint_type=endpoint.kind,
            success=False,
            classification=exc.classification,
            retry_state="restart_or_reconnect_required",
        )
        raise


def pa_mcp_servers(
    settings: Settings,
    *,
    owner_environment: Mapping[str, str] | None = None,
    session_environment: Mapping[str, str] | None = None,
) -> list[McpServerStdio]:
    """Stdio MCP bridge so ACP agents get PA tools in-session."""
    # The ACP provider may have a different cwd, PATH, or inherited PA_* set.
    # Pin both the bridge executable and its owner API target to this server.
    # The server creates and forwards the CLI bearer token so the MCP child
    # never needs to create auth state. Mutations go through PA_LOCAL_API_URL.
    cli_token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    endpoint = owner_endpoint(settings, owner_environment)
    owner_env = {
        "PA_DATA_DIR": str(settings.data_dir),
        "PA_LOCAL_API_URL": endpoint.url,
        "PA_LOCAL_API_ENDPOINT_TYPE": endpoint.kind,
        **({"PA_LOCAL_API_SOCKET": endpoint.uds} if endpoint.uds else {}),
        "PA_LOCAL_API_TOKEN": cli_token,
        "PA_INSTANCE_ID": settings.instance_id,
    }
    browser_source = os.environ if session_environment is None else session_environment
    browser_env = {
        name: browser_source[name]
        for name in (
            "PA_BROWSER_CDP_URL",
            "PA_BROWSER_TARGET_ID",
            "PA_BROWSER_ATTACHMENT_ID",
            "PA_BROWSER_SESSION_ID",
        )
        if browser_source.get(name)
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


async def _probe_pa_mcp_stdio_async(
    settings: Settings,
    *,
    timeout: float,
    owner_environment: Mapping[str, str] | None,
    session_environment: Mapping[str, str] | None,
) -> dict[str, str | int]:
    server = pa_mcp_servers(
        settings,
        owner_environment=owner_environment,
        session_environment=session_environment,
    )[0]
    environment = {item.name: item.value for item in server.env}
    context = _bootstrap_context(settings, server, owner_environment=owner_environment)
    _ensure_supported_mcp_sdk(context)
    params = StdioServerParameters(
        command=server.command,
        args=list(server.args),
        env=environment,
    )
    stage = "spawn"
    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stderr:
        try:
            async with asyncio.timeout(timeout):
                async with stdio_client(params, errlog=stderr) as (read, write):
                    stage = "initialize"
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        stage = "tools_list"
                        tools = await session.list_tools()
                        stage = "shutdown"
                return {
                    "state": "connected",
                    "classification": "ok",
                    "tool_count": len(tools.tools),
                }
        except Exception as exc:
            classification = (
                "timeout" if isinstance(exc, TimeoutError) else f"{stage}_failed"
            )
            stderr.seek(0)
            child_stderr = redact_log_text(stderr.read().strip())
            leaves = _exception_leaves(exc)
            root = redact_log_text(
                "; ".join(
                    f"{leaf.__class__.__name__}: {leaf}"
                    for leaf in leaves
                    if str(leaf).strip()
                )
            )
            # The outer AnyIO ExceptionGroup is generic; captured child stderr
            # contains import/validation failures and is therefore authoritative.
            detail = child_stderr or root or redact_log_text(str(exc))
            raise McpHandshakeError(
                classification,
                "Run `pa doctor --verbose`; repair the reported dependency, owner endpoint, or loaded service drift, then retry placement.",
                detail[:2000],
                phase=stage,
                context=context,
                root_exception=root[:1000] or exc.__class__.__name__,
            ) from exc


def probe_pa_mcp_stdio(
    settings: Settings,
    *,
    timeout: float = 10.0,
    owner_environment: Mapping[str, str] | None = None,
    session_environment: Mapping[str, str] | None = None,
) -> dict[str, str | int]:
    """Exercise initialize/tools-list/clean shutdown against the pinned MCP child."""
    return asyncio.run(
        _probe_pa_mcp_stdio_async(
            settings,
            timeout=timeout,
            owner_environment=owner_environment,
            session_environment=session_environment,
        )
    )
