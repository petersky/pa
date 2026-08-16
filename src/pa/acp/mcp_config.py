"""ACP MCP server configuration."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import os
import platform
import sys
import tempfile
import time
from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from acp.schema import EnvVariable, McpServerStdio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from pa.acp.environment import (
    ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV,
    ASSIGNED_SERVICE_AUTHORITY_URL_ENV,
    ASSIGNED_SERVICE_CREDENTIAL_ENV,
    ASSIGNED_SERVICE_DISPATCH_ENV,
    ASSIGNED_SERVICE_MODE_ENV,
    ASSIGNED_SERVICE_SESSION_ENV,
)
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
    if major != 2:
        raise McpHandshakeError(
            "dependency_incompatible",
            "Run `pa update`, then `pa restart`, so PA resolves its supported mcp>=2.0.0,<3 SDK.",
            f"installed mcp SDK {version} is outside PA's supported mcp>=2.0.0,<3 range",
            phase="dependency_preflight",
            context=context,
            root_exception=f"UnsupportedMcpSdkVersion: {version}",
        )


def owner_sandbox_directories(
    settings: Settings, environment: Mapping[str, str] | None = None
) -> list[str]:
    """Return the owner-socket directory Codex must admit for PA MCP stdio."""
    endpoint = owner_endpoint(settings, environment)
    if not endpoint.uds:
        return []
    return [str(Path(endpoint.uds).parent)]


_OWNER_PERMISSIONS_PROFILE = "pa-owner"


def _grant_unix_socket(network: dict[str, Any], socket_path: str) -> dict[str, Any]:
    sockets = dict(network.get("unix_sockets") or {})
    sockets[socket_path] = "allow"
    network["unix_sockets"] = sockets
    return network


def _is_legacy_network_grant(value: Any) -> bool:
    """True when ``permissions.network`` is PA's old unix-socket grant, not a profile."""
    if not isinstance(value, dict) or "extends" in value:
        return False
    keys = set(value)
    return "unix_sockets" in keys and keys <= {"unix_sockets", "enabled"}


def _codex_permissions_profile_name(payload: Mapping[str, Any]) -> str:
    """Pick the named profile Codex will actually apply for this session."""
    current = payload.get("default_permissions")
    if isinstance(current, str):
        name = current.strip()
        if name and not name.startswith(":"):
            return name
    return _OWNER_PERMISSIONS_PROFILE


def merge_codex_owner_sandbox_config(
    existing_config: str | None,
    *,
    socket_path: str,
) -> str:
    """Grant the PA owner socket without widening Codex sandbox roots.

    Recent Codex treats ``permissions`` as named profiles selected by
    ``default_permissions``. A top-level ``permissions.network`` block is
    interpreted as a profile named ``network`` and rejected unless that key
    is set. Put the socket grant on a real profile and keep the older
    ``sandbox_workspace_write`` roots for CLIs that still use that path.
    """
    payload: dict[str, Any] = {}
    if existing_config and existing_config.strip():
        try:
            loaded = json.loads(existing_config)
        except json.JSONDecodeError:
            loaded = None
        if isinstance(loaded, dict):
            payload = dict(loaded)
    socket_dir = str(Path(socket_path).parent)
    sandbox = dict(payload.get("sandbox_workspace_write") or {})
    roots = [
        str(item)
        for item in sandbox.get("writable_roots") or []
        if isinstance(item, str) and item
    ]
    if socket_dir not in roots:
        roots.append(socket_dir)
    sandbox["writable_roots"] = roots
    payload["sandbox_workspace_write"] = sandbox

    permissions = dict(payload.get("permissions") or {})
    legacy_network = permissions.get("network")
    if _is_legacy_network_grant(legacy_network):
        permissions.pop("network", None)
    else:
        legacy_network = None

    existing_default = payload.get("default_permissions")
    profile_name = _codex_permissions_profile_name(payload)
    profile = dict(permissions.get(profile_name) or {})
    if profile_name == _OWNER_PERMISSIONS_PROFILE:
        parent = (
            existing_default.strip()
            if isinstance(existing_default, str) and existing_default.strip().startswith(":")
            else ":workspace"
        )
        profile.setdefault("extends", parent)
        payload["default_permissions"] = profile_name
    elif not (isinstance(existing_default, str) and existing_default.strip()):
        payload["default_permissions"] = profile_name

    network = dict(profile.get("network") or {})
    if isinstance(legacy_network, dict):
        legacy_sockets = dict(legacy_network.get("unix_sockets") or {})
        sockets = dict(network.get("unix_sockets") or {})
        sockets.update(
            {
                str(path): action
                for path, action in legacy_sockets.items()
                if isinstance(path, str) and isinstance(action, str)
            }
        )
        network["unix_sockets"] = sockets
        if "enabled" in legacy_network and "enabled" not in network:
            network["enabled"] = legacy_network["enabled"]
    network = _grant_unix_socket(network, socket_path)
    profile["network"] = network
    permissions[profile_name] = profile
    payload["permissions"] = permissions

    features = dict(payload.get("features") or {})
    proxy = dict(features.get("network_proxy") or {})
    proxy = _grant_unix_socket(proxy, socket_path)
    features["network_proxy"] = proxy
    payload["features"] = features
    return json.dumps(payload)


def apply_codex_owner_sandbox_environment(
    environment: Mapping[str, str],
    settings: Settings,
    *,
    owner_environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Pin Codex MCP injection and owner-socket sandbox grants for this process."""
    merged = dict(environment)
    # Keep PA's injected `pa` stdio server when ~/.codex already names one.
    merged["DISABLE_MCP_CONFIG_FILTERING"] = "true"
    endpoint = owner_endpoint(settings, owner_environment)
    if endpoint.uds:
        merged["CODEX_CONFIG"] = merge_codex_owner_sandbox_config(
            merged.get("CODEX_CONFIG"),
            socket_path=endpoint.uds,
        )
    return merged


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
    private_environment: Mapping[str, str] | None = None,
) -> list[McpServerStdio]:
    """Stdio MCP bridge so ACP agents get PA tools in-session."""
    # The ACP provider may have a different cwd, PATH, or inherited PA_* set.
    # Pin both the bridge executable and its owner API target to this server.
    private_environment = dict(private_environment or {})
    forbidden_assigned_secrets = {
        ASSIGNED_SERVICE_CREDENTIAL_ENV,
        ASSIGNED_SERVICE_AUTHORITY_URL_ENV,
        ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV,
    }
    if forbidden_assigned_secrets & private_environment.keys():
        raise ValueError(
            "assigned MCP configuration must not serialize credentials or authority data"
        )
    assigned_mode = private_environment.get(ASSIGNED_SERVICE_MODE_ENV) == "1"
    assigned_service_env = {
        name: private_environment.get(name, "").strip()
        for name in (
            ASSIGNED_SERVICE_MODE_ENV,
            ASSIGNED_SERVICE_DISPATCH_ENV,
            ASSIGNED_SERVICE_SESSION_ENV,
        )
    }
    if assigned_mode and not all(assigned_service_env.values()):
        raise ValueError("assigned MCP session binding is incomplete")
    if not assigned_mode and any(assigned_service_env.values()):
        raise ValueError("assigned MCP session binding requires assigned mode")
    if not assigned_mode:
        assigned_service_env = {}
    endpoint = owner_endpoint(settings, owner_environment)
    owner_env = {
        "PA_DATA_DIR": str(settings.data_dir),
        "PA_LOCAL_API_URL": endpoint.url,
        "PA_LOCAL_API_ENDPOINT_TYPE": endpoint.kind,
        **({"PA_LOCAL_API_SOCKET": endpoint.uds} if endpoint.uds else {}),
        "PA_INSTANCE_ID": settings.instance_id,
    }
    if not assigned_mode:
        # Ordinary MCP bridges retain the owner bearer contract. Assigned bridges
        # have a server-enforced tool surface and never receive this broad token.
        owner_env["PA_LOCAL_API_TOKEN"] = (
            UserDirectory(settings.data_dir).ensure_default_user().cli_token
        )
    browser_env: dict[str, str] = {}
    if not assigned_mode:
        browser_source = (
            os.environ if session_environment is None else session_environment
        )
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
        for name, value in {
            **owner_env,
            **browser_env,
            **assigned_service_env,
        }.items()
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
    private_environment: Mapping[str, str] | None = None,
) -> dict[str, str | int]:
    server = pa_mcp_servers(
        settings,
        owner_environment=owner_environment,
        session_environment=session_environment,
        private_environment=private_environment,
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
            async with AsyncExitStack() as stack:
                # Bound spawn and the handshake itself.  AsyncExitStack unwinds
                # after this timeout scope, so the MCP SDK's separately bounded
                # process teardown cannot turn a successful initialize/tools-list
                # exchange into a false handshake timeout on a busy instance.
                async with asyncio.timeout(timeout):
                    read, write = await stack.enter_async_context(
                        stdio_client(params, errlog=stderr)
                    )
                    stage = "initialize"
                    session = await stack.enter_async_context(
                        ClientSession(read, write)
                    )
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
    private_environment: Mapping[str, str] | None = None,
) -> dict[str, str | int]:
    """Exercise initialize/tools-list/clean shutdown against the pinned MCP child."""
    return asyncio.run(
        _probe_pa_mcp_stdio_async(
            settings,
            timeout=timeout,
            owner_environment=owner_environment,
            session_environment=session_environment,
            private_environment=private_environment,
        )
    )
