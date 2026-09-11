"""Lightweight ACP initialize probe (no full session)."""

from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from typing import Any

from pa.acp.environment import (
    inject_agent_github_environment,
    sanitize_provider_environment,
)
from pa.acp.providers.base import AgentProviderSpec
from pa.config import get_settings

logger = logging.getLogger(__name__)


def probe_acp_initialize(
    spec: AgentProviderSpec, *, timeout: float = 25.0
) -> dict[str, Any]:
    """Spawn the provider briefly and call initialize; return capability summary."""
    try:
        return asyncio.run(_probe_async(spec, timeout=timeout))
    except Exception as exc:
        logger.warning("ACP probe failed for %s (%s)", spec.id, type(exc).__name__)
        return {
            "ok": False,
            "error": f"ACP initialize probe failed ({type(exc).__name__})",
            "provider_id": spec.id,
        }


async def probe_acp_catalog(spec: AgentProviderSpec, *, timeout: float = 8.0):
    """Read a temporary session's advertisement; never prompt or attach tools."""
    with tempfile.TemporaryDirectory(prefix="pa-catalog-") as cwd:
        try:
            async with asyncio.timeout(timeout):
                return await _probe_async(spec, timeout=timeout, catalog_cwd=cwd)
        except Exception:
            return {"ok": False, "provider_id": spec.id}


async def _probe_async(
    spec: AgentProviderSpec, *, timeout: float, catalog_cwd: str | None = None
) -> dict[str, Any]:
    from acp import PROTOCOL_VERSION

    from pa.acp.client import (
        PAClient,
        extract_models_modes_config,
        permission_cancelled,
    )
    from pa.acp.transport import spawn_agent
    from pa.packaging.paths import resolve_executable

    class _ProbeStore:
        """Minimal stand-in; probe never persists sessions."""

    class ProbeClient(PAClient):
        async def request_permission(self, **kwargs):
            return permission_cancelled()

        async def read_text_file(self, **kwargs):
            raise PermissionError("Discovery does not expose filesystem access")

        async def write_text_file(self, **kwargs):
            raise PermissionError("Discovery does not expose filesystem access")

        async def session_update(self, **kwargs):
            pass

        async def ext_method(self, method, params):
            raise PermissionError("Discovery does not expose client extensions")

    command = spec.command
    resolved = resolve_executable(command)
    if resolved:
        command = str(resolved)
    child_env = sanitize_provider_environment(os.environ, spec.env)
    child_env = {k: v for k, v in child_env.items() if k not in spec.excluded_env}
    child_env, _github_auth_source = inject_agent_github_environment(
        child_env, get_settings()
    )
    client = ProbeClient(store=_ProbeStore())  # type: ignore[arg-type]
    ctx = spawn_agent(
        client,
        command,
        *list(spec.args or []),
        env=child_env,
        cwd=catalog_cwd,
        transport_kwargs={"shutdown_timeout": 0.25} if catalog_cwd else None,
    )
    entry = asyncio.create_task(ctx.__aenter__())
    proc = None
    try:
        conn, proc = await asyncio.wait_for(asyncio.shield(entry), timeout=timeout)
        init = await asyncio.wait_for(
            conn.initialize(protocol_version=PROTOCOL_VERSION),
            timeout=timeout,
        )
        caps = getattr(init, "agent_capabilities", None) or getattr(
            init, "agentCapabilities", None
        )
        auth = getattr(init, "auth_methods", None) or getattr(init, "authMethods", None)
        if catalog_cwd:
            session = await conn.new_session(cwd=catalog_cwd, mcp_servers=[])
            return {"ok": True, **extract_models_modes_config(session)}
        return {
            "ok": True,
            "provider_id": spec.id,
            "protocol_version": PROTOCOL_VERSION,
            "agent_capabilities": _plain(caps),
            "auth_methods": _plain(auth),
            "command": command,
            "args": list(spec.args),
        }
    finally:

        async def reap():
            nonlocal proc
            try:
                if proc is None:
                    _, proc = await asyncio.wait_for(entry, timeout=2)
                await asyncio.wait_for(ctx.__aexit__(None, None, None), timeout=2)
            except Exception:
                pass
            finally:
                if proc is not None and proc.returncode is None:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
                    await asyncio.wait_for(proc.wait(), timeout=1)

        # Caller cancellation/deadlines must never cancel transport shutdown.
        # Keep ownership until the bounded cleanup has reaped the child.
        cleanup = asyncio.create_task(reap())
        cancelled = False
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                cancelled = True
        cleanup.result()
        if cancelled:
            raise asyncio.CancelledError


def _plain(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump(mode="json", by_alias=True)
        except TypeError:
            return value.model_dump(by_alias=True)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
