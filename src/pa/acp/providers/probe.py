"""Lightweight ACP initialize probe (no full session)."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from pa.acp.providers.base import AgentProviderSpec

logger = logging.getLogger(__name__)


def probe_acp_initialize(spec: AgentProviderSpec, *, timeout: float = 25.0) -> dict[str, Any]:
    """Spawn the provider briefly and call initialize; return capability summary."""
    try:
        return asyncio.run(_probe_async(spec, timeout=timeout))
    except Exception as exc:
        logger.exception("ACP probe failed for %s", spec.id)
        return {"ok": False, "error": str(exc), "provider_id": spec.id}


async def _probe_async(spec: AgentProviderSpec, *, timeout: float) -> dict[str, Any]:
    from acp import PROTOCOL_VERSION

    from pa.acp.client import PAClient
    from pa.acp.transport import spawn_agent
    from pa.packaging.paths import resolve_executable

    class _ProbeStore:
        """Minimal stand-in; probe never persists sessions."""

    prev = {k: os.environ.get(k) for k in spec.env}
    try:
        for k, v in spec.env.items():
            os.environ[k] = v
        command = spec.command
        resolved = resolve_executable(command)
        if resolved:
            command = str(resolved)
        client = PAClient(store=_ProbeStore())  # type: ignore[arg-type]
        ctx = spawn_agent(client, command, *list(spec.args or []))
        try:
            conn, _proc = await asyncio.wait_for(ctx.__aenter__(), timeout=timeout)
            init = await asyncio.wait_for(
                conn.initialize(protocol_version=PROTOCOL_VERSION),
                timeout=timeout,
            )
            caps = getattr(init, "agent_capabilities", None) or getattr(
                init, "agentCapabilities", None
            )
            auth = getattr(init, "auth_methods", None) or getattr(init, "authMethods", None)
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
            try:
                await ctx.__aexit__(None, None, None)
            except Exception:
                pass
    finally:
        for k, old in prev.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old


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
