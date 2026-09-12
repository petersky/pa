"""agent_providers tools: authenticated owner API proxies."""

from __future__ import annotations

from pa.core.context import AppContext


def register_mcp(mcp, ctx: AppContext) -> None:
    settings = ctx.settings
    runtime = ctx.require_service("async_runtime")

    async def forward(instance_id, method, suffix, *, body=None, timeout=120.0):
        from functools import partial
        from urllib.parse import quote
        from pa.mcp.local_api import request_local_pa

        base = (
            f"/api/fleet/instances/{quote(instance_id, safe='')}/agent-providers"
            if instance_id else "/api/agent/providers"
        )
        return await runtime.run_blocking(
            "http.provider_owner_proxy",
            partial(
                request_local_pa, settings, method, base + suffix,
                json=body, timeout_seconds=timeout,
            ),
            timeout=timeout + 5.0,
        )

    @mcp.tool()
    async def agent_providers_list(
        instance_id: str | None = None,
    ) -> list[dict]:
        """List ACP providers and install status on this host or a fleet peer."""
        return await forward(instance_id, "GET", "")

    @mcp.tool()
    async def agent_provider_status(
        provider_id: str, instance_id: str | None = None
    ) -> dict:
        """Status for one ACP provider (cursor, codex, openinterpreter, …)."""
        return await forward(instance_id, "GET", f"/{provider_id}")

    @mcp.tool()
    async def agent_provider_install(
        provider_id: str, instance_id: str | None = None
    ) -> dict:
        """Install or verify an ACP provider on this host or a fleet peer."""
        return await forward(instance_id, "POST", f"/{provider_id}/install", timeout=910.0)

    @mcp.tool()
    async def agent_provider_update(
        provider_id: str, instance_id: str | None = None
    ) -> dict:
        """Update an ACP provider package/binary."""
        return await forward(instance_id, "POST", f"/{provider_id}/update", timeout=910.0)

    @mcp.tool()
    async def agent_provider_configure(
        provider_id: str,
        env: dict[str, str] | None = None,
        secrets: dict[str, str] | None = None,
        no_browser: bool | None = None,
        model: str | None = None,
        model_provider: str | None = None,
        model_provider_name: str | None = None,
        model_provider_base_url: str | None = None,
        model_provider_env_key: str | None = None,
        model_provider_wire_api: str | None = None,
        instance_id: str | None = None,
    ) -> dict:
        """Configure ACP/model provider settings; secrets stay on the target host."""
        body = {
            "env": env or {},
            "secrets": secrets or {},
            "no_browser": no_browser,
            "model": model,
            "model_provider": model_provider,
            "model_provider_name": model_provider_name,
            "model_provider_base_url": model_provider_base_url,
            "model_provider_env_key": model_provider_env_key,
            "model_provider_wire_api": model_provider_wire_api,
        }
        return await forward(instance_id, "POST", f"/{provider_id}/configure", body=body)

    @mcp.tool()
    async def agent_provider_probe(
        provider_id: str, instance_id: str | None = None
    ) -> dict:
        """Probe ACP initialize handshake for a provider."""
        return await forward(instance_id, "POST", f"/{provider_id}/probe")

    @mcp.tool()
    async def agent_provider_login_start(
        provider_id: str,
        consent: bool,
        timeout_seconds: int = 600,
        instance_id: str | None = None,
    ) -> dict:
        """Explicitly start a bounded Codex device-login job on a target instance."""
        return await forward(instance_id, "POST", f"/{provider_id}/login-jobs", body={"consent": consent, "timeout_seconds": timeout_seconds})

    @mcp.tool()
    async def agent_provider_login_status(
        provider_id: str, job_id: str, instance_id: str | None = None
    ) -> dict:
        """Read a device-login job without returning credentials."""
        return await forward(instance_id, "GET", f"/{provider_id}/login-jobs/{job_id}")

    @mcp.tool()
    async def agent_provider_login_cancel(
        provider_id: str, job_id: str, instance_id: str | None = None
    ) -> dict:
        """Cancel an active device-login job."""
        return await forward(instance_id, "POST", f"/{provider_id}/login-jobs/{job_id}/cancel")
