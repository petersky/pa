"""REST + MCP APIs for ACP provider install/status/configure on this host."""

from __future__ import annotations

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from pa.acp.providers.base import ProviderConfigureBody
from pa.acp.providers.codex_auth import get_codex_login_store, resolve_codex_cli
from pa.acp.providers.registry import get_provider
from pa.acp.providers.resolve import list_provider_summaries
from pa.core.async_runtime import BlockingQueueFull
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.subprocesses import run_process
from pa.domain.instance_config import update_instance_config

router = APIRouter(prefix="/agent/providers")


class ProviderActionGate:
    """Hard-bound request-scoped provider subprocesses and their waiters."""

    def __init__(self, *, max_active: int = 2, max_queue: int = 8) -> None:
        self.max_active = max_active
        self.max_queue = max_queue
        self.active = 0
        self.queued = 0
        self._slots = asyncio.Semaphore(max_active)

    @asynccontextmanager
    async def slot(self):
        if self.active + self.queued >= self.max_active + self.max_queue:
            raise BlockingQueueFull("provider action queue is full")
        self.queued += 1
        try:
            await self._slots.acquire()
        except BaseException:
            self.queued -= 1
            raise
        self.queued -= 1
        self.active += 1
        try:
            yield
        finally:
            self.active -= 1
            self._slots.release()

    def snapshot(self) -> dict[str, int]:
        return {
            "active": self.active,
            "queued": self.queued,
            "max_active": self.max_active,
            "max_queue": self.max_queue,
        }


class ConfigureBody(BaseModel):
    env: dict[str, str] = Field(default_factory=dict)
    secrets: dict[str, str] = Field(default_factory=dict)
    no_browser: bool | None = None
    codex_path: str | None = None
    initial_agent_mode: str | None = None
    model: str | None = None
    model_provider: str | None = None
    model_provider_name: str | None = None
    model_provider_base_url: str | None = None
    model_provider_env_key: str | None = None
    model_provider_wire_api: str | None = None


class InstanceProviderBody(BaseModel):
    """Set the instance-default ACP provider (persisted to config.json)."""

    provider: str


class LoginBody(BaseModel):
    consent: bool = False
    timeout_seconds: int = Field(default=600, ge=60, le=1800)


class _LoginActiveError(ValueError):
    def __init__(self, job_id: str) -> None:
        super().__init__("A Codex login is already active")
        self.job_id = job_id


def _data_dir(request: Request):
    return request.app.state.ctx.settings.data_dir


async def _offload_request(
    request: Request,
    operation: str,
    call,
    *args,
    timeout: float = 120.0,
    **kwargs,
):
    runtime = request.app.state.ctx.require_service("async_runtime")
    return await runtime.run_blocking(
        operation, call, *args, timeout=timeout, **kwargs
    )


@router.get("")
async def list_local_providers(request: Request) -> list[dict]:
    return await _offload_request(
        request, "provider.list", list_provider_summaries, _data_dir(request)
    )


@router.get("/{provider_id}")
async def get_local_provider(request: Request, provider_id: str) -> dict:
    try:
        get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _offload_request(
        request,
        "provider.status",
        _local_provider_action,
        _data_dir(request),
        provider_id,
        "status",
    )


@router.post("/{provider_id}/install")
async def install_provider(request: Request, provider_id: str) -> dict:
    try:
        get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _run_provider_action(
        _data_dir(request),
        provider_id,
        "install",
        timeout=900.0,
        async_runtime=request.app.state.ctx.require_service("async_runtime"),
        gate=request.app.state.ctx.services.get("provider_action_gate"),
    )


@router.post("/{provider_id}/update")
async def update_provider(request: Request, provider_id: str) -> dict:
    try:
        get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _run_provider_action(
        _data_dir(request),
        provider_id,
        "update",
        timeout=900.0,
        async_runtime=request.app.state.ctx.require_service("async_runtime"),
        gate=request.app.state.ctx.services.get("provider_action_gate"),
    )


@router.post("/{provider_id}/configure")
async def configure_provider(
    request: Request, provider_id: str, body: ConfigureBody
) -> dict:
    try:
        get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    try:
        return await _offload_request(
            request,
            "provider.configure",
            _configure_local_provider,
            _data_dir(request),
            provider_id,
            ProviderConfigureBody(
                env=body.env,
                secrets=body.secrets,
                no_browser=body.no_browser,
                codex_path=body.codex_path,
                initial_agent_mode=body.initial_agent_mode,
                model=body.model,
                model_provider=body.model_provider,
                model_provider_name=body.model_provider_name,
                model_provider_base_url=body.model_provider_base_url,
                model_provider_env_key=body.model_provider_env_key,
                model_provider_wire_api=body.model_provider_wire_api,
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/{provider_id}/probe")
async def probe_provider(request: Request, provider_id: str) -> dict:
    try:
        get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _run_provider_action(
        _data_dir(request),
        provider_id,
        "probe",
        timeout=60.0,
        async_runtime=request.app.state.ctx.require_service("async_runtime"),
        gate=request.app.state.ctx.services.get("provider_action_gate"),
    )


@router.post("/{provider_id}/login-jobs", status_code=202)
async def start_provider_login(
    request: Request, provider_id: str, body: LoginBody
) -> dict:
    """Start device auth only after explicit user consent."""
    if provider_id != "codex":
        raise HTTPException(
            status_code=400, detail="Device login is supported only for Codex"
        )
    if not body.consent:
        raise HTTPException(
            status_code=400, detail="Explicit consent is required to start sign-in"
        )
    try:
        return await _offload_request(
            request,
            "provider.login_start",
            _start_local_login,
            _data_dir(request),
            provider_id,
            body.consent,
            body.timeout_seconds,
        )
    except _LoginActiveError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "A Codex login is already active",
                "job_id": exc.job_id,
            },
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/{provider_id}/codex-cli/install")
async def install_provider_codex_cli(request: Request, provider_id: str) -> dict:
    if provider_id != "codex":
        raise HTTPException(
            status_code=400, detail="Codex CLI applies only to the Codex provider"
        )
    return await _run_provider_action(
        request.app.state.ctx.settings.data_dir,
        provider_id,
        "codex-cli-install",
        timeout=900.0,
        async_runtime=request.app.state.ctx.require_service("async_runtime"),
        gate=request.app.state.ctx.services.get("provider_action_gate"),
    )


@router.get("/{provider_id}/login-jobs/{job_id}")
async def get_provider_login(request: Request, provider_id: str, job_id: str) -> dict:
    if provider_id != "codex":
        raise HTTPException(status_code=404, detail="Login job not found")
    try:
        return await _offload_request(
            request,
            "provider.login_status",
            _read_local_login,
            _data_dir(request),
            provider_id,
            job_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Login job not found") from exc


@router.get("/{provider_id}/login-jobs/{job_id}/events")
async def get_provider_login_events(
    request: Request, provider_id: str, job_id: str, after: int = 0
) -> dict:
    if provider_id != "codex":
        raise HTTPException(status_code=404, detail="Login job not found")
    try:
        return await _offload_request(
            request,
            "provider.login_events",
            _read_local_login_events,
            _data_dir(request),
            provider_id,
            job_id,
            after,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Login job not found") from exc


@router.post("/{provider_id}/login-jobs/{job_id}/cancel")
async def cancel_provider_login(
    request: Request, provider_id: str, job_id: str
) -> dict:
    if provider_id != "codex":
        raise HTTPException(status_code=404, detail="Login job not found")
    try:
        return await _offload_request(
            request,
            "provider.login_cancel",
            _cancel_local_login,
            _data_dir(request),
            provider_id,
            job_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Login job not found") from exc


@router.put("/default")
async def set_default_provider(request: Request, body: InstanceProviderBody) -> dict:
    try:
        get_provider(body.provider)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    settings = request.app.state.ctx.settings
    await _offload_request(
        request,
        "provider.default_write",
        update_instance_config,
        settings.data_dir,
        agent_provider=body.provider,
    )
    settings.agent_provider = body.provider
    return {"agent_provider": body.provider}


class AgentProvidersModule(Module):
    @property
    def name(self) -> str:
        return "agent_providers"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "ACP provider install, configure, update, and probe APIs"

    def on_load(self, ctx: AppContext) -> None:
        ctx.register_service("provider_action_gate", ProviderActionGate())

    def api_routers(self):
        return [("/api", router, ["agent-providers"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        settings = ctx.settings
        runtime = ctx.require_service("async_runtime")

        async def offload(operation: str, call, *args, timeout: float = 120.0):
            return await runtime.run_blocking(
                operation, call, *args, timeout=timeout
            )

        @mcp.tool()
        async def agent_providers_list(
            instance_id: str | None = None,
        ) -> list[dict]:
            """List ACP providers and install status on this host or a fleet peer."""
            if instance_id:
                return await _fleet_proxy(
                    ctx, instance_id, "GET", "/api/agent/providers"
                )
            return await offload(
                "provider.list", list_provider_summaries, settings.data_dir
            )

        @mcp.tool()
        async def agent_provider_status(
            provider_id: str, instance_id: str | None = None
        ) -> dict:
            """Status for one ACP provider (cursor, codex, openinterpreter, …)."""
            if instance_id:
                return await _fleet_proxy(
                    ctx,
                    instance_id,
                    "GET",
                    f"/api/agent/providers/{provider_id}",
                )
            return await offload(
                "provider.status",
                _local_provider_action,
                settings.data_dir,
                provider_id,
                "status",
            )

        @mcp.tool()
        async def agent_provider_install(
            provider_id: str, instance_id: str | None = None
        ) -> dict:
            """Install or verify an ACP provider on this host or a fleet peer."""
            if instance_id:
                return await _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/install",
                )
            return await _run_provider_action(
                settings.data_dir,
                provider_id,
                "install",
                timeout=900.0,
                async_runtime=runtime,
                gate=ctx.require_service("provider_action_gate"),
            )

        @mcp.tool()
        async def agent_provider_update(
            provider_id: str, instance_id: str | None = None
        ) -> dict:
            """Update an ACP provider package/binary."""
            if instance_id:
                return await _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/update",
                )
            return await _run_provider_action(
                settings.data_dir,
                provider_id,
                "update",
                timeout=900.0,
                async_runtime=runtime,
                gate=ctx.require_service("provider_action_gate"),
            )

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
            if instance_id:
                return await _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/configure",
                    json_body=body,
                )
            return await offload(
                "provider.configure",
                _configure_local_provider,
                settings.data_dir,
                provider_id,
                ProviderConfigureBody(
                    env=body["env"],
                    secrets=body["secrets"],
                    no_browser=no_browser,
                    model=model,
                    model_provider=model_provider,
                    model_provider_name=model_provider_name,
                    model_provider_base_url=model_provider_base_url,
                    model_provider_env_key=model_provider_env_key,
                    model_provider_wire_api=model_provider_wire_api,
                ),
            )

        @mcp.tool()
        async def agent_provider_probe(
            provider_id: str, instance_id: str | None = None
        ) -> dict:
            """Probe ACP initialize handshake for a provider."""
            if instance_id:
                return await _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/probe",
                )
            return await _run_provider_action(
                settings.data_dir,
                provider_id,
                "probe",
                timeout=60.0,
                async_runtime=runtime,
                gate=ctx.require_service("provider_action_gate"),
            )

        @mcp.tool()
        async def agent_provider_login_start(
            provider_id: str,
            consent: bool,
            timeout_seconds: int = 600,
            instance_id: str | None = None,
        ) -> dict:
            """Explicitly start a bounded Codex device-login job on a target instance."""
            body = {"consent": consent, "timeout_seconds": timeout_seconds}
            if instance_id:
                return await _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/login-jobs",
                    json_body=body,
                )
            return await offload(
                "provider.login_start",
                _start_local_login,
                settings.data_dir,
                provider_id,
                consent,
                timeout_seconds,
            )

        @mcp.tool()
        async def agent_provider_login_status(
            provider_id: str, job_id: str, instance_id: str | None = None
        ) -> dict:
            """Read a device-login job without returning credentials."""
            if instance_id:
                return await _fleet_proxy(
                    ctx,
                    instance_id,
                    "GET",
                    f"/api/agent/providers/{provider_id}/login-jobs/{job_id}",
                )
            return await offload(
                "provider.login_status",
                _read_local_login,
                settings.data_dir,
                provider_id,
                job_id,
            )

        @mcp.tool()
        async def agent_provider_login_cancel(
            provider_id: str, job_id: str, instance_id: str | None = None
        ) -> dict:
            """Cancel an active device-login job."""
            if instance_id:
                return await _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/login-jobs/{job_id}/cancel",
                )
            return await offload(
                "provider.login_cancel",
                _cancel_local_login,
                settings.data_dir,
                provider_id,
                job_id,
            )


def _local_provider_action(data_dir, provider_id: str, action: str) -> dict:
    provider = get_provider(provider_id)
    result = getattr(provider, action)(data_dir)
    return result if isinstance(result, dict) else result.model_dump(mode="json")


async def _run_provider_action(
    data_dir,
    provider_id: str,
    action: str,
    *,
    timeout: float,
    async_runtime=None,
    gate: ProviderActionGate | None = None,
) -> dict:
    async def execute():
        process = run_process(
            [
                sys.executable,
                "-m",
                "pa.acp.providers.action_runner",
                provider_id,
                action,
                str(data_dir),
            ],
            timeout=timeout,
            output_limit=1024 * 1024,
        )
        return (
            await async_runtime.observe(
                f"subprocess.provider_{action}", process, timeout=timeout + 5.0
            )
            if async_runtime
            else await process
        )

    if gate:
        async with gate.slot():
            result = await execute()
    else:
        result = await execute()
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-500:]
        raise RuntimeError(
            f"Provider {action} failed with exit {result.returncode}: {detail}"
        )
    for line in reversed(result.stdout.splitlines()):
        if line.startswith("PA_PROVIDER_RESULT="):
            payload = json.loads(line.removeprefix("PA_PROVIDER_RESULT="))
            if isinstance(payload, dict):
                return payload
            break
    raise RuntimeError(f"Provider {action} returned an invalid result")


def _configure_local_provider(
    data_dir, provider_id: str, body: ProviderConfigureBody
) -> dict:
    return get_provider(provider_id).configure(data_dir, body).model_dump(mode="json")


def _start_local_login(
    data_dir, provider_id: str, consent: bool, timeout_seconds: int
) -> dict:
    if provider_id != "codex" or not consent:
        raise ValueError("Codex device login requires explicit consent")
    configured_path = (
        get_provider("codex")
        .resolve_spawn(data_dir=data_dir)
        .env.get("CODEX_PATH")
    )
    codex = resolve_codex_cli(configured_path)
    if not codex:
        raise ValueError("Codex CLI is not installed on this instance")
    store = get_codex_login_store(data_dir)
    if active := store.latest_active():
        raise _LoginActiveError(active.job_id)
    job = store.create(timeout_seconds=timeout_seconds)
    store.start(job, codex)
    return job.public_dict()


def _read_local_login(data_dir, provider_id: str, job_id: str) -> dict:
    job = get_codex_login_store(data_dir).get(job_id)
    if provider_id != "codex" or not job:
        raise ValueError("Login job not found")
    return job.public_dict()


def _read_local_login_events(
    data_dir, provider_id: str, job_id: str, after: int
) -> dict:
    job = get_codex_login_store(data_dir).get(job_id)
    if provider_id != "codex" or not job:
        raise ValueError("Login job not found")
    return {
        "job_id": job.job_id,
        "state": job.state.value,
        "events": [
            event.model_dump(mode="json")
            for event in job.events
            if event.sequence > after
        ],
    }


def _cancel_local_login(data_dir, provider_id: str, job_id: str) -> dict:
    job = get_codex_login_store(data_dir).cancel(job_id)
    if provider_id != "codex" or not job:
        raise ValueError("Login job not found")
    return job.public_dict()


async def _fleet_proxy(
    ctx: AppContext,
    instance_id: str,
    method: str,
    path: str,
    json_body: dict[str, Any] | None = None,
) -> Any:
    from pa.fleet.registry import FleetRegistry

    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    inst = None
    for candidate in fleet.list_instances():
        if candidate.instance_id == instance_id:
            inst = candidate
            break
    if not inst:
        raise ValueError(f"Unknown fleet instance: {instance_id}")
    settings = ctx.settings
    headers: dict[str, str] = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    url = f"{inst.url.rstrip('/')}{path}"
    client = ctx.services.get("fleet_http_client")
    owns_client = client is None
    client = client or httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=5.0),
        limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
    )
    runtime = ctx.require_service("async_runtime")
    try:
        resp = await runtime.observe(
            "http.provider_fleet_proxy",
            client.request(
                method,
                url,
                headers=headers,
                json=json_body,
                timeout=120.0,
            ),
            timeout=125.0,
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"{method} {path} → {resp.status_code}: {resp.text[:300]}"
            )
        return await runtime.run_blocking(
            "provider.response_json", resp.json, timeout=3.0
        )
    finally:
        if owns_client:
            await client.aclose()
