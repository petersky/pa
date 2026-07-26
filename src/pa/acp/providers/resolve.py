"""Resolve ACP provider from surface → user → instance cascade."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pa.acp.providers.base import AgentProviderSpec
from pa.acp.providers.registry import DEFAULT_PROVIDER_ID, get_provider, list_providers
from pa.acp.surfaces import AgentInvocationContext
from pa.core.async_runtime import AsyncRuntime
from pa.config import Settings
from pa.core.preferences import SurfaceAgentPrefs, get_preferences_store


@dataclass(frozen=True)
class ResolvedAgentProvider:
    provider_id: str
    spec: AgentProviderSpec
    source: str  # override | surface | user | instance | default
    surface: str


def resolve_provider_id(
    settings: Settings,
    ctx: AgentInvocationContext,
    *,
    project_tool_config: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Return (provider_id, source)."""
    if ctx.provider_override:
        return ctx.provider_override.strip().lower(), "override"

    # Project tool_config can pin a provider for project surface work.
    if project_tool_config:
        proj = project_tool_config.get("agent_provider") or project_tool_config.get(
            "provider"
        )
        if isinstance(proj, str) and proj.strip():
            return proj.strip().lower(), "project"

    user_id = ctx.user_id()
    # Surface prefs: user file first, then global
    for scope_user in ((user_id,) if user_id else ()) + (None,):
        store = get_preferences_store(settings.data_dir, user_id=scope_user)
        prefs = store.load()
        surface_prefs = (prefs.agent_surfaces or {}).get(ctx.surface)
        if isinstance(surface_prefs, SurfaceAgentPrefs) and surface_prefs.provider:
            source = "surface" if scope_user else "surface_global"
            return surface_prefs.provider.strip().lower(), source
        if isinstance(surface_prefs, dict) and surface_prefs.get("provider"):
            source = "surface" if scope_user else "surface_global"
            return str(surface_prefs["provider"]).strip().lower(), source

    if user_id:
        user_prefs = get_preferences_store(settings.data_dir, user_id=user_id).load()
        if user_prefs.agent_provider:
            return user_prefs.agent_provider.strip().lower(), "user"

    global_prefs = get_preferences_store(settings.data_dir).load()
    if global_prefs.agent_provider:
        return global_prefs.agent_provider.strip().lower(), "instance_prefs"

    if settings.agent_provider:
        return settings.agent_provider.strip().lower(), "instance"

    return DEFAULT_PROVIDER_ID, "default"


def resolve_surface_preferences(
    settings: Settings, ctx: AgentInvocationContext
) -> SurfaceAgentPrefs:
    """Merge global and user defaults for one surface, field by field."""
    global_prefs = get_preferences_store(settings.data_dir).load()
    global_surface = (global_prefs.agent_surfaces or {}).get(ctx.surface)
    if not isinstance(global_surface, SurfaceAgentPrefs):
        global_surface = SurfaceAgentPrefs.model_validate(global_surface or {})

    user_surface = SurfaceAgentPrefs()
    user_id = ctx.user_id()
    if user_id:
        user_prefs = get_preferences_store(
            settings.data_dir, user_id=user_id
        ).load()
        raw = (user_prefs.agent_surfaces or {}).get(ctx.surface)
        if not isinstance(raw, SurfaceAgentPrefs):
            raw = SurfaceAgentPrefs.model_validate(raw or {})
        user_surface = raw

    return SurfaceAgentPrefs(
        provider=user_surface.provider or global_surface.provider,
        model_id=user_surface.model_id or global_surface.model_id,
        mode_id=user_surface.mode_id or global_surface.mode_id,
        effort=user_surface.effort or global_surface.effort,
        config={**global_surface.config, **user_surface.config},
    )


def resolve_agent_provider(
    settings: Settings,
    ctx: AgentInvocationContext,
    *,
    project_tool_config: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
) -> ResolvedAgentProvider:
    provider_id, source = resolve_provider_id(
        settings, ctx, project_tool_config=project_tool_config
    )
    provider = get_provider(provider_id)
    command_override, args_override = _spawn_overrides(settings, provider_id)
    spec = provider.resolve_spawn(
        command_override=command_override,
        args_override=args_override,
        extra_env=extra_env,
        data_dir=settings.data_dir,
    )
    return ResolvedAgentProvider(
        provider_id=provider_id,
        spec=spec,
        source=source,
        surface=ctx.surface,
    )


def _spawn_overrides(
    settings: Settings, provider_id: str
) -> tuple[str | None, list[str] | None]:
    """Return command/args overrides, ignoring legacy Cursor defaults for other providers."""
    cmd = settings.agent_command
    args = settings.agent_args
    if provider_id != "cursor":
        cursor_default_cmd = cmd is None or cmd == "agent"
        cursor_default_args = args is None or args == ["acp"]
        if cursor_default_cmd and cursor_default_args:
            return None, None
    return cmd, args


def provider_session_evidence(manager: Any | None) -> dict[str, int]:
    """Count connected local ACP runtimes without exposing principals or profiles."""
    evidence: dict[str, int] = {}
    if manager is None:
        return evidence
    try:
        runtimes = list(manager.list_runtimes())
    except Exception:
        return evidence
    for runtime in runtimes:
        if getattr(runtime, "_closed", False) or not getattr(runtime, "connected", False):
            continue
        provider_id = str(getattr(getattr(runtime, "session", None), "agent_name", ""))
        if provider_id:
            evidence[provider_id] = evidence.get(provider_id, 0) + 1
    return evidence


def _safe_provider_failure(
    provider: Any, state: str, *, duration_ms: float
) -> dict[str, Any]:
    attempted_at = datetime.now(UTC).isoformat()
    label = "timed out" if state == "timed_out" else "failed"
    return {
        "id": provider.id,
        "display_name": provider.display_name,
        "installed": False,
        "available": False,
        "auth_configured": False,
        "auth_method": "unknown",
        "auth_state": state,
        "auth_status": f"Provider status probe {label}; retry on the target.",
        "auth_error": f"provider status {label}",
        "auth_evidence": [],
        "auth_scope": "service_user",
        "active_session_count": 0,
        "last_attempted_at": attempted_at,
        "last_successful_at": None,
        "probe_duration_ms": round(duration_ms, 1),
        "error": f"provider status {label}",
        "meta": {},
    }


def _apply_session_evidence(
    summary: dict[str, Any], evidence: dict[str, int]
) -> dict[str, Any]:
    result = dict(summary)
    count = evidence.get(str(result.get("id") or ""), 0)
    result["active_session_count"] = count
    if not count:
        return result
    direct_state = str(result.get("auth_state") or "unknown")
    direct_status = result.get("auth_status")
    result["direct_auth_state"] = direct_state
    result["direct_auth_status"] = direct_status
    result["auth_state"] = "authenticated"
    result["auth_configured"] = True
    result["available"] = True
    if direct_state == "authenticated":
        result["auth_scope"] = "service_user_and_active_sessions"
    else:
        result["auth_method"] = "active_acp_session"
        result["auth_scope"] = "active_sessions"
        result["auth_status"] = (
            f"{count} connected {result.get('display_name') or result.get('id')} ACP "
            "session(s) successfully initialized for existing session profiles."
        )
    evidence_labels = list(result.get("auth_evidence") or [])
    if "active_acp_session" not in evidence_labels:
        evidence_labels.append("active_acp_session")
    result["auth_evidence"] = evidence_labels
    return result


def _public_provider_summary(status: Any, evidence: dict[str, int]) -> dict[str, Any]:
    result = (
        status.model_dump(mode="json")
        if hasattr(status, "model_dump")
        else dict(status)
    )
    meta = dict(result.get("meta") or {})
    for key in ("config_path", "interpreter_home", "credential_keys"):
        meta.pop(key, None)
    result["meta"] = meta
    return _apply_session_evidence(result, evidence)


def list_provider_summaries(
    data_dir: Path, *, manager: Any | None = None
) -> list[dict[str, Any]]:
    """Compatibility sync status list with redacted, explicit failure states."""
    evidence = provider_session_evidence(manager)
    out: list[dict[str, Any]] = []
    for provider in list_providers():
        started = time.perf_counter()
        try:
            out.append(_public_provider_summary(provider.status(data_dir), evidence))
        except Exception:
            out.append(
                _apply_session_evidence(
                    _safe_provider_failure(
                        provider,
                        "probe_failed",
                        duration_ms=(time.perf_counter() - started) * 1000,
                    ),
                    evidence,
                )
            )
    return out


async def list_provider_summaries_bounded(
    data_dir: Path,
    *,
    manager: Any | None = None,
    async_runtime: AsyncRuntime | None = None,
    timeout: float = 3.5,
) -> list[dict[str, Any]]:
    """Probe providers independently so one optional integration cannot block peers."""
    evidence = provider_session_evidence(manager)

    async def one(provider: Any) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            if async_runtime is not None:
                status = await async_runtime.run_blocking(
                    f"provider.status.{provider.id}",
                    provider.status,
                    data_dir,
                    timeout=timeout,
                )
            else:
                # CLI and isolated tests have no application executor.
                status = await asyncio.wait_for(
                    asyncio.to_thread(provider.status, data_dir), timeout=timeout
                )
            return _public_provider_summary(status, evidence)
        except (TimeoutError, asyncio.TimeoutError):
            result = _safe_provider_failure(
                provider,
                "timed_out",
                duration_ms=(time.perf_counter() - started) * 1000,
            )
        except Exception:
            result = _safe_provider_failure(
                provider,
                "probe_failed",
                duration_ms=(time.perf_counter() - started) * 1000,
            )
        return _apply_session_evidence(result, evidence)

    return list(await asyncio.gather(*(one(provider) for provider in list_providers())))
