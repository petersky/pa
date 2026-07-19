"""Resolve ACP provider from surface → user → instance cascade."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pa.acp.providers.base import AgentProviderSpec
from pa.acp.providers.registry import DEFAULT_PROVIDER_ID, get_provider, list_providers
from pa.acp.surfaces import AgentInvocationContext
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


def list_provider_summaries(data_dir: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for provider in list_providers():
        try:
            st = provider.status(data_dir)
            out.append(st.model_dump(mode="json"))
        except Exception as exc:
            out.append(
                {
                    "id": provider.id,
                    "display_name": provider.display_name,
                    "available": False,
                    "error": str(exc),
                }
            )
    return out
