"""CLI for ACP provider lifecycle on local and fleet hosts."""

from __future__ import annotations

import json
from typing import Annotated, Optional

import httpx
import typer

from pa.acp.providers.base import ProviderConfigureBody
from pa.acp.providers.registry import get_provider, list_providers
from pa.acp.providers.resolve import list_provider_summaries
from pa.config import get_settings
from pa.fleet.registry import FleetRegistry

agent_provider_app = typer.Typer(help="Manage ACP agent providers (Cursor, Codex, …)")


def _remote(
    instance_id: str,
    method: str,
    path: str,
    body: dict | None = None,
) -> object:
    settings = get_settings()
    fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
    inst = None
    for candidate in fleet.list_instances():
        if candidate.instance_id == instance_id:
            inst = candidate
            break
    if not inst:
        raise typer.BadParameter(f"Unknown fleet instance: {instance_id}")
    headers: dict[str, str] = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    url = f"{inst.url.rstrip('/')}{path}"
    with httpx.Client(timeout=120.0) as client:
        resp = client.request(method, url, headers=headers, json=body)
    if resp.status_code >= 400:
        typer.echo(resp.text, err=True)
        raise typer.Exit(1)
    return resp.json()


@agent_provider_app.command("list")
def list_cmd(
    instance: Annotated[
        Optional[str], typer.Option("--instance", help="Fleet instance id")
    ] = None,
) -> None:
    """List registered ACP providers and local status."""
    if instance:
        data = _remote(instance, "GET", "/api/agent/providers")
    else:
        data = list_provider_summaries(get_settings().data_dir)
    typer.echo(json.dumps(data, indent=2))


@agent_provider_app.command("status")
def status_cmd(
    provider: Annotated[str, typer.Option("--provider", "-p")] = "codex",
    instance: Annotated[Optional[str], typer.Option("--instance")] = None,
) -> None:
    """Show install/auth status for one provider."""
    if instance:
        data = _remote(instance, "GET", f"/api/agent/providers/{provider}")
    else:
        data = get_provider(provider).status(get_settings().data_dir).model_dump(
            mode="json"
        )
    typer.echo(json.dumps(data, indent=2))


@agent_provider_app.command("install")
def install_cmd(
    provider: Annotated[str, typer.Option("--provider", "-p")] = "codex",
    instance: Annotated[Optional[str], typer.Option("--instance")] = None,
) -> None:
    """Install or verify a provider on this host or a fleet peer."""
    if instance:
        data = _remote(instance, "POST", f"/api/agent/providers/{provider}/install")
    else:
        data = get_provider(provider).install(get_settings().data_dir).model_dump(
            mode="json"
        )
    typer.echo(json.dumps(data, indent=2))
    if isinstance(data, dict) and not data.get("ok", True):
        raise typer.Exit(1)


@agent_provider_app.command("update")
def update_cmd(
    provider: Annotated[str, typer.Option("--provider", "-p")] = "codex",
    instance: Annotated[Optional[str], typer.Option("--instance")] = None,
) -> None:
    """Update a provider package/binary."""
    if instance:
        data = _remote(instance, "POST", f"/api/agent/providers/{provider}/update")
    else:
        data = get_provider(provider).update(get_settings().data_dir).model_dump(
            mode="json"
        )
    typer.echo(json.dumps(data, indent=2))
    if isinstance(data, dict) and not data.get("ok", True):
        raise typer.Exit(1)


@agent_provider_app.command("configure")
def configure_cmd(
    provider: Annotated[str, typer.Option("--provider", "-p")] = "codex",
    instance: Annotated[Optional[str], typer.Option("--instance")] = None,
    api_key: Annotated[
        Optional[str], typer.Option("--api-key", help="CODEX_API_KEY / OPENAI_API_KEY")
    ] = None,
    no_browser: Annotated[bool, typer.Option("--no-browser/--browser")] = True,
    env_json: Annotated[
        Optional[str], typer.Option("--env-json", help="Extra env as JSON object")
    ] = None,
) -> None:
    """Configure provider env and optional API key (stored only on target host)."""
    env: dict[str, str] = {}
    if env_json:
        env.update(json.loads(env_json))
    secrets: dict[str, str] = {}
    if api_key:
        secrets["CODEX_API_KEY"] = api_key
    body = {
        "env": env,
        "secrets": secrets,
        "no_browser": no_browser if provider == "codex" else None,
    }
    if instance:
        data = _remote(
            instance, "POST", f"/api/agent/providers/{provider}/configure", body=body
        )
    else:
        data = (
            get_provider(provider)
            .configure(get_settings().data_dir, ProviderConfigureBody(**body))
            .model_dump(mode="json")
        )
    typer.echo(json.dumps(data, indent=2))


@agent_provider_app.command("probe")
def probe_cmd(
    provider: Annotated[str, typer.Option("--provider", "-p")] = "codex",
    instance: Annotated[Optional[str], typer.Option("--instance")] = None,
) -> None:
    """Run ACP initialize probe against a provider."""
    if instance:
        data = _remote(instance, "POST", f"/api/agent/providers/{provider}/probe")
    else:
        data = get_provider(provider).probe(get_settings().data_dir)
    typer.echo(json.dumps(data, indent=2))
    if isinstance(data, dict) and not data.get("ok", False):
        raise typer.Exit(1)


@agent_provider_app.command("ids")
def ids_cmd() -> None:
    """Print registered provider ids."""
    for p in list_providers():
        typer.echo(f"{p.id}\t{p.display_name}")
