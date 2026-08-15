"""Post-start / post-restart terminal summary for the host service."""

from __future__ import annotations

import time

import httpx
import typer

from pa import __version__
from pa.auth.users import UserDirectory
from pa.cli import presentation as ui
from pa.cli import service as svc
from pa.config import Settings


def local_web_url(settings: Settings) -> str:
    """URL openable on this machine (never advertise 0.0.0.0)."""
    host = settings.host
    if host in ("0.0.0.0", "::", "[::]"):
        host = "127.0.0.1"
    elif host == "localhost":
        host = "127.0.0.1"
    return f"http://{host}:{settings.port}"


def wait_for_health(url: str, *, timeout_s: float = 12.0) -> bool:
    """Process liveness only. Prefer wait_for_ready for operator admission."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=2.0) as client:
                resp = client.get(f"{url.rstrip('/')}/api/health")
                if resp.status_code == 200:
                    return True
        except httpx.HTTPError:
            pass
        time.sleep(0.35)
    return False


def wait_for_ready(
    settings: Settings,
    url: str,
    *,
    timeout_s: float = 120.0,
) -> bool:
    """Poll /api/ready until local initialization has finished."""
    token = ""
    try:
        token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    except OSError:
        token = ""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=2.0) as client:
                resp = client.get(f"{url.rstrip('/')}/api/ready", headers=headers)
                if resp.status_code == 200:
                    return True
        except httpx.HTTPError:
            pass
        time.sleep(0.35)
    return False


def print_service_ready(settings: Settings, *, action: str = "started") -> None:
    """Print web URL and useful startup info after the service is up."""
    local = local_web_url(settings)
    advertised = (settings.instance_url or "").rstrip("/")
    ready = wait_for_ready(settings, local)
    status = svc.get_status(settings)

    ui.echo(f"PA service {action}.", style="success")
    typer.echo(f"  Web UI:      {local}")
    if advertised and advertised.rstrip("/") != local.rstrip("/"):
        typer.echo(f"  Advertised:  {advertised}")
    if ready:
        ui.status("OK", "Ready:       ok")
    else:
        ui.status("WARN", "Ready:       not ready yet — try pa logs -f")
    typer.echo(f"  Instance:    {settings.instance_name} ({settings.instance_id})")
    typer.echo(f"  Version:     {__version__}")
    typer.echo(f"  Data:        {settings.data_dir}")
    if status.backend != "none":
        if status.running:
            state = "running"
        elif status.loaded:
            state = "loaded"
        elif status.installed:
            state = "stopped"
        else:
            state = "not installed"
        typer.echo(f"  Service:     {state} ({status.backend})")
        typer.echo(f"  Logs:        {status.log_path}")
    if settings.subscribed_realms:
        typer.echo(f"  Realms:      {', '.join(settings.subscribed_realms)}")
    if settings.peers:
        typer.echo(f"  Peers:       {len(settings.peers)} configured")
    typer.echo("  Commands:    pa status | pa logs -f | pa doctor | pa stop")
