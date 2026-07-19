"""Post-install health checks for PA instances."""

from __future__ import annotations

import asyncio
import sys

import httpx
import typer

from pa.cli import service as svc
from pa.config import get_settings
from pa.domain.instance_config import load_instance_config
from pa.install.metadata import load_install_metadata


async def _check_health(url: str, sync_token: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            headers = {}
            if sync_token:
                headers["Authorization"] = f"Bearer {sync_token}"
            resp = await client.get(f"{url.rstrip('/')}/api/health", headers=headers)
            return resp.status_code == 200
    except httpx.HTTPError:
        return False


async def _check_peers(peers: list[str], sync_token: str) -> list[tuple[str, bool]]:
    results: list[tuple[str, bool]] = []
    async with httpx.AsyncClient(timeout=5.0) as client:
        for peer in peers:
            try:
                headers = {}
                if sync_token:
                    headers["Authorization"] = f"Bearer {sync_token}"
                resp = await client.get(
                    f"{peer.rstrip('/')}/api/health", headers=headers
                )
                results.append((peer, resp.status_code == 200))
            except httpx.HTTPError:
                results.append((peer, False))
    return results


def run_doctor() -> int:
    """Run health checks. Returns exit code (0 = ok, 1 = failures)."""
    settings = get_settings()
    config = load_instance_config(settings.data_dir)
    install_meta = load_install_metadata(settings.data_dir)
    svc_status = svc.get_status(settings)
    failures: list[str] = []
    warnings: list[str] = []

    typer.echo(f"PA doctor — {settings.instance_name}")
    typer.echo("")

    pa_bin = svc.find_pa_binary()
    if pa_bin:
        typer.echo(f"  [ok]   Binary: {pa_bin}")
    else:
        failures.append("pa binary not found in PATH")

    if config:
        typer.echo(f"  [ok]   Config: {settings.data_dir / 'config.json'}")
        typer.echo(f"         instance_id={config.instance_id}")
        typer.echo(f"         fleet_id={config.fleet_id}")
        typer.echo(f"         realms={', '.join(config.subscribed_realms)}")
        typer.echo(f"         track={config.release_track}")
    else:
        failures.append("config.json missing — run pa init")

    if install_meta:
        typer.echo(
            f"  [ok]   Install: v{install_meta.version} ({install_meta.channel})"
        )
    else:
        warnings.append("install.json missing — run pa install --record-only")

    if svc.service_supported():
        if svc_status.installed:
            state = "running" if svc_status.running else "stopped"
            tag = "ok" if svc_status.running else "warn"
            typer.echo(f"  [{tag}] Service ({svc_status.backend}): {state}")
            if not svc_status.running:
                warnings.append(f"service not running ({svc_status.backend})")
        else:
            warnings.append(
                "service unit not installed — run pa install --service-only"
            )
    else:
        warnings.append(f"no service manager on {sys.platform}")

    instance_url = settings.instance_url or f"http://{settings.host}:{settings.port}"
    typer.echo(f"  [..]   Instance URL: {instance_url}")

    reachable = asyncio.run(_check_health(instance_url, settings.sync_token))
    if reachable:
        typer.echo("  [ok]   Health endpoint reachable")
    elif svc_status.running:
        warnings.append(f"health check failed for {instance_url}")
    else:
        warnings.append(
            f"instance not reachable at {instance_url} (service may be stopped)"
        )

    if settings.peers:
        typer.echo(f"  [..]   Peers ({len(settings.peers)}):")
        for peer, ok in asyncio.run(_check_peers(settings.peers, settings.sync_token)):
            tag = "ok" if ok else "fail"
            typer.echo(f"         [{tag}]  {peer}")
            if not ok:
                warnings.append(f"peer unreachable: {peer}")
    else:
        typer.echo("  [info] No peers configured")

    from pa.acp.providers.resolve import list_provider_summaries

    typer.echo("  [..]   ACP providers:")
    for st in list_provider_summaries(settings.data_dir):
        tag = "ok" if st.get("available") else "warn"
        ver = st.get("version") or "—"
        auth = "auth=yes" if st.get("auth_configured") else "auth=?"
        typer.echo(
            f"         [{tag}]  {st.get('id')}: available={st.get('available')} "
            f"version={ver} {auth}"
        )
        if not st.get("available"):
            warnings.append(f"ACP provider not available: {st.get('id')}")

    from pa.sync.infrastructure import get_event_log, get_object_store

    event_log = get_event_log(settings)
    obj_store = get_object_store(settings)
    for realm in settings.subscribed_realms:
        head = event_log.get_head(realm) or "—"
        typer.echo(
            f"  [ok]   Sync {realm}: head={head} objects={len(obj_store.list_hashes())}"
        )

    typer.echo("")
    for w in warnings:
        typer.echo(f"  warning: {w}")
    for f in failures:
        typer.echo(f"  FAIL: {f}", err=True)

    if failures:
        typer.echo("")
        typer.echo("Doctor found failures.", err=True)
        return 1
    typer.echo("Doctor checks passed." + (" (with warnings)" if warnings else ""))
    return 0
