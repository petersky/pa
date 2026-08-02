"""Post-install health checks for PA instances."""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import typer

from pa import __version__
from pa.acp.environment import (
    private_provider_environment_names,
    sanitize_provider_environment,
)
from pa.acp.mcp_config import (
    McpHandshakeError,
    OwnerChannelError,
    owner_endpoint,
    pa_mcp_servers,
    probe_owner_channel,
    probe_pa_mcp_stdio,
)
from pa.cli import service as svc
from pa.config import get_settings
from pa.domain.instance_config import load_instance_config
from pa.install.metadata import load_install_metadata
from pa.packaging.service_env import service_environment, service_values_equal


async def _check_health(url: str, sync_token: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            headers = {}
            if sync_token:
                headers["Authorization"] = f"Bearer {sync_token}"
            response = await client.get(
                f"{url.rstrip('/')}/api/health", headers=headers
            )
            return response.status_code == 200
    except httpx.HTTPError:
        return False


async def _fetch_status(url: str, sync_token: str) -> dict[str, Any] | None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            headers = {}
            if sync_token:
                headers["Authorization"] = f"Bearer {sync_token}"
            response = await client.get(
                f"{url.rstrip('/')}/api/status", headers=headers
            )
            if response.status_code != 200:
                return None
            payload = response.json()
            return payload if isinstance(payload, dict) else None
    except httpx.HTTPError, ValueError:
        return None


async def _check_peers(peers: list[str], sync_token: str) -> list[tuple[str, bool]]:
    results: list[tuple[str, bool]] = []
    async with httpx.AsyncClient(timeout=5.0) as client:
        for peer in peers:
            try:
                headers = {}
                if sync_token:
                    headers["Authorization"] = f"Bearer {sync_token}"
                response = await client.get(
                    f"{peer.rstrip('/')}/api/health", headers=headers
                )
                results.append((peer, response.status_code == 200))
            except httpx.HTTPError:
                results.append((peer, False))
    return results


def _binary_version(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        result = subprocess.run(
            [str(path), "version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except OSError, subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    match = re.search(r"\bpa\s+v?([0-9][^\s]*)", result.stdout, re.IGNORECASE)
    return match.group(1) if match else None


def _same_path(left: str | Path | None, right: str | Path | None) -> bool:
    if not left or not right:
        return False
    try:
        return Path(left).expanduser().resolve() == Path(right).expanduser().resolve()
    except OSError:
        return str(left) == str(right)


def _check_loaded_service(
    settings,
    svc_status,
    service_bin: Path | None,
    failures: list[str],
    warnings: list[str],
) -> svc.LoadedServiceDefinition | None:
    if not svc.service_supported():
        warnings.append(f"no service manager on {sys.platform}")
        return None
    if not svc_status.installed:
        warnings.append("service unit not installed — run pa install --service-only")
        return None

    state = "running" if svc_status.running else "stopped"
    tag = "ok" if svc_status.running else "warn"
    typer.echo(f"  [{tag}] Service ({svc_status.backend}): {state}")
    if not svc_status.running:
        warnings.append(f"service not running ({svc_status.backend})")
        return None

    loaded = svc.loaded_service_definition()
    if loaded is None:
        failures.append(
            "loaded service definition unavailable — inspect the service manager and reload PA"
        )
        return None
    typer.echo(f"  [ok]   Loaded service: {loaded.command or 'command unknown'}")
    if service_bin and loaded.command and not _same_path(service_bin, loaded.command):
        failures.append(
            "loaded service binary differs from the installed service binary "
            f"({loaded.command} != {service_bin}) — reinstall/reload the service"
        )
    expected_environment = service_environment(settings)
    for name, expected in expected_environment.items():
        if not name.startswith("PA_"):
            continue
        actual = loaded.environment.get(name)
        if not service_values_equal(name, expected, actual):
            failures.append(
                f"loaded service environment mismatch for {name} "
                f"(expected from config/install: {expected!r}; "
                f"actual from {loaded.backend}: {actual!r}) — reinstall/reload the service"
            )
    return loaded


def _check_owner_and_mcp(
    settings,
    owner_environment,
    public_status: dict[str, Any] | None,
    failures: list[str],
) -> None:
    endpoint = owner_endpoint(settings, owner_environment)
    typer.echo(
        "  [..]   Owner channel: "
        + (
            f"unix://{endpoint.uds}"
            if endpoint.uds
            else f"{endpoint.kind} {endpoint.url}"
        )
    )
    owner_ok = False
    try:
        owner_health = probe_owner_channel(
            settings,
            timeout=4.0,
            environment=owner_environment,
        )
    except OwnerChannelError as exc:
        failures.append(
            f"owner-channel {exc.classification} (endpoint={exc.endpoint_kind}) — {exc.recovery}"
        )
    else:
        owner_ok = True
        typer.echo(f"  [ok]   Owner readiness/auth/identity: {owner_health['state']}")

    if public_status:
        reported_owner = dict(public_status.get("owner_channel") or {})
        if reported_owner.get("endpoint_type") != endpoint.kind:
            failures.append(
                "status owner endpoint type differs from the loaded service configuration"
            )
        if reported_owner.get("state") not in {"bound", "connected"}:
            failures.append(
                "status reports owner-channel failure "
                f"({reported_owner.get('failure_classification') or reported_owner.get('state')})"
            )
        provider_environment = dict(
            public_status.get("provider_execution_environment") or {}
        )
        if not provider_environment.get("sanitized"):
            failures.append(
                "running service does not report a sanitized provider execution environment"
            )
        if provider_environment.get("private_variables_present"):
            failures.append(
                "running provider execution environment contains private PA variables"
            )

    sanitized = sanitize_provider_environment(owner_environment)
    leaked = private_provider_environment_names(sanitized)
    if leaked:
        failures.append(
            "provider environment sanitizer retained private variables: "
            + ", ".join(leaked)
        )
    else:
        typer.echo("  [ok]   Provider environment excludes private PA controls")

    bridge = pa_mcp_servers(
        settings,
        owner_environment=owner_environment,
    )[0]
    bridge_environment = {item.name: item.value for item in bridge.env}
    expected_bridge = {
        "PA_DATA_DIR": str(settings.data_dir),
        "PA_INSTANCE_ID": settings.instance_id,
        "PA_LOCAL_API_URL": endpoint.url,
        "PA_LOCAL_API_ENDPOINT_TYPE": endpoint.kind,
    }
    if endpoint.uds:
        expected_bridge["PA_LOCAL_API_SOCKET"] = endpoint.uds
    for name, expected in expected_bridge.items():
        if bridge_environment.get(name) != expected:
            failures.append(f"pinned PA MCP environment mismatch for {name}")
    if not bridge_environment.get("PA_LOCAL_API_TOKEN"):
        failures.append("pinned PA MCP environment is missing its owner token")

    if not owner_ok:
        typer.echo("  [skip] MCP handshake blocked by owner-channel failure")
        return
    try:
        mcp_health = probe_pa_mcp_stdio(
            settings,
            timeout=12.0,
            owner_environment=owner_environment,
        )
    except McpHandshakeError as exc:
        failures.append(f"MCP handshake {exc.classification} — {exc.recovery}")
    else:
        typer.echo(
            "  [ok]   PA stdio MCP initialize/tools-list/shutdown "
            f"({mcp_health['tool_count']} tools)"
        )


def run_doctor() -> int:
    """Run end-to-end health checks. Returns 0 only when required checks pass."""
    settings = get_settings()
    config = load_instance_config(settings.data_dir)
    install_meta = load_install_metadata(settings.data_dir)
    svc_status = svc.get_status(settings)
    failures: list[str] = []
    warnings: list[str] = []

    typer.echo(f"PA doctor — {settings.instance_name}")
    typer.echo("")

    pa_bin = svc.find_pa_binary()
    service_bin = svc.find_service_binary()
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
        if config.instance_id != settings.instance_id:
            failures.append(
                "resolved config instance mismatch — verify PA_DATA_DIR and the loaded service environment"
            )
        if config.fleet_id != settings.fleet_id:
            failures.append(
                "resolved config fleet mismatch — reload the service from config.json"
            )
    else:
        failures.append("config.json missing — run pa init")

    if install_meta:
        typer.echo(
            f"  [ok]   Install: v{install_meta.version} ({install_meta.channel})"
        )
    else:
        warnings.append("install.json missing — run pa install --record-only")

    loaded_service = _check_loaded_service(
        settings, svc_status, service_bin, failures, warnings
    )

    installed_version = install_meta.version if install_meta else None
    binary_version = _binary_version(pa_bin)
    service_binary_version = _binary_version(service_bin)
    if binary_version and binary_version != __version__:
        failures.append(
            f"resolved CLI version {binary_version} differs from doctor version {__version__}"
        )
    if (
        installed_version
        and service_binary_version
        and (installed_version != service_binary_version)
    ):
        failures.append(
            "installed metadata version differs from the service binary version "
            f"({installed_version} != {service_binary_version})"
        )

    instance_url = settings.instance_url or f"http://{settings.host}:{settings.port}"
    typer.echo(f"  [..]   Instance URL: {instance_url}")
    if asyncio.run(_check_health(instance_url, settings.sync_token)):
        typer.echo("  [ok]   Health endpoint reachable")
    else:
        failures.append(f"public health check failed for {instance_url}")

    public_status = asyncio.run(_fetch_status(instance_url, settings.sync_token))
    if public_status:
        typer.echo("  [ok]   Status endpoint reachable")
        running_version = str(public_status.get("version") or "")
        if (
            running_version
            and installed_version
            and running_version != installed_version
        ):
            failures.append(
                "running service version differs from installed metadata "
                f"({running_version} != {installed_version}) — restart PA"
            )
        if public_status.get("instance_id") != settings.instance_id:
            failures.append(
                "public status instance identity differs from resolved config"
            )
    else:
        failures.append("public status endpoint unavailable or invalid")

    owner_environment = dict(
        loaded_service.environment if loaded_service is not None else os.environ
    )
    # The running process is authoritative for its private ephemeral endpoint.
    # A CLI must not fabricate a fallback path from its own environment.
    reported_owner = dict((public_status or {}).get("owner_channel") or {})
    discovered_socket = str(reported_owner.get("socket_path") or "").strip()
    if reported_owner.get("endpoint_type") == "unix" and discovered_socket:
        derived_socket = owner_endpoint(settings, owner_environment).uds
        owner_environment["PA_OWNER_SOCKET"] = discovered_socket
        if derived_socket and derived_socket != discovered_socket:
            typer.echo(
                "  [info] Owner endpoint discovered from live status "
                f"({discovered_socket}; local derivation was {derived_socket})"
            )
    _check_owner_and_mcp(
        settings,
        owner_environment,
        public_status,
        failures,
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
    for provider in list_provider_summaries(settings.data_dir):
        tag = "ok" if provider.get("available") else "warn"
        version = provider.get("version") or "—"
        auth = "auth=yes" if provider.get("auth_configured") else "auth=?"
        typer.echo(
            f"         [{tag}]  {provider.get('id')}: "
            f"available={provider.get('available')} version={version} {auth}"
        )
        if not provider.get("available"):
            warnings.append(f"ACP provider not available: {provider.get('id')}")

    from pa.sync.infrastructure import get_event_log, get_object_store

    event_log = get_event_log(settings)
    object_store = get_object_store(settings)
    for realm in settings.subscribed_realms:
        head = event_log.get_head(realm) or "—"
        typer.echo(
            f"  [ok]   Sync {realm}: "
            f"head={head} objects={len(object_store.list_hashes())}"
        )

    typer.echo("")
    for warning in warnings:
        typer.echo(f"  warning: {warning}")
    for failure in failures:
        typer.echo(f"  FAIL: {failure}", err=True)

    if failures:
        typer.echo("")
        typer.echo("Doctor found failures.", err=True)
        return 1
    typer.echo("Doctor checks passed." + (" (with warnings)" if warnings else ""))
    return 0
