import json
import sys
from pathlib import Path
from typing import Annotated

import typer
import uvicorn

from pa import __version__
from pa.config import Settings, get_settings, reset_settings
from pa.core.kernel import Kernel, reset_kernel
from pa.core.registry import ENTRYPOINT_GROUP
from pa.domain.store import get_store
from pa.install.metadata import load_install_metadata

app = typer.Typer(
    name="pa",
    help="PA — human–agent orchestration",
    no_args_is_help=True,
)

plugins_app = typer.Typer(help="Discover and inspect plugins")
app.add_typer(plugins_app, name="plugins")

release_app = typer.Typer(help="Create releases (maintainers)")
app.add_typer(release_app, name="release")

channel_app = typer.Typer(help="Release tracks for install and update")
app.add_typer(channel_app, name="channel")

fleet_app = typer.Typer(help="Fleet management")
app.add_typer(fleet_app, name="fleet")

realm_app = typer.Typer(help="Realm management")
app.add_typer(realm_app, name="realm")

sync_app = typer.Typer(help="Sync status and control")
app.add_typer(sync_app, name="sync")

project_app = typer.Typer(help="Project management")
app.add_typer(project_app, name="project")

user_app = typer.Typer(help="User management")
app.add_typer(user_app, name="user")

from pa.cli.agent_provider import agent_provider_app

app.add_typer(agent_provider_app, name="agent-provider")


@app.command()
def version() -> None:
    """Show PA version."""
    typer.echo(f"pa {__version__}")


@app.command()
def init(
    name: Annotated[str, typer.Option(help="Instance name")] = "local",
    data_dir: Annotated[str | None, typer.Option(help="Data directory")] = None,
    fleet_id: Annotated[str | None, typer.Option(help="Fleet ID")] = None,
    realm: Annotated[str | None, typer.Option(help="Primary realm ID")] = None,
    url: Annotated[str | None, typer.Option(help="Public instance URL (Tailscale)")] = None,
    track: Annotated[str | None, typer.Option(help="Release track")] = None,
    peers: Annotated[str | None, typer.Option(help="Comma-separated peer URLs")] = None,
    sync_token: Annotated[str | None, typer.Option(help="Shared sync token")] = None,
) -> None:
    """Initialize a PA instance (creates data directory and config)."""
    from pa.domain.instance_config import InstanceConfig, save_instance_config

    reset_settings()
    reset_kernel()
    kwargs: dict = {"instance_name": name}
    if data_dir:
        kwargs["data_dir"] = Path(data_dir)
    settings = Settings(**kwargs)
    settings.ensure_dirs()

    peer_list = [p.strip() for p in peers.split(",") if p.strip()] if peers else []

    config = InstanceConfig(
        instance_id=settings.instance_id,
        instance_name=settings.instance_name,
        data_dir=str(settings.data_dir),
        fleet_id=fleet_id or settings.fleet_id,
        fleet_owner="local",
        instance_url=url or "",
        subscribed_realms=[realm] if realm else list(settings.subscribed_realms),
        zone=settings.zone,
        capabilities=list(settings.capabilities),
        relay_enabled=settings.relay_enabled,
        peers=peer_list,
        release_track=track or settings.release_track,
        sync_token=sync_token or "",
        session_secret=settings.session_secret,
    )
    save_instance_config(settings.data_dir, config)
    typer.echo(f"Initialized PA instance '{settings.instance_name}'")
    typer.echo(f"  ID:    {settings.instance_id}")
    typer.echo(f"  Fleet: {config.fleet_id}")
    typer.echo(f"  Realm: {', '.join(config.subscribed_realms)}")
    if config.instance_url:
        typer.echo(f"  URL:   {config.instance_url}")
    typer.echo(f"  Data:  {settings.data_dir}")


@app.command()
def status() -> None:
    """Show current instance status."""
    from pa.status.info import build_status_snapshot

    settings = get_settings()
    kernel = Kernel.boot(load_modules=True)
    snap = build_status_snapshot(kernel.ctx, module_count=len(kernel.registry.modules))

    typer.echo(f"PA {snap['version']} — {snap['instance_name']}")
    typer.echo(f"  Instance ID: {snap['instance_id']}")
    typer.echo(f"  Data dir:    {snap['data_dir']}")
    typer.echo(f"  Server:      {snap['server_url']}")
    typer.echo(f"  Binary:      {snap['binary'] or 'not found'}")
    if snap.get("service_binary") and snap["service_binary"] != snap["binary"]:
        typer.echo(f"  Service bin: {snap['service_binary']}")
    if snap["installed_version"]:
        typer.echo(f"  Installed:   {snap['installed_version']} ({snap['install_method']})")
        if snap.get("install_channel"):
            typer.echo(f"  Track:       {snap['install_channel']}")
        if snap["installed_version"] != snap["version"]:
            typer.echo(
                f"  Note:       running {snap['version']}; restart service to apply {snap['installed_version']}"
            )
    typer.echo(f"  Update:      {snap['release_track']} track")
    from pa.cli import service as svc

    svc_info = snap["service"]
    if svc_info["installed"] or svc.service_supported():
        typer.echo(
            f"  Service:     {svc_info['state']} ({svc_info['backend']})"
        )
        if svc_info["installed"]:
            typer.echo(f"  Unit:        {svc_info['unit_path']}")
    typer.echo(f"  Debug:       {snap['debug']}")
    typer.echo(f"  Agent:       {'enabled' if snap['agent_enabled'] else 'disabled'}")
    typer.echo(f"  Fleet:       {snap['fleet_id']}")
    typer.echo(f"  Realms:      {', '.join(snap['realms'])}")
    typer.echo(f"  Zone:        {snap['zone']}")
    typer.echo(f"  Peers:       {snap['peer_count']}")
    typer.echo(f"  Modules:     {snap['module_count']}")
    typer.echo(f"  Items:       {snap['item_count']}")
    typer.echo(f"  Sessions:    {snap['session_count']}")
    typer.echo(f"  Knowledge:   {snap['knowledge_count']} recent entries")


@app.command()
def install(
    service_only: Annotated[
        bool,
        typer.Option("--service-only", help="Only register/reload host service unit"),
    ] = False,
    record_only: Annotated[
        bool,
        typer.Option("--record-only", help="Only write install.json metadata"),
    ] = False,
    channel: Annotated[str, typer.Option(help="Release track for install metadata")] = "release",
    name: Annotated[str, typer.Option(help="Instance name")] = "local",
    no_start: Annotated[bool, typer.Option(help="Do not start service after install")] = False,
    from_source: Annotated[
        Path | None,
        typer.Option("--from-source", help="Install from local path (repo root)"),
    ] = None,
) -> None:
    """Install PA on the host (uv tool + init + launchd/systemd)."""
    from pa.cli import service as svc
    from pa.install.runner import install_from_path, record_install

    if record_only:
        record_install(channel=channel, pa_bin=svc.find_pa_binary())
        typer.echo(f"Recorded install metadata (track: {channel}).")
        return

    if service_only:
        if not svc.service_supported():
            typer.echo("Service management is not supported on this platform.", err=True)
            raise typer.Exit(1)
        settings = get_settings()
        pa_bin = svc.find_service_binary()
        if not pa_bin:
            typer.echo("pa binary not found in PATH.", err=True)
            raise typer.Exit(1)
        path = svc.install_service(settings, pa_bin)
        svc.bootstrap()
        record_install(channel=channel, pa_bin=pa_bin)
        typer.echo(f"Registered {svc.get_status(settings).backend} service: {path}")
        typer.echo(f"Service binary: {pa_bin}")
        return

    try:
        install_from_path(from_source, name=name, channel=channel, start_service=not no_start)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    settings = get_settings()
    typer.echo("PA installed successfully.")
    typer.echo(f"  Server: http://{settings.host}:{settings.port}")
    typer.echo("  Commands: pa status | pa restart | pa update")


@app.command()
def start(
    no_acp_resume: Annotated[
        bool,
        typer.Option("--no-acp-resume", help="Do not resume quiesced ACP sessions on startup"),
    ] = False,
) -> None:
    """Start the PA host service (launchd or systemd)."""
    from pa.cli import service as svc
    from pa.cli.acp_lifecycle import mark_no_resume
    from pa.cli.startup import print_service_ready

    settings = get_settings()
    if no_acp_resume:
        mark_no_resume(settings)
    try:
        svc.start(settings)
        print_service_ready(settings, action="started")
        if no_acp_resume:
            typer.echo("  ACP:         resume disabled for this start")
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@app.command()
def stop(
    no_acp_quiesce: Annotated[
        bool,
        typer.Option("--no-acp-quiesce", help="Skip waiting for ACP sessions before stop"),
    ] = False,
) -> None:
    """Stop the PA host service."""
    from pa.cli import service as svc
    from pa.cli.acp_lifecycle import quiesce_running_agent
    from pa.instance.quiesce import request_skip_quiesce

    settings = get_settings()
    if not no_acp_quiesce:
        result = quiesce_running_agent(settings, reason="stop")
        if result is not None:
            request_skip_quiesce(settings.data_dir)
    else:
        request_skip_quiesce(settings.data_dir)
    try:
        svc.stop()
        typer.echo("PA service stopped.")
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@app.command(name="restart")
def restart_cmd(
    no_acp_quiesce: Annotated[
        bool,
        typer.Option("--no-acp-quiesce", help="Skip waiting for ACP sessions before restart"),
    ] = False,
    no_acp_resume: Annotated[
        bool,
        typer.Option("--no-acp-resume", help="Do not resume quiesced ACP sessions after restart"),
    ] = False,
) -> None:
    """Restart the PA host service."""
    from pa.cli import service as svc
    from pa.cli.acp_lifecycle import mark_no_resume, quiesce_running_agent
    from pa.cli.startup import print_service_ready
    from pa.instance.quiesce import request_skip_quiesce

    settings = get_settings()
    if no_acp_quiesce:
        request_skip_quiesce(settings.data_dir)
    else:
        result = quiesce_running_agent(settings, reason="restart")
        if result is not None:
            request_skip_quiesce(settings.data_dir)
    if no_acp_resume:
        mark_no_resume(settings)
    try:
        svc.restart(settings)
        print_service_ready(settings, action="restarted")
        if no_acp_resume:
            typer.echo("  ACP:         resume disabled for this restart")
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@app.command()
def logs(
    follow: Annotated[bool, typer.Option("-f", "--follow", help="Follow log output")] = False,
    lines: Annotated[int, typer.Option("-n", help="Number of lines")] = 50,
) -> None:
    """Tail PA server logs."""
    from pa.cli import service as svc

    try:
        svc.tail_logs(lines=lines, follow=follow)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@app.command()
def update(
    check: Annotated[bool, typer.Option("--check", help="Check for updates only")] = False,
    restart: Annotated[
        bool,
        typer.Option("--restart", help="Restart service after update without prompting"),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Apply update without confirmation"),
    ] = False,
    channel: Annotated[
        str | None,
        typer.Option(help="Release track: release, beta, alpha, dev, or pypi"),
    ] = None,
) -> None:
    """Check for and install PA updates."""
    from pa.cli import service as svc
    from pa.update.runner import apply_update, check_update, format_release_notes

    settings = get_settings()
    result = check_update(settings, channel_name=channel)

    typer.echo(f"Installed: {result.current}")
    typer.echo(f"Latest:    {result.latest or 'unknown'}")

    if not result.upgrade_available:
        typer.echo("Up to date.")
        return

    typer.echo("Update available.")
    typer.echo("")
    typer.echo(format_release_notes(result.release))
    typer.echo("")

    if check:
        raise typer.Exit(1)

    if not yes and not typer.confirm(f"Apply update to {result.latest}?", default=True):
        typer.echo("Update cancelled.")
        raise typer.Exit(0)

    try:
        result = apply_update(
            settings,
            channel_name=channel,
            restart=False,
            release=result.release,
        )
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    previous = result.previous or result.current
    typer.echo(f"Updated PA {previous} → {result.current}")

    status = svc.get_status(settings)
    if not status.installed:
        return

    should_restart = restart
    if not should_restart and not yes:
        if status.running:
            should_restart = typer.confirm(
                "PA service is running. Restart it to load the new version?",
                default=True,
            )
        else:
            should_restart = typer.confirm(
                "Restart the PA service now?",
                default=False,
            )

    if not should_restart:
        if status.running:
            typer.echo("Service left running. Run: pa restart")
        else:
            typer.echo("Service left stopped. Run: pa start")
        return

    try:
        svc.restart(settings)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo("Service restarted.")


def _release_command(bump: str):
    def command(
        no_push: Annotated[
            bool,
            typer.Option("--no-push", help="Skip pushing commit and tag to origin"),
        ] = False,
        no_commit: Annotated[
            bool,
            typer.Option("--no-commit", help="Skip git commit (still creates tag)"),
        ] = False,
        message: Annotated[
            str | None,
            typer.Option("-m", "--message", help="Commit/tag message"),
        ] = None,
    ) -> None:
        from pa.release.runner import create_release

        try:
            result = create_release(
                bump,
                commit=not no_commit,
                push=not no_push,
                message=message,
            )
        except (RuntimeError, ValueError) as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(1) from exc

        typer.echo(f"Bumped {result.old_version} → {result.new_version}")
        typer.echo(f"Tag: {result.tag} ({result.track} track)")
        if no_push:
            typer.echo("Local only (--no-push). Re-run without --no-push to publish.")
        else:
            typer.echo("Pushed to origin.")

    return command


for _bump in ("patch", "minor", "major", "alpha", "beta", "rc"):
    release_app.command(
        _bump,
        help=f"Bump {_bump} version and create git tag",
    )(_release_command(_bump))


@channel_app.command("list")
def channel_list() -> None:
    """List release tracks and their latest versions."""
    from pa.update.registry import describe_tracks
    from pa.update.channels import get_channel

    settings = get_settings()
    typer.echo(f"Repository: {settings.update_repo}")
    typer.echo("")
    for track in describe_tracks():
        channel = get_channel(track["name"], repo=settings.update_repo)
        release = channel.latest()
        latest = release.version if release else "unknown"
        ref = release.tag if release and release.tag else "—"
        typer.echo(f"  {track['name']:<8} {latest:<16} ref={ref}")
        typer.echo(f"           {track['description']}")
    typer.echo("")
    typer.echo("Install: PA_CHANNEL=<track> curl .../install-remote.sh | bash")
    typer.echo("Update:  pa update --channel <track>")


@plugins_app.command("list")
def plugins_list() -> None:
    """List loaded and discoverable modules."""
    import importlib.metadata

    kernel = Kernel.boot(load_modules=True)
    typer.echo("Loaded modules:")
    for entry in kernel.registry.describe():
        typer.echo(f"  {entry['name']} v{entry['version']} ({entry['source']})")
        if entry["description"]:
            typer.echo(f"    {entry['description']}")

    typer.echo(f"\nEntry point group: {ENTRYPOINT_GROUP}")
    try:
        eps = importlib.metadata.entry_points(group=ENTRYPOINT_GROUP)
    except TypeError:
        eps = importlib.metadata.entry_points().get(ENTRYPOINT_GROUP, [])

    if eps:
        typer.echo("Registered entry points (not necessarily loaded if duplicate name):")
        for ep in eps:
            typer.echo(f"  {ep.name} = {ep.value}")
    else:
        typer.echo("No external entry points registered.")


@app.command()
def doctor() -> None:
    """Run post-install health checks."""
    from pa.cli.doctor import run_doctor

    raise typer.Exit(run_doctor())


@app.command()
def serve(
    host: Annotated[str | None, typer.Option(help="Bind host")] = None,
    port: Annotated[int | None, typer.Option(help="Bind port")] = None,
    reload: Annotated[bool, typer.Option(help="Auto-reload on code changes")] = False,
    debug: Annotated[
        bool,
        typer.Option("--debug", help="Enable debug logging, hooks history, and dev tools"),
    ] = False,
) -> None:
    """Start the PA backend server."""
    reset_kernel()
    if debug:
        reset_settings()
        import os

        os.environ["PA_DEBUG"] = "true"
        os.environ["PA_DEV_TOOLS"] = "true"
        os.environ["PA_LOG_LEVEL"] = "DEBUG"

    settings = get_settings()
    bind_host = host or settings.host
    bind_port = port or settings.port
    typer.echo(f"Starting PA on http://{bind_host}:{bind_port}")
    if settings.debug:
        typer.echo("  Debug mode enabled")
    uvicorn.run(
        "pa.server.app:create_app",
        factory=True,
        host=bind_host,
        port=bind_port,
        reload=reload,
        log_level="debug" if settings.debug else "info",
    )


@app.command()
def mcp() -> None:
    """Run PA's MCP server over stdio (for agent sessions)."""
    from pa.mcp.server import run_stdio

    run_stdio()


@app.command()
def login(
    username: Annotated[str, typer.Option(prompt=True)] = "local",
    password: Annotated[str, typer.Option(prompt=True, hide_input=True)] = "",
) -> None:
    """Authenticate and show CLI token."""
    from pa.auth.users import UserDirectory

    users = UserDirectory(get_settings().data_dir)
    users.ensure_default_user()
    if password:
        user = users.authenticate(username, password)
        if not user:
            typer.echo("Invalid credentials.", err=True)
            raise typer.Exit(1)
    else:
        user = users.get("local") or users.ensure_default_user()
    typer.echo(f"Logged in as {user.username}")
    typer.echo(f"CLI token: {user.cli_token}")
    typer.echo("Use: export PA_CLI_TOKEN=... or Authorization: Bearer <token>")


@user_app.command("set-password")
def user_set_password(
    username: Annotated[str, typer.Argument(help="Username")],
    password: Annotated[
        str,
        typer.Option(prompt=True, hide_input=True, confirmation_prompt=True),
    ],
) -> None:
    """Set a user's password."""
    from pa.auth.users import UserDirectory

    users = UserDirectory(get_settings().data_dir)
    users.ensure_default_user()
    try:
        user = users.set_password(username, password)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"Password updated for {user.username}")


def _fleet_api_base(settings: Settings) -> str:
    host = settings.host if settings.host not in ("0.0.0.0", "::") else "127.0.0.1"
    return f"http://{host}:{settings.port}"


def _fleet_api_post(settings: Settings, path: str, body: dict | None = None) -> dict | None:
    """POST to the local running server; return JSON or None if unreachable."""
    import httpx

    from pa.auth.csrf import COOKIE_NAME, HEADER_NAME

    base = _fleet_api_base(settings)
    try:
        with httpx.Client(timeout=5.0) as client:
            client.get(f"{base}/api/health")
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            token = client.cookies.get(COOKIE_NAME)
            if token:
                headers[HEADER_NAME] = token
            if settings.sync_token:
                headers["Authorization"] = f"Bearer {settings.sync_token}"
            resp = client.post(f"{base}{path}", json=body or {}, headers=headers)
            if resp.status_code >= 400:
                return None
            return resp.json()
    except httpx.HTTPError:
        return None


@fleet_app.command("list")
def fleet_list() -> None:
    """List instances in this fleet (re-probes health)."""
    import httpx

    from pa.fleet.registry import FleetRegistry

    settings = get_settings()
    fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
    for inst in fleet.list_instances():
        healthy = False
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(f"{inst.url.rstrip('/')}/api/health")
                healthy = resp.status_code == 200
        except httpx.HTTPError:
            pass
        inst.healthy = healthy
        fleet.upsert_instance(inst)
        status = "up" if healthy else "down"
        typer.echo(f"  {inst.name:<16} {inst.url:<30} zone={inst.zone} [{status}]")


@fleet_app.command("join-token")
def fleet_join_token() -> None:
    """Generate a one-time token to add an instance to this fleet."""
    from pa.fleet.join import ensure_sync_token, owner_public_url, readiness_warnings
    from pa.fleet.registry import FleetRegistry

    settings = get_settings()
    for warning in readiness_warnings(settings):
        typer.echo(f"warning: {warning}", err=True)

    ensure_sync_token(settings)
    data = _fleet_api_post(settings, "/api/fleet/join-token")
    if data and data.get("token"):
        token = data["token"]
        expires = data.get("expires_at", "")
        owner = data.get("owner_url") or owner_public_url(settings)
        typer.echo(f"Token: {token}")
        if expires:
            typer.echo(f"Expires: {expires}")
        typer.echo("(created via running server)")
    else:
        fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
        join = fleet.create_join_token()
        token = join.token
        owner = owner_public_url(settings)
        typer.echo(f"Token: {token}")
        typer.echo(f"Expires: {join.expires_at.isoformat()}")
        typer.echo("(server unreachable — wrote token to disk; live server will reload it)")

    typer.echo(f"Owner URL: {owner}")
    typer.echo("Join on remote:")
    typer.echo(
        f"  PA_FLEET_OWNER_URL={owner} pa fleet join {token} "
        f"--url http://<remote-host>:8080 --name <remote-name>"
    )
    typer.echo("Or push-install from this host:")
    typer.echo(
        f"  pa fleet install-remote user@host --name <name> --url http://<host>:8080"
    )


@fleet_app.command("join")
def fleet_join(
    token: Annotated[str, typer.Argument(help="Fleet join token")],
    url: Annotated[str, typer.Option(help="This instance's URL")] = "",
    owner: Annotated[str, typer.Option(help="Fleet owner URL")] = "",
    name: Annotated[str | None, typer.Option(help="Instance name")] = None,
) -> None:
    """Join this instance to a fleet using a join token."""
    import asyncio
    import os

    import httpx

    from pa.domain.instance_config import load_instance_config
    from pa.fleet.join import apply_join_response, join_fleet, refresh_service_env
    from pa.config import reset_settings as _reset

    settings = get_settings()
    config = load_instance_config(settings.data_dir)
    if not config:
        typer.echo("Run pa init first.", err=True)
        raise typer.Exit(1)

    owner_url = owner or os.environ.get("PA_FLEET_OWNER_URL", "") or settings.fleet_owner_url
    if not owner_url:
        typer.echo("Set --owner or PA_FLEET_OWNER_URL.", err=True)
        raise typer.Exit(1)

    instance_url = url or settings.instance_url or f"http://{settings.host}:{settings.port}"
    instance_name = name or settings.instance_name

    async def _join():
        return await join_fleet(
            owner_url,
            token,
            instance_id=config.instance_id,
            name=instance_name,
            url=instance_url,
            zone=settings.zone,
            capabilities=list(settings.capabilities),
            sync_token=settings.sync_token,
        )

    try:
        result = asyncio.run(_join())
    except httpx.HTTPStatusError as exc:
        # Prefer our enriched message when present.
        detail = str(exc)
        if exc.response is not None and "Fleet join failed" not in detail:
            try:
                body = exc.response.json()
                if body.get("detail"):
                    detail = f"Fleet join failed: {body['detail']}"
            except Exception:
                pass
        typer.echo(detail, err=True)
        if "Invalid or expired" in detail:
            typer.echo(
                "Mint a fresh token on the owner: pa fleet join-token  (or Fleet → Add existing).",
                err=True,
            )
        raise typer.Exit(1) from exc
    except httpx.HTTPError as exc:
        typer.echo(f"Fleet join failed: {exc}", err=True)
        raise typer.Exit(1) from exc

    apply_join_response(
        settings.data_dir,
        fleet_id=result["fleet_id"],
        owner_url=result.get("owner_url", owner_url),
        subscribed_realms=result.get("subscribed_realms"),
        sync_token=result.get("sync_token"),
        peers=result.get("peers"),
    )
    _reset()
    settings = get_settings()
    if refresh_service_env(settings):
        typer.echo("Service env refreshed.")
    typer.echo(f"Joined fleet {result['fleet_id']}")
    typer.echo(f"  Owner: {result.get('owner_url', owner_url)}")


@fleet_app.command("remove")
def fleet_remove(
    instance_id: Annotated[str, typer.Argument(help="Fleet instance ID to remove")],
) -> None:
    """Remove an instance from the local fleet registry and peer routes."""
    import httpx

    from pa.auth.csrf import COOKIE_NAME, HEADER_NAME
    from pa.fleet.join import remove_peer_url, unwire_instance_peers
    from pa.fleet.registry import FleetRegistry
    from pa.network.peer_table import PeerTable

    settings = get_settings()
    if instance_id == settings.instance_id:
        typer.echo("Cannot remove the local instance.", err=True)
        raise typer.Exit(1)

    base = _fleet_api_base(settings)
    try:
        with httpx.Client(timeout=5.0) as client:
            client.get(f"{base}/api/health")
            headers: dict[str, str] = {"Accept": "application/json"}
            csrf = client.cookies.get(COOKIE_NAME)
            if csrf:
                headers[HEADER_NAME] = csrf
            if settings.sync_token:
                headers["Authorization"] = f"Bearer {settings.sync_token}"
            resp = client.delete(f"{base}/api/fleet/instances/{instance_id}", headers=headers)
            if resp.status_code < 400:
                typer.echo(f"Removed {instance_id}")
                return
    except httpx.HTTPError:
        pass

    fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
    inst = fleet.get_instance(instance_id)
    if not inst:
        typer.echo(f"Instance not found: {instance_id}", err=True)
        raise typer.Exit(1)
    peer_table = PeerTable(settings.data_dir)
    unwire_instance_peers(peer_table, instance_id=instance_id, url=inst.url)
    remove_peer_url(settings, inst.url)
    fleet.remove_instance(instance_id)
    typer.echo(f"Removed {instance_id}")


@fleet_app.command("register")
def fleet_register(
    url: Annotated[str, typer.Option(help="Remote instance URL")],
    name: Annotated[str, typer.Option(help="Instance name")] = "remote",
    instance_id: Annotated[str | None, typer.Option("--id", help="Instance ID")] = None,
    zone: Annotated[str, typer.Option(help="Zone")] = "default",
) -> None:
    """Manually register a remote instance (no join token)."""
    from uuid import uuid4

    from pa.fleet.join import register_joiner_on_owner
    from pa.fleet.registry import FleetRegistry
    from pa.network.peer_table import PeerTable

    settings = get_settings()
    body = {
        "instance_id": instance_id or str(uuid4()),
        "name": name,
        "url": url.rstrip("/"),
        "zone": zone,
        "capabilities": [],
    }
    data = _fleet_api_post(settings, "/api/fleet/register-remote", body)
    if data:
        typer.echo(f"Registered {data.get('name')} ({data.get('instance_id')}) via server")
        return

    fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
    peer_table = PeerTable(settings.data_dir)
    inst, _ = register_joiner_on_owner(
        fleet,
        peer_table,
        settings,
        joiner_id=body["instance_id"],
        name=name,
        url=url,
        zone=zone,
        realms=list(settings.subscribed_realms),
    )
    typer.echo(f"Registered {inst.name} ({inst.instance_id})")


@fleet_app.command("install-remote")
def fleet_install_remote(
    target: Annotated[str, typer.Argument(help="SSH target user@host")],
    name: Annotated[str, typer.Option(help="Remote instance name")],
    url: Annotated[str, typer.Option(help="Remote advertised URL (Tailscale)")],
    port: Annotated[int, typer.Option(help="SSH port")] = 22,
    identity: Annotated[str | None, typer.Option("--identity", "-i", help="SSH identity file")] = None,
    password: Annotated[
        bool,
        typer.Option("--ask-password", help="Prompt for SSH password (not stored)"),
    ] = False,
    passphrase: Annotated[
        bool,
        typer.Option("--ask-passphrase", help="Prompt for key passphrase (not stored)"),
    ] = False,
    channel: Annotated[str, typer.Option(help="Release track")] = "release",
    realm: Annotated[str, typer.Option(help="Realm to subscribe")] = "",
    join_only: Annotated[
        bool,
        typer.Option("--join-only", help="Only run fleet join on an existing install"),
    ] = False,
) -> None:
    """Push-install PA on a remote host over SSH and join this fleet."""
    import asyncio
    import getpass

    from pa.fleet.join import readiness_warnings
    from pa.fleet.registry import FleetRegistry
    from pa.fleet.remote_install import (
        RemoteInstallRequest,
        get_job_store,
        run_install_job,
    )

    settings = get_settings()
    for warning in readiness_warnings(settings):
        typer.echo(f"warning: {warning}", err=True)

    if "@" in target:
        user, host = target.split("@", 1)
    else:
        typer.echo("Target must be user@host", err=True)
        raise typer.Exit(1)

    pw = getpass.getpass("SSH password: ") if password else ""
    pp = getpass.getpass("Key passphrase: ") if passphrase else ""

    req = RemoteInstallRequest(
        host=host,
        user=user,
        port=port,
        identity_file=identity or "",
        password=pw,
        passphrase=pp,
        instance_name=name,
        instance_url=url,
        channel=channel,
        realm=realm,
        join_only=join_only,
    )
    fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
    store = get_job_store(settings)
    job = store.create(req)

    async def _run():
        return await run_install_job(settings, fleet, store, job, req)

    result = asyncio.run(_run())
    for line in result.log_lines:
        typer.echo(line)
    if result.status.value != "succeeded":
        typer.echo(result.error or "Remote install failed", err=True)
        raise typer.Exit(1)
    typer.echo(f"OK — {name} at {url}")


@realm_app.command("list")
def realm_list() -> None:
    """List subscribed realms and memberships."""
    from pa.fleet.membership import MembershipStore

    settings = get_settings()
    membership = MembershipStore(settings.data_dir)
    for realm in membership.list_realms():
        typer.echo(f"  {realm.id:<20} {realm.name or realm.id}")
    typer.echo("")
    for m in membership.list_memberships():
        typer.echo(f"    {m.principal_type.value}:{m.principal_id} → {m.realm_id} ({m.role.value})")


@realm_app.command("invite")
def realm_invite(
    realm: Annotated[str, typer.Option(help="Realm ID")] = "",
    role: Annotated[str, typer.Option(help="Role")] = "editor",
) -> None:
    """Generate a realm invite token."""
    from pa.domain.models import RealmRole
    from pa.fleet.membership import MembershipStore

    settings = get_settings()
    membership = MembershipStore(settings.data_dir)
    realm_id = realm or settings.primary_realm
    invite = membership.create_invite(realm_id, RealmRole(role))
    typer.echo(f"Invite token: {invite.token}")
    typer.echo(f"Realm: {invite.realm_id}  Role: {invite.role.value}")
    if invite.expires_at:
        typer.echo(f"Expires: {invite.expires_at.isoformat()}")


@app.command("peers")
def peers_list() -> None:
    """List configured peers and discovery status."""
    import asyncio

    from pa.network.registry import PeerRegistry

    settings = get_settings()
    registry = PeerRegistry(settings)
    typer.echo(f"Local: {settings.instance_name} ({settings.instance_id})")
    typer.echo(f"Configured peers: {len(settings.peers)}")
    for url in settings.peers:
        typer.echo(f"  {url}")

    async def discover():
        return await registry.discover_peers()

    discovered = asyncio.run(discover())
    if discovered:
        typer.echo("Discovered:")
        for p in discovered:
            typer.echo(f"  {p.name} ({p.id}) fleet={p.fleet_id} realms={p.subscribed_realms}")


@project_app.command("list")
def project_list(
    realm: Annotated[str | None, typer.Option(help="Realm ID")] = None,
) -> None:
    """List projects in a realm."""
    from pa.domain.store import get_store

    settings = get_settings()
    store = get_store()
    realm_id = realm or settings.primary_realm
    for project in store.list_projects(realm_id=realm_id):
        typer.echo(f"  {project.id:<36} {project.title}")


@project_app.command("create")
def project_create(
    title: Annotated[str, typer.Argument(help="Project title")],
    description: Annotated[str, typer.Option(help="Description")] = "",
    realm: Annotated[str | None, typer.Option(help="Realm ID")] = None,
) -> None:
    """Create a new project."""
    from pa.domain.models import ProjectCreate
    from pa.domain.store import get_store

    settings = get_settings()
    store = get_store()
    realm_id = realm or settings.primary_realm
    project = store.create_project(
        ProjectCreate(realm_id=realm_id, title=title, description=description),
        instance_id=settings.instance_id,
    )
    typer.echo(f"Created project {project.id}: {project.title}")


@sync_app.command("status")
def sync_status(
    realm: Annotated[str | None, typer.Option(help="Realm ID")] = None,
) -> None:
    """Show sync status for a realm."""
    from pa.sync.infrastructure import get_sync_engine

    settings = get_settings()
    realm_id = realm or settings.primary_realm
    engine = get_sync_engine(settings)
    status = engine.status(realm_id)
    typer.echo(f"Realm:   {status['realm_id']}")
    typer.echo(f"Head:    {status.get('head') or '—'}")
    typer.echo(f"Objects: {status.get('object_count', 0)}")
    typer.echo(f"Peers:   {status.get('peer_count', 0)}")
    typer.echo(f"Zone:    {status.get('zone')}")
