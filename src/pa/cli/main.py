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
    from pa.cli import service as svc

    settings = get_settings()
    store = get_store()
    kernel = Kernel.boot(load_modules=True)
    items = store.list_items()
    sessions = store.list_sessions()
    knowledge = store.list_knowledge(limit=5)
    svc_status = svc.get_status(settings)
    install_meta = load_install_metadata(settings.data_dir)
    pa_bin = svc.find_pa_binary()

    typer.echo(f"PA {__version__} — {settings.instance_name}")
    typer.echo(f"  Instance ID: {settings.instance_id}")
    typer.echo(f"  Data dir:    {settings.data_dir}")
    typer.echo(f"  Server:      http://{settings.host}:{settings.port}")
    typer.echo(f"  Binary:      {pa_bin or 'not found'}")
    if install_meta:
        typer.echo(f"  Installed:   {install_meta.version} ({install_meta.method})")
        if install_meta.channel:
            typer.echo(f"  Track:       {install_meta.channel}")
    typer.echo(f"  Update:      {settings.release_track} track")
    if svc_status.installed or svc.service_supported():
        typer.echo(
            f"  Service:     {'running' if svc_status.running else 'stopped'}"
            f" ({svc_status.backend})"
        )
        if svc_status.installed:
            typer.echo(f"  Unit:        {svc_status.plist_path}")
    typer.echo(f"  Debug:       {settings.debug}")
    typer.echo(f"  Agent:       {'enabled' if settings.agent_enabled else 'disabled'}")
    typer.echo(f"  Fleet:       {settings.fleet_id}")
    typer.echo(f"  Realms:      {', '.join(settings.subscribed_realms)}")
    typer.echo(f"  Zone:        {settings.zone}")
    typer.echo(f"  Peers:       {len(settings.peers)}")
    typer.echo(f"  Modules:     {len(kernel.registry.modules)}")
    typer.echo(f"  Items:       {len(items)}")
    typer.echo(f"  Sessions:    {len(sessions)}")
    typer.echo(f"  Knowledge:   {len(knowledge)} recent entries")


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
        pa_bin = svc.find_pa_binary()
        if not pa_bin:
            typer.echo("pa binary not found in PATH.", err=True)
            raise typer.Exit(1)
        path = svc.install_service(settings, pa_bin)
        svc.bootstrap()
        typer.echo(f"Registered {svc.get_status(settings).backend} service: {path}")
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
def start() -> None:
    """Start the PA host service (launchd or systemd)."""
    from pa.cli import service as svc

    try:
        svc.start()
        typer.echo("PA service started.")
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@app.command()
def stop() -> None:
    """Stop the PA host service."""
    from pa.cli import service as svc

    try:
        svc.stop()
        typer.echo("PA service stopped.")
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@app.command(name="restart")
def restart_cmd() -> None:
    """Restart the PA host service."""
    from pa.cli import service as svc

    try:
        svc.restart()
        typer.echo("PA service restarted.")
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
    restart: Annotated[bool, typer.Option("--restart", help="Restart service after update")] = False,
    channel: Annotated[
        str | None,
        typer.Option(help="Release track: release, beta, alpha, dev, or pypi"),
    ] = None,
) -> None:
    """Check for and install PA updates."""
    from pa.update.runner import check_update, run_update

    settings = get_settings()

    if check:
        result = check_update(settings, channel_name=channel)
        typer.echo(f"Installed: {result.current}")
        typer.echo(f"Latest:    {result.latest or 'unknown'}")
        if result.upgrade_available:
            typer.echo("Update available.")
            raise typer.Exit(1)
        typer.echo("Up to date.")
        return

    try:
        result = run_update(settings, channel_name=channel, restart=restart)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    if not result.upgrade_available:
        typer.echo(f"PA {result.current} is up to date.")
        return

    typer.echo(f"Updated PA {result.current} → {result.latest}")
    if restart:
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


@fleet_app.command("list")
def fleet_list() -> None:
    """List instances in this fleet."""
    from pa.fleet.registry import FleetRegistry

    settings = get_settings()
    fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
    for inst in fleet.list_instances():
        status = "up" if inst.healthy else "down"
        typer.echo(f"  {inst.name:<16} {inst.url:<30} zone={inst.zone} [{status}]")


@fleet_app.command("join-token")
def fleet_join_token() -> None:
    """Generate a one-time token to add an instance to this fleet."""
    from pa.fleet.registry import FleetRegistry

    settings = get_settings()
    fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
    join = fleet.create_join_token()
    typer.echo(f"Token: {join.token}")
    typer.echo(f"Expires: {join.expires_at.isoformat()}")
    owner = settings.instance_url or f"http://{settings.host}:{settings.port}"
    typer.echo(f"Remote install:")
    typer.echo(f"  PA_FLEET_OWNER_URL={owner} PA_FLEET_TOKEN={join.token} \\")
    typer.echo(f"  PA_INSTANCE_URL=http://<remote-host>:8080 curl .../install-remote.sh | bash")


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
    from pa.fleet.join import apply_join_response, join_fleet

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
    except httpx.HTTPError as exc:
        typer.echo(f"Fleet join failed: {exc}", err=True)
        raise typer.Exit(1) from exc

    apply_join_response(
        settings.data_dir,
        fleet_id=result["fleet_id"],
        owner_url=result.get("owner_url", owner_url),
        subscribed_realms=result.get("subscribed_realms"),
    )
    typer.echo(f"Joined fleet {result['fleet_id']}")
    typer.echo(f"  Owner: {result.get('owner_url', owner_url)}")
    typer.echo("Re-run pa install --service-only to refresh service env.")


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
    from pa.fleet.membership import MembershipStore
    from pa.network.peer_table import PeerTable
    from pa.sync.engine import SyncEngine
    from pa.sync.infrastructure import get_event_log, get_object_store

    settings = get_settings()
    realm_id = realm or settings.primary_realm
    engine = SyncEngine(
        settings,
        get_object_store(settings),
        get_event_log(settings),
        PeerTable(settings.data_dir),
        MembershipStore(settings.data_dir),
    )
    status = engine.status(realm_id)
    typer.echo(f"Realm:   {status['realm_id']}")
    typer.echo(f"Head:    {status.get('head') or '—'}")
    typer.echo(f"Objects: {status.get('object_count', 0)}")
    typer.echo(f"Peers:   {status.get('peer_count', 0)}")
    typer.echo(f"Zone:    {status.get('zone')}")
