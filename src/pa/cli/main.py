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

    config = InstanceConfig(
        instance_id=settings.instance_id,
        instance_name=settings.instance_name,
        data_dir=str(settings.data_dir),
        fleet_id=fleet_id or settings.fleet_id,
        fleet_owner="local",
        subscribed_realms=[realm] if realm else list(settings.subscribed_realms),
        zone=settings.zone,
        capabilities=list(settings.capabilities),
        relay_enabled=settings.relay_enabled,
    )
    save_instance_config(settings.data_dir, config)
    typer.echo(f"Initialized PA instance '{settings.instance_name}'")
    typer.echo(f"  ID:    {settings.instance_id}")
    typer.echo(f"  Fleet: {config.fleet_id}")
    typer.echo(f"  Realm: {', '.join(config.subscribed_realms)}")
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
    if sys.platform == "darwin":
        typer.echo(f"  Service:     {'running' if svc_status.running else 'stopped'}")
        if svc_status.installed:
            typer.echo(f"  Plist:       {svc_status.plist_path}")
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
        typer.Option("--service-only", help="Only register/reload launchd plist"),
    ] = False,
    name: Annotated[str, typer.Option(help="Instance name")] = "local",
    no_start: Annotated[bool, typer.Option(help="Do not start service after install")] = False,
    from_source: Annotated[
        Path | None,
        typer.Option("--from-source", help="Install from local path (repo root)"),
    ] = None,
) -> None:
    """Install PA on the host (uv tool + init + launchd)."""
    from pa.cli import service as svc
    from pa.install.runner import install_from_path

    if service_only:
        if sys.platform != "darwin":
            typer.echo("Service management is only supported on macOS.", err=True)
            raise typer.Exit(1)
        settings = get_settings()
        pa_bin = svc.find_pa_binary()
        if not pa_bin:
            typer.echo("pa binary not found in PATH.", err=True)
            raise typer.Exit(1)
        path = svc.install_plist(settings, pa_bin)
        svc.bootstrap()
        typer.echo(f"Registered launchd service: {path}")
        return

    try:
        install_from_path(from_source, name=name, start_service=not no_start)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    settings = get_settings()
    typer.echo("PA installed successfully.")
    typer.echo(f"  Server: http://{settings.host}:{settings.port}")
    typer.echo("  Commands: pa status | pa restart | pa update")


@app.command()
def start() -> None:
    """Start the PA launchd service (macOS)."""
    from pa.cli import service as svc

    try:
        svc.start()
        typer.echo("PA service started.")
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@app.command()
def stop() -> None:
    """Stop the PA launchd service (macOS)."""
    from pa.cli import service as svc

    try:
        svc.stop()
        typer.echo("PA service stopped.")
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@app.command(name="restart")
def restart_cmd() -> None:
    """Restart the PA launchd service (macOS)."""
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
        push: Annotated[
            bool,
            typer.Option("--push", help="Push commit and tag to origin"),
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
                push=push,
                message=message,
            )
        except (RuntimeError, ValueError) as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(1) from exc

        typer.echo(f"Bumped {result.old_version} → {result.new_version}")
        typer.echo(f"Tag: {result.tag} ({result.track} track)")
        if push:
            typer.echo("Pushed to origin.")
        else:
            typer.echo("Local tag created. Run with --push to publish.")

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
    typer.echo(f"Remote: PA_FLEET_TOKEN={join.token} curl .../install-remote.sh | bash")


@fleet_app.command("join")
def fleet_join(
    token: Annotated[str, typer.Argument(help="Fleet join token")],
    url: Annotated[str, typer.Option(help="This instance's URL")] = "",
) -> None:
    """Join this instance to a fleet using a join token."""
    import httpx

    settings = get_settings()
    base = url or f"http://{settings.host}:{settings.port}"
    typer.echo(f"Joining fleet with token on {base}...")
    typer.echo("Configure fleet owner URL in PA_FLEET_OWNER_URL to auto-register.")


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
    from pa.domain.store import get_store

    settings = get_settings()
    store = get_store()
    realm_id = realm or settings.primary_realm
    if store.sync_engine:
        status = store.sync_engine.status(realm_id)
        typer.echo(f"Realm:   {status['realm_id']}")
        typer.echo(f"Head:    {status.get('head') or '—'}")
        typer.echo(f"Objects: {status.get('object_count', 0)}")
        typer.echo(f"Peers:   {status.get('peer_count', 0)}")
        typer.echo(f"Zone:    {status.get('zone')}")
    else:
        typer.echo("Sync engine not initialized.")
