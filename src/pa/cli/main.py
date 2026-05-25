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


@app.command()
def version() -> None:
    """Show PA version."""
    typer.echo(f"pa {__version__}")


@app.command()
def init(
    name: Annotated[str, typer.Option(help="Instance name")] = "local",
    data_dir: Annotated[str | None, typer.Option(help="Data directory")] = None,
) -> None:
    """Initialize a PA instance (creates data directory and config)."""
    reset_settings()
    reset_kernel()
    kwargs: dict = {"instance_name": name}
    if data_dir:
        kwargs["data_dir"] = Path(data_dir)
    settings = Settings(**kwargs)
    settings.ensure_dirs()

    config_path = settings.data_dir / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "instance_id": settings.instance_id,
                "instance_name": settings.instance_name,
                "data_dir": str(settings.data_dir),
            },
            indent=2,
        )
    )
    typer.echo(f"Initialized PA instance '{settings.instance_name}'")
    typer.echo(f"  ID:   {settings.instance_id}")
    typer.echo(f"  Data: {settings.data_dir}")


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
    if sys.platform == "darwin":
        typer.echo(f"  Service:     {'running' if svc_status.running else 'stopped'}")
        if svc_status.installed:
            typer.echo(f"  Plist:       {svc_status.plist_path}")
    typer.echo(f"  Debug:       {settings.debug}")
    typer.echo(f"  Agent:       {'enabled' if settings.agent_enabled else 'disabled'}")
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
        typer.Option(help="Update channel: github or pypi"),
    ] = None,
) -> None:
    """Check for and install PA updates."""
    from pa.update.runner import check_update, run_update

    settings = get_settings()

    if check:
        result = check_update(settings)
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
