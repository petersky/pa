"""CLI for viewing and editing instance config.json."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from pa.cli import presentation as ui
from pa.cli.config_tui import ExitCode, run_config_editor
from pa.config import get_settings
from pa.configuration.service import (
    apply_update,
    configuration_snapshot,
    diff_update,
    schema_document,
    validate_update,
)
from pa.domain.config_edit import (
    FIELD_SPECS,
    ConfigError,
    MutateResult,
    add_config_value,
    config_as_dict,
    format_value,
    get_field_spec,
    list_field_specs,
    parse_value,
    refresh_after_mutate,
    remove_config_value,
    require_config,
    set_config_value,
    unset_config_value,
)
from pa.domain.instance_config import config_path
from pa.mcp.local_api import (
    LocalPARequestError,
    LocalPAServerUnavailable,
    request_local_pa,
)

config_app = typer.Typer(
    help="View and edit instance config.json",
    no_args_is_help=False,
    invoke_without_command=True,
)

console = ui.console()


def _data_dir() -> Path:
    return get_settings().data_dir


def _uses_owner_api() -> bool:
    """Tests/offline fixtures may point _data_dir elsewhere; real writes use PA."""
    try:
        return _data_dir().resolve() == get_settings().data_dir.resolve()
    except OSError:
        return True


def _request(
    method: str,
    path: str,
    *,
    target: str = "local",
    payload: dict | None = None,
    params: dict | None = None,
) -> dict:
    query = {"target": target, **(params or {})} if method == "GET" else None
    body = dict(payload or {})
    if method != "GET":
        body.setdefault("target", target)
    try:
        return request_local_pa(
            get_settings(), method, path, params=query, json=body or None
        )
    except LocalPARequestError as exc:
        typer.echo(str(exc), err=True)
        if exc.status in {409, 428}:
            code = ExitCode.CONFLICT
        elif exc.status in {400, 422}:
            code = ExitCode.VALIDATION_FAILED
        elif exc.status in {401, 403}:
            code = ExitCode.AUTHORIZATION_FAILED
        else:
            code = 1
        raise typer.Exit(int(code)) from exc
    except LocalPAServerUnavailable as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(int(ExitCode.CONNECTION_FAILED)) from exc


def _load_patch(path: Path) -> tuple[dict, list[str]]:
    try:
        document = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"Cannot read configuration patch {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ConfigError("Configuration patch must be a JSON object")
    if "changes" in document or "clear" in document:
        changes = document.get("changes") or {}
        clear = document.get("clear") or []
    else:
        changes, clear = document, []
    if not isinstance(changes, dict) or not isinstance(clear, list):
        raise ConfigError("Patch requires an object 'changes' and array 'clear'")
    return changes, [str(key) for key in clear]


def _print_json(value: object) -> None:
    typer.echo(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str))


def _echo_result(result: MutateResult, *, reveal: bool = False) -> None:
    spec = get_field_spec(result.key)
    before = format_value(result.before, reveal=reveal, sensitive=spec.sensitive)
    after = format_value(result.after, reveal=reveal, sensitive=spec.sensitive)
    typer.echo(f"{result.op.value} {result.key}: {before} → {after}")
    refreshed = refresh_after_mutate(_data_dir(), result)
    if refreshed:
        typer.echo("  Service unit environment refreshed.")
    if result.restart_required:
        typer.echo("  Explicit PA restart required — run: pa restart")


def _print_table(*, reveal: bool = False) -> None:
    data_dir = _data_dir()
    config = require_config(data_dir)
    data = config_as_dict(config)
    path = config_path(data_dir)

    table = Table(
        title=f"PA config — {path}",
        show_header=True,
        header_style="bold",
    )
    table.add_column("Key", style="cyan", no_wrap=True)
    table.add_column("Value")
    table.add_column("Notes", style="dim")

    for spec in list_field_specs():
        raw = data.get(spec.name)
        value = format_value(raw, reveal=reveal, sensitive=spec.sensitive)
        notes: list[str] = []
        if not spec.editable:
            notes.append("read-only")
        if spec.list_ops:
            notes.append("list")
        if spec.sensitive and not reveal:
            notes.append("masked")
        table.add_row(spec.name, value, ", ".join(notes))

    console.print(table)


@config_app.callback()
def config_callback(
    ctx: typer.Context,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Open interactive config TUI",
        ),
    ] = False,
    line_mode: Annotated[
        bool,
        typer.Option(
            "--line",
            help="Use the accessible line-oriented editor",
        ),
    ] = False,
) -> None:
    """Manage instance configuration (config.json).

    Run with no subcommand (or ``-i``) for an interactive terminal UI.
    """
    if ctx.invoked_subcommand is not None:
        return
    if interactive or ctx.invoked_subcommand is None:
        try:
            code = run_interactive(reveal=False, force_line=line_mode)
        except ConfigError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(1) from exc
        raise typer.Exit(int(code))


@config_app.command("show")
def show_cmd(
    reveal: Annotated[
        bool,
        typer.Option("--reveal", help="Show sensitive values in full"),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Print raw JSON"),
    ] = False,
) -> None:
    """Show all config keys and values."""
    try:
        if as_json:
            config = require_config(_data_dir())
            data = config_as_dict(config)
            if not reveal:
                for name, spec in FIELD_SPECS.items():
                    if spec.sensitive and data.get(name):
                        data[name] = format_value(
                            data[name], reveal=False, sensitive=True
                        )
            typer.echo(json.dumps(data, indent=2))
        else:
            _print_table(reveal=reveal)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("list")
def list_cmd(
    as_json: Annotated[
        bool, typer.Option("--json", help="Emit stable JSON for automation")
    ] = False,
    target: Annotated[
        str, typer.Option("--target", help="Local or named fleet instance")
    ] = "local",
) -> None:
    """List every setting with configured/effective values and metadata."""
    try:
        result = (
            _request("GET", "/api/configuration", target=target)
            if _uses_owner_api()
            else configuration_snapshot(get_settings())
        )
        if as_json:
            _print_json(result)
            return
        table = Table(show_header=True, header_style="bold")
        for column in ("Key", "Configured", "Effective", "Source", "Apply"):
            table.add_column(column)
        for row in result["settings"]:
            table.add_row(
                row["key"],
                format_value(row["configured_value"]),
                format_value(row["effective_value"]),
                row["source"],
                row["apply"],
            )
        console.print(table)
        if result.get("unknown"):
            console.print(
                f"[yellow]{len(result['unknown'])} unknown persisted key(s)[/yellow]"
            )
        if result.get("deprecated"):
            console.print(
                f"[yellow]{len(result['deprecated'])} deprecated key(s)[/yellow]"
            )
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(int(ExitCode.VALIDATION_FAILED)) from exc


@config_app.command("schema")
def schema_cmd(
    as_json: Annotated[
        bool, typer.Option("--json", help="Emit stable JSON for automation")
    ] = False,
    target: Annotated[
        str, typer.Option("--target", help="Local or named fleet instance")
    ] = "local",
) -> None:
    """List the authoritative registry, validation, and surface metadata."""
    result = (
        schema_document()
        if target == "local"
        else _request("GET", "/api/configuration/schema", target=target)
    )
    if as_json:
        _print_json(result)
        return
    table = Table(show_header=True, header_style="bold")
    for column in ("Key", "Type", "Default", "Scope", "Exposure", "Apply"):
        table.add_column(column)
    for row in result["settings"]:
        table.add_row(
            row["key"],
            row["kind"],
            format_value(row["default"]),
            row["scope"],
            row["exposure"],
            row["apply"],
        )
    console.print(table)


def _patch_operation(
    operation: str,
    patch: Path,
    *,
    as_json: bool,
    target: str,
    dry_run: bool = False,
    expected_revision: str | None = None,
) -> None:
    try:
        changes, clear = _load_patch(patch)
        path = (
            "/api/configuration/validate"
            if operation == "validate"
            else "/api/configuration/diff"
        )
        if operation == "apply" and not dry_run:
            if _uses_owner_api():
                snapshot = _request("GET", "/api/configuration", target=target)
                revision = expected_revision or snapshot["revision"]
                result = _request(
                    "PATCH",
                    "/api/configuration",
                    target=target,
                    payload={
                        "changes": changes,
                        "clear": clear,
                        "expected_revision": revision,
                        "idempotency_key": f"cli:{uuid4()}",
                        "interface": "cli",
                    },
                )
            else:
                from pa.config import Settings

                local_settings = Settings(data_dir=_data_dir())
                base = require_config(_data_dir())
                from pa.domain.config_edit import config_revision

                applied = apply_update(
                    local_settings,
                    changes,
                    clear,
                    expected_revision=expected_revision or config_revision(base),
                    idempotency_key=f"cli:{uuid4()}",
                    principal_id="user:cli",
                    interface="cli",
                )
                result = {
                    "ok": True,
                    "changed": sorted(applied.changed),
                    "reload_required": sorted(applied.reload - applied.restart),
                    "restart_required": sorted(applied.restart),
                }
        elif _uses_owner_api():
            result = _request(
                "POST",
                "/api/configuration/diff" if dry_run else path,
                target=target,
                payload={"changes": changes, "clear": clear},
            )
        else:
            if operation == "validate" and not dry_run:
                validate_update(_data_dir(), changes, clear)
            result = diff_update(_data_dir(), changes, clear)
        if as_json:
            _print_json(result)
        else:
            rows = result.get("changes") or []
            if rows and isinstance(rows[0], dict):
                for row in rows:
                    typer.echo(
                        f"{row['key']}: {format_value(row.get('before'))} -> "
                        f"{format_value(row.get('after'))} [{row.get('apply', 'live')}]"
                    )
            elif result.get("changed"):
                typer.echo(f"Applied: {', '.join(result['changed'])}")
            else:
                typer.echo("Configuration is valid; no effective changes.")
            if result.get("restart_required"):
                typer.echo("Explicit PA restart required for staged restart settings.")
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(int(ExitCode.VALIDATION_FAILED)) from exc


@config_app.command("validate")
def validate_cmd(
    patch: Annotated[Path, typer.Argument(help="JSON patch or configuration file")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
) -> None:
    """Validate a JSON configuration patch without writing."""
    _patch_operation("validate", patch, as_json=as_json, target=target)


@config_app.command("diff")
def diff_cmd(
    patch: Annotated[Path, typer.Argument(help="JSON patch or configuration file")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
) -> None:
    """Show a secret-safe configuration diff without writing."""
    _patch_operation("apply", patch, as_json=as_json, target=target, dry_run=True)


@config_app.command("apply")
def apply_cmd(
    patch: Annotated[Path, typer.Argument(help="JSON patch or configuration file")],
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
    expected_revision: Annotated[
        str | None, typer.Option("--expected-revision")
    ] = None,
) -> None:
    """Atomically apply a JSON patch, or validate/diff it with --dry-run."""
    _patch_operation(
        "apply",
        patch,
        as_json=as_json,
        target=target,
        dry_run=dry_run,
        expected_revision=expected_revision,
    )


@config_app.command("audit")
def audit_cmd(
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
    limit: Annotated[int, typer.Option("--limit", min=1, max=1000)] = 100,
) -> None:
    """List who changed configuration, when, and through which interface."""
    if _uses_owner_api():
        result = _request(
            "GET",
            "/api/configuration/audit",
            target=target,
            params={"limit": limit},
        )
    else:
        from pa.configuration.service import audit_events

        result = {"events": audit_events(_data_dir(), limit=limit)}
    if as_json:
        _print_json(result)
        return
    for event in result["events"]:
        typer.echo(
            f"{event['occurred_at']} {event['principal_id']} "
            f"{event['interface']} {', '.join(event['keys'])}"
        )


@config_app.command("get")
def get_cmd(
    key: Annotated[str, typer.Argument(help="Config key")],
    reveal: Annotated[
        bool,
        typer.Option("--reveal", help="Show sensitive values in full"),
    ] = False,
    effective: Annotated[
        bool, typer.Option("--effective", help="Print the effective value")
    ] = False,
    source: Annotated[
        bool, typer.Option("--source", help="Include source and precedence")
    ] = False,
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
) -> None:
    """Print one configured or effective value."""
    try:
        spec = get_field_spec(key)
        if _uses_owner_api():
            snapshot = _request("GET", "/api/configuration", target=target)
            row = next(
                item for item in snapshot["settings"] if item["key"] == spec.name
            )
            if as_json:
                _print_json(row)
                return
            value = row["effective_value"] if effective else row["configured_value"]
            typer.echo(format_value(value, reveal=reveal, sensitive=False))
            if source:
                typer.echo(
                    f"source={row['source']} precedence={'>'.join(snapshot['precedence'])}"
                )
        else:
            config = require_config(_data_dir())
            value = getattr(config, spec.name)
            if as_json:
                _print_json(
                    {
                        "key": spec.name,
                        "configured_value": (
                            format_value(value, sensitive=True)
                            if spec.sensitive and not reveal
                            else value
                        ),
                    }
                )
            else:
                typer.echo(format_value(value, reveal=reveal, sensitive=spec.sensitive))
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("set")
def set_cmd(
    key: Annotated[str, typer.Argument(help="Config key")],
    value: Annotated[
        str | None,
        typer.Argument(help="New value (lists: comma-separated or JSON)"),
    ] = None,
    stdin: Annotated[
        bool, typer.Option("--stdin", help="Read the value from standard input")
    ] = False,
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
) -> None:
    """Set a config key (validated)."""
    try:
        spec = get_field_spec(key)
        if stdin:
            value = sys.stdin.read().rstrip("\n")
        if value is None:
            raise ConfigError("value is required (or use --stdin)")
        if spec.sensitive and not stdin and _uses_owner_api():
            raise ConfigError(
                f"{spec.name} is a secret; pipe it with --stdin to avoid shell history"
            )
        if _uses_owner_api():
            parsed = parse_value(spec, value)
            snapshot = _request("GET", "/api/configuration", target=target)
            result = _request(
                "PATCH",
                "/api/configuration",
                target=target,
                payload={
                    "changes": {spec.name: parsed},
                    "expected_revision": snapshot["revision"],
                    "idempotency_key": f"cli:{uuid4()}",
                    "interface": "cli",
                },
            )
            if as_json:
                _print_json(result)
            else:
                typer.echo(f"set {spec.name}")
                if result.get("restart_required"):
                    typer.echo("  Explicit PA restart required.")
                elif result.get("reload_required"):
                    typer.echo("  Service/runtime reload required.")
        else:
            direct = set_config_value(_data_dir(), key, value)
            if as_json:
                _print_json(
                    {
                        "ok": True,
                        "changed": [direct.key],
                        "restart_required": direct.restart_required,
                    }
                )
            else:
                _echo_result(direct)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("add")
def add_cmd(
    key: Annotated[str, typer.Argument(help="List config key")],
    value: Annotated[str, typer.Argument(help="Item to append")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
) -> None:
    """Append an item to a list config key."""
    try:
        spec = get_field_spec(key)
        if not spec.list_ops:
            raise ConfigError(f"{spec.name} is not a list setting")
        if _uses_owner_api():
            snapshot = _request("GET", "/api/configuration", target=target)
            row = next(
                item for item in snapshot["settings"] if item["key"] == spec.name
            )
            current = (
                row["configured_value"] if row["configured"] else row["effective_value"]
            )
            values = list(current or [])
            if value in values:
                raise ConfigError(f"{value!r} already in {spec.name}")
            values.append(value)
            result = _request(
                "PATCH",
                "/api/configuration",
                target=target,
                payload={
                    "changes": {spec.name: values},
                    "expected_revision": snapshot["revision"],
                    "idempotency_key": f"cli:{uuid4()}",
                    "interface": "cli",
                },
            )
            _print_json(result) if as_json else typer.echo(f"add {spec.name}: {value}")
        else:
            direct = add_config_value(_data_dir(), key, value)
            _print_json(
                {"ok": True, "changed": [direct.key]}
            ) if as_json else _echo_result(direct)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("remove")
def remove_cmd(
    key: Annotated[str, typer.Argument(help="List config key")],
    value: Annotated[str, typer.Argument(help="Item to remove")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
) -> None:
    """Remove an item from a list config key."""
    try:
        spec = get_field_spec(key)
        if not spec.list_ops:
            raise ConfigError(f"{spec.name} is not a list setting")
        if _uses_owner_api():
            snapshot = _request("GET", "/api/configuration", target=target)
            row = next(
                item for item in snapshot["settings"] if item["key"] == spec.name
            )
            current = (
                row["configured_value"] if row["configured"] else row["effective_value"]
            )
            values = list(current or [])
            if value not in values:
                raise ConfigError(f"{value!r} not found in {spec.name}")
            values = [item for item in values if item != value]
            result = _request(
                "PATCH",
                "/api/configuration",
                target=target,
                payload={
                    "changes": {spec.name: values},
                    "expected_revision": snapshot["revision"],
                    "idempotency_key": f"cli:{uuid4()}",
                    "interface": "cli",
                },
            )
            _print_json(result) if as_json else typer.echo(
                f"remove {spec.name}: {value}"
            )
        else:
            direct = remove_config_value(_data_dir(), key, value)
            _print_json(
                {"ok": True, "changed": [direct.key]}
            ) if as_json else _echo_result(direct)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("unset")
def unset_cmd(
    key: Annotated[str, typer.Argument(help="Config key to reset")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
    target: Annotated[str, typer.Option("--target")] = "local",
) -> None:
    """Reset a config key to its default / empty value."""
    try:
        spec = get_field_spec(key)
        if _uses_owner_api():
            snapshot = _request("GET", "/api/configuration", target=target)
            result = _request(
                "PATCH",
                "/api/configuration",
                target=target,
                payload={
                    "clear": [spec.name],
                    "expected_revision": snapshot["revision"],
                    "idempotency_key": f"cli:{uuid4()}",
                    "interface": "cli",
                },
            )
            if as_json:
                _print_json(result)
            else:
                typer.echo(f"unset {spec.name}")
        else:
            direct = unset_config_value(_data_dir(), key)
            if as_json:
                _print_json({"ok": True, "changed": [direct.key]})
            else:
                _echo_result(direct)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("keys")
def keys_cmd() -> None:
    """List known config keys and whether they are editable."""
    table = Table(show_header=True, header_style="bold")
    table.add_column("Key", style="cyan")
    table.add_column("Type")
    table.add_column("Editable")
    table.add_column("Description")
    for spec in list_field_specs():
        table.add_row(
            spec.name,
            spec.kind,
            "yes" if spec.editable else "no",
            spec.description,
        )
    console.print(table)


@config_app.command("edit")
def edit_cmd(
    reveal: Annotated[
        bool,
        typer.Option("--reveal", help="Show sensitive values in full"),
    ] = False,
    line_mode: Annotated[
        bool,
        typer.Option("--line", help="Use the accessible line-oriented editor"),
    ] = False,
) -> None:
    """Interactive terminal UI for managing config.json."""
    try:
        code = run_interactive(reveal=reveal, force_line=line_mode)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    raise typer.Exit(int(code))


def run_interactive(*, reveal: bool = False, force_line: bool = False) -> ExitCode:
    """Compatibility entry point for the staged terminal editor."""
    del reveal  # secrets are never revealable in the staged editor
    return run_config_editor(_data_dir(), force_line=force_line)


def _run_legacy_interactive(*, reveal: bool = False) -> None:
    """Legacy prompt implementation retained temporarily for API compatibility."""
    data_dir = _data_dir()
    require_config(data_dir)  # fail fast if missing

    console.print(
        Panel.fit(
            "[bold]PA config[/bold]\n"
            "Commands: [cyan]s[/cyan]et  [cyan]a[/cyan]dd  [cyan]r[/cyan]emove  "
            "[cyan]u[/cyan]nset  [cyan]g[/cyan]et  [cyan]l[/cyan]ist  "
            "[cyan]v[/cyan]reveal  [cyan]h[/cyan]elp  [cyan]q[/cyan]uit",
            border_style="blue",
        )
    )
    _print_table(reveal=reveal)

    aliases = {
        "s": "set",
        "a": "add",
        "r": "remove",
        "u": "unset",
        "g": "get",
        "l": "list",
        "v": "reveal",
        "h": "help",
        "q": "quit",
    }

    while True:
        try:
            raw_choice = (
                Prompt.ask(
                    "\n[bold]config[/bold] [dim](s/a/r/u/g/l/v/h/q)[/dim]",
                    default="l",
                )
                .strip()
                .lower()
            )
        except EOFError, KeyboardInterrupt:
            console.print("\nBye.")
            return

        choice = aliases.get(raw_choice, raw_choice)

        if choice == "quit":
            console.print("Bye.")
            return

        if choice == "help":
            console.print(
                Panel(
                    "[bold]s / set[/bold]     Set a key to a value\n"
                    "[bold]a / add[/bold]     Append to a list key (peers, realms, …)\n"
                    "[bold]r / remove[/bold]  Remove an item from a list key\n"
                    "[bold]u / unset[/bold]   Reset a key to default/empty\n"
                    "[bold]g / get[/bold]     Print one key\n"
                    "[bold]l / list[/bold]    Refresh the config table\n"
                    "[bold]v / reveal[/bold]  Toggle masking of secrets\n"
                    "[bold]q / quit[/bold]    Exit",
                    title="Help",
                    border_style="dim",
                )
            )
            continue

        if choice == "list":
            _print_table(reveal=reveal)
            continue

        if choice == "reveal":
            reveal = not reveal
            console.print(f"Sensitive values {'shown' if reveal else 'masked'}.")
            _print_table(reveal=reveal)
            continue

        if choice == "get":
            key = Prompt.ask("Key")
            try:
                spec = get_field_spec(key)
                config = require_config(data_dir)
                value = getattr(config, key)
                console.print(
                    f"[cyan]{key}[/cyan] = "
                    f"{format_value(value, reveal=reveal, sensitive=spec.sensitive)}"
                )
            except ConfigError as exc:
                console.print(str(exc), style="failure")
            continue

        if choice == "set":
            key = Prompt.ask("Key")
            try:
                spec = get_field_spec(key)
            except ConfigError as exc:
                console.print(str(exc), style="failure")
                continue
            if not spec.editable:
                console.print(f"{key} is read-only", style="failure")
                continue
            hint = (
                "true/false"
                if spec.kind == "bool"
                else (
                    "comma-separated or JSON list" if "list" in spec.kind else "value"
                )
            )
            if spec.kind.startswith("optional"):
                hint += " (empty/null to clear)"
            raw = Prompt.ask(f"Value ({hint})", password=spec.sensitive and not reveal)
            try:
                result = set_config_value(data_dir, key, raw)
                _print_mutate(result, reveal=reveal)
                _print_table(reveal=reveal)
            except ConfigError as exc:
                console.print(str(exc), style="failure")
            continue

        if choice == "add":
            key = Prompt.ask("List key", default="peers")
            raw = Prompt.ask("Item to add")
            try:
                result = add_config_value(data_dir, key, raw)
                _print_mutate(result, reveal=reveal)
                _print_table(reveal=reveal)
            except ConfigError as exc:
                console.print(str(exc), style="failure")
            continue

        if choice == "remove":
            key = Prompt.ask("List key", default="peers")
            raw = Prompt.ask("Item to remove")
            try:
                result = remove_config_value(data_dir, key, raw)
                _print_mutate(result, reveal=reveal)
                _print_table(reveal=reveal)
            except ConfigError as exc:
                console.print(str(exc), style="failure")
            continue

        if choice == "unset":
            key = Prompt.ask("Key to reset")
            try:
                get_field_spec(key)
                if not Confirm.ask(
                    f"Reset [cyan]{key}[/cyan] to default?", default=False
                ):
                    continue
                result = unset_config_value(data_dir, key)
                _print_mutate(result, reveal=reveal)
                _print_table(reveal=reveal)
            except ConfigError as exc:
                console.print(str(exc), style="failure")
            continue

        console.print(
            f"[red]Unknown command {raw_choice!r}[/red] — type [cyan]h[/cyan] for help"
        )


def _print_mutate(result: MutateResult, *, reveal: bool) -> None:
    spec = get_field_spec(result.key)
    before = format_value(result.before, reveal=reveal, sensitive=spec.sensitive)
    after = format_value(result.after, reveal=reveal, sensitive=spec.sensitive)
    line = ui.styled(result.op.value, "success")
    line.append(f" {result.key}: {before} → {after}")
    console.print(line)
    refreshed = refresh_after_mutate(_data_dir(), result)
    if refreshed:
        console.print("Service unit environment refreshed.", style="muted")
    if result.restart_required:
        console.print("Restart required — run: pa restart", style="warning")
